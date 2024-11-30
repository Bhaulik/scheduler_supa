from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.asyncio import AsyncIOExecutor
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Any, Dict, List, Optional, Union
from supabase import create_client, Client
from pydantic import BaseModel, UUID4
import logging
import json
import os
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import pytz
from dotenv import load_dotenv
import asyncio
import traceback


# Load environment variables from .env.local
load_dotenv('.env.local')

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobType(Enum):
    EVENT_REMINDER = "event_reminder"
    NOTIFICATION = "notification"
    EMAIL = "email"
    SMS = "sms"
    CUSTOM = "custom"

class TriggerType(Enum):
    ONCE = "once"          # Run once at specific time
    DAILY = "daily"        # Run daily at specific time
    WEEKLY = "weekly"      # Run weekly on specific days
    MONTHLY = "monthly"    # Run monthly on specific dates
    INTERVAL = "interval"  # Run at fixed intervals
    CRON = "cron"         # Run on custom cron schedule

class ScheduledJob(BaseModel):
    job_id: str
    job_type: JobType
    run_date: datetime
    status: JobStatus
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3

    class Config:
        from_attributes = True
        json_encoders = {
            JobStatus: lambda v: v.value,
            JobType: lambda v: v.value,
            datetime: lambda v: v.isoformat(),
            UUID4: str
        }

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        d['status'] = self.status.value
        d['job_type'] = self.job_type.value
        return d

class JobSchedulerConfig(BaseModel):
    retry_failed_jobs: bool = True
    max_retries: int = 3
    store_job_results: bool = True
    cleanup_after_days: int = 7
    
class SupabaseJobScheduler:
    def __init__(self, config: Optional[JobSchedulerConfig] = None, timezone: str = "UTC"):
        self.config = config or JobSchedulerConfig()
        
        # Use AsyncIOScheduler instead of BackgroundScheduler
        executors = {
            'default': AsyncIOExecutor()
        }
        
        self.scheduler = AsyncIOScheduler(
            executors=executors,
            timezone=pytz.timezone(timezone)
        )
        
        # Initialize Supabase
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")
        self.supabase: Client = create_client(supabase_url, supabase_key)
        
        # Start scheduler and restore jobs
        self.scheduler.start()
        self._restore_jobs()
        self._schedule_cleanup_job()
        self.timezone = pytz.timezone(timezone)

    def schedule_reminder(
        self,
        event_id: Union[str, UUID4],
        event_time: datetime,
        notification_func: Callable,
        minutes_before: int = 15,
        **kwargs
    ) -> ScheduledJob:
        """
        Schedule an event reminder
        """
        job_id = f"reminder_{event_id}"
        reminder_time = event_time - timedelta(minutes=minutes_before)
        
        return self._create_job(
            job_id=job_id,
            job_type=JobType.EVENT_REMINDER,
            run_date=reminder_time,
            func=notification_func,
            metadata={
                "event_id": str(event_id),
                "event_time": event_time.isoformat(),
                "minutes_before": minutes_before,
                **kwargs
            }
        )

    def schedule_notification(
        self,
        recipient_id: str,
        message: str,
        send_at: datetime,
        notification_type: str = "push",
        **kwargs
    ) -> ScheduledJob:
        """
        Schedule a notification (push, email, SMS)
        """
        job_id = f"notify_{recipient_id}_{datetime.now().timestamp()}"
        
        return self._create_job(
            job_id=job_id,
            job_type=JobType.NOTIFICATION,
            run_date=send_at,
            func=self._send_notification,
            metadata={
                "recipient_id": recipient_id,
                "message": message,
                "type": notification_type,
                **kwargs
            }
        )

    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job
        """
        try:
            self.scheduler.remove_job(job_id)
            self._update_job_status(job_id, JobStatus.CANCELLED)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    def get_job(self, job_id: str) -> Optional[ScheduledJob]:
        """
        Get a specific job's details
        """
        try:
            response = self.supabase.table('scheduled_jobs')\
                .select('*')\
                .eq('job_id', job_id)\
                .single()\
                .execute()
            return ScheduledJob(**response.data) if response.data else None
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None

    def get_jobs_by_status(self, status: JobStatus) -> List[ScheduledJob]:
        """
        Get all jobs with a specific status
        """
        try:
            response = self.supabase.table('scheduled_jobs')\
                .select('*')\
                .eq('status', status.value)\
                .execute()
            return [ScheduledJob(**job) for job in response.data]
        except Exception as e:
            logger.error(f"Failed to get jobs with status {status}: {e}")
            return []

    def retry_job(self, job_id: str) -> Optional[ScheduledJob]:
        """
        Retry a failed job
        """
        job = self.get_job(job_id)
        if not job or job.status != JobStatus.FAILED:
            return None
            
        if job.retry_count >= job.max_retries:
            logger.warning(f"Job {job_id} has exceeded max retries")
            return None
            
        return self._create_job(
            job_id=f"{job_id}_retry_{job.retry_count + 1}",
            job_type=job.job_type,
            run_date=datetime.now() + timedelta(minutes=5),
            func=self._get_function_for_job_type(job.job_type),
            metadata=job.metadata,
            retry_count=job.retry_count + 1
        )

    def _job_wrapper(self, func: Callable) -> Callable:
        """
        Wrapper for job execution with error handling and status updates
        """
        async def wrapped_func(**kwargs):
            job_id = kwargs.get('job_id')
            try:
                await self._update_job_status(job_id, JobStatus.RUNNING)
                print(f"Starting job {job_id}...")
                
                if asyncio.iscoroutinefunction(func):
                    print(f"Executing async function for job {job_id}")
                    result = await func(**kwargs)
                else:
                    print(f"Executing sync function for job {job_id}")
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, 
                        lambda: func(**kwargs)
                    )
                
                print(f"Completed job {job_id}")
                await self._update_job_status(job_id, JobStatus.COMPLETED)
                return result
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                await self._update_job_status(job_id, JobStatus.FAILED)
                if self.config.retry_failed_jobs:
                    await self.retry_job(job_id)
                raise

        def sync_wrapper(**kwargs):
            """Synchronous wrapper that runs the async function"""
            # Always create a new event loop for the job
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                return loop.run_until_complete(wrapped_func(**kwargs))
            except Exception as e:
                logger.error(f"Error in sync_wrapper: {str(e)}")
                raise
            finally:
                try:
                    # Cancel all running tasks
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    
                    # Run the event loop one last time to clean up
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    
                    loop.run_until_complete(loop.shutdown_asyncgens())
                finally:
                    loop.close()

        return sync_wrapper

    async def _update_job_status(self, job_id: str, status: JobStatus) -> None:
        """Update job status in Supabase"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.supabase.table('scheduled_jobs')
                    .update({'status': status.value})
                    .eq('job_id', job_id)
                    .execute()
            )
            logger.info(f"Updated job status for {job_id} to {status.value}")
        except Exception as e:
            logger.error(f"Failed to update job status for {job_id}: {str(e)}")

    def _restore_jobs(self) -> None:
        """Restore jobs from Supabase on startup"""
        try:
            response = self.supabase.table('scheduled_jobs')\
                .select('*')\
                .eq('status', 'scheduled')\
                .execute()
            
            now = datetime.now(pytz.UTC)  # Get current time in UTC
            
            for job in response.data:
                # Parse the stored datetime and ensure it's timezone aware
                run_date = datetime.fromisoformat(job['run_date'])
                if run_date.tzinfo is None:
                    run_date = pytz.UTC.localize(run_date)
                
                # Only restore future jobs
                if run_date > now:
                    metadata = json.loads(job['metadata'])
                    self._reschedule_job(job['job_id'], run_date, metadata)
                    
            logger.info("Restored scheduled jobs from Supabase")
        except Exception as e:
            logger.error(f"Failed to restore jobs from Supabase: {str(e)}")
            logger.error(traceback.format_exc())  # Add stack trace for debugging

    def _reschedule_job(self, job_id: str, run_date: datetime, metadata: Dict[str, Any]) -> None:
        """Reschedule a job from stored metadata"""
        try:
            # Your logic to recreate the job based on metadata
            # This will depend on how you store function references
            pass
        except Exception as e:
            logger.error(f"Failed to reschedule job {job_id}: {str(e)}")

    def _store_job_metadata(self, job_id: str, run_date: datetime, metadata: Dict[str, Any]) -> None:
        """Store job metadata in Supabase"""
        try:
            job_data = {
                'job_id': job_id,
                'run_date': run_date.isoformat(),
                'metadata': json.dumps(metadata),
                'status': 'scheduled',
                'created_at': datetime.now().isoformat()
            }
            
            self.supabase.table('scheduled_jobs').insert(job_data).execute()
            logger.info(f"Stored job metadata for {job_id} in Supabase")
        except Exception as e:
            logger.error(f"Failed to store job metadata in Supabase: {str(e)}")

    def _schedule_cleanup_job(self) -> None:
        """Schedule a job to clean up old jobs"""
        try:
            self.scheduler.add_job(
                func=self._cleanup_jobs,
                trigger='interval',
                minutes=1440,
                id='cleanup_jobs'
            )
            logger.info("Scheduled job to clean up old jobs")
        except Exception as e:
            logger.error(f"Failed to schedule job to clean up old jobs: {str(e)}")

    def _cleanup_jobs(self) -> None:
        """Clean up old jobs"""
        try:
            # Your logic to clean up old jobs
            # This will depend on how you store job metadata
            pass
            logger.info("Cleaned up old jobs")
        except Exception as e:
            logger.error(f"Failed to clean up old jobs: {str(e)}")

    def schedule_recurring(
        self,
        func: Callable,
        trigger_type: TriggerType,
        job_id: str,
        run_date: datetime,
        **trigger_args
    ) -> ScheduledJob:
        """
        Schedule a function to run on a recurring basis
        """
        # Convert datetime to ISO format string for JSON serialization
        metadata = {
            "trigger_type": trigger_type.value,
            "trigger_args": {
                k: v.isoformat() if isinstance(v, datetime) else v 
                for k, v in trigger_args.items()
            }
        }
        
        return self._create_job(
            job_id=job_id,
            job_type=JobType.CUSTOM,
            run_date=run_date,
            func=func,
            metadata=metadata
        )

    def _create_trigger(self, trigger_type: TriggerType, **kwargs) -> Union[CronTrigger, IntervalTrigger, DateTrigger]:
        """Create the appropriate trigger based on type and arguments"""
        if trigger_type == TriggerType.ONCE:
            return DateTrigger(
                run_date=kwargs.get('run_date'),
                timezone=self.timezone
            )
            
        elif trigger_type == TriggerType.DAILY:
            return CronTrigger(
                hour=kwargs.get('hour', 0),
                minute=kwargs.get('minute', 0),
                timezone=self.timezone
            )
            
        elif trigger_type == TriggerType.WEEKLY:
            return CronTrigger(
                day_of_week=kwargs.get('days', 'mon-fri'),
                hour=kwargs.get('hour', 0),
                minute=kwargs.get('minute', 0),
                timezone=self.timezone
            )
            
        elif trigger_type == TriggerType.MONTHLY:
            return CronTrigger(
                day=kwargs.get('day', 1),
                hour=kwargs.get('hour', 0),
                minute=kwargs.get('minute', 0),
                timezone=self.timezone
            )
            
        elif trigger_type == TriggerType.INTERVAL:
            return IntervalTrigger(
                hours=kwargs.get('hours', 0),
                minutes=kwargs.get('minutes', 0),
                seconds=kwargs.get('seconds', 0),
                timezone=self.timezone
            )
            
        elif trigger_type == TriggerType.CRON:
            return CronTrigger.from_crontab(
                kwargs.get('cron_expression'),
                timezone=self.timezone
            )

    def schedule_one_time_job(
        self,
        func: Callable,
        run_at: datetime,
        job_id: Optional[str] = None,
        **kwargs
    ) -> ScheduledJob:
        """Schedule a one-time job"""
        job_id = job_id or f"job_{datetime.now().timestamp()}"
        
        return self._create_job(
            job_id=job_id,
            job_type=JobType.CUSTOM,
            run_date=run_at,
            func=func,
            metadata=kwargs
        )

    def _create_job(
        self,
        job_id: str,
        job_type: JobType,
        run_date: datetime,
        func: Callable,
        metadata: Dict[str, Any],
        retry_count: int = 0
    ) -> ScheduledJob:
        """Create and store a job"""
        # Ensure run_date is timezone-aware
        if run_date.tzinfo is None:
            run_date = pytz.UTC.localize(run_date)
            
        now = datetime.now(pytz.UTC)
        
        job_dict = {
            'job_id': job_id,
            'job_type': job_type.value,
            'run_date': run_date.isoformat(),
            'status': JobStatus.SCHEDULED.value,
            'metadata': metadata,
            'created_at': now.isoformat(),
            'retry_count': retry_count,
            'max_retries': self.config.max_retries
        }

        # Store in Supabase
        self.supabase.table('scheduled_jobs').insert(job_dict).execute()
        
        # Schedule in APScheduler with the wrapped function
        wrapped_func = self._job_wrapper(func)
        self.scheduler.add_job(
            func=wrapped_func,
            trigger='date',
            run_date=run_date,
            id=job_id,
            kwargs={'job_id': job_id, **metadata},
            misfire_grace_time=None
        )

        return ScheduledJob(**job_dict)

    def _get_function_for_job_type(self, job_type: str):
        """
        Returns the appropriate function for the given job type
        
        Args:
            job_type (str): The type of job to execute
            
        Returns:
            callable: The function to execute for this job type
        """
        job_type_mapping = {
            # Add other job types here
        }
        
        if job_type not in job_type_mapping:
            raise ValueError(f"Unknown job type: {job_type}")
        
        return job_type_mapping[job_type]

# Create the Supabase table (run this SQL in Supabase SQL editor):
"""
create table scheduled_jobs (
    id bigint generated by default as identity primary key,
    job_id text not null,
    run_date timestamp with time zone not null,
    metadata jsonb not null,
    status text not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone default now()
);

-- Enable realtime for this table
alter table scheduled_jobs replica identity full;
alter publication supabase_realtime add table scheduled_jobs;
"""

# Create a global instance
scheduler = SupabaseJobScheduler()

# Expose convenient functions
def schedule_event_reminder(
    event_id: str,
    event_time: datetime,
    reminder_func: Callable,
    minutes_before: int = 15,
    **kwargs: Any
) -> str:
    """
    Schedule an event reminder
    
    Args:
        event_id: Event identifier
        event_time: When the event starts
        reminder_func: Function to call for reminder
        minutes_before: Minutes before event to send reminder
        **kwargs: Additional arguments for the reminder function
    """
    reminder_time = event_time - timedelta(minutes=minutes_before)
    return scheduler.schedule_job(
        job_id=f"reminder_{event_id}",
        run_date=reminder_time,
        func=reminder_func,
        **kwargs
    )

def cancel_event_reminder(event_id: str) -> bool:
    """Cancel an event reminder"""
    return scheduler.cancel_job(f"reminder_{event_id}")

def get_scheduled_reminders() -> List[Dict[str, Any]]:
    """Get all scheduled reminders"""
    return scheduler.get_jobs()

def shutdown_scheduler():
    """Shutdown the scheduler"""
    scheduler.shutdown() 