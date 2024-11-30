# Supabase Job Scheduler Documentation
scheduler.py is the core file

A robust asynchronous job scheduler that integrates with Supabase for persistent job storage. This scheduler supports various types of jobs, including one-time and recurring tasks, with built-in error handling and job status tracking.

## Key Features

- 🔄 Persistent job storage in Supabase
- ⏰ Multiple trigger types (one-time, daily, weekly, monthly, interval, cron)
- 🔁 Automatic job restoration after system restarts
- ⚠️ Error handling and automatic retries
- 📊 Job status tracking
- 🧹 Automatic cleanup of old jobs

## Core Components

### Job Types (`JobType`)
```python
class JobType(Enum):
    EVENT_REMINDER = "event_reminder"
    NOTIFICATION = "notification"
    EMAIL = "email"
    SMS = "sms"
    CUSTOM = "custom"
```

### Trigger Types (`TriggerType`)
```python
class TriggerType(Enum):
    ONCE = "once"          # Run once at specific time
    DAILY = "daily"        # Run daily at specific time
    WEEKLY = "weekly"      # Run weekly on specific days
    MONTHLY = "monthly"    # Run monthly on specific dates
    INTERVAL = "interval"  # Run at fixed intervals
    CRON = "cron"         # Run on custom cron schedule
```

### Job Status (`JobStatus`)
```python
class JobStatus(Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
```

## Usage Examples

### 1. Basic Setup
```python
from scheduler import SupabaseJobScheduler

# Create scheduler instance
scheduler = SupabaseJobScheduler()
```

### 2. Schedule an Event Reminder
```python
async def send_reminder(event_id: str, message: str):
    # Your reminder logic here
    pass

scheduler.schedule_reminder(
    event_id="123",
    event_time=datetime(2024, 3, 1, 14, 0),
    notification_func=send_reminder,
    minutes_before=15,
    message="Don't forget your meeting!"
)
```

### 3. Schedule a Recurring Job
```python
scheduler.schedule_recurring(
    func=your_function,
    trigger_type=TriggerType.DAILY,
    job_id="daily_report",
    run_date=datetime.now(),
    hour=8,
    minute=0
)
```

### 4. Schedule a One-time Job
```python
scheduler.schedule_one_time_job(
    func=your_function,
    run_at=datetime(2024, 3, 1, 14, 0),
    job_id="one_time_task"
)
```

## Job Management

### Cancel a Job
```python
scheduler.cancel_job("job_id")
```

### Get Job Details
```python
job = scheduler.get_job("job_id")
```

### Get Jobs by Status
```python
failed_jobs = scheduler.get_jobs_by_status(JobStatus.FAILED)
```

### Retry Failed Job
```python
scheduler.retry_job("job_id")
```

## Database Setup

The scheduler requires a Supabase table with the following schema:

```sql
create table scheduled_jobs (
    id bigint generated by default as identity primary key,
    job_id text not null,
    job_type text not null,
    run_date timestamp with time zone not null,
    status text not null,
    metadata jsonb not null,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone default now(),
    last_run timestamp with time zone,
    next_run timestamp with time zone,
    retry_count integer default 0,
    max_retries integer default 3
);

-- Add indexes for common queries
create index idx_scheduled_jobs_job_id on scheduled_jobs(job_id);
create index idx_scheduled_jobs_status on scheduled_jobs(status);
create index idx_scheduled_jobs_run_date on scheduled_jobs(run_date);

-- Enable realtime for this table (optional)
alter table scheduled_jobs replica identity full;
alter publication supabase_realtime add table scheduled_jobs;

-- Add enum types for status and job_type (optional but recommended)
create type job_status as enum ('scheduled', 'running', 'completed', 'failed', 'cancelled');
create type job_type as enum ('event_reminder', 'notification', 'email', 'sms', 'custom');

-- Add comment for documentation
comment on table scheduled_jobs is 'Stores scheduled job information for the background task scheduler';
```

## Configuration

You can customize the scheduler behavior using `JobSchedulerConfig`:

```python
config = JobSchedulerConfig(
    retry_failed_jobs=True,
    max_retries=3,
    store_job_results=True,
    cleanup_after_days=7
)

scheduler = SupabaseJobScheduler(config=config)
```

## Environment Variables

Required environment variables in `.env.local`:
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_ANON_KEY`: Your Supabase anonymous key

This scheduler is particularly useful for applications that need reliable job scheduling with persistence, error handling, and status tracking. It's built on top of APScheduler and uses Supabase as a backing store, making it robust and scalable.