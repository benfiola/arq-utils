# arq-utils

Utilities around [arq](https://arq-docs.helpmanual.io/) intended to streamline working with the library

## Quickstart

```python3
from arq_utils import RedisSettings, Registry, Schedule
from enum import Enum


# define queues
class Queue(Enum):
    Default = "Default"
    Other = "Other"


# create a registry
registry = Registry(
    Queue.Default,
    automatic_job_id=True,
    graceful_termination=True,
    reenqueue_complete_tasks=True,
)


# export APIs
Worker = registry.worker_class()
create_pool = registry.create_pool
enqueue_task = registry.enqueue_task


# register "task" as a task function
@registry.task
async def task(a: int):
    print(a)


# scheduled tasks
# register "task_two" as a task function
# schedule "task_two" to run every minute with b set to "one" on queue "other"
# schedule "task_two" to run every hour with b set to "two" on the default queue
@registry.task
@registry.schedule(Schedule.cron("* * * * *"), arguments={"b": "one"}, queue=Queue.Other)
@registry.schedule(Schedule.cron("0 * * * *"), arguments={"b": "two"})
async def task_two(b: str):
    print(a)



async def as_client():
    # manually enqueue "task" with a set to 1 onto queue "other"
    pool = await create_pool(RedisSettings())
    await enqueue_task(pool, task.create_task(a=1), queue=Queue.Other)


async def as_worker():
    # create and run a worker listening to queues "default" and "other"
    pool = await create_pool(RedisSettings())
    worker = Worker(pool, {Queue.Default, Queue.Other})
    await worker.run_async()
```

## Features

* Decorator-based task registration
* Decorator-based scheduled task definition
* Support for synchronous task functions
* Typed `create_task` factory which inherits type signature from decorated task function
* Queue aware `Worker` class and `enqueue_task` typed interface
* `Worker` class capable of listening to multiple queues at once

## Optional

* `automatic_job_id`: When set to `True`, hashes the function name and input arguments to generate a job id for all enqueued tasks.  Assumes that all tasks should be unique as a function of function + input arguments.
* `graceful_termination`: When set to `True`, cancels and awaits all running tasks prior to terminating a `Worker`.  This allows task functions to perform cleanup on task cancellation prior to worker shutdown - but assumes cooperative task function implementation.
* `reenqueue_complete_tasks`: When set to `True`, will forcefully expire completed tasks for `job_id` prior to enqueueing a new task with `job_id`.  This enables `job_id` to _only_ deduplicate on pending and in-progress tasks (and not completed tasks).
