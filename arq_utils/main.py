import asyncio
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
import hashlib
import importlib
import inspect
import json
import logging
import re
from signal import SIGINT, SIGTERM, Signals
from typing import Any, Callable, cast, Generic, Optional, Protocol, Type, TypeVar
from typing_extensions import ParamSpec

import arq
import arq.constants
import arq.connections
import arq.jobs
import arq.worker


logger = logging.getLogger("arq_utils")


RedisSettings = arq.connections.RedisSettings


def import_function(function_import_path: str) -> Callable:
    module_name, attr = function_import_path.split(":")
    module = importlib.import_module(module_name)
    function = getattr(module, attr)

    if not inspect.isfunction(function):
        raise ValueError(f"not a function: {function_import_path}")

    return function


def get_function_import_path(f: Callable) -> str:
    module = getattr(f, "__module__", None)

    if not module:
        raise ValueError(f"module not defined: {f}")

    return f"{module}:{f.__name__}"


@dataclass
class Task:
    function_import_path: str
    arguments: dict[str, Any]
    job_id: Optional[str]


@dataclass
class Schedule:
    second: set[int] = field(default_factory=set)
    minute: set[int] = field(default_factory=set)
    hour: set[int] = field(default_factory=set)
    day: set[int] = field(default_factory=set)
    month: set[int] = field(default_factory=set)
    weekday: set[int] = field(default_factory=set)

    @classmethod
    def cron(cls, expression: str) -> "Schedule":
        invalid_cron_expression = partial(
            ValueError, f"invalid cron expression: {expression}"
        )
        expression = expression.strip()

        parts = expression.split(" ")
        parts_meta = [
            dict(key="minute", min=0, max=59),
            dict(key="hour", min=0, max=23),
            dict(key="day", min=1, max=31),
            dict(key="month", min=1, max=12),
            dict(key="weekday", min=0, max=6),
        ]
        if len(parts) != len(parts_meta):
            raise invalid_cron_expression()

        re_step = re.compile(r"(.*)/(\d+)")
        re_range = re.compile(r"(\d+)-(\d+)")
        re_wildcard = re.compile(r"\*")
        re_value = re.compile(r"(\d+)")

        kwargs = {}
        for part, part_meta in zip(parts, parts_meta):
            key = str(part_meta["key"])
            part_min = int(part_meta["min"])
            part_max = int(part_meta["max"])

            values = kwargs[key] = set()

            for subpart in part.split(","):
                step = 1

                if match := re_step.match(subpart):
                    subpart = str(match.groups()[0])
                    step = int(match.groups()[1])

                if match := re_range.match(subpart):
                    start = int(match.groups()[0])
                    end = int(match.groups()[1])
                elif match := re_value.match(subpart):
                    start = end = int(match.groups()[0])
                elif match := re_wildcard.match(subpart):
                    start = part_min
                    end = part_max
                else:
                    raise invalid_cron_expression()

                if any([step <= 0, start > end, start < part_min, end > part_max]):
                    raise invalid_cron_expression()

                for value in range(start, end + 1, step):
                    values.add(value)

        return cls(second=set([0]), **kwargs)


ST_Queue = TypeVar("ST_Queue", bound=Enum)


@dataclass
class ScheduledTask(Task, Generic[ST_Queue]):
    schedule: Schedule
    queue: Optional[ST_Queue]


TC_P = ParamSpec("TC_P")
TC_RV = TypeVar("TC_RV", covariant=True)


class TaskCallable(Protocol[TC_P, TC_RV]):
    def __call__(self, *args: TC_P.args, **kwargs: TC_P.kwargs) -> TC_RV:
        ...

    def create_task(
        self, _job_id: Optional[str] = None, *args: TC_P.args, **kwargs: TC_P.kwargs
    ) -> Task:
        ...


R_P = ParamSpec("R_P")
R_Queue = TypeVar("R_Queue", bound=Enum)
R_RV = TypeVar("R_RV")


class Registry(Generic[R_Queue]):
    automatic_job_id: bool
    default_queue: R_Queue
    expire_completed_tasks_on_requeue: bool
    graceful_termination: bool
    scheduled_tasks: list[ScheduledTask[R_Queue]]
    task_function_import_paths: list[str]

    def __init__(
        self,
        default_queue: R_Queue,
        automatic_job_id: bool = False,
        expire_completed_tasks_on_requeue: bool = False,
        graceful_termination: bool = False,
    ):
        self.automatic_job_id = automatic_job_id
        self.default_queue = default_queue
        self.graceful_termination = graceful_termination
        self.expire_completed_tasks_on_requeue = expire_completed_tasks_on_requeue
        self.scheduled_tasks = []
        self.task_function_import_paths = []

    def schedule(
        self,
        schedule: Schedule,
        arguments: Optional[dict[str, Any]] = None,
        job_id: Optional[str] = None,
        queue: Optional[R_Queue] = None,
    ):
        registry = self

        def inner(f: Callable[R_P, R_RV]) -> Callable[R_P, R_RV]:
            scheduled_task = ScheduledTask(
                arguments=arguments or {},
                function_import_path=get_function_import_path(f),
                queue=queue,
                schedule=schedule,
                job_id=job_id,
            )
            registry.scheduled_tasks.append(scheduled_task)
            return f

        return inner

    def task(self, f: Callable[R_P, R_RV]) -> TaskCallable[R_P, R_RV]:
        function_import_path = get_function_import_path(f)

        def create_task(
            _job_id: Optional[str] = None, *args: R_P.args, **kwargs: R_P.kwargs
        ):
            arguments = inspect.signature(f).bind(*args, **kwargs).arguments

            return Task(
                arguments=arguments,
                function_import_path=function_import_path,
                job_id=_job_id,
            )

        setattr(f, "create_task", create_task)
        f = cast(TaskCallable[R_P, R_RV], f)

        self.task_function_import_paths.append(function_import_path)

        return f

    async def enqueue_task(
        self,
        pool: arq.connections.ArqRedis,
        task: Task,
        queue: Optional[R_Queue] = None,
    ):
        queue = queue or self.default_queue

        if not isinstance(pool, ArqRedisWrapper):
            pool = ArqRedisWrapper(self, pool)

        job_id = task.job_id
        if job_id is None and self.automatic_job_id:
            job_id = self.get_job_id(task)

        await pool.enqueue_job(
            task.function_import_path,
            _queue_name=queue.value,
            _job_id=job_id,
            **task.arguments,
        )

    def get_job_id(self, task: Task) -> str:
        try:
            import pydantic.json
        except ImportError:
            raise ValueError(f"pydantic required to create data hashes")

        data = {"name": task.function_import_path, "data": task.arguments}
        data_str = json.dumps(
            data, default=pydantic.json.pydantic_encoder, sort_keys=True
        )
        data_bytes = data_str.encode("utf-8")

        return hashlib.blake2b(data_bytes, digest_size=32).hexdigest()

    async def create_pool(self, *args, **kwargs) -> arq.connections.ArqRedis:
        pool = await arq.create_pool(*args, **kwargs)
        return ArqRedisWrapper(self, pool)

    def worker_class(self) -> "Type[Worker[R_Queue]]":
        registry = self

        class RegistryWorker(Worker):
            def __init__(self, *args, **kwargs):
                self.registry = registry
                super().__init__(*args, **kwargs)

        return RegistryWorker


class ArqRedisWrapper(arq.connections.ArqRedis):
    __registry: Registry
    __wrapped: arq.connections.ArqRedis

    def __init__(self, registry: Registry, wrapped: arq.connections.ArqRedis):
        self.__registry = registry
        self.__wrapped = wrapped

    def __getattr__(self, attr: str):
        return getattr(self.__wrapped, attr)

    async def enqueue_job(self, *args, **kwargs):
        if self.__registry.expire_completed_tasks_on_requeue:
            if job_id := kwargs.get("_job_id"):
                async with self.__wrapped.pipeline(transaction=True) as pipeline:
                    result_key = arq.constants.result_key_prefix + job_id
                    await pipeline.delete(result_key)
                    await pipeline.execute()

        return await self.__wrapped.enqueue_job(*args, **kwargs)


CWC_P = ParamSpec("CWC_P")
CWC_RV = TypeVar("CWC_RV")


def create_worker_callable(f: Callable, defaults: Optional[dict[str, Any]] = None):
    defaults = defaults or {}

    async def inner(ctx: dict[Any, Any], *args: CWC_P.args, **kwargs: CWC_P.kwargs):
        kwargs_ = dict(**defaults)
        kwargs_.update(kwargs)

        if not inspect.iscoroutinefunction(f):
            loop = asyncio.get_event_loop()
            coro = loop.run_in_executor(None, lambda: f(*args, **kwargs_))
        else:
            coro = f(*args, **kwargs_)

        return await coro

    return inner


W_Queue = TypeVar("W_Queue", bound=Enum)


class Worker(Generic[W_Queue]):
    registry: Registry[W_Queue] = cast(Registry[W_Queue], None)
    workers: list[arq.worker.Worker]

    def __init__(
        self,
        pool: arq.connections.ArqRedis,
        queues: set[W_Queue],
        **kwargs,
    ):
        if not self.registry:
            raise ValueError(f"registry unset: {self.__class__}")

        kwargs.pop("queue_name", None)
        kwargs.pop("redis_pool", None)
        kwargs.pop("redis_settings", None)

        if not isinstance(pool, ArqRedisWrapper):
            pool = ArqRedisWrapper(self.registry, pool)

        handle_signals = kwargs.pop("handle_signals", True)
        if handle_signals:
            for signal in [SIGINT, SIGTERM]:
                asyncio.get_event_loop().add_signal_handler(signal, self.handle_signal)

        self.arq_workers = []
        functions = []
        for task_function_import_path in self.registry.task_function_import_paths:
            task_function = import_function(task_function_import_path)
            function = arq.func(
                create_worker_callable(task_function), name=task_function_import_path
            )
            functions.append(function)

        scheduled_task_map: dict[W_Queue, list[ScheduledTask[W_Queue]]] = {}
        for scheduled_task in self.registry.scheduled_tasks:
            queue = scheduled_task.queue or self.registry.default_queue
            scheduled_tasks = scheduled_task_map.setdefault(queue, [])
            scheduled_tasks.append(scheduled_task)

        for queue in queues:
            cron_jobs = []
            scheduled_tasks = scheduled_task_map.get(queue, [])
            for scheduled_task in scheduled_tasks:
                function_import_path = scheduled_task.function_import_path
                schedule = scheduled_task.schedule
                _job_id = self.registry.get_job_id(scheduled_task)

                job_id = scheduled_task.job_id
                if job_id is None and self.registry.automatic_job_id:
                    job_id = _job_id

                task_function = import_function(function_import_path)
                name = f"cron:{function_import_path}:{_job_id}"

                cron_job = arq.cron(
                    create_worker_callable(
                        task_function, defaults=scheduled_task.arguments
                    ),
                    name=name,
                    job_id=job_id,
                    second=schedule.second,
                    minute=schedule.minute,
                    hour=schedule.hour,
                    day=schedule.day,
                    month=schedule.month,
                    weekday=schedule.weekday,
                )
                cron_jobs.append(cron_job)

            arq_worker = arq.worker.Worker(
                functions,
                cron_jobs=cron_jobs,
                queue_name=queue.value,
                redis_pool=pool,
                handle_signals=False,
                **kwargs,
            )
            self.arq_workers.append(arq_worker)

    async def handle_signal_async(self, signal: Signals):
        if self.registry.graceful_termination:
            for worker in self.workers:
                for task in worker.tasks.values():
                    if not task.cancel():
                        continue
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        for worker in self.workers:
            worker.handle_sig(signal)

    def handle_signal(self, signal: Signals):
        asyncio.get_event_loop().call_soon_threadsafe(self.handle_signal_async, signal)

    def run(self):
        asyncio.get_event_loop().run_until_complete(self.run_async(close=True))

    async def run_async(self, close: bool = False):
        try:
            await self.main()
        except asyncio.CancelledError:
            pass
        finally:
            if close:
                await self.close()

    async def main(self):
        await asyncio.gather(*(w.main() for w in self.arq_workers))

    async def close(self):
        await asyncio.gather(*(w.close() for w in self.arq_workers))
