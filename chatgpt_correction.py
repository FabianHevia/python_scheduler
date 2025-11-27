"""
AdvancedCron.py
Motor tipo cron avanzado (sin dependencias externas).
Soporta:
 - cron expressions: "*/5 * * * *" (min granularity) y with seconds: "0/10 * * * * *"
 - special: "@every 10s", "@every 5m", "@every 2h"
 - timezone support via zoneinfo (Python 3.9+)
 - sync & async callables
 - thread pool execution with per-job concurrency limits
 - retries, timeouts, misfire handling, persistence
"""

import re
import time
import json
import uuid
import asyncio
import logging
import inspect
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Callable, Optional, Any, Dict, List
from concurrent.futures import ThreadPoolExecutor
import threading
import random

# --- Logging setup ---
logger = logging.getLogger("AdvancedCron")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
logger.addHandler(_handler)

# --- Utilities for parsing ---
def _parse_every_spec(spec: str) -> Optional[timedelta]:
    # "@every 10s", "@every 5m", "@every 2h", "@every 1d"
    m = re.match(r"@every\s+(\d+)([smhd])$", spec)
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2)
    if unit == "s": return timedelta(seconds=val)
    if unit == "m": return timedelta(minutes=val)
    if unit == "h": return timedelta(hours=val)
    if unit == "d": return timedelta(days=val)
    return None

# Cron field ranges
FIELD_RANGES = {
    "sec": (0, 59),
    "min": (0, 59),
    "hour": (0, 23),
    "dom": (1, 31),
    "month": (1, 12),
    "dow": (0, 6),  # 0=Sunday
}

def _expand_field(token: str, name: str) -> List[int]:
    """
    Parse a single cron field token into list of matching ints.
    Supports: "*", "*/n", "a-b", "a-b/n", "a,b,c", numbers
    """
    lo, hi = FIELD_RANGES[name]
    if token == "*" or token == "?":
        return list(range(lo, hi+1))
    parts = token.split(",")
    out = set()
    for part in parts:
        part = part.strip()
        step = 1
        if "/" in part:
            part, step_s = part.split("/")
            step = int(step_s)
        if part == "*" or part == "":
            rng_lo, rng_hi = lo, hi
        elif "-" in part:
            a, b = part.split("-")
            rng_lo, rng_hi = int(a), int(b)
        else:
            # single number
            val = int(part)
            if val < lo or val > hi:
                raise ValueError(f"Value {val} out of range for {name}")
            out.add(val)
            continue
        for v in range(rng_lo, rng_hi+1, step):
            if v < lo or v > hi:
                continue
            out.add(v)
    return sorted(out)

@dataclass
class CronExpression:
    """
    Representa una expresión cron.
    Soporta 5 or 6 fields (if 6 fields present, first is seconds).
    """
    expr: str
    has_seconds: bool = field(init=False)
    fields: Dict[str, List[int]] = field(init=False)

    def __post_init__(self):
        expr = self.expr.strip()
        # special @every
        every = _parse_every_spec(expr)
        if every is not None:
            self.has_seconds = True
            # We'll store as special: fields empty and store interval in metadata externally.
            self.fields = {"every": every}
            return

        parts = expr.split()
        if len(parts) == 6:
            # sec min hour dom month dow
            self.has_seconds = True
            sec_t, min_t, hour_t, dom_t, mon_t, dow_t = parts
        elif len(parts) == 5:
            # min hour dom month dow -> assume seconds = 0
            self.has_seconds = False
            sec_t = "0"
            min_t, hour_t, dom_t, mon_t, dow_t = parts
        else:
            raise ValueError("Cron expression must have 5 or 6 fields or @every")
        self.fields = {
            "sec": _expand_field(sec_t, "sec"),
            "min": _expand_field(min_t, "min"),
            "hour": _expand_field(hour_t, "hour"),
            "dom": _expand_field(dom_t, "dom"),
            "month": _expand_field(mon_t, "month"),
            "dow": _expand_field(dow_t, "dow"),
        }

    def matches(self, dt: datetime) -> bool:
        # dt should be timezone-aware
        if "every" in self.fields:
            return True  # caller will handle interval logic
        f = self.fields
        return (dt.second in f["sec"]
                and dt.minute in f["min"]
                and dt.hour in f["hour"]
                and dt.day in f["dom"]
                and dt.month in f["month"]
                and dt.weekday() in f["dow"])

    def next_after(self, after_dt: datetime, max_search_seconds: int = 365*24*3600) -> Optional[datetime]:
        """
        Find the next datetime strictly after `after_dt` that matches the expression.
        Searches forward but with an upper bound (default 1 year).
        This is done by stepping second-by-second; acceptable for a scheduler demo.
        """
        if "every" in self.fields:
            # Not used for cron expression next - caller uses interval
            return after_dt + self.fields["every"]
        # start searching from the next second
        t = after_dt + timedelta(seconds=1)
        end = after_dt + timedelta(seconds=max_search_seconds)
        while t <= end:
            if self.matches(t):
                return t
            t += timedelta(seconds=1)
        return None

# --- Job dataclass ---
@dataclass
class CronJob:
    id: str
    name: str
    expr: str  # cron expression or @every
    func: Callable
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    tz: Optional[str] = None  # timezone name
    enabled: bool = True
    max_concurrency: int = 1  # per-job parallelism limit
    timeout_seconds: Optional[int] = None
    retries: int = 0
    retry_backoff: float = 1.0  # multiplier
    misfire_policy: str = "run_immediately"  # or 'skip'
    run_on_start: bool = False
    jitter_seconds: int = 0  # random jitter upto this many seconds
    last_run: Optional[str] = None
    next_run_iso: Optional[str] = None
    persistent_meta: Dict[str, Any] = field(default_factory=dict)

    # Internals (not persisted unless wanted)
    _expr_parsed: CronExpression = field(init=False, repr=False)
    _running_count: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self):
        self._expr_parsed = CronExpression(self.expr)

# --- Scheduler ---
class AdvancedCronScheduler:
    def __init__(self,
                 timezone_name: str = "UTC",
                 max_workers: int = 10,
                 poll_interval: float = 0.5,
                 persist_file: Optional[str] = None):
        self.tz = ZoneInfo(timezone_name)
        self.jobs: Dict[str, CronJob] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._async_loop = asyncio.new_event_loop()
        self._loop_thread = None
        self._running = False
        self._poll_interval = poll_interval
        self._global_semaphore = threading.Semaphore(max_workers)
        self._persist_file = persist_file
        self._stop_event = threading.Event()

    # --- job management ---
    def add_job(self, name: str, expr: str, func: Callable,
                args=None, kwargs=None, tz: Optional[str]=None,
                **job_kwargs) -> str:
        jid = str(uuid.uuid4())
        args = args or []
        kwargs = kwargs or {}
        job = CronJob(id=jid, name=name, expr=expr, func=func,
                      args=args, kwargs=kwargs, tz=tz or self.tz.key if hasattr(self.tz, 'key') else str(self.tz),
                      **job_kwargs)
        # compute initial next_run
        job.next_run_iso = self._compute_next_run_iso(job, base_dt=datetime.now(timezone.utc))
        self.jobs[jid] = job
        logger.info(f"Added job {job.name} ({jid}) next at {job.next_run_iso}")
        if self._persist_file:
            self._save_persist()
        return jid

    def remove_job(self, job_id: str):
        if job_id in self.jobs:
            del self.jobs[job_id]
            if self._persist_file:
                self._save_persist()

    def list_jobs(self) -> List[Dict[str,Any]]:
        return [self._job_summary(j) for j in self.jobs.values()]

    def _job_summary(self, job: CronJob) -> Dict[str,Any]:
        return {
            "id": job.id,
            "name": job.name,
            "expr": job.expr,
            "enabled": job.enabled,
            "next_run": job.next_run_iso,
            "last_run": job.last_run,
        }

    # --- persistence ---
    def _save_persist(self):
        data = []
        for j in self.jobs.values():
            jd = asdict(j)
            # remove non-serializable items
            jd.pop("_expr_parsed", None)
            jd.pop("_lock", None)
            jd.pop("_running_count", None)
            data.append(jd)
        with open(self._persist_file, "w", encoding="utf-8") as f:
            json.dump({"tz": getattr(self.tz, 'key', str(self.tz)), "jobs": data}, f, indent=2, default=str)
        logger.info("Persisted jobs to " + self._persist_file)

    def load_persist(self):
        if not self._persist_file:
            raise RuntimeError("No persist file configured")
        with open(self._persist_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for jd in data.get("jobs", []):
            # reconstruct CronJob (note: functions can't be persisted, user must re-add or use import path approach)
            # We'll keep placeholders; user should rebind functions by name or re-add jobs programmatically.
            jd_fields = jd.copy()
            func_placeholder = lambda *a, **k: logger.warning(f"Placeholder job {jd_fields.get('name')} ran - rebind a real function.")
            jid = jd_fields.pop("id")
            job = CronJob(id=jid, func=func_placeholder, **jd_fields)
            self.jobs[jid] = job
        logger.info(f"Loaded {len(self.jobs)} jobs from {self._persist_file}")

    # --- scheduling logic ---
    def _now_in_tz(self, tzname: Optional[str]) -> datetime:
        tzobj = ZoneInfo(tzname) if tzname else self.tz
        return datetime.now(tzobj)

    def _compute_next_run_iso(self, job: CronJob, base_dt: Optional[datetime]=None) -> Optional[str]:
        base_dt = base_dt or datetime.now(timezone.utc)
        # use job tz
        tzobj = ZoneInfo(job.tz) if job.tz else self.tz
        base_local = base_dt.astimezone(tzobj)
        cp: CronExpression = job._expr_parsed
        if "every" in cp.fields:
            # use interval
            # if last_run exists, schedule from last_run; else from base
            if job.last_run:
                last = datetime.fromisoformat(job.last_run).astimezone(timezone.utc)
                candidate = last + cp.fields["every"]
            else:
                candidate = base_local + cp.fields["every"]
            # jitter
            if job.jitter_seconds:
                candidate = candidate + timedelta(seconds=random.randint(0, job.jitter_seconds))
            return candidate.astimezone(timezone.utc).isoformat()
        else:
            # search next match in local tz
            # We rely on CronExpression.next_after which works with aware datetime
            next_dt = cp.next_after(base_local)
            if not next_dt:
                return None
            # apply jitter
            if job.jitter_seconds:
                next_dt = next_dt + timedelta(seconds=random.randint(0, job.jitter_seconds))
            # return UTC ISO
            return next_dt.astimezone(timezone.utc).isoformat()

    def start(self):
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        # start asyncio loop in separate thread to support async tasks
        self._loop_thread = threading.Thread(target=self._async_loop_worker, daemon=True)
        self._loop_thread.start()
        # start main poller
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        logger.info("AdvancedCronScheduler started")

    def stop(self, wait: bool = True):
        self._running = False
        self._stop_event.set()
        # stop async loop
        try:
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        except Exception:
            pass
        if wait:
            if self._loop_thread:
                self._loop_thread.join(timeout=5)
            if self._poll_thread:
                self._poll_thread.join(timeout=5)
        self._executor.shutdown(wait=wait)
        logger.info("AdvancedCronScheduler stopped")

    def _async_loop_worker(self):
        asyncio.set_event_loop(self._async_loop)
        self._async_loop.run_forever()

    def _poll_loop(self):
        while not self._stop_event.is_set():
            now_utc = datetime.now(timezone.utc)
            for job in list(self.jobs.values()):
                try:
                    if not job.enabled:
                        continue
                    # compute next_run if missing
                    if not job.next_run_iso:
                        job.next_run_iso = self._compute_next_run_iso(job, base_dt=now_utc)
                    if not job.next_run_iso:
                        continue
                    next_run_dt = datetime.fromisoformat(job.next_run_iso).astimezone(timezone.utc)
                    # if due or past due
                    if now_utc >= next_run_dt:
                        # misfire handling: decide whether to run
                        should_run = True
                        if now_utc - next_run_dt > timedelta(hours=24) and job.misfire_policy == "skip":
                            logger.info(f"Skipping job {job.name} due to misfire policy (old)")
                            should_run = False
                        if should_run:
                            # concurrency guards
                            if job._running_count >= job.max_concurrency:
                                logger.info(f"Job {job.name} concurrency limit reached ({job._running_count})")
                            else:
                                # submit for execution
                                job._running_count += 1
                                self._executor.submit(self._run_job_wrapper, job)
                        # schedule next run
                        job.last_run = now_utc.isoformat()
                        job.next_run_iso = self._compute_next_run_iso(job, base_dt=now_utc)
                except Exception:
                    logger.error("Error while polling jobs:\n" + traceback.format_exc())
            if self._persist_file:
                self._save_persist()
            time.sleep(self._poll_interval)

    def _run_job_wrapper(self, job: CronJob):
        """
        Wrap job execution: handle sync/async functions, retries, timeout, callbacks
        """
        try:
            # run with retries
            attempt = 0
            backoff = 1.0
            while True:
                attempt += 1
                try:
                    result = None
                    # handle coroutine functions
                    if inspect.iscoroutinefunction(job.func):
                        coro = job.func(*job.args, **job.kwargs)
                        fut = asyncio.run_coroutine_threadsafe(coro, self._async_loop)
                        if job.timeout_seconds:
                            result = fut.result(timeout=job.timeout_seconds)
                        else:
                            result = fut.result()
                    else:
                        # run in thread but with timeout using future from executor
                        fut = self._executor.submit(job.func, *job.args, **job.kwargs)
                        if job.timeout_seconds:
                            result = fut.result(timeout=job.timeout_seconds)
                        else:
                            result = fut.result()
                    # success
                    logger.info(f"Job {job.name} succeeded (attempt {attempt})")
                    # call success hook if provided in persistent_meta
                    if callable(job.persistent_meta.get("on_success")):
                        try:
                            job.persistent_meta["on_success"](job, result)
                        except Exception:
                            logger.warning("on_success hook failed:\n" + traceback.format_exc())
                    break
                except Exception as e:
                    logger.error(f"Job {job.name} failed on attempt {attempt}: {e}\n{traceback.format_exc()}")
                    if attempt > job.retries:
                        # final failure
                        if callable(job.persistent_meta.get("on_failure")):
                            try:
                                job.persistent_meta["on_failure"](job, e)
                            except Exception:
                                logger.warning("on_failure hook failed:\n" + traceback.format_exc())
                        break
                    else:
                        # backoff then retry
                        time.sleep(backoff * job.retry_backoff)
                        backoff *= 2
                        continue
        finally:
            # decrement running count
            with job._lock:
                if job._running_count > 0:
                    job._running_count -= 1

# --- Usage examples ---
if __name__ == "__main__":
    # Example usage demonstration
    async def async_task(name):
        await asyncio.sleep(1)
        print(f"[async task] Hello {name} @ {datetime.now().isoformat()}")

    def sync_task(x, y):
        time.sleep(0.5)
        print(f"[sync task] {x+y} @ {datetime.now().isoformat()}")

    def failing_task():
        raise RuntimeError("boom")

    scheduler = AdvancedCronScheduler(timezone_name="America/Santiago", max_workers=6, poll_interval=0.5)

    # add a job every 10 seconds using @every
    jid1 = scheduler.add_job("say-hello-every-10s", "@every 10s", func=sync_task, args=[1,2], retries=1, retry_backoff=1.5)
    # add a cron-like job (every minute at sec 0)
    jid2 = scheduler.add_job("async-hello", "0 */1 * * * *", func=async_task, args=["world"], max_concurrency=1, jitter_seconds=2)
    # add a failing job to demo retries
    jid3 = scheduler.add_job("failing", "@every 15s", func=failing_task, retries=2, timeout_seconds=3, max_concurrency=1)

    # optional persistence file
    # scheduler._persist_file = "cron_jobs.json"

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping scheduler...")
        scheduler.stop()
