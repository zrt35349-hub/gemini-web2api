"""Lane-scheduled agent fleet: the parallel executors behind every POST route.

The HTTP worker pool accepts connections fast; this fleet executes the
upstream (ChatGPT) part of each request on named, supervised agent threads.
Agents are grouped into three lanes:

  express   short, image-free, tool-free requests — the common chat case
  standard  everything in between
  heavy     large payloads

Each lane owns reserved agents, so a wave of heavy/large requests can
never starve small chats. When a lane has no work its agents help the other
lanes (own lane always first; express agents never leave the fast path), so
reserved capacity never sits idle. A supervisor thread revives crashed agents
and fails their in-flight job with AgentError instead of leaving a waiting
client hanging forever.

Public API::

    pool = AgentPool(express=6, standard=20, heavy=6)
    pool.run("express", fn)      # execute fn on an agent, re-raise its error
    pool.submit("heavy", fn)     # -> Job (job.wait() / job.result_or_raise())
    pool.describe()              # live lane/agent counters for /health
    pool.stop()
"""

import threading
import time
from collections import deque

EXPRESS = "express"
STANDARD = "standard"
HEAVY = "heavy"
LANES = (EXPRESS, STANDARD, HEAVY)

# Idle agents help other lanes in this order. Express agents only ever help
# the standard lane — they never pick up heavy work, so the capacity reserved
# for small chat requests is never consumed by large-payload backlogs.
HELP_ORDER = {
    EXPRESS: (STANDARD,),
    STANDARD: (EXPRESS, HEAVY),
    HEAVY: (EXPRESS, STANDARD),
}

SUPERVISE_INTERVAL_SEC = 1.0   # supervisor wakeup: revive dead agents quickly
TAKE_POLL_SEC = 0.5            # idle agent wakeup
MAX_REVIVE_DELAY_SEC = 5.0     # restart backoff ceiling for a crash-looping agent
MEDIA_SCAN_LIMIT = 50000       # nodes scanned before giving up (huge bodies are heavy anyway)

# keys that mean "this payload carries an image" (OpenAI shape)
_MEDIA_KEYS = ("image_url", "inline_data", "file_data")
_MEDIA_TYPES = ("image", "input_image")


class AgentError(RuntimeError):
    """The fleet itself failed (agent died mid-job, pool stopped, disabled)."""


class PoolBusy(RuntimeError):
    """Lane queues are saturated; the caller should shed load (429)."""


class Job:
    """One unit of work handed to an agent lane."""

    __slots__ = ("lane", "fn", "name", "created_at", "started_at", "finished_at",
                 "result", "error", "_done")

    def __init__(self, lane, fn, name=""):
        self.lane = lane
        self.fn = fn
        self.name = name or "job"
        self.created_at = time.monotonic()
        self.started_at = None
        self.finished_at = None
        self.result = None
        self.error = None
        self._done = threading.Event()

    @property
    def done(self):
        return self._done.is_set()

    @property
    def elapsed_ms(self):
        end = self.finished_at if self.finished_at is not None else time.monotonic()
        return (end - (self.started_at or self.created_at)) * 1000.0

    def _start(self):
        self.started_at = time.monotonic()

    def _finish(self, result):
        self.result = result
        self.finished_at = time.monotonic()
        self._done.set()

    def _fail(self, error):
        self.error = error
        self.finished_at = time.monotonic()
        self._done.set()

    def wait(self, timeout=None):
        """Blocks until the job finishes; True when it is done."""
        return self._done.wait(timeout)

    def result_or_raise(self, timeout=None):
        """Waits and returns the result, re-raising the job's error if any."""
        if not self._done.wait(timeout):
            raise AgentError(f"{self.name}: job did not finish in time")
        if self.error is not None:
            raise self.error
        return self.result


class Agent:
    """A named worker thread that pulls jobs for its lane (plus help lanes)."""

    def __init__(self, pool, lane, index):
        self.pool = pool
        self.lane = lane
        self.name = f"chatgpt-{lane}-agent-{index}"
        self.thread = None
        self.current_job = None   # job being executed right now
        self.orphan_job = None    # job left behind by an agent-level crash
        self._job_started_at = None  # monotonic ts of the current job (stuck watchdog)
        self.revives = 0
        self.revive_at = 0.0
        self.completed = 0
        self.failed = 0
        self.total_ms = 0.0
        self.last_error = ""

    @property
    def alive(self):
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        # start before publishing: a concurrent stop() must never join a
        # thread object that has not been started yet (RuntimeError race)
        thread = threading.Thread(target=self._loop, name=self.name, daemon=True)
        thread.start()
        self.thread = thread

    def _loop(self):
        pool = self.pool
        while pool._running:
            job = pool._take(self)
            if job is None:
                continue
            self.current_job = job
            self._job_started_at = time.monotonic()
            try:
                self._execute(job)
            except Exception:  # noqa: BLE001 - agent-level crash
                # A crash here means a bug in the fleet itself, not a request
                # failure: park the job for the supervisor, end the thread and
                # let the supervisor fail the job + revive this agent.
                # (BaseException like KeyboardInterrupt must propagate, never
                # be swallowed into a job failure.)
                try:
                    if not job.done:
                        self.orphan_job = job
                    else:
                        self.orphan_job = None
                except Exception:
                    self.orphan_job = None
                self.last_error = f"agent {self.name} crashed"
                return
            finally:
                self.current_job = None
                self._job_started_at = None

    def _execute(self, job):
        """Runs one job; request errors are captured in the job, never raised."""
        job._start()
        started = job.started_at
        try:
            result = job.fn()
        except Exception as exc:  # noqa: BLE001 - re-raised in the waiter
            job._fail(exc)
            self.failed += 1
        else:
            job._finish(result)
            self.completed += 1
        self.total_ms += (time.monotonic() - started) * 1000.0


class AgentPool:
    """Fixed fleet of lane-bound agent threads with bounded queues."""

    def __init__(self, express=6, standard=20, heavy=6, queue_depth=256,
                 submit_wait_sec=5.0):
        counts = {EXPRESS: max(0, int(express or 0)),
                  STANDARD: max(0, int(standard or 0)),
                  HEAVY: max(0, int(heavy or 0))}
        self.enabled = sum(counts.values()) > 0
        self.queue_depth = max(1, int(queue_depth or 1))
        self.submit_wait_sec = max(0.0, float(submit_wait_sec or 0.0))
        self.agents = []
        for lane in LANES:
            for index in range(counts[lane]):
                self.agents.append(Agent(self, lane, index + 1))
        self.lane_counts = counts
        self._queues = {lane: deque() for lane in LANES}
        self._cond = threading.Condition()
        self._life_lock = threading.Lock()
        self._running = False
        self._supervisor = None
        self.restarts = 0
        self.shed = 0
        self.last_error = ""

    # ------------------------------------------------------------- lifecycle

    def start(self):
        """Spins the fleet up (idempotent; called lazily on first submit)."""
        with self._life_lock:
            if self._running or not self.enabled:
                return self._running
            self._running = True
            for agent in self.agents:
                agent.start()
            self._supervisor = threading.Thread(target=self._supervise,
                                                name="chatgpt-agent-supervisor",
                                                daemon=True)
            self._supervisor.start()
            return True

    def stop(self, timeout=2.0):
        """Stops the fleet; queued jobs fail fast instead of hanging waiters."""
        with self._life_lock:
            if not self._running:
                return
            self._running = False
            with self._cond:
                self._fail_queued_locked(AgentError("agent pool stopped"))
                self._cond.notify_all()
            threads = [agent.thread for agent in self.agents if agent.thread is not None]
            if self._supervisor is not None:
                threads.append(self._supervisor)
        # join outside _life_lock with a shared budget (32 agents x 2s = 64s
        # sequential was a shutdown hang; now the whole fleet gets ~timeout)
        deadline = time.monotonic() + max(0.5, float(timeout or 2.0))
        for thread in threads:
            if thread is threading.current_thread():
                continue
            remaining = max(0.05, deadline - time.monotonic())
            try:
                thread.join(timeout=remaining)
            except RuntimeError:  # revived in the publish window; daemon anyway
                pass
        with self._life_lock:
            self._supervisor = None

    @property
    def running(self):
        return self._running

    # -------------------------------------------------------------- dispatch

    def submit(self, lane, fn, name="", timeout=None):
        """Queues ``fn`` on ``lane``; raises PoolBusy when the queue is full.

        ``timeout`` bounds the wait for a free queue slot (None waits forever).
        The queue is a depth-limited deque: overload is shed in milliseconds
        with an honest PoolBusy instead of parking requests behind a backlog.
        """
        if not self.enabled:
            raise AgentError("agent pool is disabled")
        self.start()
        if lane not in self._queues:
            lane = STANDARD
        job = Job(lane, fn, name=name)
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        with self._cond:
            queue = self._queues[lane]
            while len(queue) >= self.queue_depth:
                if not self._running:
                    raise AgentError("agent pool is stopping")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    self.shed += 1
                    raise PoolBusy(
                        f"{lane} lane queue is full ({self.queue_depth} waiting)")
                self._cond.wait(remaining)
            queue.append(job)
            self._cond.notify_all()
        return job

    def run(self, lane, fn, name="", timeout=None, exec_timeout=None):
        """Submits ``fn`` and waits for it, re-raising whatever it raised.

        ``timeout`` bounds the queue-slot wait; ``exec_timeout`` bounds the
        execution wait (None = wait for the job; prefer a request-deadline
        here so a stalled upstream never pins the HTTP worker forever).
        """
        job = self.submit(lane, fn, name=name, timeout=timeout)
        return job.result_or_raise(timeout=exec_timeout)

    def _take(self, agent):
        """Blocks until a job is available for this agent; None when stopping."""
        with self._cond:
            while self._running:
                job = self._pop_locked(agent)
                if job is not None:
                    return job
                self._cond.wait(TAKE_POLL_SEC)
        return None

    def _pop_locked(self, agent):
        own = self._queues[agent.lane]
        if own:
            return own.popleft()
        for lane in HELP_ORDER[agent.lane]:
            queue = self._queues[lane]
            if queue:
                return queue.popleft()
        return None

    def _fail_queued_locked(self, reason):
        for queue in self._queues.values():
            while queue:
                queue.popleft()._fail(reason)

    # ------------------------------------------------------------ supervision

    def _supervise(self):
        """Revives crashed agents and fails the job they died on."""
        while self._running:
            time.sleep(SUPERVISE_INTERVAL_SEC)
            if not self._running:
                return
            now = time.monotonic()
            for agent in self.agents:
                if agent.alive or now < agent.revive_at:
                    continue
                self._revive(agent)

    def _revive(self, agent):
        job = agent.orphan_job
        agent.orphan_job = None
        if job is not None and not job.done:
            job._fail(AgentError(f"agent {agent.name} died mid-job"))
        agent.revives += 1
        self.restarts += 1
        agent.revive_at = time.monotonic() + min(
            MAX_REVIVE_DELAY_SEC, 0.25 * agent.revives)
        self.last_error = f"{agent.name} revived (crash #{agent.revives})"
        try:
            agent.start()
        except Exception as exc:  # noqa: BLE001 - keep supervising the rest
            self.last_error = f"could not restart {agent.name}: {exc}"

    # ------------------------------------------------------------------ stats

    def describe(self):
        """Live fleet state for /health: per-lane agents, queues and timings."""
        lanes = {}
        # snapshot counters under the queue lock so /health never reports
        # torn busy/completed numbers while workers mutate them
        with self._cond:
            queued_snap = {lane: len(self._queues[lane]) for lane in LANES}
        for lane in LANES:
            members = [a for a in self.agents if a.lane == lane]
            # current_job is set/cleared by the agent thread; read it
            # defensively (a torn read only skews one /health poll)
            try:
                busy = sum(1 for a in members if a.current_job is not None)
                completed = sum(a.completed for a in members)
                failed = sum(a.failed for a in members)
                total_ms = sum(a.total_ms for a in members)
            except Exception:
                busy, completed, failed, total_ms = 0, 0, 0, 0.0
            lanes[lane] = {
                "agents": len(members),
                "busy": busy,
                "queued": queued_snap.get(lane, 0),
                "queue_depth": self.queue_depth,
                "completed": completed,
                "failed": failed,
                "avg_job_ms": round(total_ms / completed, 1) if completed else 0.0,
            }
        completed = sum(lane["completed"] for lane in lanes.values())
        failed = sum(lane["failed"] for lane in lanes.values())
        return {
            "enabled": self.enabled,
            "running": self._running,
            "agents": len(self.agents),
            "busy": sum(lane["busy"] for lane in lanes.values()),
            "queued": sum(lane["queued"] for lane in lanes.values()),
            "completed": completed,
            "failed": failed,
            "shed": self.shed,
            "restarts": self.restarts,
            "submit_wait_sec": self.submit_wait_sec,
            "lanes": lanes,
            "last_error": self.last_error or None,
        }


# ---------------------------------------------------------------------------
# Request classification
# ---------------------------------------------------------------------------

def payload_has_media(node, limit=MEDIA_SCAN_LIMIT):
    """True when a request body carries an image part (OpenAI shape)."""
    stack = [node]
    seen = 0
    while stack:
        item = stack.pop()
        seen += 1
        if seen > limit:
            return False  # absurdly large body: size-based routing handles it
        if isinstance(item, dict):
            if item.get("type") in _MEDIA_TYPES:
                return True
            for key, value in item.items():
                if key in _MEDIA_KEYS and value:
                    return True
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(item, list):
            for value in item:
                if isinstance(value, (dict, list)):
                    stack.append(value)
    return False


def classify(body_len, data, express_chars=4000, heavy_chars=60000):
    """Maps one parsed request body to an agent lane.

    express  : small, image-free, tool-free bodies -> reserved fast agents
    heavy    : images, tool calls or large payloads -> dedicated heavy agents
    standard : everything else
    Never raises; the worst case is the standard lane.
    """
    try:
        express_chars = max(1, int(express_chars or 1))
        heavy_chars = max(express_chars, int(heavy_chars or 1))
    except (TypeError, ValueError):
        express_chars, heavy_chars = 4000, 60000
    if body_len and body_len >= heavy_chars:
        return HEAVY
    if isinstance(data, dict):
        try:
            if data.get("tools"):
                return HEAVY
            if payload_has_media(data):
                return HEAVY
        except Exception:  # noqa: BLE001 - classification must never fail a request
            return STANDARD
    if body_len and body_len <= express_chars:
        return EXPRESS
    return STANDARD


def build_pool(config):
    """Builds the fleet described by ``config``; None when disabled/empty."""
    if not bool(getattr(config, "agents_enabled", True)):
        return None
    pool = AgentPool(
        express=int(getattr(config, "agents_express", 6) or 0),
        standard=int(getattr(config, "agents_standard", 20) or 0),
        heavy=int(getattr(config, "agents_heavy", 6) or 0),
        queue_depth=int(getattr(config, "agent_queue_depth", 256) or 256),
        submit_wait_sec=float(getattr(config, "agent_submit_wait_sec", 5.0) or 0.0),
    )
    return pool if pool.enabled else None
