"""Adaptive admission control for upstream ChatGPT calls.

A fixed concurrency cap plus a long queue wait is what turns a 2-second ChatGPT
answer into a 20-second client-visible latency: once the cap is saturated every
extra request sits in the queue doing nothing. This module replaces the fixed
cap with an AIMD (additive-increase / multiplicative-decrease) gate that:

  * starts at a warm limit and grows while calls succeed and clients wait,
  * shrinks immediately on upstream pressure (429 / stall / 5xx / transport
    error) and cools down before probing again,
  * never blocks the HTTP worker pool for longer than the caller's timeout
    (overload is shed fast instead of piling up),
  * exposes counters so /health can show the current load state.

The public API (``acquire(timeout=...) -> bool`` / ``release()``) matches
``threading.BoundedSemaphore``, so existing call sites keep working while
gaining adaptive behaviour.
"""

import threading
import time


class AdaptiveConcurrency:
    """Thread-safe AIMD gate for upstream calls.

    Parameters
    ----------
    initial / minimum / maximum : int
        Permit range. The current limit always stays inside [minimum, maximum].
    adaptive : bool
        When False the gate behaves like a plain semaphore fixed at ``initial``.
    latency_target_sec : float
        Observability only: used to flag "slow" completions in stats.
    """

    PRESSURE_FACTORS = {
        "rate_limit": 0.5,   # upstream 429 -> halve decisively
        "stall": 0.75,       # no first byte / read stalled
        "timeout": 0.75,
        "error": 0.75,       # 5xx / transport / protocol error
    }
    COOLDOWNS = {"rate_limit": 2.0, "stall": 1.5, "timeout": 1.5, "error": 1.0}

    def __init__(self, initial=8, minimum=2, maximum=32, adaptive=True,
                 latency_target_sec=7.0):
        maximum = max(1, int(maximum))
        minimum = max(1, min(int(minimum), maximum))
        initial = max(minimum, min(int(initial), maximum))
        self._cond = threading.Condition()
        self._limit = initial
        self._minimum = minimum
        self._maximum = maximum
        self._adaptive = bool(adaptive)
        self._in_flight = 0
        self._waiting = 0
        self._successes = 0
        self._cooldown_until = 0.0
        self._last_pressure = ""
        self.latency_target_sec = float(latency_target_sec or 0.0)
        # observability (read without the lock is fine for counters)
        self.acquired = 0
        self.completed = 0
        self.failed = 0
        self.shed = 0
        self.increases = 0
        self.decreases = 0
        self.peak_limit = initial

    # ------------------------------------------------------------------ gate

    def acquire(self, timeout=None, count_shed=True):
        """Take a permit, waiting up to ``timeout`` seconds (None = forever).

        Returns True when a permit was taken. On timeout returns False without
        taking anything; ``count_shed=False`` is used by internal probes
        (hedges) that must not be reported as shed client load.
        """
        if timeout is not None:
            deadline = time.monotonic() + max(0.0, float(timeout))
        else:
            deadline = None
        with self._cond:
            self._waiting += 1
            try:
                while self._in_flight >= self._limit:
                    if deadline is None:
                        self._cond.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        if count_shed:
                            self.shed += 1
                        return False
                    self._cond.wait(remaining)
                self._in_flight += 1
                self.acquired += 1
                return True
            finally:
                self._waiting -= 1

    def release(self, success=True, pressure=None, latency=None, neutral=False):
        """Return a permit and feed the AIMD controller.

        ``success=False`` or a truthy ``pressure`` string
        (``rate_limit``/``stall``/``timeout``/``error``) shrinks the limit;
        a clean success lets it grow when clients are waiting. ``neutral=True``
        returns a permit without voting (used by cancelled hedge attempts,
        which are not upstream pressure and must not shrink the limit).
        """
        with self._cond:
            if self._in_flight > 0:
                self._in_flight -= 1
            if neutral:
                pass
            elif success and not pressure:
                self.completed += 1
                self._on_success()
            else:
                self.failed += 1
                self._on_pressure(pressure or "error")
            self._cond.notify_all()

    # -------------------------------------------------------------- internals

    def _on_pressure(self, kind):
        self._successes = 0
        self._last_pressure = kind
        if not self._adaptive:
            return
        now = time.monotonic()
        if now < self._cooldown_until:
            return  # already backing off for this wave; don't shrink per error
        factor = self.PRESSURE_FACTORS.get(kind, 0.75)
        target = max(self._minimum, int(self._limit * factor))
        if target < self._limit:
            self._limit = target
            self.decreases += 1
        self._cooldown_until = now + self.COOLDOWNS.get(kind, 1.5)

    def _on_success(self):
        if not self._adaptive or self._limit >= self._maximum:
            return
        now = time.monotonic()
        if now < self._cooldown_until:
            return
        self._successes += 1
        if self._waiting > 0:
            # Clients are queued: grow fast enough to drain the backlog before
            # their patience/timeout runs out. Errors shrink just as fast.
            step = max(1, self._limit // 2)
        elif self._successes >= 8:
            step = 1  # idle probing: slow additive growth
        else:
            return
        self._limit = min(self._maximum, self._limit + step)
        self.increases += 1
        self.peak_limit = max(self.peak_limit, self._limit)
        self._successes = 0

    # ------------------------------------------------------------------ stats

    def describe(self):
        with self._cond:
            limit, in_flight, waiting = self._limit, self._in_flight, self._waiting
        return {
            "limit": limit,
            "in_flight": in_flight,
            "waiting": waiting,
            "utilization": round(in_flight / limit, 3) if limit else 0.0,
            "minimum": self._minimum,
            "maximum": self._maximum,
            "adaptive": self._adaptive,
            "acquired": self.acquired,
            "completed": self.completed,
            "failed": self.failed,
            "shed": self.shed,
            "increases": self.increases,
            "decreases": self.decreases,
            "peak_limit": self.peak_limit,
            "last_pressure": self._last_pressure,
        }

    @property
    def limit(self):
        with self._cond:
            return self._limit

    @property
    def in_flight(self):
        with self._cond:
            return self._in_flight

    @property
    def waiting(self):
        with self._cond:
            return self._waiting

    def __len__(self):  # convenience only
        return self.in_flight
