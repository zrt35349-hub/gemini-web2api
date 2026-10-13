"""Configuration loading, cookie-file parsing and CLI argument handling.

Config search order: --config flag -> GEMINI_WEB2API_CONFIG env -> ./config.json
-> ~/.config/gemini-web2api/config.json. Missing files are fine (defaults win).
"""

import argparse
import json
import os
import re
from dataclasses import dataclass, fields
from typing import List, Optional

ENV_CONFIG_VAR = "GEMINI_WEB2API_CONFIG"
LOCAL_CONFIG = "config.json"
USER_CONFIG = os.path.join("~", ".config", "gemini-web2api", "config.json")

INT_FIELDS = {"port", "retry_attempts", "request_timeout_sec", "auth_user",
              "state_refresh_sec", "pool_workers", "pool_queue_depth",
              "min_concurrent_requests", "initial_concurrent_requests",
              "max_concurrent_requests", "hedge_max_inflight",
              "agents_express", "agents_standard", "agents_heavy",
              "agent_queue_depth", "lane_express_chars", "lane_heavy_chars"}
FLOAT_FIELDS = {"retry_delay_sec", "queue_wait_sec", "request_deadline_sec",
                "first_byte_timeout_sec", "stream_read_timeout_sec",
                "hedge_after_sec", "max_retry_wait_sec", "sse_keepalive_sec",
                "latency_target_sec", "agent_submit_wait_sec",
                "max_stream_sec", "handler_timeout_sec"}
BOOL_FIELDS = {"log_requests", "temporary_chats", "adaptive_concurrency",
               "agents_enabled"}


@dataclass
class Config:
    port: int = 8000
    host: str = "0.0.0.0"
    retry_attempts: int = 2
    retry_delay_sec: float = 1.0
    request_timeout_sec: int = 180
    gemini_bl: Optional[str] = None          # auto-refreshed at runtime
    auth_user: Optional[int] = None          # /u/N/ account index
    xsrf_token: Optional[str] = None         # SNlM0e
    default_model: str = "gemini-3.6-flash"
    api_keys: List[str] = None               # empty list = no auth
    cookie_file: Optional[str] = None        # cookie.txt path
    cookie_string: Optional[str] = None      # full Cookie header (env COOKIE_STRING)
    log_requests: bool = True
    temporary_chats: bool = False            # true = history not saved on the account
    state_refresh_sec: int = 300             # background bl/xsrf refresh cadence (0 = off)

    # -- concurrency / admission control -----------------------------------
    max_concurrent_requests: int = 32        # hard cap on in-flight upstream calls
    min_concurrent_requests: int = 2         # AIMD floor
    initial_concurrent_requests: int = 32    # start at the permitted cap; AIMD shrinks on pressure
    adaptive_concurrency: bool = True        # False = fixed initial limit
    hedge_max_inflight: int = 2              # global cap on tail-latency hedge calls
    queue_wait_sec: float = 5.0              # max wait for a slot before 429 (0 = forever)
    pool_workers: int = 160                  # HTTP worker threads serving connections
    pool_queue_depth: int = 1024             # accepted-but-not-yet-served connections

    # -- agent fleet (parallel lane-scheduled upstream executors) ----------
    agents_enabled: bool = True              # False = run requests inline (legacy)
    agents_express: int = 8                  # agents reserved for small/fast requests
    agents_standard: int = 24                # agents for normal traffic
    agents_heavy: int = 8                    # agents for images / tools / big payloads
    agent_queue_depth: int = 256             # max queued jobs per lane before 429
    agent_submit_wait_sec: float = 5.0       # max wait for a lane slot (0 = forever)
    lane_express_chars: int = 4000           # body bytes <= this -> express lane
    lane_heavy_chars: int = 60000            # body bytes >= this -> heavy lane

    # -- latency guards -----------------------------------------------------
    request_deadline_sec: float = 75.0       # whole-call budget incl. retries (0 = off)
    first_byte_timeout_sec: float = 20.0     # abort a stream with no upstream byte (0 = off)
    stream_read_timeout_sec: float = 60.0    # max silence between streamed events
    hedge_after_sec: float = 3.5             # race a 2nd attempt if silent this long (0 = off)
    max_retry_wait_sec: float = 3.0          # never sleep longer than this inside a slot
    sse_keepalive_sec: float = 15.0          # SSE keep-alive comment cadence (0 = off)
    latency_target_sec: float = 7.0          # health/reporting target (not a hard cap)
    # long sessions: setup stays bounded by request_deadline_sec; once deltas
    # flow the stream survives on progress up to max_stream_sec (website builds)
    max_stream_sec: float = 600.0
    handler_timeout_sec: float = 600.0

    max_body_mb: float = 10.0                # request body size limit
    strict_models: bool = True               # unknown model name -> 400 (else fallback)

    def __post_init__(self):
        if self.api_keys is None:
            self.api_keys = []
        # keep the admission-control range sane whatever the config file says
        self.max_concurrent_requests = max(1, int(self.max_concurrent_requests or 1))
        self.min_concurrent_requests = max(
            1, min(int(self.min_concurrent_requests or 1), self.max_concurrent_requests))
        self.initial_concurrent_requests = max(
            self.min_concurrent_requests,
            min(int(self.initial_concurrent_requests or self.min_concurrent_requests),
                self.max_concurrent_requests))
        self.pool_workers = max(8, int(self.pool_workers or 8))
        self.pool_queue_depth = max(1, int(self.pool_queue_depth or 1))
        # agent fleet: counts are clamped so a bad config can never crash startups
        self.agents_express = max(0, int(self.agents_express or 0))
        self.agents_standard = max(0, int(self.agents_standard or 0))
        self.agents_heavy = max(0, int(self.agents_heavy or 0))
        self.agent_queue_depth = max(1, int(self.agent_queue_depth or 1))
        self.agent_submit_wait_sec = max(0.0, float(self.agent_submit_wait_sec or 0.0))
        try:
            self.max_stream_sec = max(60.0, float(self.max_stream_sec or 600.0))
        except (TypeError, ValueError):
            self.max_stream_sec = 600.0
        try:
            self.handler_timeout_sec = max(60.0, float(self.handler_timeout_sec or 600.0))
        except (TypeError, ValueError):
            self.handler_timeout_sec = 600.0
        self.lane_express_chars = max(1, int(self.lane_express_chars or 1))
        self.lane_heavy_chars = max(self.lane_express_chars,
                                    int(self.lane_heavy_chars or self.lane_express_chars))


_CONFIG_FIELDS = {f.name for f in fields(Config)}

ENV_LIST_FIELDS = {"api_keys"}

# env var -> config field maps (string env values are parsed per type)
_INT_ENV = {
    "RETRY_ATTEMPTS": "retry_attempts",
    "STATE_REFRESH_SEC": "state_refresh_sec",
    "REQUEST_TIMEOUT_SEC": "request_timeout_sec",
    "MAX_CONCURRENT_REQUESTS": "max_concurrent_requests",
    "MIN_CONCURRENT_REQUESTS": "min_concurrent_requests",
    "INITIAL_CONCURRENT_REQUESTS": "initial_concurrent_requests",
    "HEDGE_MAX_INFLIGHT": "hedge_max_inflight",
    "POOL_WORKERS": "pool_workers",
    "POOL_QUEUE_DEPTH": "pool_queue_depth",
    "AGENTS_EXPRESS": "agents_express",
    "AGENTS_STANDARD": "agents_standard",
    "AGENTS_HEAVY": "agents_heavy",
    "AGENT_QUEUE_DEPTH": "agent_queue_depth",
    "LANE_EXPRESS_CHARS": "lane_express_chars",
    "LANE_HEAVY_CHARS": "lane_heavy_chars",
    "PORT": "port",
}
_FLOAT_ENV = {
    "RETRY_DELAY_SEC": "retry_delay_sec",
    "MAX_BODY_MB": "max_body_mb",
    "QUEUE_WAIT_SEC": "queue_wait_sec",
    "REQUEST_DEADLINE_SEC": "request_deadline_sec",
    "FIRST_BYTE_TIMEOUT_SEC": "first_byte_timeout_sec",
    "STREAM_READ_TIMEOUT_SEC": "stream_read_timeout_sec",
    "HEDGE_AFTER_SEC": "hedge_after_sec",
    "MAX_RETRY_WAIT_SEC": "max_retry_wait_sec",
    "SSE_KEEPALIVE_SEC": "sse_keepalive_sec",
    "LATENCY_TARGET_SEC": "latency_target_sec",
    "AGENT_SUBMIT_WAIT_SEC": "agent_submit_wait_sec",
    "MAX_STREAM_SEC": "max_stream_sec",
    "HANDLER_TIMEOUT_SEC": "handler_timeout_sec",
}
_BOOL_ENV = {
    "TEMPORARY_CHATS": "temporary_chats",
    "ADAPTIVE_CONCURRENCY": "adaptive_concurrency",
    "LOG_REQUESTS": "log_requests",
    "AGENTS_ENABLED": "agents_enabled",
}
_FALSEY = {"0", "false", "no", "off", "none", ""}


def load_env_file(path=".env"):
    """Parses a .env file (KEY=VALUE lines, # comments, optional quotes). -> dict."""
    env = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    env[key] = value
    except OSError:
        pass
    return env


def _apply_env_overrides(cfg: Config, env: dict) -> None:
    """Applies PROXY_API_KEY / PROXY_API_KEYS / tuning env vars (env > config.json)."""
    keys = list(cfg.api_keys)
    raw_single = env.get("PROXY_API_KEY", "").strip()
    if raw_single and raw_single not in keys:
        keys.append(raw_single)
    raw_multi = env.get("PROXY_API_KEYS", "").strip()
    if raw_multi:
        for k in raw_multi.split(","):
            k = k.strip()
            if k and k not in keys:
                keys.append(k)
    cfg.api_keys = keys

    raw_cookie = env.get("COOKIE_STRING", "").strip()
    if raw_cookie:
        cfg.cookie_string = raw_cookie
        if not cfg.cookie_file:
            cfg.cookie_file = None  # env cookie wins when no file configured

    for env_name, field in _INT_ENV.items():
        raw = str(env.get(env_name, "")).strip()
        if raw.isdigit():
            setattr(cfg, field, int(raw))
    for env_name, field in _FLOAT_ENV.items():
        raw = str(env.get(env_name, "")).strip()
        if raw:
            try:
                setattr(cfg, field, float(raw))
            except ValueError:
                pass
    for env_name, field in _BOOL_ENV.items():
        raw = str(env.get(env_name, "")).strip().lower()
        if raw:
            setattr(cfg, field, raw not in _FALSEY)
    # re-normalize the AIMD range after env overrides
    cfg.__post_init__()


def _apply(cfg: Config, data: dict) -> None:
    for key, value in (data or {}).items():
        if key not in _CONFIG_FIELDS or value is None:
            continue
        if key == "api_keys":
            cfg.api_keys = [str(k) for k in value] if isinstance(value, list) else [str(value)]
        elif key in INT_FIELDS:
            try:
                setattr(cfg, key, int(value))
            except (TypeError, ValueError):
                pass
        elif key in FLOAT_FIELDS:
            try:
                setattr(cfg, key, float(value))
            except (TypeError, ValueError):
                pass
        elif key in BOOL_FIELDS:
            # JSON booleans pass through; the strings "false"/"0"/"no"/"off"
            # must disable (bool("false") is True — a silent config trap)
            if isinstance(value, bool):
                setattr(cfg, key, value)
            else:
                setattr(cfg, key, str(value).strip().lower() not in _FALSEY)
        else:
            setattr(cfg, key, str(value))
    cfg.__post_init__()


def find_config(explicit: Optional[str] = None) -> Optional[str]:
    if explicit:
        return explicit
    env = os.environ.get(ENV_CONFIG_VAR)
    if env:
        return env
    for cand in (LOCAL_CONFIG, os.path.expanduser(USER_CONFIG)):
        if os.path.isfile(cand):
            return cand
    return None


def load_config(explicit: Optional[str] = None) -> Config:
    cfg = Config()
    path = find_config(explicit)
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8-sig") as fh:
                _apply(cfg, json.load(fh))
        except (ValueError, OSError) as exc:
            print("[WARN] config file %s unreadable (%s) — defaults + env win" % (path, exc),
                  flush=True)
            cfg.config_path = None
        else:
            try:
                cfg.config_path = path
            except Exception:
                pass
    env = dict(os.environ)
    env.update(load_env_file(".env"))
    _apply_env_overrides(cfg, env)
    return cfg


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="gemini-web2api",
        description="OpenAI-compatible API server for the Gemini web app (reverse-engineered).",
    )
    parser.add_argument("--port", type=int, help="listen port (default 8000)")
    parser.add_argument("--host", help="bind address (default 0.0.0.0)")
    parser.add_argument("--config", help="path to config.json")
    parser.add_argument("--cookie-file", help="path to cookie file (header string or JSON)")
    return parser.parse_args(argv)


def apply_args(cfg: Config, args) -> Config:
    if getattr(args, "port", None) is not None:
        cfg.port = args.port
    if getattr(args, "host", None):
        cfg.host = args.host
    if getattr(args, "cookie_file", None):
        cfg.cookie_file = args.cookie_file
    cfg.__post_init__()
    return cfg


class StaticCookieStore:
    """Cookie source for a fixed Cookie header string (e.g. env COOKIE_STRING)."""

    def __init__(self, cookie: str):
        self.cookie = cookie
        m = re.search(r"SAPISID=([^;\s]+)", cookie)
        self.sapisid = m.group(1).strip() if m else ""

    def get(self):
        return self.cookie, self.sapisid


class CookieStore:
    """Reads a cookie file (plain Cookie-header string or JSON {cookie, sapisid}).

    The file is re-read whenever its mtime changes, so swapping cookies does not
    require a server restart. Reads are atomic (truncated mid-write files keep
    serving the last good jar) and guarded by a lock.
    """

    SAPISID_RE = re.compile(r"SAPISID=([^;\s]+)")

    def __init__(self, path: str):
        import threading as _th
        self.path = path
        self._lock = _th.Lock()
        self._mtime = None
        self._cookie = ""
        self._sapisid = ""

    def get(self):
        """Returns (cookie_header, sapisid); either may be empty."""
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            with self._lock:
                return self._cookie, self._sapisid
        with self._lock:
            if self._mtime is not None and mtime == self._mtime:
                return self._cookie, self._sapisid
            cookie, sapisid = self._cookie, self._sapisid
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    raw = fh.read().strip()
            except OSError:
                return cookie, sapisid
            if not raw:
                return cookie, sapisid
            parsed_cookie, parsed_sapisid = raw, ""
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    parsed_cookie = str(data.get("cookie", "")).strip()
                    parsed_sapisid = str(data.get("sapisid", "")).strip()
            except ValueError:
                pass
            if parsed_cookie and not parsed_sapisid:
                m = self.SAPISID_RE.search(parsed_cookie)
                if m:
                    parsed_sapisid = m.group(1).strip()
            # truncated mid-write (no SAPISID, tiny file): keep last good jar
            if parsed_cookie and len(parsed_cookie) >= 16:
                self._cookie, self._sapisid = parsed_cookie, parsed_sapisid
                self._mtime = mtime
            return self._cookie, self._sapisid

    def _load(self):
        with open(self.path, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
        cookie, sapisid = raw, ""
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                cookie = str(data.get("cookie", "")).strip()
                sapisid = str(data.get("sapisid", "")).strip()
        except ValueError:
            pass
        if cookie and not sapisid:
            m = self.SAPISID_RE.search(cookie)
            if m:
                sapisid = m.group(1).strip()
        self._cookie, self._sapisid = cookie, sapisid
