"""Configuration loading, cookie-file parsing and CLI argument handling.

Config search order: --config flag -> CHATGPT_WEB2API_CONFIG env -> ./config.json
-> ~/.config/chatgpt-web2api/config.json. Missing files are fine (defaults win).

Environment overrides (env wins over config.json):
  - PROXY_API_KEY / PROXY_API_KEYS  (API key(s))
  - COOKIE_STRING                   (full Cookie header)
  - PORT, HOST                      (network)
  - CHATGPT_PROXY                   (upstream proxy)
  - All *_SEC / *_MS tuning vars    (see _INT_ENV / _FLOAT_ENV / _BOOL_ENV)
"""

import argparse
import json
import os
import re
import threading
from dataclasses import dataclass, field, fields
from typing import List, Optional

ENV_CONFIG_VAR = "CHATGPT_WEB2API_CONFIG"
LOCAL_CONFIG = "config.json"
USER_CONFIG = os.path.join("~", ".config", "chatgpt-web2api", "config.json")

INT_FIELDS = {"port", "retry_attempts", "request_timeout_sec", "models_refresh_sec",
              "state_refresh_sec", "pool_workers", "pool_queue_depth",
              "min_concurrent_requests", "initial_concurrent_requests",
              "max_concurrent_requests", "hedge_max_inflight",
              "agents_express", "agents_standard", "agents_heavy",
              "agent_queue_depth", "lane_express_chars", "lane_heavy_chars"}
FLOAT_FIELDS = {"retry_delay_sec", "queue_wait_sec", "request_deadline_sec",
                "first_byte_timeout_sec", "stream_read_timeout_sec",
                "hedge_after_sec", "max_retry_wait_sec", "sse_keepalive_sec",
                "latency_target_sec", "agent_submit_wait_sec", "max_body_mb"}
BOOL_FIELDS = {"log_requests", "temporary_chats", "adaptive_concurrency",
               "agents_enabled", "strict_models"}


@dataclass
class Config:
    port: int = 8100
    host: str = "127.0.0.1"
    cookie_file: Optional[str] = "gpt_cookie.txt"
    cookie_string: Optional[str] = None
    config_path: Optional[str] = None
    api_keys: List[str] = None
    default_model: str = "auto"
    temporary_chats: bool = True
    models_refresh_sec: int = 600
    state_refresh_sec: int = 300
    request_timeout_sec: int = 300
    retry_attempts: int = 2
    retry_delay_sec: float = 1.0

    # -- concurrency / admission control -----------------------------------
    max_concurrent_requests: int = 32
    min_concurrent_requests: int = 2
    initial_concurrent_requests: int = 32
    adaptive_concurrency: bool = True
    hedge_max_inflight: int = 2
    queue_wait_sec: float = 5.0
    pool_workers: int = 128
    pool_queue_depth: int = 1024

    # -- agent fleet (parallel lane-scheduled upstream executors) ----------
    agents_enabled: bool = True
    agents_express: int = 6
    agents_standard: int = 20
    agents_heavy: int = 6
    agent_queue_depth: int = 256
    agent_submit_wait_sec: float = 5.0
    lane_express_chars: int = 4000
    lane_heavy_chars: int = 60000

    # -- latency guards ----------------------------------------------------
    request_deadline_sec: float = 75.0
    first_byte_timeout_sec: float = 15.0
    stream_read_timeout_sec: float = 30.0
    hedge_after_sec: float = 3.5
    max_retry_wait_sec: float = 3.0
    sse_keepalive_sec: float = 15.0
    latency_target_sec: float = 7.0

    max_body_mb: float = 10.0
    strict_models: bool = False
    log_requests: bool = True

    # cookie / proxy
    proxy: Optional[str] = None

    # per-instance cookie cache (never share mtime across Config objects)
    _cookie_mtime: Optional[float] = field(default=None, repr=False, compare=False)
    _cached_cookie: Optional[str] = field(default=None, repr=False, compare=False)
    _cookie_lock: threading.Lock = field(default_factory=threading.Lock,
                                         repr=False, compare=False)

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
        self.lane_express_chars = max(1, int(self.lane_express_chars or 1))
        self.lane_heavy_chars = max(self.lane_express_chars,
                                    int(self.lane_heavy_chars or self.lane_express_chars))

    # ------------------------------------------------------------------
    # Cookie handling
    # ------------------------------------------------------------------

    def resolve_cookie(self):
        """Returns the cookie header string, or None if not configured."""
        if self.cookie_string:
            return self.cookie_string
        return self.load_cookie()

    def load_cookie(self):
        """Read the cookie jar; beginner-proof against BOM / 'Cookie:' prefix /
        multi-line pastes. Raises FileNotFoundError if no cookie config exists."""
        # env cookie always wins and never touches the file cache
        if self.cookie_string:
            return self.cookie_string
        path = self.cookie_file
        if not path:
            raise FileNotFoundError("cookie_file not configured")
        if not os.path.isabs(path):
            here = os.path.dirname(os.path.abspath(self.config_path or "config.json"))
            for base in (here, os.getcwd()):
                cand = os.path.join(base, path)
                if os.path.exists(cand):
                    path = cand
                    break
            else:
                raise FileNotFoundError(f"cookie file not found: {path}")
        # --- read with mtime caching for hot-reload (thread-safe, per-instance) ---
        return self._read_cookie_cached(path)

    def _read_cookie_cached(self, path):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            # file vanished: keep serving the last good cookie instead of
            # sending "Cookie: None" upstream (which always 401s)
            return self._cached_cookie
        with self._cookie_lock:
            if self._cookie_mtime == mtime and self._cached_cookie is not None:
                return self._cached_cookie
            try:
                with open(path, "r", encoding="utf-8-sig") as fh:
                    raw = fh.read()
            except OSError:
                return self._cached_cookie
            raw = raw.strip()
            if "\n" in raw or "\r" in raw:
                lines = [ln.strip().rstrip(";") for ln in raw.splitlines()
                         if ln.strip() and not ln.strip().startswith("#")]
                if lines and all("=" in ln for ln in lines):
                    raw = "; ".join(lines)
                else:
                    raw = " ".join(raw.split())
            if raw.lower().startswith("cookie:"):
                raw = raw[7:].strip()
            raw = raw.strip()
            if raw:
                self._cached_cookie = raw
                self._cookie_mtime = mtime
            return self._cached_cookie

    @property
    def cookie_path(self):
        """Human-readable path for /health display."""
        if self.cookie_string:
            return "env:COOKIE_STRING"
        if self.cookie_file and os.path.isabs(self.cookie_file):
            return self.cookie_file
        if self.cookie_file:
            here = os.path.dirname(os.path.abspath(self.config_path or "config.json"))
            for base in (here, os.getcwd()):
                cand = os.path.join(base, self.cookie_file)
                if os.path.exists(cand):
                    return cand
        return self.cookie_file or "gpt_cookie.txt"


_CONFIG_FIELDS = {f.name for f in fields(Config)}

ENV_LIST_FIELDS = {"api_keys"}

# env var -> config field maps (string env values are parsed per type)
_INT_ENV = {
    "RETRY_ATTEMPTS": "retry_attempts",
    "STATE_REFRESH_SEC": "state_refresh_sec",
    "MODELS_REFRESH_SEC": "models_refresh_sec",
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
}
_BOOL_ENV = {
    "TEMPORARY_CHATS": "temporary_chats",
    "ADAPTIVE_CONCURRENCY": "adaptive_concurrency",
    "LOG_REQUESTS": "log_requests",
    "AGENTS_ENABLED": "agents_enabled",
    "STRICT_MODELS": "strict_models",
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
    """Applies PROXY_API_KEY / PROXY_API_KEYS / COINY_STRING / tuning env vars (env > config.json)."""
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

    raw_proxy = env.get("CHATGPT_PROXY", "").strip()
    if raw_proxy:
        cfg.proxy = raw_proxy

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
    """Loads config from file + env overrides and returns a fully-initialized Config."""
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
            cfg.config_path = path
    env = dict(os.environ)
    env.update(load_env_file(".env"))
    _apply_env_overrides(cfg, env)
    return cfg


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="chatgptwebapp2api",
        description="OpenAI-compatible API server for the ChatGPT web app (reverse-engineered).",
    )
    parser.add_argument("--port", type=int, help="listen port (default 8100)")
    parser.add_argument("--host", help="bind address (default 127.0.0.1)")
    parser.add_argument("--config", help="path to config.json")
    parser.add_argument("--cookie-file", help="path to cookie file (header string or JSON)")
    parser.add_argument("--proxy", help="upstream proxy URL, e.g. http://127.0.0.1:7890")
    parser.add_argument("--version", action="store_true", help="print version and exit")
    return parser.parse_args(argv)


def apply_args(cfg: Config, args) -> Config:
    if getattr(args, "port", None) is not None:
        cfg.port = args.port
    if getattr(args, "host", None):
        cfg.host = args.host
    if getattr(args, "cookie_file", None):
        cfg.cookie_file = args.cookie_file
    if getattr(args, "proxy", None):
        cfg.proxy = args.proxy
    cfg.__post_init__()
    return cfg
