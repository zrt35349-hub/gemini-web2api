"""ChatGPT sentinel gate: chat-requirements + proof-of-work client.

Replicates what the chatgpt.com web app does before every message:
  1. Build a browser-fingerprint config array (18 fields) and solve a small
     pre-challenge ("p" token, fixed difficulty 0fffff, prefix gAAAAAC).
  2. POST /sentinel/chat-requirements with {"p": <token>} -> returns a chat
     token, optional proof-of-work (seed + difficulty) and optional turnstile.
  3. Proof-of-work: find config with i/i>>1 counters such that
     sha3_512(seed + base64(config_json))[:len(diff_bytes)] <= diff_bytes,
     token = "gAAAAAB" + base64(config_json).
  4. Conversation request carries openai-sentinel-chat-requirements-token and
     openai-sentinel-proof-token headers (turnstile header only when we have
     a token — headless we cannot solve turnstile, so we proceed without it).

Algorithm follows the observable browser behaviour as implemented by the
open-source chat2api project.
"""

import base64
import hashlib
import json
import random
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")
SEC_CH_UA = '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"'

_HOME = "https://chatgpt.com"
_FALLBACK_SCRIPT = "https://chatgpt.com/backend-api/sentinel/sdk.js"

# Browser-observed navigator property snapshots (subset; server cannot verify
# exact values, only shape/plausibility).
_NAV_KEYS = [
    "registerProtocolHandler−function registerProtocolHandler() { [native code] }",
    "storage−[object StorageManager]",
    "appCodeName−Mozilla",
    "webdriver−false",
    "vendor−Google Inc.",
    "cookieEnabled−true",
    "product−Gecko",
    "productSub−20030107",
    "appName−Netscape",
    "onLine−true",
    "pdfViewerEnabled−true",
    "language−en-US",
    "hardwareConcurrency−32",
    "doNotTrack",
]
_DOC_KEYS = ["location"]
_WIN_KEYS = [
    "window", "self", "document", "name", "location", "customElements",
    "history", "navigation", "locationbar", "menubar", "personalbar",
    "scrollbars", "statusbar", "toolbar", "status", "closed", "frames",
    "length", "top", "opener", "parent", "navigator", "origin", "external",
    "screen", "innerWidth", "innerHeight", "scrollX", "pageXOffset",
    "scrollY", "pageYOffset", "visualViewport", "screenX", "screenY",
    "outerWidth", "outerHeight", "devicePixelRatio", "clientInformation",
    "screenLeft", "screenTop", "styleMedia", "isSecureContext", "trustedTypes",
    "performance", "crypto", "indexedDB", "sessionStorage", "localStorage",
    "crossOriginIsolated", "scheduler", "alert", "atob", "btoa", "fetch",
    "chrome", "caches", "cookieStore", "speechSynthesis", "webpackChunk_N_E",
    "__NEXT_DATA__", "_N_E", "next",
]


def _parse_time():
    now = datetime.now(timezone(timedelta(hours=-5)))
    return now.strftime("%a %b %d %Y %H:%M:%S") + " GMT-0500 (Eastern Standard Time)"


def generate_answer(seed, diff, config):
    """SHA3-512 prefix search; returns (base64_config, solved_bool)."""
    diff_len = len(diff)
    seed_bytes = seed.encode()
    try:
        target = bytes.fromhex(diff)
    except ValueError:
        target = b"\xff" * max(1, diff_len // 2)
    for i in range(500000):
        config[3] = i
        config[9] = i >> 1
        blob = json.dumps(config, separators=(",", ":"), ensure_ascii=False).encode()
        b64 = base64.b64encode(blob)
        if hashlib.sha3_512(seed_bytes + b64).digest()[:diff_len] <= target:
            return b64.decode(), True
    return ("wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"
            + base64.b64encode(f'"{seed}"'.encode()).decode()), False


def requirements_token(config):
    """Pre-challenge token sent in the chat-requirements body."""
    answer, _ = generate_answer(format(random.random()), "0fffff", config)
    return "gAAAAAC" + answer


def answer_token(seed, diff, config):
    """Proof-of-work token for the conversation request."""
    answer, solved = generate_answer(seed, diff, config)
    return "gAAAAAB" + answer, solved


class Sentinel:
    """Per-client sentinel state: build label cache + fingerprint config.

    The build label (``data-build`` / ``dpl=`` / ``_next/static``) is refreshed
    every 900s and shared across all request threads on this client instance.
    The fingerprint config is derived from the build label and cached for 3600s.
    """

    def __init__(self, user_agent=CHROME_UA):
        self.user_agent = user_agent
        self._scripts = [_FALLBACK_SCRIPT]
        self._dpl = ""
        self._dpl_ts = 0.0
        self._config = None
        self._config_ts = 0.0
        self._lock = threading.RLock()  # reentrant: config() holds it across refresh_build()

    def refresh_build(self, session):
        with self._lock:
            if time.time() - self._dpl_ts < 900:
                return
            try:
                r = session.get(_HOME + "/", headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": self.user_agent,
                })
                html = r.text or ""
            except Exception:
                html = ""
            try:
                r = session.get(_HOME + "/", headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": self.user_agent,
                })
                html = r.text or ""
            except Exception:
                html = ""
            scripts = re.findall(r'<script[^>]+src="([^"]+)"', html)
            if scripts:
                self._scripts = scripts
            dpl = ""
            m = re.search(r'data-build="([^"]+)"', html)
            if m:
                dpl = m.group(1)
            if not dpl:
                m = re.search(r'dpl=([0-9a-f]{16,})', html)
                if m:
                    dpl = "prod-" + m.group(1)
            if not dpl:
                m = re.search(r'/_next/static/([^/]+)/_', html)
                if m:
                    dpl = m.group(1)
            self._dpl = dpl
            self._dpl_ts = time.time()

    def config(self, session, max_age=3600):
        with self._lock:
            now = time.time()
            if self._config is None or now - self._config_ts > max_age:
                self.refresh_build(session)
                perf = time.perf_counter() * 1000
                self._config = [
                    random.choice([3000, 4000, 3120, 4160]),
                    _parse_time(),
                    4294705152,
                    0,
                    self.user_agent,
                    random.choice(self._scripts) if self._scripts else _FALLBACK_SCRIPT,
                    self._dpl,
                    "en-US",
                    "en-US,es-US,en,es",
                    0,
                    random.choice(_NAV_KEYS),
                    random.choice(_DOC_KEYS),
                    random.choice(_WIN_KEYS),
                    perf,
                    str(uuid.uuid4()),
                    "",
                    random.choice([8, 16, 24, 32]),
                    time.time() * 1000 - perf,
                ]
                self._config_ts = now
            return list(self._config)
