"""Upstream client for the ChatGPT web app (chatgpt.com backend-api).

Auth = the browser cookie jar pasted into gpt_cookie.txt. Requests go through
curl_cffi with a Chrome TLS fingerprint + matching UA/sec-ch-ua headers so the
Cloudflare WAF sees a browser-shaped client. One session per thread (curl
handles are not thread-safe).

Before every conversation the client runs the sentinel gate (see sentinel.py):
POST /sentinel/chat-requirements with a pre-challenge token, solve the
proof-of-work, then attach the openai-sentinel-* headers.

Anonymous vs logged-in: GET /backend-api/me returns id "ua-<device-id>" with
empty email for a logged-out cookie jar — those requests go to
/backend-anon (like the real web app); a real session keeps /backend-api.

Conversation SSE: the parser is deliberately tolerant — it understands the
{"v": "<delta>"} encoding, {"v": {"message": ...}} envelopes, the older
full-snapshot {"message": ...} format and {"p": ..., "v": ...} patch events,
so minor web-app protocol drift does not break streaming.

Retry / latency:
  - split timeouts (connect read vs total) so a stalled upstream fails fast
  - bounded retries with backoff for transient 429 / 5xx / transport errors
  - a request-wide deadline bounds the total end-to-end cost across retries
  - the limiter (if injected) gates concurrency with AIMD admission control
"""

import json
import re
import threading
import time
import uuid
from collections import deque

from curl_cffi.requests import Session

from .sentinel import CHROME_UA, SEC_CH_UA, Sentinel, answer_token, requirements_token

BASE = "https://chatgpt.com"

DEBUG_SSE = __import__("os").environ.get("CHATGPT2API_DEBUG") == "1"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class ChatGPTError(Exception):
    """Upstream failure, mapped to an HTTP status for the OpenAI response."""

    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


class AuthError(ChatGPTError):
    def __init__(self, message=None):
        super().__init__(message or
                         "Cookie invalid ya expire ho gaya — chatgpt.com se FRESH cookie copy karke "
                         "gpt_cookie.txt me daalo (login ke baad, sab cookies)", status=401)


class RateLimitedError(ChatGPTError):
    def __init__(self, message=None, retry_after=None):
        super().__init__(message or
                         "ChatGPT rate limit/quota hit — thodi der baad try karo", status=429)
        self.retry_after = retry_after


class UpstreamTimeoutError(ChatGPTError):
    """A local deadline / first-byte guard tripped before usable output."""
    pass


def _no_login_hint():
    return ("Is cookie me login session nahi hai (ChatGPT ise anonymous browser samajh raha hai). "
            "Browser me chatgpt.com kholo, LOGIN karo, phir F12 -> Application -> Cookies -> "
            "https://chatgpt.com ki SAB cookies copy karke gpt_cookie.txt me paste kardo "
            "(__Secure-next-auth.session-token / oai-sc zaroori hai).")


_MISS = object()


def _looks_like_cloudflare(text):
    low = (text or "").lower()
    return ("just a moment" in low or "challenge-platform" in low
            or "cf-error-details" in low or "attention required" in low)


def extract_device_id(cookie):
    m = re.search(r"(?:^|[;\s])oai-did=([^;]+)", cookie or "")
    return m.group(1).strip() if m else None


class ChatGPTClient:
    def __init__(self, cookie, proxy=None, timeout=300.0, retry_attempts=2,
                 retry_delay_sec=1.0, limiter=None):
        self.cookie = cookie
        self.device_id = extract_device_id(cookie)
        self.timeout = float(timeout)
        self._proxy = proxy
        self._local = threading.local()
        self._sentinel = Sentinel()
        self._models_lock = threading.Lock()
        self._models = []
        self._models_ts = 0.0
        self._persona_lock = threading.Lock()
        self._anon = None  # None = unknown, True = anonymous device identity
        self._last_turnstile = None
        # --- retry / latency configuration ---
        self.retry_attempts = max(1, int(retry_attempts or 1))
        self.retry_delay_sec = float(retry_delay_sec or 0.0)
        self.limiter = limiter  # optional AdaptiveConcurrency injected by the server
        self._deadline = None  # request-wide deadline (monotonic), set per conversation
        self._last_retry_after = 0.0

    # -- sessions / headers -------------------------------------------------
    @property
    def session(self):
        s = getattr(self._local, "session", None)
        if s is None:
            kwargs = {}
            if self._proxy:
                kwargs["proxies"] = {"http": self._proxy, "https": self._proxy}
            s = Session(impersonate="chrome", timeout=self.timeout, **kwargs)
            self._local.session = s
        return s

    def _headers(self, stream=False, with_body=False):
        h = {
            "Accept": "text/event-stream" if stream else "application/json",
            "Origin": BASE,
            "Referer": BASE + "/",
            "oai-language": "en-US",
            "User-Agent": CHROME_UA,
            "sec-ch-ua": SEC_CH_UA,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Cookie": self.cookie,
        }
        if self.device_id:
            h["oai-device-id"] = self.device_id
        if with_body:
            h["Content-Type"] = "application/json"
        return h

    @property
    def base(self):
        return BASE + ("/backend-anon" if self._anon else "/backend-api")

    def _raise_for_status(self, resp, what):
        code = resp.status_code
        if code == 200:
            return
        try:
            snippet = (resp.text or "")[:300]
        except Exception:
            snippet = ""
        if code in (401, 403):
            if _looks_like_cloudflare(snippet):
                raise ChatGPTError(
                    "Cloudflare ne block kiya (challenge) — thodi der baad try karo, ya browser me "
                    "chatgpt.com kholke fresh cookie (esp. __cf_bm) copy karo", status=403)
            if self._anon and self._last_turnstile:
                raise ChatGPTError(
                    "Anonymous chat ko Turnstile challenge chahiye jo headless solve nahi ho sakta. "
                    + _no_login_hint(), status=403)
            raise AuthError(f"{what} failed: HTTP {code}. " + _no_login_hint())
        if code == 429:
            retry_after = None
            raw = resp.headers.get("Retry-After")
            if raw:
                try:
                    retry_after = float(raw)
                except ValueError:
                    pass
            raise RateLimitedError(f"{what} failed: HTTP 429 — {snippet[:150]}",
                                   retry_after=retry_after)
        if code == 413:
            raise ChatGPTError(
                f"{what} failed: HTTP 413 — request too large; shorten the conversation and try again",
                status=413)
        raise ChatGPTError(f"{what} failed: HTTP {code} — {snippet[:200]}", status=502)

    # -- persona ------------------------------------------------------------
    def detect_persona(self):
        """Decide anonymous vs logged-in via /backend-api/me (cached)."""
        if self._anon is not None:
            return self._anon
        with self._persona_lock:
            if self._anon is not None:
                return self._anon
            try:
                resp = self.session.get(BASE + "/backend-api/me", headers=self._headers())
                data = resp.json() if resp.status_code == 200 else {}
                uid = str(data.get("id") or "")
                self._anon = uid.startswith("ua-")
            except Exception:
                self._anon = False
            return self._anon

    # -- models ---------------------------------------------------------------
    def list_models(self, refresh_sec=600):
        now = time.time()
        with self._models_lock:
            if self._models and now - self._models_ts < refresh_sec:
                return list(self._models)
        resp = self.session.get(
            BASE + "/backend-api/models",
            params={"history_and_training_disabled": "false"},
            headers=self._headers(),
        )
        if resp.status_code != 200 and self._anon:
            resp = self.session.get(
                self.base + "/models",
                params={"history_and_training_disabled": "false"},
                headers=self._headers(),
            )
        self._raise_for_status(resp, "GET models")
        try:
            data = resp.json()
        except ValueError:
            raise ChatGPTError("Models response JSON nahi hai — protocol badla hua lagta hai", status=502)
        slugs = []
        for m in data.get("models", []):
            slug = m.get("slug")
            if slug and slug not in slugs:
                slugs.append(slug)
        if not slugs:
            raise ChatGPTError("Models list khali aayi — protocol badla hua lagta hai", status=502)
        with self._models_lock:
            self._models = slugs
            self._models_ts = now
        return list(slugs)

    def cached_models(self):
        return list(self._models)

    # -- backoff / deadline ---------------------------------------------------

    def _backoff(self, attempt, exc):
        """Kind-aware retry delay in seconds for transient failures."""
        if isinstance(exc, RateLimitedError) and exc.retry_after:
            return exc.retry_after + 0.1
        code = getattr(exc, "status", None)
        if code in RETRYABLE_STATUSES:
            return self.retry_delay_sec * (2 ** attempt) + 0.1
        return self.retry_delay_sec

    def _sleep_before_retry(self, attempt, exc, deadline):
        """Bounded, deadline-aware backoff."""
        max_wait = 8.0  # never sleep more than 8s inside a slot
        delay = self._backoff(attempt, exc)
        delay = min(delay, max_wait)
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0.05:
                raise UpstreamTimeoutError(
                    "request deadline reached before the next upstream attempt")
            delay = min(delay, max(0.0, remaining - 0.05))
        if delay > 0:
            time.sleep(delay)

    # -- sentinel ---------------------------------------------------------------
    def _chat_requirements(self):
        """Run the sentinel gate; returns (chat_token, proof_token, persona).

        Retries transient failures (429/5xx/transport) with backoff. Auth errors
        (401/403) and oversized requests (413) are never retried.
        """
        self.detect_persona()
        last_exc = None
        for attempt in range(self.retry_attempts):
            if self._deadline is not None and self._deadline - time.monotonic() <= 0:
                raise UpstreamTimeoutError(
                    "request deadline reached before chat-requirements")
            config = self._sentinel.config(self.session)
            body = {"p": requirements_token(list(config))}
            try:
                resp = self.session.post(self.base + "/sentinel/chat-requirements",
                                         json=body, headers=self._headers(with_body=True))
            except Exception as exc:
                last_exc = ChatGPTError(f"ChatGPT tak request nahi pahunchi: {exc}", status=502)
                if attempt < self.retry_attempts - 1:
                    self._sleep_before_retry(attempt, last_exc, self._deadline)
                    continue
                raise last_exc
            try:
                self._raise_for_status(resp, "chat-requirements")
            except RateLimitedError as exc:
                last_exc = exc
                if attempt < self.retry_attempts - 1:
                    self._sleep_before_retry(attempt, exc, self._deadline)
                    continue
                raise
            except ChatGPTError as exc:
                if exc.status in RETRYABLE_STATUSES and attempt < self.retry_attempts - 1:
                    last_exc = exc
                    self._sleep_before_retry(attempt, exc, self._deadline)
                    continue
                raise

            try:
                data = resp.json()
            except ValueError:
                raise ChatGPTError("chat-requirements JSON nahi hai — " + (resp.text or "")[:150], status=502)
            chat_token = data.get("token")
            if not chat_token:
                raise ChatGPTError("chat-requirements token nahi mila — " + json.dumps(data)[:150], status=502)
            persona = data.get("persona")

            proof_token = ""
            pow_info = data.get("proofofwork") or {}
            if pow_info.get("required"):
                proof_token, solved = answer_token(pow_info.get("seed", ""), pow_info.get("difficulty", "0fffff"), config)
                if not solved:
                    raise ChatGPTError(
                        "Proof-of-work solve nahi hua (difficulty bahut high) — thodi der baad try karo", status=502)

            turnstile = data.get("turnstile") or {}
            self._last_turnstile = bool(turnstile.get("required"))
            return chat_token, proof_token, persona

        raise last_exc

    # -- conversation ---------------------------------------------------------
    def start_conversation(self, prompt, model_slug="auto", history_disabled=True,
                           deadline_sec=None):
        """Start a conversation with bounded retries and an overall deadline.

        ``deadline_sec`` bounds the total time across all retry attempts (0 or
        None disables the deadline).
        """
        self._deadline = None
        if deadline_sec and deadline_sec > 0:
            self._deadline = time.monotonic() + deadline_sec

        last_exc = None
        for attempt in range(self.retry_attempts):
            if self._deadline is not None and self._deadline - time.monotonic() <= 0:
                raise UpstreamTimeoutError("upstream conversation exceeded the request deadline")
            chat_token, proof_token, _persona = self._chat_requirements()

            payload = {
                "action": "next",
                "client_contextual_info": {
                    "is_dark_mode": False,
                    "time_since_loaded": 87,
                    "page_height": 768,
                    "page_width": 1366,
                    "pixel_ratio": 1.5,
                    "screen_height": 864,
                    "screen_width": 1536,
                },
                "conversation_mode": {"kind": "primary_assistant"},
                "conversation_origin": None,
                "force_paragen": False,
                "force_paragen_model_slug": "",
                "force_rate_limit": False,
                "force_use_sse": True,
                "history_and_training_disabled": bool(history_disabled),
                "messages": [{
                    "id": str(uuid.uuid4()),
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": [prompt]},
                    "metadata": {},
                }],
                "model": model_slug,
                "paragen_cot_summary_display_override": "allow",
                "paragen_stream_type_override": None,
                "parent_message_id": str(uuid.uuid4()),
                "reset_rate_limits": False,
                "suggestions": [],
                "supported_encodings": [],
                "system_hints": [],
                "timezone": "America/Los_Angeles",
                "timezone_offset_min": -480,
                "variant_purpose": "comparison_implicit",
                "websocket_request_id": str(uuid.uuid4()),
            }
            headers = self._headers(stream=True, with_body=True)
            headers["openai-sentinel-chat-requirements-token"] = chat_token
            headers["openai-sentinel-proof-token"] = proof_token
            try:
                resp = self.session.post(self.base + "/conversation",
                                         json=payload, headers=headers, stream=True)
            except Exception as exc:
                last_exc = ChatGPTError(f"ChatGPT tak request nahi pahunchi: {exc}", status=502)
                if attempt < self.retry_attempts - 1:
                    self._sleep_before_retry(attempt, last_exc, self._deadline)
                    continue
                raise last_exc
            try:
                self._raise_for_status(resp, "POST conversation")
            except RateLimitedError as exc:
                last_exc = exc
                # respect upstream Retry-After if present, but bounded
                if attempt < self.retry_attempts - 1:
                    self._sleep_before_retry(attempt, exc, self._deadline)
                    continue
                raise
            except ChatGPTError as exc:
                if exc.status in RETRYABLE_STATUSES and attempt < self.retry_attempts - 1:
                    last_exc = exc
                    self._sleep_before_retry(attempt, exc, self._deadline)
                    continue
                raise

            # 200 OK — return the streaming response
            self._last_retry_after = 0.0
            return resp

        raise last_exc

    # -- SSE parsing -------------------------------------------------------------
    @staticmethod
    def iter_deltas(resp, state):
        """Parse the conversation SSE; yield assistant text deltas.

        `state` accumulates conversation_id / message_id / model_slug / usage /
        text / finished / moderated. Raises ChatGPTError on upstream error
        events.
        """
        state.setdefault("conversation_id", None)
        state.setdefault("message_id", None)
        state.setdefault("model_slug", None)
        state.setdefault("usage", None)
        state.setdefault("text", "")
        state.setdefault("got_delta", False)
        state.setdefault("finished", False)
        state.setdefault("moderated", False)
        tail = deque(maxlen=40)
        saw_event = False
        for raw in resp.iter_lines():
            if not raw:
                continue
            line = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
            if DEBUG_SSE:
                print("[SSE] " + line[:300], flush=True)
            tail.append(line)
            if line.startswith("data:"):
                data = line[5:].strip()
            elif line.lstrip().startswith("{"):
                data = line.strip()
            else:
                continue
            if not data:
                continue
            if data == "[DONE]":
                state["finished"] = True
                break
            try:
                obj = json.loads(data)
            except ValueError:
                continue
            saw_event = True
            delta = _event_delta(obj, state)
            if delta:
                yield delta
        if saw_event and not state.get("text") and not DEBUG_SSE:
            print("[WARN] Stream khatam par text khali aaya. Last SSE events (debug ke liye):\n"
                  + "\n".join(tail[-10:]), flush=True)


def _event_delta(obj, state):
    """Extract assistant text from one SSE event; update `state` in place.

    Returns the text that should be emitted as a delta for this event.
    Raises ChatGPTError when the event is an upstream error.
    """
    if isinstance(obj, str):
        if obj:
            state["got_delta"] = True
            state["text"] += obj
        return obj
    if not isinstance(obj, dict):
        return ""

    if obj.get("error"):
        err = obj["error"]
        if isinstance(err, dict):
            msg = err.get("message") or json.dumps(err)[:200]
            code = str(err.get("code") or "")
        else:
            msg, code = str(err), ""
        if "rate" in code.lower() or "too many" in msg.lower() or "429" in msg:
            raise RateLimitedError(f"ChatGPT error: {msg}")
        if ("expired" in msg.lower() or "unauthorized" in msg.lower()
                or "sign in" in msg.lower() or "unusual activity" in msg.lower()):
            raise AuthError(f"ChatGPT error: {msg}")
        raise ChatGPTError(f"ChatGPT error: {msg}", status=502)

    if obj.get("type") == "moderation":
        state["moderated"] = True
        mod = obj.get("moderation_response") or {}
        if isinstance(mod, dict) and mod.get("blocked"):
            raise ChatGPTError("ChatGPT ne request block kar di (moderation)", status=400)
        return ""

    delta = ""
    v = obj.get("v", _MISS)
    if v is not _MISS:
        delta += _event_delta(v, state)

    msg = obj.get("message")
    if isinstance(msg, dict):
        delta += _message_event(msg, obj, state)

    conv = obj.get("conversation_id")
    if conv and not state.get("conversation_id"):
        state["conversation_id"] = conv
    if obj.get("moderation_response"):
        state["moderated"] = True
    return delta


def _message_event(msg, envelope, state):
    conv = (envelope or {}).get("conversation_id") or msg.get("conversation_id")
    if conv and not state.get("conversation_id"):
        state["conversation_id"] = conv
    author = msg.get("author") or {}
    role = author.get("role")
    meta = msg.get("metadata") or {}
    if meta.get("model_slug"):
        state["model_slug"] = meta["model_slug"]
    usage = meta.get("usage")
    if isinstance(usage, dict) and usage.get("total_tokens"):
        state["usage"] = usage
    if role == "assistant":
        if msg.get("id") and not state.get("message_id"):
            state["message_id"] = msg["id"]
        if msg.get("status") == "finished_successfully":
            state["finished"] = True

    content = msg.get("content") or {}
    if content.get("content_type") == "text" and role == "assistant":
        parts = content.get("parts") or []
        text = "".join(p for p in parts if isinstance(p, str))
        if text:
            if state.get("got_delta"):
                # v1 mode: string deltas already streamed everything; a later
                # full-text envelope would only duplicate it.
                return ""
            prev = state.get("text", "")
            if text.startswith(prev):
                state["text"] = text
                return text[len(prev):]
            state["text"] = text
            return text
    return ""
