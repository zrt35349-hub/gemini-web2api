"""OpenAI-compatible HTTP server over the ChatGPT web app.

Endpoints:
  GET  / , /health          health JSON (with live load + fleet metrics)
  GET  /v1/models           model slugs from chatgpt.com/backend-api/models
  POST /v1/chat/completions chat (stream + non-stream)

Built on a bounded worker-pool HTTPServer — no web framework. Chunked request
bodies are decoded manually. Optional auth via config.api_keys (empty list =
open). Under heavy load the AIMD admission gate and lane-scheduled agent fleet
keep latency predictable instead of piling up.

Latency/stability notes:
  - SSE headers flush immediately; upstream events forward as they arrive
  - bounded worker pool + bounded queue shed overload cleanly (503/Retry-After)
  - lane-scheduled agent fleet executes upstream calls in parallel: express
    agents keep small chat requests fast while large payloads run on their own
    agents, and a supervisor revives crashed agents
  - AIMD admission gate sheds overload fast (429 + Retry-After, never a hang)
  - unknown routes never consume admission capacity
  - split timeouts (request deadline) so a stalled upstream fails fast
  - bounded retries with backoff for transient 429/5xx/transport errors
  - SSE keep-alive comments keep proxies from timing out on slow streams
  - oversized bodies are rejected with 413 before touching upstream
"""

import json
import queue
import socket
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from . import __version__
from .agents import AgentError, PoolBusy, build_pool, classify as classify_lane
from .chatgpt import (ChatGPTClient, ChatGPTError, AuthError, RateLimitedError,
                      UpstreamTimeoutError)
from .limiter import AdaptiveConcurrency

_CREATED_TS = int(time.time())

# Client names ChatGPT web does not know → nearest web equivalent.
_MODEL_ALIASES = {
    "gpt-3.5-turbo": "auto",
    "gpt-4": "auto",
    "gpt-4-turbo": "auto",
    "gpt-4o": "auto",
    "gpt-4o-mini": "auto",
    "gpt-4.1": "auto",
    "gpt-4.1-mini": "auto",
}
_FALLBACK_MODELS = ["auto", "gpt-5", "gpt-5-mini", "gpt-4o", "gpt-4o-mini",
                    "gpt-4.1", "gpt-4.1-mini", "o4-mini"]
_PROMPT_BUDGETS_BYTES = (78000, 58000, 43000, 32000, 24000)
_OMITTED_HISTORY_NOTICE = (
    "[Some earlier conversation history was omitted to fit the request size limit. "
    "The initial project request and latest user request are preserved.]")


class PoolHTTPServer(HTTPServer):
    """HTTP server on a fixed pool of daemon worker threads.

    ThreadingHTTPServer grows a thread per connection with no ceiling — under
    a traffic spike that means thousands of threads and trashed memory. A
    bounded pool keeps footprint flat, reuses threads (no per-connection
    spawn cost), and when the queue is full the connection is rejected
    immediately with 503 + Retry-After instead of piling up.
    """

    request_queue_size = 512

    def __init__(self, address, handler, pool_workers=64, queue_depth=512):
        super().__init__(address, handler)
        self._tasks = queue.Queue(maxsize=queue_depth)
        for _ in range(max(1, int(pool_workers))):
            threading.Thread(target=self._work, daemon=True,
                             name="chatgpt-web2api-worker").start()

    def _work(self):
        while True:
            task = self._tasks.get()
            if task is None:
                return
            request, client = task
            try:
                self.finish_request(request, client)
            except (ConnectionError, TimeoutError, socket.timeout):
                pass  # client hung up / timed out — routine under load
            except Exception:
                self.handle_error(request, client)
            finally:
                self.shutdown_request(request)

    def process_request(self, request, client_address):
        try:
            self._tasks.put_nowait((request, client_address))
        except queue.Full:
            # saturated: answer with a real 503 + Retry-After instead of a
            # silent connection drop (clients otherwise retry-storm instantly)
            self._reject_overloaded(request)
            self.shutdown_request(request)

    @staticmethod
    def _reject_overloaded(request):
        body = (b'{"error":{"message":"server overloaded: request queue is full",'
                b'"type":"rate_limit_error","code":"overloaded"}}')
        head = (b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Content-Type: application/json; charset=utf-8\r\n"
                b"Retry-After: 1\r\n"
                b"Connection: close\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n")
        try:
            request.sendall(head + body)
        except OSError:
            pass


class App:
    """Wires config + ChatGPT client + limiter + agent fleet together."""

    def __init__(self, config, client=None):
        self.config = config
        self.limiter = AdaptiveConcurrency(
            initial=getattr(config, "initial_concurrent_requests",
                            getattr(config, "max_concurrent_requests", 8)),
            minimum=getattr(config, "min_concurrent_requests", 1),
            maximum=getattr(config, "max_concurrent_requests", 8),
            adaptive=getattr(config, "adaptive_concurrency", True),
            latency_target_sec=getattr(config, "latency_target_sec", 0.0),
        )
        # Backwards-compatible name: call sites talk to app.semaphore.
        # It is now an AIMD gate, not a fixed threading semaphore.
        self.semaphore = self.limiter
        # Inject the limiter into the client for capacity-aware retry behavior
        if client is not None:
            try:
                client.limiter = self.limiter
            except Exception:
                pass
        self.client = client
        # Parallel agent fleet: every POST request's upstream work runs on a
        # lane-scheduled agent thread (None = legacy inline execution).
        self.agents = build_pool(config)
        self.agent_submit_wait_sec = max(
            0.0, float(getattr(config, "agent_submit_wait_sec", 0) or 0))
        self.lane_express_chars = int(getattr(config, "lane_express_chars", 4000) or 4000)
        self.lane_heavy_chars = int(getattr(config, "lane_heavy_chars", 60000) or 60000)
        # max seconds a request may wait for a free upstream slot before a 429
        self.queue_wait_sec = max(0.0, float(getattr(config, "queue_wait_sec", 0) or 0))
        self._metrics_lock = threading.Lock()
        self.requests_total = 0
        self.requests_failed = 0
        self._latency_samples = 0
        self._queue_ewma = 0.0
        self._ttfb_ewma = 0.0
        self._total_ewma = 0.0
        self._last_queue_wait_ms = 0.0
        self._last_total_ms = 0.0

    def warmup(self):
        """Prefetch models + persona in the background; never fatal."""
        try:
            cfg = self.config
            slugs = self.client.list_models(cfg.models_refresh_sec)
            state["upstream_ok"] = True
            print("[OK] Cookie valid hai — models: " + ", ".join(slugs[:10])
                  + (" ..." if len(slugs) > 10 else ""), flush=True)
        except ChatGPTError as exc:
            state["upstream_ok"] = False
            print("[FAIL] ChatGPT cookie check: %s" % exc, flush=True)

    def state_refresher(self):
        """Periodically re-fetches the sentinel build label so requests never
        pay the page-fetch round-trip and stay resilient to protocol drift.
        Runs until process exit; failures are quiet (stale state keeps working
        until upstream actually rejects it)."""
        interval = max(0, int(getattr(self.config, "state_refresh_sec", 300)))
        if not interval:
            return
        while True:
            time.sleep(interval)
            try:
                self.client._sentinel.refresh_build(self.client.session)
            except Exception:
                pass

    def note_request(self, queue_wait_ms=0.0, ttfb_ms=None, total_ms=0.0, ok=True):
        """Records one client request for the /health latency snapshot."""
        alpha = 0.2
        ttfb_val = float(total_ms if ttfb_ms is None else ttfb_ms)
        with self._metrics_lock:
            self.requests_total += 1
            if not ok:
                self.requests_failed += 1
            self._last_queue_wait_ms = float(queue_wait_ms)
            self._last_total_ms = float(total_ms)
            if not self._latency_samples:
                self._queue_ewma = float(queue_wait_ms)
                self._ttfb_ewma = ttfb_val
                self._total_ewma = float(total_ms)
            else:
                self._queue_ewma = (1 - alpha) * self._queue_ewma + alpha * queue_wait_ms
                self._ttfb_ewma = (1 - alpha) * self._ttfb_ewma + alpha * ttfb_val
                self._total_ewma = (1 - alpha) * self._total_ewma + alpha * total_ms
            self._latency_samples += 1

    def load_snapshot(self):
        """Live load + latency state for /health and operators."""
        snapshot = self.limiter.describe()
        with self._metrics_lock:
            snapshot.update({
                "requests": self.requests_total,
                "failures": self.requests_failed,
                "queue_wait_ms": round(self._queue_ewma, 1),
                "last_queue_wait_ms": round(self._last_queue_wait_ms, 1),
                "ttfb_ms": round(self._ttfb_ewma, 1),
                "latency_ms": round(self._total_ewma, 1),
                "last_latency_ms": round(self._last_total_ms, 1),
            })
        snapshot["queue_wait_sec"] = round(self.queue_wait_sec, 3)
        snapshot["latency_target_ms"] = int(
            float(getattr(self.config, "latency_target_sec", 0) or 0) * 1000)
        snapshot["agents"] = (self.agents.describe() if self.agents is not None
                              else {"enabled": False})
        return snapshot

    def lane_for(self, data, body_len):
        """Routes one parsed request body to an agent lane (express/standard/heavy)."""
        return classify_lane(body_len, data,
                             express_chars=self.lane_express_chars,
                             heavy_chars=self.lane_heavy_chars)

    def shutdown(self):
        """Stops the agent fleet (embedded use / clean process exit)."""
        if self.agents is not None:
            self.agents.stop()


# Module-level state for /health and handler access
_app = None
state = {"config": None, "client": None, "upstream_ok": None}


def set_state(cfg, client):
    state["config"] = cfg
    state["client"] = client


def resolve_model(requested, available, default):
    if not requested:
        return default or "auto"
    if requested in available:
        return requested
    low = str(requested).lower()
    for slug in available:
        if slug.lower() == low:
            return slug
    return _MODEL_ALIASES.get(low, default or "auto")


def _text_of(content):
    """OpenAI message content (string or parts list) → plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        out = []
        for part in content:
            if isinstance(part, str):
                out.append(part)
            elif isinstance(part, dict):
                ptype = part.get("type")
                if ptype == "text":
                    out.append(str(part.get("text") or ""))
                elif ptype in ("image_url", "input_image", "image"):
                    out.append("[image attached — is server me abhi sirf text support hai]")
        return "\n".join(t for t in out if t)
    return ""


class PromptTooLong(ValueError):
    """The latest message and system instructions exceed the safe upstream limit."""


def _serialized_prompt_size(prompt):
    return len(json.dumps(prompt, ensure_ascii=True).encode("utf-8"))


def messages_to_prompt(messages, max_bytes=None):
    """Flatten OpenAI chat messages into one ChatGPT prompt.

    Every request opens a fresh ChatGPT conversation, so a single user
    message goes as-is and longer histories become a labelled transcript.
    """
    sys_parts = []
    chat = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        text = _text_of(m.get("content")).strip()
        if not text:
            continue
        if role in ("system", "developer"):
            sys_parts.append(text)
        elif role in ("user", "assistant", "tool"):
            chat.append((role, text))
    sys_block = "\n\n".join(sys_parts)

    def render(history, omitted=False):
        if not history:
            return sys_parts[0] if sys_parts else "Hello"
        if len(history) == 1 and history[0][0] == "user" and not omitted:
            return (sys_block + "\n\n" + history[0][1]) if sys_block else history[0][1]
        lines = []
        if sys_block:
            lines.append("Instructions (follow these strictly):\n" + sys_block)
        if omitted:
            lines.append(_OMITTED_HISTORY_NOTICE)
        for role, text in history:
            lines.append(("User: " if role == "user" else "Assistant: ") + text)
        lines.append(
            "Answer the latest User message fully and directly. For software tasks, "
            "provide a complete, practical implementation rather than only a brief summary. "
            "If a genuine limit prevents completing the request, clearly say what remains.")
        return "\n\n".join(lines)

    prompt = render(chat)
    if max_bytes is None or _serialized_prompt_size(prompt) <= max_bytes:
        return prompt

    first_user = next((i for i, (role, _) in enumerate(chat) if role == "user"), None)
    latest_user = next((i for i in range(len(chat) - 1, -1, -1)
                        if chat[i][0] == "user"), len(chat) - 1)
    retained_indices = {latest_user}
    if first_user is not None:
        retained_indices.add(first_user)

    def render_indices(indices):
        return render([chat[i] for i in sorted(indices)], omitted=True)

    prompt = render_indices(retained_indices)
    if _serialized_prompt_size(prompt) > max_bytes:
        if first_user == latest_user:
            reason = "The latest message and system instructions exceed"
        else:
            reason = "The initial project request, latest message, and system instructions exceed"
        raise PromptTooLong(
            "%s the %d-byte request limit. Shorten those instructions or split the request "
            "into smaller parts." % (reason, max_bytes))

    for index in range(len(chat) - 1, -1, -1):
        if index in retained_indices:
            continue
        candidate_indices = retained_indices | {index}
        candidate_prompt = render_indices(candidate_indices)
        if _serialized_prompt_size(candidate_prompt) <= max_bytes:
            retained_indices = candidate_indices
            prompt = candidate_prompt
    return prompt


def start_conversation_with_compaction(client, messages, model, history_disabled,
                                       on_retry=None, on_attempt=None,
                                       deadline_sec=None):
    """Retry a rejected oversized prompt with progressively smaller history.

    ``deadline_sec`` (optional) bounds the total upstream cost of each attempt.
    """
    for index, budget in enumerate(_PROMPT_BUDGETS_BYTES):
        prompt = messages_to_prompt(messages, max_bytes=budget)
        if on_attempt:
            on_attempt(prompt)
        try:
            kwargs = {}
            if deadline_sec:
                kwargs["deadline_sec"] = deadline_sec
            response = client.start_conversation(
                prompt, model, history_disabled=history_disabled, **kwargs)
            return prompt, response
        except ChatGPTError as exc:
            if exc.status != 413 or index == len(_PROMPT_BUDGETS_BYTES) - 1:
                raise
            if on_retry:
                on_retry("upstream rejected prompt with HTTP 413; retrying with a %d-byte limit"
                         % _PROMPT_BUDGETS_BYTES[index + 1])


def _est_tokens(text):
    return max(1, len(text or "") // 4)


class BodyTooLargeError(Exception):
    pass


def make_handler(app):
    """Returns a request handler class bound to the given App."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        # Nagle off: small SSE chunks flush immediately instead of waiting on
        # delayed-ACK interaction (visible per-chunk latency on Windows)
        disable_nagle_algorithm = True
        # idle keep-alive connections and stuck clients are reaped after 120s
        # instead of pinning a pool worker forever (slowloris protection)
        timeout = 120
        server_version = "chatgptwebapp2api/" + __version__
        _response_started = False

        # ------------------------------------------------------------ plumbing

        def setup(self):
            super().setup()
            self._sse_lock = threading.Lock()
            self._stream_stop = threading.Event()
            self._first_byte_ts = None
            self._keepalive_thread = None
            self._upstream_error = None

        def log_message(self, fmt, *args):  # default stderr logging is noisy
            pass

        def _log(self, msg):
            if app.config.log_requests:
                sys.stdout.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), msg))
                sys.stdout.flush()

        def _send_json(self, obj, status=200, headers=None):
            body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self._first_byte_ts = time.monotonic()
            self.wfile.write(body)

        def _send_error_obj(self, status, message, err_type="invalid_request_error",
                            headers=None):
            self._send_json({"error": {"message": message, "type": err_type,
                                       "code": status}},
                            status, headers=headers)

        def _fail(self, status, message, err_type="invalid_request_error", headers=None):
            if self._response_started:
                # headers already flushed (SSE) — nothing to do but hang up
                self.close_connection = True
                return
            self._send_error_obj(status, message, err_type, headers=headers)

        def _start_stream(self, content_type):
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            self._response_started = True
            self._first_byte_ts = time.monotonic()
            self._start_keepalive()

        def _start_keepalive(self):
            """Periodic SSE comments keep proxies/clients from timing out on a
            slow upstream."""
            interval = float(getattr(app.config, "sse_keepalive_sec", 0) or 0)
            if interval <= 0:
                return
            self._stream_stop.clear()
            thread = threading.Thread(target=self._keepalive_loop, args=(interval,),
                                      name="chatgpt-sse-keepalive", daemon=True)
            self._keepalive_thread = thread
            thread.start()

        def _keepalive_loop(self, interval):
            while not self._stream_stop.wait(interval):
                try:
                    with self._sse_lock:
                        if self._stream_stop.is_set():
                            return
                        self.wfile.write(b": keep-alive\n\n")
                        self.wfile.flush()
                except OSError:
                    return

        def _end_stream(self):
            stop = getattr(self, "_stream_stop", None)
            if stop is not None:
                stop.set()

        def _read_body(self):
            limit = int(float(getattr(app.config, "max_body_mb", 10) or 10) * 1_000_000)
            te = (self.headers.get("Transfer-Encoding") or "").lower()
            if "chunked" in te:
                parts = []
                total = 0
                while True:
                    size_line = self.rfile.readline(65536).strip()
                    try:
                        size = int(size_line.split(b";")[0] or b"0", 16)
                    except ValueError:
                        break
                    if size <= 0:
                        self.rfile.readline(65536)
                        break
                    chunk = self.rfile.read(size)
                    parts.append(chunk)
                    total += len(chunk)
                    if total > limit:
                        raise BodyTooLargeError()
                    self.rfile.readline(65536)
                return b"".join(parts)
            length = int(self.headers.get("Content-Length") or 0)
            if length > limit:
                raise BodyTooLargeError()
            return self.rfile.read(length) if length > 0 else b""

        def _api_key(self):
            auth = (self.headers.get("Authorization") or "").strip()
            if auth.lower().startswith("bearer "):
                return auth[7:].strip()
            value = (self.headers.get("x-api-key") or "").strip()
            if value:
                return value
            query = parse_qs(urlparse(self.path).query)
            if query.get("key"):
                return query["key"][0]
            return None

        def _authorized(self):
            keys = app.config.api_keys
            if not keys:
                return True
            return self._api_key() in keys

        # --------------------------------------------------------------- verbs

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             "Authorization, Content-Type, x-api-key")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            try:
                self._route_get()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self._end_stream()

        def do_POST(self):
            try:
                self._route_post()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self._end_stream()

        # -------------------------------------------------------------- routes

        def _route_get(self):
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path in ("/", "/health"):
                self._handle_health()
                return
            if path == "/favicon.ico":
                # browsers request this unprompted on every page visit; a
                # 204 keeps 401 noise out of otherwise-clean request logs
                self.send_response(204)
                self.end_headers()
                return
            if not self._authorized():
                self._send_error_obj(401, "Api key galat hai (Authorization: Bearer <key>)",
                                     "authentication_error")
                return
            if path == "/v1/models":
                self._handle_models()
                return
            if path == "/v1":
                self._send_json({
                    "object": "api",
                    "service": "chatgptwebapp2api",
                    "message": "API is running. Use POST /v1/chat/completions to send a chat request.",
                    "endpoints": ["/v1/models", "/v1/chat/completions"],
                })
                return
            self._send_error_obj(404, "Unknown path: %s" % path)

        def _handle_health(self):
            cfg, client = app.config, app.client
            self._send_json({
                "status": "ok",
                "service": "chatgptwebapp2api",
                "version": __version__,
                "base_url": "http://localhost:%d/v1" % cfg.port,
                "cookie_file": cfg.cookie_path,
                "device_id": (client.device_id or "")[:8] + "...",
                "models_cached": client.cached_models(),
                "upstream_ok": state["upstream_ok"],
                "load": app.load_snapshot(),
            })

        def _handle_models(self):
            cfg, client = app.config, app.client
            try:
                slugs = client.list_models(cfg.models_refresh_sec)
                state["upstream_ok"] = True
            except ChatGPTError as exc:
                if client.cached_models():
                    slugs = client.cached_models()
                else:
                    self._log("models error %s: %s" % (exc.status, exc))
                    return self._send_error_obj(exc.status, str(exc), "upstream_error")
            self._send_json({"object": "list", "data": [
                {"id": s, "object": "model", "created": _CREATED_TS, "owned_by": "chatgpt-web"}
                for s in slugs]})

        def _route_post(self):
            path = self.path.split("?")[0].rstrip("/")
            if not self._authorized():
                self._send_error_obj(401, "Api key galat hai (Authorization: Bearer <key>)",
                                     "authentication_error")
                return
            try:
                raw = self._read_body()
            except BodyTooLargeError:
                self._send_error_obj(
                    413, "request body exceeds %g MB limit"
                    % float(getattr(app.config, "max_body_mb", 10) or 10),
                    "request_too_large")
                return
            try:
                data = json.loads(raw.decode("utf-8", "replace") or "{}")
            except ValueError:
                self._send_error_obj(400, "Request body valid JSON nahi hai")
                return
            # resolve the route BEFORE taking an upstream slot: unknown routes
            # (404) must not consume admission capacity or count as failures
            runner = self._endpoint_runner(path, data)
            if runner is None:
                return
            started = time.monotonic()
            slot_held = False
            success = False
            pressure = None
            queue_wait_ms = 0.0
            try:
                wait_budget = app.queue_wait_sec
                waited_at = time.monotonic()
                if wait_budget > 0:
                    acquired = app.limiter.acquire(timeout=wait_budget)
                else:
                    acquired = app.limiter.acquire()  # legacy: wait indefinitely
                queue_wait_ms = (time.monotonic() - waited_at) * 1000.0
                if not acquired:
                    # fast, honest overload signal: a 429 + Retry-After beats
                    # parking the request behind a long queue (that queue wait
                    # is exactly what users see as huge latency)
                    retry_after = max(1, int(min(10.0, max(wait_budget, 1.0))))
                    self._send_json(
                        {"error": {
                            "message": ("server busy: all upstream slots are in use, "
                                        "retry after a short pause"),
                            "type": "rate_limit_error", "code": "server_busy"}},
                        429, headers={"Retry-After": str(retry_after)})
                    return
                slot_held = True
                try:
                    lane_busy = False
                    if app.agents is not None:
                        # execute the upstream part on a lane-bound agent:
                        # small chats never queue behind large-payload work
                        try:
                            app.agents.run(
                                app.lane_for(data, len(raw)), runner, name=path,
                                timeout=(app.agent_submit_wait_sec or None))
                        except PoolBusy:
                            lane_busy = True
                    else:
                        runner()
                    if lane_busy:
                        # lane queues saturated: same fast, honest overload signal
                        # as the admission gate (a 429 beats queueing silently)
                        retry_after = max(1, int(min(
                            10.0, max(app.agent_submit_wait_sec, 1.0))))
                        self._send_json(
                            {"error": {
                                "message": ("server busy: the agent lanes are "
                                            "saturated, retry after a short pause"),
                                "type": "rate_limit_error", "code": "server_busy"}},
                            429, headers={"Retry-After": str(retry_after)})
                    elif self._upstream_error is not None:
                        # in-band SSE failure: still counts as upstream pressure
                        pressure = "error"
                    else:
                        success = True
                except PromptTooLong:
                    pressure = None  # client error: no upstream pressure
                    raise
                except RateLimitedError:
                    pressure = "rate_limit"
                    raise
                except UpstreamTimeoutError:
                    pressure = "timeout"
                    raise
                except AgentError:
                    pressure = None  # fleet fault, not upstream pressure
                    raise
                except ChatGPTError:
                    pressure = "error"
                    raise
                except Exception:
                    pressure = "error"
                    raise
            except PromptTooLong as exc:
                self._fail(413, str(exc))
            except RateLimitedError as exc:
                headers = {}
                if getattr(exc, "retry_after", None):
                    headers["Retry-After"] = str(max(1, int(float(exc.retry_after))))
                self._fail(429, str(exc), "rate_limit_error", headers=headers)
            except UpstreamTimeoutError as exc:
                self._fail(504, str(exc), "timeout_error",
                           headers={"Retry-After": "1"})
            except AgentError as exc:
                # the fleet itself failed (agent died mid-job / pool stopped)
                self._fail(502, "agent failure: %s" % exc, "upstream_error")
            except ChatGPTError as exc:
                # preserve the upstream status (413/401/403/...) for the client
                self._fail(exc.status or 502, str(exc), "upstream_error")
            except Exception as exc:  # noqa: BLE001 - last-resort guard
                self._fail(500, "internal error: %s" % exc, "internal_error")
            finally:
                if slot_held:
                    if success:
                        app.limiter.release(success=True)
                    elif pressure:
                        app.limiter.release(success=False, pressure=pressure)
                    else:
                        app.limiter.release(neutral=True)
                ttfb = getattr(self, "_first_byte_ts", None)
                app.note_request(
                    queue_wait_ms=queue_wait_ms,
                    ttfb_ms=(ttfb - started) * 1000.0 if ttfb else None,
                    total_ms=(time.monotonic() - started) * 1000.0,
                    ok=success)

        def _endpoint_runner(self, path, data):
            """Returns the callable for a POST route (None = route 404 was sent).

            The returned callable is the unit of work an agent lane executes.
            """
            if path in ("/v1/chat/completions", "/chat/completions"):
                return lambda: self._chat(data)
            self._send_error_obj(404, "Unknown path: %s" % path)
            return None

        # --------------------------------------------------- chat completions

        def _chat(self, data):
            cfg, client = app.config, app.client
            messages = data.get("messages")
            if not isinstance(messages, list) or not messages:
                self._send_error_obj(400, "'messages' array required hai")
                return
            available = client.cached_models() or _FALLBACK_MODELS
            slug = resolve_model(data.get("model"), available, cfg.default_model)
            t0 = time.time()
            try:
                prompt, resp = start_conversation_with_compaction(
                    client, messages, slug, history_disabled=cfg.temporary_chats,
                    deadline_sec=getattr(cfg, "request_deadline_sec", 0) or 0,
                    on_retry=self._log,
                    on_attempt=lambda value: self._log(
                        "chat start: model=%s stream=%s chars=%d"
                        % (slug, bool(data.get("stream")), len(value))))
            except PromptTooLong:
                raise
            except ChatGPTError:
                raise
            if _serialized_prompt_size(prompt) < _serialized_prompt_size(
                    messages_to_prompt(messages)):
                self._log("chat history compacted to fit upstream request limit")

            if data.get("stream"):
                self._stream_openai(resp, slug, t0)
            else:
                self._collect_openai(resp, slug, len(prompt), t0)

        def _sse(self, obj):
            if isinstance(obj, str):
                payload = obj
            else:
                payload = json.dumps(obj, ensure_ascii=False)
            with self._sse_lock:
                self.wfile.write(b"data: " + payload.encode("utf-8") + b"\n\n")
                self.wfile.flush()

        def _sse_stream_error(self, exc):
            """In-stream error for clients after headers/role chunk went out."""
            self._sse({"error": {"message": str(exc), "type": "upstream_error",
                                 "code": getattr(exc, "status", 502)}})
            self._sse("[DONE]")

        def _chunk(self, cid, created, slug, delta, finish=None):
            self._sse({
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": slug,
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            })

        def _stream_openai(self, resp, slug, t0):
            cid = "chatcmpl-" + uuid.uuid4().hex[:24]
            created = int(time.time())
            self._start_stream("text/event-stream; charset=utf-8")

            stream_state = {}
            model_seen = slug
            try:
                self._chunk(cid, created, model_seen, {"role": "assistant", "content": ""})
                for delta in app.client.iter_deltas(resp, stream_state):
                    if delta:
                        self._chunk(cid, created, model_seen, {"content": delta})
                    if stream_state.get("model_slug"):
                        model_seen = stream_state["model_slug"]
                finish = "content_filter" if stream_state.get("moderated") else "stop"
                self._chunk(cid, created, model_seen, {}, finish=finish)
                usage = stream_state.get("usage")
                if isinstance(usage, dict) and usage.get("total_tokens"):
                    self._sse({"id": cid, "object": "chat.completion.chunk", "created": created,
                               "model": model_seen, "choices": [], "usage": usage})
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                self._log("chat stream ok %ds model=%s chars=%d"
                          % (time.time() - t0, model_seen, len(stream_state.get("text", ""))))
            except (BrokenPipeError, ConnectionResetError):
                self._log("chat stream: client disconnected")
            except ChatGPTError as exc:
                # upstream failed after the stream opened — surface in-band
                self._log("chat stream error %s: %s" % (exc.status, exc))
                self._upstream_error = exc
                try:
                    self._sse_stream_error(exc)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        def _collect_openai(self, resp, slug, prompt_chars, t0):
            stream_state = {}
            # non-streaming sends nothing until the end, so upstream errors
            # propagate to _route_post which maps them to the right status
            for _ in app.client.iter_deltas(resp, stream_state):
                pass
            text = stream_state.get("text", "")
            model_seen = stream_state.get("model_slug") or slug
            usage = stream_state.get("usage")
            if not (isinstance(usage, dict) and usage.get("total_tokens")):
                pt, ct = _est_tokens_prompt(prompt_chars), _est_tokens(text)
                usage = {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct}
            self._log("chat ok %ds model=%s chars=%d" % (time.time() - t0, model_seen, len(text)))
            self._send_json({
                "id": "chatcmpl-" + uuid.uuid4().hex[:24],
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model_seen,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "content_filter" if stream_state.get("moderated") else "stop",
                    "logprobs": None,
                }],
                "usage": usage,
            })

    return Handler


def _est_tokens_prompt(chars):
    return max(1, chars // 4)


def build_server(cfg, client):
    """Wires App + handler + bounded pool; returns (server, app).

    Separated from serve() so the exact production wiring is testable.
    """
    app = App(cfg, client=client)
    set_state(cfg, client)
    handler = make_handler(app)
    server = PoolHTTPServer(
        (cfg.host, cfg.port), handler,
        pool_workers=max(8, int(getattr(cfg, "pool_workers", 128))),
        queue_depth=max(16, int(getattr(cfg, "pool_queue_depth", 1024))))
    return server, app


def serve(cfg, client):
    try:
        server, app = build_server(cfg, client)
    except OSError as exc:
        print("ERROR: Port %d pe bind nahi hua (%s)" % (cfg.port, exc))
        print("Shayad purana server chal raha hai — usko band karo ya config.json me port badlo.")
        sys.exit(1)
    threading.Thread(target=app.warmup, daemon=True).start()
    threading.Thread(target=app.state_refresher, daemon=True).start()
    if app.agents is not None:
        app.agents.start()

    print("=" * 48)
    print(" chatgptwebapp2api v%s — ChatGPT web -> OpenAI API" % __version__)
    print(" Base URL : http://localhost:%d/v1" % cfg.port)
    print(" Model    : %s (aur /v1/models me sab dikhenge)" % cfg.default_model)
    print(" Temporary chats: %s (ChatGPT history me save nahi hoga)" % ("ON" if cfg.temporary_chats else "OFF"))
    if app.agents is not None:
        fleet = app.agents.describe()
        lanes = fleet["lanes"]
        print(" Agents   : %d (express %d | standard %d | heavy %d) | lane wait: %gs"
              % (fleet["agents"], lanes["express"]["agents"],
                 lanes["standard"]["agents"], lanes["heavy"]["agents"],
                 cfg.agent_submit_wait_sec))
    print(" Band karne ke liye: Ctrl+C")
    print("=" * 48, flush=True)
    try:
        # 50ms selector wakeup instead of the 500ms default: a fresh client
        # connection is accepted (and its SSE headers flushed) within a few
        # tens of ms even when it arrives between polls
        server.serve_forever(poll_interval=0.05)
    except KeyboardInterrupt:
        print("\nBand ho gaya. Bye!")
    finally:
        app.shutdown()
        server.server_close()
