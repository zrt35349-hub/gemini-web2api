"""HTTP server exposing OpenAI + Google native endpoints over the Gemini protocol.

Endpoints:
  GET  /                                                  health JSON
  GET  /v1/models                                         OpenAI model list
  POST /v1/chat/completions                               OpenAI chat (stream + tools + images)
  POST /v1/responses                                      OpenAI Responses API (Codex CLI)
  GET  /v1beta/models                                     Google native model list
  POST /v1beta/models/{model}:generateContent             Google native, non-streaming
  POST /v1beta/models/{model}:streamGenerateContent       Google native, streaming

Built on a bounded worker-pool HTTPServer — no web framework. Chunked request
bodies are decoded manually. Auth (optional): Authorization: Bearer, x-api-key,
x-goog-api-key or ?key= against config.api_keys (empty list = open).

Latency/stability notes:
  - SSE headers flush immediately; upstream events forward as they arrive
  - bounded worker pool + bounded queue shed overload cleanly (429/Retry-After)
  - lane-scheduled agent fleet executes upstream calls in parallel: express
    agents keep small chat requests fast while heavy image/tool requests run
    on their own agents, and a supervisor revives crashed agents
  - reasoning effort (off/low/medium/high/max) maps to upstream thinking depth
"""

import json
import queue
import socket
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__
from .agents import AgentError, PoolBusy, build_pool
from .agents import classify as classify_lane
from .gemini import (GeminiError, RateLimitedError, RetryableError,
                     UpstreamTimeoutError)
from .limiter import AdaptiveConcurrency
from .models import (THINK_SUFFIX_RE, apply_reasoning_effort, apply_thinking_budget,
                     is_known_model, list_model_infos, resolve_model)
from .multimodal import UploadError, detect_image_mime, make_file_name
from .tools import (PromptError, google_contents_to_messages, messages_to_prompt,
                    normalize_tools, parse_tool_calls, resolve_image,
                    responses_input_to_messages)

_CREATED_TS = int(time.time())


class PoolHTTPServer(HTTPServer):
    """HTTP server on a fixed pool of daemon worker threads.

    ThreadingHTTPServer grows a thread per connection with no ceiling — under
    a traffic spike that means thousands of threads and trashed memory. A
    bounded pool keeps footprint flat, reuses threads (no per-connection
    spawn cost), and when the queue is full the connection is dropped
    immediately instead of piling up.
    """

    # accept backlog for bursty connection storms
    request_queue_size = 512

    def __init__(self, address, handler, pool_workers=64, queue_depth=512):
        super().__init__(address, handler)
        self._tasks = queue.Queue(maxsize=queue_depth)
        for _ in range(max(1, int(pool_workers))):
            threading.Thread(target=self._work, daemon=True,
                             name="gemini-web2api-worker").start()

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
    """Wires config + gemini client + uploader together; injectable for tests."""

    def __init__(self, config, gemini=None, uploader=None, image_engine=True):
        self.config = config
        self.limiter = AdaptiveConcurrency(
            initial=getattr(config, "initial_concurrent_requests",
                            getattr(config, "max_concurrent_requests", 8)),
            minimum=getattr(config, "min_concurrent_requests", 1),
            maximum=getattr(config, "max_concurrent_requests", 8),
            adaptive=getattr(config, "adaptive_concurrency", True),
            latency_target_sec=getattr(config, "latency_target_sec", 0.0),
        )
        # Backwards-compatible name: existing call sites talk to app.semaphore.
        # It is now an AIMD gate, not a fixed threading semaphore.
        self.semaphore = self.limiter
        # Parallel agent fleet: every POST route executes its upstream work on
        # a lane-bound agent thread (None = legacy inline execution).
        self.agents = build_pool(config)
        self.agent_submit_wait_sec = max(
            0.0, float(getattr(config, "agent_submit_wait_sec", 0) or 0))
        self.lane_express_chars = int(getattr(config, "lane_express_chars", 4000) or 4000)
        self.lane_heavy_chars = int(getattr(config, "lane_heavy_chars", 60000) or 60000)
        if gemini is None:
            from .gemini import GeminiClient
            gemini = GeminiClient(config)
        try:
            gemini.limiter = self.limiter  # enables capacity-aware hedging
        except Exception:
            pass
        if uploader is None:
            from .multimodal import ImageUploader
            uploader = ImageUploader(gemini)
        self.gemini = gemini
        self.uploader = uploader
        # max seconds a request may wait for a free upstream slot before a
        # 429 + Retry-After is returned (0 = wait forever, legacy behavior)
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
        self.image_engine = None
        if image_engine is True:
            try:
                from .library_engine import LibraryEngine
                if LibraryEngine.available():
                    self.image_engine = LibraryEngine(config, gemini.cookies)
            except Exception:
                self.image_engine = None

    def warmup(self):
        """Prefetch build label + xsrf token in the background; never fatal.

        Both live on the same /app page, so this single fetch also leaves a
        warm TLS connection in the pool for the first real request, and no
        live request ever pays the state-fetch round-trip.
        """
        try:
            self.gemini.refresh_state()
        except Exception as exc:  # transport/ValueError must not kill the daemon
            self._warn(f"state prefetch failed: {exc}")

    def state_refresher(self):
        """Periodically re-fetches build label + xsrf so requests never pay
        the 405/400 recovery path (page fetch + full retry), and keeps a
        pooled connection warm. Runs until process exit; failures are quiet
        (stale state keeps working until upstream actually rejects it)."""
        interval = max(0, int(getattr(self.config, "state_refresh_sec", 300)))
        if not interval:
            return
        while True:
            time.sleep(interval)
            try:
                self.gemini.refresh_state()
            except Exception:
                pass

    @staticmethod
    def _warn(message):
        sys.stderr.write(f"[gemini-web2api] WARN {message}\n")

    def note_request(self, queue_wait_ms=0.0, ttfb_ms=None, total_ms=0.0, ok=True):
        """Records one client request for the /health latency snapshot."""
        alpha = 0.2
        ttfb_ms = float(total_ms if ttfb_ms is None else ttfb_ms)
        with self._metrics_lock:
            self.requests_total += 1
            if not ok:
                self.requests_failed += 1
            self._last_queue_wait_ms = float(queue_wait_ms)
            self._last_total_ms = float(total_ms)
            if not self._latency_samples:
                self._queue_ewma = float(queue_wait_ms)
                self._ttfb_ewma = ttfb_ms
                self._total_ewma = float(total_ms)
            else:
                self._queue_ewma = (1 - alpha) * self._queue_ewma + alpha * queue_wait_ms
                self._ttfb_ewma = (1 - alpha) * self._ttfb_ewma + alpha * ttfb_ms
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

    def upload_images(self, images):
        """image specs -> [ref, filename] pairs for the payload. Raises UploadError (-> 502)."""
        refs = []
        for spec in images or []:
            data, mime = resolve_image(spec, proxy=self.config.proxy)
            mime = mime or detect_image_mime(data)
            if not mime:
                raise UploadError("unsupported image format (magic bytes not recognised)")
            try:
                refs.append((self.uploader.upload(data, mime), make_file_name(mime)))
            except UploadError:
                raise
            except GeminiError:
                raise
            except Exception as exc:
                raise UploadError(str(exc))
        return refs

    def prepare_image_data(self, images):
        """image specs -> [(bytes, mime)] for the library engine."""
        pairs = []
        for spec in images or []:
            data, mime = resolve_image(spec, proxy=self.config.proxy)
            mime = mime or detect_image_mime(data)
            if not mime:
                raise UploadError("unsupported image format (magic bytes not recognised)")
            pairs.append((data, mime))
        return pairs

    def complete_with_images(self, prompt, images, model_info):
        """Full-text generation for image prompts via the best available engine."""
        if self.image_engine is not None:
            return self.image_engine.generate(prompt, self.prepare_image_data(images))
        return self.gemini.generate(prompt, self.upload_images(images), model_info)

    def images_via_engine(self, images):
        """True when image requests should take the library-engine path."""
        return bool(images) and self.image_engine is not None


def _usage(prompt, text):
    pt = max(1, len(prompt) // 4)
    ct = max(1, len(text) // 4)
    return {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct}


def _chat_chunk(cid, created, model, delta, finish=None):
    return {
        "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }


def _tc_wire(call, index):
    return {"id": call["id"], "type": "function", "index": index,
            "function": {"name": call["function"]["name"],
                         "arguments": call["function"]["arguments"]}}


def _resp_object(rid, created, model, output, prompt, text):
    usage = _usage(prompt, text)
    return {
        "id": rid, "object": "response", "created_at": created, "status": "completed",
        "model": model, "output": output,
        "usage": {"input_tokens": usage["prompt_tokens"],
                  "output_tokens": usage["completion_tokens"],
                  "total_tokens": usage["total_tokens"]},
        "metadata": {},
    }


def _resp_message_item(text, status="completed"):
    return {"type": "message", "id": "msg_" + uuid.uuid4().hex[:24], "status": status,
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}]}


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
        server_version = f"gemini-web2api/{__version__}"
        _response_started = False

        # ------------------------------------------------ plumbing

        def setup(self):
            super().setup()
            self._sse_lock = threading.Lock()
            self._stream_stop = threading.Event()
            self._first_byte_ts = None
            self._keepalive_thread = None
            self._upstream_error = None

        def log_message(self, fmt, *args):
            if app.config.log_requests:
                sys.stderr.write("[{}] {} {}\n".format(
                    self.log_date_time_string(), self.address_string(), fmt % args))

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

        def _send_error_json(self, status, message, err_type="invalid_request_error",
                             code=None, headers=None):
            self._send_json({"error": {"message": message, "type": err_type, "code": code}},
                            status, headers=headers)

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
            self._start_keepalive(content_type)

        def _start_keepalive(self, content_type):
            """Periodic SSE comments keep proxies/clients from timing out on a
            slow upstream. Only for text/event-stream (NDJSON must stay pure)."""
            interval = float(getattr(app.config, "sse_keepalive_sec", 0) or 0)
            if interval <= 0 or "text/event-stream" not in (content_type or ""):
                return
            self._stream_stop.clear()
            thread = threading.Thread(target=self._keepalive_loop, args=(interval,),
                                      name="sse-keepalive", daemon=True)
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

        def _sse(self, payload, event=None):
            data = payload if isinstance(payload, str) else json.dumps(
                payload, separators=(",", ":"))
            chunk = (f"event: {event}\n" if event else "") + f"data: {data}\n\n"
            with self._sse_lock:
                self.wfile.write(chunk.encode("utf-8"))
                self.wfile.flush()

        def _sse_stream_error(self, exc):
            """In-stream error for clients after headers/role chunk went out."""
            self._sse({"error": {"message": str(exc),
                                 "type": "upstream_error", "code": "upstream_error"}})
            self._sse("[DONE]")

        def _read_body(self):
            limit = int(app.config.max_body_mb * 1_000_000)
            te = (self.headers.get("Transfer-Encoding") or "").lower()
            if "chunked" in te:
                data = bytearray()
                while True:
                    size_line = self.rfile.readline(65536).strip()
                    if b";" in size_line:
                        size_line = size_line.split(b";", 1)[0]
                    try:
                        size = int(size_line or b"0", 16)
                    except ValueError:
                        return bytes(data)
                    if size == 0:
                        self.rfile.readline(65536)  # trailing CRLF
                        return bytes(data)
                    remaining = size
                    while remaining > 0:
                        part = self.rfile.read(min(remaining, 65536))
                        if not part:
                            return bytes(data)
                        data.extend(part)
                        remaining -= len(part)
                        if len(data) > limit:
                            raise BodyTooLargeError()
                    self.rfile.readline(65536)  # CRLF after each chunk
            length = int(self.headers.get("Content-Length") or 0)
            if length > limit:
                raise BodyTooLargeError()
            data = bytearray()
            while len(data) < length:
                part = self.rfile.read(length - len(data))
                if not part:
                    break
                data.extend(part)
            return bytes(data)

        # ------------------------------------------------ auth

        def _api_key(self):
            auth = self.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                return auth[7:].strip()
            for name in ("x-api-key", "x-goog-api-key"):
                value = self.headers.get(name)
                if value:
                    return value.strip()
            query = parse_qs(urlparse(self.path).query)
            if query.get("key"):
                return query["key"][0]
            return None

        def _authorized(self):
            keys = app.config.api_keys
            if not keys:
                return True
            return self._api_key() in keys

        # ------------------------------------------------ verbs

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type, x-api-key, x-goog-api-key")
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

        # ------------------------------------------------ routing

        def _route_get(self):
            path = urlparse(self.path).path
            if path in ("/", "/health"):
                cookie_state = "not set"
                cookies = getattr(app.gemini, "cookies", None)
                if cookies is not None:
                    cookie_state = ("loaded" if any(cookies.get())
                                    else "file empty")
                self._send_json({
                    "status": "ok", "version": __version__,
                    "default_model": app.config.default_model,
                    "models": len(list_model_infos()),
                    "cookie": cookie_state,
                    "images": (app.image_engine.status()
                               if app.image_engine is not None
                               else "native engine (pip install gemini-webapi for best results)"),
                    "load": app.load_snapshot(),
                })
                return
            if path == "/favicon.ico":
                # browsers request this unprompted on every page visit; a
                # 204 keeps 401 noise out of otherwise-clean request logs
                self.send_response(204)
                self.end_headers()
                return
            if not self._authorized():
                self._send_error_json(401, "invalid API key", "authentication_error")
                return
            if path == "/v1/models":
                data = [{"id": m.name, "object": "model", "created": _CREATED_TS,
                         "owned_by": "gemini-web2api"} for m in list_model_infos()]
                self._send_json({"object": "list", "data": data})
            elif path == "/v1beta/models":
                models = [{"name": f"models/{m.name}", "displayName": m.name,
                           "description": m.description or m.name,
                           "supportedGenerationMethods": ["generateContent",
                                                          "streamGenerateContent"]}
                          for m in list_model_infos()]
                self._send_json({"models": models})
            else:
                self._send_error_json(404, f"not found: {path}")

        def _route_post(self):
            path = unquote(urlparse(self.path).path)
            if not self._authorized():
                self._send_error_json(401, "invalid API key", "authentication_error")
                return
            try:
                raw = self._read_body()
            except BodyTooLargeError:
                self._send_error_json(
                    413, f"request body exceeds {app.config.max_body_mb:g} MB limit",
                    "request_too_large")
                return
            try:
                data = json.loads(raw.decode("utf-8") or "{}")
            except ValueError:
                self._send_error_json(400, "request body is not valid JSON")
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
                    # parking the request behind a 30s queue (that queue wait is
                    # exactly what users see as "17s latency")
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
                        # express jobs never queue behind heavy image/tool work
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
                except PromptError:
                    pressure = None  # client error: no upstream pressure
                    raise
                except RateLimitedError:
                    pressure = "rate_limit"
                    raise
                except RetryableError as exc:
                    pressure = "stall" if exc.kind in ("stall", "timeout") else "error"
                    raise
                except (GeminiError, TimeoutError):
                    pressure = "error"
                    raise
                except Exception:
                    pressure = "error"
                    raise
            except PromptError as exc:
                self._fail(400, str(exc))
            except UploadError as exc:
                self._fail(502, f"image upload failed: {exc}", "upstream_error")
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
                self._fail(502, f"agent failure: {exc}", "upstream_error")
            except GeminiError as exc:
                self._fail(502, str(exc), "upstream_error")
            except Exception as exc:  # noqa: BLE001 - last-resort guard
                self._fail(500, f"internal error: {exc}", "internal_error")
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
            if path == "/v1/chat/completions":
                return lambda: self._chat_completions(data)
            if path == "/v1/responses":
                return lambda: self._responses(data)
            if path.startswith("/v1beta/models/") and ":" in path:
                model_path, action = path.rsplit(":", 1)
                model = model_path.split("/v1beta/models/", 1)[1]
                if action == "generateContent":
                    return lambda: self._google_generate(data, model, stream=False)
                if action == "streamGenerateContent":
                    return lambda: self._google_generate(data, model, stream=True)
                self._send_error_json(404, f"unknown action :{action}")
                return None
            self._send_error_json(404, f"not found: {path}")
            return None

        def _resolve_model_or_error(self, requested):
            """Strict model validation: unknown explicit names -> 400 (config-gated)."""
            name = (requested or "").strip()
            if name:
                clean = name
                m = THINK_SUFFIX_RE.search(clean)
                if m:
                    clean = clean[: m.start()]
                if not is_known_model(clean):
                    if app.config.strict_models:
                        supported = ", ".join(sorted(m.name for m in list_model_infos()))
                        self._send_error_json(
                            400, f"model '{name}' is not supported. Supported models: {supported}",
                            "invalid_request_error", "model_not_found")
                        return None
                    return resolve_model(name)  # legacy silent fallback
            return resolve_model(name or app.config.default_model)

        def _fail(self, status, message, err_type="invalid_request_error", headers=None):
            if self._response_started:
                # headers already flushed (SSE) — nothing to do but hang up
                self.close_connection = True
                return
            self._send_error_json(status, message, err_type, headers=headers)

        # ------------------------------------------------ OpenAI chat

        def _chat_completions(self, data):
            model_info = self._resolve_model_or_error(data.get("model"))
            if model_info is None:
                return
            model_info = apply_reasoning_effort(model_info, data.get("reasoning_effort"))
            messages = data.get("messages") or []
            if not messages:
                self._send_error_json(400, "messages[] is required")
                return
            tools = normalize_tools(data.get("tools"))
            prompt, images = messages_to_prompt(messages, tools=tools or None,
                                                tool_choice=data.get("tool_choice", "auto"))
            engine_images = app.images_via_engine(images)
            file_refs = [] if engine_images else (app.upload_images(images) if images else [])
            cid = "chatcmpl-" + uuid.uuid4().hex[:24]
            created = int(time.time())
            wants_stream = bool(data.get("stream"))

            if tools or engine_images:
                # full response is needed to parse tool_call blocks; image
                # requests via the library engine are non-streamed as well
                if engine_images:
                    text = app.complete_with_images(prompt, images, model_info)
                    clean, calls = parse_tool_calls(text) if tools else (text, [])
                else:
                    text = app.gemini.generate(prompt, file_refs, model_info)
                    clean, calls = parse_tool_calls(text)
                if wants_stream:
                    self._start_stream("text/event-stream; charset=utf-8")
                    self._sse(_chat_chunk(cid, created, model_info.name,
                                          {"role": "assistant", "content": ""}))
                    if calls:
                        self._sse(_chat_chunk(
                            cid, created, model_info.name,
                            {"tool_calls": [_tc_wire(c, i) for i, c in enumerate(calls)]}))
                        finish = "tool_calls"
                    else:
                        self._sse(_chat_chunk(cid, created, model_info.name,
                                              {"content": clean}))
                        finish = "stop"
                    self._sse(_chat_chunk(cid, created, model_info.name, {}, finish))
                    self._sse("[DONE]")
                    return
                message = {"role": "assistant", "content": clean or None}
                if calls:
                    message["tool_calls"] = [_tc_wire(c, i) for i, c in enumerate(calls)]
                self._send_json({
                    "id": cid, "object": "chat.completion", "created": created,
                    "model": model_info.name,
                    "choices": [{"index": 0, "message": message,
                                 "finish_reason": "tool_calls" if calls else "stop"}],
                    "usage": _usage(prompt, clean or json.dumps(calls)),
                })
                return

            if wants_stream:
                # headers + role chunk go out immediately — the client sees a
                # live stream instead of blocking on the first upstream token
                gen = app.gemini.stream_events(prompt, file_refs, model_info)
                self._start_stream("text/event-stream; charset=utf-8")
                self._sse(_chat_chunk(cid, created, model_info.name,
                                      {"role": "assistant", "content": ""}))
                pieces = []

                def _emit(event):
                    kind, delta = event
                    if kind == "thought":
                        self._sse(_chat_chunk(cid, created, model_info.name,
                                              {"reasoning_content": delta}))
                    else:
                        pieces.append(delta)
                        self._sse(_chat_chunk(cid, created, model_info.name,
                                              {"content": delta}))

                try:
                    for event in gen:
                        _emit(event)
                except GeminiError as exc:
                    # upstream failed after the stream opened — surface in-band
                    self._upstream_error = exc
                    self._sse_stream_error(exc)
                    return
                text = "".join(pieces)
                final = _chat_chunk(cid, created, model_info.name, {}, "stop")
                final["usage"] = _usage(prompt, text)
                self._sse(final)
                self._sse("[DONE]")
            else:
                reasoning = []
                pieces = []
                for kind, delta in app.gemini.stream_events(prompt, file_refs, model_info):
                    (reasoning if kind == "thought" else pieces).append(delta)
                text = "".join(pieces)
                message = {"role": "assistant", "content": text}
                if reasoning:
                    message["reasoning_content"] = "".join(reasoning)
                self._send_json({
                    "id": cid, "object": "chat.completion", "created": created,
                    "model": model_info.name,
                    "choices": [{"index": 0, "message": message,
                                 "finish_reason": "stop"}],
                    "usage": _usage(prompt, text),
                })

        # ------------------------------------------------ OpenAI Responses

        def _responses(self, data):
            model_info = self._resolve_model_or_error(data.get("model"))
            if model_info is None:
                return
            reasoning = data.get("reasoning")
            effort = reasoning.get("effort") if isinstance(reasoning, dict) else None
            model_info = apply_reasoning_effort(model_info, effort)
            messages = responses_input_to_messages(data.get("input"),
                                                   data.get("instructions"))
            tools = normalize_tools(data.get("tools"))
            prompt, images = messages_to_prompt(messages, tools=tools or None,
                                                tool_choice=data.get("tool_choice", "auto"))
            engine_images = app.images_via_engine(images)
            file_refs = [] if engine_images else (app.upload_images(images) if images else [])
            rid = "resp_" + uuid.uuid4().hex[:24]
            created = int(time.time())

            if tools or engine_images:
                if engine_images:
                    text = app.complete_with_images(prompt, images, model_info)
                    clean, calls = parse_tool_calls(text) if tools else (text, [])
                else:
                    text = app.gemini.generate(prompt, file_refs, model_info)
                    clean, calls = parse_tool_calls(text)
                output = []
                if clean:
                    output.append(_resp_message_item(clean))
                for call in calls:
                    output.append({"type": "function_call",
                                   "id": "fc_" + uuid.uuid4().hex[:24],
                                   "call_id": call["id"],
                                   "name": call["function"]["name"],
                                   "arguments": call["function"]["arguments"],
                                   "status": "completed"})
                resp_obj = _resp_object(rid, created, model_info.name, output, prompt,
                                        clean or json.dumps(calls))
                if data.get("stream"):
                    self._stream_responses_tools(resp_obj)
                else:
                    self._send_json(resp_obj)
                return

            gen = app.gemini.stream_generate(prompt, file_refs, model_info)
            if data.get("stream"):
                self._stream_responses_text(rid, created, model_info.name, prompt, gen)
            else:
                text = "".join(gen)
                self._send_json(_resp_object(
                    rid, created, model_info.name,
                    [_resp_message_item(text)] if text else [], prompt, text))

        def _stream_responses_text(self, rid, created, model, prompt, gen):
            msg_id = "msg_" + uuid.uuid4().hex[:24]
            seq = [0]

            def ev(etype, extra):
                payload = {"type": etype, "sequence_number": seq[0]}
                seq[0] += 1
                payload.update(extra)
                self._sse(payload, event=etype)

            self._start_stream("text/event-stream; charset=utf-8")
            ev("response.created", {"response": {"id": rid, "object": "response",
                                                 "created_at": created, "status": "in_progress",
                                                 "model": model, "output": []}})
            ev("response.in_progress", {"response": {"id": rid, "status": "in_progress"}})
            item = {"type": "message", "id": msg_id, "status": "in_progress",
                    "role": "assistant", "content": []}
            ev("response.output_item.added", {"output_index": 0, "item": item})
            part = {"type": "output_text", "text": "", "annotations": []}
            ev("response.content_part.added",
               {"item_id": msg_id, "output_index": 0, "content_index": 0, "part": part})

            pieces = []
            try:
                for delta in gen:
                    pieces.append(delta)
                    ev("response.output_text.delta",
                       {"item_id": msg_id, "output_index": 0, "content_index": 0,
                        "delta": delta, "logprobs": []})
            except GeminiError as exc:
                self._upstream_error = exc
                ev("response.failed",
                   {"response": {"id": rid, "object": "response", "status": "failed",
                                 "error": {"message": str(exc),
                                           "type": "upstream_error"}}})
                return
            text = "".join(pieces)

            ev("response.output_text.done",
               {"item_id": msg_id, "output_index": 0, "content_index": 0, "text": text})
            ev("response.content_part.done",
               {"item_id": msg_id, "output_index": 0, "content_index": 0,
                "part": {"type": "output_text", "text": text, "annotations": []}})
            done_item = {"type": "message", "id": msg_id, "status": "completed",
                         "role": "assistant",
                         "content": [{"type": "output_text", "text": text,
                                      "annotations": []}]}
            ev("response.output_item.done", {"output_index": 0, "item": done_item})
            resp_obj = _resp_object(rid, created, model, [done_item] if text else [],
                                    prompt, text)
            ev("response.completed", {"response": resp_obj})

        def _stream_responses_tools(self, resp_obj):
            seq = [0]

            def ev(etype, extra):
                payload = {"type": etype, "sequence_number": seq[0]}
                seq[0] += 1
                payload.update(extra)
                self._sse(payload, event=etype)

            self._start_stream("text/event-stream; charset=utf-8")
            ev("response.created", {"response": {"id": resp_obj["id"],
                                                 "object": "response",
                                                 "created_at": resp_obj["created_at"],
                                                 "status": "in_progress",
                                                 "model": resp_obj["model"],
                                                 "output": []}})
            ev("response.in_progress", {"response": {"id": resp_obj["id"],
                                                     "status": "in_progress"}})
            for index, item in enumerate(resp_obj["output"]):
                ev("response.output_item.added", {"output_index": index, "item": item})
                if item["type"] == "function_call":
                    ev("response.function_call_arguments.done",
                       {"item_id": item["id"], "output_index": index,
                        "arguments": item["arguments"]})
                ev("response.output_item.done", {"output_index": index, "item": item})
            ev("response.completed", {"response": resp_obj})

        # ------------------------------------------------ Google native

        def _google_generate(self, data, model, stream):
            model = (model or "").rsplit("/", 1)[-1]
            model_info = self._resolve_model_or_error(model)
            if model_info is None:
                return
            gen_config = data.get("generationConfig") or data.get("generation_config") or {}
            think_cfg = (gen_config.get("thinkingConfig")
                         or gen_config.get("thinking_config") or {})
            budget = (think_cfg.get("thinkingBudget")
                      if think_cfg.get("thinkingBudget") is not None
                      else think_cfg.get("thinking_budget"))
            if budget is not None:
                model_info = apply_thinking_budget(model_info, budget)
            contents = data.get("contents") or []
            si = data.get("systemInstruction") or data.get("system_instruction")
            tools_raw = data.get("tools")
            tool_config = data.get("toolConfig") or data.get("tool_config")
            messages, images, tool_choice = google_contents_to_messages(
                contents, si, tool_config)
            prompt, images2 = messages_to_prompt(
                messages, tools=normalize_tools(tools_raw) or None,
                tool_choice=tool_choice, google_format=True)
            all_images = images + images2
            engine_images = app.images_via_engine(all_images)
            file_refs = ([] if engine_images else
                         (app.upload_images(all_images) if all_images else []))

            alt_sse = parse_qs(urlparse(self.path).query).get("alt", [""])[0] == "sse"

            def gusage(text):
                usage = _usage(prompt, text)
                return {"promptTokenCount": usage["prompt_tokens"],
                        "candidatesTokenCount": usage["completion_tokens"],
                        "totalTokenCount": usage["total_tokens"]}

            def emit(obj):
                if stream:
                    self._sse(obj) if alt_sse else self.wfile.write(
                        (json.dumps(obj) + "\n").encode("utf-8"))
                else:
                    self._send_json(obj)

            def full_text():
                if engine_images:
                    return app.complete_with_images(prompt, all_images, model_info)
                return app.gemini.generate(prompt, file_refs, model_info)

            if tools_raw or engine_images:
                if stream:
                    self._start_stream("text/event-stream" if alt_sse
                                       else "application/x-ndjson")
                text = full_text()
                calls = (parse_tool_calls(text, google_format=True)[1]
                         if tools_raw else [])
                if calls:
                    parts = [{"functionCall": {"name": c["name"], "args": c["args"]}}
                             for c in calls]
                else:
                    parts = [{"text": text}]
                emit({"candidates": [{"content": {"parts": parts, "role": "model"},
                                      "finishReason": "STOP", "index": 0,
                                      "safetyRatings": []}],
                      "usageMetadata": gusage(text),
                      "modelVersion": model_info.name})
                return

            if stream:
                content_type = ("text/event-stream" if alt_sse
                                else "application/x-ndjson")
                self._start_stream(content_type)
                gen = app.gemini.stream_generate(prompt, file_refs, model_info)

                def emit_delta(delta):
                    emit({"candidates": [{"content": {"parts": [{"text": delta}],
                                                      "role": "model"}, "index": 0}],
                          "modelVersion": model_info.name})

                pieces = []
                try:
                    for delta in gen:
                        pieces.append(delta)
                        emit_delta(delta)
                except GeminiError as exc:
                    self._upstream_error = exc
                    emit({"error": {"code": 502, "message": str(exc),
                                    "status": "UPSTREAM_ERROR"}})
                    return
                text = "".join(pieces)
                emit({"candidates": [{"content": {"parts": [], "role": "model"},
                                      "finishReason": "STOP", "index": 0}],
                      "usageMetadata": gusage(text),
                      "modelVersion": model_info.name})
            else:
                text = app.gemini.generate(prompt, file_refs, model_info)
                emit({"candidates": [{"content": {"parts": [{"text": text}],
                                                  "role": "model"},
                                      "finishReason": "STOP", "index": 0,
                                      "safetyRatings": []}],
                      "usageMetadata": gusage(text),
                      "modelVersion": model_info.name})

    return Handler


def serve(config):
    """Blocking entrypoint: builds the app, starts the HTTP server, serves forever."""
    app = App(config)
    handler = make_handler(app)
    server = PoolHTTPServer(
        (config.host, config.port), handler,
        pool_workers=max(8, int(getattr(config, "pool_workers", 128))),
        queue_depth=max(16, int(getattr(config, "pool_queue_depth", 1024))))
    threading.Thread(target=app.warmup, daemon=True).start()
    threading.Thread(target=app.state_refresher, daemon=True).start()
    if app.agents is not None:
        app.agents.start()

    auth = "API key" if config.api_keys else "open (no auth)"
    print(f"gemini-web2api v{__version__} listening on http://{config.host}:{config.port}")
    print(f"  endpoints : /v1/chat/completions, /v1/responses, /v1beta/models/*, /v1/models")
    print(f"  reasoning : reasoning_effort=off|low|medium|high|max or model@think=off..max")
    cookie_wired = bool(getattr(config, "cookie_file", None)
                        or getattr(config, "cookie_string", None))
    print(f"  default   : {config.default_model} | auth: {auth}"
          + (" | cookie: yes" if cookie_wired else " | cookie: no (anonymous)"))
    if app.agents is not None:
        fleet = app.agents.describe()
        lanes = fleet["lanes"]
        print(f"  agents    : {fleet['agents']} (express {lanes['express']['agents']}"
              f" | standard {lanes['standard']['agents']}"
              f" | heavy {lanes['heavy']['agents']})"
              f" | lane wait: {config.agent_submit_wait_sec:g}s")
    try:
        # 50ms selector wakeup instead of the 500ms default: a fresh client
        # connection is accepted (and its SSE headers flushed) within a few
        # tens of ms even when it arrives between polls
        server.serve_forever(poll_interval=0.05)
    except KeyboardInterrupt:
        pass
    finally:
        if app.agents is not None:
            app.agents.stop()
        server.server_close()
