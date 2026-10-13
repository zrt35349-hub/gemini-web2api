"""Library-backed engine for file-attached (image) generation.

Google's bot detection treats file-attached StreamGenerate requests much more
strictly than text-only ones; as of Oct 2026 they need the full browser-session
dance (f.sid, session-bound upload, Chrome TLS via curl_cffi) that the
maintained `gemini-webapi` package implements. This engine delegates image
requests to that package while the rest of the server uses the native engine.

Requires: pip install gemini-webapi  +  a cookie file with __Secure-1PSID.
"""

import asyncio
import base64
import io
import os
import re
import threading

from .gemini import GeminiError
from .multimodal import make_file_name


_CITE_RE = re.compile(r"\s*\[cite: \d+\]")


class LibraryEngine:
    def __init__(self, config, cookies):
        self._config = config
        self._cookies = cookies
        self._client = None
        self._client_mtime = None
        self._loop = None
        self._lock = threading.Lock()

    # ------------------------------------------------ availability

    @staticmethod
    def available():
        try:
            import gemini_webapi  # noqa: F401
            return True
        except ImportError:
            return False

    def _psid_pair(self):
        if self._cookies is None:
            return None, None
        cookie, _ = self._cookies.get()
        psid = re.search(r"__Secure-1PSID=([^;]+)", cookie)
        if not psid:
            return None, None
        psidts = re.search(r"__Secure-1PSIDTS=([^;]+)", cookie)
        return psid.group(1).strip(), (psidts.group(1).strip() if psidts else None)

    @staticmethod
    def _cookie_jar(cookie):
        """Parses a Cookie header into a {name: value} dict.

        Google's user-status RPC rejects sessions presenting only
        __Secure-1PSID/__Secure-1PSIDTS; the full browser jar authenticates.
        """
        jar = {}
        for part in cookie.split(";"):
            name, sep, value = part.strip().partition("=")
            if sep and name and value:
                jar[name] = value
        return jar

    def _cookie_mtime(self):
        if self._cookies is None or not getattr(self._cookies, "path", None):
            return None
        try:
            return os.path.getmtime(self._cookies.path)
        except OSError:
            return None

    def status(self):
        if not self.available():
            return "engine missing (pip install gemini-webapi)"
        psid, _ = self._psid_pair()
        if not psid:
            return "needs __Secure-1PSID cookie"
        return "ready"

    # ------------------------------------------------ session management

    def _ensure_client(self):
        with self._lock:
            mtime = self._cookie_mtime()
            if self._client is not None:
                if mtime == self._client_mtime:
                    return
                self._client = None  # cookie file changed -> rebuild with fresh credentials
            if not self.available():
                raise GeminiError("gemini-webapi package is not installed "
                                  "(pip install gemini-webapi)")
            psid, psidts = self._psid_pair()
            if not psid:
                raise GeminiError("image requests need a cookie file with __Secure-1PSID")
            try:
                from gemini_webapi import GeminiClient as LibClient
                cookie, _ = self._cookies.get()
                client = LibClient(psid, psidts)
                client.cookies = self._cookie_jar(cookie)
                if self._loop is None:
                    self._loop = asyncio.new_event_loop()
                    thread = threading.Thread(target=self._loop.run_forever, daemon=True)
                    thread.start()
                self._run(client.init(), timeout=180)
                status = getattr(getattr(client, "account_status", None), "name", "")
                if status and status != "AVAILABLE":
                    raise GeminiError(f"image engine session rejected "
                                      f"(account status: {status})")
            except GeminiError:
                raise
            except Exception as exc:
                raise GeminiError(f"image engine init failed: {exc}")
            self._client = client
            self._client_mtime = mtime

    def _run(self, coro, timeout):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=timeout)
        except asyncio.TimeoutError:
            fut.cancel()
            raise GeminiError("image engine timed out")
        except GeminiError:
            raise
        except Exception as exc:
            raise GeminiError(str(exc))

    # ------------------------------------------------ public API

    def generate(self, prompt, images):
        """images: [(bytes, mime)] -> model text (single final answer).

        Uploads happen here with a proper image filename (so Gemini treats the
        file as an image, not a text attachment); generation goes through the
        library's internal _generate with req_file_data.
        """
        self._ensure_client()

        async def _generate():
            from gemini_webapi.utils.upload_file import upload_file
            file_data = []
            for data, mime in images:
                filename = make_file_name(mime)
                ref = await upload_file(io.BytesIO(data),
                                        client=self._client._live_client,
                                        push_id=self._client.push_id,
                                        filename=filename)
                file_data.append([[ref], filename])
            out = None
            async for chunk in self._client._generate(
                    prompt=prompt,
                    req_file_data=file_data,
                    session_state={"last_texts": {}, "last_thoughts": {}}):
                out = chunk
            return out

        output = self._run(_generate(), timeout=self._config.request_timeout_sec + 60)
        text = getattr(output, "text", None)
        if not text:
            raise GeminiError("image request returned an empty response")
        return _CITE_RE.sub("", text).strip()
