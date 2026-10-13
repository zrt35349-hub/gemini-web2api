# Third-party licenses

Direct runtime dependencies (see `requirements.txt`). Each dependency remains
under its own license; those licenses apply to the dependency, not to this
project's own code (which is MIT — see `LICENSE`).

| Package | Version pinned/tested | License | Source |
|---|---|---|---|
| `httpx` | >= 0.26 (tested 0.28.1) | BSD-3-Clause | https://github.com/encode/httpx |
| `curl_cffi` | >= 0.7 (tested 0.16.3) | MIT | https://github.com/yifeikong/curl_cffi |
| `gemini-webapi` | >= 2.1 (tested 2.1.1) | see its own repository for license terms | https://github.com/HanaokaYuzu/Gemini-API |

Notes:
- `httpx` pulls transitive dependencies (`httpcore`, `anyio`, `certifi`,
  `idna`, `sniffio`, …); `curl_cffi` ships its own TLS backend. All transitive
  dependencies keep their own licenses — consult their distributions.
- Python standard library modules used (`http.server`, `threading`, `queue`,
  `hashlib`, …) are under the Python Software Foundation License.
- Docker images built from `Dockerfile` add base-image/OS layer licenses;
  check the image you deploy.
