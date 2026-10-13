# Third-party licenses

Direct runtime dependencies (see `requirements.txt`). Each dependency remains
under its own license; those licenses apply to the dependency, not to this
project's own code (which is MIT — see `LICENSE`).

| Package | Version pinned/tested | License | Source |
|---|---|---|---|
| `curl_cffi` | >= 0.7 (tested 0.16.3) | MIT | https://github.com/yifeikong/curl_cffi |

Notes:
- `curl_cffi` ships its own TLS backend. All transitive
  dependencies keep their own licenses — consult their distributions.
- Python standard library modules used (`http.server`, `threading`, `queue`,
  `hashlib`, …) are under the Python Software Foundation License.
- Docker images built from `Dockerfile` add base-image/OS layer licenses;
  check the image you deploy.
