"""python -m chatgptwebapp2api — server start karo."""

import os
import sys

from . import __version__
from .chatgpt import ChatGPTClient
from .config import apply_args, load_config, parse_args
from . import server


def main():
    args = parse_args()
    if getattr(args, "version", False):
        print("chatgptwebapp2api " + __version__)
        return
    # load_config() applies config.json + env/.env overrides; explicit
    # --config wins. parse_args --port/--host/--cookie-file/--proxy win over file.
    cfg = load_config(getattr(args, "config", None))
    cfg = apply_args(cfg, args)

    try:
        cookie = cfg.resolve_cookie()
    except FileNotFoundError:
        cookie = None
    if not cookie:
        print("ERROR: cookie file nahi mila: %s" % cfg.cookie_path)
        print("chatgpt.com kholo -> F12 (DevTools) -> Application -> Cookies -> https://chatgpt.com")
        print("Sab cookie values copy karke ek line me is file me paste karo.")
        print("Ya COOKIE_STRING env var set karo.")
        sys.exit(1)

    client = ChatGPTClient(
        cookie,
        proxy=cfg.proxy,
        timeout=cfg.request_timeout_sec,
        retry_attempts=cfg.retry_attempts,
        retry_delay_sec=cfg.retry_delay_sec,
        max_retry_wait_sec=getattr(cfg, "max_retry_wait_sec", 3.0),
        first_byte_timeout_sec=getattr(cfg, "first_byte_timeout_sec", 15.0),
        stream_read_timeout_sec=getattr(cfg, "stream_read_timeout_sec", 30.0),
    )
    if not client.device_id:
        print("[WARN] Cookie me oai-did nahi mila — chat requests fail ho sakti hain.")
    server.serve(cfg, client)


if __name__ == "__main__":
    main()
