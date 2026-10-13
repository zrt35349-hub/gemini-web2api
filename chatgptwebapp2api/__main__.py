"""python -m chatgptwebapp2api — server start karo."""

import argparse
import os
import sys

from . import __version__
from .chatgpt import ChatGPTClient
from .config import Config
from . import server


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="ChatGPT web -> OpenAI-compatible API")
    ap.add_argument("--config", default=os.path.join(here, "config.json"))
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--host", default=None)
    ap.add_argument("--version", action="version", version="chatgptwebapp2api " + __version__)
    args = ap.parse_args()

    cfg = Config(args.config)
    if args.port:
        cfg.port = args.port
    if args.host:
        cfg.host = args.host

    try:
        cookie = cfg.load_cookie()
    except FileNotFoundError:
        print("ERROR: cookie file nahi mila: %s" % cfg.cookie_path)
        print("chatgpt.com kholo -> F12 (DevTools) -> Application -> Cookies -> https://chatgpt.com")
        print("Sab cookie values copy karke ek line me is file me paste karo.")
        sys.exit(1)
    if not cookie:
        print("ERROR: cookie file khali hai: %s" % cfg.cookie_path)
        sys.exit(1)

    client = ChatGPTClient(cookie, proxy=cfg.proxy, timeout=cfg.request_timeout_sec)
    if not client.device_id:
        print("[WARN] Cookie me oai-did nahi mila — chat requests fail ho sakti hain.")
    server.serve(cfg, client)


if __name__ == "__main__":
    main()
