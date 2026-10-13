# chatgpt-web2api

ChatGPT ke **web app** (chatgpt.com) ke internal (reverse-engineered) backend ko
**OpenAI-compatible local API** banata hai — tumhari browser cookie se chalta hai,
koi OpenAI API key nahi chahiye. Koi bhi OpenAI client (Cherry Studio, ChatBox,
openai-python SDK, Codex CLI) isse jud sakta hai.

- Base URL: `http://localhost:8100/v1`
- Models: live — `/v1/models` se aate hain (`auto`, `gpt-6`, `gpt-6-mini`,
  `gpt-5-5`, `gpt-5-6`, `gpt-5-5-mini`, `gpt-5-6-mini`, `gpt-5-3-mini`)
- Login cookie na ho to bhi chalta hai (anonymous tier), par login wali cookie
  doge to tumhare account ke models/quota milenge.

> **Testing purposes only — production/fair use ke liye nahi** (dekho `TESTING-ONLY.md`).

## Quick start

```bash
pip install -r requirements.txt   # sirf curl_cffi
python -m chatgptwebapp2api       # http://localhost:8100 par start
# ya: start.bat double-click
```

Verify:

```bash
curl http://localhost:8100/
curl http://localhost:8100/v1/models
curl http://localhost:8100/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"auto","messages":[{"role":"user","content":"Hello!"}]}'
```

## Cookie setup

1. Browser me `chatgpt.com` kholo (login hua hua).
2. `F12` → **Application** tab → **Cookies** → `https://chatgpt.com`.
3. Sab cookies ki `name=value` pairs ek hi line me banao:
   `name1=value1; name2=value2; ...`
4. `chatgptwebapp2api/gpt_cookie.txt` me paste karke server restart karo
   (ya `COOKIE_STRING` env var set karo).

- Login session cookies (`__Secure-next-auth.session-token` / `oai-sc`) sabse
  important hain — bina inke ChatGPT anonymous samjhega.
- Cookie file **hot-reload** hoti hai — file badlo, restart ki zaroorat nahi
  (cookie ke liye; proxy change bhi auto pick hota hai).

**Verify karo:** browser me `http://localhost:8100/` kholo → `"status": "ok"` dikhega.

## Config (`chatgptwebapp2api/config.json`)

| Key | Default | Matlab |
|---|---|---|
| `port` / `host` | 8100 / 127.0.0.1 | server bind |
| `api_keys` | `[]` | empty = no auth; `["sk-x"]` = Bearer/x-api-key/`?key=` auth |
| `default_model` | auto | client model na de to fallback |
| `cookie_file` | gpt_cookie.txt | cookie path |
| `proxy` | null | upstream proxy, e.g. `http://127.0.0.1:7890` (`CHATGPT_PROXY` env / `--proxy` bhi) |
| `temporary_chats` | true | true = ChatGPT history me chat save nahi hogi |
| `request_timeout_sec` | 300 | upstream timeout |
| `log_requests` | true | request logging |

CLI: `--port`, `--host`, `--config`, `--cookie-file`, `--proxy`.
Config search order: `--config` → `CHATGPT_WEB2API_CONFIG` env → `./config.json` →
`~/.config/chatgpt-web2api/config.json`. Env overrides (env wins): `PROXY_API_KEY` /
`PROXY_API_KEYS`, `COOKIE_STRING`, `CHATGPT_PROXY`, `PORT`, tuning vars.

Proxy set ho to startup par `Proxy: http://...` dikhega aur `/health` me
`"proxy"` + `"proxy_active": true` aayega.

## Project layout

```
├── chatgptwebapp2api/            # main package
│   ├── __main__.py               #   CLI entry: python -m chatgptwebapp2api
│   ├── server.py                 #   OpenAI-compatible HTTP server + health/metrics
│   ├── chatgpt.py                #   ★ CORE: backend-api client (conversation/SSE/models)
│   ├── sentinel.py               #   chat-requirements + proof-of-work gate
│   ├── agents.py                 #   lane-scheduled parallel agent fleet
│   ├── limiter.py                #   AIMD admission control
│   ├── config.py                 #   config.json + env + CLI
│   └── gpt_cookie.txt            #   tumhari cookie (gitignored, commit nahi hogi)
├── tests/
│   └── test_chatgpt_web2api.py   #   unit tests (mocked, bina network)
├── Dockerfile / docker-compose.yml
├── requirements.txt              #   sirf curl_cffi
└── start.bat
```

## API

| Endpoint | Kaam |
|---|---|
| `GET /`, `/health` | status + models_cached + proxy + load metrics |
| `GET /v1/models` | live ChatGPT model slugs |
| `POST /v1/chat/completions` | chat (`stream: true/false`) |

Har request nayi ChatGPT conversation kholti hai — OpenAI multi-turn history
prompt ke andar flatten hoti hai. Bahut lambi history auto-compact hoti hai
(project request + latest message preserved); limit se badi ho to clean `413`.

## Tests

```bash
python -m unittest discover -s tests   # sab mocked — bina network
```

## Deployment

**Local:** `pip install -r requirements.txt && python -m chatgptwebapp2api`

**Docker:**
```bash
docker compose up -d          # http://localhost:8100
```

**Railway:**
1. GitHub repo ko Railway se connect karo
   (ya CLI: `npx @railway/cli login` → `railway init` → `railway up`).
2. Railway Variables me set karo:
   - `PROXY_API_KEY` = tumhari private key (.env jaisi, khud generate ki hui)
   - `COOKIE_STRING` = poori Cookie header string
   - `CHATGPT_PROXY` = (optional) upstream proxy URL
3. Deploy hoga Dockerfile se — server `0.0.0.0:$PORT` par bind hota hai.
4. Railway Settings → Networking → Generate Domain → wahi production Base URL hai:
   `https://<railway-domain>/v1`

## Limitations / Risks

- **Sirf text chat** (image input/upload nahi).
- **Single-turn upstream** — multi-turn prompt-embedding se simulate hota hai.
- **Token counts approximate** (`len/4`).
- OpenAI kabhi bhi protocol badal sakta hai → tool toot sakta hai.
  High-frequency use par rate-limit/block.
- Reverse-engineered private endpoint use OpenAI ToS ke daayare se bahar ho
  sakta hai — **personal use me hi rakho.**

## License

- Apna code **MIT License** me hai — dekho `LICENSE`
  (root + `chatgptwebapp2api/LICENSE` me copy hai).
- Trademark/ToS attribution aur user-responsibility notes: `NOTICE`.
- Dependency licenses: `THIRD-PARTY-LICENSES.md`.
- "ChatGPT" (OpenAI) unke owners ke trademarks hain;
  ye project unofficial hai, unse affiliated nahi.
- **Testing purposes only — production/fair use ke liye nahi** (dekho `TESTING-ONLY.md`).
