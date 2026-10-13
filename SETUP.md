# SETUP — step-by-step guide

## 1. Install

```bash
cd web2api
pip install httpx        # sirf ye ek dependency (streaming ke liye)
```

## 2. Run

```bash
python -m gemini_web2api             # default: http://0.0.0.0:8000
# ya options ke saath:
python -m gemini_web2api --port 8000 --cookie-file cookie.txt
```

Anonymous mode me turant chal jayega (Flash models). Verify:

```bash
curl http://localhost:8000/
# {"status": "ok", ...}

curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"Hi"}]}'
```

## 3. (Recommended) Cookie setup

Anonymous works, lekin cookies se: rate-limit kam, Pro routing possible,
image upload reliable. **Personal/family Google account use karo.**

### Method A — browser DevTools (2 minute)

1. Chrome me `gemini.google.com` kholo aur login karo.
2. DevTools → **Network** tab → koi bhi request select karo → **Request Headers**
   me `Cookie:` ki poori value copy karo (F12 → Application → Cookies se bhi le
   sakte ho, par full header string sabse aasan hai).
3. File banao `cookie.txt` (project folder me):
   ```
   SID=...; HSID=...; SSID=...; APISID=...; SAPISID=...; __Secure-1PSID=...
   ```
   ya JSON format:
   ```json
   {"cookie": "SID=...; SAPISID=...", "sapisid": "..."}
   ```
   `sapisid` JSON me na do to `SAPISID=...` cookie string se auto-extract hota hai.
4. `config.json` me set karo: `"cookie_file": "cookie.txt"` (ya CLI `--cookie-file`).

Cookie file **mtime-cached** hai — file badlo, server restart ki zaroorat nahi.

**Verify karo:** browser me `http://localhost:8000/` kholo → `"cookie": "loaded"` dikhega.
Images ke liye `"images": "ready"` bhi dikhna chahiye (install: `pip install gemini-webapi`).

### Multiple Google accounts

Agar tum `google.com/u/N/` (N≠0) par Gemini use karte ho, to config me
`"auth_user": N` set karo — URL aur headers dono adjust ho jayenge.

### Optional: xsrf token

Cookie hone par `SNlM0e` token page se **auto-fetch** hota hai (400 errors par
auto-refresh bhi). Haath se dena ho to `"xsrf_token": "..."` config me.

## 4. Config file

```bash
cp config.example.json config.json
```

| Key | Default | Matlab |
|---|---|---|
| `port` / `host` | 8000 / 0.0.0.0 | server bind |
| `api_keys` | `[]` | empty = no auth; `["sk-x"]` = Bearer/x-api-key/`?key=` auth |
| `default_model` | gemini-3.6-flash | unknown model ka fallback |
| `cookie_file` | null | cookie.txt / cookie.json path |
| `retry_attempts` / `retry_delay_sec` | 3 / 2 | upstream fail par retry (exponential backoff ke saath) |
| `request_timeout_sec` | 180 | upstream timeout |
| `temporary_chats` | false | true = history account me save nahi hogi |
| `log_requests` | true | request logging (stderr) |
| `agents_enabled` | true | parallel agent fleet on/off (false = sab inline) |
| `agents_express` | 6 | ultra-fast lane ke agents (chhoti chat requests) |
| `agents_standard` | 20 | normal traffic agents |
| `agents_heavy` | 6 | images / tools / bade payloads ke agents |
| `agent_queue_depth` | 256 | per-lane queue; bharne par clean 429 + Retry-After |
| `agent_submit_wait_sec` | 5 | lane slot ke liye max wait (0 = infinite) |

Config search order: `--config` flag → `GEMINI_WEB2API_CONFIG` env →
`./config.json` → `~/.config/gemini-web2api/config.json`.

## 5. Clients jodna

**Cherry Studio / ChatBox / koi bhi OpenAI app**

- API Host / Base URL: `http://localhost:8000/v1`
- API Key: kuch bhi (auth off hai to)
- Model: `gemini-3.6-flash`, `gemini-3.5-flash-thinking`, ...

**openai-python SDK**

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8000/v1", api_key="anything")
r = client.chat.completions.create(
    model="gemini-3.6-flash",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)
for chunk in r:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

**Codex CLI** (Responses API)

```bash
export OPENAI_BASE_URL=http://localhost:8000/v1
export OPENAI_API_KEY=anything
codex --model gemini-3.6-flash
```

**Gemini CLI** (Google native)

```bash
export GEMINI_API_KEY=anything
# config me endpoint: http://localhost:8000
gemini -m gemini-3.6-flash
```

## 6. Docker

```bash
cp config.example.json config.json
docker compose up -d          # http://localhost:8000
```

Agar upstream 403/429 de (Docker Desktop NAT IP ranges Gemini reject karta hai):
Linux par `network_mode: host` use karo.

## 7. Tests

```bash
python -m unittest discover -s tests     # 100+ tests — sab mocked, bina network
```

## 8. Troubleshooting

| Symptom | Matlab / Fix |
|---|---|
| `Could not extract build label` | gemini.google.com tumhare network se nahi khul raha ya page layout badal gaya |
| `upstream 405` | build label expire — auto-refresh hota hai; baar-baar aaye to server restart |
| `upstream 400` | xsrf token — cookie ke saath auto-refresh hota hai; kai baar fail ho to cookies dobara copy karo |
| `upstream 403` | cookies missing/rejected — naya cookie copy karo, ya anonymous raho |
| `upstream 429` | rate limit — cookies lagao, `retry_delay_sec` badhao |
| Image upload fails | images ke liye cookie zaroori (`__Secure-1PSID` cookie.txt me) + `pip install gemini-webapi`; `http://localhost:8000/` par `"images": "ready"` hona chahiye |
| Pro model Flash jaisa behave karta hai | expected — Pro ke liye Gemini Advanced cookie chahiye |
| Streaming ek hi chunk me | tools use kar rahe ho ya image bheji hai — full response chahiye (by design) |

## 9. Security notes

- `cookie.txt` = tumhara Google session. **Use/share mat karo**, `.gitignore` me already hai.
- Server ko public internet par kholo mat (`host: 0.0.0.0` + `api_keys` empty = sabke
  liye open). LAN ke liye `api_keys` set karo, ya `host: 127.0.0.1` rakho.
- Ye reverse-engineered endpoint hai — Google ToS ke daayare se bahar ho sakta hai.
  Personal use ke liye hi banao, commercial/abusive use nahi.
