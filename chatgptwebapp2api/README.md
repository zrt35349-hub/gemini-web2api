# chatgptwebapp2api

ChatGPT web (chatgpt.com) ko **OpenAI-compatible local API** bana deta hai — aapki
browser cookie se chalta hai, koi OpenAI API key nahi chahiye.

- Base URL: `http://localhost:8100/v1`
- Model: `auto` (ya jo `/v1/models` me dikhe — gpt-6, gpt-5-6, gpt-5-3-mini, ...)
- Login cookie na ho to bhi chalta hai (anonymous tier), par login wali cookie
  doge to aapke account ke models milenge.
- Base URL ko browser me kholne par API status dikhega. Models ke liye
  `http://localhost:8100/v1/models` kholo; chat requests
  `POST http://localhost:8100/v1/chat/completions` par bhejo.

## Kaise chalayein

1. `start.bat` double-click karo (ya `python -m chatgptwebapp2api`).
2. Terminal me `[OK] Cookie valid hai — models: ...` dikhe to ready hai.
3. Kisi bhi OpenAI client me base URL set karo:
   - OpenAI Python: `OpenAI(base_url="http://localhost:8100/v1", api_key="x")`
   - curl example neeche.

## Test

```bash
curl http://localhost:8100/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"auto","stream":true,"messages":[{"role":"user","content":"Hello!"}]}'
```

## Cookie kaise dalein / refresh karein

1. Browser me `chatgpt.com` kholo (login hua hua).
2. `F12` -> **Application** tab -> **Cookies** -> `https://chatgpt.com`.
3. Sab cookies ki `name=value` pairs ek hi line me banao:
   `name1=value1; name2=value2; ...`
4. `gpt_cookie.txt` me paste karke server restart karo.

- Login session cookies (`__Secure-next-auth.session-token` / `oai-sc`) sabse
  important hain — bina inke ChatGPT anonymous samajh lega.
- `__cf_bm` / Cloudflare cookies purani ho gayi to bhi refresh karna theek hai.

## Files

| File | Kaam |
|---|---|
| `gpt_cookie.txt` | Aapki ChatGPT cookie (git me commit nahi hogi) |
| `config.json` | Port (8100), host, default model, `temporary_chats`, `api_keys` |
| `start.bat` | One-click start |

## Config options (`config.json`)

- `port` — default `8100` (Gemini wala 8000 pe alag chalta hai)
- `host` — default `127.0.0.1` (sirf aapke PC se). LAN ke liye `0.0.0.0`
- `api_keys` — khali = open. List do to `Authorization: Bearer <key>` lagana hoga
- `temporary_chats` — `true` (default) = ChatGPT history me chat save NAHI hogi
- `default_model` — jab client koi model na de

## Limitations

- Sirf **text** chat (image input/upload abhi nahi)
- Har request nayi ChatGPT conversation banati hai — OpenAI multi-turn history
  prompt ke andar hi flatten hoti hai
- Bahut lambi history me initial project request, latest user request aur system
  instructions ko bachakar recent context fit kiya jaata hai; purane oversized
  replies skip kiye jaate hain. Agar ChatGPT phir bhi `413` de, proxy history ko
  aur compact karke kuch baar retry karta hai. Agar zaroori instructions hi
  limit se bade hon, API `413` error deti hai. Upstream ChatGPT ki apni
  context/output limits phir bhi lagu hoti hain.
- Quota/rate limit wahi hai jo browser me milta hai
