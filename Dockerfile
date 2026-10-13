FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# All licenses ride with the image (testing-only distribution — see TESTING-ONLY.md).
COPY LICENSE NOTICE THIRD-PARTY-LICENSES.md TESTING-ONLY.md ./
COPY gemini_web2api ./gemini_web2api
COPY chatgptwebapp2api ./chatgptwebapp2api

# Railway supplies PORT; default 8000 for local docker runs.
# Secrets (PROXY_API_KEY, cookie) are injected via environment/volumes — never baked in.
EXPOSE 8000
CMD ["sh", "-c", "python -m gemini_web2api --host 0.0.0.0 --port ${PORT:-8000}"]
