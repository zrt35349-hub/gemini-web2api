FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# All licenses ride with the image (testing-only distribution — see TESTING-ONLY.md).
COPY LICENSE NOTICE THIRD-PARTY-LICENSES.md TESTING-ONLY.md ./
COPY chatgptwebapp2api ./chatgptwebapp2api

# Railway supplies PORT as an env var (read by the app itself);
# default 8100 for local docker runs.
# Secrets (PROXY_API_KEY, cookie) are injected via environment/volumes — never baked in.
EXPOSE 8100
CMD ["python", "-m", "chatgptwebapp2api", "--host", "0.0.0.0"]
