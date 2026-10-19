FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    google-genai \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp-proto-http \
    psutil \
    httpx

# Copy the server
COPY server.py .

# Expose web UI port
EXPOSE 5601

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:5601/api/config')" || exit 1

CMD ["python3", "server.py"]
