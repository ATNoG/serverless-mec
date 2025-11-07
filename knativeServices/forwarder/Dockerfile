# syntax=docker/dockerfile:1
FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder.py ./app.py

# Knative sets $PORT; don't hardcode it.
CMD ["sh", "-c", "exec gunicorn app:app -b :${PORT:-8080} -w 1 --threads 8 --timeout 30"]
