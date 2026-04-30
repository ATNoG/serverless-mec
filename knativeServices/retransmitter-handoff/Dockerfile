FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache ca-certificates && update-ca-certificates

RUN pip install --no-cache-dir \
    Flask \
    cloudevents \
    requests \
    gunicorn \
    kubernetes

COPY retransmitter_handoff.py ./app.py

EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "-t", "300", "app:app"]
