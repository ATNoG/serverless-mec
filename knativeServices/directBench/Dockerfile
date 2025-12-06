FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache ca-certificates && update-ca-certificates

RUN pip install --no-cache-dir \
    Flask \
    cloudevents \
    gunicorn

COPY direct_bench.py ./app.py

EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
