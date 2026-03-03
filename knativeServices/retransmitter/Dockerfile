FROM python:3.12-alpine

# Avoid .pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Workdir inside the container
WORKDIR /app

# Install system dependencies (minimal)
# If you run into build issues for any lib, uncomment build-base.
RUN apk add --no-cache \
    ca-certificates \
    && update-ca-certificates
    # && apk add --no-cache --virtual .build-deps build-base

# Install Python dependencies
RUN pip install --no-cache-dir \
    Flask \
    cloudevents \
    requests \
    gunicorn

COPY retransmitter.py ./app.py

# Knative expects the container to listen on port 8080
EXPOSE 8080

# Start the Flask app via gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
