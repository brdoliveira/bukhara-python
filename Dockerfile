FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
    OTEL_LOGS_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=otlp \
    OTEL_TRACES_EXPORTER=otlp \
    PYTHONPATH=/app:/app/services/order_service:/app/services/inventory_service:/app/services/payment_service:/app/services/notification_service

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir .

