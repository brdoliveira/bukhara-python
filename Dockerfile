FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/services/order_service:/app/services/inventory_service:/app/services/payment_service:/app/services/notification_service

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir .

