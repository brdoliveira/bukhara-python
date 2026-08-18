#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
topics=(
  orders.events
  inventory.events
  payments.events
  notifications.events
  inventory.retry.1
  inventory.retry.2
  inventory.retry.3
  payment.retry.1
  payment.retry.2
  payment.retry.3
  notification.retry.1
  notification.retry.2
  notification.retry.3
  inventory.dlq
  payment.dlq
  notification.dlq
)

for topic in "${topics[@]}"; do
  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_server" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

