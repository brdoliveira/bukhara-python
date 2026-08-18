#!/usr/bin/env bash
set -euo pipefail

bootstrap_server="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
topics=(
  order.created
  inventory.reserved
  inventory.rejected
  inventory.release.requested
  inventory.released
  payment.approved
  payment.failed
  notification.sent
  inventory.retry
  payment.retry
  notification.retry
  inventory.dlq
  payment.dlq
  notification.dlq
)

for topic in "${topics[@]}"; do
  /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap_server" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

