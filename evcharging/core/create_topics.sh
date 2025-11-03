#!/bin/bash

echo "Creando topics de Kafka para EVCharging..."

TOPICS=(
  "evSupplyRequest"
  "evSupplyAuth"
  "evAuthResult"
  "evSupplyConnected"
  "evSupplyEnd"
  "evSupplyTicket"
  "evSupplyStarted"
  "evSupplyAuthDri"
  "evSupplyHeartbeat"
  "evCentralHeartbeat"
  "evDriverSupplyHeartbeat"
  "evDriverSupplyError"
)

for topic in "${TOPICS[@]}"; do
  echo "Creando topic: $topic"
  docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --if-not-exists
done

echo "✅ Todos los topics han sido creados con éxito."
echo
echo "📋 Lista actual de topics:"
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

