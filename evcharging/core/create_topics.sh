#!/bin/bash

echo "Creando topics de Kafka para EVCharging..."

TOPICS=(
  "evRegister"
  "evHealth"
  "evCommands"
  "evSupplyRequest"
  "evSupplyAuth"
  "evSupplyStart"
  "evSupplyTelemetry"
  "evSupplyDone"
)

for topic in "${TOPICS[@]}"; do
  echo "Creando topic: $topic"
  docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic "$topic" --bootstrap-server localhost:9092
done

echo "Todos los topics han sido creados con éxito."
echo "Lista actual de topics:"
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092