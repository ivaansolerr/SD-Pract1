@echo off
echo Creando topics de Kafka para EVCharging...

set TOPICS=evSupplyRequest evSupplyAuth evAuthResult evSupplyConnected evSupplyEnd evSupplyTicket evSupplyStarted evSupplyAuthDri  evSupplyHeartbeat evCentralHeartbeat evDriverSupplyHeartbeat evDriverSupplyError

for %%T in (%TOPICS%) do (
    echo Creando topic: %%T
    docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic %%T --bootstrap-server localhost:9092
)

echo Todos los topics han sido creados correctamente.
echo Lista actual de topics:
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

pause