@echo off
echofd Creando topics de Kafka para EVCharging...

set TOPICS=ev.register ev.health ev.commands ev.supply.request ev.supply.auth ev.supply.start ev.supply.telemetry ev.supply.done

for %%T in (%TOPICS%) do (
    echo Creando topic: %%T
    docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic %%T --bootstrap-server localhost:9092
)

echo Todos los topics han sido creados con éxito.
echo Lista actual de topics:
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

pause