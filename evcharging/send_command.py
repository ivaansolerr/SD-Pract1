from evcharging import kafka_utils, topics
import sys

if len(sys.argv) != 4:
    print("Uso: python send_command.py <broker> <cp_id> <cmd>")
    print("Ejemplo: python send_command.py 192.168.1.42:9092 CP_1 KO")
    sys.exit(1)

broker = sys.argv[1]          # Ej: 192.168.1.42:9092
cp_id = sys.argv[2]           # Ej: CP_1
cmd = sys.argv[3].upper()     # Ej: KO / RESUME / STOP_SUPPLY

prod = kafka_utils.build_producer(broker)

kafka_utils.send(prod, topics.EV_COMMANDS, {
    "cp_id": cp_id,
    "cmd": cmd
})

print(f"[CMD] Enviado comando '{cmd}' a CP '{cp_id}' en broker {broker}")
