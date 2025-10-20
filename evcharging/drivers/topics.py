# Definición centralizada de topics Kafka
EV_REGISTER = "ev.register"           # CP_M -> Central (alta/estado inicial)
EV_HEALTH = "ev.health"               # CP_M -> Central (heartbeat OK/KO, avería, recovery)
EV_COMMANDS = "ev.commands"           # Central -> CP_E (ordenes OUT_OF_ORDER, RESUME, START_SUPPLY, STOP_SUPPLY)

EV_SUPPLY_REQUEST = "ev.supply.request"   # Driver -> Central
EV_SUPPLY_AUTH = "ev.supply.auth"         # Central -> Driver (OK/KO + motivo)
EV_SUPPLY_START = "ev.supply.start"       # Central -> CP_E (orden de empezar)
EV_SUPPLY_TELEMETRY = "ev.supply.telemetry" # CP_E -> Central (kW, €)
EV_SUPPLY_DONE = "ev.supply.done"         # CP_E -> Central (ticket final)