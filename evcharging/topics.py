# Definición centralizada de topics Kafka
EV_REGISTER = "evRegister"           # CP_M -> Central (alta/estado inicial)
EV_HEALTH = "evHealth"               # CP_M -> Central (heartbeat OK/KO, avería, recovery)
EV_COMMANDS = "evCommands"           # Central -> CP_E (ordenes OUT_OF_ORDER, RESUME, START_SUPPLY, STOP_SUPPLY)
EV_SUPPLY_REQUEST = "evSupplyRequest"   # Driver -> Central
EV_SUPPLY_AUTH = "evSupplyAuth"         # Central -> Driver (OK/KO + motivo)
EV_SUPPLY_START = "evSupplyStart"       # Central -> CP_E (orden de empezar)
EV_SUPPLY_TELEMETRY = "evSupplyTelemetry" # CP_E -> Central (kW, €)
EV_SUPPLY_DONE = "evSupplyDone"         # CP_E -> Central (ticket final)