# Definición centralizada de topics Kafka

# --- Registro y autenticación ---
EV_REGISTER = "evRegister"           # CP_M -> Central (alta/registro inicial)
EV_AUTH_REQUEST = "evAuthRequest"    # CP_M -> Central (solicitud de autenticación)
EV_AUTH_RESULT = "evAuthResult"      # Central -> CP_M (resultado autenticación)
EV_AUTH_RESULT_ENG = "evAuthResultEng"    # CP_M -> CP_E(resultado autenticación)

# --- Health & comandos ---
EV_HEALTH = "evHealth"               # CP_M -> Central (heartbeat OK/KO)
EV_COMMANDS = "evCommands"           # Central -> CP_E (STOP_SUPPLY, OUT_OF_ORDER, RESUME)

# --- Ciclo de suministro ---
EV_SUPPLY_REQUEST = "evSupplyRequest"   # Driver -> Central
EV_SUPPLY_AUTH = "evSupplyAuth"         # Central -> Driver
EV_SUPPLY_START = "evSupplyStart"       # Central -> CP_E (inicio de suministro)
EV_SUPPLY_TELEMETRY = "evSupplyTelemetry" # CP_E -> Central (telemetría)
EV_SUPPLY_DONE = "evSupplyDone"         # CP_E -> Central (fin de sesión)
