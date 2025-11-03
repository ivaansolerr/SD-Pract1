EV_SUPPLY_REQUEST = "evSupplyRequest"       # Driver -> Central (solicitud de recarga)

EV_SUPPLY_AUTH_DRI = "evSupplyAuthDri"      # Central -> Driver (autorización) 
EV_SUPPLY_STARTED = "evSupplyStarted"       # Central -> Driver (Comienza el supply driver) 
EV_SUPPLY_TICKET = "evSupplyTicket"         # Central -> Driver (ticket final con precio)


EV_SUPPLY_CONNECTED = "evSupplyConnected" # Engine -> Central (Comienza el supply engine) 
EV_SUPPLY_AUTH = "evSupplyAuth" # Engine -> Central (iniciar o rechazar)
EV_SUPPLY_END = "evSupplyEnd"   # Engine -> Central (fin del supply, trigger para que central mande el ticket)