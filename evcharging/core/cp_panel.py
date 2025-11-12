from colorama import Fore, Style
import os
import time
from . import db

def printCpPanel():
    cps = db.listChargingPoints()
    sessions = list(db.sessions.find({}, {"_id": 0, "driver_id": 1, "cp_id": 1}))
    print(f"\n=== Charging Points en Sistema === ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    
    if not cps:
        print("No hay puntos de carga registrados.")
        return

    for cp in cps:
        cp_id = cp.get('id')
        state = (cp.get('state') or 'UNKNOWN').upper()
        price = cp.get('price_eur_kwh', 'N/A')
        location = cp.get('location', 'N/A')

        # Buscar si este CP tiene una sesión activa
        driver_id = None
        if state == "SUPPLYING":
            for s in sessions:
                if s.get("cp_id") == cp_id:
                    driver_id = s.get("driver_id")
                    break

        # Colorear según el estado
        if state in ("AVAILABLE", "SUPPLYING"):
            color = Fore.GREEN
        elif state in ("AVERIEDO", "UNAVAILABLE", "AVERIA"):
            color = Fore.RED
        elif state == "DISCONNECTED":
            color = Style.RESET_ALL
        else:
            color = Fore.YELLOW

        # Construir la línea de salida
        line = (
            f"ID: {cp_id} | "
            f"State: {color}{state}{Style.RESET_ALL} | "
            f"Price: {price} €/kWh | "
            f"Location: {location}"
        )
        if driver_id:
            line += f" | Driver: {Fore.CYAN}{driver_id}{Style.RESET_ALL}"

        print(line)


def auto_refresh_panel(interval=3):
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        printCpPanel()
        time.sleep(interval)


if __name__ == "__main__":
    auto_refresh_panel()
