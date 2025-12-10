import socket, sys, time, threading, requests
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

import urllib3 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def handshake(sock, cp, key, name=""):
    try:
        sock.settimeout(5)
        sock.send(b"<ENC>")
        if sock.recv(1024) != socketCommunication.ACK:
            print(f"[MONITOR] {name} no envió ACK tras <ENC>")
            return False

        sock.send(socketCommunication.encodeMess(key)) # aquí pasaba el cp, ahora paso la key
        if sock.recv(1024) != socketCommunication.ACK:
            print(f"[MONITOR] {name} no envió ACK tras CP_ID")
            return False

        ans = socketCommunication.parseFrame(sock.recv(1024))
        if ans != "OK":
            print(f"[MONITOR] {name} rechazó autenticación ({ans})")
            return False

        sock.send(socketCommunication.ACK)
        if sock.recv(1024) != b"<EOT>":
            print(f"[MONITOR] {name} no envió <EOT>")
            return False

        return True
    except socket.timeout:
        print(f"[MONITOR] Timeout durante handshake con {name}")
        return False
    except Exception as e:
        print(f"[MONITOR] Error en handshake con {name}: {e}")
        return False

def connectWithRetry(ip, port, name, retries=5, wait=3):
    for attempt in range(1, retries + 1):
        try:
            sock = socket.socket()
            sock.settimeout(5)
            sock.connect((ip, port))
            print(f"[MONITOR] Conectado a {name} ({ip}:{port})")
            return sock
        except Exception as e:
            print(f"[MONITOR] Intento {attempt}/{retries} falló con {name}: {e}")
            time.sleep(wait)
    return None

def monitorCentral(ipC, pC, cp, ipE, pE, shared_state, key):
    sc = None
    while True:
        try:
            sc = connectWithRetry(ipC, pC, "CENTRAL", retries=1, wait=3)
            if sc and handshake(sc, cp, "CENTRAL", key):
                shared_state["sc"] = sc
                print("[MONITOR] ✅ CP validado por CENTRAL")
                break
            else:
                print("[MONITOR] Fallo en handshake con CENTRAL, reintentando...")
        except Exception as e:
            print(f"[MONITOR] Error conectando con CENTRAL: {e}")
        time.sleep(5)

    # Heartbeat a CENTRAL en bucle
    sc.settimeout(3)
    central_alive = True
    heartbeat_interval = 5

    while True:
        try:
            sc.sendall(b"PING")
            resp = sc.recv(1024)
    
            if resp != b"PONG":
                try:
                    resp2 = sc.recv(1024)
                    resp = resp2
                except:
                    pass
                
            if not resp:
                raise ConnectionError("socket closed")
            if resp != b"PONG":
                raise ConnectionError(f"unexpected reply: {resp!r}")
    
        except (socket.timeout, ConnectionError, OSError):
            if central_alive:
                print("[MONITOR] ⚠️ CENTRAL no responde, intentando reconectar...")
                try:
                    se = socket.socket()
                    se.settimeout(1)
                    se.connect((ipE, pE))
                    se.send(b"CENTRAL_DOWN")
                    print("[MONITOR] 🚨 Aviso enviado a ENGINE: CENTRAL caída")
                except Exception as e:
                    print("[MONITOR] ENGINE caído (no se pudo avisar):", e)
                central_alive = False
    
            while True:
                sc = connectWithRetry(ipC, pC, "CENTRAL", retries=1, wait=5)
                if sc and handshake(sc, cp, "CENTRAL"):
                    print("[MONITOR] ✅ Reconexión exitosa con CENTRAL")
                    try:
                        se = socket.socket()
                        se.settimeout(1)
                        se.connect((ipE, pE))
                        se.send(b"CENTRAL_UP_AGAIN")
                    except Exception as e:
                        print("[MONITOR] ENGINE sigue caído (no se pudo avisar):", e)
                    central_alive = True
                    break
                else:
                    print("[MONITOR] Fallo reconectando con CENTRAL, reintentando...")
                    time.sleep(5)
    
        time.sleep(heartbeat_interval)

def monitorEngine(ipE, pE, cp, shared_state):
    while shared_state["sc"] is None:
        time.sleep(0.2)  # esperar a que CENTRAL conecte primero

    sc = shared_state["sc"]

    failedAttempts = 0
    ko_sent = False

    while True:
        se = connectWithRetry(ipE, pE, "ENGINE", retries=1, wait=2)
        if se and handshake(se, cp, "ENGINE"):
            print("[MONITOR] ✅ CP registrado en ENGINE")

            if ko_sent:
                try:
                    sc.send(b"OK")
                    print("[MONITOR] ✅ OK enviado a CENTRAL: CP recuperado")
                    ko_sent = False
                except Exception as e:
                    print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")

            failedAttempts = 0
            break
        else:
            failedAttempts += 1
            print(f"[MONITOR] Fallo handshake/conexión con ENGINE ({failedAttempts}/5)")
            if failedAttempts == 1 and not ko_sent:
                sc.send(b"KO")
                print("[MONITOR] ⚠️ KO enviado a CENTRAL")
                ko_sent = True
            elif failedAttempts >= 5 and not ko_sent:
                try:
                    sc.send(b"KO")
                    print("[MONITOR] ⚠️ KO enviado a CENTRAL")
                    ko_sent = True
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
            time.sleep(2)

    # Heartbeat loop con ENGINE
    se.settimeout(3)
    heartbeat_interval = 2
    engine_alive = True

    while True:
        try:
            se.send(b"PING")
            resp = se.recv(1024)
            if resp != b"PONG":
                raise socket.timeout
            print("[MONITOR] Conexión con ENGINE: OK")
        except (socket.timeout, ConnectionError, OSError):
            if engine_alive:
                print("[MONITOR] ⚠️ ENGINE no responde → enviar KO a CENTRAL")
                try:
                    sc.send(b"KO")
                    print("[MONITOR] KO enviado a CENTRAL por caída del ENGINE")
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
                engine_alive = False

            # Intentar reconectar ENGINE
            while True:
                se = connectWithRetry(ipE, pE, "ENGINE", retries=1, wait=2)
                if se and handshake(se, cp, "ENGINE"):
                    print("[MONITOR] ✅ Reconexión exitosa con ENGINE")
                    try:
                        sc.send(b"OK")
                        print("[MONITOR] ✅ OK enviado a CENTRAL: CP recuperado")
                    except Exception as e:
                        print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")
                    engine_alive = True
                    break
                else:
                    print("[MONITOR] Fallo reconectando con ENGINE, reintentando...")
                    time.sleep(3)
        time.sleep(heartbeat_interval)

def main():
    if len(sys.argv) != 10:
        print("Uso: monitor.py <ipCentral> <portCentral> <ipEngine> <portEngine> <CP_ID> <CP_Location> <CP_Price> <IpRegistry> <PortRegistry>")
        return

    ipC = sys.argv[1]
    pC = int(sys.argv[2])
    ipE = sys.argv[3]
    pE = int(sys.argv[4])

    cpId = sys.argv[5] # CP id
    cpLocation = sys.argv[6] # CP location
    cpPrice = float(sys.argv[7]) # CP Price

    ipR = sys.argv[8] # IP registry
    portR = sys.argv[9]

    # si ya existe el archivo key no hacemos el request otra vez
    fileExists = False

    try:
        with open(f"evcharging/cp/{cpId}.txt", "r", encoding="utf-8") as f:
            fileExists = True
            key = (f.read())
    except:
        pass

    if not fileExists:
        dataForRegistry = {
            "id": cpId,
            "location": cpLocation,
            "price": cpPrice
        }

        # url de la api request
        url = f"https://{ipR}:{portR}/addCP"
        response = requests.post(url, json=dataForRegistry, verify=False)

        # comprobamos que no exista
        if response.status_code == 409:
            print("ID del CP repetida, introduce una ID que no exista")
            return

        # guardamos la clave
        key = response.json().get("message")

        with open(f"evcharging/cp/{cpId}.txt", "w", encoding="utf-8") as f:
            f.write(key)

    shared_state = {"sc": None}
    
    # Tenemos ya el key para autenticar en central simpre guardado

    #Lanzamos hilo para mantener CENTRAL
    threading.Thread(target=monitorCentral, args=(ipC, pC, cpId, ipE, pE, shared_state, key), daemon=True).start()
    # Lanzamos hilo para mantener ENGINE (requiere socket CENTRAL activo)
    # threading.Thread(target=monitorEngine, args=(ipE, pE, cpId, shared_state), daemon=True).start()

    # Bucle principal (solo mantiene el proceso vivo)
    while True:
       time.sleep(1)

main()