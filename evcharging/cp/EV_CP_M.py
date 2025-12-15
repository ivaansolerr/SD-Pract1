import socket, sys, time, threading, requests, os
from datetime import datetime
from typing import Dict, Any
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer
import ssl
import os

# IMPORTACIONES PARA GUI (Tkinter)
import tkinter as tk
from tkinter import messagebox

import urllib3 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Variable global para controlar el cierre de hilos de forma limpia
stop_threads = False
TLS_INSECURE = True
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

def make_tls_client_context():
    ctx = ssl.create_default_context()
    if TLS_INSECURE:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx

def handshake(sock, cp, key, name=""):
    try:
        sock.settimeout(5)
        sock.send(b"<ENC>")
        if sock.recv(1024) != socketCommunication.ACK:
            print(f"[MONITOR] {name} no envió ACK tras <ENC>")
            return False

        sock.send(socketCommunication.encodeMess(cp))  # enviamos el cp
        sock.send(socketCommunication.encodeMess(key)) # ahora paso la key
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
    ctx = make_tls_client_context()

    for attempt in range(1, retries + 1):
        if stop_threads:
            return None
        try:
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.settimeout(5)
            raw.connect((ip, port))

            # Envolvemos con TLS
            sock = ctx.wrap_socket(raw, server_hostname=ip)  # server_hostname da igual si TLS_INSECURE=True

            print(f"[MONITOR] 🔒 Conectado TLS a {name} ({ip}:{port})")
            return sock
        except Exception as e:
            print(f"[MONITOR] Intento {attempt}/{retries} falló con {name}: {e}")
            time.sleep(wait)
    return None


def monitorCentral(ipC, pC, cp, ipE, pE, shared_state, key):
    global stop_threads
    sc = None
    while not stop_threads:
        try:
            sc = connectWithRetry(ipC, pC, "CENTRAL", retries=1, wait=3)
            if sc and handshake(sc, cp, key, "CENTRAL"):
                shared_state["sc"] = sc
                print("[MONITOR] ✅ CP validado por CENTRAL")
                break
            else:
                print("[MONITOR] Fallo en handshake con CENTRAL, reintentando...")
        except Exception as e:
            print(f"[MONITOR] Error conectando con CENTRAL: {e}")
        time.sleep(5)

    if stop_threads: return

    # Heartbeat a CENTRAL en bucle
    sc.settimeout(3)
    central_alive = True
    heartbeat_interval = 5

    while not stop_threads:
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
                # Si recibimos otra cosa, lo ignoramos momentáneamente o lanzamos error
                pass 
    
        except (socket.timeout, ConnectionError, OSError):
            if stop_threads: break
            if central_alive:
                print("[MONITOR] ⚠️ CENTRAL no responde, intentando reconectar...")
                try:
                    ctx = make_tls_client_context()
                    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    raw.settimeout(2)
                    raw.connect((ipE, pE))
                    se = ctx.wrap_socket(raw, server_hostname=ipE)
                    se.send(b"CENTRAL_DOWN")
                    se.close()
                    print("[MONITOR] 🚨 Aviso TLS enviado a ENGINE: CENTRAL caída")
                except Exception as e:
                    print("[MONITOR] ENGINE caído (no se pudo avisar):", e)

    
            while not stop_threads:
                sc = connectWithRetry(ipC, pC, "CENTRAL", retries=1, wait=5)
                if sc and handshake(sc, cp, key, "CENTRAL"):
                    print("[MONITOR] ✅ Reconexión exitosa con CENTRAL")
                    shared_state["sc"] = sc # Actualizar socket compartido
                    try:
                        ctx = make_tls_client_context()
                        se = ctx.wrap_socket(socket.socket(), server_hostname=ipE)
                        se.settimeout(1)
                        se.connect((ipE, pE))
                        se.send(b"CENTRAL_UP_AGAIN")
                        se.close()
                    except Exception as e:
                        print("[MONITOR] ENGINE sigue caído (no se pudo avisar):", e)
                    central_alive = True
                    break
                else:
                    print("[MONITOR] Fallo reconectando con CENTRAL, reintentando...")
                    time.sleep(5)
    
        time.sleep(heartbeat_interval)

def monitorEngine(ipE, pE, cp, shared_state, key):
    global stop_threads
    while shared_state["sc"] is None and not stop_threads:
        time.sleep(0.2)  # esperar a que CENTRAL conecte primero

    if stop_threads: return

    sc = shared_state["sc"]
    se = None # Socket engine

    failedAttempts = 0
    ko_sent = False

    while not stop_threads:
        se = connectWithRetry(ipE, pE, "ENGINE", retries=1, wait=2)
        if se and handshake(se, cp, key, "ENGINE"):
            shared_state["se"] = se # Guardamos socket engine por si necesitamos enviar mensaje de baja
            print("[MONITOR] ✅ CP registrado en ENGINE")

            if ko_sent:
                try:
                    if shared_state["sc"]:
                        shared_state["sc"].send(b"OK")
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
                try:
                    if shared_state["sc"]: shared_state["sc"].send(b"KO")
                    print("[MONITOR] ⚠️ KO enviado a CENTRAL")
                    ko_sent = True
                except: pass
            elif failedAttempts >= 5 and not ko_sent:
                try:
                    if shared_state["sc"]: shared_state["sc"].send(b"KO")
                    print("[MONITOR] ⚠️ KO enviado a CENTRAL")
                    ko_sent = True
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
            time.sleep(2)

    # Heartbeat loop con ENGINE
    if not se: return 
    se.settimeout(3)
    heartbeat_interval = 2
    engine_alive = True

    while not stop_threads:
        try:
            se.send(b"PING")
            resp = se.recv(1024)
            if resp != b"PONG":
                raise socket.timeout
            # print("[MONITOR] Conexión con ENGINE: OK") # Comentado para no saturar log
        except (socket.timeout, ConnectionError, OSError):
            if stop_threads: break
            if engine_alive:
                print("[MONITOR] ⚠️ ENGINE no responde → enviar KO a CENTRAL")
                try:
                    if shared_state["sc"]: shared_state["sc"].send(b"KO")
                    print("[MONITOR] KO enviado a CENTRAL por caída del ENGINE")
                except Exception as e:
                    print(f"[MONITOR] Error enviando KO a CENTRAL: {e}")
                engine_alive = False

            # Intentar reconectar ENGINE
            while not stop_threads:
                se = connectWithRetry(ipE, pE, "ENGINE", retries=1, wait=2)
                if se and handshake(se, cp, key, "ENGINE"):
                    print("[MONITOR] ✅ Reconexión exitosa con ENGINE")
                    shared_state["se"] = se
                    try:
                        if shared_state["sc"]: shared_state["sc"].send(b"OK")
                        print("[MONITOR] ✅ OK enviado a CENTRAL: CP recuperado")
                    except Exception as e:
                        print(f"[MONITOR] Error enviando OK a CENTRAL: {e}")
                    engine_alive = True
                    break
                else:
                    print("[MONITOR] Fallo reconectando con ENGINE, reintentando...")
                    time.sleep(3)
        time.sleep(heartbeat_interval)

def darDeBaja(cpId, ipR, portR, shared_state, root):
    global stop_threads
    
    confirm = messagebox.askyesno("Confirmar Baja", f"¿Estás seguro de que deseas eliminar permanentemente el CP {cpId}?")
    if not confirm:
        return

    print(f"[BAJA] Iniciando proceso de baja para {cpId}...")

    # 1) Enviar KO a CENTRAL primero (y esperar con Event)
    done = shared_state.get("central_ko_sent")
    if done is None:
        done = threading.Event()
        shared_state["central_ko_sent"] = done
    else:
        done.clear()

    threading.Thread(
        target=send_ko_to_central,
        args=(shared_state, done),
        daemon=True
    ).start()

    # Esperar a que el hilo intente enviar KO (máx 2s, para no quedarnos colgados)
    done.wait(timeout=2.0)
    
    # 2) Ahora dar de baja por API
    url = f"https://{ipR}:{portR}/deleteCP"
    try:
        response = requests.delete(url, json={"id": cpId}, verify=False, timeout=5)
        
        if response.status_code == 200:
            print("[BAJA] ✅ Eliminado correctamente de la Base de Datos.")
        else:
            print(f"[BAJA] ⚠️ Error borrando de BD: {response.status_code} - {response.text}")
            messagebox.showerror("Error", f"No se pudo borrar de la BD: {response.text}")
            return 
            
    except Exception as e:
        print(f"[BAJA] ❌ Error de conexión con API: {e}")
        messagebox.showerror("Error", f"Error conectando con API: {e}")
        return

    # 3) Parar hilos y limpiar
    stop_threads = True

    if shared_state.get("sc"):
        try:
            print("[BAJA] Cerrando socket CENTRAL...")
            shared_state["sc"].close()
        except Exception as e:
            print(f"[BAJA] Error cerrando socket Central: {e}")

    if shared_state.get("se"):
        try:
            print("[BAJA] Cerrando socket ENGINE...")
            shared_state["se"].close()
        except Exception as e:
            print(f"[BAJA] Error cerrando socket Engine: {e}")

    # 4) Borrar archivo local del CP
    file_path = f"evcharging/cp/{cpId}.txt"
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"[BAJA] ✅ Archivo {file_path} eliminado.")
        except Exception as e:
            print(f"[BAJA] ⚠️ No se pudo eliminar el archivo: {e}")

    messagebox.showinfo("Baja Exitosa", "El CP ha sido dado de baja y el programa se cerrará.")
    root.destroy()
    sys.exit(0)


def reAuth(cpId, ipR, portR, ipC, pC, ipE, pE, shared_state):
    global stop_threads

    key_path = os.path.join(BASE_DIR, "keys", f"{cpId}_key.txt")
    
    if os.path.exists(key_path):
        messagebox.showwarning("Ya existe clave", f"Ya existe una clave local para {cpId}.")
        return
        
    confirm = messagebox.askyesno(
        "Re-auth",
        f"¿Quieres volver a autenticar el CP {cpId} y regenerar las claves?"
    )
    if not confirm:
        return

    print(f"[REAUTH] 🔁 Reautenticando CP {cpId}...")

    fileExists = False
    key = ""

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
        try:
            response = requests.post(url, json=dataForRegistry, verify=False)

            # comprobamos que no exista
            if response.status_code == 409:
                print("ID del CP repetida, introduce una ID que no exista")
                return

            # guardamos la clave
            key = response.json().get("message")

            # Asegurar directorio
            os.makedirs("evcharging/cp", exist_ok=True)
            with open(f"evcharging/cp/{cpId}.txt", "w", encoding="utf-8") as f:
                f.write(key)
        except Exception as e:
            print(f"Error registrando CP: {e}")
            return

    # 2) Parar hilos actuales y cerrar sockets
    stop_threads = True
    try:
        if shared_state.get("sc"):
            try: shared_state["sc"].close()
            except: pass
        if shared_state.get("se"):
            try: shared_state["se"].close()
            except: pass
    finally:
        shared_state["sc"] = None
        shared_state["se"] = None

    time.sleep(0.5)  # mini pausa para que los loops salgan

    # 3) Reiniciar monitor con la nueva key (nuevo handshake)
    stop_threads = False
    threading.Thread(
        target=monitorCentral,
        args=(ipC, pC, cpId, ipE, pE, shared_state, key),
        daemon=True
    ).start()

    threading.Thread(
        target=monitorEngine,
        args=(ipE, pE, cpId, shared_state, key),
        daemon=True
    ).start()

    print("[REAUTH] ✅ Re-auth lanzado. Se repetirá el handshake con CENTRAL y ENGINE.")
    messagebox.showinfo("Re-auth", "Re-autenticación iniciada. Revisa consola para ver el handshake.")


def send_ko_to_central(shared_state, done_event: threading.Event, timeout=2.0):
    try:
        sc = shared_state.get("sc")
        if not sc:
            print("[BAJA] ⚠️ No hay socket a CENTRAL (sc=None). No se pudo enviar KO.")
            return
        sc.sendall(b"KO")
        print("[BAJA] ⚠️ KO enviado a CENTRAL (pre-baja)")
    except Exception as e:
        print(f"[BAJA] ⚠️ No se pudo enviar KO a CENTRAL: {e}")
    finally:
        done_event.set()


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
    key = ""

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
        try:
            response = requests.post(url, json=dataForRegistry, verify=False)

            # comprobamos que no exista
            if response.status_code == 409:
                print("ID del CP repetida, introduce una ID que no exista")
                return

            # guardamos la clave
            key = response.json().get("message")

            # Asegurar directorio
            os.makedirs("evcharging/cp", exist_ok=True)
            with open(f"evcharging/cp/{cpId}.txt", "w", encoding="utf-8") as f:
                f.write(key)
        except Exception as e:
            print(f"Error registrando CP: {e}")
            return

    shared_state = {"sc": None, "se": None, "central_ko_sent": threading.Event()}
    
    # Lanzamos hilo para mantener CENTRAL
    threading.Thread(target=monitorCentral, args=(ipC, pC, cpId, ipE, pE, shared_state, key), daemon=True).start()
    # Lanzamos hilo para mantener ENGINE
    threading.Thread(target=monitorEngine, args=(ipE, pE, cpId, shared_state, key), daemon=True).start()

    # Dar de baja con tkinter el cp
    root = tk.Tk()
    root.title(f"Monitor CP: {cpId}")
    root.geometry("300x180")

    lbl_info = tk.Label(root, text=f"CP ID: {cpId}\nLocation: {cpLocation}\nPrice: {cpPrice} €", pady=10)
    lbl_info.pack()

    btn_frame = tk.Frame(root)
    btn_frame.pack(pady=20)

    # Botón DAR DE BAJA
    btn_baja = tk.Button(
        btn_frame,
        text="DAR DE BAJA",
        bg="red",
        fg="white",
        font=("Arial", 10, "bold"),
        command=lambda: darDeBaja(cpId, ipR, portR, shared_state, root)
    )
    btn_baja.pack(side="left", padx=5)

    btn_reauth = tk.Button(
        btn_frame,
        text="RE-AUTH",
        bg="orange",
        fg="black",
        font=("Arial", 10, "bold"),
        command=lambda: reAuth(
            cpId,
            ipR,
            portR,
            ipC,
            pC,
            ipE,
            pE,
            shared_state
        )
    )
    btn_reauth.pack(side="left", padx=5)

    print(f"[MONITOR] Ventana de control abierta para {cpId}")
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        global stop_threads
        stop_threads = True
        sys.exit()

main()