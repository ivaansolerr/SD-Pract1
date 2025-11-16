import socket, sys, threading, time, ssl
from typing import Dict, Any
from datetime import datetime, timezone
from . import db
from .. import topics, kafka_utils, utils, socketCommunication
from confluent_kafka import Producer, Consumer

import base64
import os
from cryptography.fernet import Fernet

# cuidado porque esto va en el ordenador de los drivers, entonces habría que cambiar la ip en el
# db.py para poder comunicarse bien en caso de que las credenciales haya que guardarlas en la base
# de datos
# las credenciales se deben tener y comprobar en ambos lados? central y cp, después de usar el
# registry

# en caso de que tengamos que guardar en la base de datos se tendría que ver en todo momento
# si el cp existe y realizar las correspondientes confirmaciones

def sslCommunication(ssocket, context):
    with context.wrap_socket(ssocket, server_side=True) as ssock:
        conn, addr = ssock.accept()
        while True:
            print(conn.recv(1024))
            return
            # try:
            #     if conn.recv(1024) != b"<ENC>":
            #         conn.close()
            #         return
                
            #     conn.send(socketCommunication.encodeMess("ACK")) # mandamos la respuesta

            #     cp = socketCommunication.parseFrame(conn.recv(1024)) # sacamos el id del CP

            #     # credential generation
            #     key = Fernet.generate_key()
            #     fernet = Fernet(key)
            #     encMessage = fernet.encrypt(cp.encode())
    
            #     print("encrypted message: ", encMessage)
            #     print("decrypted message: ", fernet.decrypt(encMessage).decode())
                
            #     conn.send(socketCommunication.encodeMess(encMessage)) # mandamos la respuesta

            #     if conn.recv(1024) != b"ACK":
            #         conn.close()
            #         return

            #     conn.send(b"END") # fin de comunicación
            #     print(f"[CENTRAL] Registro comletado con {cp}")
    
            # except Exception as e:
            #     print(f"[CENTRAL] Error con {addr}: {e}")

def main():
    if len(sys.argv) != 2:
        print("Uso: EV_Registry.py <Puerto de escucha>")
        return

    port = int(sys.argv[1])
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.listen(5)

    sslCommunication(s, context)
    # t = threading.Thread(target=sslCommunication, args=(s, context), daemon=True)
    # t.start()

main()