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

# hay que pillar todos los datos que monitor pille por comando y recibirlos aquí y crear desde aquí los chargingpoints
# si el cp existe y realizar las correspondientes confirmaciones



def main():
    if len(sys.argv) != 2:
        print("Uso: EV_Registry.py <Puerto de escucha>")
        return



main()