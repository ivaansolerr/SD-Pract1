STX = b"\x02"
ETX = b"\x03"
ACK = b"<ACK>"
NACK = b"<NACK>"

# validamos que la transmisión es correcta byte a byte
def xorChecksum(data):
    lrc = 0
    for b in data:
        lrc = lrc ^ b # vamos bit por bit
    return bytes([lrc])

# encodeamos para mandarlo por el socket en base al protocolo definido
def encodeMess(msg):
    data = msg.encode()
    return STX + data + ETX + xorChecksum(data) # data aquí sería el request

def parseFrame(frame):
    if len(frame) < 3 or frame[0] != 2 or frame[-2] != 3: # verifica que la comunicación siga el protocolo
        return None
    data = frame[1:-2] # sacamos los datos
    if xorChecksum(data) == frame[-1:]:
        return data.decode() 
    else:
        return None