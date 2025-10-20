
STX = b'\x02'
ETX = b'\x03'

def lrc(data: bytes) -> int:
    xor = 0
    for b in data:
        xor ^= b
    return xor

def pack_message(payload: str) -> bytes:
    data = payload.encode('utf-8')
    checksum = lrc(data).to_bytes(1, 'big')
    return STX + data + ETX + checksum

def unpack_message(stream: bytes):
    if not stream or stream[0:1] != STX:
        return None, 0
    try:
        etx_index = stream.index(ETX, 1)
    except ValueError:
        return None, 0
    if etx_index + 1 >= len(stream):
        return None, 0
    data = stream[1:etx_index]
    recv_lrc = stream[etx_index+1:etx_index+2]
    if len(recv_lrc) < 1:
        return None, 0
    if recv_lrc != lrc(data).to_bytes(1, 'big'):
        return None, etx_index+2
    payload = data.decode('utf-8')
    total_len = etx_index + 2
    return payload, total_len
