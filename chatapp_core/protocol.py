import json
import socket
import struct
from typing import Any, Dict, Optional

MAX_FRAME_SIZE = 1024 * 1024
HEADER_STRUCT = struct.Struct('!I')


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError('Socket closed while receiving data')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def encode_message(payload: Dict[str, Any]) -> bytes:
    raw = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    if len(raw) > MAX_FRAME_SIZE:
        raise ValueError('Payload too large')
    return HEADER_STRUCT.pack(len(raw)) + raw


def send_message(sock: socket.socket, payload: Dict[str, Any]) -> None:
    sock.sendall(encode_message(payload))


def recv_message(sock: socket.socket) -> Dict[str, Any]:
    header = _recv_exact(sock, HEADER_STRUCT.size)
    (size,) = HEADER_STRUCT.unpack(header)
    if size <= 0 or size > MAX_FRAME_SIZE:
        raise ValueError(f'Invalid frame size: {size}')
    raw = _recv_exact(sock, size)
    return json.loads(raw.decode('utf-8'))


class MessageReader:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._expected_length: Optional[int] = None

    def feed(self, data: bytes):
        self._buffer.extend(data)
        messages = []
        while True:
            if self._expected_length is None:
                if len(self._buffer) < HEADER_STRUCT.size:
                    break
                self._expected_length = HEADER_STRUCT.unpack(self._buffer[:HEADER_STRUCT.size])[0]
                del self._buffer[:HEADER_STRUCT.size]
                if self._expected_length <= 0 or self._expected_length > MAX_FRAME_SIZE:
                    raise ValueError(f'Invalid frame size: {self._expected_length}')
            if len(self._buffer) < self._expected_length:
                break
            raw = bytes(self._buffer[:self._expected_length])
            del self._buffer[:self._expected_length]
            self._expected_length = None
            messages.append(json.loads(raw.decode('utf-8')))
        return messages
