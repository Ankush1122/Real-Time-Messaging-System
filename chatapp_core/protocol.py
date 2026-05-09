import json
import socket
import struct
from typing import Any, Dict, List, Optional

MAX_FRAME_SIZE = 1024 * 1024
HEADER = struct.Struct('!I')


def encode_message(payload: Dict[str, Any]) -> bytes:
    raw = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    if not raw or len(raw) > MAX_FRAME_SIZE:
        raise ValueError(f'invalid payload size: {len(raw)}')
    return HEADER.pack(len(raw)) + raw


def send_message(sock: socket.socket, payload: Dict[str, Any]) -> None:
    sock.sendall(encode_message(payload))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: List[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError('socket closed while reading frame')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def recv_message(sock: socket.socket) -> Dict[str, Any]:
    header = _recv_exact(sock, HEADER.size)
    (length,) = HEADER.unpack(header)
    if length <= 0 or length > MAX_FRAME_SIZE:
        raise ValueError(f'invalid frame length: {length}')
    raw = _recv_exact(sock, length)
    return json.loads(raw.decode('utf-8'))


class MessageReader:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._expected: Optional[int] = None

    def feed(self, data: bytes):
        self._buffer.extend(data)
        messages = []
        while True:
            if self._expected is None:
                if len(self._buffer) < HEADER.size:
                    break
                self._expected = HEADER.unpack(self._buffer[:HEADER.size])[0]
                del self._buffer[:HEADER.size]
                if self._expected <= 0 or self._expected > MAX_FRAME_SIZE:
                    raise ValueError(f'invalid frame length: {self._expected}')
            if len(self._buffer) < self._expected:
                break
            raw = bytes(self._buffer[:self._expected])
            del self._buffer[:self._expected]
            self._expected = None
            messages.append(json.loads(raw.decode('utf-8')))
        return messages
