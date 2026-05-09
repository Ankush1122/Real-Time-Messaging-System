import json
import socket
import threading
from datetime import datetime, timezone

HOST = '0.0.0.0'
PORT = 12345
clients = set()
clients_lock = threading.Lock()


def send_line(sock, payload):
    data = json.dumps(payload, separators=(',', ':')).encode('utf-8') + b'\n'
    sock.sendall(data)


def broadcast(sender, payload):
    stale = []
    with clients_lock:
        targets = list(clients)
    for client in targets:
        if client is sender:
            continue
        try:
            send_line(client, payload)
        except OSError:
            stale.append(client)
    if stale:
        with clients_lock:
            for client in stale:
                clients.discard(client)


def handle_client(sock, addr):
    with clients_lock:
        clients.add(sock)
    send_line(sock, {'type': 'welcome', 'message': 'connected to RTMS v1'})
    file = sock.makefile('rb')
    try:
        for raw in file:
            try:
                payload = json.loads(raw.decode('utf-8'))
            except json.JSONDecodeError:
                send_line(sock, {'type': 'error', 'message': 'invalid json'})
                continue
            text = str(payload.get('text', '')).strip()
            if not text:
                send_line(sock, {'type': 'error', 'message': 'text is required'})
                continue
            broadcast(sock, {
                'type': 'message',
                'from': f'{addr[0]}:{addr[1]}',
                'text': text,
                'created_at': datetime.now(timezone.utc).isoformat(),
            })
            send_line(sock, {'type': 'ack'})
    finally:
        with clients_lock:
            clients.discard(sock)
        sock.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(256)
    print(f'RTMS v1 listening on {HOST}:{PORT}')
    while True:
        sock, addr = server.accept()
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == '__main__':
    main()
