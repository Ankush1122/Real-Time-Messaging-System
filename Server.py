import socket
import threading
from datetime import datetime, timezone

from chatapp_core.protocol import recv_message, send_message

HOST = '0.0.0.0'
PORT = 12345
clients = set()
clients_lock = threading.Lock()


def broadcast(sender, payload):
    stale = []
    with clients_lock:
        targets = list(clients)
    for client in targets:
        if client is sender:
            continue
        try:
            send_message(client, payload)
        except OSError:
            stale.append(client)
    if stale:
        with clients_lock:
            for client in stale:
                clients.discard(client)


def handle_client(sock, addr):
    with clients_lock:
        clients.add(sock)
    send_message(sock, {'type': 'welcome', 'message': 'connected to RTMS v2'})
    try:
        while True:
            payload = recv_message(sock)
            text = str(payload.get('text', '')).strip()
            if not text:
                send_message(sock, {'type': 'error', 'message': 'text is required'})
                continue
            broadcast(sock, {
                'type': 'message',
                'from': f'{addr[0]}:{addr[1]}',
                'text': text,
                'created_at': datetime.now(timezone.utc).isoformat(),
            })
            send_message(sock, {'type': 'ack'})
    except (ConnectionError, OSError, ValueError):
        pass
    finally:
        with clients_lock:
            clients.discard(sock)
        sock.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(256)
    print(f'RTMS v2 listening on {HOST}:{PORT}')
    while True:
        sock, addr = server.accept()
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == '__main__':
    main()
