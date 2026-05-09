import socket
import threading
from datetime import datetime, timezone

from chatapp_core.protocol import recv_message, send_message

HOST = '0.0.0.0'
PORT = 12345
sessions = {}
sessions_lock = threading.Lock()


def push_to_user(user_id, payload):
    with sessions_lock:
        sock = sessions.get(str(user_id))
    if not sock:
        return False
    try:
        send_message(sock, payload)
        return True
    except OSError:
        return False


def handle_login(sock, payload):
    user_id = str(payload.get('user_id', '')).strip()
    if not user_id:
        send_message(sock, {'type': 'login_error', 'message': 'user_id is required'})
        return None
    with sessions_lock:
        old = sessions.get(user_id)
        sessions[user_id] = sock
    if old and old is not sock:
        try:
            send_message(old, {'type': 'session_closed', 'reason': 'duplicate_login'})
            old.close()
        except OSError:
            pass
    send_message(sock, {'type': 'login_ok', 'user_id': user_id})
    return user_id


def handle_client(sock, addr):
    user_id = None
    try:
        while True:
            payload = recv_message(sock)
            event_type = payload.get('type')
            if event_type == 'login':
                user_id = handle_login(sock, payload)
                continue
            if not user_id:
                send_message(sock, {'type': 'error', 'message': 'login required'})
                continue
            if event_type == 'send_message':
                receiver_id = str(payload.get('receiver_id', '')).strip()
                text = str(payload.get('text', '')).strip()
                if not receiver_id or not text:
                    send_message(sock, {'type': 'send_error', 'message': 'receiver_id and text are required'})
                    continue
                message = {
                    'type': 'message',
                    'sender_id': user_id,
                    'receiver_id': receiver_id,
                    'text': text,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                }
                delivered = push_to_user(receiver_id, message)
                send_message(sock, {'type': 'send_ack', 'delivered': delivered})
            else:
                send_message(sock, {'type': 'error', 'message': f'unsupported type: {event_type}'})
    except (ConnectionError, OSError, ValueError):
        pass
    finally:
        if user_id:
            with sessions_lock:
                if sessions.get(user_id) is sock:
                    sessions.pop(user_id, None)
        sock.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(512)
    print(f'RTMS v3 listening on {HOST}:{PORT}')
    while True:
        sock, addr = server.accept()
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == '__main__':
    main()
