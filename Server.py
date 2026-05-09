import hashlib
import socket
import threading
from collections import defaultdict
from datetime import datetime, timezone

from chatapp_core.protocol import recv_message, send_message

HOST = '0.0.0.0'
PORT = 12345
sessions = {}
users = {}
sessions_lock = threading.Lock()
users_lock = threading.Lock()
history = defaultdict(list)
history_lock = threading.Lock()


def hash_password(password):
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def verify_or_create_user(user_id, password):
    with users_lock:
        expected = users.get(user_id)
        if expected is None:
            users[user_id] = hash_password(password)
            return True
        return expected == hash_password(password)


def chat_key(a, b):
    return tuple(sorted((str(a), str(b))))


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
    password = str(payload.get('password', '')).strip()
    if not user_id or not password:
        send_message(sock, {'type': 'login_error', 'message': 'user_id and password are required'})
        return None
    if not verify_or_create_user(user_id, password):
        send_message(sock, {'type': 'login_error', 'message': 'invalid credentials'})
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


def list_chats(user_id):
    rows = []
    with history_lock:
        for key, messages in history.items():
            if user_id not in key or not messages:
                continue
            peer = key[1] if key[0] == user_id else key[0]
            rows.append({'peer_id': peer, 'last_message': messages[-1]})
    return sorted(rows, key=lambda item: item['last_message']['created_at'], reverse=True)


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
                message = {'type': 'message', 'message_id': len(history[chat_key(user_id, receiver_id)]) + 1, 'sender_id': user_id, 'receiver_id': receiver_id, 'text': text, 'created_at': datetime.now(timezone.utc).isoformat()}
                with history_lock:
                    history[chat_key(user_id, receiver_id)].append(message)
                delivered = push_to_user(receiver_id, message)
                send_message(sock, {'type': 'send_ack', 'message_id': message['message_id'], 'delivered': delivered})
            elif event_type == 'fetch_history':
                peer_id = str(payload.get('peer_id', '')).strip()
                with history_lock:
                    messages = history[chat_key(user_id, peer_id)][-50:]
                send_message(sock, {'type': 'history', 'peer_id': peer_id, 'messages': messages})
            elif event_type == 'list_chats':
                send_message(sock, {'type': 'chat_list', 'chats': list_chats(user_id)})
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
    print(f'RTMS v6 listening on {HOST}:{PORT}')
    while True:
        sock, addr = server.accept()
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == '__main__':
    main()
