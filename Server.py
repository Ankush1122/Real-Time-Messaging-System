import socket
import threading

from chatapp_core.protocol import recv_message, send_message
from chatapp_core.storage import PostgresStorage

HOST = '0.0.0.0'
PORT = 12345
sessions = {}
sessions_lock = threading.Lock()
storage = PostgresStorage()


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
    if not storage.verify_or_create_user(user_id, password):
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


def handle_client(sock, addr):
    user_id = None
    try:
        while True:
            payload = recv_message(sock)
            kind = payload.get('type')
            if kind == 'login':
                user_id = handle_login(sock, payload)
                continue
            if not user_id:
                send_message(sock, {'type': 'error', 'message': 'login required'})
                continue
            if kind == 'send_message':
                receiver_id = str(payload.get('receiver_id', '')).strip()
                text = str(payload.get('text', '')).strip()
                client_message_id = payload.get('client_message_id')
                if not receiver_id or not text:
                    send_message(sock, {'type': 'send_error', 'message': 'receiver_id and text are required'})
                    continue
                saved = storage.insert_message(user_id, receiver_id, text, client_message_id)
                message = storage._row_to_message({'message_id': saved.message_id, 'chat_id': saved.chat_id, 'sender_id': saved.sender_id, 'receiver_id': saved.receiver_id, 'message_text': saved.text, 'inserted_at': saved.created_at})
                delivered = push_to_user(receiver_id, message)
                send_message(sock, {'type': 'send_ack', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'delivered': delivered})
            elif kind == 'fetch_history':
                send_message(sock, {'type': 'history', 'messages': storage.fetch_history(user_id, str(payload.get('peer_id', '')))})
            elif kind == 'list_chats':
                send_message(sock, {'type': 'chat_list', 'chats': storage.list_chats(user_id)})
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
    server.listen(1024)
    print(f'RTMS persistent blocking server listening on {HOST}:{PORT}')
    while True:
        sock, addr = server.accept()
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == '__main__':
    main()
