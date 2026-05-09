import selectors
import socket
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from chatapp_core.protocol import MessageReader, encode_message
from chatapp_core.storage import PostgresStorage

HOST = '0.0.0.0'
PORT = 12345


@dataclass
class Session:
    sock: socket.socket
    addr: tuple
    reader: MessageReader = field(default_factory=MessageReader)
    outbox: bytearray = field(default_factory=bytearray)
    user_id: Optional[str] = None


class SelectorChatServer:
    def __init__(self, host=HOST, port=PORT):
        self.selector = selectors.DefaultSelector()
        self.host = host
        self.port = port
        self.sessions_by_socket: Dict[socket.socket, Session] = {}
        self.sessions_by_user: Dict[str, Session] = {}
        self.storage = PostgresStorage()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(False)
        server.bind((self.host, self.port))
        server.listen(2048)
        self.selector.register(server, selectors.EVENT_READ, self.accept)
        print(f'RTMS selector server listening on {self.host}:{self.port}')
        while True:
            for key, mask in self.selector.select(timeout=1):
                callback = key.data
                callback(key.fileobj, mask)

    def accept(self, server, mask):
        sock, addr = server.accept()
        sock.setblocking(False)
        session = Session(sock=sock, addr=addr)
        self.sessions_by_socket[sock] = session
        self.selector.register(sock, selectors.EVENT_READ, self.on_socket_ready)

    def on_socket_ready(self, sock, mask):
        session = self.sessions_by_socket.get(sock)
        if not session:
            return
        if mask & selectors.EVENT_READ:
            try:
                data = sock.recv(65536)
                if not data:
                    self.close(session)
                    return
                for payload in session.reader.feed(data):
                    self.handle_payload(session, payload)
            except Exception:
                self.close(session)
                return
        if mask & selectors.EVENT_WRITE:
            try:
                sent = sock.send(session.outbox)
                del session.outbox[:sent]
            except OSError:
                self.close(session)
                return
        self.update_interest(session)

    def queue(self, session: Session, payload: Dict):
        session.outbox.extend(encode_message(payload))
        self.update_interest(session)

    def update_interest(self, session: Session):
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if session.outbox else 0)
        try:
            self.selector.modify(session.sock, events, self.on_socket_ready)
        except Exception:
            pass

    def handle_payload(self, session: Session, payload: Dict):
        kind = payload.get('type')
        if kind == 'login':
            user_id = str(payload.get('user_id', '')).strip()
            password = str(payload.get('password', '')).strip()
            if not user_id or not self.storage.verify_or_create_user(user_id, password):
                self.queue(session, {'type': 'login_error', 'message': 'invalid credentials'})
                return
            old = self.sessions_by_user.get(user_id)
            if old and old is not session:
                self.queue(old, {'type': 'session_closed', 'reason': 'duplicate_login'})
                self.close(old)
            session.user_id = user_id
            self.sessions_by_user[user_id] = session
            self.queue(session, {'type': 'login_ok', 'user_id': user_id})
            return
        if not session.user_id:
            self.queue(session, {'type': 'error', 'message': 'login required'})
            return
        if kind == 'send_message':
            receiver_id = str(payload.get('receiver_id', '')).strip()
            text = str(payload.get('text', '')).strip()
            saved = self.storage.insert_message(session.user_id, receiver_id, text, payload.get('client_message_id'))
            message = {'type': 'message', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'sender_id': saved.sender_id, 'receiver_id': saved.receiver_id, 'text': saved.text, 'created_at': saved.created_at}
            receiver = self.sessions_by_user.get(receiver_id)
            if receiver:
                self.queue(receiver, message)
            self.queue(session, {'type': 'send_ack', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'delivered': bool(receiver)})
        elif kind == 'fetch_history':
            self.queue(session, {'type': 'history', 'messages': self.storage.fetch_history(session.user_id, str(payload.get('peer_id', '')))})
        elif kind == 'list_chats':
            self.queue(session, {'type': 'chat_list', 'chats': self.storage.list_chats(session.user_id)})

    def close(self, session: Session):
        if session.user_id and self.sessions_by_user.get(session.user_id) is session:
            self.sessions_by_user.pop(session.user_id, None)
        self.sessions_by_socket.pop(session.sock, None)
        try:
            self.selector.unregister(session.sock)
        except Exception:
            pass
        try:
            session.sock.close()
        except Exception:
            pass


def main():
    SelectorChatServer().start()


if __name__ == '__main__':
    main()
