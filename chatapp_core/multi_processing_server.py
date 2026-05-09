
import multiprocessing as mp
import os
import selectors
import socket
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

from chatapp_core.protocol import MessageReader, encode_message
from chatapp_core.storage import PostgresStorage

HOST = os.getenv('CHAT_HOST', '0.0.0.0')
PORT = int(os.getenv('CHAT_PORT', '12345'))
PROCESS_COUNT = int(os.getenv('CHAT_PROCESSES', str(max(2, os.cpu_count() or 2))))
BACKLOG = int(os.getenv('CHAT_BACKLOG', '4096'))
READ_CHUNK_SIZE = 64 * 1024


@dataclass
class ClientSession:
    conn: socket.socket
    addr: tuple
    reader: MessageReader = field(default_factory=MessageReader)
    outbox: Deque[bytes] = field(default_factory=deque)
    user_id: Optional[str] = None
    closing: bool = False


class MultiProcessWorker:
    """One selector loop per process.

    This version introduces process-level socket scaling with SO_REUSEPORT.
    Cross-process delivery is intentionally not handled yet; that is added in
    the Redis routing commit that follows.
    """

    def __init__(self, index: int) -> None:
        self.index = index
        self.selector = selectors.DefaultSelector()
        self.sessions: Dict[socket.socket, ClientSession] = {}
        self.local_users: Dict[str, ClientSession] = {}
        self.storage = PostgresStorage()
        self.server_socket = self._create_server_socket()
        self.selector.register(self.server_socket, selectors.EVENT_READ, self._accept)

    def _create_server_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not hasattr(socket, 'SO_REUSEPORT'):
            raise RuntimeError('SO_REUSEPORT is required for this runtime')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((HOST, PORT))
        sock.listen(BACKLOG)
        sock.setblocking(False)
        return sock

    def run(self) -> None:
        print(f'process {self.index} listening on {HOST}:{PORT}')
        while True:
            for key, mask in self.selector.select(timeout=1):
                callback = key.data
                callback(key.fileobj, mask)

    def _accept(self, sock: socket.socket, _mask: int) -> None:
        conn, addr = sock.accept()
        conn.setblocking(False)
        session = ClientSession(conn=conn, addr=addr)
        self.sessions[conn] = session
        self.selector.register(conn, selectors.EVENT_READ, self._on_socket_ready)

    def _on_socket_ready(self, sock: socket.socket, mask: int) -> None:
        session = self.sessions.get(sock)
        if session is None:
            return
        if mask & selectors.EVENT_READ:
            try:
                data = sock.recv(READ_CHUNK_SIZE)
                if not data:
                    self.close(session)
                    return
                for payload in session.reader.feed(data):
                    self.handle_payload(session, payload)
            except Exception:
                self.close(session)
                return
        if mask & selectors.EVENT_WRITE:
            self.flush_writes(session)
        self.update_interest(session)

    def queue(self, session: ClientSession, payload: dict) -> None:
        session.outbox.append(encode_message(payload))
        self.update_interest(session)

    def flush_writes(self, session: ClientSession) -> None:
        try:
            while session.outbox:
                frame = session.outbox[0]
                sent = session.conn.send(frame)
                if sent < len(frame):
                    session.outbox[0] = frame[sent:]
                    break
                session.outbox.popleft()
        except OSError:
            self.close(session)

    def update_interest(self, session: ClientSession) -> None:
        if session.closing:
            return
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if session.outbox else 0)
        try:
            self.selector.modify(session.conn, events, self._on_socket_ready)
        except Exception:
            pass

    def handle_payload(self, session: ClientSession, payload: dict) -> None:
        kind = payload.get('type')
        if kind == 'login':
            user_id = str(payload.get('user_id', '')).strip()
            password = str(payload.get('password', '')).strip()
            if not user_id or not self.storage.verify_or_create_user(user_id, password):
                self.queue(session, {'type': 'login_error', 'message': 'invalid credentials'})
                return
            session.user_id = user_id
            self.local_users[user_id] = session
            self.queue(session, {'type': 'login_ok', 'user_id': user_id})
            return

        if not session.user_id:
            self.queue(session, {'type': 'error', 'message': 'login required'})
            return

        if kind == 'send_message':
            receiver_id = str(payload.get('receiver_id', '')).strip()
            saved = self.storage.insert_message(
                session.user_id,
                receiver_id,
                str(payload.get('text', '')),
                payload.get('client_message_id'),
            )
            message = {
                'type': 'message',
                'message_id': saved.message_id,
                'chat_id': saved.chat_id,
                'sender_id': saved.sender_id,
                'receiver_id': saved.receiver_id,
                'text': saved.text,
                'created_at': saved.created_at,
            }
            receiver = self.local_users.get(receiver_id)
            delivered = False
            if receiver:
                self.queue(receiver, message)
                delivered = True
            self.queue(session, {
                'type': 'send_ack',
                'message_id': saved.message_id,
                'chat_id': saved.chat_id,
                'delivered': delivered,
            })
        elif kind == 'fetch_history':
            self.queue(session, {'type': 'history', 'messages': self.storage.fetch_history(session.user_id, str(payload.get('peer_id', '')))})
        elif kind == 'list_chats':
            self.queue(session, {'type': 'chat_list', 'chats': self.storage.list_chats(session.user_id)})

    def close(self, session: ClientSession) -> None:
        session.closing = True
        if session.user_id and self.local_users.get(session.user_id) is session:
            self.local_users.pop(session.user_id, None)
        self.sessions.pop(session.conn, None)
        try:
            self.selector.unregister(session.conn)
        except Exception:
            pass
        try:
            session.conn.close()
        except Exception:
            pass


def _worker_main(index: int) -> None:
    MultiProcessWorker(index).run()


def main() -> None:
    workers = [mp.Process(target=_worker_main, args=(idx,), daemon=False) for idx in range(PROCESS_COUNT)]
    for worker in workers:
        worker.start()
    print(f'RTMS multi-process server running with {len(workers)} processes')
    for worker in workers:
        worker.join()


if __name__ == '__main__':
    main()
