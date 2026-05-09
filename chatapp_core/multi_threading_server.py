import os
import queue
import selectors
import socket
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional

from chatapp_core.protocol import MessageReader, encode_message
from chatapp_core.storage import PostgresStorage

HOST = '0.0.0.0'
PORT = 12345
WORKERS = int(os.getenv('CHAT_WORKERS', str(os.cpu_count() or 4)))
HIGH_WATERMARK = 512 * 1024
LOW_WATERMARK = 128 * 1024


@dataclass
class Session:
    sock: socket.socket
    addr: tuple
    worker: 'ReactorWorker'
    reader: MessageReader = field(default_factory=MessageReader)
    outbox: bytearray = field(default_factory=bytearray)
    user_id: Optional[str] = None


class ShardedRouter:
    def __init__(self, shards=64):
        self.shards = [{'lock': threading.RLock(), 'users': {}} for _ in range(shards)]

    def _shard(self, user_id):
        return self.shards[hash(user_id) % len(self.shards)]

    def set(self, user_id, session):
        shard = self._shard(user_id)
        with shard['lock']:
            old = shard['users'].get(user_id)
            shard['users'][user_id] = session
            return old

    def get(self, user_id):
        shard = self._shard(user_id)
        with shard['lock']:
            return shard['users'].get(user_id)

    def remove_if_current(self, user_id, session):
        shard = self._shard(user_id)
        with shard['lock']:
            if shard['users'].get(user_id) is session:
                shard['users'].pop(user_id, None)


class ReactorWorker(threading.Thread):
    def __init__(self, idx, router, storage):
        super().__init__(daemon=True, name=f'reactor-{idx}')
        self.idx = idx
        self.selector = selectors.DefaultSelector()
        self.tasks = queue.SimpleQueue()
        self.router = router
        self.storage = storage
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False); self._wake_w.setblocking(False)
        self.selector.register(self._wake_r, selectors.EVENT_READ, (self._drain_wakeup, None))

    def submit(self, fn, *args):
        self.tasks.put((fn, args))
        try:
            self._wake_w.send(b'x')
        except BlockingIOError:
            pass

    def add_socket(self, sock, addr):
        session = Session(sock=sock, addr=addr, worker=self)
        sock.setblocking(False)
        self.selector.register(sock, selectors.EVENT_READ, (self._on_socket_ready, session))

    def queue_frame(self, session, payload):
        if len(session.outbox) > HIGH_WATERMARK:
            return False
        session.outbox.extend(encode_message(payload))
        self._update_interest(session)
        return True

    def run(self):
        while True:
            for key, mask in self.selector.select(timeout=1):
                callback, data_session = key.data
                if data_session is None:
                    callback(key.fileobj, mask)
                else:
                    callback(key.fileobj, mask, data_session)

    def _drain_wakeup(self, sock, mask):
        try:
            while sock.recv(4096):
                pass
        except BlockingIOError:
            pass
        while True:
            try:
                fn, args = self.tasks.get_nowait()
            except queue.Empty:
                break
            fn(*args)

    def _on_socket_ready(self, sock, mask, session):
        if mask & selectors.EVENT_READ:
            try:
                data = sock.recv(65536)
                if not data:
                    self.close(session); return
                for payload in session.reader.feed(data):
                    self.handle_payload(session, payload)
            except Exception:
                self.close(session); return
        if mask & selectors.EVENT_WRITE:
            try:
                sent = sock.send(session.outbox)
                del session.outbox[:sent]
            except OSError:
                self.close(session); return
        self._update_interest(session)

    def _update_interest(self, session):
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if session.outbox else 0)
        try:
            self.selector.modify(session.sock, events, (self._on_socket_ready, session))
        except Exception:
            pass

    def handle_payload(self, session, payload):
        kind = payload.get('type')
        if kind == 'login':
            user_id = str(payload.get('user_id', '')).strip()
            password = str(payload.get('password', '')).strip()
            if not user_id or not self.storage.verify_or_create_user(user_id, password):
                self.queue_frame(session, {'type': 'login_error', 'message': 'invalid credentials'})
                return
            old = self.router.set(user_id, session)
            if old and old is not session:
                old.worker.submit(old.worker.force_close, old, 'duplicate_login')
            session.user_id = user_id
            self.queue_frame(session, {'type': 'login_ok', 'user_id': user_id})
            return
        if not session.user_id:
            self.queue_frame(session, {'type': 'error', 'message': 'login required'}); return
        if kind == 'send_message':
            receiver_id = str(payload.get('receiver_id', '')).strip()
            saved = self.storage.insert_message(session.user_id, receiver_id, str(payload.get('text', '')), payload.get('client_message_id'))
            message = {'type': 'message', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'sender_id': saved.sender_id, 'receiver_id': saved.receiver_id, 'text': saved.text, 'created_at': saved.created_at}
            receiver = self.router.get(receiver_id)
            delivered = False
            if receiver:
                receiver.worker.submit(receiver.worker.queue_frame, receiver, message)
                delivered = True
            self.queue_frame(session, {'type': 'send_ack', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'delivered': delivered})
        elif kind == 'message_received_ack':
            self.storage.mark_delivered(session.user_id, int(payload.get('chat_id', 0)), int(payload.get('message_id', 0)))
            self.queue_frame(session, {'type': 'delivery_ack_saved', 'message_id': int(payload.get('message_id', 0))})
        elif kind == 'message_read_ack':
            self.storage.mark_read(session.user_id, int(payload.get('chat_id', 0)), int(payload.get('message_id', 0)))
            self.queue_frame(session, {'type': 'read_ack_saved', 'message_id': int(payload.get('message_id', 0))})
        elif kind == 'fetch_history':
            self.queue_frame(session, {'type': 'history', 'messages': self.storage.fetch_history(session.user_id, str(payload.get('peer_id', '')))})
        elif kind == 'list_chats':
            self.queue_frame(session, {'type': 'chat_list', 'chats': self.storage.list_chats(session.user_id)})

    def force_close(self, session, reason):
        self.queue_frame(session, {'type': 'session_closed', 'reason': reason})
        self.close(session)

    def close(self, session):
        if session.user_id:
            self.router.remove_if_current(session.user_id, session)
        try:
            self.selector.unregister(session.sock)
        except Exception:
            pass
        try:
            session.sock.close()
        except Exception:
            pass


class MultiThreadedChatServer:
    def __init__(self):
        self.router = ShardedRouter()
        self.storage = PostgresStorage()
        self.workers = [ReactorWorker(i, self.router, self.storage) for i in range(WORKERS)]
        self.next_worker = 0

    def start(self):
        for worker in self.workers:
            worker.start()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT)); server.listen(4096)
        print(f'RTMS multi-threading server listening on {HOST}:{PORT} with {len(self.workers)} workers')
        while True:
            sock, addr = server.accept()
            worker = self.workers[self.next_worker % len(self.workers)]
            self.next_worker += 1
            worker.submit(worker.add_socket, sock, addr)


def main():
    MultiThreadedChatServer().start()


if __name__ == '__main__':
    main()
