
import base64
import json
import multiprocessing as mp
import os
import selectors
import socket
import time
from collections import deque
import uuid
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

from chatapp_core.protocol import MessageReader, encode_message
from chatapp_core.storage import PostgresStorage

try:
    import redis
except ImportError:
    redis = None

HOST = os.getenv('CHAT_HOST', '0.0.0.0')
PORT = int(os.getenv('CHAT_PORT', '12345'))
PROCESS_COUNT = int(os.getenv('CHAT_PROCESSES', str(max(2, os.cpu_count() or 2))))
BACKLOG = int(os.getenv('CHAT_BACKLOG', '4096'))
READ_CHUNK_SIZE = 64 * 1024
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CONTROL_PREFIX = os.getenv('CHAT_CONTROL_PREFIX', 'chat')
PRESENCE_TTL_SECONDS = int(os.getenv('CHAT_PRESENCE_TTL_SECONDS', '90'))
REDIS_QUEUE_TTL_SECONDS = int(os.getenv('CHAT_REDIS_QUEUE_TTL_SECONDS', str(24 * 3600)))


def presence_key(user_id: str) -> str:
    return f'{CONTROL_PREFIX}:presence:{user_id}'


def route_channel(process_id: str) -> str:
    return f'{CONTROL_PREFIX}:route:{process_id}'


def backlog_key(user_id: str) -> str:
    return f'{CONTROL_PREFIX}:backlog:{user_id}'


@dataclass
class ClientSession:
    conn: socket.socket
    addr: tuple
    reader: MessageReader = field(default_factory=MessageReader)
    outbox: Deque[bytes] = field(default_factory=deque)
    user_id: Optional[str] = None
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    closing: bool = False


class RedisPlane:
    def __init__(self, process_id: str) -> None:
        if redis is None:
            raise RuntimeError('redis package missing. Run: pip install redis')
        self.process_id = process_id
        self.channel = route_channel(process_id)
        self.client = redis.Redis.from_url(REDIS_URL, decode_responses=False)
        self.client.ping()
        self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(self.channel)

    def set_presence(self, user_id: str, session_id: str) -> Optional[dict]:
        key = presence_key(user_id)
        old_raw = self.client.get(key)
        payload = json.dumps({
            'process_id': self.process_id,
            'channel': self.channel,
            'session_id': session_id,
            'ts': time.time(),
        }).encode('utf-8')
        self.client.set(key, payload, ex=PRESENCE_TTL_SECONDS)
        if not old_raw:
            return None
        try:
            return json.loads(old_raw.decode('utf-8'))
        except Exception:
            return None

    def clear_presence_if_owner(self, user_id: Optional[str], session_id: Optional[str]) -> None:
        if not user_id:
            return
        key = presence_key(user_id)
        raw = self.client.get(key)
        if not raw:
            return
        try:
            current = json.loads(raw.decode('utf-8'))
        except Exception:
            return
        if current.get('process_id') == self.process_id and current.get('session_id') == session_id:
            self.client.delete(key)

    def publish_kick(self, channel: str, user_id: str) -> None:
        payload = {'type': 'kick_user', 'user_id': user_id, 'reason': 'duplicate_login'}
        self.client.publish(channel, json.dumps(payload).encode('utf-8'))

    def get_presence(self, user_id: str) -> Optional[dict]:
        raw = self.client.get(presence_key(user_id))
        if not raw:
            return None
        try:
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return None

    def publish_frame(self, channel: str, to_user_id: str, frame: bytes) -> int:
        payload = {'type': 'deliver_frame', 'to_user_id': to_user_id, 'frame_b64': base64.b64encode(frame).decode('ascii')}
        return self.client.publish(channel, json.dumps(payload).encode('utf-8'))

    def push_backlog(self, user_id: str, frame: bytes) -> None:
        pipe = self.client.pipeline(transaction=False)
        pipe.rpush(backlog_key(user_id), frame)
        pipe.expire(backlog_key(user_id), REDIS_QUEUE_TTL_SECONDS)
        pipe.execute()

    def pop_backlog(self, user_id: str, limit: int = 64) -> list[bytes]:
        key = backlog_key(user_id)
        pipe = self.client.pipeline(transaction=False)
        for _ in range(limit):
            pipe.lpop(key)
        return [item for item in pipe.execute() if item is not None]

    def poll(self, limit: int = 128) -> list[dict]:
        messages = []
        for _ in range(limit):
            item = self.pubsub.get_message(timeout=0)
            if item is None:
                break
            try:
                messages.append(json.loads(item['data'].decode('utf-8')))
            except Exception:
                pass
        return messages


class MultiProcessWorker:
    """SO_REUSEPORT worker with Redis presence and pub/sub routing."""

    def __init__(self, index: int) -> None:
        self.index = index
        self.process_id = f'{socket.gethostname()}:{os.getpid()}:{index}'
        self.selector = selectors.DefaultSelector()
        self.sessions: Dict[socket.socket, ClientSession] = {}
        self.local_users: Dict[str, ClientSession] = {}
        self.storage = PostgresStorage()
        self.redis = RedisPlane(self.process_id)
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
        print(f'process {self.index} listening on {HOST}:{PORT} with Redis routing')
        while True:
            for event in self.redis.poll():
                self.handle_control_event(event)
            for key, mask in self.selector.select(timeout=0.2):
                key.data(key.fileobj, mask)

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

    def queue_raw_frame(self, session: ClientSession, frame: bytes) -> None:
        session.outbox.append(frame)
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
            old_local = self.local_users.get(user_id)
            if old_local and old_local is not session:
                self.force_close(old_local, 'duplicate_login')
            session.user_id = user_id
            self.local_users[user_id] = session
            old_remote = self.redis.set_presence(user_id, session.session_id)
            if old_remote and old_remote.get('channel'):
                self.redis.publish_kick(old_remote['channel'], user_id)
            self.queue(session, {'type': 'login_ok', 'user_id': user_id, 'session_id': session.session_id})
            for frame in self.redis.pop_backlog(user_id):
                self.queue_raw_frame(session, frame)
            return

        if not session.user_id:
            self.queue(session, {'type': 'error', 'message': 'login required'})
            return

        if kind == 'send_message':
            receiver_id = str(payload.get('receiver_id', '')).strip()
            saved = self.storage.insert_message(session.user_id, receiver_id, str(payload.get('text', '')), payload.get('client_message_id'))
            message = {'type': 'message', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'sender_id': saved.sender_id, 'receiver_id': saved.receiver_id, 'text': saved.text, 'created_at': saved.created_at}
            frame = encode_message(message)
            delivered = self.deliver_frame(receiver_id, frame)
            self.queue(session, {'type': 'send_ack', 'message_id': saved.message_id, 'chat_id': saved.chat_id, 'delivered': delivered})
        elif kind == 'message_received_ack':
            self.storage.mark_delivered(session.user_id, int(payload.get('chat_id', 0)), int(payload.get('message_id', 0)))
            self.queue(session, {'type': 'delivery_ack_saved', 'message_id': int(payload.get('message_id', 0))})
        elif kind == 'message_read_ack':
            self.storage.mark_read(session.user_id, int(payload.get('chat_id', 0)), int(payload.get('message_id', 0)))
            self.queue(session, {'type': 'read_ack_saved', 'message_id': int(payload.get('message_id', 0))})
        elif kind == 'fetch_history':
            self.queue(session, {'type': 'history', 'messages': self.storage.fetch_history(session.user_id, str(payload.get('peer_id', '')))})
        elif kind == 'list_chats':
            self.queue(session, {'type': 'chat_list', 'chats': self.storage.list_chats(session.user_id)})

    def deliver_frame(self, receiver_id: str, frame: bytes) -> bool:
        local = self.local_users.get(receiver_id)
        if local:
            self.queue_raw_frame(local, frame)
            return True
        presence = self.redis.get_presence(receiver_id)
        if presence and presence.get('channel'):
            subscribers = self.redis.publish_frame(presence['channel'], receiver_id, frame)
            if subscribers:
                return True
        self.redis.push_backlog(receiver_id, frame)
        return False

    def handle_control_event(self, event: dict) -> None:
        if event.get('type') == 'deliver_frame':
            session = self.local_users.get(str(event.get('to_user_id')))
            if not session:
                return
            frame = base64.b64decode(event.get('frame_b64', ''))
            self.queue_raw_frame(session, frame)
        elif event.get('type') == 'kick_user':
            session = self.local_users.get(str(event.get('user_id')))
            if session:
                self.force_close(session, event.get('reason', 'duplicate_login'))

    def force_close(self, session: ClientSession, reason: str) -> None:
        self.queue(session, {'type': 'session_closed', 'reason': reason})
        self.close(session)

    def close(self, session: ClientSession) -> None:
        session.closing = True
        if session.user_id and self.local_users.get(session.user_id) is session:
            self.local_users.pop(session.user_id, None)
            self.redis.clear_presence_if_owner(session.user_id, session.session_id)
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
