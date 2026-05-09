import errno
import os
import queue
import selectors
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional, Tuple

from chatapp_core.protocol import MessageReader, encode_message
from chatapp_core.storage import (
    AUTO_CREATE_USERS,
    DEFAULT_LOAD_TEST_PASSWORD,
    AuthenticationError,
    ConflictError,
    PostgresStorage,
    StorageError,
)

try:
    import redis
except ImportError:  # server can start without redis only if REDIS_REQUIRED=0
    redis = None

HOST = os.getenv('CHAT_HOST', '0.0.0.0')
PORT = int(os.getenv('CHAT_PORT', '12345'))
BACKLOG = int(os.getenv('CHAT_BACKLOG', '8192'))
READ_CHUNK_SIZE = int(os.getenv('CHAT_READ_CHUNK_SIZE', str(64 * 1024)))
WORKER_COUNT = int(os.getenv('CHAT_WORKERS', str(max(2, min(8, os.cpu_count() or 4)))))
IDLE_TIMEOUT_SECONDS = int(os.getenv('CHAT_IDLE_TIMEOUT_SECONDS', '720'))

WRITE_HIGH_WATERMARK_BYTES = int(os.getenv('CHAT_WRITE_HIGH_WATERMARK_BYTES', str(512 * 1024)))
WRITE_LOW_WATERMARK_BYTES = int(os.getenv('CHAT_WRITE_LOW_WATERMARK_BYTES', str(256 * 1024)))
REDIS_DRAIN_BATCH = int(os.getenv('CHAT_REDIS_DRAIN_BATCH', '64'))
REDIS_QUEUE_TTL_SECONDS = int(os.getenv('CHAT_REDIS_QUEUE_TTL_SECONDS', str(24 * 3600)))
REDIS_REQUIRED = os.getenv('CHAT_REDIS_REQUIRED', '1') == '1'
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


def safe_user_key(user_id: str) -> str:
    return 'chat:backlog:' + str(user_id).replace(' ', '_')


class RedisBacklog:
    def __init__(self, url: str) -> None:
        self.enabled = False
        self.client = None
        if redis is None:
            if REDIS_REQUIRED:
                raise RuntimeError('redis package missing. Run: pip install redis')
            print('WARNING: redis package missing. Persistent backpressure disabled.')
            return
        try:
            self.client = redis.Redis.from_url(url, decode_responses=False)
            self.client.ping()
            self.enabled = True
            print(f'Redis backlog connected: {url}')
        except Exception as exc:
            if REDIS_REQUIRED:
                raise RuntimeError(f'Cannot connect to Redis at {url}: {exc}') from exc
            print(f'WARNING: Redis unavailable ({exc}). Persistent backpressure disabled.')

    def push(self, user_id: Optional[str], frame: bytes) -> bool:
        if not self.enabled or not user_id:
            return False
        key = safe_user_key(user_id)
        pipe = self.client.pipeline(transaction=False)
        pipe.rpush(key, frame)
        pipe.expire(key, REDIS_QUEUE_TTL_SECONDS)
        pipe.execute()
        return True

    def pop_batch(self, user_id: Optional[str], limit: int) -> list[bytes]:
        if not self.enabled or not user_id or limit <= 0:
            return []
        key = safe_user_key(user_id)
        pipe = self.client.pipeline(transaction=False)
        for _ in range(limit):
            pipe.lpop(key)
        raw = pipe.execute()
        return [x for x in raw if x is not None]


@dataclass
class ClientSession:
    conn: socket.socket
    addr: tuple
    worker: 'ReactorWorker'
    reader: MessageReader = field(default_factory=MessageReader)
    username: Optional[str] = None
    user_id: Optional[str] = None
    authenticated: bool = False
    last_seen: float = field(default_factory=time.time)
    write_queue: Deque[bytes] = field(default_factory=deque)
    queued_bytes: int = 0
    current_write_offset: int = 0
    want_write: bool = False
    closing: bool = False


class RouterShard:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.users: Dict[str, Tuple['ReactorWorker', socket.socket]] = {}


class ShardedRouter:
    def __init__(self, shard_count: int = 256) -> None:
        self.shards = [RouterShard() for _ in range(shard_count)]
        self.shard_count = shard_count

    def _shard(self, user_id: str) -> RouterShard:
        return self.shards[hash(user_id) % self.shard_count]

    def register(self, user_id: str, worker: 'ReactorWorker', conn: socket.socket) -> Optional[Tuple['ReactorWorker', socket.socket]]:
        shard = self._shard(user_id)
        with shard.lock:
            old = shard.users.get(user_id)
            shard.users[user_id] = (worker, conn)
            return old

    def unregister(self, user_id: Optional[str], worker: 'ReactorWorker', conn: socket.socket) -> None:
        if not user_id:
            return
        shard = self._shard(user_id)
        with shard.lock:
            current = shard.users.get(user_id)
            if current is not None and current[0] is worker and current[1] is conn:
                shard.users.pop(user_id, None)

    def lookup(self, user_id: str) -> Optional[Tuple['ReactorWorker', socket.socket]]:
        shard = self._shard(user_id)
        with shard.lock:
            return shard.users.get(user_id)

    def route(self, user_id: str, frame: bytes) -> bool:
        target = self.lookup(user_id)
        if target is None:
            return False
        worker, conn = target
        worker.post({'type': 'deliver_frame', 'conn': conn, 'frame': frame})
        return True


class ReactorWorker(threading.Thread):
    def __init__(self, worker_id: int, router: ShardedRouter, backlog: RedisBacklog, storage: PostgresStorage) -> None:
        super().__init__(name=f'reactor-{worker_id}', daemon=True)
        self.worker_id = worker_id
        self.router = router
        self.backlog = backlog
        self.storage = storage
        self.selector = selectors.DefaultSelector()
        self.tasks: 'queue.SimpleQueue[dict[str, Any]]' = queue.SimpleQueue()
        self.sessions: Dict[socket.socket, ClientSession] = {}
        self.running = True
        self.wakeup_r, self.wakeup_w = socket.socketpair()
        self.wakeup_r.setblocking(False)
        self.wakeup_w.setblocking(False)
        self.selector.register(self.wakeup_r, selectors.EVENT_READ, self._on_wakeup)

    def post(self, task: dict) -> None:
        self.tasks.put(task)
        self._wakeup()

    def _wakeup(self) -> None:
        try:
            self.wakeup_w.send(b'1')
        except (BlockingIOError, InterruptedError, OSError):
            pass

    def _on_wakeup(self, _sock: socket.socket, _mask: int) -> None:
        try:
            while self.wakeup_r.recv(4096):
                pass
        except (BlockingIOError, InterruptedError):
            pass
        self._process_tasks()

    def run(self) -> None:
        print(f'Reactor worker {self.worker_id} started')
        while self.running:
            events = self.selector.select(timeout=1.0)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
            self._process_tasks()
            self._sweep_idle()

    def stop(self) -> None:
        self.running = False
        self._wakeup()

    def _process_tasks(self) -> None:
        while True:
            try:
                task = self.tasks.get_nowait()
            except queue.Empty:
                return
            typ = task.get('type')
            if typ == 'add_connection':
                self._add_connection(task['conn'], task['addr'])
            elif typ == 'deliver_frame':
                session = self.sessions.get(task['conn'])
                if session is not None:
                    self._queue_frame(session, task['frame'])
            elif typ == 'close_conn':
                session = self.sessions.get(task['conn'])
                if session is not None:
                    self._close(session)

    def _add_connection(self, conn: socket.socket, addr: tuple) -> None:
        conn.setblocking(False)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        session = ClientSession(conn=conn, addr=addr, worker=self)
        self.sessions[conn] = session
        self.selector.register(conn, selectors.EVENT_READ, self._service_client)

    def _service_client(self, conn: socket.socket, mask: int) -> None:
        session = self.sessions.get(conn)
        if session is None or session.closing:
            return
        if mask & selectors.EVENT_READ:
            self._read(session)
        if mask & selectors.EVENT_WRITE:
            self._flush(session)

    def _read(self, session: ClientSession) -> None:
        try:
            data = session.conn.recv(READ_CHUNK_SIZE)
            if not data:
                self._close(session)
                return
            session.last_seen = time.time()
            for msg in session.reader.feed(data):
                self._handle_message(session, msg)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            if exc.errno not in (errno.ECONNRESET, errno.EPIPE):
                print(f'Read failure worker={self.worker_id} addr={session.addr}: {exc}')
            self._close(session)
        except Exception as exc:
            self._send_error(session, f'Bad request: {exc}')

    def _handle_message(self, session: ClientSession, message: dict) -> None:
        typ = message.get('type')
        if typ == 'register':
            self._handle_register(session, message)
        elif typ == 'login':
            self._handle_login(session, message)
        elif typ == 'send_message':
            self._handle_send_message(session, message)
        elif typ == 'message_received_ack':
            self._handle_message_received_ack(session, message)
        elif typ == 'start_chat':
            self._handle_start_chat(session, message)
        elif typ == 'fetch_history':
            self._handle_fetch_history(session, message)
        elif typ == 'fetch_chats':
            self._handle_fetch_chats(session, message)
        elif typ == 'ping':
            self._queue_payload(session, {'type': 'pong', 'ts': time.time()})
        else:
            self._send_error(session, f'Unsupported message type: {typ}')

    def _handle_register(self, session: ClientSession, message: dict) -> None:
        user_id = (message.get('user_id') or message.get('username') or '').strip()
        username = (message.get('username') or user_id).strip()
        password = message.get('password') or ''
        try:
            user = self.storage.register_user(user_id=user_id, username=username, password=password)
        except ConflictError as exc:
            self._queue_payload(session, {'type': 'register_failed', 'reason': 'user_exists', 'message': str(exc)})
            return
        except (AuthenticationError, StorageError) as exc:
            self._queue_payload(session, {'type': 'register_failed', 'reason': 'invalid_request', 'message': str(exc)})
            return
        self._queue_payload(session, {'type': 'register_ok', 'self': {'user_id': user.user_id, 'username': user.username}})

    def _handle_login(self, session: ClientSession, message: dict) -> None:
        user_id = (message.get('user_id') or message.get('username') or '').strip()
        username = (message.get('username') or user_id).strip()
        password = message.get('password') or DEFAULT_LOAD_TEST_PASSWORD
        create_if_missing = bool(message.get('create_if_missing', AUTO_CREATE_USERS))
        if not user_id:
            self._queue_payload(session, {'type': 'login_failed', 'message': 'user_id is required'})
            return
        try:
            user = self.storage.authenticate_user(user_id=user_id, username=username, password=password, create_if_missing=create_if_missing)
        except (AuthenticationError, StorageError) as exc:
            self._queue_payload(session, {'type': 'login_failed', 'message': str(exc)})
            return

        session.username = user.username
        session.user_id = user.user_id
        session.authenticated = True
        old = self.router.register(user.user_id, self, session.conn)
        if old is not None and old[1] is not session.conn:
            old_worker, old_conn = old
            old_worker.post({'type': 'close_conn', 'conn': old_conn})

        chats = self.storage.get_user_chats(user.user_id, limit=50)
        self._queue_payload(session, {
            'type': 'login_ok',
            'self': {'user_id': user.user_id, 'username': user.username},
            'chats': chats,
            'server_time': time.time(),
            'worker_id': self.worker_id,
            'architecture': 'multi_threading_postgresql',
        })
        self._drain_redis_backlog(session)

    def _handle_start_chat(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._send_error(session, 'Authenticate first')
            return
        peer_id = (message.get('to_user_id') or '').strip()
        if not peer_id:
            self._queue_payload(session, {'type': 'chat_start_failed', 'to_user_id': peer_id, 'reason': 'user_id_required', 'message': 'Enter a user id to start chat.'})
            return
        if peer_id == session.user_id:
            self._queue_payload(session, {'type': 'chat_start_failed', 'to_user_id': peer_id, 'reason': 'self_chat_not_allowed', 'message': 'You cannot start chat with yourself.'})
            return
        peer = self.storage.get_user(peer_id)
        if peer is None:
            self._queue_payload(session, {'type': 'chat_start_failed', 'to_user_id': peer_id, 'reason': 'user_not_found', 'message': f'No registered user exists with user id {peer_id}.'})
            return
        try:
            conversation = self.storage.ensure_direct_conversation(session.user_id, peer_id)
            history = self.storage.get_latest_messages(conversation['chat_id'], session.user_id, limit=int(message.get('limit') or 50))
        except StorageError as exc:
            self._queue_payload(session, {'type': 'chat_start_failed', 'to_user_id': peer_id, 'reason': 'storage_error', 'message': str(exc)})
            return
        conversation['history'] = history
        conversation['server_ts'] = time.time()
        self._queue_payload(session, {'type': 'chat_started', **conversation})

    def _handle_fetch_chats(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._send_error(session, 'Authenticate first')
            return
        limit = int(message.get('limit') or 50)
        self._queue_payload(session, {'type': 'chat_list', 'chats': self.storage.get_user_chats(session.user_id, limit=limit)})

    def _handle_fetch_history(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._send_error(session, 'Authenticate first')
            return
        try:
            chat_id = int(message.get('chat_id') or 0)
        except (TypeError, ValueError):
            chat_id = 0
        if chat_id <= 0:
            peer_id = (message.get('peer_id') or message.get('to_user_id') or '').strip()
            if not peer_id:
                self._send_error(session, 'chat_id or peer_id is required')
                return
            try:
                chat_id = int(self.storage.ensure_direct_conversation(session.user_id, peer_id)['chat_id'])
            except StorageError as exc:
                self._send_error(session, str(exc))
                return
        limit = int(message.get('limit') or 50)
        try:
            messages = self.storage.get_latest_messages(chat_id, session.user_id, limit=limit)
        except StorageError as exc:
            self._send_error(session, str(exc))
            return
        self._queue_payload(session, {'type': 'history', 'chat_id': chat_id, 'messages': messages})

    def _handle_send_message(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id or not session.username:
            self._send_error(session, 'Authenticate first')
            return
        recipient_id = (message.get('to_user_id') or '').strip()
        body = (message.get('message') or '').strip()
        client_message_id = (message.get('client_message_id') or '').strip()
        if not recipient_id or not body or not client_message_id:
            self._send_error(session, 'Recipient, message and client_message_id are required')
            return
        try:
            chat_id = int(message.get('chat_id') or 0)
        except (TypeError, ValueError):
            chat_id = 0
        if chat_id <= 0 and self.storage.get_user(recipient_id) is None:
            self._queue_payload(session, {'type': 'delivery_status', 'client_message_id': client_message_id, 'status': 'failed', 'detail': f'recipient user_id not found: {recipient_id}', 'to_user_id': recipient_id, 'server_ts': time.time()})
            return
        try:
            if chat_id <= 0:
                conversation = self.storage.ensure_direct_conversation(session.user_id, recipient_id)
                chat_id = int(conversation['chat_id'])
            stored = self.storage.insert_message(chat_id, session.user_id, body, client_message_id=client_message_id)
        except StorageError as exc:
            self._queue_payload(session, {'type': 'delivery_status', 'client_message_id': client_message_id, 'status': 'failed', 'detail': str(exc), 'to_user_id': recipient_id, 'server_ts': time.time()})
            return

        server_ts = time.time()
        chat_payload = {
            'type': 'chat_message',
            'message_id': stored['message_id'],
            'chat_id': chat_id,
            'from_user_id': session.user_id,
            'from_username': session.username,
            'to_user_id': recipient_id,
            'message': body,
            'client_message_id': client_message_id,
            'inserted_at': stored.get('inserted_at'),
            'server_ts': server_ts,
        }
        frame = encode_message(chat_payload)
        routed = self.router.route(recipient_id, frame)
        status = 'queued_to_socket' if routed else 'stored'
        detail = 'message_stored_and_queued_to_recipient_worker' if routed else 'message_stored_recipient_offline'
        self._queue_payload(session, {
            'type': 'delivery_status',
            'client_message_id': client_message_id,
            'message_id': stored['message_id'],
            'chat_id': chat_id,
            'status': status,
            'to_user_id': recipient_id,
            'detail': detail,
            'server_ts': server_ts,
        })

    def _handle_message_received_ack(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            return
        try:
            message_id = int(message.get('message_id') or 0)
            chat_id = int(message.get('chat_id') or 0)
        except (TypeError, ValueError):
            return
        if message_id <= 0 or chat_id <= 0:
            return
        self.storage.mark_delivered(session.user_id, chat_id, message_id)
        sender_id = (message.get('from_user_id') or message.get('to_user_id') or '').strip()
        if not sender_id:
            return
        ack_payload = {
            'type': 'delivery_status',
            'client_message_id': message.get('client_message_id'),
            'message_id': message_id,
            'chat_id': chat_id,
            'status': 'delivered_to_client',
            'to_user_id': session.user_id,
            'detail': 'recipient_client_acknowledged_message',
            'server_ts': time.time(),
        }
        self.router.route(sender_id, encode_message(ack_payload))

    def _send_error(self, session: ClientSession, message: str) -> None:
        self._queue_payload(session, {'type': 'error', 'message': message})

    def _queue_payload(self, session: ClientSession, payload: dict) -> None:
        self._queue_frame(session, encode_message(payload))

    def _queue_frame(self, session: ClientSession, frame: bytes) -> None:
        if session.closing:
            return
        if session.queued_bytes + len(frame) > WRITE_HIGH_WATERMARK_BYTES:
            if self.backlog.push(session.user_id, frame):
                return
            self._close(session)
            return
        was_empty = session.queued_bytes == 0
        session.write_queue.append(frame)
        session.queued_bytes += len(frame)
        if was_empty or not session.want_write:
            self._set_interest(session, True)

    def _drain_redis_backlog(self, session: ClientSession) -> None:
        if not session.user_id or session.queued_bytes > WRITE_LOW_WATERMARK_BYTES:
            return
        frames = self.backlog.pop_batch(session.user_id, REDIS_DRAIN_BATCH)
        if not frames:
            return
        was_empty = session.queued_bytes == 0
        for frame in frames:
            if session.queued_bytes + len(frame) > WRITE_HIGH_WATERMARK_BYTES:
                self.backlog.push(session.user_id, frame)
                break
            session.write_queue.append(frame)
            session.queued_bytes += len(frame)
        if was_empty and session.queued_bytes > 0:
            self._set_interest(session, True)

    def _flush(self, session: ClientSession) -> None:
        while session.write_queue:
            frame = session.write_queue[0]
            try:
                sent = session.conn.send(frame[session.current_write_offset:])
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return
                self._close(session)
                return
            if sent <= 0:
                return
            session.current_write_offset += sent
            if session.current_write_offset >= len(frame):
                finished = session.write_queue.popleft()
                session.queued_bytes -= len(finished)
                session.current_write_offset = 0
                if session.queued_bytes <= WRITE_LOW_WATERMARK_BYTES:
                    self._drain_redis_backlog(session)
        self._set_interest(session, False)

    def _set_interest(self, session: ClientSession, want_write: bool) -> None:
        if session.closing or session.want_write == want_write:
            return
        session.want_write = want_write
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if want_write else 0)
        try:
            self.selector.modify(session.conn, events, self._service_client)
        except (KeyError, ValueError, OSError):
            self._close(session)

    def _close(self, session: ClientSession) -> None:
        if session.closing:
            return
        session.closing = True
        self.router.unregister(session.user_id, self, session.conn)
        self.sessions.pop(session.conn, None)
        try:
            self.selector.unregister(session.conn)
        except Exception:
            pass
        try:
            session.conn.close()
        except Exception:
            pass

    def _sweep_idle(self) -> None:
        now = time.time()
        if not hasattr(self, '_last_sweep'):
            self._last_sweep = now
        if now - self._last_sweep < 30:
            return
        self._last_sweep = now
        for session in list(self.sessions.values()):
            if now - session.last_seen > IDLE_TIMEOUT_SECONDS:
                self._close(session)


class MultiThreadingChatServer:
    def __init__(self, host: str = HOST, port: int = PORT, worker_count: int = WORKER_COUNT) -> None:
        self.host = host
        self.port = port
        self.router = ShardedRouter()
        self.backlog = RedisBacklog(REDIS_URL)
        self.storage = PostgresStorage()
        self.workers = [ReactorWorker(i, self.router, self.backlog, self.storage) for i in range(worker_count)]
        self.next_worker = 0
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(BACKLOG)
        self.server_socket.setblocking(True)

    def start(self) -> None:
        for worker in self.workers:
            worker.start()
        print(f'Multi-threading chat server listening on {self.host}:{self.port}')
        print(f'Workers={len(self.workers)} PostgreSQL=enabled RedisBacklog={self.backlog.enabled}')
        try:
            while self.running:
                conn, addr = self.server_socket.accept()
                worker = self.workers[self.next_worker]
                self.next_worker = (self.next_worker + 1) % len(self.workers)
                worker.post({'type': 'add_connection', 'conn': conn, 'addr': addr})
        except KeyboardInterrupt:
            print('Shutting down...')
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if not self.running:
            return
        self.running = False
        try:
            self.server_socket.close()
        except Exception:
            pass
        for worker in self.workers:
            worker.stop()
        for worker in self.workers:
            worker.join(timeout=2)
        self.storage.close()


def main() -> None:
    MultiThreadingChatServer().start()


if __name__ == '__main__':
    main()
