import base64
import errno
import heapq
import json
import multiprocessing as mp
import os
import selectors
import signal
import socket
import time
import uuid
import zlib
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

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
except ImportError:
    redis = None

HOST = os.getenv('CHAT_HOST', '0.0.0.0')
PORT = int(os.getenv('CHAT_PORT', '12345'))
BACKLOG = int(os.getenv('CHAT_BACKLOG', '8192'))
READ_CHUNK_SIZE = int(os.getenv('CHAT_READ_CHUNK_SIZE', str(64 * 1024)))
PROCESS_COUNT = int(os.getenv('CHAT_PROCESSES', str(max(2, os.cpu_count() or 2))))
IDLE_TIMEOUT_SECONDS = int(os.getenv('CHAT_IDLE_TIMEOUT_SECONDS', '720'))

WRITE_HIGH_WATERMARK_BYTES = int(os.getenv('CHAT_WRITE_HIGH_WATERMARK_BYTES', str(512 * 1024)))
WRITE_LOW_WATERMARK_BYTES = int(os.getenv('CHAT_WRITE_LOW_WATERMARK_BYTES', str(256 * 1024)))
REDIS_DRAIN_BATCH = int(os.getenv('CHAT_REDIS_DRAIN_BATCH', '64'))
REDIS_QUEUE_TTL_SECONDS = int(os.getenv('CHAT_REDIS_QUEUE_TTL_SECONDS', str(24 * 3600)))
PRESENCE_TTL_SECONDS = int(os.getenv('CHAT_PRESENCE_TTL_SECONDS', '90'))
PRESENCE_REFRESH_SECONDS = float(os.getenv('CHAT_PRESENCE_REFRESH_SECONDS', '30'))
PRESENCE_REFRESH_SPREAD_SECONDS = float(os.getenv('CHAT_PRESENCE_REFRESH_SPREAD_SECONDS', str(PRESENCE_REFRESH_SECONDS)))
PRESENCE_REFRESH_BATCH = int(os.getenv('CHAT_PRESENCE_REFRESH_BATCH', '500'))
PRESENCE_MAINTENANCE_TICK_SECONDS = float(os.getenv('CHAT_PRESENCE_MAINTENANCE_TICK_SECONDS', '1.0'))
PUBSUB_POLL_LIMIT = int(os.getenv('CHAT_PUBSUB_POLL_LIMIT', '512'))
DIRECT_CONVERSATION_CACHE_SIZE = int(os.getenv('CHAT_DIRECT_CONVERSATION_CACHE_SIZE', '100000'))
PERSIST_DELIVERY_ACKS = os.getenv('CHAT_PERSIST_DELIVERY_ACKS', '1') == '1'
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CONTROL_PREFIX = os.getenv('CHAT_CONTROL_PREFIX', 'chat')


def safe_user_key(user_id: str) -> str:
    return f'{CONTROL_PREFIX}:backlog:' + str(user_id).replace(' ', '_')


def presence_key(user_id: str) -> str:
    return f'{CONTROL_PREFIX}:presence:' + str(user_id).replace(' ', '_')


def route_channel(process_id: str) -> str:
    return f'{CONTROL_PREFIX}:route:{process_id}'


@dataclass
class ClientSession:
    conn: socket.socket
    addr: tuple
    reader: MessageReader = field(default_factory=MessageReader)
    username: Optional[str] = None
    user_id: Optional[str] = None
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    authenticated: bool = False
    last_seen: float = field(default_factory=time.time)
    presence_refresh_due: float = 0.0
    presence_generation: int = 0
    write_queue: Deque[bytes] = field(default_factory=deque)
    queued_bytes: int = 0
    current_write_offset: int = 0
    want_write: bool = False
    closing: bool = False


class RedisPlane:
    def __init__(self, url: str, process_id: str) -> None:
        if redis is None:
            raise RuntimeError('redis package missing. Run: pip install redis')
        self.client = redis.Redis.from_url(url, decode_responses=False)
        self.client.ping()
        self.process_id = process_id
        self.channel = route_channel(process_id)
        self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(self.channel)

    def close(self) -> None:
        try:
            self.pubsub.close()
        except Exception:
            pass

    def _presence_payload(self, user_id: str, username: str, session_id: str) -> bytes:
        return json.dumps({
            'process_id': self.process_id,
            'channel': self.channel,
            'username': username,
            'session_id': session_id,
            'ts': time.time(),
        }, separators=(',', ':')).encode('utf-8')

    def set_presence(self, user_id: str, username: str, session_id: str) -> Optional[dict]:
        key = presence_key(user_id)
        old_raw = self.client.get(key)
        self.client.set(key, self._presence_payload(user_id, username, session_id), ex=PRESENCE_TTL_SECONDS)
        if not old_raw:
            return None
        try:
            return json.loads(old_raw.decode('utf-8'))
        except Exception:
            return None

    def refresh_presence_many(self, entries: list[tuple[str, str, str]]) -> None:
        """Safely upsert presence for still-connected local sessions.

        Do not use EXPIRE-only here. If a worker is delayed past the TTL, the
        key may already be gone. EXPIRE on a missing key is a no-op, which makes
        a live socket look offline forever. Refresh therefore rewrites the full
        presence value using SET ... EX.

        To avoid an old duplicate-login session overwriting a newer login, we
        only rewrite when the current key is missing or still belongs to this
        same process/session_id.
        """
        if not entries:
            return

        get_pipe = self.client.pipeline(transaction=False)
        for user_id, _username, _session_id in entries:
            get_pipe.get(presence_key(user_id))
        current_values = get_pipe.execute()

        set_pipe = self.client.pipeline(transaction=False)
        writes = 0
        for (user_id, username, session_id), raw in zip(entries, current_values):
            should_write = False
            if not raw:
                should_write = True
            else:
                try:
                    current = json.loads(raw.decode('utf-8'))
                    should_write = (
                        current.get('process_id') == self.process_id
                        and current.get('session_id') == session_id
                    )
                except Exception:
                    # Corrupt local presence owned by this process cannot be
                    # trusted. Recreate it for the active in-memory session.
                    should_write = True

            if should_write:
                set_pipe.set(
                    presence_key(user_id),
                    self._presence_payload(user_id, username, session_id),
                    ex=PRESENCE_TTL_SECONDS,
                )
                writes += 1

        if writes:
            set_pipe.execute()

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
        if current.get('process_id') != self.process_id:
            return
        # Session token prevents an old duplicate-login connection from deleting
        # the newer login's presence key when both are handled by the same worker.
        if session_id is not None and current.get('session_id') != session_id:
            return
        self.client.delete(key)

    def clear_presence_if_process_owner(self, user_id: Optional[str]) -> None:
        self.clear_presence_if_owner(user_id, session_id=None)

    def get_presence(self, user_id: str) -> Optional[dict]:
        raw = self.client.get(presence_key(user_id))
        if not raw:
            return None
        try:
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return None

    def publish_frame(self, channel: str, to_user_id: str, frame: bytes) -> int:
        payload = {
            'type': 'deliver_frame',
            'to_user_id': to_user_id,
            'frame_b64': base64.b64encode(frame).decode('ascii'),
        }
        return self.client.publish(channel, json.dumps(payload, separators=(',', ':')).encode('utf-8'))

    def publish_kick(self, channel: str, user_id: str) -> None:
        payload = {'type': 'kick_user', 'user_id': user_id, 'reason': 'duplicate_login'}
        self.client.publish(channel, json.dumps(payload, separators=(',', ':')).encode('utf-8'))

    def push_backlog(self, user_id: Optional[str], frame: bytes) -> bool:
        if not user_id:
            return False
        key = safe_user_key(user_id)
        pipe = self.client.pipeline(transaction=False)
        pipe.rpush(key, frame)
        pipe.expire(key, REDIS_QUEUE_TTL_SECONDS)
        pipe.execute()
        return True

    def pop_backlog_batch(self, user_id: Optional[str], limit: int) -> list[bytes]:
        if not user_id or limit <= 0:
            return []
        key = safe_user_key(user_id)
        pipe = self.client.pipeline(transaction=False)
        for _ in range(limit):
            pipe.lpop(key)
        raw = pipe.execute()
        return [x for x in raw if x is not None]


class MultiProcessingWorker:
    def __init__(self, index: int, host: str, port: int) -> None:
        self.index = index
        self.process_id = f'{socket.gethostname()}:{os.getpid()}:{index}'
        self.host = host
        self.port = port
        self.selector = selectors.DefaultSelector()
        self.sessions: Dict[socket.socket, ClientSession] = {}
        self.local_users: Dict[str, ClientSession] = {}
        self.running = True
        self.last_presence_refresh = time.time()
        self.last_idle_sweep = time.time()
        self.presence_heap: list[tuple[float, int, str]] = []
        self.direct_chat_cache: OrderedDict[tuple[str, str], int] = OrderedDict()
        self.storage = PostgresStorage()
        self.redis = RedisPlane(REDIS_URL, self.process_id)
        self.server_socket = self._create_server_socket()
        self.selector.register(self.server_socket, selectors.EVENT_READ, self._accept)

    def _create_server_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not hasattr(socket, 'SO_REUSEPORT'):
            raise RuntimeError('SO_REUSEPORT is required for multi_processing_server.py')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((self.host, self.port))
        sock.listen(BACKLOG)
        sock.setblocking(False)
        return sock

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        print(f'Multi-processing worker {self.index} listening on {self.host}:{self.port} channel={self.redis.channel}', flush=True)
        try:
            while self.running:
                events = self.selector.select(timeout=0.05)
                for key, mask in events:
                    key.data(key.fileobj, mask)
                self._poll_pubsub()
                self._periodic_maintenance()
        finally:
            self.shutdown()

    def stop(self) -> None:
        self.running = False

    def shutdown(self) -> None:
        for session in list(self.sessions.values()):
            self._close(session)
        try:
            self.selector.unregister(self.server_socket)
        except Exception:
            pass
        try:
            self.server_socket.close()
        except Exception:
            pass
        self.redis.close()
        self.storage.close()

    def _accept(self, server_socket: socket.socket, _mask: int) -> None:
        while True:
            try:
                conn, addr = server_socket.accept()
                conn.setblocking(False)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                session = ClientSession(conn=conn, addr=addr)
                self.sessions[conn] = session
                self.selector.register(conn, selectors.EVENT_READ, self._service_client)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                if exc.errno not in (errno.EBADF, errno.EINVAL):
                    print(f'accept failure process={self.index}: {exc}', flush=True)
                return

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
            for message in session.reader.feed(data):
                self._handle_message(session, message)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            if exc.errno not in (errno.ECONNRESET, errno.EPIPE):
                print(f'read failure process={self.index} addr={session.addr}: {exc}', flush=True)
            self._close(session)
        except Exception as exc:
            self._queue_payload(session, {'type': 'error', 'message': f'Bad request: {exc}'})

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
            self._queue_payload(session, {'type': 'error', 'message': f'Unsupported message type: {typ}'})

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
            user = self.storage.authenticate_user(
                user_id=user_id,
                username=username,
                password=password,
                create_if_missing=create_if_missing,
            )
        except AuthenticationError as exc:
            self._queue_payload(session, {'type': 'login_failed', 'message': str(exc)})
            return
        except StorageError as exc:
            self._queue_payload(session, {'type': 'login_failed', 'message': str(exc)})
            return

        user_id = user.user_id
        username = user.username
        old_presence = self.redis.set_presence(user_id, username, session.session_id)
        if old_presence and old_presence.get('channel'):
            if old_presence.get('process_id') == self.process_id:
                old_session = self.local_users.get(user_id)
                if old_session is not None and old_session is not session:
                    self._close(old_session)
            else:
                self.redis.publish_kick(old_presence['channel'], user_id)

        if session.user_id and session.user_id != user_id:
            self.local_users.pop(session.user_id, None)
            self.redis.clear_presence_if_owner(session.user_id, session.session_id)

        session.username = username
        session.user_id = user_id
        session.authenticated = True
        self.local_users[user_id] = session
        self._schedule_presence_refresh(session)
        include_chats = bool(message.get('include_chats', True))
        chats = self.storage.get_user_chats(user_id, limit=50) if include_chats else []
        self._queue_payload(session, {
            'type': 'login_ok',
            'self': {'user_id': user_id, 'username': username},
            'chats': chats,
            'server_time': time.time(),
            'worker_id': self.index,
            'process_id': self.process_id,
            'architecture': 'multi_processing_postgresql_redis_pubsub',
        })
        self._drain_redis_backlog(session)

    def _handle_start_chat(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._queue_payload(session, {'type': 'error', 'message': 'Authenticate first'})
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
            conversation = self._ensure_direct_conversation_cached(session.user_id, peer_id)
            history = self.storage.get_latest_messages(conversation['chat_id'], session.user_id, limit=int(message.get('limit') or 50))
        except StorageError as exc:
            self._queue_payload(session, {'type': 'chat_start_failed', 'to_user_id': peer_id, 'reason': 'storage_error', 'message': str(exc)})
            return
        conversation['history'] = history
        conversation['server_ts'] = time.time()
        self._queue_payload(session, {'type': 'chat_started', **conversation})

    def _handle_fetch_chats(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._queue_payload(session, {'type': 'error', 'message': 'Authenticate first'})
            return
        limit = int(message.get('limit') or 50)
        self._queue_payload(session, {'type': 'chat_list', 'chats': self.storage.get_user_chats(session.user_id, limit=limit)})

    def _handle_fetch_history(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id:
            self._queue_payload(session, {'type': 'error', 'message': 'Authenticate first'})
            return
        try:
            chat_id = int(message.get('chat_id') or 0)
        except (TypeError, ValueError):
            chat_id = 0
        if chat_id <= 0:
            peer_id = (message.get('peer_id') or message.get('to_user_id') or '').strip()
            if not peer_id:
                self._queue_payload(session, {'type': 'error', 'message': 'chat_id or peer_id is required'})
                return
            try:
                chat_id = int(self.storage.ensure_direct_conversation(session.user_id, peer_id)['chat_id'])
            except StorageError as exc:
                self._queue_payload(session, {'type': 'error', 'message': str(exc)})
                return
        limit = int(message.get('limit') or 50)
        try:
            messages = self.storage.get_latest_messages(chat_id, session.user_id, limit=limit)
        except StorageError as exc:
            self._queue_payload(session, {'type': 'error', 'message': str(exc)})
            return
        self._queue_payload(session, {'type': 'history', 'chat_id': chat_id, 'messages': messages})

    def _handle_send_message(self, session: ClientSession, message: dict) -> None:
        if not session.authenticated or not session.user_id or not session.username:
            self._queue_payload(session, {'type': 'error', 'message': 'Authenticate first'})
            return
        recipient_id = (message.get('to_user_id') or '').strip()
        body = (message.get('message') or '').strip()
        client_message_id = (message.get('client_message_id') or '').strip()
        if not recipient_id or not body or not client_message_id:
            self._queue_payload(session, {'type': 'error', 'message': 'Recipient, message and client_message_id are required'})
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
                chat_id = self._get_direct_chat_id_cached(session.user_id, recipient_id)
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
        status, detail = self._route_online(recipient_id, frame)
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
        if PERSIST_DELIVERY_ACKS:
            try:
                self.storage.mark_delivered(session.user_id, chat_id, message_id)
            except StorageError as exc:
                print(f'mark_delivered failed process={self.index} user_id={session.user_id} chat_id={chat_id} message_id={message_id}: {exc}', flush=True)
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
        self._route_online(sender_id, encode_message(ack_payload))

    def _route_online(self, recipient_id: str, frame: bytes) -> tuple[str, str]:
        local = self.local_users.get(recipient_id)
        if local is not None and not local.closing:
            self._queue_frame(local, frame)
            return 'queued_to_socket', 'message_stored_and_queued_to_local_socket'

        presence = self.redis.get_presence(recipient_id)
        if presence and presence.get('channel'):
            subscribers = self.redis.publish_frame(presence['channel'], recipient_id, frame)
            if subscribers > 0:
                return 'published_to_process', 'message_stored_and_published_to_recipient_process'

        return 'stored', 'message_stored_recipient_offline_or_presence_missing'

    def _poll_pubsub(self) -> None:
        for _ in range(PUBSUB_POLL_LIMIT):
            item = self.redis.pubsub.get_message(timeout=0)
            if item is None:
                return
            if item.get('type') != 'message':
                continue
            try:
                payload = json.loads(item['data'].decode('utf-8'))
            except Exception:
                continue
            typ = payload.get('type')
            if typ == 'deliver_frame':
                to_user_id = payload.get('to_user_id')
                session = self.local_users.get(to_user_id)
                frame = base64.b64decode(payload.get('frame_b64', ''))
                if session is not None and not session.closing:
                    self._queue_frame(session, frame)
                else:
                    # Do not silently drop frames published to a stale process channel.
                    # Store them in the bounded Redis backlog and remove stale presence
                    # owned by this process, so future sends stop publishing here.
                    self.redis.push_backlog(to_user_id, frame)
                    self.redis.clear_presence_if_process_owner(to_user_id)
            elif typ == 'kick_user':
                session = self.local_users.get(payload.get('user_id'))
                if session is not None:
                    self._queue_payload(session, {'type': 'error', 'message': 'Logged out because this user_id logged in elsewhere'})
                    self._close(session)

    def _queue_payload(self, session: ClientSession, payload: dict) -> None:
        self._queue_frame(session, encode_message(payload))

    def _queue_frame(self, session: ClientSession, frame: bytes) -> None:
        if session.closing:
            return
        if session.queued_bytes + len(frame) > WRITE_HIGH_WATERMARK_BYTES:
            if self.redis.push_backlog(session.user_id, frame):
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
        frames = self.redis.pop_backlog_batch(session.user_id, REDIS_DRAIN_BATCH)
        was_empty = session.queued_bytes == 0
        for frame in frames:
            if session.queued_bytes + len(frame) > WRITE_HIGH_WATERMARK_BYTES:
                self.redis.push_backlog(session.user_id, frame)
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
        if session.user_id:
            current = self.local_users.get(session.user_id)
            if current is session:
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

    def _presence_spread_offset(self, user_id: str, generation: int) -> float:
        spread = max(PRESENCE_REFRESH_SPREAD_SECONDS, 0.0)
        if spread <= 0.0:
            return 0.0
        slots = max(int(spread * 1000), 1)
        raw = f'{user_id}:{generation}'.encode('utf-8', errors='ignore')
        return (zlib.crc32(raw) % slots) / 1000.0

    def _schedule_presence_refresh(self, session: ClientSession, now: Optional[float] = None) -> None:
        if not session.user_id:
            return
        now = time.time() if now is None else now
        session.presence_generation += 1
        due = now + PRESENCE_REFRESH_SECONDS + self._presence_spread_offset(session.user_id, session.presence_generation)
        session.presence_refresh_due = due
        heapq.heappush(self.presence_heap, (due, session.presence_generation, session.user_id))

    def _refresh_due_presence(self, now: float) -> None:
        if now - self.last_presence_refresh < PRESENCE_MAINTENANCE_TICK_SECONDS:
            return
        self.last_presence_refresh = now
        due_entries: list[tuple[str, str, str]] = []
        while self.presence_heap and len(due_entries) < PRESENCE_REFRESH_BATCH:
            due, generation, user_id = self.presence_heap[0]
            if due > now:
                break
            heapq.heappop(self.presence_heap)
            session = self.local_users.get(user_id)
            if session is None or session.closing or session.presence_generation != generation:
                continue
            due_entries.append((user_id, session.username or user_id, session.session_id))
        if not due_entries:
            return
        try:
            self.redis.refresh_presence_many(due_entries)
        except Exception as exc:
            print(f'presence refresh failed process={self.index} count={len(due_entries)}: {exc}', flush=True)
        for user_id, _username, _session_id in due_entries:
            session = self.local_users.get(user_id)
            if session is not None and not session.closing:
                self._schedule_presence_refresh(session, now=now)

    @staticmethod
    def _direct_cache_key(user_a: str, user_b: str) -> tuple[str, str]:
        return tuple(sorted((str(user_a), str(user_b))))

    def _get_direct_chat_id_cached(self, user_a: str, user_b: str) -> int:
        key = self._direct_cache_key(user_a, user_b)
        cached_chat_id = self.direct_chat_cache.get(key)
        if cached_chat_id is not None:
            self.direct_chat_cache.move_to_end(key)
            return int(cached_chat_id)
        conversation = self.storage.ensure_direct_conversation(user_a, user_b)
        chat_id = int(conversation['chat_id'])
        if DIRECT_CONVERSATION_CACHE_SIZE > 0:
            self.direct_chat_cache[key] = chat_id
            self.direct_chat_cache.move_to_end(key)
            while len(self.direct_chat_cache) > DIRECT_CONVERSATION_CACHE_SIZE:
                self.direct_chat_cache.popitem(last=False)
        return chat_id

    def _ensure_direct_conversation_cached(self, user_a: str, user_b: str) -> dict:
        chat_id = self._get_direct_chat_id_cached(user_a, user_b)
        return self.storage._conversation_payload(chat_id, user_a, user_b)

    def _periodic_maintenance(self) -> None:
        now = time.time()
        self._refresh_due_presence(now)
        if now - self.last_idle_sweep >= 30:
            self.last_idle_sweep = now
            for session in list(self.sessions.values()):
                if now - session.last_seen > IDLE_TIMEOUT_SECONDS:
                    self._close(session)


def _worker_entry(index: int, host: str, port: int) -> None:
    MultiProcessingWorker(index, host, port).run()


class MultiProcessingChatServer:
    def __init__(self, host: str = HOST, port: int = PORT, process_count: int = PROCESS_COUNT) -> None:
        self.host = host
        self.port = port
        self.process_count = process_count
        self.children: list[mp.Process] = []

    def start(self) -> None:
        if redis is None:
            raise RuntimeError('redis package missing. Run: pip install redis')
        redis.Redis.from_url(REDIS_URL).ping()
        PostgresStorage().close()
        print(f'Multi-processing chat server starting on {self.host}:{self.port}')
        print(f'Processes={self.process_count} Redis={REDIS_URL} PostgreSQL=enabled SO_REUSEPORT=required')
        for index in range(self.process_count):
            proc = mp.Process(target=_worker_entry, args=(index, self.host, self.port), name=f'chat-process-{index}')
            proc.start()
            self.children.append(proc)
        try:
            for proc in self.children:
                proc.join()
        except KeyboardInterrupt:
            print('Shutting down multi-processing chat server...')
            self.shutdown()

    def shutdown(self) -> None:
        for proc in self.children:
            if proc.is_alive():
                proc.terminate()
        for proc in self.children:
            proc.join(timeout=3)
            if proc.is_alive():
                proc.kill()


def main() -> None:
    MultiProcessingChatServer().start()


if __name__ == '__main__':
    main()
