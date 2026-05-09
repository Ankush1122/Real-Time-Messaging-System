import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except Exception:  # lets simple tests import this module without optional services installed
    PasswordHasher = None
    VerifyMismatchError = Exception
    dict_row = None
    ConnectionPool = None


@dataclass
class StoredMessage:
    message_id: int
    chat_id: int
    sender_id: str
    receiver_id: str
    text: str
    created_at: str
    client_message_id: Optional[str] = None


class PostgresStorage:
    def __init__(self, dsn: Optional[str] = None, auto_create_users: bool = True):
        if ConnectionPool is None or PasswordHasher is None:
            raise RuntimeError('PostgreSQL dependencies are not installed')
        self.dsn = dsn or os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@127.0.0.1:5432/rtms')
        self.pool = ConnectionPool(self.dsn, min_size=1, max_size=int(os.getenv('DB_POOL_SIZE', '8')), kwargs={'row_factory': dict_row})
        self.hasher = PasswordHasher()
        self.auto_create_users = auto_create_users

    def close(self):
        self.pool.close()

    def verify_or_create_user(self, user_id: str, password: str) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute('SELECT password_hash FROM users WHERE user_id = %s', (user_id,)).fetchone()
            if row is None:
                if not self.auto_create_users:
                    return False
                conn.execute('INSERT INTO users(user_id, password_hash) VALUES (%s, %s)', (user_id, self.hasher.hash(password)))
                return True
            try:
                return self.hasher.verify(row['password_hash'], password)
            except VerifyMismatchError:
                return False

    def ensure_direct_conversation(self, user_a: str, user_b: str) -> int:
        low, high = sorted((user_a, user_b))
        with self.pool.connection() as conn:
            existing = conn.execute('SELECT chat_id FROM direct_conversations WHERE user_low=%s AND user_high=%s', (low, high)).fetchone()
            if existing:
                return existing['chat_id']
            chat_id = conn.execute("INSERT INTO conversations(kind) VALUES ('direct') RETURNING chat_id").fetchone()['chat_id']
            conn.execute('INSERT INTO direct_conversations(user_low, user_high, chat_id) VALUES (%s, %s, %s)', (low, high, chat_id))
            conn.execute('INSERT INTO conversation_participants(chat_id, user_id) VALUES (%s, %s), (%s, %s)', (chat_id, user_a, chat_id, user_b))
            return chat_id

    def insert_message(self, sender_id: str, receiver_id: str, text: str, client_message_id: Optional[str] = None) -> StoredMessage:
        chat_id = self.ensure_direct_conversation(sender_id, receiver_id)
        with self.pool.connection() as conn:
            row = conn.execute(
                '''INSERT INTO messages(chat_id, sender_id, receiver_id, message_text, client_message_id)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING message_id, chat_id, sender_id, receiver_id, message_text, client_message_id, inserted_at''',
                (chat_id, sender_id, receiver_id, text, client_message_id),
            ).fetchone()
            conn.execute('UPDATE conversations SET last_message_id=%s, updated_at=now() WHERE chat_id=%s', (row['message_id'], chat_id))
        return StoredMessage(row['message_id'], row['chat_id'], row['sender_id'], row['receiver_id'], row['message_text'], row['inserted_at'].isoformat(), row.get('client_message_id'))

    def fetch_history(self, user_id: str, peer_id: str, limit: int = 50) -> List[Dict]:
        chat_id = self.ensure_direct_conversation(user_id, peer_id)
        with self.pool.connection() as conn:
            rows = conn.execute(
                '''SELECT message_id, chat_id, sender_id, receiver_id, message_text, inserted_at
                   FROM messages WHERE chat_id=%s ORDER BY message_id DESC LIMIT %s''',
                (chat_id, limit),
            ).fetchall()
        return [self._row_to_message(row) for row in reversed(rows)]

    def list_chats(self, user_id: str) -> List[Dict]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                '''SELECT c.chat_id, dc.user_low, dc.user_high, m.message_id, m.sender_id, m.receiver_id, m.message_text, m.inserted_at
                   FROM conversation_participants cp
                   JOIN conversations c ON c.chat_id = cp.chat_id
                   JOIN direct_conversations dc ON dc.chat_id = c.chat_id
                   LEFT JOIN messages m ON m.message_id = c.last_message_id
                   WHERE cp.user_id=%s ORDER BY c.updated_at DESC''',
                (user_id,),
            ).fetchall()
        chats = []
        for row in rows:
            peer = row['user_high'] if row['user_low'] == user_id else row['user_low']
            chats.append({'chat_id': row['chat_id'], 'peer_id': peer, 'last_message': self._row_to_message(row) if row.get('message_id') else None})
        return chats

    def mark_delivered(self, user_id: str, chat_id: int, message_id: int) -> None:
        with self.pool.connection() as conn:
            conn.execute('''INSERT INTO user_conversation_state(user_id, chat_id, last_delivered_message_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT(user_id, chat_id) DO UPDATE
                            SET last_delivered_message_id = GREATEST(user_conversation_state.last_delivered_message_id, EXCLUDED.last_delivered_message_id)''', (user_id, chat_id, message_id))

    def mark_read(self, user_id: str, chat_id: int, message_id: int) -> None:
        with self.pool.connection() as conn:
            conn.execute('''INSERT INTO user_conversation_state(user_id, chat_id, last_read_message_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT(user_id, chat_id) DO UPDATE
                            SET last_read_message_id = GREATEST(user_conversation_state.last_read_message_id, EXCLUDED.last_read_message_id)''', (user_id, chat_id, message_id))

    @staticmethod
    def _row_to_message(row: Dict) -> Dict:
        return {
            'type': 'message',
            'message_id': row['message_id'],
            'chat_id': row['chat_id'],
            'sender_id': row['sender_id'],
            'receiver_id': row['receiver_id'],
            'text': row.get('message_text'),
            'created_at': row['inserted_at'].isoformat() if hasattr(row['inserted_at'], 'isoformat') else str(row['inserted_at']),
        }
