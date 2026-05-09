from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError:
    psycopg = None
    dict_row = None
    ConnectionPool = None

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError, VerificationError
except ImportError:
    PasswordHasher = None
    VerifyMismatchError = Exception
    VerificationError = Exception

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://chatapp:chatapp@localhost:5432/chatapp")
DB_POOL_MIN_SIZE = int(os.getenv("CHAT_DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_SIZE = int(os.getenv("CHAT_DB_POOL_MAX_SIZE", "10"))
AUTO_INIT_SCHEMA = os.getenv("CHAT_DB_AUTO_INIT", "1") == "1"
AUTO_CREATE_USERS = os.getenv("CHAT_AUTO_CREATE_USERS", "1") == "1"
DEFAULT_LOAD_TEST_PASSWORD = os.getenv("CHAT_DEFAULT_PASSWORD", "loadtest")


class StorageError(RuntimeError):
    pass


class AuthenticationError(StorageError):
    pass


class ConflictError(StorageError):
    pass


@dataclass(frozen=True)
class AuthenticatedUser:
    user_id: str
    username: str


class PostgresStorage:
    """Small repository/service layer around PostgreSQL.

    The schema follows:
      users
      conversations
      direct_conversations
      conversation_participants
      messages
      user_conversation_state
    """

    def __init__(self, dsn: str = DATABASE_URL, min_size: int = DB_POOL_MIN_SIZE, max_size: int = DB_POOL_MAX_SIZE) -> None:
        if psycopg is None or ConnectionPool is None:
            raise RuntimeError("PostgreSQL support missing. Run: pip install 'psycopg[binary,pool]'")
        if PasswordHasher is None:
            raise RuntimeError("Password hashing support missing. Run: pip install argon2-cffi")
        self.dsn = dsn
        self.password_hasher = PasswordHasher()
        self.pool = ConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": False},
            open=True,
        )
        if AUTO_INIT_SCHEMA:
            self.init_schema()

    def close(self) -> None:
        self.pool.close()

    def init_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS conversations (
            chat_id BIGSERIAL PRIMARY KEY,
            chat_type TEXT NOT NULL CHECK (chat_type IN ('direct', 'group')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_message_id BIGINT
        );

        CREATE TABLE IF NOT EXISTS direct_conversations (
            user_low TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            user_high TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            chat_id BIGINT NOT NULL UNIQUE REFERENCES conversations(chat_id) ON DELETE CASCADE,
            PRIMARY KEY (user_low, user_high),
            CHECK (user_low < user_high)
        );

        CREATE TABLE IF NOT EXISTS conversation_participants (
            chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
            user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (chat_id, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_conversation_participants_user
            ON conversation_participants(user_id, chat_id);

        CREATE TABLE IF NOT EXISTS messages (
            message_id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
            sender_id TEXT NOT NULL REFERENCES users(user_id),
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            message_text TEXT NOT NULL,
            client_message_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_messages_chat_latest
            ON messages(chat_id, inserted_at DESC, message_id DESC);

        CREATE INDEX IF NOT EXISTS idx_messages_client_message_id
            ON messages(client_message_id) WHERE client_message_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS user_conversation_state (
            user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
            last_read_message_id BIGINT,
            last_delivered_message_id BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, chat_id)
        );
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def register_user(self, user_id: str, username: str, password: str) -> AuthenticatedUser:
        user_id = self._normalize_user_id(user_id)
        username = (username or user_id).strip() or user_id
        if not password:
            raise AuthenticationError("password is required")
        password_hash = self.password_hasher.hash(password)
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO users(user_id, username, password_hash)
                        VALUES (%s, %s, %s)
                        RETURNING user_id, username
                        """,
                        (user_id, username, password_hash),
                    )
                    row = cur.fetchone()
                conn.commit()
        except psycopg.errors.UniqueViolation as exc:
            raise ConflictError(f"user_id already exists: {user_id}") from exc
        return AuthenticatedUser(user_id=row["user_id"], username=row["username"])

    def authenticate_user(
        self,
        user_id: str,
        password: str,
        *,
        username: Optional[str] = None,
        create_if_missing: bool = False,
    ) -> AuthenticatedUser:
        user_id = self._normalize_user_id(user_id)
        password = password or ""
        if not password:
            raise AuthenticationError("password is required")

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, password_hash FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()

        if row is None:
            if create_if_missing:
                return self.register_user(user_id=user_id, username=username or user_id, password=password)
            raise AuthenticationError("invalid user_id or password")

        try:
            ok = self.password_hasher.verify(row["password_hash"], password)
        except (VerifyMismatchError, VerificationError):
            ok = False
        if not ok:
            raise AuthenticationError("invalid user_id or password")

        if self.password_hasher.check_needs_rehash(row["password_hash"]):
            self._update_password_hash(user_id, password)

        return AuthenticatedUser(user_id=row["user_id"], username=row["username"])

    def _update_password_hash(self, user_id: str, password: str) -> None:
        password_hash = self.password_hasher.hash(password)
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET password_hash = %s WHERE user_id = %s", (password_hash, user_id))
            conn.commit()

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        user_id = self._normalize_user_id(user_id)
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, created_at FROM users WHERE user_id = %s", (user_id,))
                return cur.fetchone()

    def ensure_direct_conversation(self, user_a: str, user_b: str) -> Dict[str, Any]:
        user_a = self._normalize_user_id(user_a)
        user_b = self._normalize_user_id(user_b)
        if user_a == user_b:
            raise StorageError("self chat is not allowed")
        low, high = sorted((user_a, user_b))

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT chat_id FROM direct_conversations WHERE user_low = %s AND user_high = %s",
                        (low, high),
                    )
                    existing = cur.fetchone()
                    if existing:
                        conn.commit()
                        return self._conversation_payload(existing["chat_id"], user_a, user_b)

                    cur.execute("INSERT INTO conversations(chat_type) VALUES ('direct') RETURNING chat_id")
                    created = cur.fetchone()
                    chat_id = created["chat_id"]
                    cur.execute(
                        """
                        INSERT INTO direct_conversations(user_low, user_high, chat_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_low, user_high) DO NOTHING
                        RETURNING chat_id
                        """,
                        (low, high, chat_id),
                    )
                    inserted = cur.fetchone()
                    if inserted is None:
                        cur.execute("DELETE FROM conversations WHERE chat_id = %s", (chat_id,))
                        cur.execute(
                            "SELECT chat_id FROM direct_conversations WHERE user_low = %s AND user_high = %s",
                            (low, high),
                        )
                        row = cur.fetchone()
                        if not row:
                            raise StorageError("failed to resolve direct conversation after conflict")
                        chat_id = row["chat_id"]
                    cur.execute(
                        """
                        INSERT INTO conversation_participants(chat_id, user_id)
                        VALUES (%s, %s), (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (chat_id, user_a, chat_id, user_b),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._conversation_payload(chat_id, user_a, user_b)

    def _conversation_payload(self, chat_id: int, self_user_id: str, peer_user_id: str) -> Dict[str, Any]:
        peer = self.get_user(peer_user_id)
        return {
            "chat_id": chat_id,
            "chat_type": "direct",
            "peer": {
                "user_id": peer_user_id,
                "username": (peer or {}).get("username", peer_user_id),
            },
        }

    def insert_message(self, chat_id: int, sender_id: str, message_text: str, client_message_id: Optional[str] = None) -> Dict[str, Any]:
        sender_id = self._normalize_user_id(sender_id)
        message_text = (message_text or "").strip()
        if not message_text:
            raise StorageError("message_text is required")

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM conversation_participants WHERE chat_id = %s AND user_id = %s",
                        (chat_id, sender_id),
                    )
                    if cur.fetchone() is None:
                        raise StorageError("sender is not a participant of this chat")

                    cur.execute(
                        """
                        INSERT INTO messages(chat_id, sender_id, message_text, client_message_id)
                        VALUES (%s, %s, %s, %s)
                        RETURNING message_id, chat_id, sender_id, message_text, client_message_id, inserted_at
                        """,
                        (chat_id, sender_id, message_text, client_message_id),
                    )
                    message = cur.fetchone()
                    cur.execute(
                        "UPDATE conversations SET last_message_id = %s WHERE chat_id = %s",
                        (message["message_id"], chat_id),
                    )
                conn.commit()
                return self._message_payload(message)
            except Exception:
                conn.rollback()
                raise

    def get_direct_peer_for_chat(self, chat_id: int, self_user_id: str) -> Optional[Dict[str, Any]]:
        self_user_id = self._normalize_user_id(self_user_id)
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cp.user_id, u.username
                    FROM conversation_participants cp
                    JOIN users u ON u.user_id = cp.user_id
                    WHERE cp.chat_id = %s AND cp.user_id <> %s
                    LIMIT 1
                    """,
                    (chat_id, self_user_id),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def get_latest_messages(self, chat_id: int, requester_user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        requester_user_id = self._normalize_user_id(requester_user_id)
        limit = max(1, min(int(limit or 50), 200))
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM conversation_participants WHERE chat_id = %s AND user_id = %s",
                    (chat_id, requester_user_id),
                )
                if cur.fetchone() is None:
                    raise StorageError("user is not a participant of this chat")
                cur.execute(
                    """
                    SELECT m.message_id, m.chat_id, m.sender_id, u.username AS sender_username,
                           m.message_text, m.client_message_id, m.inserted_at
                    FROM messages m
                    JOIN users u ON u.user_id = m.sender_id
                    WHERE m.chat_id = %s
                    ORDER BY m.inserted_at DESC, m.message_id DESC
                    LIMIT %s
                    """,
                    (chat_id, limit),
                )
                rows = cur.fetchall()
        return [self._message_payload(row) for row in reversed(rows)]

    def get_user_chats(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        user_id = self._normalize_user_id(user_id)
        limit = max(1, min(int(limit or 50), 200))
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.chat_id,
                           c.chat_type,
                           m.message_text AS last_message,
                           m.inserted_at AS last_message_at,
                           m.message_id AS last_message_id,
                           peer.user_id AS peer_user_id,
                           peer_user.username AS peer_username,
                           ucs.last_read_message_id
                    FROM conversation_participants cp
                    JOIN conversations c ON c.chat_id = cp.chat_id
                    LEFT JOIN messages m ON m.message_id = c.last_message_id
                    LEFT JOIN user_conversation_state ucs
                        ON ucs.chat_id = c.chat_id AND ucs.user_id = cp.user_id
                    LEFT JOIN conversation_participants peer
                        ON peer.chat_id = c.chat_id AND peer.user_id <> cp.user_id
                    LEFT JOIN users peer_user ON peer_user.user_id = peer.user_id
                    WHERE cp.user_id = %s
                    ORDER BY m.inserted_at DESC NULLS LAST, c.created_at DESC
                    LIMIT %s
                    """,
                    (user_id, limit),
                )
                rows = cur.fetchall()
        chats: List[Dict[str, Any]] = []
        for row in rows:
            chats.append(
                {
                    "chat_id": row["chat_id"],
                    "chat_type": row["chat_type"],
                    "peer": {
                        "user_id": row.get("peer_user_id"),
                        "username": row.get("peer_username") or row.get("peer_user_id"),
                    },
                    "last_message": row.get("last_message"),
                    "last_message_id": row.get("last_message_id"),
                    "last_message_at": self._iso(row.get("last_message_at")),
                    "last_read_message_id": row.get("last_read_message_id"),
                }
            )
        return chats

    def mark_delivered(self, user_id: str, chat_id: int, message_id: int) -> None:
        user_id = self._normalize_user_id(user_id)
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_conversation_state(user_id, chat_id, last_delivered_message_id, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (user_id, chat_id)
                    DO UPDATE SET
                        last_delivered_message_id = GREATEST(
                            COALESCE(user_conversation_state.last_delivered_message_id, 0),
                            EXCLUDED.last_delivered_message_id
                        ),
                        updated_at = now()
                    """,
                    (user_id, chat_id, message_id),
                )
            conn.commit()

    def mark_read(self, user_id: str, chat_id: int, message_id: int) -> None:
        user_id = self._normalize_user_id(user_id)
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_conversation_state(user_id, chat_id, last_read_message_id, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (user_id, chat_id)
                    DO UPDATE SET
                        last_read_message_id = GREATEST(
                            COALESCE(user_conversation_state.last_read_message_id, 0),
                            EXCLUDED.last_read_message_id
                        ),
                        updated_at = now()
                    """,
                    (user_id, chat_id, message_id),
                )
            conn.commit()

    def _message_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "message_id": row["message_id"],
            "chat_id": row["chat_id"],
            "sender_id": row["sender_id"],
            "sender_username": row.get("sender_username") or row.get("sender_id"),
            "message": row["message_text"],
            "client_message_id": row.get("client_message_id"),
            "inserted_at": self._iso(row.get("inserted_at")),
        }

    @staticmethod
    def _normalize_user_id(user_id: str) -> str:
        normalized = str(user_id or "").strip()
        if not normalized:
            raise StorageError("user_id is required")
        return normalized

    @staticmethod
    def _iso(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
