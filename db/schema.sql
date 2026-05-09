CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conversations (
    chat_id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL DEFAULT 'direct',
    last_message_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS direct_conversations (
    user_low TEXT NOT NULL REFERENCES users(user_id),
    user_high TEXT NOT NULL REFERENCES users(user_id),
    chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
    PRIMARY KEY (user_low, user_high),
    UNIQUE (chat_id)
);

CREATE TABLE IF NOT EXISTS conversation_participants (
    chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
    message_id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
    sender_id TEXT NOT NULL REFERENCES users(user_id),
    receiver_id TEXT NOT NULL REFERENCES users(user_id),
    message_text TEXT NOT NULL,
    client_message_id TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_id_message_id ON messages(chat_id, message_id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_client_message_id ON messages(client_message_id);

CREATE TABLE IF NOT EXISTS user_conversation_state (
    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE,
    last_delivered_message_id BIGINT NOT NULL DEFAULT 0,
    last_read_message_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, chat_id)
);
