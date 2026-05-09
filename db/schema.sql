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
