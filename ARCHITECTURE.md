# Architecture

This document explains the internal architecture of the Real-Time Messaging System: protocol framing, server runtimes, PostgreSQL persistence, Redis routing, delivery semantics, AWS benchmark topology and the main scaling trade-offs.

The project is a raw TCP chat gateway, not an HTTP/WebSocket application. The server keeps long-lived TCP connections open, stores messages in PostgreSQL, uses Redis for online routing/presence and exposes the same JSON message protocol to both the PyQt client and the load tester.

---

## 1. Architecture at a glance

```mermaid
flowchart TD
    Client[PyQt Client / Load Tester Client]
    Protocol[4-byte length-prefixed JSON protocol\nchatapp_core/protocol.py]
    Entry[Server.py\nruntime selector]

    MP[Multi-processing runtime\nchatapp_core/multi_processing_server.py]
    MT[Multi-threading runtime\nchatapp_core/multi_threading_server.py]

    PG[(PostgreSQL\nsource of truth)]
    Redis[(Redis\npresence + pub/sub + bounded backlog)]

    Client -->|TCP| Protocol
    Protocol --> Entry
    Entry -->|CHAT_SERVER_MODE=multi_processing| MP
    Entry -->|CHAT_SERVER_MODE=multi_threading| MT
    MP --> PG
    MT --> PG
    MP --> Redis
    MT -. limited use for slow socket spillover .-> Redis
```

Main idea:

- TCP sockets stay connected for low-latency chat delivery.
- PostgreSQL is the durable source of truth for users, conversations and messages.
- Redis is a control plane for online routing, duplicate-login handling, pub/sub delivery between processes and bounded slow-socket spillover.
- The default runtime is multi-processing. It was the runtime used for the 100k-client AWS benchmark.

---

## 2. Code ownership by file

| File | Responsibility |
|---|---|
| `Server.py` | Entrypoint. Selects multi-processing or multi-threading runtime using `CHAT_SERVER_MODE`. |
| `chatapp_core/protocol.py` | Encodes/decodes TCP frames using a 4-byte big-endian length prefix plus UTF-8 JSON payload. |
| `chatapp_core/storage.py` | PostgreSQL connection pool, schema initialization, authentication, conversations, messages and delivery/read state. |
| `chatapp_core/multi_processing_server.py` | Default high-concurrency runtime. Uses `SO_REUSEPORT`, one selector loop per process, Redis presence and Redis pub/sub routing. |
| `chatapp_core/multi_threading_server.py` | Alternative single-process runtime. Uses multiple selector-based reactor threads with cross-thread task queues and socketpair wakeups. |
| `chatApp.py` | PyQt desktop client with login, chat list, history fetch, message sending, pending state and delivery ticks. |
| `load_tester/load_test.py` | Multiprocess virtual-client load generator with ramp-up, measurement window filtering and multi-machine user ranges. |
| `load_tester/merge_results.py` | Merges result JSON files from multiple load generators. |
| `db/schema.sql` | SQL schema equivalent to the schema initialized by `PostgresStorage`. |

---

## 3. Protocol layer

File: `chatapp_core/protocol.py`

Each application message is framed as:

```text
[4-byte unsigned big-endian payload length][UTF-8 JSON payload]
```

Why this exists:

- TCP is a byte stream. It does not preserve application message boundaries.
- A single `recv()` can return a partial JSON object, one full JSON object or multiple JSON objects.
- The 4-byte length prefix tells the receiver exactly how many bytes to read for the next JSON payload.
- This avoids fragile delimiter-based parsing.

Important implementation details:

| Item | Value |
|---|---|
| Header format | `struct.Struct('!I')` |
| Max payload size | `1 MiB` |
| Encoding | UTF-8 JSON |
| Streaming parser | `MessageReader.feed(data)` buffers partial frames and returns complete decoded messages. |

Example logical payload:

```json
{
  "type": "send_message",
  "to_user_id": "25001",
  "message": "hello",
  "client_message_id": "client-generated-id"
}
```

---

## 4. Runtime selection

File: `Server.py`

```text
CHAT_SERVER_MODE=multi_processing   -> chatapp_core/multi_processing_server.py
CHAT_SERVER_MODE=multi_threading    -> chatapp_core/multi_threading_server.py
```

Default behavior:

```text
CHAT_SERVER_MODE is omitted -> multi_processing
```

The two runtimes expose the same client protocol and use the same PostgreSQL storage layer. This makes the runtime strategy replaceable without changing the client.

---

## 5. Multi-processing runtime

File: `chatapp_core/multi_processing_server.py`

This is the primary runtime and the one used for the AWS benchmark.

```mermaid
flowchart TD
    LB[Linux kernel accept distribution\nSO_REUSEPORT]
    P1[Process 1\nselector + local_users]
    P2[Process 2\nselector + local_users]
    Pn[Process N\nselector + local_users]
    Redis[(Redis\npresence + route channels)]
    PG[(PostgreSQL)]

    ClientA[Client A] --> LB
    ClientB[Client B] --> LB
    LB --> P1
    LB --> P2
    LB --> Pn

    P1 --> PG
    P2 --> PG
    Pn --> PG

    P1 <--> Redis
    P2 <--> Redis
    Pn <--> Redis
```

### 5.1 Process model

Each worker process:

- binds the same host/port using `SO_REUSEPORT`.
- owns its own `selectors.DefaultSelector()` loop.
- owns its accepted sockets.
- owns an in-memory `local_users` map for users connected to that process.
- owns an independent PostgreSQL connection pool.
- owns a Redis pub/sub subscription channel.

A socket is never shared between processes after accept. The process that accepted the socket owns all reads and writes for that client.

### 5.2 Redis control plane

The multi-processing runtime uses Redis keys/channels with the `CHAT_CONTROL_PREFIX` prefix. The default prefix is `chat`.

| Purpose | Shape |
|---|---|
| Presence key | `chat:presence:<user_id>` |
| Process route channel | `chat:route:<hostname>:<pid>:<worker_index>` |
| Slow-socket backlog key | `chat:backlog:<user_id>` |

Presence payload contains:

```json
{
  "process_id": "hostname:pid:index",
  "channel": "chat:route:hostname:pid:index",
  "username": "user name",
  "session_id": "uuid-like-session-token",
  "ts": 1710000000.0
}
```

The `session_id` is important. It prevents an older duplicate-login connection from deleting or refreshing a newer connection's presence key.

### 5.3 Login flow

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Server Process
    participant DB as PostgreSQL
    participant R as Redis

    C->>P: login(user_id, password, create_if_missing)
    P->>DB: authenticate_user(...)
    DB-->>P: user row / auto-created user
    P->>R: SET chat:presence:user_id {process_id, channel, session_id} EX ttl
    alt old presence existed
        P->>R: publish kick_user to old process channel
    end
    P->>DB: get_user_chats(user_id)
    P-->>C: login_ok(self, chats, worker_id, process_id)
    P->>R: drain bounded backlog for this user
```

### 5.4 Send-message flow

The server always writes to PostgreSQL before attempting online delivery.

```mermaid
sequenceDiagram
    participant S as Sender Client
    participant PS as Sender Process
    participant DB as PostgreSQL
    participant R as Redis
    participant PR as Receiver Process
    participant RC as Receiver Client

    S->>PS: send_message(to_user_id, message, client_message_id)
    PS->>DB: ensure_direct_conversation(...)
    PS->>DB: insert_message(...)
    PS->>DB: update conversations.last_message_id

    alt receiver connected to same process
        PS->>RC: queue frame to local socket
        PS-->>S: delivery_status queued_to_socket
    else receiver presence found in Redis
        PS->>R: publish deliver_frame to receiver process channel
        R->>PR: deliver_frame
        PR->>RC: queue frame to local socket
        PS-->>S: delivery_status published_to_process
    else receiver offline / missing presence
        PS-->>S: delivery_status stored
    end

    RC-->>PR: message_received_ack
    PR->>R: route delivered_to_client back to sender if online
```

### 5.5 Local delivery path

For a recipient connected to the same process:

```text
send_message
  -> insert message into PostgreSQL
  -> find recipient in local_users
  -> encode chat_message frame
  -> append frame to recipient.write_queue
  -> enable EVENT_WRITE interest on recipient socket
  -> selector flushes queued bytes when socket is writable
```

The code does not call `sendall()` in the hot path. It appends to a write queue and lets the selector flush bytes when the kernel says the socket is writable.

### 5.6 Cross-process delivery path

For a recipient connected to another process:

```text
send_message
  -> insert message into PostgreSQL
  -> Redis GET chat:presence:<recipient_id>
  -> publish encoded frame to presence.channel
  -> recipient process receives pub/sub message
  -> recipient process finds local_users[recipient_id]
  -> recipient process appends frame to that socket's write_queue
```

The sender process does not know or touch the receiver socket. It only publishes the already-encoded frame to the process that owns the receiver.

### 5.7 Backpressure and bounded Redis backlog

Each connected session has:

- `write_queue`
- `queued_bytes`
- high watermark: `CHAT_WRITE_HIGH_WATERMARK_BYTES`, default `512 KiB`
- low watermark: `CHAT_WRITE_LOW_WATERMARK_BYTES`, default `256 KiB`

If a connected user's write queue grows above the high watermark, the frame is pushed to Redis backlog:

```text
chat:backlog:<user_id>
```

When the socket drains below the low watermark, the server pops a bounded batch from Redis and queues those frames back to the socket.

Important distinction:

- PostgreSQL is durable offline storage.
- Redis backlog is a bounded spillover mechanism for already-online users whose socket is temporarily slow.

### 5.8 Presence refresh

Presence keys use a TTL. The server periodically refreshes presence for active sessions.

The current implementation avoids synchronized Redis spikes by using:

| Setting | Role |
|---|---|
| `CHAT_PRESENCE_TTL_SECONDS` | TTL for presence keys. |
| `CHAT_PRESENCE_REFRESH_SECONDS` | Base refresh interval. |
| `CHAT_PRESENCE_REFRESH_SPREAD_SECONDS` | Spreads refresh deadlines using a deterministic offset. |
| `CHAT_PRESENCE_REFRESH_BATCH` | Maximum presence entries refreshed per maintenance tick. |
| `CHAT_PRESENCE_MAINTENANCE_TICK_SECONDS` | How often the process checks for due refreshes. |

Refresh is safe because it rewrites the full presence value only when the current Redis key is missing or still belongs to the same `process_id` and `session_id`.

---

## 6. Multi-threading runtime

File: `chatapp_core/multi_threading_server.py`

The multi-threading runtime is an alternative single-process design.

```mermaid
flowchart TD
    Accept[Accept loop]
    W1[ReactorWorker 1\nselector + sockets]
    W2[ReactorWorker 2\nselector + sockets]
    Wn[ReactorWorker N\nselector + sockets]
    Router[ShardedRouter\nuser_id -> worker/session]
    PG[(Shared PostgreSQL pool)]
    Redis[(Redis backlog spillover)]

    Accept -->|round-robin| W1
    Accept -->|round-robin| W2
    Accept -->|round-robin| Wn
    W1 <--> Router
    W2 <--> Router
    Wn <--> Router
    W1 --> PG
    W2 --> PG
    Wn --> PG
    W1 -. slow socket spillover .-> Redis
    W2 -. slow socket spillover .-> Redis
```

Key rules:

- A worker thread owns its sockets.
- Another thread never writes directly to a socket it does not own.
- Cross-thread delivery posts a task into the destination worker queue.
- A `socketpair()` wakeup notifies the destination selector that work is pending.
- Actual socket writes happen from the owner worker when the socket becomes writable.

This avoids unsafe cross-thread socket writes and keeps socket ownership simple.

### Cross-thread delivery sequence

```text
sender worker receives send_message
  -> store message in PostgreSQL
  -> ShardedRouter finds receiver's owning worker
  -> sender worker posts delivery task into receiver worker's task queue
  -> sender worker writes to receiver worker's wakeup socket
  -> receiver selector wakes up
  -> receiver worker drains task queue
  -> receiver worker appends frame to receiver.write_queue
  -> receiver worker flushes when socket is writable
```

---

## 7. PostgreSQL storage model

File: `chatapp_core/storage.py`

PostgreSQL is the durable source of truth.

```mermaid
flowchart TD
    Users[users]
    Conversations[conversations]
    Direct[direct_conversations]
    Participants[conversation_participants]
    Messages[messages]
    State[user_conversation_state]

    Users --> Participants
    Conversations --> Direct
    Conversations --> Participants
    Conversations --> Messages
    Users --> Messages
    Users --> State
    Conversations --> State
```

### 7.1 Tables

```text
users
  user_id TEXT PRIMARY KEY
  username TEXT NOT NULL
  password_hash TEXT NOT NULL
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()

conversations
  chat_id BIGSERIAL PRIMARY KEY
  chat_type TEXT NOT NULL CHECK (chat_type IN ('direct', 'group'))
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  last_message_id BIGINT

direct_conversations
  user_low TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
  user_high TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
  chat_id BIGINT NOT NULL UNIQUE REFERENCES conversations(chat_id) ON DELETE CASCADE
  PRIMARY KEY (user_low, user_high)
  CHECK (user_low < user_high)

conversation_participants
  chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE
  user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
  joined_at TIMESTAMPTZ NOT NULL DEFAULT now()
  PRIMARY KEY (chat_id, user_id)

messages
  message_id BIGSERIAL PRIMARY KEY
  chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE
  sender_id TEXT NOT NULL REFERENCES users(user_id)
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
  message_text TEXT NOT NULL
  client_message_id TEXT

user_conversation_state
  user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE
  chat_id BIGINT NOT NULL REFERENCES conversations(chat_id) ON DELETE CASCADE
  last_read_message_id BIGINT
  last_delivered_message_id BIGINT
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  PRIMARY KEY (user_id, chat_id)
```

### 7.2 Important indexes

```sql
CREATE INDEX IF NOT EXISTS idx_conversation_participants_user
    ON conversation_participants(user_id, chat_id);

CREATE INDEX IF NOT EXISTS idx_messages_chat_latest
    ON messages(chat_id, inserted_at DESC, message_id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_client_message_id
    ON messages(client_message_id) WHERE client_message_id IS NOT NULL;
```

### 7.3 Direct conversation creation

Direct conversations are normalized using sorted user IDs:

```text
low, high = sorted(user_a, user_b)
PRIMARY KEY (user_low, user_high)
```

This prevents duplicate direct chats like `(1, 2)` and `(2, 1)`.

The multi-processing runtime also keeps an LRU direct-conversation cache to avoid repeatedly resolving the same user-pair in PostgreSQL during load tests.

---

## 8. Delivery semantics

A server ACK means the server accepted and processed the message. It does not always mean the receiver client has read it.

| Status | Meaning |
|---|---|
| `stored` | Message was committed to PostgreSQL. Recipient was offline or presence was missing. |
| `queued_to_socket` | Message was committed and queued to a local recipient socket. |
| `published_to_process` | Message was committed and published to the process that owns the recipient. |
| `delivered_to_client` | Receiver client read the message frame and sent `message_received_ack`. |
| `failed` | Validation, auth or database write failed. |

This distinction is intentional. Redis publish success is not the same as user-visible delivery.

### ACK persistence

`CHAT_PERSIST_DELIVERY_ACKS` controls whether `message_received_ack` updates are written to PostgreSQL.

| Value | Behavior |
|---|---|
| `1` | Persist `last_delivered_message_id` into `user_conversation_state`. Better correctness. |
| `0` | Skip delivery-state DB writes. Useful for pure transport/routing benchmarks. |

The AWS 100k benchmark used `CHAT_PERSIST_DELIVERY_ACKS=0` to avoid turning delivery ACK persistence into the bottleneck.

---

## 9. Offline delivery model

Offline delivery is PostgreSQL-backed.

```text
user offline
  -> sender stores message in PostgreSQL
  -> delivery_status=stored
  -> receiver reconnects later
  -> client fetches chats/history
  -> server reads messages from PostgreSQL
```

Redis is not the durable offline queue. Redis backlog is only a bounded temporary spillover for online users with slow sockets or stale process-channel edge cases.

---

## 10. Authentication model

Passwords are stored as Argon2 encoded hashes.

Supported client actions:

```text
register
login
```

For load testing:

```text
CHAT_AUTO_CREATE_USERS=1
```

This allows generated clients to login with `create_if_missing=true` so users can be created during warm-up runs.


---

## 11. AWS benchmark architecture

The included 100k-client benchmark used one server instance and four load-generator instances in the same Availability Zone.

```mermaid
flowchart LR
    L1[rtms-load-1\nm7i.4xlarge\n25k clients]
    L2[rtms-load-2\nm7i.4xlarge\n25k clients]
    L3[rtms-load-3\nm7i.4xlarge\n25k clients]
    L4[rtms-load-4\nm7i.4xlarge\n25k clients]

    S[rtms-server-8x\nm7i.8xlarge\n24 server processes]
    PG[(PostgreSQL Docker\n127.0.0.1:5432)]
    R[(Redis Docker\n127.0.0.1:6379)]

    L1 -->|TCP 12345 private IP| S
    L2 -->|TCP 12345 private IP| S
    L3 -->|TCP 12345 private IP| S
    L4 -->|TCP 12345 private IP| S
    S --> PG
    S --> R
```

| Role | Instance | Type | Count | Notes |
|---|---|---:|---:|---|
| Server | `rtms-server-8x` | `m7i.8xlarge` | 1 | Runs `Server.py`, PostgreSQL Docker container and Redis Docker container. |
| Load generator | `rtms-load-1` to `rtms-load-4` | `m7i.4xlarge` | 4 | Each creates 25k TCP clients using 8 local load-test processes. |

Benchmark placement:

```text
Availability Zone: ap-south-1a
Server private IP used by load generators: 172.31.39.180
Server port: 12345
```

Required OS setting on server and every load-generator shell:

```bash
ulimit -n 200000
```

Reason: every virtual client uses a socket/file descriptor. The server also needs enough descriptors for 100k accepted sockets plus service connections.

---

## 12. Server configuration used for the benchmark

PostgreSQL and Redis were started with Docker before running the server.

Server runtime configuration:

```bash
export DATABASE_URL="postgresql://chatapp:chatapp@127.0.0.1:5432/chatapp"
export REDIS_URL="redis://127.0.0.1:6379/0"
export CHAT_DB_AUTO_INIT=1
export CHAT_AUTO_CREATE_USERS=1
export CHAT_SERVER_MODE=multi_processing
export CHAT_PROCESSES=24
export CHAT_HOST="0.0.0.0"
export CHAT_PORT=12345
export CHAT_IDLE_TIMEOUT_SECONDS=3600
export CHAT_PRESENCE_TTL_SECONDS=900
export CHAT_PRESENCE_REFRESH_SECONDS=60
export CHAT_PRESENCE_REFRESH_SPREAD_SECONDS=120
export CHAT_PRESENCE_REFRESH_BATCH=1000
export CHAT_PRESENCE_MAINTENANCE_TICK_SECONDS=1.0
export CHAT_PERSIST_DELIVERY_ACKS=0

python3 Server.py
```

Important architectural effect of these values:

| Setting | Effect |
|---|---|
| `CHAT_PROCESSES=24` | Creates 24 independent selector processes on the same port using `SO_REUSEPORT`. |
| `CHAT_IDLE_TIMEOUT_SECONDS=3600` | Prevents idle load-test sockets from being closed during long benchmark runs. |
| `CHAT_PRESENCE_TTL_SECONDS=900` | Keeps presence stable during large tests even if refresh is delayed. |
| `CHAT_PRESENCE_REFRESH_SPREAD_SECONDS=120` | Avoids all 100k clients refreshing presence at the same instant. |
| `CHAT_PRESENCE_REFRESH_BATCH=1000` | Caps refresh work per maintenance tick. |
| `CHAT_PERSIST_DELIVERY_ACKS=0` | Measures transport/routing without delivery-state DB write amplification. |

---

## 13. Load-test architecture

The load test was split across four machines.

| Machine | User range | `--start-index` | `--clients` | `--processes` | `--total-clients` |
|---|---:|---:|---:|---:|---:|
| `rtms-load-1` | `1-25000` | `1` | `25000` | `8` | `100000` |
| `rtms-load-2` | `25001-50000` | `25001` | `25000` | `8` | `100000` |
| `rtms-load-3` | `50001-75000` | `50001` | `25000` | `8` | `100000` |
| `rtms-load-4` | `75001-100000` | `75001` | `25000` | `8` | `100000` |

Recipient mode used:

```text
--recipient-mode fixed-offsets
--fixed-recipient-count 10
--fixed-recipient-step 1000
```

This limits each sender to deterministic offset-based recipients instead of creating a fully random 100k-user conversation graph. It makes the benchmark more stable and reduces uncontrolled conversation-cardinality explosion in PostgreSQL.

### Warm-up requirement

Run 2-3 warm-up tests before the measured run.

Warm-ups are needed to:

- create all generated users.
- create direct conversations used by the fixed-offset recipient pattern.
- populate PostgreSQL indexes/table pages.
- absorb expensive first-run Argon2 hashing cost.
- verify connection limits, Redis presence stability and DB readiness before recording final numbers.

Do not use warm-up results as final benchmark results.

### Measurement window

The final run used these timing parameters:

```text
--duration 1200
--ramp-up 600
--min-first-send-delay 120
--max-first-send-delay 240
--mean-message-gap 50
--noise-period 60
--drain-seconds 120
```

Measured send window:

```text
duration - ramp_up - max_first_send_delay - noise_period
= 1200 - 600 - 240 - 60
= 300 seconds
```

`drain-seconds` keeps clients connected after the send window so late ACKs and received messages can be counted.

---

## 14. Included benchmark result

Benchmark file:

```text
load_tester/benchmark_results/aws-100k-gap50-global.json
```

| Metric | Value |
|---|---:|
| Target clients | `100,000` |
| Connected clients | `100,000` |
| Successful logins | `100,000` |
| Send attempts | `602,851` |
| Server ACKs | `602,851` |
| Received messages | `602,802` |
| Offered load | `~2,007.88 msg/s` |
| ACK throughput | `~2,007.88 msg/s` |
| Receive throughput | `~2,007.71 msg/s` |
| ACK success rate | `100.000%` |
| Receive success rate | `~99.992%` |
| ACK p50 | `~10.39 ms` |
| ACK p95 | `~34.84 ms` |
| ACK p99 | `~212.38 ms` |
| Receive p50 | `~20 ms` |
| Receive p95 | `~52 ms` |
| Receive p99 | `~297 ms` |
| Errors | `0` |
| Disconnects | `0` |
| Login timeouts | `0` |
| Offline count | `0` |
---