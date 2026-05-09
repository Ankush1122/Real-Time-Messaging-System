# Real-Time Messaging System

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-source%20of%20truth-blue)
![Redis](https://img.shields.io/badge/Redis-presence%20%2B%20pubsub-red)
![Benchmark](https://img.shields.io/badge/Benchmark-100k%20clients-green)

A high-concurrency TCP chat gateway built in Python. The project includes a custom socket protocol, two interchangeable server runtimes, PostgreSQL persistence, Redis-based online routing, a PyQt desktop client and a multi-process load tester used for a 100k-client AWS benchmark.

This is mainly a systems/backend project. The interesting parts are socket framing, selector-based I/O, cross-thread/cross-process message routing, Redis presence, PostgreSQL-backed chat history and benchmark tooling.

---

## Table of contents

- [What this project demonstrates](#what-this-project-demonstrates)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Project layout](#project-layout)
- [Message protocol](#message-protocol)
- [Server runtimes](#server-runtimes)
- [Persistence model](#persistence-model)
- [Delivery semantics](#delivery-semantics)
- [AWS benchmark result](#aws-benchmark-result)
- [AWS setup used for benchmark](#aws-setup-used-for-benchmark)
- [Install locally](#install-locally)
- [Run PostgreSQL and Redis with Docker](#run-postgresql-and-redis-with-docker)
- [Run the server](#run-the-server)
- [Run the PyQt client](#run-the-pyqt-client)
- [Load testing](#load-testing)
- [Merge results from multiple load generators](#merge-results-from-multiple-load-generators)
- [Important load-test parameters](#important-load-test-parameters)
- [Useful monitoring commands](#useful-monitoring-commands)
- [Known limitations](#known-limitations)

---

## What this project demonstrates

- Custom TCP application protocol using length-prefixed JSON frames.
- Selector-based socket server with non-blocking reads and writes.
- Two runtime designs:
  - multi-processing with `SO_REUSEPORT`.
  - multi-threading with per-thread reactors.
- PostgreSQL as the source of truth for users, conversations and messages.
- Redis for online presence, pub/sub routing, duplicate-login safety and bounded slow-socket spillover.
- PyQt desktop client with login, chat list, history fetch, delivery ticks and message ACKs.
- Multiprocess load tester that can split users across multiple EC2 load generators.
- 100k-client AWS benchmark with roughly 2k messages/second offered load.

---

## Architecture

```text
+-----------------------+
|     PyQt Client       |
| chatApp.py            |
+----------+------------+
           |
           | TCP socket
           | 4-byte length prefix + UTF-8 JSON
           v
+----------+------------+
|       Server.py        |
| runtime selector       |
+----------+------------+
           |
           +-------------------------------+
           |                               |
           v                               v
+-----------------------+       +--------------------------+
| Multi-processing mode |       | Multi-threading mode     |
| SO_REUSEPORT          |       | ReactorWorker threads    |
| Redis pub/sub         |       | cross-thread task queues |
+----------+------------+       +-------------+------------+
           |                                  |
           +------------------+---------------+
                              |
                              v
                +-------------+--------------+
                | PostgreSQL storage layer   |
                | chatapp_core/storage.py    |
                +-------------+--------------+
                              |
                              v
                users / conversations / messages

Redis side plane:
  - online presence
  - process channel lookup
  - pub/sub message forwarding
  - duplicate-login safety
  - bounded backpressure spillover
```

### Runtime selection

`Server.py` selects the runtime using `CHAT_SERVER_MODE`.

```text
CHAT_SERVER_MODE=multi_processing   -> chatapp_core/multi_processing_server.py
CHAT_SERVER_MODE=multi_threading    -> chatapp_core/multi_threading_server.py
```

The default mode is `multi_processing`.

---

## Tech stack

| Area | Technology |
|---|---|
| Language | Python 3.11+ |
| Network protocol | Raw TCP sockets |
| I/O model | `selectors` |
| Server concurrency | multiprocessing or multithreading |
| Persistence | PostgreSQL |
| DB driver | `psycopg[binary,pool]` |
| Password hashing | Argon2 via `argon2-cffi` |
| Routing/cache side plane | Redis |
| Desktop client | PyQt5 |
| Load generation | custom multiprocess Python load tester |

---

## Project layout

```text
.
├── Server.py
├── chatApp.py
├── requirements.txt
├── db/
│   └── schema.sql
├── chatapp_core/
│   ├── protocol.py
│   ├── storage.py
│   ├── multi_processing_server.py
│   └── multi_threading_server.py
├── load_tester/
│   ├── load_test.py
│   ├── merge_results.py
│   └── benchmark_results/
├── ARCHITECTURE.md
```

| File | Purpose |
|---|---|
| `Server.py` | Entrypoint. Selects multi-processing or multi-threading runtime. |
| `chatApp.py` | PyQt desktop chat client. |
| `chatapp_core/protocol.py` | 4-byte length-prefixed JSON frame encoder/decoder. |
| `chatapp_core/storage.py` | PostgreSQL connection pool, schema init, auth, conversations, messages. |
| `chatapp_core/multi_processing_server.py` | High-concurrency process-based server using `SO_REUSEPORT` and Redis pub/sub. |
| `chatapp_core/multi_threading_server.py` | Selector-based multi-threaded server using per-worker reactors. |
| `load_tester/load_test.py` | Multiprocess virtual-client load generator. |
| `load_tester/merge_results.py` | Merges per-loader/per-process benchmark JSON files. |
| `db/schema.sql` | SQL schema matching the storage layer. |

---

## Message protocol

The project does not use HTTP or WebSocket. It uses a custom TCP protocol.

Each frame is:

```text
[4-byte unsigned big-endian payload length][UTF-8 JSON payload]
```

Why this matters:

- TCP is a byte stream, not a message stream.
- One `recv()` call may contain half a JSON object, one JSON object or many JSON objects.
- The 4-byte length prefix tells the receiver exactly how many bytes belong to the next JSON payload.
- This avoids delimiter bugs and reduces fragile parsing logic.

Example JSON payload:

```json
{
  "type": "send_message",
  "to_user_id": "2",
  "message": "hello"
}
```

---

## Server runtimes

### 1. Multi-processing runtime

File: `chatapp_core/multi_processing_server.py`

This is the default runtime used for the 100k-client AWS test.

```text
N worker processes
  -> all bind same host:port with SO_REUSEPORT
  -> Linux distributes new TCP connections across processes
  -> each process owns its own selector loop
  -> each process owns its local connected-user map
  -> Redis stores user_id -> process channel presence
  -> Redis pub/sub forwards messages across processes
```

Important behavior:

- Each accepted socket is owned by exactly one process.
- Other processes never write directly to that socket.
- Cross-process delivery happens through Redis pub/sub.
- Redis presence includes a `session_id` so old duplicate-login sessions do not delete newer sessions.
- Presence refresh is staggered in bounded batches to avoid huge Redis spikes.
- Redis backlog is bounded spillover for slow online sockets.

### 2. Multi-threading runtime

File: `chatapp_core/multi_threading_server.py`

```text
accept loop
  -> round-robin assignment to ReactorWorker threads
  -> each worker owns a selector
  -> each worker owns its connected sockets
  -> cross-thread delivery posts a task into the destination worker queue
  -> socketpair wakeup notifies the destination selector
```

Important behavior:

- One thread does not write directly to another thread's socket.
- Cross-thread messages are appended to the owner worker's task queue.
- The destination worker wakes, drains its task queue, then appends bytes to the connection's write queue.
- Actual socket writes happen when the selector says the socket is writable.

---

## Persistence model

PostgreSQL is the source of truth.

Main tables:

```text
users
conversations
direct_conversations
conversation_participants
messages
user_conversation_state
```

Simplified flow for a direct message:

```text
send_message
  -> validate authenticated sender
  -> resolve or create direct conversation
  -> insert message into PostgreSQL
  -> update conversations.last_message_id
  -> route to online receiver if presence exists
  -> sender receives server ACK
  -> receiver sends message_received_ack after reading frame
  -> sender may receive delivered_to_client if still online
```

Authentication uses Argon2 encoded password hashes. Missing users can be auto-created during demos or load tests when `CHAT_AUTO_CREATE_USERS=1`.

---

## Delivery semantics

Server ACK status values are intentionally separated from actual client delivery.

| Status | Meaning |
|---|---|
| `stored` | Message was committed to PostgreSQL. Receiver was not routable online. |
| `queued_to_socket` | Message was committed then queued to a local recipient socket. |
| `published_to_process` | Message was committed then published to the process that owns the recipient. |
| `delivered_to_client` | Receiver client read the message frame then acknowledged it. |
| `failed` | Validation or DB write failed. |

Redis publish success is not treated as final delivery. True client delivery requires a `message_received_ack` from the receiver.

---

## AWS benchmark result

Benchmark file included in repo:

```text
load_tester/benchmark_results/aws-100k-gap50-global.json
```

### Result summary

| Metric | Value |
|---|---:|
| Target clients | 100,000 |
| Connected clients | 100,000 |
| Successful logins | 100,000 |
| Send attempts | 602,851 |
| Server ACKs | 602,851 |
| Received messages | 602,802 |
| Offered load | ~2,007.88 msg/s |
| ACK throughput | ~2,007.88 msg/s |
| Receive throughput | ~2,007.71 msg/s |
| ACK success rate | 100.000% |
| Receive success rate | ~99.992% |
| ACK p50 | ~10.39 ms |
| ACK p95 | ~34.84 ms |
| ACK p99 | ~212.38 ms |
| Receive p50 | ~20 ms |
| Receive p95 | ~52 ms |
| Receive p99 | ~297 ms |
| Errors | 0 |
| Disconnects | 0 |
| Login timeouts | 0 |
| Offline count | 0 |


---

## AWS setup used for benchmark

The 100k-client test used one server instance plus four load-generator instances in the same AWS Availability Zone.

| Role | Name | Instance type | Count |
|---|---|---:|---:|
| Server | `rtms-server-8x` | `m7i.8xlarge` | 1 |
| Load generator | `rtms-load-1` to `rtms-load-4` | `m7i.4xlarge` | 4 |

AWS placement used during the test:

```text
Availability Zone: ap-south-1a
Server private IP used by load generators: 172.31.39.180
Server port: 12345
```

Security group shape:

| Target | Rule |
|---|---|
| Server | allow inbound TCP `12345` from the load-generator security group or private IPs. |
| Server | allow inbound SSH `22` only from your own IP. |
| PostgreSQL | keep bound to `127.0.0.1` on the server instance. |
| Redis | keep bound to `127.0.0.1` on the server instance. |

Use private IP traffic inside the VPC for the benchmark. Public IP traffic adds unnecessary network variability.

---

## Install locally

```bash
git clone https://github.com/Ankush1122/Real-Time-Messaging-System.git
cd Real-Time-Messaging-System

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

For high connection counts, raise the file-descriptor limit in every shell that runs the server or a load tester:

```bash
ulimit -n 200000
```

Check it:

```bash
ulimit -n
```

If the command fails, your hard limit is lower than 200000. Increase the OS/user limit first, then open a new shell.

---

## Run PostgreSQL and Redis with Docker

Run these on the server machine before starting `Server.py`.

```bash
sudo systemctl start docker
```

PostgreSQL:

```bash
docker run -d \
  --name rtms-postgres \
  -e POSTGRES_USER=chatapp \
  -e POSTGRES_PASSWORD=chatapp \
  -e POSTGRES_DB=chatapp \
  -p 127.0.0.1:5432:5432 \
  -v rtms_pgdata:/var/lib/postgresql/data \
  postgres:16
```

Redis:

```bash
docker run -d \
  --name rtms-redis \
  -p 127.0.0.1:6379:6379 \
  redis:7
```

Verify both services:

```bash
docker ps
```

Optional quick checks:

```bash
docker exec -it rtms-postgres psql -U chatapp -d chatapp -c "select 1;"
docker exec -it rtms-redis redis-cli ping
```

---

## Run the server

Set the file-descriptor limit first:

```bash
ulimit -n 200000
```

Export the benchmark server environment:

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
```

Start the server:

```bash
python3 Server.py
```

### What the important server env vars do

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string. |
| `REDIS_URL` | Redis connection string. |
| `CHAT_DB_AUTO_INIT=1` | Auto-create schema on startup. |
| `CHAT_AUTO_CREATE_USERS=1` | Allow load-test users to be created on login. |
| `CHAT_SERVER_MODE=multi_processing` | Use the process-based runtime. |
| `CHAT_PROCESSES=24` | Start 24 server worker processes. |
| `CHAT_HOST=0.0.0.0` | Listen on all network interfaces. |
| `CHAT_PORT=12345` | TCP port exposed to clients/load testers. |
| `CHAT_IDLE_TIMEOUT_SECONDS=3600` | Keep idle load-test sockets alive for long runs. |
| `CHAT_PRESENCE_TTL_SECONDS=900` | Redis presence key TTL. |
| `CHAT_PRESENCE_REFRESH_SECONDS=60` | Base presence refresh interval. |
| `CHAT_PRESENCE_REFRESH_SPREAD_SECONDS=120` | Spread refresh deadlines to avoid synchronized Redis bursts. |
| `CHAT_PRESENCE_REFRESH_BATCH=1000` | Max presence keys refreshed per maintenance tick. |
| `CHAT_PERSIST_DELIVERY_ACKS=0` | Skip DB writes for delivery ACKs during pure transport benchmarking. |

For normal correctness testing, use `CHAT_PERSIST_DELIVERY_ACKS=1`. For the AWS transport benchmark above, it was set to `0` to avoid turning delivery ACK persistence into the bottleneck.

---

## Run the PyQt client

Start PostgreSQL, Redis and the server first. Then run:

```bash
python3 chatApp.py
```

The client supports login, chat list fetch, starting direct chats, fetching message history, showing local pending messages and delivery ticks.

---

## Load testing

The load tester is in:

```text
load_tester/load_test.py
```

It creates many virtual TCP clients, logs them in, sends messages with a controlled distribution, tracks ACK/receive latency and writes JSON result files.

### Required before every large run

Run this on the server and all load-generator machines:

```bash
ulimit -n 200000
```

### 2-3 warm-up tests are needed

Before recording the final benchmark, run 2-3 warm-up tests using the same user ranges and recipient mode.

Warm-ups are needed to:

- create all 100k users through auto-create login.
- create the fixed direct conversations used by the benchmark.
- initialize PostgreSQL tables/index pages under realistic access.
- reduce first-run Argon2 hashing cost during the measured run.
- expose presence/connection issues before the final benchmark.

Do not report warm-up runs as final benchmark numbers.

### User ranges across four load generators

| Machine | Users | `--start-index` | `--clients` | `--total-clients` | `--run-id` |
|---|---:|---:|---:|---:|---|
| `rtms-load-1` | `1-25000` | `1` | `25000` | `100000` | `aws-100k-gap50-load-1` |
| `rtms-load-2` | `25001-50000` | `25001` | `25000` | `100000` | `aws-100k-gap50-load-2` |
| `rtms-load-3` | `50001-75000` | `50001` | `25000` | `100000` | `aws-100k-gap50-load-3` |
| `rtms-load-4` | `75001-100000` | `75001` | `25000` | `100000` | `aws-100k-gap50-load-4` |

### Warm-up command template

Run this on each load generator. Change only `--start-index` and `--run-id` per machine.

Example for `rtms-load-1`:

```bash
ulimit -n 200000

python3 -m load_tester.load_test \
  --host 172.31.39.180 \
  --port 12345 \
  --clients 25000 \
  --processes 8 \
  --start-index 1 \
  --total-clients 100000 \
  --password loadtest \
  --duration 1200 \
  --ramp-up 600 \
  --min-first-send-delay 120 \
  --max-first-send-delay 240 \
  --mean-message-gap 50 \
  --noise-period 60 \
  --drain-seconds 120 \
  --recipient-mode fixed-offsets \
  --fixed-recipient-count 10 \
  --fixed-recipient-step 1000 \
  --run-id warmup-100k-gap50-load-1
```

Repeat warm-up 2-3 times. Use unique run IDs such as:

```text
warmup-100k-gap50-load-1-run-1
warmup-100k-gap50-load-1-run-2
warmup-100k-gap50-load-1-run-3
```

### Final AWS benchmark commands

Run these commands at roughly the same time on the four load-generator instances.

`rtms-load-1`:

```bash
ulimit -n 200000

python3 -m load_tester.load_test \
  --host 172.31.39.180 \
  --port 12345 \
  --clients 25000 \
  --processes 8 \
  --start-index 1 \
  --total-clients 100000 \
  --password loadtest \
  --duration 1200 \
  --ramp-up 600 \
  --min-first-send-delay 120 \
  --max-first-send-delay 240 \
  --mean-message-gap 50 \
  --noise-period 60 \
  --drain-seconds 120 \
  --recipient-mode fixed-offsets \
  --fixed-recipient-count 10 \
  --fixed-recipient-step 1000 \
  --run-id aws-100k-gap50-load-1
```

`rtms-load-2`:

```bash
ulimit -n 200000

python3 -m load_tester.load_test \
  --host 172.31.39.180 \
  --port 12345 \
  --clients 25000 \
  --processes 8 \
  --start-index 25001 \
  --total-clients 100000 \
  --password loadtest \
  --duration 1200 \
  --ramp-up 600 \
  --min-first-send-delay 120 \
  --max-first-send-delay 240 \
  --mean-message-gap 50 \
  --noise-period 60 \
  --drain-seconds 120 \
  --recipient-mode fixed-offsets \
  --fixed-recipient-count 10 \
  --fixed-recipient-step 1000 \
  --run-id aws-100k-gap50-load-2
```

`rtms-load-3`:

```bash
ulimit -n 200000

python3 -m load_tester.load_test \
  --host 172.31.39.180 \
  --port 12345 \
  --clients 25000 \
  --processes 8 \
  --start-index 50001 \
  --total-clients 100000 \
  --password loadtest \
  --duration 1200 \
  --ramp-up 600 \
  --min-first-send-delay 120 \
  --max-first-send-delay 240 \
  --mean-message-gap 50 \
  --noise-period 60 \
  --drain-seconds 120 \
  --recipient-mode fixed-offsets \
  --fixed-recipient-count 10 \
  --fixed-recipient-step 1000 \
  --run-id aws-100k-gap50-load-3
```

`rtms-load-4`:

```bash
ulimit -n 200000

python3 -m load_tester.load_test \
  --host 172.31.39.180 \
  --port 12345 \
  --clients 25000 \
  --processes 8 \
  --start-index 75001 \
  --total-clients 100000 \
  --password loadtest \
  --duration 1200 \
  --ramp-up 600 \
  --min-first-send-delay 120 \
  --max-first-send-delay 240 \
  --mean-message-gap 50 \
  --noise-period 60 \
  --drain-seconds 120 \
  --recipient-mode fixed-offsets \
  --fixed-recipient-count 10 \
  --fixed-recipient-step 1000 \
  --run-id aws-100k-gap50-load-4
```

---

## Merge results from multiple load generators

After the run, copy result folders or JSON files from all four load-generator machines to one machine.

Example folder layout:

```text
results/
├── load1/
├── load2/
├── load3/
└── load4/
```

Merge:

```bash
python3 -m load_tester.merge_results \
  --inputs results/load1 results/load2 results/load3 results/load4 \
  --output results/aws-100k-gap50-global.json
```

---

## Important load-test parameters

| Parameter | Used value | Purpose |
|---|---:|---|
| `--host` | `172.31.39.180` | Private IP of the server inside the VPC. |
| `--port` | `12345` | TCP server port. |
| `--clients` | `25000` | Virtual clients per load-generator machine. |
| `--processes` | `8` | Local load-generator processes per machine. |
| `--start-index` | machine-specific | First numeric user ID for that machine. |
| `--total-clients` | `100000` | Global user ID range used for recipient selection. |
| `--password` | `loadtest` | Password used by generated users. |
| `--duration` | `1200` | Total runtime per load-test process. |
| `--ramp-up` | `600` | Spreads connection/login creation across 10 minutes. |
| `--min-first-send-delay` | `120` | Minimum delay before a logged-in client sends first message. |
| `--max-first-send-delay` | `240` | Maximum delay before first send. |
| `--mean-message-gap` | `50` | Average per-client gap between messages. |
| `--noise-period` | `60` | Extra period excluded from measurement after ramp-up and first-send delay. |
| `--drain-seconds` | `120` | Keeps clients alive after send window to collect late receives/ACKs. |
| `--recipient-mode` | `fixed-offsets` | Uses deterministic recipients instead of fully random pairs. |
| `--fixed-recipient-count` | `10` | Each sender chooses from 10 offset-based recipient candidates. |
| `--fixed-recipient-step` | `1000` | Generated offsets are 1000, 2000, ... 10000. |
| `--run-id` | machine-specific | Output file prefix. |

With the final benchmark values, measured window is:

```text
1200 - 600 - 240 - 60 = 300 seconds
```

That is why the included global benchmark reports a measured window of roughly 300 seconds.

---

## Useful monitoring commands

### Count established server connections

Run on the server:

```bash
ss -tan state established '( sport = :12345 )' | wc -l
```

### Count Redis presence keys

Presence count should stay close to active logged-in users during the live test.

```bash
docker exec -it rtms-redis redis-cli --scan --pattern 'chat:presence:*' | wc -l
```

### Watch Docker resource usage

```bash
docker stats
```

### Watch server process usage

```bash
htop
```

### Check listening port

```bash
ss -ltnp | grep 12345
```

---

## Known limitations

This project has strong benchmark value, but it is not a fully production-ready chat platform.

Main limitations:

- The AWS benchmark is a single-server, multi-process setup. It is not full horizontal multi-server routing behind a load balancer yet.
- PostgreSQL remains the likely throughput bottleneck because every message performs a DB insert plus conversation metadata update.
- The current server does blocking PostgreSQL work inside selector workers, so DB latency can affect socket-loop latency.
- No TLS is implemented in the raw TCP protocol.
- No token-based session authentication is implemented.
- No rate limiting, abuse control or per-user quota enforcement is implemented.

---

