
# Architecture

## Protocol

Clients send UTF-8 JSON payloads framed by a 4-byte big-endian length prefix.
This avoids ambiguous TCP reads and lets the server parse fragmented frames safely.

## Server runtimes

The project contains two runtimes:

1. `multi_threading_server.py` uses an accept loop, worker reactor threads, socketpair wakeups and a sharded in-memory router.
2. `multi_processing_server.py` uses `SO_REUSEPORT`, one selector loop per process, Redis presence and Redis pub/sub routing.

## Persistence

PostgreSQL stores users, conversations, direct conversation pairs, messages and per-user read/delivery state.

## Routing

For local receivers, the owning process queues the frame directly. For receivers connected to another process, Redis presence provides the destination process channel and Redis pub/sub carries the encoded frame.

## Backpressure

Frames are queued per socket. When a receiver is slow or offline, frames can be moved to a Redis backlog and drained when the user logs in again.
