
# Real-Time Messaging System

A Python TCP chat system built as an incremental systems project.

## Current capabilities

- 4-byte length-prefixed JSON protocol
- Password-based login backed by PostgreSQL
- Direct conversations and message history
- PyQt desktop client
- Selector-based multi-threaded server runtime
- Multi-process server runtime using `SO_REUSEPORT`
- Redis presence and pub/sub routing between processes
- Duplicate-login session ownership checks
- Redis overflow backlog for temporarily slow or offline receivers
- Multiprocess load tester with ACK latency metrics

## Run

```bash
python Server.py
```

The default runtime is multi-process. Use the environment variable below to run the threaded reactor instead:

```bash
CHAT_SERVER_MODE=multi_threading python Server.py
```

## Load testing

```bash
python -m load_tester.load_test --clients 1000 --processes 8 --duration 120
```

Merge result files with:

```bash
python -m load_tester.merge_results load_tester/results/*.json --output load_tester/results/merged.json
```
