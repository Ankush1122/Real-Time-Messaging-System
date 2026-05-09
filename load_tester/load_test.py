import argparse
import json
import multiprocessing as mp
import os
import random
import selectors
import socket
import statistics
import time
import uuid
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

from chatapp_core.protocol import MessageReader, encode_message

_original_print = print

def print(*args, **kwargs):
    """Print one log line followed by a blank line for readability."""
    _original_print(*args, **kwargs)
    if args or kwargs.get("end", "\n") != "":
        _original_print(flush=kwargs.get("flush", False))

READ_CHUNK_SIZE = 65536
PROGRESS_INTERVAL_SECONDS = 20.0
CONNECT_TIMEOUT_SECONDS = 10.0
LOGIN_WAIT_FALLBACK_SECONDS = 90.0
ERROR_SAMPLE_LIMIT = 12

DEFAULT_DURATION_SECONDS = 600
DEFAULT_DRAIN_SECONDS = 30.0
DEFAULT_NOISE_PERIOD_SECONDS = 30.0
DEFAULT_RAMP_UP_SECONDS = 30.0
DEFAULT_MIN_FIRST_SEND_DELAY_SECONDS = 30.0
DEFAULT_MAX_FIRST_SEND_DELAY_SECONDS = 120.0
DEFAULT_MEAN_MESSAGE_GAP_SECONDS = 60.0
DEFAULT_MIN_LOGGED_IN_RATIO = 0.995


def exp_delay(mean_seconds: float) -> float:
    mean_seconds = max(mean_seconds, 0.001)
    return random.expovariate(1.0 / mean_seconds)


def percentile_ms(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percentile))
    return ordered[index]


def safe_mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def make_message_id(process_index: int, sender_abs_index: int, sent_at_epoch_ms: int) -> str:
    # Timestamp is embedded so any load-test process can calculate receive latency
    # without Redis or shared memory.
    return f"lt:{sent_at_epoch_ms}:{process_index}:{sender_abs_index}:{uuid.uuid4().hex}"


def extract_sent_epoch_ms(client_message_id: str) -> Optional[int]:
    try:
        parts = client_message_id.split(":", 4)
        if len(parts) >= 2 and parts[0] == "lt":
            return int(parts[1])
    except (TypeError, ValueError):
        pass
    return None


@dataclass(frozen=True)
class WorkerConfig:
    process_index: int
    process_count: int
    host: str
    port: int
    clients: int
    total_clients: int
    prefix: str
    start_index: int
    run_id: str
    output_dir: str
    duration_seconds: int
    drain_seconds: float
    ramp_up_seconds: float
    noise_period_seconds: float
    mean_message_gap_seconds: float
    min_first_send_delay_seconds: float
    max_first_send_delay_seconds: float
    min_logged_in_ratio: float
    random_seed: int
    password: str
    create_if_missing: bool
    include_login_chats: bool
    recipient_mode: str
    recipient_offsets: List[int]


@dataclass
class PendingMessage:
    sent_at_monotonic: float
    acked: bool = False
    offline: bool = False


@dataclass
class Stats:
    clients_target: int
    process_index: int
    process_count: int
    start_index: int
    total_clients: int

    connected: int = 0
    login_ok: int = 0
    send_attempts: int = 0
    server_ack: int = 0
    offline: int = 0
    errors: int = 0
    received: int = 0
    disconnects: int = 0
    login_timeouts: int = 0
    connect_exceptions: int = 0

    start_time: float = field(default_factory=time.monotonic)
    wall_start_epoch_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    measurement_start_time: Optional[float] = None
    measurement_end_time: Optional[float] = None
    wall_measurement_start_epoch_ms: Optional[int] = None
    wall_measurement_end_epoch_ms: Optional[int] = None

    pending_messages: Dict[str, PendingMessage] = field(default_factory=dict)
    ack_latencies_ms: List[float] = field(default_factory=list)
    receive_latencies_ms: List[float] = field(default_factory=list)
    error_kinds: Counter = field(default_factory=Counter)
    error_samples: List[str] = field(default_factory=list)
    delivery_status_counts: Counter = field(default_factory=Counter)
    server_ack_status_counts: Counter = field(default_factory=Counter)
    delivered_status_updates: int = 0

    def mark_sent(self, client_message_id: str, sent_at_monotonic: float) -> None:
        if self.measurement_start_time is not None and sent_at_monotonic < self.measurement_start_time:
            return
        self.send_attempts += 1
        self.pending_messages[client_message_id] = PendingMessage(sent_at_monotonic=sent_at_monotonic)

    def mark_server_ack(self, client_message_id: str, now: float, status: str) -> bool:
        pending = self.pending_messages.get(client_message_id)
        if pending is None or pending.acked or pending.offline:
            return False
        pending.acked = True
        self.server_ack += 1
        self.server_ack_status_counts[status] += 1
        self.ack_latencies_ms.append((now - pending.sent_at_monotonic) * 1000.0)
        self._cleanup_pending(client_message_id)
        return True

    def mark_offline(self, client_message_id: str) -> None:
        pending = self.pending_messages.get(client_message_id)
        if pending is None or pending.offline:
            return
        pending.offline = True
        self.offline += 1
        self.pending_messages.pop(client_message_id, None)

    def mark_received(self, client_message_id: str, now_epoch_ms: int) -> None:
        # Do not count warmup/noise-period receives. Otherwise old backlog or
        # pre-measurement traffic can create fake huge throughput because the
        # measured elapsed time is still effectively zero.
        if (
            self.wall_measurement_start_epoch_ms is not None
            and now_epoch_ms < self.wall_measurement_start_epoch_ms
        ):
            return

        sent_epoch_ms = extract_sent_epoch_ms(client_message_id)
        if sent_epoch_ms is None:
            return
        if (
            self.wall_measurement_start_epoch_ms is not None
            and sent_epoch_ms < self.wall_measurement_start_epoch_ms
        ):
            return

        self.received += 1
        self.receive_latencies_ms.append(max(0.0, float(now_epoch_ms - sent_epoch_ms)))

    def _cleanup_pending(self, client_message_id: str) -> None:
        pending = self.pending_messages.get(client_message_id)
        if pending is None:
            return
        if pending.offline or pending.acked:
            self.pending_messages.pop(client_message_id, None)

    def note_error(self, kind: str, detail: str) -> None:
        self.error_kinds[kind] += 1
        if len(self.error_samples) < ERROR_SAMPLE_LIMIT:
            self.error_samples.append(f"{kind}: {detail}")


@dataclass
class VirtualClient:
    index: int
    host: str
    port: int
    selector: selectors.BaseSelector
    clients_in_process: int
    total_clients: int
    login_prefix: str
    start_index: int
    stats: Stats
    config: WorkerConfig
    connect_at: float

    reader: MessageReader = field(default_factory=MessageReader)
    socket_obj: Optional[socket.socket] = None
    user_id: str = ""
    username: str = ""
    connect_started: float = 0.0
    tcp_connected: bool = False
    login_sent: bool = False
    login_confirmed: bool = False
    closed: bool = False
    activated: bool = False
    login_deadline: float = 0.0
    send_enabled_at: float = float("inf")
    next_send_at: float = float("inf")
    sending_stopped: bool = False
    write_queue: Deque[bytes] = field(default_factory=deque)
    current_write_offset: int = 0

    @property
    def absolute_index(self) -> int:
        return self.start_index + self.index

    def choose_recipient_index(self) -> int:
        if self.config.recipient_mode == "fixed-offsets":
            candidates = [
                ((self.absolute_index - 1 + offset) % self.total_clients) + 1
                for offset in self.config.recipient_offsets
            ]
            candidates = [candidate for candidate in candidates if candidate != self.absolute_index]
            if candidates:
                return random.choice(candidates)

        recipient_index = random.randint(1, self.total_clients)
        while recipient_index == self.absolute_index:
            recipient_index = random.randint(1, self.total_clients)
        return recipient_index

    def _set_selector_events(self, events: int) -> None:
        if self.socket_obj is None:
            return
        try:
            self.selector.modify(self.socket_obj, events, self)
        except KeyError:
            self.selector.register(self.socket_obj, events, self)

    def activate(self, now: float) -> None:
        if self.activated:
            return

        self.user_id = f"{self.login_prefix}{self.absolute_index}"
        self.username = self.user_id
        self.connect_started = now
        self.login_deadline = now + max(LOGIN_WAIT_FALLBACK_SECONDS, CONNECT_TIMEOUT_SECONDS)

        self.socket_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_obj.setblocking(False)
        self.socket_obj.connect_ex((self.host, self.port))
        self.selector.register(self.socket_obj, selectors.EVENT_READ | selectors.EVENT_WRITE, self)
        self.activated = True

        first_send_delay = random.uniform(
            self.config.min_first_send_delay_seconds,
            self.config.max_first_send_delay_seconds,
        )
        self.send_enabled_at = self.connect_at + first_send_delay
        self.next_send_at = self.send_enabled_at

    def _refresh_selector_events(self) -> None:
        if self.closed or self.socket_obj is None:
            return
        events = selectors.EVENT_READ
        if not self.tcp_connected or self.write_queue:
            events |= selectors.EVENT_WRITE
        self._set_selector_events(events)

    def _queue_payload(self, payload: dict) -> None:
        self.write_queue.append(encode_message(payload))
        self._refresh_selector_events()

    def _flush_writes(self) -> None:
        if self.socket_obj is None:
            return
        while self.write_queue:
            frame = self.write_queue[0]
            try:
                sent = self.socket_obj.send(frame[self.current_write_offset:])
            except (BlockingIOError, InterruptedError):
                return
            if sent <= 0:
                return
            self.current_write_offset += sent
            if self.current_write_offset >= len(frame):
                self.write_queue.popleft()
                self.current_write_offset = 0
        self._refresh_selector_events()

    def on_write_ready(self, now: float) -> None:
        if self.closed or self.socket_obj is None or not self.activated:
            return

        if not self.tcp_connected:
            err = self.socket_obj.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                raise ConnectionError(f"connect failed with errno={err}")
            self.tcp_connected = True
            self.stats.connected += 1

        if not self.login_sent:
            self._queue_payload({
                "type": "login",
                "username": self.username,
                "user_id": self.user_id,
                "password": self.config.password,
                "create_if_missing": self.config.create_if_missing,
                "include_chats": self.config.include_login_chats,
            })
            self.login_sent = True

        self._flush_writes()

        if not self.login_confirmed and now > self.login_deadline:
            self.stats.login_timeouts += 1
            raise TimeoutError(f"login timeout for {self.username}")

    def maybe_send(self, now: float) -> None:
        if self.closed or self.socket_obj is None or not self.activated:
            return
        if not self.tcp_connected or not self.login_confirmed:
            return
        if self.total_clients <= 1 or self.sending_stopped:
            return

        required_logged_in = max(1, int(self.clients_in_process * self.config.min_logged_in_ratio))
        if self.stats.login_ok < required_logged_in:
            return
        if now < self.send_enabled_at or now < self.next_send_at:
            return

        recipient_index = self.choose_recipient_index()
        recipient_name = f"{self.login_prefix}{recipient_index}"
        sent_at_epoch_ms = int(time.time() * 1000)
        client_message_id = make_message_id(
            self.config.process_index,
            self.absolute_index,
            sent_at_epoch_ms,
        )

        self._queue_payload(
            {
                "type": "send_message",
                "to_user_id": recipient_name,
                "message": f"hello from {self.user_id}",
                "client_message_id": client_message_id,
            }
        )

        self.stats.mark_sent(client_message_id, now)
        self.next_send_at = now + exp_delay(self.config.mean_message_gap_seconds)

    def on_read_ready(self, now: float) -> None:
        if self.closed or self.socket_obj is None:
            return

        data = self.socket_obj.recv(READ_CHUNK_SIZE)
        if not data:
            raise ConnectionError("server closed connection")

        now_epoch_ms = int(time.time() * 1000)
        for payload in self.reader.feed(data):
            message_type = payload.get("type")

            if message_type == "login_ok":
                if not self.login_confirmed:
                    self.login_confirmed = True
                    self.stats.login_ok += 1

            elif message_type == "delivery_status":
                status = str(payload.get("status") or "unknown")
                client_message_id = payload.get("client_message_id", "")
                self.stats.delivery_status_counts[status] += 1
                if status in {"queued_to_socket", "published_to_process", "stored"}:
                    self.stats.mark_server_ack(client_message_id, now, status)
                elif status == "delivered_to_client":
                    self.stats.delivered_status_updates += 1
                elif status in {"offline", "offline_queued", "failed"}:
                    self.stats.mark_offline(client_message_id)

            elif message_type == "chat_message":
                self.stats.mark_received(payload.get("client_message_id", ""), now_epoch_ms)
                if payload.get("message_id") and payload.get("chat_id"):
                    self._queue_payload({
                        "type": "message_received_ack",
                        "message_id": payload.get("message_id"),
                        "chat_id": payload.get("chat_id"),
                        "from_user_id": payload.get("from_user_id"),
                        "client_message_id": payload.get("client_message_id"),
                    })

            elif message_type == "pong":
                pass

            elif message_type in {"error", "login_failed", "register_failed"}:
                self.stats.errors += 1
                self.stats.note_error(str(message_type), str(payload.get("message") or "unknown server error"))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.stats.disconnects += 1
        if self.socket_obj is not None:
            try:
                self.selector.unregister(self.socket_obj)
            except Exception:
                pass
            try:
                self.socket_obj.close()
            except Exception:
                pass


def measurement_elapsed_seconds(stats: Stats, final: bool = False) -> float:
    now = time.monotonic()
    if stats.measurement_start_time is None:
        return 0.001
    if final and stats.measurement_end_time is not None:
        return max(stats.measurement_end_time - stats.measurement_start_time, 0.001)
    if now > stats.measurement_start_time:
        return max(now - stats.measurement_start_time, 0.001)
    return 0.001


def summary_from_stats(stats: Stats, final: bool = False) -> dict:
    elapsed = measurement_elapsed_seconds(stats, final=final)
    measurement_active = (
        stats.measurement_start_time is not None
        and (final or time.monotonic() >= stats.measurement_start_time)
    )
    ack_avg = safe_mean(stats.ack_latencies_ms)
    recv_avg = safe_mean(stats.receive_latencies_ms)
    offered_load = stats.send_attempts / elapsed if measurement_active else 0.0
    ack_throughput = stats.server_ack / elapsed if measurement_active else 0.0
    receive_throughput = stats.received / elapsed if measurement_active else 0.0
    return {
        "process_index": stats.process_index,
        "process_count": stats.process_count,
        "clients_target": stats.clients_target,
        "start_index": stats.start_index,
        "total_clients": stats.total_clients,
        "connected": stats.connected,
        "login_ok": stats.login_ok,
        "send_attempts": stats.send_attempts,
        "server_ack": stats.server_ack,
        "received": stats.received,
        "offline": stats.offline,
        "pending_ack": len(stats.pending_messages),
        "errors": stats.errors,
        "disconnects": stats.disconnects,
        "login_timeouts": stats.login_timeouts,
        "connect_exceptions": stats.connect_exceptions,
        "measured_window_seconds": elapsed,
        "offered_load_msg_per_sec": offered_load,
        "ack_throughput_msg_per_sec": ack_throughput,
        "receive_throughput_msg_per_sec": receive_throughput,
        "ack_avg_ms": ack_avg,
        "ack_p50_ms": percentile_ms(stats.ack_latencies_ms, 0.50),
        "ack_p95_ms": percentile_ms(stats.ack_latencies_ms, 0.95),
        "ack_p99_ms": percentile_ms(stats.ack_latencies_ms, 0.99),
        "recv_avg_ms": recv_avg,
        "recv_p50_ms": percentile_ms(stats.receive_latencies_ms, 0.50),
        "recv_p95_ms": percentile_ms(stats.receive_latencies_ms, 0.95),
        "recv_p99_ms": percentile_ms(stats.receive_latencies_ms, 0.99),
        "ack_success_rate_pct": (stats.server_ack / stats.send_attempts * 100.0) if stats.send_attempts else 0.0,
        "receive_success_rate_pct": (stats.received / stats.send_attempts * 100.0) if stats.send_attempts else 0.0,
        "error_breakdown": dict(stats.error_kinds),
        "error_samples": list(stats.error_samples),
        "delivery_status_counts": dict(stats.delivery_status_counts),
        "server_ack_status_counts": dict(stats.server_ack_status_counts),
        "delivered_status_updates": stats.delivered_status_updates,
        "wall_measurement_start_epoch_ms": stats.wall_measurement_start_epoch_ms,
        "wall_measurement_end_epoch_ms": stats.wall_measurement_end_epoch_ms,
    }


def print_progress(stats: Stats, duration: int, final: bool = False) -> None:
    summary = summary_from_stats(stats, final=final)
    elapsed_total = int(time.monotonic() - stats.start_time)
    elapsed_display = min(elapsed_total, duration)
    prefix = f"completed [{duration}/{duration}]" if final else f"progress [{elapsed_display}/{duration}]"
    proc = f"p{stats.process_index}/{stats.process_count}"

    print(
        f"{proc} {prefix}: "
        f"connected={summary['connected']}/{summary['clients_target']} "
        f"login_ok={summary['login_ok']}/{summary['clients_target']} "
        f"sent={summary['send_attempts']} "
        f"ack={summary['server_ack']} "
        f"received={summary['received']} "
        f"offline={summary['offline']} "
        f"pending_ack={summary['pending_ack']} "
        f"errors={summary['errors']} "
        f"offered={summary['offered_load_msg_per_sec']:.2f}/s "
        f"ack_tput={summary['ack_throughput_msg_per_sec']:.2f}/s "
        f"recv_tput={summary['receive_throughput_msg_per_sec']:.2f}/s "
        f"ack_p95={summary['ack_p95_ms']:.2f}ms "
        f"recv_p95={summary['recv_p95_ms']:.2f}ms",
        flush=True,
    )

    if final:
        print(f"{proc} final_metrics:", flush=True)
        for key in [
            "connected",
            "login_ok",
            "send_attempts",
            "server_ack",
            "received",
            "offline",
            "pending_ack",
            "ack_success_rate_pct",
            "receive_success_rate_pct",
            "measured_window_seconds",
            "offered_load_msg_per_sec",
            "ack_throughput_msg_per_sec",
            "receive_throughput_msg_per_sec",
            "ack_avg_ms",
            "ack_p95_ms",
            "ack_p99_ms",
            "recv_avg_ms",
            "recv_p95_ms",
            "recv_p99_ms",
            "errors",
            "login_timeouts",
            "connect_exceptions",
            "disconnects",
        ]:
            value = summary[key]
            if isinstance(value, float):
                print(f"  {key}={value:.2f}", flush=True)
            else:
                print(f"  {key}={value}", flush=True)
        if stats.server_ack_status_counts:
            joined = ", ".join(f"{kind}={count}" for kind, count in sorted(stats.server_ack_status_counts.items()))
            print(f"  server_ack_status_counts={joined}", flush=True)
        if stats.delivery_status_counts:
            joined = ", ".join(f"{kind}={count}" for kind, count in sorted(stats.delivery_status_counts.items()))
            print(f"  delivery_status_counts={joined}", flush=True)
        print(f"  delivered_status_updates={stats.delivered_status_updates}", flush=True)
        if stats.error_kinds:
            joined = ", ".join(f"{kind}={count}" for kind, count in stats.error_kinds.items())
            print(f"  error_breakdown={joined}", flush=True)
        if stats.error_samples:
            print("  error_samples:", flush=True)
            for sample in stats.error_samples:
                print(f"    - {sample}", flush=True)


def preflight_check(host: str, port: int, timeout: float = 2.0) -> None:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(timeout)
    try:
        probe.connect((host, port))
    except OSError as exc:
        raise SystemExit(
            f"Preflight failed: could not connect to {host}:{port}. "
            f"Make sure the chat server is running. Detail: {exc}"
        ) from exc
    finally:
        try:
            probe.close()
        except Exception:
            pass


def write_worker_result(config: WorkerConfig, stats: Stats) -> Path:
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{config.run_id}_process_{config.process_index:03d}.json"
    result = {
        "run_id": config.run_id,
        "config": asdict(config),
        "summary": summary_from_stats(stats, final=True),
        "ack_latencies_ms": stats.ack_latencies_ms,
        "receive_latencies_ms": stats.receive_latencies_ms,
    }
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    tmp_path.replace(path)
    return path


def run_worker(config: WorkerConfig) -> None:
    random.seed(config.random_seed + config.process_index)
    preflight_check(config.host, config.port)

    selector = selectors.DefaultSelector()
    stats = Stats(
        clients_target=config.clients,
        process_index=config.process_index,
        process_count=config.process_count,
        start_index=config.start_index,
        total_clients=config.total_clients,
    )

    stats.measurement_start_time = (
        stats.start_time
        + config.ramp_up_seconds
        + config.max_first_send_delay_seconds
        + config.noise_period_seconds
    )
    stats.wall_measurement_start_epoch_ms = int(
        (time.time() + (stats.measurement_start_time - time.monotonic())) * 1000
    )

    measured_window_seconds = max(
        config.duration_seconds
        - config.ramp_up_seconds
        - config.max_first_send_delay_seconds
        - config.noise_period_seconds,
        0.001,
    )

    start_time = stats.start_time
    end_time = start_time + config.duration_seconds
    virtual_clients: List[VirtualClient] = []

    for index in range(config.clients):
        connect_at = start_time + (config.ramp_up_seconds * index / max(config.clients, 1))
        virtual_clients.append(
            VirtualClient(
                index=index,
                host=config.host,
                port=config.port,
                selector=selector,
                clients_in_process=config.clients,
                total_clients=config.total_clients,
                login_prefix=config.prefix,
                start_index=config.start_index,
                stats=stats,
                config=config,
                connect_at=connect_at,
            )
        )

    print(
        f"p{config.process_index}/{config.process_count} started: "
        f"target={config.clients} users=[{config.start_index},{config.start_index + config.clients - 1}] "
        f"total_clients={config.total_clients} server={config.host}:{config.port} "
        f"duration={config.duration_seconds}s ramp_up={config.ramp_up_seconds:.1f}s "
        f"first_send_delay=[{config.min_first_send_delay_seconds:.1f},{config.max_first_send_delay_seconds:.1f}]s "
        f"mean_gap={config.mean_message_gap_seconds:.1f}s "
        f"recipient_mode={config.recipient_mode} recipient_offsets={config.recipient_offsets} "
        f"measurement_starts_after={config.ramp_up_seconds + config.max_first_send_delay_seconds + config.noise_period_seconds:.1f}s "
        f"measured_window={measured_window_seconds:.1f}s",
        flush=True,
    )

    next_progress_at = time.monotonic() + PROGRESS_INTERVAL_SECONDS

    try:
        while time.monotonic() < end_time:
            now = time.monotonic()

            for client in virtual_clients:
                if not client.activated and now >= client.connect_at:
                    try:
                        client.activate(now)
                    except OSError as exc:
                        stats.connect_exceptions += 1
                        stats.note_error(type(exc).__name__, str(exc))
                        client.close()

            for client in virtual_clients:
                client.maybe_send(now)

            events = selector.select(timeout=0.25)
            for key, mask in events:
                client = key.data
                now = time.monotonic()
                try:
                    if mask & selectors.EVENT_WRITE:
                        client.on_write_ready(now)
                    if mask & selectors.EVENT_READ:
                        client.on_read_ready(now)
                except TimeoutError as exc:
                    stats.note_error("login_timeout", str(exc))
                    client.close()
                except OSError as exc:
                    stats.connect_exceptions += 1
                    stats.note_error(type(exc).__name__, str(exc))
                    client.close()
                except Exception as exc:
                    stats.errors += 1
                    stats.note_error(type(exc).__name__, str(exc))
                    client.close()

            if now >= next_progress_at:
                print_progress(stats, config.duration_seconds)
                next_progress_at = now + PROGRESS_INTERVAL_SECONDS

        for client in virtual_clients:
            client.sending_stopped = True

        stats.measurement_end_time = time.monotonic()
        stats.wall_measurement_end_epoch_ms = int(time.time() * 1000)

        print(f"p{config.process_index}/{config.process_count} starting drain phase for {config.drain_seconds:.1f}s...", flush=True)
        drain_end_time = time.monotonic() + config.drain_seconds
        while time.monotonic() < drain_end_time:
            events = selector.select(timeout=0.25)
            for key, mask in events:
                client = key.data
                now = time.monotonic()
                try:
                    if mask & selectors.EVENT_WRITE:
                        client.on_write_ready(now)
                    if mask & selectors.EVENT_READ:
                        client.on_read_ready(now)
                except TimeoutError as exc:
                    stats.note_error("login_timeout", str(exc))
                    client.close()
                except OSError as exc:
                    stats.connect_exceptions += 1
                    stats.note_error(type(exc).__name__, str(exc))
                    client.close()
                except Exception as exc:
                    stats.errors += 1
                    stats.note_error(type(exc).__name__, str(exc))
                    client.close()

    finally:
        print_progress(stats, config.duration_seconds, final=True)
        result_path = write_worker_result(config, stats)
        print(f"p{config.process_index}/{config.process_count} wrote {result_path}", flush=True)

        for client in virtual_clients:
            client.close()
        selector.close()


def split_clients(total_clients: int, process_count: int) -> List[Tuple[int, int]]:
    base = total_clients // process_count
    remainder = total_clients % process_count
    ranges = []
    start = 0
    for process_index in range(process_count):
        count = base + (1 if process_index < remainder else 0)
        ranges.append((start, count))
        start += count
    return ranges


def aggregate_results(run_id: str, output_dir: str, process_count: int) -> Optional[Path]:
    output = Path(output_dir)
    results = []
    for process_index in range(process_count):
        path = output / f"{run_id}_process_{process_index:03d}.json"
        if not path.exists():
            print(f"aggregate warning: missing result file {path}", flush=True)
            continue
        with path.open("r", encoding="utf-8") as f:
            results.append(json.load(f))

    if not results:
        return None

    summaries = [item["summary"] for item in results]
    ack_latencies = [lat for item in results for lat in item.get("ack_latencies_ms", [])]
    recv_latencies = [lat for item in results for lat in item.get("receive_latencies_ms", [])]

    start_times = [s.get("wall_measurement_start_epoch_ms") for s in summaries if s.get("wall_measurement_start_epoch_ms")]
    end_times = [s.get("wall_measurement_end_epoch_ms") for s in summaries if s.get("wall_measurement_end_epoch_ms")]
    if start_times and end_times:
        measured_window_seconds = max((max(end_times) - min(start_times)) / 1000.0, 0.001)
    else:
        measured_window_seconds = max(s["measured_window_seconds"] for s in summaries)

    sent = sum(s["send_attempts"] for s in summaries)
    ack = sum(s["server_ack"] for s in summaries)
    received = sum(s["received"] for s in summaries)
    offline = sum(s["offline"] for s in summaries)

    error_breakdown = Counter()
    delivery_status_counts = Counter()
    server_ack_status_counts = Counter()
    error_samples = []
    for s in summaries:
        error_breakdown.update(s.get("error_breakdown", {}))
        delivery_status_counts.update(s.get("delivery_status_counts", {}))
        server_ack_status_counts.update(s.get("server_ack_status_counts", {}))
        error_samples.extend(s.get("error_samples", []))

    aggregate = {
        "run_id": run_id,
        "processes_completed": len(results),
        "processes_expected": process_count,
        "clients_target": sum(s["clients_target"] for s in summaries),
        "connected": sum(s["connected"] for s in summaries),
        "login_ok": sum(s["login_ok"] for s in summaries),
        "send_attempts": sent,
        "server_ack": ack,
        "received": received,
        "offline": offline,
        "pending_ack": sum(s["pending_ack"] for s in summaries),
        "errors": sum(s["errors"] for s in summaries),
        "disconnects": sum(s["disconnects"] for s in summaries),
        "login_timeouts": sum(s["login_timeouts"] for s in summaries),
        "connect_exceptions": sum(s["connect_exceptions"] for s in summaries),
        "measured_window_seconds": measured_window_seconds,
        "offered_load_msg_per_sec": sent / measured_window_seconds,
        "ack_throughput_msg_per_sec": ack / measured_window_seconds,
        "receive_throughput_msg_per_sec": received / measured_window_seconds,
        "ack_success_rate_pct": (ack / sent * 100.0) if sent else 0.0,
        "receive_success_rate_pct": (received / sent * 100.0) if sent else 0.0,
        "ack_avg_ms": safe_mean(ack_latencies),
        "ack_p50_ms": percentile_ms(ack_latencies, 0.50),
        "ack_p95_ms": percentile_ms(ack_latencies, 0.95),
        "ack_p99_ms": percentile_ms(ack_latencies, 0.99),
        "recv_avg_ms": safe_mean(recv_latencies),
        "recv_p50_ms": percentile_ms(recv_latencies, 0.50),
        "recv_p95_ms": percentile_ms(recv_latencies, 0.95),
        "recv_p99_ms": percentile_ms(recv_latencies, 0.99),
        "error_breakdown": dict(error_breakdown),
        "error_samples": error_samples[:ERROR_SAMPLE_LIMIT],
        "delivery_status_counts": dict(delivery_status_counts),
        "server_ack_status_counts": dict(server_ack_status_counts),
        "delivered_status_updates": sum(s.get("delivered_status_updates", 0) for s in summaries),
        "process_result_files": [str(output / f"{run_id}_process_{i:03d}.json") for i in range(process_count)],
    }

    path = output / f"{run_id}_aggregate.json"
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump({"aggregate": aggregate, "process_summaries": summaries}, f, indent=2)
    tmp_path.replace(path)

    print("aggregate_final_metrics:", flush=True)
    for key in [
        "clients_target",
        "connected",
        "login_ok",
        "send_attempts",
        "server_ack",
        "received",
        "offline",
        "pending_ack",
        "ack_success_rate_pct",
        "receive_success_rate_pct",
        "measured_window_seconds",
        "offered_load_msg_per_sec",
        "ack_throughput_msg_per_sec",
        "receive_throughput_msg_per_sec",
        "ack_avg_ms",
        "ack_p95_ms",
        "ack_p99_ms",
        "recv_avg_ms",
        "recv_p95_ms",
        "recv_p99_ms",
        "errors",
        "login_timeouts",
        "connect_exceptions",
    ]:
        value = aggregate[key]
        if isinstance(value, float):
            print(f"  {key}={value:.2f}", flush=True)
        else:
            print(f"  {key}={value}", flush=True)
    if server_ack_status_counts:
        joined = ", ".join(f"{kind}={count}" for kind, count in sorted(server_ack_status_counts.items()))
        print(f"  server_ack_status_counts={joined}", flush=True)
    if delivery_status_counts:
        joined = ", ".join(f"{kind}={count}" for kind, count in sorted(delivery_status_counts.items()))
        print(f"  delivery_status_counts={joined}", flush=True)
    print(f"  delivered_status_updates={aggregate['delivered_status_updates']}", flush=True)
    print(f"  result_file={path}", flush=True)

    return path


def parse_recipient_offsets(value: Optional[str], count: int, step: int) -> List[int]:
    if value:
        offsets = []
        for raw in value.split(","):
            raw = raw.strip()
            if not raw:
                continue
            offsets.append(int(raw))
    else:
        offsets = [step * i for i in range(1, count + 1)]

    offsets = sorted(set(offset for offset in offsets if offset > 0))
    if not offsets:
        raise SystemExit("recipient offsets must contain at least one positive integer")
    return offsets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multiprocess load test for the chat gateway")
    parser.add_argument("--host", default="127.0.0.1", help="Chat server host/IP")
    parser.add_argument("--port", type=int, default=12345, help="Chat server port")
    parser.add_argument("--clients", type=int, default=1000, help="Total virtual clients across all processes")
    parser.add_argument("--processes", type=int, default=1, help="Number of local load-generator processes")
    parser.add_argument("--prefix", default="", help="Optional user id prefix. Default creates numeric ids: 1..N")
    parser.add_argument("--start-index", type=int, default=1, help="First numeric user id created by this load-generator machine")
    parser.add_argument("--total-clients", type=int, default=None, help="Global user id count for recipient selection. Defaults to --clients")
    parser.add_argument("--password", default="loadtest", help="Password used by generated users")
    parser.add_argument("--no-create-users", action="store_true", help="Do not auto-register missing load-test users during login")
    parser.add_argument("--include-login-chats", action="store_true", help="Ask server to include chat list in login_ok. Disabled by default for load testing.")
    parser.add_argument("--recipient-mode", choices=["random", "fixed-offsets"], default="random", help="Recipient selection mode. random picks from all users. fixed-offsets picks from deterministic offset candidates per sender.")
    parser.add_argument("--fixed-recipient-count", type=int, default=10, help="For --recipient-mode fixed-offsets, generate this many offsets when --recipient-offsets is omitted.")
    parser.add_argument("--fixed-recipient-step", type=int, default=1000, help="For --recipient-mode fixed-offsets, generated offsets are step, 2*step, ... count*step.")
    parser.add_argument("--recipient-offsets", default=None, help="Comma-separated positive offsets for fixed-offsets mode, e.g. 1000,2000,...,10000. Overrides count/step.")
    parser.add_argument("--output-dir", default="load_tester/load_test_output", help="Directory for per-process JSON result files")
    parser.add_argument("--run-id", default=None, help="Run id used in output file names")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--drain-seconds", type=float, default=DEFAULT_DRAIN_SECONDS)
    parser.add_argument("--ramp-up", type=float, default=DEFAULT_RAMP_UP_SECONDS)
    parser.add_argument("--noise-period", type=float, default=DEFAULT_NOISE_PERIOD_SECONDS)
    parser.add_argument("--mean-message-gap", type=float, default=DEFAULT_MEAN_MESSAGE_GAP_SECONDS)
    parser.add_argument("--min-first-send-delay", type=float, default=DEFAULT_MIN_FIRST_SEND_DELAY_SECONDS)
    parser.add_argument("--max-first-send-delay", type=float, default=DEFAULT_MAX_FIRST_SEND_DELAY_SECONDS)
    parser.add_argument("--min-logged-in-ratio", type=float, default=DEFAULT_MIN_LOGGED_IN_RATIO)
    parser.add_argument("--random-seed", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clients <= 0:
        raise SystemExit("--clients must be > 0")
    if args.processes <= 0:
        raise SystemExit("--processes must be > 0")
    if args.processes > args.clients:
        raise SystemExit("--processes cannot be greater than --clients")
    if args.start_index <= 0:
        raise SystemExit("--start-index must be >= 1")
    if args.total_clients is None:
        args.total_clients = args.clients
    if args.total_clients <= 1:
        raise SystemExit("--total-clients must be > 1")
    if args.start_index + args.clients - 1 > args.total_clients:
        raise SystemExit("--start-index + --clients - 1 cannot exceed --total-clients")
    if args.max_first_send_delay < args.min_first_send_delay:
        raise SystemExit("--max-first-send-delay must be >= --min-first-send-delay")
    if args.fixed_recipient_count <= 0:
        raise SystemExit("--fixed-recipient-count must be > 0")
    if args.fixed_recipient_step <= 0:
        raise SystemExit("--fixed-recipient-step must be > 0")

    recipient_offsets = parse_recipient_offsets(
        args.recipient_offsets,
        args.fixed_recipient_count,
        args.fixed_recipient_step,
    )
    if args.recipient_mode == "fixed-offsets":
        usable_offsets = [offset for offset in recipient_offsets if offset % args.total_clients != 0]
        if not usable_offsets:
            raise SystemExit("fixed recipient offsets all point back to the sender; choose different offsets")
        recipient_offsets = usable_offsets

    preflight_check(args.host, args.port)

    run_id = args.run_id or f"loadtest-{int(time.time())}"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    random_seed = args.random_seed if args.random_seed is not None else int(time.time())

    print(
        f"Starting multiprocess load test: run_id={run_id} server={args.host}:{args.port} "
        f"clients={args.clients} processes={args.processes} start_index={args.start_index} total_clients={args.total_clients} "
        f"recipient_mode={args.recipient_mode} recipient_offsets={recipient_offsets} output_dir={output_dir}",
        flush=True,
    )

    workers: List[mp.Process] = []
    for process_index, (relative_start_index, client_count) in enumerate(split_clients(args.clients, args.processes)):
        start_index = args.start_index + relative_start_index
        config = WorkerConfig(
            process_index=process_index,
            process_count=args.processes,
            host=args.host,
            port=args.port,
            clients=client_count,
            total_clients=args.total_clients,
            prefix=args.prefix,
            start_index=start_index,
            run_id=run_id,
            output_dir=str(output_dir),
            duration_seconds=args.duration,
            drain_seconds=args.drain_seconds,
            ramp_up_seconds=args.ramp_up,
            noise_period_seconds=args.noise_period,
            mean_message_gap_seconds=args.mean_message_gap,
            min_first_send_delay_seconds=args.min_first_send_delay,
            max_first_send_delay_seconds=args.max_first_send_delay,
            min_logged_in_ratio=args.min_logged_in_ratio,
            random_seed=random_seed,
            password=args.password,
            create_if_missing=not args.no_create_users,
            include_login_chats=args.include_login_chats,
            recipient_mode=args.recipient_mode,
            recipient_offsets=recipient_offsets,
        )
        process = mp.Process(target=run_worker, args=(config,), name=f"load-worker-{process_index}")
        process.start()
        workers.append(process)

    failed = False
    try:
        for process in workers:
            process.join()
            if process.exitcode != 0:
                failed = True
                print(f"worker {process.name} exited with code {process.exitcode}", flush=True)
    except KeyboardInterrupt:
        print("Interrupted. Terminating workers...", flush=True)
        failed = True
        for process in workers:
            process.terminate()
        for process in workers:
            process.join()

    aggregate_results(run_id, str(output_dir), args.processes)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    mp.set_start_method("fork" if hasattr(os, "fork") else "spawn", force=True)
    main()
