"""Merge load-test results from multiple EC2 load-generator machines.

Usage example after copying all result directories/files to one machine:

    python3 -m load_tester.merge_results \
      --inputs results/load1 results/load2 results/load3 results/load4 \
      --output results/aws-100k-global.json

The script can read either per-process JSON files produced by load_test.py or
*_aggregate.json files. Exact global p95/p99 is only possible when per-process
files with raw latency arrays are available.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, List


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


def discover_files(inputs: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        path = Path(item)
        if path.is_dir():
            files.extend(sorted(path.rglob("*.json")))
        elif path.is_file():
            files.append(path)
    # Prefer per-process files when both process and aggregate files are present,
    # because process files contain raw latency arrays for exact global percentiles.
    process_files = [p for p in files if "_process_" in p.name]
    return process_files or [p for p in files if p.name.endswith("_aggregate.json")]


def load_result(path: Path) -> tuple[list[dict[str, Any]], list[float], list[float]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if "summary" in data:
        return [data["summary"]], data.get("ack_latencies_ms", []), data.get("receive_latencies_ms", [])
    if "aggregate" in data:
        # Aggregate files do not contain raw latency arrays. Percentiles in the
        # final merge will be approximate/zero unless process files are supplied.
        return [data["aggregate"]], [], []
    raise ValueError(f"Unrecognized result file format: {path}")


def merge(files: list[Path]) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    ack_latencies: list[float] = []
    recv_latencies: list[float] = []

    for path in files:
        file_summaries, file_ack, file_recv = load_result(path)
        summaries.extend(file_summaries)
        ack_latencies.extend(float(x) for x in file_ack)
        recv_latencies.extend(float(x) for x in file_recv)

    if not summaries:
        raise SystemExit("No usable result files found")

    start_times = [s.get("wall_measurement_start_epoch_ms") for s in summaries if s.get("wall_measurement_start_epoch_ms")]
    end_times = [s.get("wall_measurement_end_epoch_ms") for s in summaries if s.get("wall_measurement_end_epoch_ms")]
    if start_times and end_times:
        measured_window_seconds = max((max(end_times) - min(start_times)) / 1000.0, 0.001)
    else:
        measured_window_seconds = max(float(s.get("measured_window_seconds", 0.0)) for s in summaries) or 0.001

    sent = sum(int(s.get("send_attempts", 0)) for s in summaries)
    ack = sum(int(s.get("server_ack", 0)) for s in summaries)
    received = sum(int(s.get("received", 0)) for s in summaries)

    error_breakdown = Counter()
    delivery_status_counts = Counter()
    server_ack_status_counts = Counter()
    for s in summaries:
        error_breakdown.update(s.get("error_breakdown", {}))
        delivery_status_counts.update(s.get("delivery_status_counts", {}))
        server_ack_status_counts.update(s.get("server_ack_status_counts", {}))

    return {
        "source_files": [str(p) for p in files],
        "source_file_count": len(files),
        "exact_latency_percentiles": bool(ack_latencies or recv_latencies),
        "clients_target": sum(int(s.get("clients_target", 0)) for s in summaries),
        "connected": sum(int(s.get("connected", 0)) for s in summaries),
        "login_ok": sum(int(s.get("login_ok", 0)) for s in summaries),
        "send_attempts": sent,
        "server_ack": ack,
        "received": received,
        "offline": sum(int(s.get("offline", 0)) for s in summaries),
        "pending_ack": sum(int(s.get("pending_ack", 0)) for s in summaries),
        "errors": sum(int(s.get("errors", 0)) for s in summaries),
        "disconnects": sum(int(s.get("disconnects", 0)) for s in summaries),
        "login_timeouts": sum(int(s.get("login_timeouts", 0)) for s in summaries),
        "connect_exceptions": sum(int(s.get("connect_exceptions", 0)) for s in summaries),
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
        "delivery_status_counts": dict(delivery_status_counts),
        "server_ack_status_counts": dict(server_ack_status_counts),
        "delivered_status_updates": sum(int(s.get("delivered_status_updates", 0)) for s in summaries),
        "error_breakdown": dict(error_breakdown),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge load-test JSON results from multiple machines")
    parser.add_argument("--inputs", nargs="+", required=True, help="Result files or directories to merge")
    parser.add_argument("--output", required=True, help="Output JSON path")
    args = parser.parse_args()

    files = discover_files(args.inputs)
    result = merge(files)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as f:
        json.dump({"aggregate": result}, f, indent=2)

    print("global_aggregate_final_metrics:")
    for key in [
        "clients_target",
        "connected",
        "login_ok",
        "send_attempts",
        "server_ack",
        "received",
        "ack_success_rate_pct",
        "receive_success_rate_pct",
        "offered_load_msg_per_sec",
        "ack_throughput_msg_per_sec",
        "receive_throughput_msg_per_sec",
        "ack_p95_ms",
        "ack_p99_ms",
        "recv_p95_ms",
        "recv_p99_ms",
        "errors",
        "login_timeouts",
    ]:
        value = result[key]
        print(f"  {key}={value:.2f}" if isinstance(value, float) else f"  {key}={value}")
    if result["server_ack_status_counts"]:
        print("  server_ack_status_counts=" + ", ".join(f"{k}={v}" for k, v in sorted(result["server_ack_status_counts"].items())))
    if result["delivery_status_counts"]:
        print("  delivery_status_counts=" + ", ".join(f"{k}={v}" for k, v in sorted(result["delivery_status_counts"].items())))
    print(f"  exact_latency_percentiles={result['exact_latency_percentiles']}")
    print(f"  result_file={output}")


if __name__ == "__main__":
    main()
