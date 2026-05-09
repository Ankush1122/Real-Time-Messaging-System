
import argparse
import json
import multiprocessing as mp
import random
import socket
import statistics
import time
from pathlib import Path

from chatapp_core.protocol import recv_message, send_message


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    index = min(len(values) - 1, int((pct / 100.0) * len(values)))
    return values[index]


def client_worker(args, worker_index, users):
    results = {'connected': 0, 'login_ok': 0, 'sent': 0, 'acks': 0, 'errors': 0, 'ack_latencies_ms': []}
    sockets = []
    for user_id in users:
        try:
            sock = socket.create_connection((args.host, args.port), timeout=10)
            send_message(sock, {'type': 'login', 'user_id': str(user_id), 'password': args.password})
            response = recv_message(sock)
            results['connected'] += 1
            if response.get('type') == 'login_ok':
                results['login_ok'] += 1
                sockets.append((user_id, sock))
            else:
                sock.close()
        except Exception:
            results['errors'] += 1

    deadline = time.time() + args.duration
    while time.time() < deadline and sockets:
        sender_id, sock = random.choice(sockets)
        receiver_id = random.choice(users)
        if receiver_id == sender_id:
            receiver_id = users[(users.index(sender_id) + 1) % len(users)]
        client_message_id = f'{worker_index}-{sender_id}-{time.time_ns()}'
        started = time.perf_counter()
        try:
            send_message(sock, {'type': 'send_message', 'receiver_id': str(receiver_id), 'text': 'load-test', 'client_message_id': client_message_id})
            response = recv_message(sock)
            results['sent'] += 1
            if response.get('type') == 'send_ack':
                results['acks'] += 1
                results['ack_latencies_ms'].append((time.perf_counter() - started) * 1000)
        except Exception:
            results['errors'] += 1
        time.sleep(max(0.0, random.expovariate(1.0 / args.mean_message_gap)))

    for _user_id, sock in sockets:
        try:
            sock.close()
        except Exception:
            pass
    return results


def split_users(start, total, workers):
    users = list(range(start, start + total))
    return [users[i::workers] for i in range(workers)]


def main():
    parser = argparse.ArgumentParser(description='Multiprocess RTMS load tester')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=12345)
    parser.add_argument('--start-user-id', type=int, default=1)
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--processes', type=int, default=4)
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--mean-message-gap', type=float, default=10.0)
    parser.add_argument('--password', default='password')
    parser.add_argument('--output', default='load_tester/results/result.json')
    args = parser.parse_args()

    groups = split_users(args.start_user_id, args.clients, args.processes)
    with mp.Pool(processes=args.processes) as pool:
        partials = pool.starmap(client_worker, [(args, index, group) for index, group in enumerate(groups)])

    merged = {'connected': 0, 'login_ok': 0, 'sent': 0, 'acks': 0, 'errors': 0, 'ack_latencies_ms': []}
    for item in partials:
        for key in ['connected', 'login_ok', 'sent', 'acks', 'errors']:
            merged[key] += item[key]
        merged['ack_latencies_ms'].extend(item['ack_latencies_ms'])

    merged['ack_p50_ms'] = statistics.median(merged['ack_latencies_ms']) if merged['ack_latencies_ms'] else None
    merged['ack_p95_ms'] = percentile(merged['ack_latencies_ms'], 95)
    merged['ack_p99_ms'] = percentile(merged['ack_latencies_ms'], 99)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(merged, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in merged.items() if k != 'ack_latencies_ms'}, indent=2))


if __name__ == '__main__':
    main()
