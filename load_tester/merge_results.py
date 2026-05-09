
import argparse
import json
from pathlib import Path


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    index = min(len(values) - 1, int((pct / 100.0) * len(values)))
    return values[index]


def main():
    parser = argparse.ArgumentParser(description='Merge RTMS load-test result JSON files')
    parser.add_argument('files', nargs='+')
    parser.add_argument('--output', default='load_tester/results/merged.json')
    args = parser.parse_args()

    merged = {'connected': 0, 'login_ok': 0, 'sent': 0, 'acks': 0, 'errors': 0, 'ack_latencies_ms': []}
    for filename in args.files:
        data = json.loads(Path(filename).read_text(encoding='utf-8'))
        for key in ['connected', 'login_ok', 'sent', 'acks', 'errors']:
            merged[key] += int(data.get(key, 0))
        merged['ack_latencies_ms'].extend(data.get('ack_latencies_ms', []))

    merged['ack_p95_ms'] = percentile(merged['ack_latencies_ms'], 95)
    merged['ack_p99_ms'] = percentile(merged['ack_latencies_ms'], 99)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(merged, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in merged.items() if k != 'ack_latencies_ms'}, indent=2))


if __name__ == '__main__':
    main()
