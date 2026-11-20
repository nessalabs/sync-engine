#!/usr/bin/env python3
"""Deterministic freshness contracts and measured native SQLite cache reads."""
import argparse
import json
import statistics
import subprocess
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--repetitions', type=int, default=1)
args = parser.parse_args()
assert args.repetitions > 0
subprocess.run(['cargo', 'test', '--locked', '--no-default-features', '--test', 'freshness'], check=True, capture_output=True, text=True)
results = []
for _ in range(args.repetitions):
    run = subprocess.run(['cargo', 'run', '--locked', '--quiet', '--no-default-features', '--features', 'sqlite', '--example', 'freshness'], check=True, capture_output=True, text=True)
    result = json.loads(run.stdout)
    assert result['uncertain_commit_reopen_correct'] is True
    for row, heads in zip(result['rows'], [100, 100, 9, 0], strict=True):
        assert row['useful_reads'] == 100 and row['read_head_checks'] == heads
        assert row['cold_head_checks'] == 2 and row['cold_pages'] == 4
        assert row['read_pages'] == 0 and row['reopened_rows'] == 7
        assert row['restart_times_unknown'] is True
    results.append(result)
summary = []
for i, row in enumerate(results[0]['rows']):
    times = [r['rows'][i]['elapsed_ms'] for r in results]
    summary.append({**row, 'elapsed_ms': {'min': min(times), 'median': statistics.median(times), 'max': max(times)}})
print(json.dumps({'repetitions': args.repetitions, 'deterministic_contracts_passed': True, 'conditions': results[0]['clock'], 'transport': results[0]['transport'], 'uncertain_commit_reopen_correct': True, 'rows': summary, 'runs': results}, separators=(',', ':')))
