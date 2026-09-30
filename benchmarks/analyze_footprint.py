#!/usr/bin/env python3
"""
analyze_footprint.py

Summarizes the NDJSON produced by bench_footprint.sh: for each condition
(idle, loaded), the mean CPU (millicores) and working-set memory (MiB) of each
management-plane component group, summed over its pods, with the minimum and
maximum across samples, plus per-node totals.

The groups cover the components that the platform deploys on top of
Kubernetes. The K3s server process and general-purpose cluster add-ons
(MetalLB, CoreDNS, the Metrics Server, Traefik) are not included.

Usage:
  ./analyze_footprint.py footprint_logs/footprint_<timestamp>.ndjson [...]
"""

import collections
import json
import statistics
import sys

GROUPS = [
    'MEC operator',
    'Knative Serving control plane',
    'Knative Eventing control plane',
    'Knative Eventing Kafka data plane',
    'Kafka event backbone (Strimzi)',
    'Kyverno policy engine',
    'Kourier ingress gateway',
    'Checkpoint/restore daemons',
]


def group(row):
    ns, name = row['ns'], row['name']
    if ns == 'operator-system':
        return 'MEC operator'
    if ns == 'knative-serving':
        if name.startswith('freeze-daemon'):
            return 'Checkpoint/restore daemons'
        return 'Knative Serving control plane'
    if ns == 'knative-eventing':
        if name.startswith('kafka-'):
            return 'Knative Eventing Kafka data plane'
        return 'Knative Eventing control plane'
    if ns == 'kafka':
        return 'Kafka event backbone (Strimzi)'
    if ns == 'kyverno':
        return 'Kyverno policy engine'
    if ns == 'kourier-system':
        return 'Kourier ingress gateway'
    return None


def main(paths):
    rows = []
    for p in paths:
        with open(p) as fh:
            rows.extend(json.loads(line) for line in fh if line.strip())

    for phase in sorted({r['phase'] for r in rows}):
        pods = [r for r in rows if r['phase'] == phase and r['kind'] == 'pod']
        samples = sorted({r['sample'] for r in pods})
        # per group, per sample: [cpu, mem, pod count]
        per = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0, 0]))
        for r in pods:
            g = group(r)
            if g:
                acc = per[g][r['sample']]
                acc[0] += r['cpu_m']
                acc[1] += r['mem_mi']
                acc[2] += 1

        print(f'=== {phase}: {len(samples)} samples')
        print(f'  {"Component":36s} {"Pods":>4s} {"CPU (m)":>22s} {"Memory (MiB)":>26s}')
        for g in GROUPS:
            vals = [per[g][s] for s in samples if s in per[g]]
            if not vals:
                print(f'  {g:36s} (no pods)')
                continue
            cpu = [v[0] for v in vals]
            mem = [v[1] for v in vals]
            print(f'  {g:36s} {max(v[2] for v in vals):4d} '
                  f'{statistics.mean(cpu):8.1f} [{min(cpu):5d}-{max(cpu):5d}] '
                  f'{statistics.mean(mem):9.1f} [{min(mem):6d}-{max(mem):6d}]')
        tot_cpu = [sum(per[g][s][0] for g in GROUPS if s in per[g]) for s in samples]
        tot_mem = [sum(per[g][s][1] for g in GROUPS if s in per[g]) for s in samples]
        print(f'  {"Total":36s} {"":4s} '
              f'{statistics.mean(tot_cpu):8.1f} [{min(tot_cpu):5d}-{max(tot_cpu):5d}] '
              f'{statistics.mean(tot_mem):9.1f} [{min(tot_mem):6d}-{max(tot_mem):6d}]')

        nodes = [r for r in rows if r['phase'] == phase and r['kind'] == 'node']
        for node in sorted({r['name'] for r in nodes}):
            cpu = [r['cpu_m'] for r in nodes if r['name'] == node]
            mem = [r['mem_mi'] for r in nodes if r['name'] == node]
            print(f'  node {node:31s} n={len(mem):3d} '
                  f'cpu {statistics.mean(cpu):7.1f}  mem {statistics.mean(mem):8.1f} [{min(mem)}-{max(mem)}]')

        daemons = collections.defaultdict(list)
        for r in pods:
            if r['name'].startswith('freeze-daemon'):
                daemons[r['name']].append(r['mem_mi'])
        if daemons:
            means = sorted(statistics.mean(v) for v in daemons.values())
            print(f'  per-daemon memory (MiB): {", ".join(f"{m:.1f}" for m in means)}')
        print()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
