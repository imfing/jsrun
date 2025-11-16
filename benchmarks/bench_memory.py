#!/usr/bin/env python3
"""Memory benchmark that surfaces per-isolate overhead."""

import dataclasses
import gc
import os
import time

import psutil

from jsrun import Runtime


def _rss_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def _collect() -> None:
    gc.collect()
    time.sleep(0.05)


@dataclasses.dataclass
class MemorySnapshot:
    label: str
    value: float


@dataclasses.dataclass
class MemoryResult:
    baseline: MemorySnapshot
    single_runtime: list[MemorySnapshot]
    multi_runtime: list[MemorySnapshot]
    per_runtime_overhead: float


def _single_runtime_measurement() -> list[MemorySnapshot]:
    _collect()
    before = _rss_mb()
    with Runtime() as runtime:
        runtime.eval("(() => 1 + 1)()")
        _collect()
        during = _rss_mb()
    _collect()
    after = _rss_mb()
    return [
        MemorySnapshot("baseline", before),
        MemorySnapshot("active", during),
        MemorySnapshot("after close", after),
    ]


def _multi_runtime_measurement(count: int) -> list[MemorySnapshot]:
    _collect()
    baseline = _rss_mb()

    runtimes = [Runtime() for _ in range(count)]
    try:
        for runtime in runtimes:
            runtime.eval("1 + 1")
        _collect()
        with_runtimes = _rss_mb()
    finally:
        for runtime in runtimes:
            runtime.close()
    _collect()
    after = _rss_mb()

    return [
        MemorySnapshot("baseline", baseline),
        MemorySnapshot("active", with_runtimes),
        MemorySnapshot("after close", after),
    ]


def run_benchmark(count: int = 16) -> MemoryResult:
    _collect()
    baseline = MemorySnapshot("process start", _rss_mb())
    single = _single_runtime_measurement()
    multi = _multi_runtime_measurement(count)
    per_runtime = (multi[1].value - multi[0].value) / count
    return MemoryResult(baseline, single, multi, per_runtime)


def main() -> None:
    result = run_benchmark()
    print("Memory Benchmark • Detached V8 isolates")
    print("-" * 60)
    print(f"Baseline process: {result.baseline.value:.2f} MB")

    print("\nSingle runtime lifecycle:")
    for snapshot in result.single_runtime:
        print(f"  {snapshot.label:<12}: {snapshot.value:6.2f} MB")

    print("\nMulti-runtime burst (16 isolates):")
    for snapshot in result.multi_runtime:
        print(f"  {snapshot.label:<12}: {snapshot.value:6.2f} MB")

    print("-" * 60)
    print(f"Approximate per-runtime overhead: {result.per_runtime_overhead:.2f} MB")


if __name__ == "__main__":
    main()
