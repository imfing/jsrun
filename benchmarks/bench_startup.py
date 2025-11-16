#!/usr/bin/env python3
"""Startup benchmark: jsrun runtime vs subprocess overhead"""

import subprocess
import time
from jsrun import Runtime


def bench_jsrun(iterations: int = 100) -> float:
    """Measure jsrun runtime creation overhead"""
    start = time.perf_counter()
    for _ in range(iterations):
        with Runtime() as rt:
            rt.eval("2 + 2")
    return time.perf_counter() - start


def bench_subprocess(iterations: int = 100) -> float:
    """Measure subprocess overhead"""
    start = time.perf_counter()
    for _ in range(iterations):
        subprocess.run(
            ["node", "-e", "2 + 2"],
            capture_output=True,
            check=True,
        )
    return time.perf_counter() - start


def main() -> None:
    iterations = 100
    print(f"Comparing {iterations} JavaScript evaluations:")
    print(f"  • jsrun: create Runtime → eval → destroy")
    print(f"  • Node.js: spawn process → eval → terminate")
    print("-" * 60)

    # Warmup
    bench_jsrun(5)
    bench_subprocess(5)

    jsrun_time = bench_jsrun(iterations)
    subprocess_time = bench_subprocess(iterations)

    jsrun_per = (jsrun_time / iterations) * 1000
    subprocess_per = (subprocess_time / iterations) * 1000

    print(f"jsrun Runtime:      {jsrun_time:.3f}s total, {jsrun_per:.2f}ms per cycle")
    print(
        f"Node.js subprocess: {subprocess_time:.3f}s total, {subprocess_per:.2f}ms per cycle"
    )
    print("-" * 60)
    print(f"jsrun is {subprocess_time / jsrun_time:.2f}x faster")


if __name__ == "__main__":
    main()
