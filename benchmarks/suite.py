#!/usr/bin/env python3
"""Orchestrated benchmark suite for showcasing jsrun strengths."""
from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

import bench_memory
import bench_regex
import bench_startup
import bench_threading

ROOT = Path(__file__).resolve().parent


def _print_title(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def _have_hyperfine() -> bool:
    return shutil.which("hyperfine") is not None


def _run_command(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def _run_hyperfine(
    commands: list[str],
    warmup: int,
    runs: int,
    export_md: Path | None = None,
) -> None:
    args = ["hyperfine", "--warmup", str(warmup), "--runs", str(runs)]
    if export_md is not None:
        args += ["--export-markdown", str(export_md)]
    _run_command(args + commands)


def cmd_startup(iterations: int) -> None:
    _print_title("1) Startup cost (jsrun vs subprocess)")
    bench_startup.main()


def cmd_threading(n: int, threads: int) -> None:
    _print_title("2) Threaded CPU workloads")
    results = bench_threading.run_benchmark(n=n, threads=threads)
    for item in results:
        print(
            f"{item.target:<22}: {item.elapsed:6.3f}s "
            f"({item.threads} threads, fib({item.n}))"
        )
    py, js = results
    print(f"Speedup: {py.elapsed / js.elapsed:.2f}x")


def cmd_regex(iterations: int) -> None:
    _print_title("3) Regex heavy parsing")
    results = bench_regex.run_benchmark(iterations=iterations)
    for item in results:
        print(f"{item.engine:<16}: {item.elapsed:6.3f}s ({item.matches} matches)")
    py, js = results
    print(f"Speedup: {py.elapsed / js.elapsed:.2f}x")


def cmd_memory(count: int) -> None:
    _print_title("4) Runtime memory footprint")
    result = bench_memory.run_benchmark(count=count)
    print(f"Baseline process: {result.baseline.value:.2f} MB")
    print("Single runtime lifecycle:")
    for snapshot in result.single_runtime:
        print(f"  {snapshot.label:<12} -> {snapshot.value:6.2f} MB")
    print(f"Multi-runtime burst ({count} isolates):")
    for snapshot in result.multi_runtime:
        print(f"  {snapshot.label:<12} -> {snapshot.value:6.2f} MB")
    print(f"Per-runtime overhead: {result.per_runtime_overhead:.2f} MB")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    startup_parser = subparsers.add_parser("startup", help="Compare cold starts.")
    startup_parser.add_argument("--iterations", type=int, default=100)

    threading_parser = subparsers.add_parser("threading", help="CPU parallelism benchmark.")
    threading_parser.add_argument("--n", type=int, default=35)
    threading_parser.add_argument("--threads", type=int, default=4)

    regex_parser = subparsers.add_parser("regex", help="Regex performance benchmark.")
    regex_parser.add_argument("--iterations", type=int, default=250)

    memory_parser = subparsers.add_parser("memory", help="Measure per-runtime RSS.")
    memory_parser.add_argument("--count", type=int, default=16)

    subparsers.add_parser("all", help="Run the full suite sequentially.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "startup":
        cmd_startup(args.iterations)
    elif args.command == "threading":
        cmd_threading(args.n, args.threads)
    elif args.command == "regex":
        cmd_regex(args.iterations)
    elif args.command == "memory":
        cmd_memory(args.count)
    elif args.command == "all":
        cmd_startup(100)
        cmd_threading(35, 4)
        cmd_regex(250)
        cmd_memory(16)
    else:
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
