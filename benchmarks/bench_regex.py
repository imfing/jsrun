#!/usr/bin/env python3
"""Regex benchmark that highlights V8's JIT for complex patterns."""

import dataclasses
import re
import time
from typing import Tuple

from jsrun import Runtime


def generate_email_corpus(domains: int = 50, per_domain: int = 1000) -> str:
    chunks = []
    for domain_id in range(domains):
        for i in range(per_domain):
            chunks.append(
                f"user{i}_{domain_id}@example{domain_id}.com "
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
            )

    return "".join(chunks)


def _benchmark_python_regex(text: str, iterations: int) -> Tuple[float, int]:
    pattern = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")

    started = time.perf_counter()
    matches = 0
    for _ in range(iterations):
        matches = len(pattern.findall(text))

    return time.perf_counter() - started, matches


def _benchmark_jsrun_regex(text: str, iterations: int) -> Tuple[float, int]:
    with Runtime() as runtime:
        escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

        code = f"""
        const corpus = "{escaped}";
        const regexp = /\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{{2,}}\\b/g;
        let matches = [];
        for (let i = 0; i < {iterations}; i++) {{
            matches = corpus.match(regexp);
        }}
        matches.length;
        """

        started = time.perf_counter()
        count = runtime.eval(code)
        return time.perf_counter() - started, count


@dataclasses.dataclass
class RegexResult:
    engine: str
    elapsed: float
    matches: int


def run_benchmark(iterations: int = 250) -> list[RegexResult]:
    text = generate_email_corpus()
    python_elapsed, python_matches = _benchmark_python_regex(text, iterations)
    jsrun_elapsed, jsrun_matches = _benchmark_jsrun_regex(text, iterations)
    return [
        RegexResult("Python re", python_elapsed, python_matches),
        RegexResult("jsrun / V8", jsrun_elapsed, jsrun_matches),
    ]


def main() -> None:
    print("Regex Benchmark • Extracting 50k synthetic emails")
    print("-" * 60)
    results = run_benchmark()
    for result in results:
        print(
            f"{result.engine:<16} -> {result.elapsed:6.3f}s ({result.matches} matches)"
        )

    py, js = results
    print("-" * 60)
    print(f"jsrun speedup: {py.elapsed / js.elapsed:.2f}x")


if __name__ == "__main__":
    main()
