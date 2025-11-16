# Benchmarks

## Prerequisites

```bash
# Build jsrun in release mode (important for accurate benchmarks!)
make build-prod  # or: uv run maturin develop --uv --release

# Install psutil for memory benchmarks
uv pip install psutil

# Ensure Node.js is installed (for comparison)
node --version
```

## Running Benchmarks

```bash
# Run all benchmarks
cd benchmarks
uv run suite.py all

# Or run individual benchmarks
uv run suite.py startup --iterations 200
uv run suite.py threading --threads 8
uv run suite.py regex
uv run suite.py memory
```

## Results

```
1) Startup cost (jsrun vs subprocess)
-------------------------------------
Comparing 100 JavaScript evaluations:
  • jsrun: create Runtime → eval → destroy
  • Node.js: spawn process → eval → terminate
------------------------------------------------------------
jsrun Runtime:      0.363s total, 3.63ms per cycle
Node.js subprocess: 3.978s total, 39.78ms per cycle
------------------------------------------------------------
jsrun is 10.95x faster

2) Threaded CPU workloads
-------------------------
Python (GIL bound)    :  3.750s (4 threads, fib(35))
jsrun (GIL released)  :  0.132s (4 threads, fib(35))
Speedup: 28.37x

3) Regex heavy parsing
----------------------
Python re       : 11.027s (50000 matches)
jsrun / V8      :  4.376s (50000 matches)
Speedup: 2.52x

4) Runtime memory footprint
---------------------------
Baseline process: 81.67 MB
Single runtime lifecycle:
  baseline     ->  81.67 MB
  active       ->  84.84 MB
  after close  ->  81.92 MB
Multi-runtime burst (16 isolates):
  baseline     ->  81.92 MB
  active       -> 123.45 MB
  after close  ->  89.53 MB
Per-runtime overhead: 2.60 MB
```

<sub>Hardware: M1 Pro 32GB</sub>