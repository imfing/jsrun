# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

jsrun is a Python library providing JavaScript runtime capabilities via Rust and V8. It exposes a Python API for executing JavaScript code in isolated V8 contexts with async support and permission controls.

**Tech Stack:**
- Rust (core runtime using deno_core)
- Python bindings via PyO3
- Build: Maturin for Python-Rust integration
- Testing: pytest with pytest-asyncio

## Build & Development Commands

### Build the project
```bash
maturin develop
```

### Build for release
```bash
maturin build --release
```

### Run tests
```bash
# Run all Python tests
pytest tests/

# Run specific test file
pytest tests/test_runtime.py

# Run tests with asyncio support
pytest tests/test_runtime.py::TestRuntimeAsync -v
```

### Run Rust tests
```bash
cargo test
```

### Run a single Rust test
```bash
cargo test test_runtime_lifecycle
```

## Architecture

### Multi-Layer Design

The project has three distinct layers that communicate via well-defined boundaries:

1. **Rust Core** (`src/runtime/`): V8 isolate management, async execution, ops system
2. **Rust-Python Bridge** (`src/runtime/python.rs`, `src/lib.rs`): PyO3 bindings
3. **Python API** (`python/jsrun/__init__.py`): User-facing interface

### Threading Model

Each JavaScript runtime runs on a **dedicated OS thread** with its own:
- V8 isolate (single-threaded, non-Send)
- Tokio single-threaded runtime for async operations
- Command channel for host communication

The main Python thread communicates with runtime threads via message passing (`HostCommand` enum in `runner.rs`).

### Key Components

**RuntimeHandle** (`src/runtime/handle.rs`):
- Clone-safe handle to a runtime thread
- Sends commands via async_mpsc channel
- Does NOT auto-shutdown on drop (explicit `close()` required)
- Thread-safe via Arc\<Mutex\> for shutdown state

**JsRuntimeCore** (`src/runtime/runner.rs`):
- Lives on the runtime thread, owns the V8 isolate
- Processes commands from host thread
- Manages primary context + secondary contexts (for isolation)
- Handles promise polling with microtask checkpoints

**Ops System** (`src/runtime/ops.rs`):
- Permission-based host function registry
- Sync and async ops with JSON serialization
- JavaScript calls ops via `__host_op_sync__()` and `__host_op_async__()`

**Context Isolation** (`src/runtime/context.rs`):
- Multiple isolated JavaScript contexts per runtime
- Each context has independent global scope but shares the isolate
- Useful for multi-tenant scenarios

**Module System** (`src/runtime/loader.rs`):
- Static module registration via `add_static_module()`
- Custom module resolution and loading
- Support for both sync and async module evaluation
- ES module imports/exports

### V8 Platform Initialization

V8 requires exactly one global platform instance. The code uses `OnceCell` to ensure `initialize_platform_once()` is safe to call multiple times. Always call this before creating runtimes (it's done automatically in `Runtime()`).

## Important Implementation Details

### Promise Handling

Async evaluation (`eval_async`) works by:
1. Executing the code and checking if result is a promise
2. If promise: poll via repeated `perform_microtask_checkpoint()` + `yield_now()`
3. Check promise state (Pending/Fulfilled/Rejected)
4. Optional timeout enforced via `tokio::time::timeout`

See `poll_promise_with_timeout()` in `runner.rs:626`.

### Op Registry and Embedder Data

Each V8 context stores a pointer to the shared `OpRegistry` in embedder slot 0. This allows JavaScript callback functions to access the registry without additional state passing. The pointer must be properly cleaned up (converted back to `Rc` and dropped) to prevent leaks.

### Python-Rust-JS Data Flow

1. Python calls `runtime.eval("code")`
2. PyO3 converts to Rust `String`
3. `RuntimeHandle` sends `HostCommand::Eval` via channel
4. Runtime thread receives command, compiles V8 script
5. Result converted to string, sent back via sync channel
6. PyO3 converts to Python `str`

For ops: JS → Rust callback → JSON → Python function → JSON → Rust → JS promise resolution

### Error Handling

- Rust errors: `Result<T, String>` (error messages as strings)
- Python exceptions: Converted to `PyRuntimeError` at boundary
- JavaScript exceptions: Caught and converted to Rust `Err`

## Testing Structure

**Rust tests** (`#[cfg(test)]` blocks in each module):
- Unit tests for core components
- Integration tests for runtime lifecycle
- Tests for ops, contexts, async execution

**Python tests** (`tests/`):
- `test_runtime.py`: Comprehensive integration tests
- Tests organized by feature (basics, async, timeout, concurrency)
- Use pytest fixtures and context managers

When adding features, add tests at both layers.

## Common Patterns

### Spawning a runtime (Python)
```python
from jsrun import Runtime

with Runtime() as runtime:
    result = runtime.eval("2 + 2")
```

### Async evaluation (Python)
```python
import asyncio

async def main():
    with Runtime() as runtime:
        result = await runtime.eval_async(
            "Promise.resolve(42)",
            timeout_ms=1000
        )
```

### Creating a new runtime from Rust
```rust
use jsrun::runtime::{RuntimeHandle, RuntimeConfig};

let config = RuntimeConfig::default();
let handle = RuntimeHandle::spawn(config)?;
let result = handle.eval_sync("40 + 2")?;
```

### Registering custom ops
```python
def my_host_function(args):
    # args is a list of JSON-serializable values
    return {"status": "ok", "value": args[0] * 2}

op_id = runtime.register_op("myOp", my_host_function, mode="sync")
```

### Module loading with custom resolver and loader
```python
import asyncio
from jsrun import Runtime

async def main():
    with Runtime() as rt:
        # Static module
        rt.add_static_module("math", "export const answer = 42;")

        # Custom resolver and loader
        def resolver(specifier: str, referrer: str) -> str | None:
            if specifier.startswith("custom:"):
                return specifier
            return None

        async def loader(specifier: str) -> str:
            if specifier == "custom:message":
                return "export const text = 'Hello from custom loader';"
            raise ValueError(f"Unknown module: {specifier}")

        rt.set_module_resolver(resolver)
        rt.set_module_loader(loader)

        # Evaluate module
        namespace = await rt.eval_module_async("entry")
```

### Threading and GIL release
```python
import threading
from jsrun import Runtime

def run_js_in_thread():
    with Runtime() as rt:
        # This releases the GIL, allowing other Python threads to run
        result = rt.eval("Math.sqrt(16)")
        print(f"JS result: {result}")

# Run JS in parallel with Python threads
js_thread = threading.Thread(target=run_js_in_thread)
js_thread.start()
# Other Python threads can make progress while JS runs
js_thread.join()
```

## File Organization

- `src/lib.rs`: PyO3 module definition, exception types
- `src/runtime/mod.rs`: V8 platform initialization, re-exports
- `src/runtime/runner.rs`: Thread spawning, event loop, JsRuntimeCore
- `src/runtime/handle.rs`: RuntimeHandle API
- `src/runtime/python.rs`: Python Runtime class bindings
- `src/runtime/context.rs`: RuntimeContext for isolated execution
- `src/runtime/config.rs`: Configuration builder
- `src/runtime/ops.rs`: Op registry and permissions
- `src/runtime/loader.rs`: Module loading and resolution
- `python/jsrun/__init__.py`: Python package entry point
- `tests/`: Python integration tests
- `examples/`: Usage examples including threading and modules

## Common Pitfalls

1. **Not calling `runtime.close()`**: RuntimeHandle does not auto-close on drop (by design, since handles are cloneable). Always use context manager or explicit close.

2. **Mixing threads with V8**: V8 isolates are NOT Send. All V8 operations must happen on the runtime thread.

3. **Forgetting V8 platform init**: While `Runtime()` handles this, raw Rust usage requires calling `initialize_platform_once()` first.

4. **Op permission mismatches**: Ops requiring permissions will fail if runtime not granted those permissions via `RuntimeConfig`.

5. **Infinite promises**: Using `eval_async` without timeout on never-resolving promises will hang. Always consider timeout_ms for untrusted code.

6. **Module resolution order**: Custom resolver is checked first, then static modules. Return `None` from resolver to fall back to static modules.

7. **GIL release during JS execution**: The runtime properly releases the GIL during JavaScript execution, allowing true parallelism with Python threads.
