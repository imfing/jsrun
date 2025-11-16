# jsrun

**Modern JavaScript runtime for Python**

jsrun is a Python library that embeds the [V8][v8] JavaScript engine, allowing you to run JavaScript code directly from Python.

Whether you need to run user scripts, integrate JavaScript libraries, execute code for AI agents, or build extensible Python applications, `jsrun` provides a robust solution.

## Highlights

- 🚀 **Fast**: Powered by [V8][v8], the JavaScript engine used in Chrome and Node.js
- 🔌 **Extensible**: Bind Python functions and objects to JavaScript
- ⚡ **Async First**: Support JavaScript [Promises][promise] and [async function][async function] in Python [asyncio][asyncio]
- 🔒 **Isolated Sandbox**: Runtime is a V8 isolate that has no I/O access by default
- 🎛️ **Resource Controls**: Prevent abuse by setting per-runtime heap memory limits and execution timeouts
- 🧵 **Thread-Safe**: Run multiple runtimes in parallel on different Python threads
- 📦 **Module Support**: ES modules with custom loaders and resolvers
- ⚙️ **WebAssembly**: Execute WebAssembly (WASM) directly in native runtime
- 🎯 **Typing**: Comprehensive type hints for PyO3 bindings

## Quick Example

```python
import jsrun

result = jsrun.eval("2 + 2")
print(result)  # 4

# Bind Python function
jsrun.bind_function("add", lambda a, b: a + b)
print(jsrun.eval("add(2, 3)"))  # 5
```

## Installation

```bash
pip install jsrun
```

## Next Steps

- Discover essential features in [Quick Start](quickstart.md)

[v8]: https://v8.dev/
[promise]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Using_promises
[async function]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function
[asyncio]: https://docs.python.org/3/library/asyncio.html
