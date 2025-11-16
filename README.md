<div align="center">
  <a href="https://github.com/imfing/jsrun">
    <picture>
      <img alt="jsrun" width="100" src="docs/assets/logo.svg"  >
    </picture>
  </a>

# jsrun

**Modern JavaScript runtime in Python**

Seamlessly run JavaScript next to Python with secure isolation, powered by V8 and bridged with Rust

<br />

[![CI](https://github.com/imfing/jsrun/actions/workflows/CI.yml/badge.svg?branch=main)][workflows-ci]
[![PyPI](https://img.shields.io/pypi/v/jsrun.svg)][jsrun-pypi]
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

<p align="center">

  <a href="https://imfing.github.io/jsrun/"><strong>Documentation</strong></a>
  ·
  <a href="https://github.com/imfing/jsrun/tree/main/examples"><strong>Examples</strong></a>
  ·
  <a href="https://github.com/imfing/jsrun/issues"><strong>Issues</strong></a>
</p>

</div>

`jsrun` is a Python library that embeds the [V8][v8] JavaScript engine with Rust ([PyO3][pyo3]). Run trusted or user-provided JavaScript alongside Python, bind Python objects into JavaScript in an isolated environment:

```python
import jsrun

result = jsrun.eval("2 + 2")
print(result)  # 4

# Bind Python function
jsrun.bind_function("add", lambda a, b: a + b)
print(jsrun.eval("add(2, 3)"))  # 5
```

## Highlights

- **Fast V8 core** – the JavaScript engine used by Chrome and Node.js, bridged via Rust and PyO3
- **Async-first APIs** – run async JavaScript without blocking Python code
- **Extensible bindings** – expose Python functions/objects to JavaScript
- **Secure defaults** – runtimes start with zero I/O, isolated V8 isolates per thread, and configurable heap/time limits
- **Module & WASM support** – load ES modules with custom resolvers and execute WebAssembly directly

## Get Started

To get started, install it from PyPI (Python 3.10+ and macOS/Linux are required):

```bash
pip install jsrun  # or uv pip install jsrun
```

> **Note**: `jsrun` is under development. Expect breaking changes between minor versions.

Quick Example

```python
import jsrun

# Simple expression
print(jsrun.eval("Math.sqrt(25)"))  # 5

# Share Python function with JavaScript
jsrun.bind_function("add", lambda a, b: a + b)
print(jsrun.eval("add(3, 4)"))  # 7
```

Need full control? Use the [`Runtime`](https://imfing.github.io/jsrun/api/runtime) class to configure heap/time limits, module loaders, and lifecycle management.

```python
from jsrun import Runtime

config = RuntimeConfig(max_heap_size=10 * 1024 * 1024)  # 10MB limit

with Runtime(config) as runtime:
    print(runtime.eval("42"))  # 42
```

## Use Cases

`jsrun` is designed for modern Python applications that need embedded JavaScript:

- **[AI agents](https://imfing.github.io/jsrun/use-cases/agents/)** – execute LLM generated JavaScript in isolated sandboxes with memory/time limits
- **[Workflow runners](https://imfing.github.io/jsrun/use-cases/workflows/)** – let users upload JavaScript automations backed by your Python host
- **[Serverless / plugin runtimes](https://imfing.github.io/jsrun/use-cases/serverless/)** – spin up per-request V8 isolates with custom APIs
- **[Data playgrounds](https://imfing.github.io/jsrun/use-cases/playground/)** – build notebooks or playgrounds that mix Python data and JS visualizations
- **[JavaScript libraries](https://github.com/imfing/jsrun/blob/main/examples/markdown_parser.py)** – use JavaScript packages like [marked.js][marked-js] directly in Python without Node.js

### Example: Code execution sandbox for AI Agent

One of the most compelling use cases for `jsrun` is building safe execution environments for AI agents. When LLMs generate code, you need a way to run it securely with strict resource limits and isolation.

This example shows how to create a [Pydantic AI](https://ai.pydantic.dev/) agent that can execute JavaScript code in a sandboxed V8 runtime with heap limits and timeouts:

```python
import asyncio

from jsrun import JavaScriptError, Runtime, RuntimeConfig
from pydantic_ai import Agent, RunContext

# Define the agent with code execution tool
agent = Agent(
    "openai:gpt-5-mini",
    system_prompt="""You are a helpful assistant that can execute JavaScript code.
    When users ask you to perform calculations or data transformations,
    you can write and execute JavaScript code to get accurate results.
    Always explain what the code does before showing the result.""",
)


@agent.tool
async def execute_javascript(ctx: RunContext, code: str) -> str:
    """
    Execute JavaScript code in a sandboxed environment.

    Args:
        code: The JavaScript code to execute

    Returns:
        The result of the code execution as a string
    """
    # Log or audit the code being executed (for observability)
    print(f"[Executing JavaScript code] '{code}'")

    try:
        # Create a runtime with safety limits
        config = RuntimeConfig(
            max_heap_size=10 * 1024 * 1024,  # 10MB heap limit
        )

        with Runtime(config) as runtime:
            result = await runtime.eval_async(code, timeout=5.0)
            print(f"[Execution result] {result}")
            return f"Result: {result}"
    except TimeoutError:
        return "Error: Code execution timed out (exceeded 5 seconds)"
    except JavaScriptError as e:
        return f"JavaScript Error: {e}"
    except Exception as e:
        return f"Execution Error: {e}"


async def main():
    result = await agent.run("Calculate the sum of squares from 1 to 100")
    print(f"AGENT OUTPUT: {result.output}")


if __name__ == "__main__":
    asyncio.run(main())
```

Explore more in [Use Cases](https://imfing.github.io/jsrun/use-cases/playground/) and [Examples](https://github.com/imfing/jsrun/tree/main/examples)

## Documentation

- [Quick Start](https://imfing.github.io/jsrun/quickstart)
- [Concepts](https://imfing.github.io/jsrun/concepts/runtime): runtimes, type conversion, resource controls
- [Guides](https://imfing.github.io/jsrun/guides/bindings): binding functions, module loading
- [API reference](https://imfing.github.io/jsrun/api/jsrun/)


[v8]: https://v8.dev
[pyo3]: https://pyo3.rs/
[jsrun-pypi]: https://pypi.org/project/jsrun/
[workflows-ci]: https://github.com/imfing/jsrun/actions/workflows/CI.yml
[marked-js]: https://github.com/markedjs/marked