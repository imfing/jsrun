"""
Type stubs for the jsrun Python extension module.
"""

import types
from typing import (
    Any,
    Callable,
    Optional,
    Self,
)

__all__ = [
    "Runtime",
    "RuntimeConfig",
]

# Core runtime types

class RuntimeConfig:
    """
    Configuration for JavaScript runtime instances.

    Use this to configure heap limits, execution timeouts, and bootstrap scripts
    before creating a Runtime.
    """

    def __init__(
        self,
        max_heap_size: Optional[int] = None,
        initial_heap_size: Optional[int] = None,
        bootstrap: Optional[str] = None,
        timeout: Optional[float | int] = None,
    ) -> None:
        """
        Create a new runtime configuration.

        Args:
            max_heap_size: Maximum heap size in bytes
            initial_heap_size: Initial heap size in bytes
            bootstrap: JavaScript source code to execute on startup
            timeout: Execution timeout in seconds (float or int)
        """
        ...

    @property
    def max_heap_size(self) -> Optional[int]:
        """Maximum heap size in bytes."""
        ...

    @max_heap_size.setter
    def max_heap_size(self, bytes: int) -> None:
        """Set maximum heap size in bytes."""
        ...

    @property
    def initial_heap_size(self) -> Optional[int]:
        """Initial heap size in bytes."""
        ...

    @initial_heap_size.setter
    def initial_heap_size(self, bytes: int) -> None:
        """Set initial heap size in bytes."""
        ...

    @property
    def bootstrap(self) -> Optional[str]:
        """Bootstrap script to execute on runtime startup."""
        ...

    @bootstrap.setter
    def bootstrap(self, source: str) -> None:
        """Set bootstrap script to execute on runtime startup."""
        ...

    @property
    def timeout(self) -> Optional[float]:
        """Execution timeout in seconds."""
        ...

    @timeout.setter
    def timeout(self, timeout: float | int) -> None:
        """
        Set execution timeout.

        Args:
            timeout: Timeout in seconds (float or int)
        """
        ...

    def __repr__(self) -> str: ...

class Runtime:
    """
    Async JavaScript runtime.

    Each Runtime runs on a dedicated OS thread with its own V8 isolate
    and provides async-first JavaScript execution with promise support.
    """

    def __init__(self, config: Optional[RuntimeConfig] = None) -> None: ...
    def eval(self, code: str) -> Any:
        """
        Evaluate JavaScript code synchronously.

        This is a convenience method that blocks on the async evaluation.
        For better performance with promises, use eval_async() instead.

        Args:
            code: JavaScript source code to evaluate

        Returns:
            The native Python representation of the result (int, str, dict, list, etc.)

        Raises:
            RuntimeError: If evaluation fails or times out
            JavaScriptError: If JavaScript code throws an exception

        Example:
            >>> runtime = v8.Runtime()
            >>> result = runtime.eval("1 + 1")
            >>> print(result)
            2
        """
        ...

    async def eval_async(self, code: str, *, timeout_ms: Optional[int] = None) -> Any:
        """
        Evaluate JavaScript code asynchronously.

        This method supports promises and will wait for them to resolve.
        It's the recommended way to execute JavaScript code.

        Args:
            code: JavaScript source code to evaluate
            timeout_ms: Optional timeout in milliseconds

        Returns:
            The native Python representation of the result (int, str, dict, list, etc.)

        Raises:
            RuntimeError: If evaluation fails
            JavaScriptError: If JavaScript code throws an exception
            PromiseTimeoutError: If timeout is exceeded

        Example:
            >>> runtime = v8.Runtime()
            >>> result = await runtime.eval_async("Promise.resolve(42)")
            >>> print(result)
            42
        """
        ...

    def register_op(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        mode: str = "sync",
    ) -> int:
        """
        Register a host operation that can be called from JavaScript.

        Args:
            name: Operation name (must be unique)
            handler: Python callable that handles the operation
            mode: Operation mode ("sync" or "async")

        Returns:
            Operation ID that can be used in JavaScript

        Raises:
            RuntimeError: If registration fails

        Note:
            Async handlers require an active asyncio context. Call
            :meth:`Runtime.eval_async` (or any other async entry point) at
            least once before invoking the op from JavaScript; otherwise the
            runtime will raise an error indicating that the event loop is not
            running.

        Example:
            >>> def add_handler(a, b):
            ...     return a + b
            >>> op_id = runtime.register_op("add", add_handler, mode="sync")
            >>> # From JavaScript: __host_op_sync__(op_id, 10, 20)  # Returns 30
        """
        ...

    def is_closed(self) -> bool:
        """
        Check if the runtime is closed.

        Returns:
            True if the runtime is closed, False otherwise
        """
        ...

    def close(self) -> None:
        """
        Close the runtime and free all resources.

        After calling close(), the runtime can no longer be used.
        This method is called automatically when using the runtime
        as a context manager.
        """
        ...

    def bind_function(
        self,
        name: str,
        handler: Callable[..., Any],
    ) -> None:
        """
        Expose a Python handler as a global JavaScript function.

        Args:
            name: Global function name (assigned on `globalThis`)
            handler: Python callable invoked when JS calls the function

        Note:
            If ``handler`` is async, the runtime must first establish asyncio
            context via :meth:`Runtime.eval_async` (or another async entry
            point). Calling an async-bound function from a purely synchronous
            evaluation without that context will result in an error about the
            event loop not running.

        Example:
            >>> runtime = v8.Runtime()
            >>> def add(a, b): return a + b
            >>> runtime.bind_function("add", add)
            >>> runtime.eval("add(1, 2)")
            3
        """
        ...

    def __enter__(self) -> Self:
        """Context manager entry - returns self."""
        ...

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> bool:
        """Context manager exit - closes the runtime."""
        ...

    def __repr__(self) -> str: ...
