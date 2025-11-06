"""
Type stubs for the jsrun Python extension module.
"""

import types
from datetime import timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Mapping,
    Optional,
    Self,
    TypeVar,
    TypedDict,
    overload,
)

__all__ = [
    "JavaScriptError",
    "Runtime",
    "RuntimeConfig",
    "RuntimeStats",
    "JsFunction",
    "JsUndefined",
    "RuntimeTerminated",
    "undefined",
]

F = TypeVar("F", bound=Callable[..., Any])

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

class RuntimeStats:
    """
    Structured snapshot of runtime resource usage and execution counters.

    Instances are returned from :meth:`Runtime.get_stats` and provide
    read-only insight into the V8 heap, total execution time, and active
    resources currently managed by the runtime.
    """

    heap_total_bytes: int
    heap_used_bytes: int
    external_memory_bytes: int
    physical_total_bytes: int
    total_execution_time_ms: int
    last_execution_time_ms: int
    last_execution_kind: str | None
    eval_sync_count: int
    eval_async_count: int
    eval_module_sync_count: int
    eval_module_async_count: int
    call_function_async_count: int
    active_async_ops: int
    open_resources: int
    active_timers: int
    active_intervals: int

    def __repr__(self) -> str: ...

class JsFunction:
    """
    Proxy for a JavaScript function returned from the runtime.

    Instances are awaitable callables that execute on the underlying V8 isolate.
    """

    def __call__(
        self, *args: Any, timeout: Optional[float | int | timedelta] = ...
    ) -> Awaitable[Any]:
        """
        Invoke the JavaScript function with the provided arguments.

        Args:
            *args: Arguments forwarded into JavaScript
            timeout: Optional timeout (seconds as float/int, or datetime.timedelta)

        Returns:
            An awaitable that resolves to the JavaScript return value.
        """
        ...

    def close(self) -> Awaitable[None]:
        """
        Release the function handle.

        After closing, the proxy can no longer be awaited.
        """
        ...

    def __repr__(self) -> str: ...

class JsUndefined:
    """
    Sentinel representing the JavaScript ``undefined`` value in Python.
    """

    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

undefined: JsUndefined

class JsFrame(TypedDict, total=False):
    """Structured stack frame information captured from V8."""

    function_name: str
    file_name: str
    line_number: int
    column_number: int

class JavaScriptError(Exception):
    """Raised when JavaScript code running in the runtime throws an exception.

    The runtime surfaces JavaScript exceptions as ``JavaScriptError`` instances
    that preserve structured metadata from V8, giving callers access to the
    original error class, message, stack trace, and individual frames.

    Attributes:
        name: JavaScript error class name (e.g. ``"TypeError"``). ``None`` when
            non-Error values are thrown.
        message: Error message string when available.
        stack: V8 formatted stack trace string if provided.
        frames: Parsed stack frames populated with optional ``function_name``,
            ``file_name``, ``line_number``, and ``column_number`` keys.

    Example:
        >>> try:
        ...     runtime.eval("throw new TypeError('Invalid value')")
        ... except JavaScriptError as err:
        ...     print(err.name, err.message)
        ...     for frame in err.frames:
        ...         print(frame.get("function_name"), frame.get("file_name"))
    """

    name: str | None
    message: str | None
    stack: str | None
    frames: list[JsFrame]

    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class RuntimeTerminated(RuntimeError):
    """Raised when JavaScript execution is aborted by Runtime.terminate()."""

    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class Runtime:
    """
    Async JavaScript runtime.

    Each Runtime runs on a dedicated OS thread with its own V8 isolate
    and provides async-first JavaScript execution with promise support.

    Runtime objects are not thread-safe; all methods must be invoked from
    the thread that created the runtime while holding the Python GIL.
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

    async def eval_async(
        self, code: str, *, timeout: Optional[float | int | timedelta] = None
    ) -> Any:
        """
        Evaluate JavaScript code asynchronously.

        This method supports promises and will wait for them to resolve.
        It's the recommended way to execute JavaScript code.

        Args:
            code: JavaScript source code to evaluate
            timeout: Optional timeout (seconds as float/int, or datetime.timedelta)

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

    def get_stats(self) -> RuntimeStats:
        """
        Capture a snapshot of runtime resource usage and execution counters.

        Returns:
            RuntimeStats: Structured metrics describing the runtime state.
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

    def terminate(self) -> None:
        """
        Forcefully stop any running JavaScript and prevent future execution.

        Must be invoked from the same thread that owns the runtime. After
        termination, subsequent operations raise ``RuntimeTerminated``.
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

    @overload
    def bind(self, handler: F, /, *, name: Optional[str] = ...) -> F:
        """Bind a synchronous or asynchronous callable to ``globalThis``."""
        ...

    @overload
    def bind(
        self, handler: None = ..., /, *, name: Optional[str] = ...
    ) -> Callable[[F], F]:
        """Return a decorator for binding sync or async callables to ``globalThis``."""

    def bind_object(self, name: str, obj: Mapping[str, Any]) -> None:
        """
        Expose a Python mapping as a global JavaScript object.

        Each key/value pair becomes a property on ``globalThis[name]``. Values
        that are Python callables are bound as callable JavaScript functions,
        following the same async detection rules as :meth:`bind_function`.

        Args:
            name: Object name assigned on ``globalThis``
            obj: Mapping with string keys and JSON-serializable values or
                callables

        Example:
            >>> runtime = v8.Runtime()
            >>> def add(a, b):
            ...     return a + b
            >>> runtime.bind_object("api", {"add": add, "value": 42})
            >>> runtime.eval("api.value")
            42
        """
        ...

    def set_module_resolver(self, resolver: Callable[[str, str], str | None]) -> None:
        """
        Set a custom module resolver for JavaScript imports.

        The resolver function is called whenever JavaScript code attempts to import a module.
        It receives the module specifier and referrer, and should return a fully-qualified
        URL or None to fall back to simple static resolution.

        Args:
            resolver: A callable that takes (specifier, referrer) and returns
                     a fully-qualified URL string or None

        Note:
            By default, no modules are loadable. You must either call this method,
            set_module_loader(), or add_static_module() to enable module loading.

        Example:
            >>> def my_resolver(specifier, referrer):
            ...     if specifier == "my-lib":
            ...         return "static:my-lib"
            ...     return None
            >>> runtime.set_module_resolver(my_resolver)
        """
        ...

    def set_module_loader(self, loader: Callable[[str], Any]) -> None:
        """
        Set a custom module loader for JavaScript imports.

        The loader function is called to fetch the source code for a module specifier.
        It should return the module source code as a string, or a coroutine that resolves
        to the source code for async loading.

        Args:
            loader: A callable that takes a module specifier and returns
                   the module source code as a string, or an awaitable that
                   resolves to the source code

        Note:
            By default, no modules are loadable. You must either call this method,
            set_module_resolver(), or add_static_module() to enable module loading.

        Example:
            >>> def my_loader(specifier):
            ...     if specifier == "my-lib":
            ...         return 'export const value = 42;'
            ...     raise ValueError(f"Unknown module: {specifier}")
            >>> runtime.set_module_loader(my_loader)
        """
        ...

    def add_static_module(self, name: str, source: str) -> None:
        """
        Add a static module that can be imported by JavaScript code.

        This is syntactic sugar for pre-registering modules without custom resolvers.
        The module can be imported by its bare name (e.g., "lib").

        Args:
            name: Module name (will be prefixed with "static:")
            source: JavaScript source code for the module

        Example:
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = runtime.eval_module("lib")
            >>> print(result["value"])
            42
        """
        ...

    def eval_module(self, specifier: str) -> Any:
        """
        Evaluate a JavaScript module synchronously.

        This is a convenience method that blocks on the async module evaluation.
        For better performance with async modules, use eval_module_async() instead.

        Args:
            specifier: Module specifier to evaluate

        Returns:
            The native Python representation of the module namespace (dict-like object)

        Raises:
            RuntimeError: If module evaluation fails or times out
            JavaScriptError: If JavaScript code throws an exception

        Example:
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = runtime.eval_module("lib")
            >>> print(result["value"])
            42
        """
        ...

    async def eval_module_async(
        self, specifier: str, *, timeout: Optional[float | int | timedelta] = None
    ) -> Any:
        """
        Evaluate a JavaScript module asynchronously.

        This method supports async modules and will wait for them to resolve.
        It's the recommended way to evaluate modules.

        Args:
            specifier: Module specifier to evaluate
            timeout: Optional timeout (seconds as float/int, or datetime.timedelta)

        Returns:
            The native Python representation of the module namespace (dict-like object)

        Raises:
            RuntimeError: If module evaluation fails
            JavaScriptError: If JavaScript code throws an exception
            PromiseTimeoutError: If timeout is exceeded

        Example:
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = await runtime.eval_module_async("static:lib")
            >>> print(result["value"])
            42
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
