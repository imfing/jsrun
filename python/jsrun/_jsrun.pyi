"""
Type stubs for the jsrun Python extension module.
"""

import types
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping
from datetime import timedelta
from typing import (
    Any,
    Self,
    TypedDict,
    TypeVar,
    overload,
)

__all__ = [
    "InspectorConfig",
    "InspectorEndpoints",
    "JavaScriptError",
    "JsFunction",
    "JsStream",
    "JsUndefined",
    "Runtime",
    "RuntimeConfig",
    "RuntimeStats",
    "RuntimeTerminated",
    "SnapshotBuilder",
    "undefined",
]

_F = TypeVar("_F", bound=Callable[..., Any])

# Core runtime types

class InspectorConfig:
    """
    Configuration for enabling the Chrome DevTools inspector.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9229,
        *,
        wait_for_connection: bool = False,
        break_on_next_statement: bool = False,
        target_url: str | None = None,
        display_name: str | None = None,
    ) -> None:
        """
        Create a new inspector configuration.

        Args:
            host: Host interface to bind (defaults to ``127.0.0.1``)
            port: TCP port for the DevTools server
            wait_for_connection: Block execution until a debugger connects
            break_on_next_statement: Pause on the first statement after a debugger attaches
            target_url: Optional string reported to DevTools for the inspected target
            display_name: Optional display title surfaced in ``chrome://inspect``
        """

    @property
    def host(self) -> str: ...
    @property
    def port(self) -> int: ...
    @property
    def wait_for_connection(self) -> bool: ...
    @wait_for_connection.setter
    def wait_for_connection(self, enabled: bool) -> None: ...
    @property
    def break_on_next_statement(self) -> bool: ...
    @break_on_next_statement.setter
    def break_on_next_statement(self, enabled: bool) -> None: ...
    @property
    def target_url(self) -> str | None: ...
    @target_url.setter
    def target_url(self, value: str | None) -> None: ...
    @property
    def display_name(self) -> str | None: ...
    @display_name.setter
    def display_name(self, value: str | None) -> None: ...
    def endpoint(self) -> str:
        """Return the ``host:port`` pair that DevTools should connect to."""

class RuntimeConfig:
    """
    Configuration for JavaScript runtime instances.

    Use this to configure heap limits, execution timeouts, and bootstrap scripts
    before creating a Runtime.
    """

    def __init__(
        self,
        max_heap_size: int | None = None,
        initial_heap_size: int | None = None,
        bootstrap: str | None = None,
        timeout: float | None = None,
        enable_console: bool | None = False,
        inspector: InspectorConfig | None = None,
        snapshot: bytes | None = None,
        max_serialization_depth: int | None = None,
        max_serialization_bytes: int | None = None,
    ) -> None:
        """
        Create a new runtime configuration.

        Args:
            max_heap_size: Maximum heap size in bytes
            initial_heap_size: Initial heap size in bytes
            bootstrap: JavaScript source code to execute on startup
            timeout: Execution timeout in seconds (float or int)
            enable_console: Whether ``console`` APIs are exposed (defaults to False)
            inspector: Optional inspector configuration enabling Chrome DevTools
            snapshot: Optional V8 startup snapshot bytes
            max_serialization_depth: Maximum nesting depth when transferring values
            max_serialization_bytes: Maximum serialized byte size when transferring values
        """

    @property
    def max_heap_size(self) -> int | None:
        """Maximum heap size in bytes."""

    @max_heap_size.setter
    def max_heap_size(self, bytes: int) -> None:
        """Set maximum heap size in bytes."""

    @property
    def initial_heap_size(self) -> int | None:
        """Initial heap size in bytes."""

    @initial_heap_size.setter
    def initial_heap_size(self, bytes: int) -> None:
        """Set initial heap size in bytes."""

    @property
    def bootstrap(self) -> str | None:
        """Bootstrap script to execute on runtime startup."""

    @bootstrap.setter
    def bootstrap(self, source: str) -> None:
        """Set bootstrap script to execute on runtime startup."""

    @property
    def timeout(self) -> float | None:
        """Execution timeout in seconds."""

    @timeout.setter
    def timeout(self, timeout: float) -> None:
        """
        Set execution timeout.

        Args:
            timeout: Timeout in seconds (float or int)
        """

    @property
    def enable_console(self) -> bool | None:
        """Whether ``console`` APIs are enabled inside the runtime."""

    @property
    def inspector(self) -> InspectorConfig | None:
        """Inspector configuration if debugging is enabled."""

    @inspector.setter
    def inspector(self, value: InspectorConfig | None) -> None:
        """Set or clear the inspector configuration."""

    @property
    def max_serialization_depth(self) -> int:
        """Maximum recursion depth allowed when serializing values."""

    @max_serialization_depth.setter
    def max_serialization_depth(self, value: int) -> None:
        """Set the maximum recursion depth allowed when serializing values."""

    @property
    def max_serialization_bytes(self) -> int:
        """Maximum byte size allowed when serializing values."""

    @max_serialization_bytes.setter
    def max_serialization_bytes(self, value: int) -> None:
        """Set the maximum byte size allowed when serializing values."""

class RuntimeStats:
    """
    Structured snapshot of runtime resource usage and execution counters.

    Instances are returned from [`Runtime.get_stats()`][jsrun.Runtime.get_stats] and provide
    read-only insight into the V8 heap, total execution time, and active
    resources currently managed by the runtime.

    Attributes:
        heap_total_bytes: Total heap size in bytes allocated by V8
        heap_used_bytes: Currently used heap memory in bytes
        external_memory_bytes: External memory tracked by V8 (e.g., ArrayBuffers)
        physical_total_bytes: Physical memory in bytes (RSS)
        total_execution_time_ms: Cumulative execution time in milliseconds
        last_execution_time_ms: Duration of the most recent execution in milliseconds
        last_execution_kind: Type of last operation (e.g., "eval_async", "call_function_sync")
        eval_sync_count: Number of synchronous eval operations
        eval_async_count: Number of asynchronous eval operations
        eval_module_sync_count: Number of synchronous module evaluations
        eval_module_async_count: Number of asynchronous module evaluations
        call_function_async_count: Number of asynchronous function calls
        call_function_sync_count: Number of synchronous function calls
        active_async_ops: Currently active async operations
        open_resources: Number of open resources (timers, streams, etc.)
        active_timers: Active setTimeout timers
        active_intervals: Active setInterval timers
        active_js_streams: Active JavaScript ReadableStreams exposed to Python
        active_py_streams: Active Python async iterables exposed to JavaScript
        total_js_streams: Total JavaScript streams created
        total_py_streams: Total Python streams created
        bytes_streamed_js_to_py: Total bytes transferred from JavaScript to Python
        bytes_streamed_py_to_js: Total bytes transferred from Python to JavaScript
    """

    @property
    def heap_total_bytes(self) -> int:
        """Total heap size in bytes allocated by V8."""

    @property
    def heap_used_bytes(self) -> int:
        """Currently used heap memory in bytes."""

    @property
    def external_memory_bytes(self) -> int:
        """External memory tracked by V8 (e.g., ArrayBuffers)."""

    @property
    def physical_total_bytes(self) -> int:
        """Physical memory in bytes (RSS)."""

    @property
    def total_execution_time_ms(self) -> int:
        """Cumulative execution time in milliseconds."""

    @property
    def last_execution_time_ms(self) -> int:
        """Duration of the most recent execution in milliseconds."""

    @property
    def last_execution_kind(self) -> str | None:
        """Type of last operation (e.g., "eval_async", "call_function_sync")."""

    @property
    def eval_sync_count(self) -> int:
        """Number of synchronous eval operations."""

    @property
    def eval_async_count(self) -> int:
        """Number of asynchronous eval operations."""

    @property
    def eval_module_sync_count(self) -> int:
        """Number of synchronous module evaluations."""

    @property
    def eval_module_async_count(self) -> int:
        """Number of asynchronous module evaluations."""

    @property
    def call_function_async_count(self) -> int:
        """Number of asynchronous function calls."""

    @property
    def call_function_sync_count(self) -> int:
        """Number of synchronous function calls."""

    @property
    def active_async_ops(self) -> int:
        """Currently active async operations."""

    @property
    def open_resources(self) -> int:
        """Number of open resources (timers, streams, etc.)."""

    @property
    def active_timers(self) -> int:
        """Active setTimeout timers."""

    @property
    def active_intervals(self) -> int:
        """Active setInterval timers."""

    @property
    def active_js_streams(self) -> int:
        """Active JavaScript ReadableStreams exposed to Python."""

    @property
    def active_py_streams(self) -> int:
        """Active Python async iterables exposed to JavaScript."""

    @property
    def total_js_streams(self) -> int:
        """Total JavaScript streams created."""

    @property
    def total_py_streams(self) -> int:
        """Total Python streams created."""

    @property
    def bytes_streamed_js_to_py(self) -> int:
        """Total bytes transferred from JavaScript to Python."""

    @property
    def bytes_streamed_py_to_js(self) -> int:
        """Total bytes transferred from Python to JavaScript."""

class InspectorEndpoints:
    """
    Runtime inspector metadata exposing DevTools URLs.

    Attributes:
        id: Unique identifier for the inspector session
        websocket_url: WebSocket URL for the Chrome DevTools Protocol
        devtools_frontend_url: DevTools frontend URL to open in Chrome
        title: Display title for the inspector target
        description: Description of the inspector target
        target_url: URL of the target being inspected
        favicon_url: Favicon URL for the inspector target
        host: Host address where the inspector is listening
    """

    id: str
    websocket_url: str
    devtools_frontend_url: str
    title: str
    description: str
    target_url: str
    favicon_url: str
    host: str

class JsFunction:
    """
    Proxy for a JavaScript function returned from the runtime.

    Instances execute on the underlying V8 isolate. Calling the proxy returns
    either a direct value (for synchronous JavaScript functions) or an awaitable
    object when the JavaScript side returns a pending Promise.
    """

    def __call__(
        self, *args: Any, timeout: float | timedelta | None = ...
    ) -> Any | Awaitable[Any]:
        """
        Invoke the JavaScript function with the provided arguments. If the JS
        function finishes synchronously, its return value is produced directly.
        Otherwise, the result is awaitable and must be awaited.

        Args:
            *args: Arguments forwarded into JavaScript
            timeout: Optional timeout (seconds as float/int, or datetime.timedelta)

        Returns:
            Either the JavaScript return value or an awaitable resolving to it.
        """

    def call_async(
        self, *args: Any, timeout: float | timedelta | None = ...
    ) -> Awaitable[Any]:
        """
        Always invoke the JavaScript function asynchronously, returning an awaitable
        regardless of whether the underlying JS completes synchronously.
        """

    def close(self) -> Awaitable[None]:
        """
        Release the function handle.

        After closing, the proxy can no longer be awaited.
        """

class JsStream:
    """
    Async iterator proxy for a JavaScript ``ReadableStream`` returned from the runtime.
    """

    def __aiter__(self) -> Self: ...
    def __anext__(self) -> Awaitable[Any]:
        """
        Await and return the next chunk yielded by the underlying JS stream.

        Raises:
            StopAsyncIteration: When the stream finishes.
            JavaScriptError: If the stream reader errors.
        """

    def close(self) -> None:
        """Cancel the stream and release its runtime resources."""

class PyStreamSource:
    """
    Handle that exposes a Python async iterable to JavaScript as a ``ReadableStream``.
    """

    def close(self) -> None: ...

class JsUndefined:
    """
    Sentinel representing the JavaScript ``undefined`` value in Python.
    """

    def __bool__(self) -> bool: ...

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
        ```python
        >>> try:
        ...     runtime.eval("throw new TypeError('Invalid value')")
        ... except JavaScriptError as err:
        ...     print(err.name, err.message)
        ...     for frame in err.frames:
        ...         print(frame.get("function_name"), frame.get("file_name"))
        ```
    """

    name: str | None
    message: str | None
    stack: str | None
    frames: list[JsFrame]

class RuntimeTerminated(RuntimeError):
    """Raised when JavaScript execution is aborted by Runtime.terminate()."""

class Runtime:
    """
    JavaScript runtime.

    Each Runtime runs on a dedicated OS thread with its own V8 isolate
    and provides async-first JavaScript execution with promise support.

    Runtime objects are not thread-safe; all methods must be invoked from
    the thread that created the runtime while holding the Python GIL.
    """

    def __init__(self, config: RuntimeConfig | None = None) -> None:
        """
        Create a runtime instance with optional configuration.

        Args:
            config: Optional :class:`RuntimeConfig` to customize heap limits, bootstrap
                scripts, inspector, etc.
        """
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
            ```python
            >>> runtime = Runtime()
            >>> result = runtime.eval("1 + 1")
            >>> print(result)
            2
            ```
        """

    async def eval_async(
        self, code: str, *, timeout: float | timedelta | None = None
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

        Example:
            ```python
            >>> runtime = Runtime()
            >>> result = await runtime.eval_async("Promise.resolve(42)")
            >>> print(result)
            42
            ```
        """

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
            ```python
            >>> def add_handler(a, b):
            ...     return a + b
            >>> op_id = runtime.register_op("add", add_handler, mode="sync")
            >>> # From JavaScript: __host_op_sync__(op_id, 10, 20)  # Returns 30
            ```
        """

    def is_closed(self) -> bool:
        """
        Check if the runtime is closed.

        Returns:
            True if the runtime is closed, False otherwise
        """

    def get_stats(self) -> RuntimeStats:
        """
        Capture a snapshot of runtime resource usage and execution counters.

        Returns:
            RuntimeStats: Structured metrics describing the runtime state.
        """

    def inspector_endpoints(self) -> InspectorEndpoints | None:
        """
        Return the DevTools endpoints if the inspector is enabled.

        Returns:
            InspectorEndpoints: describing websocket and devtools:// URLs, or \
                ``None`` when the runtime was created without inspector support.
        """

    def close(self) -> None:
        """
        Close the runtime and free all resources.

        After calling close(), the runtime can no longer be used.
        This method is called automatically when using the runtime
        as a context manager.
        """

    def terminate(self) -> None:
        """
        Forcefully stop any running JavaScript and prevent future execution.

        Must be invoked from the same thread that owns the runtime. After
        termination, subsequent operations raise ``RuntimeTerminated``.
        """

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
            ```python
            >>> runtime = Runtime()
            >>> def add(a, b): return a + b
            >>> runtime.bind_function("add", add)
            >>> runtime.eval("add(1, 2)")
            3
            ```
        """

    def stream_from_async_iterable(
        self, iterable: AsyncIterable[Any]
    ) -> PyStreamSource:
        """
        Wrap a Python async iterable and expose it to JavaScript as a ``ReadableStream``.

        Args:
            iterable: Object implementing ``__aiter__`` that yields JSON-serializable chunks.

        Returns:
            Handle that can be passed into JavaScript and consumed via stream readers.
        """

    @overload
    def bind(self, handler: _F, /, *, name: str | None = ...) -> _F:
        """Bind a synchronous or asynchronous callable to ``globalThis``."""

    @overload
    def bind(
        self, handler: None = ..., /, *, name: str | None = ...
    ) -> Callable[[_F], _F]:
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
            ```python
            >>> runtime = Runtime()
            >>> def add(a, b):
            ...     return a + b
            >>> runtime.bind_object("api", {"add": add, "value": 42})
            >>> runtime.eval("api.value")
            42
            ```
        """

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
            ```python
            >>> def my_resolver(specifier, referrer):
            ...     if specifier == "my-lib":
            ...         return "static:my-lib"
            ...     return None
            >>> runtime.set_module_resolver(my_resolver)
            ```
        """

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
            ```python
            >>> def my_loader(specifier):
            ...     if specifier == "my-lib":
            ...         return 'export const value = 42;'
            ...     raise ValueError(f"Unknown module: {specifier}")
            >>> runtime.set_module_loader(my_loader)
            ```
        """

    def add_static_module(self, name: str, source: str) -> None:
        """
        Add a static module that can be imported by JavaScript code.

        This is syntactic sugar for pre-registering modules without custom resolvers.
        The module can be imported by its bare name (e.g., ``lib``).

        Args:
            name: Module name (will be prefixed with ``static:``)
            source: JavaScript source code for the module

        Example:
            ```python
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = runtime.eval_module("lib")
            >>> print(result["value"])
            42
            ```
        """

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
            ```python
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = runtime.eval_module("lib")
            >>> print(result["value"])
            42
            ```
        """

    async def eval_module_async(
        self, specifier: str, *, timeout: float | timedelta | None = None
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

        Example:
            ```python
            >>> runtime.add_static_module("lib", "export const value = 42;")
            >>> result = await runtime.eval_module_async("static:lib")
            >>> print(result["value"])
            42
            ```
        """

    def __enter__(self) -> Self:
        """Context manager entry - returns self."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool:
        """Context manager exit - closes the runtime."""

class SnapshotBuilder:
    """
    Build a V8 snapshot covering a custom bootstrap script.

    Use [`execute_script()`][jsrun.SnapshotBuilder.execute_script] to run multiple scripts before calling
    [`build()`][jsrun.SnapshotBuilder.build] to produce the serialized bytes.
    """

    def __init__(
        self,
        *,
        bootstrap: str | None = None,
        enable_console: bool | None = False,
    ) -> None:
        """
        Create a snapshot builder.

        Args:
            bootstrap: Optional JavaScript source run before any scripts added via ``execute_script``.
            enable_console: Whether ``console`` APIs remain available while preparing the snapshot.
                Defaults to ``False`` (console is disabled by default).
        """

    def execute_script(self, name: str, source: str) -> None:
        """
        Run a JavaScript script inside the snapshot builder.

        Args:
            name: Identifier used in stack traces / debugging.
            source: Source text to compile and execute. The code is executed as a
                classic script (not an ES module), so `import` / `export` syntax is
                rejected. Wrap CommonJS bundles or other globals in an IIFE before
                calling this method so only plain script statements reach V8.
        """

    def build(self) -> bytes:
        """
        Finalize the snapshot and return its serialized bytes.
        """
