"""
High-level integration coverage for the PyO3-backed Runtime.

The suite exercises the public Python API end to end, ensuring we can spawn
isolates, run sync/async JS, preserve state, enforce shutdown semantics, and
surface promise/timeouts/errors consistently with the Rust core.
"""

import math
import time
import pytest
from jsrun import Runtime


class TestRuntimeBasics:
    """Basic Runtime creation and lifecycle tests."""

    def test_runtime_spawn(self):
        """Test that Runtime() creates a new runtime."""
        runtime = Runtime()
        assert runtime is not None
        assert not runtime.is_closed()
        runtime.close()

    def test_runtime_eval_simple(self):
        """Test basic JavaScript evaluation."""
        runtime = Runtime()
        try:
            result = runtime.eval("2 + 2")
            assert result == 4
        finally:
            runtime.close()

    def test_runtime_eval_string(self):
        """Test string evaluation."""
        runtime = Runtime()
        try:
            result = runtime.eval("'hello world'")
            assert result == "hello world"
        finally:
            runtime.close()

    def test_runtime_eval_expression(self):
        """Test more complex expressions."""
        runtime = Runtime()
        try:
            result = runtime.eval("Math.max(10, 20, 30)")
            assert result == 30
        finally:
            runtime.close()

    def test_runtime_close(self):
        """Test runtime shutdown."""
        runtime = Runtime()
        assert not runtime.is_closed()

        runtime.close()
        assert runtime.is_closed()

    def test_runtime_close_idempotent(self):
        """Test that close() can be called multiple times safely."""
        runtime = Runtime()
        runtime.close()
        runtime.close()  # Should not raise
        assert runtime.is_closed()

    def test_runtime_eval_after_close(self):
        """Test that eval after close raises an error."""
        runtime = Runtime()
        runtime.close()

        with pytest.raises(RuntimeError) as exc_info:
            runtime.eval("1 + 1")
        assert "closed" in str(exc_info.value).lower()


class TestRuntimeStatePersistence:
    """Tests for state persistence across evaluations."""

    def test_variable_persistence(self):
        """Test that variables persist across eval calls."""
        runtime = Runtime()
        try:
            runtime.eval("var x = 10;")
            result = runtime.eval("x")
            assert result == 10

            runtime.eval("x = 20;")
            result = runtime.eval("x")
            assert result == 20
        finally:
            runtime.close()

    def test_function_persistence(self):
        """Test that functions persist across eval calls."""
        runtime = Runtime()
        try:
            runtime.eval("function add(a, b) { return a + b; }")
            result = runtime.eval("add(5, 7)")
            assert result == 12
        finally:
            runtime.close()

    def test_object_persistence(self):
        """Test that objects persist across eval calls."""
        runtime = Runtime()
        try:
            runtime.eval("var obj = {name: 'test', value: 42};")
            result = runtime.eval("obj.name")
            assert result == "test"

            result = runtime.eval("obj.value")
            assert result == 42

            runtime.eval("obj.value = 100;")
            result = runtime.eval("obj.value")
            assert result == 100
        finally:
            runtime.close()

    def test_multiple_sequential_evals(self):
        """Test multiple sequential evaluations."""
        runtime = Runtime()
        try:
            runtime.eval("var counter = 0;")

            for i in range(1, 6):
                runtime.eval("counter++;")
                result = runtime.eval("counter")
                assert result == i
        finally:
            runtime.close()


class TestRuntimeContextManager:
    """Tests for context manager protocol support."""

    def test_context_manager_basic(self):
        """Test basic context manager usage."""
        with Runtime() as runtime:
            result = runtime.eval("3 + 3")
            assert result == 6
        # Runtime should be closed after exiting context

    def test_context_manager_auto_close(self):
        """Test that context manager closes runtime automatically."""
        runtime = Runtime()
        assert not runtime.is_closed()

        with runtime:
            result = runtime.eval("5 * 5")
            assert result == 25
            assert not runtime.is_closed()

        # Should be closed after exiting
        assert runtime.is_closed()

    def test_context_manager_with_exception(self):
        """Test that runtime is closed even if exception occurs."""
        runtime = Runtime()

        try:
            with runtime:
                runtime.eval("var x = 1;")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Runtime should still be closed
        assert runtime.is_closed()


class TestRuntimeConcurrent:
    """Tests for multiple concurrent runtimes."""

    def test_multiple_runtimes_independent(self):
        """Test that multiple runtimes have independent state."""
        runtime1 = Runtime()
        runtime2 = Runtime()

        try:
            runtime1.eval("var x = 'runtime1';")
            runtime2.eval("var x = 'runtime2';")

            result1 = runtime1.eval("x")
            result2 = runtime2.eval("x")

            assert result1 == "runtime1"
            assert result2 == "runtime2"
        finally:
            runtime1.close()
            runtime2.close()

    def test_sequential_runtime_creation(self):
        """Test creating multiple runtimes sequentially."""
        for i in range(3):
            runtime = Runtime()
            try:
                result = runtime.eval(f"{i} * 2")
                assert result == i * 2
            finally:
                runtime.close()


class TestRuntimeBindings:
    """Tests for the decorator-style binding helper."""

    def test_bind_decorator_uses_function_name(self):
        runtime = Runtime()
        try:
            @runtime.bind
            def add(a, b):
                return a + b

            assert runtime.eval("add(2, 3)") == 5
        finally:
            runtime.close()

    def test_bind_decorator_custom_name(self):
        runtime = Runtime()
        try:
            @runtime.bind(name="pyAdd")
            def add(a, b):
                return a + b

            assert runtime.eval("pyAdd(4, 6)") == 10
        finally:
            runtime.close()

    def test_bind_direct_call_returns_callable(self):
        runtime = Runtime()
        try:
            def multiply(a, b):
                return a * b

            bound = runtime.bind(multiply)
            assert bound is multiply
            assert runtime.eval("multiply(3, 4)") == 12
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_bind_decorator_supports_async_callables(self):
        runtime = Runtime()
        try:
            calls = []

            @runtime.bind
            async def add_async(a, b):
                calls.append((a, b))
                return a + b

            result = await runtime.eval_async("add_async(5, 7)")
            assert result == 12
            assert calls == [(5, 7)]
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_bind_direct_call_supports_async_callables(self):
        runtime = Runtime()
        try:
            async def multiply_async(a, b):
                return a * b

            bound = runtime.bind(multiply_async, name="pyMultiplyAsync")
            assert bound is multiply_async

            result = await runtime.eval_async("pyMultiplyAsync(3, 9)")
            assert result == 27
        finally:
            runtime.close()


class TestRuntimeEdgeCases:
    """Edge case tests for runtime behavior."""

    def test_eval_undefined(self):
        """Test evaluation of undefined."""
        runtime = Runtime()
        try:
            result = runtime.eval("undefined")
            assert result is None
        finally:
            runtime.close()

    def test_eval_null(self):
        """Test evaluation of null."""
        runtime = Runtime()
        try:
            result = runtime.eval("null")
            assert result is None
        finally:
            runtime.close()

    def test_eval_boolean(self):
        """Test evaluation of booleans."""
        runtime = Runtime()
        try:
            assert runtime.eval("true") is True
            assert runtime.eval("false") is False
        finally:
            runtime.close()

    def test_eval_array(self):
        """Test evaluation of arrays."""
        runtime = Runtime()
        try:
            result = runtime.eval("[1, 2, 3]")
            # Should return native Python list
            assert result == [1, 2, 3]
        finally:
            runtime.close()

    def test_eval_object(self):
        """Test evaluation of objects."""
        runtime = Runtime()
        try:
            result = runtime.eval("({a: 1, b: 2})")
            # Should return native Python dict
            assert result == {"a": 1, "b": 2}
        finally:
            runtime.close()

    def test_eval_empty_string(self):
        """Test evaluation of empty statement."""
        runtime = Runtime()
        try:
            result = runtime.eval("")
            # Empty script returns undefined -> None
            assert result is None
        finally:
            runtime.close()


class TestRuntimeAsync:
    """Test async evaluation with promises."""

    @pytest.mark.asyncio
    async def test_eval_async_without_promise(self):
        """Test async eval with non-promise value."""
        with Runtime() as runtime:
            result = await runtime.eval_async("10 + 20")
            assert result == 30

    @pytest.mark.asyncio
    async def test_eval_async_with_promise_resolved(self):
        """Test async eval with resolved promise."""
        with Runtime() as runtime:
            result = await runtime.eval_async("Promise.resolve(42)")
            assert result == 42

    @pytest.mark.asyncio
    async def test_eval_async_with_promise_string(self):
        """Test async eval with promise resolving to string."""
        with Runtime() as runtime:
            result = await runtime.eval_async("Promise.resolve('async result')")
            assert result == "async result"

    @pytest.mark.asyncio
    async def test_eval_async_with_deferred_resolution(self):
        """Test async eval with deferred promise resolution."""
        with Runtime() as runtime:
            code = """
                new Promise((resolve) => {
                    // Immediately queued microtask via then
                    Promise.resolve().then(() => resolve('deferred result'));
                })
            """
            result = await runtime.eval_async(code)
            assert result == "deferred result"

    @pytest.mark.asyncio
    async def test_eval_async_promise_chain(self):
        """Test async eval with promise chain."""
        with Runtime() as runtime:
            code = """
                Promise.resolve(10)
                    .then(x => x * 2)
                    .then(x => x + 5)
            """
            result = await runtime.eval_async(code)
            assert result == 25

    @pytest.mark.asyncio
    async def test_eval_async_promise_rejection(self):
        """Test async eval with rejected promise."""
        with Runtime() as runtime:
            with pytest.raises(RuntimeError) as exc_info:
                await runtime.eval_async("Promise.reject(new Error('test error'))")
            message = str(exc_info.value)
            assert "Evaluation failed" in message
            assert "test error" in message

    @pytest.mark.asyncio
    async def test_eval_async_promise_rejection_value(self):
        """Test async eval with rejected promise (non-Error value)."""
        with Runtime() as runtime:
            with pytest.raises(RuntimeError) as exc_info:
                await runtime.eval_async("Promise.reject('custom error')")
            message = str(exc_info.value)
            assert "Evaluation failed" in message
            assert "custom error" in message


class TestRuntimeTimeout:
    """Test timeout functionality."""

    @pytest.mark.asyncio
    async def test_eval_async_timeout_success(self):
        """Test that fast promises complete before timeout."""
        with Runtime() as runtime:
            result = await runtime.eval_async(
                "Promise.resolve('quick')", timeout_ms=1000
            )
            assert result == "quick"

    @pytest.mark.asyncio
    async def test_eval_async_timeout_expiry(self):
        """Test that timeout is enforced for slow operations."""
        with Runtime() as runtime:
            # Promise that never resolves
            code = "new Promise(() => {})"
            with pytest.raises(RuntimeError) as exc_info:
                await runtime.eval_async(code, timeout_ms=100)
            message = str(exc_info.value)
            assert "Evaluation failed" in message
            assert "pending" in message

    @pytest.mark.asyncio
    async def test_eval_async_no_timeout(self):
        """Test async eval without timeout."""
        with Runtime() as runtime:
            # Should complete without timeout
            result = await runtime.eval_async("Promise.resolve(123)")
            assert result == 123

    @pytest.mark.asyncio
    async def test_eval_async_timeout_with_promise_chain(self):
        """Test timeout with promise chains that complete in time."""
        with Runtime() as runtime:
            code = """
                Promise.resolve()
                    .then(() => Promise.resolve())
                    .then(() => 'nested')
            """
            result = await runtime.eval_async(code, timeout_ms=1000)
            assert result == "nested"


class TestRuntimeAsyncConcurrency:
    """Test concurrent async runtime operations."""

    @pytest.mark.asyncio
    async def test_multiple_async_evals_sequential(self):
        """Test sequential async evaluations."""
        with Runtime() as runtime:
            result1 = await runtime.eval_async("Promise.resolve(1)")
            result2 = await runtime.eval_async("Promise.resolve(2)")
            result3 = await runtime.eval_async("Promise.resolve(3)")

            assert result1 == 1
            assert result2 == 2
            assert result3 == 3

    @pytest.mark.asyncio
    async def test_multiple_runtimes_async(self):
        """Test multiple runtimes with async operations."""
        runtime1 = Runtime()
        runtime2 = Runtime()

        try:
            # Run async evals concurrently on different runtimes
            result1 = await runtime1.eval_async("Promise.resolve('runtime1')")
            result2 = await runtime2.eval_async("Promise.resolve('runtime2')")

            assert result1 == "runtime1"
            assert result2 == "runtime2"
        finally:
            runtime1.close()
            runtime2.close()

    @pytest.mark.asyncio
    async def test_mixed_sync_async_eval(self):
        """Test mixing sync and async eval calls."""
        with Runtime() as runtime:
            # Sync eval
            sync_result = runtime.eval("10 + 10")
            assert sync_result == 20

            # Async eval
            async_result = await runtime.eval_async("Promise.resolve(30)")
            assert async_result == 30

            # Another sync eval to verify state
            final_result = runtime.eval("5 * 5")
            assert final_result == 25

    @pytest.mark.asyncio
    async def test_async_state_persistence(self):
        """Test that state persists across async evaluations."""
        with Runtime() as runtime:
            # Set up state
            runtime.eval("var asyncCounter = 0;")

            # Multiple async evals modifying state
            await runtime.eval_async("Promise.resolve(++asyncCounter)")
            result = await runtime.eval_async("Promise.resolve(asyncCounter)")
            assert result == 1

            await runtime.eval_async("Promise.resolve(++asyncCounter)")
            result = runtime.eval("asyncCounter")
            assert result == 2


class TestRuntimeAsyncErrors:
    """Test error handling in async evaluation."""

    @pytest.mark.asyncio
    async def test_eval_async_syntax_error(self):
        """Test that syntax errors are properly reported in async eval."""
        with Runtime() as runtime:
            with pytest.raises(RuntimeError, match="failed"):
                await runtime.eval_async("this is not valid javascript {{{")

    @pytest.mark.asyncio
    async def test_eval_async_runtime_error(self):
        """Test that runtime errors in promises are caught."""
        with Runtime() as runtime:
            code = """
                new Promise((resolve, reject) => {
                    reject(new Error('Runtime error in promise'));
                })
            """
            with pytest.raises(RuntimeError) as exc_info:
                await runtime.eval_async(code)
            message = str(exc_info.value)
            assert "Evaluation failed" in message
            assert "Runtime error in promise" in message

    @pytest.mark.asyncio
    async def test_eval_async_after_close(self):
        """Test that async eval after close raises an error."""
        runtime = Runtime()
        runtime.close()

        with pytest.raises(RuntimeError, match="closed"):
            await runtime.eval_async("Promise.resolve(1)")


class TestRuntimeConfig:
    """Tests for RuntimeConfig functionality."""

    def test_runtime_config_default(self):
        """Test default RuntimeConfig creation."""
        from jsrun import RuntimeConfig

        config = RuntimeConfig()
        assert config is not None
        # Should have default values
        assert "RuntimeConfig" in repr(config)

    def test_runtime_config_constructor_with_kwargs(self):
        """Test RuntimeConfig constructor with keyword arguments."""
        from jsrun import RuntimeConfig

        config = RuntimeConfig(
            max_heap_size=100 * 1024 * 1024,
            initial_heap_size=50 * 1024 * 1024,
            bootstrap='console.log("Bootstrap executed")',
            timeout=30.0,
        )

        assert config is not None
        assert "RuntimeConfig" in repr(config)
        assert config.max_heap_size == 100 * 1024 * 1024
        assert config.initial_heap_size == 50 * 1024 * 1024
        assert config.bootstrap == 'console.log("Bootstrap executed")'
        assert config.timeout == 30.0

    def test_runtime_config_property_methods(self):
        """Test RuntimeConfig with property setters."""
        from jsrun import RuntimeConfig

        config = RuntimeConfig()
        config.max_heap_size = 100 * 1024 * 1024
        config.initial_heap_size = 50 * 1024 * 1024
        config.bootstrap = 'console.log("Bootstrap executed")'
        config.timeout = 30.0

        assert config is not None
        assert "RuntimeConfig" in repr(config)
        assert config.max_heap_size == 100 * 1024 * 1024
        assert config.initial_heap_size == 50 * 1024 * 1024
        assert config.bootstrap == 'console.log("Bootstrap executed")'
        assert config.timeout == 30.0

    def test_runtime_config_with_bootstrap(self):
        """Test Runtime with bootstrap script."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig(bootstrap="globalThis.bootstrapped = true;")

        with Runtime(config) as runtime:
            result = runtime.eval("bootstrapped")
            assert result is True

    def test_runtime_config_without_bootstrap(self):
        """Test Runtime without bootstrap script (should not have bootstrapped variable)."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig()  # No bootstrap

        with Runtime(config) as runtime:
            # Should not have bootstrapped variable
            result = runtime.eval("typeof bootstrapped")
            assert result == "undefined"

    def test_runtime_config_initial_requires_max(self):
        """Providing initial heap size without max should raise on runtime creation."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig(initial_heap_size=1 * 1024 * 1024)
        with pytest.raises(RuntimeError) as exc_info:
            Runtime(config)
        message = str(exc_info.value).lower()
        assert "initial_heap_size" in message and "max_heap_size" in message

    def test_runtime_config_initial_exceeds_max(self):
        """initial_heap_size greater than max_heap_size should raise."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig(
            initial_heap_size=8 * 1024 * 1024, max_heap_size=4 * 1024 * 1024
        )
        with pytest.raises(RuntimeError) as exc_info:
            Runtime(config)
        message = str(exc_info.value).lower()
        assert "cannot exceed" in message

    def test_runtime_config_timeout_formats(self):
        """Test RuntimeConfig timeout with different formats."""
        from jsrun import RuntimeConfig

        # Test float timeout
        config1 = RuntimeConfig(timeout=30.5)
        assert config1 is not None
        assert config1.timeout == 30.5

        # Test integer timeout
        config2 = RuntimeConfig(timeout=60)
        assert config2 is not None
        assert config2.timeout == 60.0

        # Test timedelta timeout
        import datetime

        config3 = RuntimeConfig(timeout=datetime.timedelta(seconds=45))
        assert config3 is not None
        assert config3.timeout == 45.0

    def test_runtime_config_invalid_timeout(self):
        """Test RuntimeConfig with invalid timeout."""
        from jsrun import RuntimeConfig

        config = RuntimeConfig()

        with pytest.raises(ValueError, match="Timeout must be a number"):
            config.timeout = "invalid"

    @pytest.mark.asyncio
    async def test_runtime_config_execution_timeout_applies_to_eval_async(self):
        """RuntimeConfig.timeout should act as default timeout for eval_async."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig(timeout=0.1)
        with Runtime(config) as runtime:
            start = time.monotonic()
            with pytest.raises(RuntimeError) as exc_info:
                await runtime.eval_async("new Promise(() => {})")
            elapsed = time.monotonic() - start
            assert elapsed < 1.0
            message = str(exc_info.value).lower()
            assert "timed out" in message or "pending" in message

    def test_runtime_with_config(self):
        """Test Runtime creation with RuntimeConfig."""
        from jsrun import Runtime, RuntimeConfig

        config = RuntimeConfig(bootstrap="globalThis.configured = true;")

        with Runtime(config) as runtime:
            result = runtime.eval("configured")
            assert result is True

    def test_runtime_without_config(self):
        """Test Runtime creation without RuntimeConfig (default behavior)."""
        from jsrun import Runtime

        # Should work the same as before
        with Runtime() as runtime:
            result = runtime.eval("2 + 2")
            assert result == 4

    def test_runtime_config_property_chaining(self):
        """Test that RuntimeConfig property setters work correctly."""
        from jsrun import RuntimeConfig

        config = RuntimeConfig()
        config.max_heap_size = 100 * 1024 * 1024
        config.initial_heap_size = 50 * 1024 * 1024
        config.bootstrap = 'console.log("property chaining")'
        config.timeout = 30.0

        assert config is not None
        assert config.max_heap_size == 100 * 1024 * 1024
        assert config.initial_heap_size == 50 * 1024 * 1024
        assert config.bootstrap == 'console.log("property chaining")'
        assert config.timeout == 30.0

    def test_runtime_config_multiple_instances(self):
        """Test that multiple RuntimeConfig instances are independent."""
        from jsrun import Runtime, RuntimeConfig

        config1 = RuntimeConfig(bootstrap="globalThis.instance1 = true;")
        config2 = RuntimeConfig(bootstrap="globalThis.instance2 = true;")

        with Runtime(config1) as runtime1:
            result1 = runtime1.eval("instance1")
            assert result1 is True

            # instance2 should not be defined in runtime1
            result2 = runtime1.eval("typeof instance2")
            assert result2 == "undefined"

        with Runtime(config2) as runtime2:
            result1 = runtime2.eval("instance2")
            assert result1 is True

            # instance1 should not be defined in runtime2
            result2 = runtime2.eval("typeof instance1")
            assert result2 == "undefined"


class TestRuntimeNativeTypes:
    """Tests for native Python type return from eval."""

    @pytest.mark.parametrize(
        "source, expected",
        [
            ("1 + 1", 2),
            ("'hello world'", "hello world"),
            ("true", True),
            ("false", False),
            ("null", None),
            ("undefined", None),
            ("[1, 2, 3]", [1, 2, 3]),
            ("({foo: 'bar'})", {"foo": "bar"}),
            ("({nested: {value: 42}})", {"nested": {"value": 42}}),
            ("3.14", 3.14),
            ("-5", -5),
        ],
    )
    def test_eval_returns_native(self, source, expected):
        """Test that eval returns native Python types."""
        with Runtime() as runtime:
            result = runtime.eval(source)
            assert result == expected
            assert type(result) == type(expected)

    @pytest.mark.parametrize(
        "source, expected",
        [
            ("Promise.resolve(42)", 42),
            ("Promise.resolve('async result')", "async result"),
            ("Promise.resolve([1, 2, 3])", [1, 2, 3]),
            ("Promise.resolve({key: 'value'})", {"key": "value"}),
        ],
    )
    @pytest.mark.asyncio
    async def test_eval_async_returns_native(self, source, expected):
        """Test that eval_async returns native Python types."""
        with Runtime() as runtime:
            result = await runtime.eval_async(source)
            assert result == expected
            assert type(result) == type(expected)

    def test_eval_function_raises_error(self):
        """Test that functions raise appropriate errors."""
        with Runtime() as runtime:
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval("function() { return 42; }")
            assert "function" in str(exc_info.value).lower()

    def test_eval_circular_reference_raises_error(self):
        """Test that circular references raise appropriate errors."""
        with Runtime() as runtime:
            # Create circular object without returning it (by adding void 0 at the end)
            runtime.eval("var obj = {}; obj.self = obj; void 0")
            # Now try to eval the circular object - should raise error
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval("obj")
            assert (
                "circular" in str(exc_info.value).lower()
                or "depth" in str(exc_info.value).lower()
            )

    def test_eval_bigint_overflow_raises_error(self):
        """Test that BigInt overflow raises appropriate errors."""
        with Runtime() as runtime:
            # Create a BigInt larger than i64::MAX
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval(f"{2**63}n")
            assert (
                "overflow" in str(exc_info.value).lower()
                or "bigint" in str(exc_info.value).lower()
            )

    def test_eval_nan_infinity_survives(self):
        """Test that NaN and Infinity survive round-trip."""
        with Runtime() as runtime:
            nan_result = runtime.eval("NaN")
            assert math.isnan(nan_result)

            pos_inf_result = runtime.eval("Infinity")
            assert pos_inf_result == float("inf")

            neg_inf_result = runtime.eval("-Infinity")
            assert neg_inf_result == float("-inf")


class TestRuntimeOpsNativeTypes:
    """Test that ops can handle native JavaScript types including NaN/Infinity"""

    def test_sync_op_echo_nan(self):
        """Test that sync ops can receive and return NaN"""

        def echo(value):
            return value

        with Runtime() as runtime:
            runtime.register_op("echo", echo, mode="sync")
            result = runtime.eval("__host_op_sync__(0, NaN)")
            assert math.isnan(result), f"Expected NaN but got {result}"

    def test_sync_op_echo_infinity(self):
        """Test that sync ops can receive and return Infinity"""

        def echo(value):
            return value

        with Runtime() as runtime:
            runtime.register_op("echo", echo, mode="sync")
            result = runtime.eval("__host_op_sync__(0, Infinity)")
            assert result == float("inf"), f"Expected Infinity but got {result}"

    def test_sync_op_returns_nan_from_python(self):
        """Test that sync ops can return NaN from Python"""

        def return_nan():
            return float("nan")

        with Runtime() as runtime:
            runtime.register_op("returnNaN", return_nan, mode="sync")
            result = runtime.eval("__host_op_sync__(0)")
            assert math.isnan(result), f"Expected NaN but got {result}"

    def test_sync_op_python_circular_list_raises_error(self):
        """Python handlers returning circular lists should raise conversion errors"""

        def make_circular_list():
            data = []
            data.append(data)
            return data

        with Runtime() as runtime:
            runtime.register_op("makeCircularList", make_circular_list, mode="sync")
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval("__host_op_sync__(0)")
            message = str(exc_info.value).lower()
            assert "circular" in message and "list" in message

    def test_sync_op_python_circular_dict_raises_error(self):
        """Python handlers returning circular dicts should raise conversion errors"""

        def make_circular_dict():
            data = {}
            data["self"] = data
            return data

        with Runtime() as runtime:
            runtime.register_op("makeCircularDict", make_circular_dict, mode="sync")
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval("__host_op_sync__(0)")
            message = str(exc_info.value).lower()
            assert "circular" in message and "dict" in message

    def test_sync_op_python_large_payload_hits_size_limit(self):
        """Very large Python payloads should trigger size limit errors."""

        def make_large_list():
            # Approximately 12MB of data (6k strings x 2KB)
            return ["x" * 2048 for _ in range(6000)]

        with Runtime() as runtime:
            runtime.register_op("makeLargeList", make_large_list, mode="sync")
            with pytest.raises(RuntimeError) as exc_info:
                runtime.eval("__host_op_sync__(0)")
            message = str(exc_info.value).lower()
            assert "size" in message or "limit" in message
