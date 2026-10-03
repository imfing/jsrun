"""Tests for the module-level API jsrun (module-level convenience functions)."""

import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor

import jsrun
import pytest


class TestModuleLevelEval:
    """Test module-level eval functions."""

    def test_eval_basic(self):
        """Test basic synchronous evaluation."""
        result = jsrun.eval("2 + 2")
        assert result == 4

    def test_eval_math(self):
        """Test math operations."""
        result = jsrun.eval("Math.sqrt(16)")
        assert result == 4.0

    def test_eval_string(self):
        """Test string operations."""
        result = jsrun.eval("'hello'.toUpperCase()")
        assert result == "HELLO"

    def test_eval_object(self):
        """Test object creation and access."""
        result = jsrun.eval("({a: 1, b: 2})")
        assert result == {"a": 1, "b": 2}

    def test_eval_array(self):
        """Test array operations."""
        result = jsrun.eval("[1, 2, 3].map(x => x * 2)")
        assert result == [2, 4, 6]


class TestModuleLevelEvalAsync:
    """Test module-level async eval functions."""

    @pytest.mark.asyncio
    async def test_eval_async_basic(self):
        """Test basic async evaluation."""
        result = await jsrun.eval_async("Promise.resolve(42)")
        assert result == 42

    @pytest.mark.asyncio
    async def test_eval_async_computation(self):
        """Test async computation."""
        result = await jsrun.eval_async("Promise.resolve(2 + 2)")
        assert result == 4

    @pytest.mark.asyncio
    async def test_eval_async_timeout(self):
        """Test async evaluation with timeout parameter."""
        result = await jsrun.eval_async("Promise.resolve(100)", timeout=1000)
        assert result == 100


class TestModuleLevelBindFunction:
    """Test module-level bind_function."""

    def test_bind_function_sync(self):
        """Test binding synchronous Python function."""
        jsrun.bind_function("add", lambda a, b: a + b)
        result = jsrun.eval("add(2, 3)")
        assert result == 5

    def test_bind_function_multiple_args(self):
        """Test binding function with multiple arguments."""
        jsrun.bind_function("multiply", lambda x, y: x * y)
        result = jsrun.eval("multiply(4, 5)")
        assert result == 20

    def test_bind_function_complex_return(self):
        """Test binding function with complex return type."""
        jsrun.bind_function("get_data", lambda: {"status": "ok", "value": 42})
        result = jsrun.eval("get_data()")
        assert result == {"status": "ok", "value": 42}

    @pytest.mark.asyncio
    async def test_bind_function_async(self):
        """Test binding asynchronous Python function."""

        async def async_fetch(url):
            await asyncio.sleep(0.01)
            return {"url": url, "status": 200}

        jsrun.bind_function("fetch", async_fetch)
        result = await jsrun.eval_async("fetch('https://example.com')")
        assert result == {"url": "https://example.com", "status": 200}

    @pytest.mark.asyncio
    async def test_bind_function_from_js_async(self):
        """Test calling bound async function from JavaScript."""

        async def process(value):
            await asyncio.sleep(0.01)
            return value * 2

        jsrun.bind_function("process", process)
        result = await jsrun.eval_async("process(21)")
        assert result == 42


class TestModuleLevelBindObject:
    """Test module-level bind_object."""

    def test_bind_object_basic(self):
        """Test binding a simple dict."""
        jsrun.bind_object("config", {"version": "1.0", "debug": True})
        assert jsrun.eval("config.version") == "1.0"
        assert jsrun.eval("config.debug") is True

    def test_bind_object_nested(self):
        """Test binding nested dict."""
        jsrun.bind_object(
            "settings", {"api": {"url": "https://api.example.com", "timeout": 30}}
        )
        assert jsrun.eval("settings.api.url") == "https://api.example.com"
        assert jsrun.eval("settings.api.timeout") == 30

    def test_bind_object_array_value(self):
        """Test binding dict with array values."""
        jsrun.bind_object("data", {"items": [1, 2, 3], "count": 3})
        assert jsrun.eval("data.items") == [1, 2, 3]
        assert jsrun.eval("data.count") == 3


class TestContextIsolation:
    """Test that different contexts get isolated runtimes."""

    @pytest.mark.asyncio
    async def test_async_tasks_get_separate_runtimes(self):
        """Each asyncio Task should get its own default runtime instance."""

        async def worker(tag: str):
            await jsrun.eval_async(f"globalThis.tag = '{tag}'")
            return await jsrun.eval_async("globalThis.tag")

        results = await asyncio.gather(worker("task-a"), worker("task-b"))
        assert results == ["task-a", "task-b"]

    @pytest.mark.asyncio
    async def test_async_task_isolation(self):
        """Test that different contexts get separate runtimes."""
        # Note: asyncio tasks in the same event loop share ContextVar context
        # To get true isolation, we need to run in separate contexts
        results = []

        def task1():
            ctx = contextvars.copy_context()

            def run():
                jsrun.eval("globalThis.taskId = 'task1'")
                result = jsrun.eval("globalThis.taskId")
                results.append(("task1", result))

            ctx.run(run)

        def task2():
            ctx = contextvars.copy_context()

            def run():
                jsrun.eval("globalThis.taskId = 'task2'")
                result = jsrun.eval("globalThis.taskId")
                results.append(("task2", result))

            ctx.run(run)

        # Run in separate contexts
        task1()
        task2()

        # Each context should see its own value
        assert ("task1", "task1") in results
        assert ("task2", "task2") in results

    def test_thread_isolation(self):
        """Test that different threads get separate runtimes."""
        results = []

        def thread_func(thread_id):
            jsrun.eval(f"globalThis.threadId = '{thread_id}'")
            result = jsrun.eval("globalThis.threadId")
            results.append((thread_id, result))
            jsrun.close_default_runtime()

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(thread_func, f"thread{i}") for i in range(3)]
            for future in futures:
                future.result()

        # Each thread should see its own value
        assert len(results) == 3
        for thread_id, result in results:
            assert thread_id == result

    def test_context_var_isolation(self):
        """Test that contextvars properly isolate runtimes."""

        def isolated_context(value):
            ctx = contextvars.copy_context()
            result = ctx.run(
                lambda: (
                    jsrun.eval(f"globalThis.value = {value}"),
                    jsrun.eval("globalThis.value"),
                )
            )
            return result[1]

        result1 = isolated_context(100)
        result2 = isolated_context(200)

        assert result1 == 100
        assert result2 == 200


class TestRuntimeRecreation:
    """Test that closed runtimes are automatically recreated."""

    def test_close_default_runtime_helper(self):
        """close_default_runtime should close the active runtime."""
        rt = jsrun.get_default_runtime()
        jsrun.close_default_runtime()
        assert rt.is_closed()

    def test_runtime_recreated_after_close(self):
        """Test that get_default_runtime recreates closed runtime."""
        # Use the default runtime
        result1 = jsrun.eval("2 + 2")
        assert result1 == 4

        # Get and close the runtime
        rt = jsrun.get_default_runtime()
        rt.close()
        assert rt.is_closed()

        # Next call should create a new runtime
        result2 = jsrun.eval("3 + 3")
        assert result2 == 6

        # Should be a new runtime instance
        new_rt = jsrun.get_default_runtime()
        assert not new_rt.is_closed()

    def test_state_cleared_after_recreation(self):
        """Test that state doesn't persist after runtime recreation."""
        jsrun.eval("globalThis.test = 'original'")
        assert jsrun.eval("globalThis.test") == "original"

        # Close and recreate
        rt = jsrun.get_default_runtime()
        rt.close()

        # New runtime should not have the old state
        result = jsrun.eval("globalThis.test")
        assert result is jsrun.undefined


class TestMixedUsage:
    """Test mixing module-level API with explicit Runtime usage."""

    def test_module_and_explicit_runtime_separate(self):
        """Test that module-level and explicit Runtime are independent."""
        # Set value in module-level runtime
        jsrun.eval("globalThis.source = 'module'")

        # Create explicit runtime
        with jsrun.Runtime() as rt:
            rt.eval("globalThis.source = 'explicit'")
            assert rt.eval("globalThis.source") == "explicit"

        # Module-level should be unchanged
        assert jsrun.eval("globalThis.source") == "module"

    @pytest.mark.asyncio
    async def test_module_and_explicit_runtime_async(self):
        """Test async usage with both APIs."""
        # Module-level
        result1 = await jsrun.eval_async("Promise.resolve(1)")

        # Explicit runtime
        with jsrun.Runtime() as rt:
            result2 = await rt.eval_async("Promise.resolve(2)")

        assert result1 == 1
        assert result2 == 2


class TestErrorHandling:
    """Test error handling in module-level API."""

    def test_eval_syntax_error(self):
        """Test that syntax errors are properly raised."""
        with pytest.raises(jsrun.JavaScriptError):
            jsrun.eval("invalid syntax!")

    def test_eval_runtime_error(self):
        """Test that runtime errors are properly raised."""
        with pytest.raises(jsrun.JavaScriptError):
            jsrun.eval("throw new Error('test error')")

    @pytest.mark.asyncio
    async def test_eval_async_error(self):
        """Test that async errors are properly raised."""
        with pytest.raises(jsrun.JavaScriptError):
            await jsrun.eval_async("Promise.reject(new Error('async error'))")


class TestGetDefaultRuntime:
    """Test the get_default_runtime helper."""

    def test_get_default_runtime_returns_same_instance(self):
        """Test that get_default_runtime returns the same instance in same context."""
        rt1 = jsrun.get_default_runtime()
        rt2 = jsrun.get_default_runtime()
        # Should be the same runtime instance
        assert rt1 is rt2

    def test_get_default_runtime_creates_when_needed(self):
        """Test that get_default_runtime creates runtime when needed."""
        rt = jsrun.get_default_runtime()
        assert isinstance(rt, jsrun.Runtime)
        assert not rt.is_closed()

    @pytest.mark.asyncio
    async def test_task_runtime_closed_when_task_completes(self):
        """A runtime created inside a spawned task should close automatically."""

        async def worker():
            await jsrun.eval_async("globalThis.workerActive = true")
            return jsrun.get_default_runtime()

        task = asyncio.create_task(worker())
        rt = await task
        await asyncio.sleep(0)
        assert rt.is_closed()
