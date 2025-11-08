"""Tests for JavaScript function calling from Python."""

import gc
import inspect
import weakref

import pytest
from jsrun import JavaScriptError, Runtime


def test_basic_function_call_sync():
    """Sync-returning JS functions should return directly."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => x * 2")
        assert js_func(5) == 10


def test_function_with_multiple_args():
    """Multiple arguments are handled synchronously."""
    with Runtime() as rt:
        js_func = rt.eval("(a, b, c) => a + b + c")
        assert js_func(1, 2, 3) == 6


def test_function_returns_object():
    """Objects should round-trip without awaiting."""
    with Runtime() as rt:
        js_func = rt.eval("(name) => ({ greeting: 'Hello, ' + name })")
        assert js_func("World") == {"greeting": "Hello, World"}


def test_async_function_with_immediate_result():
    """Async functions that resolve immediately should return directly."""
    with Runtime() as rt:
        js_func = rt.eval("async (x) => { return x * 3; }")
        assert js_func(7) == 21


@pytest.mark.asyncio
async def test_function_pending_promise_requires_await():
    """Pending promises still produce awaitables until resolved."""
    with Runtime() as rt:
        rt.eval("globalThis.__pendingResolvers = []")
        js_func = rt.eval(
            "() => new Promise((resolve) => __pendingResolvers.push(resolve))"
        )

        call_result = js_func(7)
        assert inspect.isawaitable(call_result)

        rt.eval("__pendingResolvers.shift()(21)")
        assert await call_result == 21


def test_function_closure_sync_calls():
    """Closure functions should stay synchronous when possible."""
    with Runtime() as rt:
        js_func = rt.eval(
            """
            (() => {
                let counter = 0;
                return () => ++counter;
            })()
            """
        )
        assert js_func() == 1
        assert js_func() == 2
        assert js_func() == 3


@pytest.mark.asyncio
async def test_function_close():
    """Sync invocation should work before close and error after."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => x + 1")
        assert js_func(10) == 11

        await js_func.close()

        with pytest.raises(RuntimeError, match="Function has been closed"):
            js_func(10)


def test_function_error_handling():
    """Test function error propagation on sync path."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => { throw new Error('Test error: ' + x); }")

        with pytest.raises(JavaScriptError) as exc_info:
            js_func(42)
        js_error = exc_info.value
        assert js_error.name == "Error"
        assert "Test error: 42" in js_error.message
        assert js_error.stack is not None


def test_function_round_trip_sync_usage():
    """Passing JsFunction into JS should keep sync calling semantics."""
    with Runtime() as rt:
        apply_fn = rt.eval("(fn, arg) => fn(arg)")
        multiply = rt.eval("(x) => x * 3")
        assert apply_fn(multiply, 7) == 21


def test_function_this_binding_sync():
    """Object methods should respect their original receiver."""
    with Runtime() as rt:
        obj = rt.eval(
            """({
            value: 42,
            getValue() { return this.value; },
            getValueArrow: () => { return this.value; }
        })"""
        )
        get_value = obj["getValue"]
        assert get_value() == 42


@pytest.mark.asyncio
async def test_closed_function_transfer():
    """Passing a closed function back to JS should fail with clear error."""
    with Runtime() as rt:
        apply_fn = rt.eval("(fn) => fn(5)")
        multiply = rt.eval("(x) => x * 2")
        await multiply.close()

        with pytest.raises(RuntimeError, match="Function has been closed"):
            apply_fn(multiply)


def test_function_from_closed_runtime_transfer():
    """Passing a function from a closed runtime should fail."""
    rt = Runtime()
    apply_fn = rt.eval("(fn) => fn(5)")
    multiply = rt.eval("(x) => x * 2")

    rt.close()

    with pytest.raises(RuntimeError, match="Runtime has been shut down"):
        apply_fn(multiply)


@pytest.mark.asyncio
async def test_call_async_for_sync_function():
    """call_async should always yield an awaitable, even for sync JS."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => x * 2")
        result = js_func.call_async(5)
        assert inspect.isawaitable(result)
        assert await result == 10


@pytest.mark.asyncio
async def test_call_async_for_async_function():
    """call_async should mirror direct call behavior for async JS."""
    with Runtime() as rt:
        js_func = rt.eval("async (x) => x * 2")
        assert await js_func.call_async(5) == 10


def test_function_finalizer_releases_handles():
    """JsFunction proxies release their V8 handles when GC'd."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => x + 1")
        assert rt._debug_tracked_function_count() == 1

        func_ref = weakref.ref(js_func)
        js_func = None

        for _ in range(3):
            gc.collect()

        assert func_ref() is None
        assert rt._debug_tracked_function_count() == 0


def test_runtime_close_releases_outstanding_functions():
    """Runtime.close releases any remaining JsFunction handles."""
    rt = Runtime()
    fn_a = rt.eval("(x) => x")
    fn_b = rt.eval("(x) => x * 2")
    assert rt._debug_tracked_function_count() == 2

    rt.close()
    assert rt._debug_tracked_function_count() == 0

    ref_a = weakref.ref(fn_a)
    ref_b = weakref.ref(fn_b)
    fn_a = None
    fn_b = None
    for _ in range(3):
        gc.collect()

    assert ref_a() is None
    assert ref_b() is None
