"""Tests for JavaScript function calling from Python."""

import gc
import weakref

import pytest
from jsrun import JavaScriptError, Runtime


@pytest.mark.asyncio
async def test_basic_function_call():
    """Test basic function return and calling."""
    with Runtime() as rt:
        # Get a JavaScript function
        js_func = rt.eval("(x) => x * 2")

        # Call it from Python
        result = await js_func(5)
        assert result == 10


@pytest.mark.asyncio
async def test_function_with_multiple_args():
    """Test function with multiple arguments."""
    with Runtime() as rt:
        js_func = rt.eval("(a, b, c) => a + b + c")
        result = await js_func(1, 2, 3)
        assert result == 6


@pytest.mark.asyncio
async def test_function_returns_object():
    """Test function that returns an object."""
    with Runtime() as rt:
        js_func = rt.eval("(name) => ({ greeting: 'Hello, ' + name })")
        result = await js_func("World")
        assert result == {"greeting": "Hello, World"}


@pytest.mark.asyncio
async def test_function_returns_promise():
    """Test function that returns a promise."""
    with Runtime() as rt:
        js_func = rt.eval("async (x) => { return x * 3; }")
        result = await js_func(7)
        assert result == 21


@pytest.mark.asyncio
async def test_function_closure():
    """Test function with closure."""
    with Runtime() as rt:
        js_func = rt.eval("""
            (() => {
                let counter = 0;
                return () => ++counter;
            })()
        """)
        assert await js_func() == 1
        assert await js_func() == 2
        assert await js_func() == 3


@pytest.mark.asyncio
async def test_function_close():
    """Test function lifecycle with explicit close."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => x + 1")

        # Function should work before close
        result = await js_func(10)
        assert result == 11

        # Close the function
        await js_func.close()

        # Calling after close should raise an error
        with pytest.raises(RuntimeError, match="Function has been closed"):
            await js_func(10)


@pytest.mark.asyncio
async def test_function_error_handling():
    """Test function error handling."""
    with Runtime() as rt:
        js_func = rt.eval("(x) => { throw new Error('Test error: ' + x); }")

        # Should propagate JavaScript errors
        with pytest.raises(JavaScriptError) as exc_info:
            await js_func(42)
        js_error = exc_info.value
        assert js_error.name == "Error"
        assert "Test error: 42" in js_error.message
        assert js_error.stack is not None


@pytest.mark.asyncio
async def test_function_round_trip():
    """Test passing JsFunction back to JavaScript."""
    with Runtime() as rt:
        # Create a JS function that takes another function and calls it
        apply_fn = rt.eval("(fn, arg) => fn(arg)")

        # Create a simple JS function
        multiply = rt.eval("(x) => x * 3")

        # Pass the function back to JS
        result = await apply_fn(multiply, 7)
        assert result == 21


@pytest.mark.asyncio
async def test_function_this_binding():
    """Test that object methods maintain their 'this' binding."""
    with Runtime() as rt:
        # Create an object with a method that uses 'this'
        obj = rt.eval("""({
            value: 42,
            getValue() { return this.value; },
            getValueArrow: () => { return this.value; }
        })""")

        # Extract the method
        get_value = obj["getValue"]

        # Call it - should return 42 with proper 'this' binding
        result = await get_value()
        assert result == 42


@pytest.mark.asyncio
async def test_closed_function_transfer():
    """Test that passing a closed function back to JS gives a clear error."""
    with Runtime() as rt:
        # Create a function that accepts another function
        apply_fn = rt.eval("(fn) => fn(5)")

        # Create a function and close it
        multiply = rt.eval("(x) => x * 2")
        await multiply.close()

        # Trying to pass the closed function should give a clear error
        with pytest.raises(RuntimeError, match="Function has been closed"):
            await apply_fn(multiply)


@pytest.mark.asyncio
async def test_function_from_closed_runtime_transfer():
    """Test that passing a function from a closed runtime gives a clear error."""
    # Create runtime and get a function
    rt = Runtime()
    apply_fn = rt.eval("(fn) => fn(5)")
    multiply = rt.eval("(x) => x * 2")

    # Close the runtime
    rt.close()

    # Trying to pass the function from closed runtime should give a clear error
    with pytest.raises(RuntimeError, match="Runtime has been shut down"):
        await apply_fn(multiply)


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
