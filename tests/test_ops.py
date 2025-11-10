"""
Integration coverage for the JS <-> Python op bridge.

These tests focus on registering handlers, invoking them from JavaScript, and
demonstrating how Python-side guards can enforce policy with the revamped API.
"""

import asyncio
import json
from datetime import datetime, timezone

import pytest
from jsrun import JsUndefined, Runtime, undefined


class TestOpRegistration:
    """Test op registration functionality."""

    def test_register_sync_op(self):
        """Test registering a synchronous op."""
        runtime = Runtime()
        try:

            def add_numbers(args):
                return args[0] + args[1]

            op_id = runtime.register_op("add", add_numbers, mode="sync")
            assert isinstance(op_id, int)
            assert op_id >= 0
        finally:
            runtime.close()

    def test_register_async_op(self):
        """Test registering an asynchronous op."""
        runtime = Runtime()
        try:

            async def fetch_data(args):
                return {"data": "fetched"}

            op_id = runtime.register_op("fetch", fetch_data, mode="async")
            assert isinstance(op_id, int)
            assert op_id >= 0
        finally:
            runtime.close()

    def test_register_mode_mismatch_sync(self):
        runtime = Runtime()
        try:

            async def handler(args):
                return args

            with pytest.raises(RuntimeError):
                runtime.register_op("asyncWrong", handler, mode="sync")
        finally:
            runtime.close()

    def test_register_mode_mismatch_async(self):
        runtime = Runtime()
        try:

            def handler(args):
                return args

            with pytest.raises(RuntimeError):
                runtime.register_op("syncWrong", handler, mode="async")
        finally:
            runtime.close()

    def test_register_op_with_custom_guard(self):
        """Python handlers can enforce their own policy."""
        runtime = Runtime()
        try:

            def read_file(path):
                if not str(path).startswith("/tmp"):
                    raise RuntimeError("Permission denied")
                return "file content"

            op_id = runtime.register_op("readFile", read_file, mode="sync")
            assert isinstance(op_id, int)

            allowed = runtime.eval(f"__host_op_sync__({op_id}, '/tmp/allowed.txt')")
            assert allowed == "file content"

            denied = runtime.eval(
                f"""
                try {{
                    __host_op_sync__({op_id}, '/etc/passwd');
                    'ok';
                }} catch (err) {{
                    err && err.message ? err.message : String(err);
                }}
                """
            )
            assert "Permission denied" in denied
        finally:
            runtime.close()

    def test_register_multiple_ops(self):
        """Test registering multiple ops."""
        runtime = Runtime()
        try:

            def op1(args):
                return "op1"

            def op2(args):
                return "op2"

            def op3(args):
                return "op3"

            op_id1 = runtime.register_op("op1", op1)
            op_id2 = runtime.register_op("op2", op2)
            op_id3 = runtime.register_op("op3", op3)

            # Each op should get a unique ID
            assert op_id1 != op_id2
            assert op_id2 != op_id3
            assert op_id1 != op_id3
        finally:
            runtime.close()

    def test_register_op_invalid_mode(self):
        """Test that invalid mode raises error."""
        runtime = Runtime()
        try:

            def my_op(args):
                return "result"

            with pytest.raises(Exception) as exc_info:
                runtime.register_op("test", my_op, mode="invalid")
            assert "Invalid mode" in str(exc_info.value)
        finally:
            runtime.close()

    def test_register_op_after_close(self):
        """Test that registering op after close raises error."""
        runtime = Runtime()
        runtime.close()

        def my_op(args):
            return "result"

        with pytest.raises(Exception) as exc_info:
            runtime.register_op("test", my_op)
        assert "closed" in str(exc_info.value).lower()


class TestOpHandlers:
    """Test op handler functionality."""

    def test_handler_receives_arguments(self):
        """Test that handler receives arguments correctly."""
        runtime = Runtime()
        try:
            received_args = []

            def capture_args(args):
                received_args.extend(args)
                return "ok"

            op_id = runtime.register_op("capture", capture_args)

            # Note: Actually calling the op from JavaScript requires the full
            # op dispatch system which is not yet complete.
            # This test just verifies registration succeeds.
            assert op_id >= 0
        finally:
            runtime.close()

    def test_handler_returns_value(self):
        """Test that handler return values are processed."""
        runtime = Runtime()
        try:

            def return_value(args):
                return {"result": 42, "status": "success"}

            op_id = runtime.register_op("getValue", return_value)
            assert op_id >= 0
        finally:
            runtime.close()

    def test_handler_with_lambda(self):
        """Test registering a lambda as handler."""
        runtime = Runtime()
        try:
            op_id = runtime.register_op("double", lambda args: args[0] * 2)
            assert op_id >= 0
        finally:
            runtime.close()

    def test_handler_with_closure(self):
        """Test registering a closure as handler."""
        runtime = Runtime()
        try:
            counter = [0]

            def increment(args):
                counter[0] += 1
                return counter[0]

            op_id = runtime.register_op("increment", increment)
            assert op_id >= 0
        finally:
            runtime.close()

    def test_sync_op_invocation(self):
        """Ensure __host_op_sync__ invokes Python handler."""
        runtime = Runtime()
        try:

            def concat(a, b, c):
                return {"joined": "".join(str(x) for x in [a, b, c])}

            op_id = runtime.register_op("concat", concat, mode="sync")

            result = runtime.eval(
                f"JSON.stringify(__host_op_sync__({op_id}, 'a', 'b', 'c'))"
            )
            payload = json.loads(result)

            assert payload["joined"] == "abc"
        finally:
            runtime.close()

    def test_sync_call_async_op_errors(self):
        """Calling async-mode ops via sync entry point should raise."""
        runtime = Runtime()
        try:

            async def async_like(args):
                return "value"

            op_id = runtime.register_op("asyncOnly", async_like, mode="async")

            result = runtime.eval(
                f"""
                try {{
                    __host_op_sync__({op_id}, 'x');
                    'ok';
                }} catch (err) {{
                    err && err.message ? err.message : String(err);
                }}
                """
            )

            assert "async" in result
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_async_op_invocation(self):
        """Ensure __host_op_async__ resolves with handler result."""
        with Runtime() as runtime:

            async def collect(a, b, c):
                return {"count": 3, "data": [a, b, c]}

            op_id = runtime.register_op("collect", collect, mode="async")

            result = await runtime.eval_async(
                f"""
                (async () => {{
                    const value = await __host_op_async__({op_id}, 1, 2, 3);
                    return JSON.stringify(value);
                }})()
                """
            )

            payload = json.loads(result)
            assert payload["count"] == 3
            assert payload["data"] == [1, 2, 3]


class TestOpValueConversions:
    """Ensure host ops round-trip richer value types."""

    def test_sync_op_receives_uint8array(self):
        runtime = Runtime()
        try:
            captured = []

            def capture(value):
                captured.append(value)
                return None

            op_id = runtime.register_op("captureBytes", capture, mode="sync")
            runtime.eval(f"__host_op_sync__({op_id}, new Uint8Array([1, 2, 3]))")
            assert isinstance(captured[0], bytes)
            assert captured[0] == b"\x01\x02\x03"
        finally:
            runtime.close()

    def test_sync_op_returns_bytes(self):
        runtime = Runtime()
        try:

            def payload():
                return b"\xaa\xbb"

            op_id = runtime.register_op("bytesOut", payload, mode="sync")
            result = runtime.eval(f"__host_op_sync__({op_id})")
            assert isinstance(result, bytes)
            assert result == b"\xaa\xbb"
        finally:
            runtime.close()

    def test_sync_op_receives_date(self):
        runtime = Runtime()
        try:
            captured = []

            def capture(value):
                captured.append(value)
                return None

            op_id = runtime.register_op("captureDate", capture, mode="sync")
            runtime.eval(f"__host_op_sync__({op_id}, new Date(1704067200000))")
            expected = datetime.fromtimestamp(1704067200, tz=timezone.utc)
            assert isinstance(captured[0], datetime)
            assert captured[0] == expected
        finally:
            runtime.close()

    def test_sync_op_returns_datetime(self):
        runtime = Runtime()
        try:

            def emit():
                return datetime(2023, 12, 25, 0, 0, tzinfo=timezone.utc)

            op_id = runtime.register_op("dateOut", emit, mode="sync")
            result = runtime.eval(f"__host_op_sync__({op_id})")
            assert isinstance(result, datetime)
            assert result == datetime(2023, 12, 25, 0, 0, tzinfo=timezone.utc)
        finally:
            runtime.close()

    def test_sync_op_receives_set(self):
        runtime = Runtime()
        try:
            captured = []

            def capture(value):
                captured.append(value)
                return None

            op_id = runtime.register_op("captureSet", capture, mode="sync")
            runtime.eval(f"__host_op_sync__({op_id}, new Set([1, 2, 3]))")
            assert isinstance(captured[0], set)
            assert captured[0] == {1, 2, 3}
        finally:
            runtime.close()

    def test_sync_op_returns_set(self):
        runtime = Runtime()
        try:

            def emit():
                return {1, 2, 3}

            op_id = runtime.register_op("setOut", emit, mode="sync")
            result = runtime.eval(f"Array.from(__host_op_sync__({op_id}))")
            assert isinstance(result, list)
            assert set(result) == {1, 2, 3}
        finally:
            runtime.close()

    def test_sync_op_receives_bigint(self):
        runtime = Runtime()
        try:
            captured = []

            def capture(value):
                captured.append(value)
                return None

            op_id = runtime.register_op("captureBigInt", capture, mode="sync")
            runtime.eval(f"__host_op_sync__({op_id}, 2n ** 64n)")
            assert isinstance(captured[0], int)
            assert captured[0] == 2**64
        finally:
            runtime.close()

    def test_sync_op_returns_bigint(self):
        runtime = Runtime()
        try:

            def emit():
                return 2**200

            op_id = runtime.register_op("bigIntOut", emit, mode="sync")
            result = runtime.eval(f"typeof __host_op_sync__({op_id})")
            assert result == "bigint"
        finally:
            runtime.close()

    def test_sync_op_receives_undefined(self):
        runtime = Runtime()
        try:
            captured = []

            def capture(value):
                captured.append(value)
                return None

            op_id = runtime.register_op("captureUndefined", capture, mode="sync")
            runtime.eval(f"__host_op_sync__({op_id}, undefined)")
            assert isinstance(captured[0], JsUndefined)
            assert captured[0] is undefined
        finally:
            runtime.close()

    def test_sync_op_returns_undefined(self):
        runtime = Runtime()
        try:

            def emit():
                return undefined

            op_id = runtime.register_op("undefinedOut", emit, mode="sync")
            result = runtime.eval(f"__host_op_sync__({op_id}) === undefined")
            assert result is True
        finally:
            runtime.close()


class TestBindFunction:
    """High-level binding helpers."""

    def test_bind_sync_function(self):
        runtime = Runtime()
        try:

            def add(a, b):
                return a + b

            runtime.bind_function("addValues", add)
            result = runtime.eval("addValues(2, 3)")
            assert result == 5
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_bind_async_function(self):
        with Runtime() as runtime:

            async def multiply(a, b):
                await asyncio.sleep(0)
                return a * b

            runtime.bind_function("pyMultiply", multiply)
            result = await runtime.eval_async(
                """
                (async () => {
                    return await pyMultiply(3, 4);
                })()
                """
            )
            assert result == 12


class TestBindObject:
    """Binding Python objects as JavaScript globals."""

    def test_bind_object_with_values(self):
        runtime = Runtime()
        try:

            def add(a, b):
                return a + b

            runtime.bind_object("api", {"add": add, "value": 123})
            assert runtime.eval("api.value") == 123
            assert runtime.eval("api.add(2, 3)") == 5
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_bind_object_with_async_function(self):
        with Runtime() as runtime:

            async def multiply(a, b):
                await asyncio.sleep(0)
                return a * b

            runtime.bind_object("api", {"multiply": multiply, "constant": 7})

            result = await runtime.eval_async(
                """
                (async () => {
                    return [await api.multiply(3, 4), api.constant];
                })()
                """
            )

            assert result == [12, 7]

    def test_bind_object_with_escaped_keys(self):
        runtime = Runtime()
        try:
            evil_key = "value']; globalThis.__pwn = true; //"
            runtime.bind_object("safeApi", {evil_key: 42})

            value = runtime.eval(f"safeApi[{json.dumps(evil_key)}]")
            assert value == 42
            assert runtime.eval("globalThis.__pwn === undefined")
        finally:
            runtime.close()

    def test_bind_object_with_special_global_name(self):
        runtime = Runtime()
        try:
            global_name = "api.with$odd.chars"
            runtime.bind_object(global_name, {"value": 9})
            observed = runtime.eval(f"globalThis[{json.dumps(global_name)}].value")
            assert observed == 9
        finally:
            runtime.close()


class TestOpGuarding:
    """Demonstrate Python-level permission enforcement."""

    def test_sync_guard_blocks_disallowed_calls(self):
        runtime = Runtime()
        try:
            allowed_prefix = "/tmp"

            def read_file(path):
                if not path.startswith(allowed_prefix):
                    raise RuntimeError("Permission denied")
                return f"read:{path}"

            op_id = runtime.register_op("readFile", read_file)

            allowed = runtime.eval(f"__host_op_sync__({op_id}, '/tmp/data')")
            assert allowed == "read:/tmp/data"

            denied = runtime.eval(
                f"""
                try {{
                    __host_op_sync__({op_id}, '/etc/passwd');
                }} catch (err) {{
                    err && err.message ? err.message : String(err);
                }}
                """
            )
            assert "Permission denied" in denied
        finally:
            runtime.close()

    def test_guard_can_mutate_shared_state(self):
        runtime = Runtime()
        try:
            audit_log = []

            def record(*args):
                audit_log.append(tuple(args))
                return len(audit_log)

            op_id = runtime.register_op("record", record)
            count = runtime.eval("__host_op_sync__({0}, 'a', 'b')".format(op_id))
            assert count == 1
            count = runtime.eval("__host_op_sync__({0}, 'c')".format(op_id))
            assert count == 2
            assert audit_log == [("a", "b"), ("c",)]
        finally:
            runtime.close()

    @pytest.mark.asyncio
    async def test_async_guard_blocks_disallowed_calls(self):
        with Runtime() as runtime:
            allowed_hosts = {"https://example.com"}

            async def fetch(url):
                if url not in allowed_hosts:
                    raise RuntimeError("Permission denied")
                return {"url": url}

            op_id = runtime.register_op("fetch", fetch, mode="async")

            allowed = await runtime.eval_async(
                f"""
                (async () => {{
                    const value = await __host_op_async__({op_id}, 'https://example.com');
                    return JSON.stringify(value);
                }})()
                """
            )
            assert json.loads(allowed)["url"] == "https://example.com"

            denied = await runtime.eval_async(
                f"""
                (async () => {{
                    try {{
                        await __host_op_async__({op_id}, 'https://forbidden.test');
                        return 'ok';
                    }} catch (err) {{
                        return err && err.message ? err.message : String(err);
                    }}
                }})()
                """
            )
            assert "Permission denied" in denied


class TestContextManager:
    """Test runtime context manager with ops."""

    def test_ops_work_with_context_manager(self):
        """Test that ops work correctly with context manager."""
        with Runtime() as runtime:

            def my_op(args):
                return "ok"

            op_id = runtime.register_op("test", my_op)
            assert op_id >= 0


class TestOpBootstrap:
    """Test that op bootstrap JavaScript is injected."""

    def test_op_globals_exist(self):
        """Test that op system globals are available in JavaScript."""
        runtime = Runtime()
        try:
            result = runtime.eval("typeof __host_op_sync__")
            assert result == "function"

            result = runtime.eval("typeof __host_op_async__")
            assert result == "function"
        finally:
            runtime.close()

    # Async guard support will be reintroduced once host ops gain driver support.
