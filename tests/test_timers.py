"""
Tests for the web-style timer API (setTimeout/setInterval and friends).

Timers are backed by deno_core's native timer machinery, exposed by the
JavaScript bridge in `src/runtime/ops.rs`.
"""

import time

from jsrun import Runtime


class TestTimerGlobals:
    def test_timer_functions_are_defined(self):
        with Runtime() as rt:
            kinds = rt.eval(
                "[typeof setTimeout, typeof clearTimeout,"
                " typeof setInterval, typeof clearInterval]"
            )
            assert kinds == ["function"] * 4

    def test_deno_global_still_hidden(self):
        with Runtime() as rt:
            assert rt.eval("typeof Deno") == "undefined"

    def test_set_timeout_returns_numeric_id(self):
        with Runtime() as rt:
            result = rt.eval(
                """
                const id = setTimeout(() => {}, 1000);
                clearTimeout(id);
                typeof id
                """
            )
            assert result == "number"


class TestSetTimeout:
    async def test_set_timeout_resolves_promise(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                "new Promise((resolve) => setTimeout(() => resolve('done'), 10))",
                timeout=5.0,
            )
            assert result == "done"

    async def test_set_timeout_passes_extra_args(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                """
                new Promise((resolve) => {
                    setTimeout((a, b) => resolve(a + b), 10, 40, 2);
                })
                """,
                timeout=5.0,
            )
            assert result == 42

    async def test_set_timeout_zero_delay(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                "new Promise((resolve) => setTimeout(resolve, 0)).then(() => 'ok')",
                timeout=5.0,
            )
            assert result == "ok"

    async def test_clear_timeout_cancels_callback(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                """
                new Promise((resolve) => {
                    const cancelled = setTimeout(() => resolve('cancelled'), 10);
                    clearTimeout(cancelled);
                    setTimeout(() => resolve('kept'), 50);
                })
                """,
                timeout=5.0,
            )
            assert result == "kept"

    def test_timer_fires_between_evals(self):
        with Runtime() as rt:
            rt.eval(
                "globalThis.fired = false; setTimeout(() => { globalThis.fired = true; }, 10);"
            )
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if rt.eval("globalThis.fired"):
                    break
                time.sleep(0.05)
            assert rt.eval("globalThis.fired") is True

    def test_timer_callback_must_be_function(self):
        with Runtime() as rt:
            result = rt.eval(
                """
                let error = null;
                try {
                    setTimeout("globalThis.evil = true", 0);
                } catch (err) {
                    error = err.constructor.name;
                }
                error
                """
            )
            assert result == "TypeError"


class TestSetInterval:
    async def test_set_interval_repeats_and_clears(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                """
                new Promise((resolve) => {
                    let count = 0;
                    const id = setInterval(() => {
                        count += 1;
                        if (count >= 3) {
                            clearInterval(id);
                            resolve(count);
                        }
                    }, 5);
                })
                """,
                timeout=5.0,
            )
            assert result == 3

    async def test_nested_timers(self):
        with Runtime() as rt:
            result = await rt.eval_async(
                """
                new Promise((resolve) => {
                    setTimeout(() => {
                        setTimeout(() => resolve('nested'), 5);
                    }, 5);
                })
                """,
                timeout=5.0,
            )
            assert result == "nested"
