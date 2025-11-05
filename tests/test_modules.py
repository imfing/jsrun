"""
Tests for JavaScript module loading and evaluation.
"""

import pytest
from jsrun import Runtime, RuntimeConfig


class TestModuleStaticLoading:
    """Tests for static module loading via add_static_module."""

    def test_add_static_module_basic(self):
        """Test adding and importing a static module."""
        with Runtime() as rt:
            rt.add_static_module("math", "export const value = 42;")
            # eval_module returns the module namespace - we can't directly inspect it from Python
            # but we can check that the module loaded successfully by not raising an error
            result = rt.eval_module("math")
            # Module namespace is returned as a dict-like object
            assert result is not None
            assert "value" in result
            assert result["value"] == 42

    def test_add_static_module_with_import(self):
        """Test importing a static module from another module."""
        with Runtime() as rt:
            rt.add_static_module("utils", "export const value = 42;")
            rt.add_static_module(
                "main",
                """
                import { value } from 'utils';
                export const result = value * 2;
                """,
            )
            result = rt.eval_module("main")
            assert result is not None

    def test_add_multiple_static_modules(self):
        """Test adding multiple static modules."""
        with Runtime() as rt:
            rt.add_static_module("lib1", "export const x = 1;")
            rt.add_static_module("lib2", "export const y = 2;")
            rt.add_static_module(
                "main",
                """
                import { x } from 'lib1';
                import { y } from 'lib2';
                export const sum = x + y;
                """,
            )
            result = rt.eval_module("main")
            assert result is not None

    def test_module_without_add_static_module_fails(self):
        """Test that importing a non-existent module fails with helpful error."""
        with Runtime() as rt:
            with pytest.raises(Exception) as exc_info:
                rt.eval_module("nonexistent")
            assert (
                "denied" in str(exc_info.value).lower()
                or "failed" in str(exc_info.value).lower()
            )


class TestModuleResolver:
    """Tests for custom module resolver."""

    def test_set_module_resolver_basic(self):
        """Test setting a custom module resolver."""
        with Runtime() as rt:
            rt.add_static_module("foo", "export const value = 'foo';")

            def resolver(specifier, referrer):
                # Map "bar" to "foo" - return a full URL
                if specifier == "bar":
                    return "jsrun://static/foo"
                return ""

            rt.set_module_resolver(resolver)
            rt.add_static_module(
                "main", "import { value } from 'bar'; export const result = value;"
            )
            result = rt.eval_module("main")
            assert result is not None
            assert "result" in result
            assert result["result"] == "foo"

    def test_resolver_returning_empty_string_fallback(self):
        """Test that resolver can return empty string to use static resolution."""
        with Runtime() as rt:
            rt.add_static_module("lib", "export const x = 10;")

            def resolver(specifier, referrer):
                # Return empty string to use static resolution
                return ""

            rt.set_module_resolver(resolver)
            result = rt.eval_module("lib")
            assert result is not None


class TestModuleLoader:
    """Tests for custom module loader."""

    def test_set_module_loader_sync(self):
        """Test setting a synchronous module loader."""
        with Runtime() as rt:

            def resolver(specifier, referrer):
                if specifier.startswith("custom:"):
                    return specifier
                return ""

            def loader(specifier):
                if specifier == "custom:hello":
                    return "export const message = 'Hello from custom loader';"
                return ""

            rt.set_module_resolver(resolver)
            rt.set_module_loader(loader)

            rt.add_static_module(
                "main",
                "import { message } from 'custom:hello'; export const result = message;",
            )
            result = rt.eval_module("main")
            assert result is not None

    @pytest.mark.asyncio
    async def test_set_module_loader_async(self):
        """Test setting an async module loader."""
        with Runtime() as rt:

            def resolver(specifier, referrer):
                if specifier.startswith("async:"):
                    return specifier
                return ""

            async def loader(specifier):
                # Simulate async loading
                if specifier == "async:data":
                    return "export const data = [1, 2, 3];"
                return ""

            rt.set_module_resolver(resolver)
            rt.set_module_loader(loader)

            rt.add_static_module(
                "main",
                "import { data } from 'async:data'; export const sum = data.reduce((a,b) => a+b, 0);",
            )
            result = await rt.eval_module_async("main")
            assert result is not None


class TestModuleEvaluation:
    """Tests for eval_module and eval_module_async."""

    def test_eval_module_sync(self):
        """Test synchronous module evaluation."""
        with Runtime() as rt:
            rt.add_static_module("test", "export const value = 123;")
            result = rt.eval_module("test")
            # Module namespace should be returned
            assert result is not None

    def test_eval_module_sync_timeout(self):
        """Sync module evaluation should respect RuntimeConfig timeout."""
        config = RuntimeConfig(timeout=0.05)
        with Runtime(config) as rt:
            rt.add_static_module(
                "loop",
                """
                while (true) {}
                export const value = 1;
                """,
            )
            with pytest.raises(RuntimeError) as exc_info:
                rt.eval_module("loop")
            assert "timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_eval_module_async(self):
        """Test asynchronous module evaluation."""
        with Runtime() as rt:
            rt.add_static_module(
                "async_test",
                """
                const promise = Promise.resolve(456);
                export const value = await promise;
                """,
            )
            result = await rt.eval_module_async("async_test")
            assert result is not None

    @pytest.mark.asyncio
    async def test_eval_module_async_with_timeout(self):
        """Test async module evaluation with timeout."""
        with Runtime() as rt:
            rt.add_static_module(
                "slow",
                """
                const promise = Promise.resolve(123);
                export const value = await promise;
                """,
            )
            # Should complete within timeout
            result = await rt.eval_module_async("slow", timeout_ms=5000)
            assert result is not None
            assert result["value"] == 123


class TestModuleErrors:
    """Tests for error handling in module operations."""

    def test_module_resolution_error(self):
        """Test that module resolution errors are properly reported."""
        with Runtime() as rt:
            with pytest.raises(Exception) as exc_info:
                rt.eval_module("nonexistent")
            error_msg = str(exc_info.value).lower()
            assert "denied" in error_msg or "failed" in error_msg

    def test_module_loader_error(self):
        """Test that module loader errors are properly reported."""
        with Runtime() as rt:

            def resolver(specifier, referrer):
                return "custom:" + specifier

            def loader(specifier):
                raise ValueError("Loader error for testing")

            rt.set_module_resolver(resolver)
            rt.set_module_loader(loader)

            with pytest.raises(Exception) as exc_info:
                rt.eval_module("test")
            assert (
                "loader" in str(exc_info.value).lower()
                or "failed" in str(exc_info.value).lower()
            )

    def test_module_syntax_error(self):
        """Test that module syntax errors are reported."""
        with Runtime() as rt:
            rt.add_static_module("bad", "export const x = ;")  # Syntax error
            with pytest.raises(Exception):
                rt.eval_module("bad")

    def test_async_loader_with_sync_eval_fails(self):
        """Test that using an async loader with eval_module (sync) fails."""
        with Runtime() as rt:

            def resolver(spec, ref):
                return "async:my_mod"

            async def my_loader(spec):
                return "export const value = 123;"

            rt.set_module_resolver(resolver)
            rt.set_module_loader(my_loader)  # Set an ASYNC loader

            with pytest.raises(Exception) as exc_info:
                # Use sync eval_module
                rt.eval_module("test")

            # Check for the specific error message
            error_msg = str(exc_info.value).lower()
            assert "async module loader" in error_msg or "async" in error_msg
            assert (
                "synchronous evaluation" in error_msg
                or "eval_module_async" in error_msg
            )
