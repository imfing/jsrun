"""High-level Python bindings for the jsrun runtime."""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, overload

from ._jsrun import Runtime, RuntimeConfig

F = TypeVar("F", bound=Callable[..., Any])


@overload
def _runtime_bind(self: Runtime, func: F, /, *, name: Optional[str] = ...) -> F:
    ...


@overload
def _runtime_bind(
    self: Runtime, func: None = ..., /, *, name: Optional[str] = ...
) -> Callable[[F], F]:
    ...


def _runtime_bind(
    self: Runtime, func: Optional[F] = None, /, *, name: Optional[str] = None
) -> Callable[[F], F] | F:
    """Bind a Python callable to the runtime via a decorator-friendly API.

    The helper accepts both synchronous and asynchronous callables, forwarding
    registration to :meth:`Runtime.bind_function` while returning the original
    callable for continued direct usage.
    """

    def _register(target: F) -> F:
        binding_name = name if name is not None else getattr(target, "__name__", None)
        if not binding_name:
            raise ValueError("A function name is required when binding to JavaScript")
        self.bind_function(binding_name, target)
        return target

    if func is None:
        return _register

    if not callable(func):
        raise TypeError("runtime.bind expects a callable or to be used as a decorator")

    return _register(func)


setattr(Runtime, "bind", _runtime_bind)

__all__ = [
    "Runtime",
    "RuntimeConfig",
]

