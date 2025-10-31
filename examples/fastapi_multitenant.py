"""Minimal FastAPI app exposing multi-tenant JavaScript execution."""

from __future__ import annotations

from typing import AsyncIterator, Dict

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from jsrun import Runtime


class EvalRequest(BaseModel):
    code: str


_tenants: Dict[str, Runtime] = {}


def get_runtime(tenant_id: str) -> Runtime:
    runtime = _tenants.get(tenant_id)
    if runtime is None or runtime.is_closed():
        runtime = Runtime()
        _tenants[tenant_id] = runtime
    return runtime


@asynccontextmanager
def lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        while _tenants:
            _, runtime = _tenants.popitem()
            runtime.close()


app = FastAPI(title="jsrun multi-tenant demo", lifespan=lifespan)


@app.post("/tenants/{tenant_id}/eval")
async def eval_js(tenant_id: str, request: EvalRequest):
    runtime = get_runtime(tenant_id)
    try:
        result = await runtime.eval_async(request.code)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"tenant": tenant_id, "result": result}


@app.post("/tenants/{tenant_id}/fib")
async def compute_fib(tenant_id: str, n: int):
    runtime = get_runtime(tenant_id)
    script = f"""
        (function fib(n) {{
            return n < 2 ? n : fib(n - 1) + fib(n - 2);
        }})({n})
    """
    result = runtime.eval(script)
    return {"tenant": tenant_id, "result": result}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
