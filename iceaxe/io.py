import asyncio
from functools import lru_cache, wraps
from typing import Any, Callable, Coroutine, TypeVar

T = TypeVar("T")


def lru_cache_async(
    maxsize: int | None = 100,
):
    def decorator(
        async_function: Callable[..., Coroutine[Any, Any, T]],
    ):
        @lru_cache(maxsize=maxsize)
        @wraps(async_function)
        def internal(*args, **kwargs):
            coroutine = async_function(*args, **kwargs)
            # Unlike regular coroutine functions, futures can be awaited multiple times
            # so our caller functions can await the same future on multiple cache hits
            return asyncio.ensure_future(coroutine)

        return internal

    return decorator
