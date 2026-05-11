import functools
import time
import unicodedata

from collections.abc import Iterable, Mapping, Sequence
from types import FunctionType
from typing import Any, Callable


# weak mapping to avoid having to cast nested json
JSONObject = Mapping[str, Any]


def elapsed_and_count[T, **P](func: Callable[P, T]) -> Callable[P, T]:
    # https://docs.astral.sh/ty/reference/typing-faq/#why-does-ty-say-callable-has-no-attribute-__name__
    assert isinstance(func, FunctionType)

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        val = None
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(
                f"<{func.__name__}: count={len(val or [])}, elapsed={time.time() - t:.2f}s>"
            )
        return val

    return wrapper_decorator


def elapsed[T, **P](func: Callable[P, T]) -> Callable[P, T]:
    assert isinstance(func, FunctionType)

    @functools.wraps(func)
    def wrapper_decorator(*args: P.args, **kwargs: P.kwargs) -> T:
        t = time.time()
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(f"<{func.__name__}: elapsed={time.time() - t:.2f}s>")
        return val

    return wrapper_decorator


def normalize_string(string: str) -> str:
    """Return NFKD-normalized, ascii-folded, lowercased form of the input string"""
    return unicodedata.normalize("NFKD", string).encode("ascii", "ignore").decode("ascii").lower()


def sanitize_string(string: str | None) -> str | None:
    """Return stripped string when not empty, otherwise None"""
    return (string.strip() if string else None) or None


def uniquify[T](iterable: Iterable[T]) -> Sequence[T]:
    """Return list of unique elements from `iterable`, preserving original order"""
    return list(dict.fromkeys(iterable))


# noop unless args.verbose is set
def verbose_print(*args, **kwargs):
    return None
