"""The decimal context every public entry point computes under.

Python's ``decimal`` reads precision and, wherever no rounding mode is passed,
the rounding mode from the caller's thread-local context. Results that enter
``result_hash`` or decide a verdict must not depend on state the caller never
recorded, so the library owns its context (docs/decimal-context.md).

Python's own defaults: results under the default context are unchanged.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from decimal import ROUND_HALF_EVEN, Context, localcontext
from typing import ParamSpec, TypeVar

ENGINE_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)

P = ParamSpec("P")
R = TypeVar("R")


def in_engine_context(func: Callable[P, R]) -> Callable[P, R]:
    """Run ``func`` under ``ENGINE_CONTEXT``, whatever the caller's context.

    ``localcontext`` works on a copy, so flags raised inside never leak into
    ``ENGINE_CONTEXT`` itself.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with localcontext(ENGINE_CONTEXT):
            return func(*args, **kwargs)

    return wrapper
