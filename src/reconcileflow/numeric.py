"""The decimal context every public entry point computes under.

Python's ``decimal`` reads precision and, wherever no rounding mode is passed,
the rounding mode from the caller's thread-local context. Results that enter
``result_hash`` or decide a verdict must not depend on state the caller never
recorded, so the library owns its context (docs/decimal-context.md).

Python's own defaults: results under the default context are unchanged.

Entry points open it with ``with localcontext(ENGINE_CONTEXT):`` and delegate
to an undecorated private function. Not a decorator: mutmut skips decorated
functions entirely, which would take every entry point out of mutation testing.
``localcontext`` works on a copy, so flags raised inside never leak into
``ENGINE_CONTEXT`` itself.
"""

from decimal import ROUND_HALF_EVEN, Context

ENGINE_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)
