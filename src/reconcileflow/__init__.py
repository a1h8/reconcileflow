"""reconcileflow — reconciliation engine.

The matching engine stays in memory and side-effect free. The provenance
module adds optional local persistence and versioned source adapters.
"""

from .engine import blocking_key, reconcile
from .models import (
    Match,
    MatchResult,
    Record,
    Reject,
    RejectReason,
    RuleId,
    Tolerance,
)

__all__ = [
    "Match",
    "MatchResult",
    "Record",
    "Reject",
    "RejectReason",
    "RuleId",
    "Tolerance",
    "blocking_key",
    "reconcile",
]
