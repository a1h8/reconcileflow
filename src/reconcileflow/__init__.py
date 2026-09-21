"""reconcileflow — reconciliation engine.

Milestone J1: matching semantics, in memory, with no streaming and no
persistent state. See the README for the roadmap and the design notes.
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
