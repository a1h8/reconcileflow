"""One timing-derived hop in a flow: the batch path's decision, taken once no
observation on time can change it.

An entry on time has ``at`` at or past the watermark. Past ``at + H``, it can no
longer be a candidate of an observation (it would end too far, hence the end
signal every such join must have), nor its neighbour for the density guard (it
would start too far, hence ``max_duration_us`` when the guard is on). So:

- a component of the candidate graph closes when the watermark has passed its
  latest member by ``H``, and ``assign`` decides it as in the batch;
- a component too large stays so: its members leave one by one past ``H``, so a
  component that never closes does not hold everything. Only unflagged components
  are walked, and they stay small;
- a closed observation can get no new candidate, so only open ones are searched,
  near the entry's end. Closed ones remain as neighbours while an observation
  open or to come could count them.

Nothing here unions anything with the authored joins; ``flow`` emits the decisions.
"""

from __future__ import annotations

import bisect
import hashlib
import heapq
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from .assignment import Edge, Status, assign
from .exact import Identity, Observation
from .timing import JoinParams, Timed, _crowded, _reference, _validate_params, candidates

METHOD = "nested_timing"


class Reason(Enum):
    """Why a timed observation has no link."""

    AMBIGUOUS = "AMBIGUOUS"
    UNMATCHED = "UNMATCHED"
    OVERSIZED = "OVERSIZED"
    NO_CANDIDATE = "NO_CANDIDATE"
    DENSE = "DENSE"
    TOO_LONG = "TOO_LONG"
    TIME_ABSENT = "TIME_ABSENT"


@dataclass(frozen=True, slots=True)
class Verdict:
    reason: Reason | None  # None: linked
    partner: Identity | None = None
    weight: int | None = None


@dataclass(frozen=True, slots=True)
class Closing:
    members: tuple[Identity, ...]  # sorted
    verdicts: Mapping[Identity, Verdict]


def _end_signal(params: JoinParams):
    return next((s for s in params.signals if s.name == "end"), None)


def horizon_of(params: JoinParams, max_duration_us: int | None) -> int:
    """How far past an observation's end something on time can still concern it."""
    end = _end_signal(params)
    horizon = abs(end.offset_us) + end.window_us
    if params.local_span_us:
        horizon = max(horizon, params.local_span_us + max_duration_us)
    return horizon


def check_join(where: str, params: JoinParams, max_duration_us: int | None) -> None:
    """Refuse a join no bounded wait can decide as the batch would."""
    _validate_params(params)
    assign((), (), (), margin=params.margin_us, max_component=params.max_component)
    if _end_signal(params) is None:
        raise ValueError(f"{where}: a join in a flow needs an end signal, got {params.signals}")
    if params.local_span_us:
        if max_duration_us is None:
            raise ValueError(f"{where}: the density guard needs max_duration_us")
        if params.signals[0].name == "duration":
            raise ValueError(f"{where}: the density guard needs a first signal in time")


class TimedJoin:
    def __init__(
        self, where: str, src: str, dst: str, params: JoinParams, max_duration_us: int | None
    ):
        check_join(where, params, max_duration_us)
        self.where = where
        self.places = {(src, "outbound"): "outbound", (dst, "inbound"): "inbound"}
        self.params = params
        self.max_duration_us = max_duration_us
        self.horizon = horizon_of(params, max_duration_us)
        self.params_id = hashlib.sha256(repr(params).encode()).hexdigest()[:16]
        self.open: dict[Identity, Timed] = {}
        self.by_end: dict[str, list[tuple[int, Identity]]] = {"outbound": [], "inbound": []}
        self.edges: dict[Identity, dict[Identity, int]] = {}
        self.flagged: set[Identity] = set()  # in a component too large
        self.past: set[Identity] = set()  # past the horizon, held by a later member
        self.due: set[Identity] = set()  # past the horizon and now flagged
        self.heap: list[tuple[int, Identity]] = []
        self.references: dict[str, list[int]] = {"outbound": [], "inbound": []}

    def side_of(self, obs: Observation) -> str | None:
        return self.places.get((obs.point, obs.side))

    def too_long(self, start_us: int, end_us: int) -> bool:
        return self.max_duration_us is not None and end_us - start_us > self.max_duration_us

    def add(self, obs: Timed) -> dict[Identity, Verdict]:
        """Enter one observation; what its component now gives, for those it touches."""
        identity = (obs.source, obs.seq)
        self.open[identity] = obs
        bisect.insort(self.by_end[obs.side], (obs.end_us, identity))
        heapq.heappush(self.heap, (obs.end_us, identity))
        if self.params.local_span_us:
            bisect.insort(self.references[obs.side], self._reference_of(obs))
        self.edges[identity] = {}
        for candidate in self._candidates_of(obs):
            self.edges[candidate.upstream][candidate.downstream] = candidate.weight
            self.edges[candidate.downstream][candidate.upstream] = candidate.weight
        members, touched = self._walk(identity)
        largest_side = max(Counter(self.open[m].side for m in members).values())
        if touched or largest_side > self.params.max_component:
            self.flagged.update(members)
            self.due.update(self.past.intersection(members))
            return dict.fromkeys(members, Verdict(Reason.OVERSIZED))
        return self._decide(members)

    def closing(self, watermark: int) -> list[Closing]:
        """What the watermark lets go, in a replayable order."""
        out = []
        while self.heap and self.heap[0][0] + self.horizon < watermark:
            _, identity = heapq.heappop(self.heap)  # popped first: the loop always ends
            if identity not in self.open:
                continue
            if identity in self.flagged:
                out.append(self._leave(identity))
                continue
            members, _ = self._walk(identity)
            if max(self.open[m].end_us for m in members) + self.horizon < watermark:
                verdicts = self._decide(members)
                for member in members:
                    self._remove(member)
                out.append(Closing(tuple(sorted(members)), verdicts))
            else:
                self.past.add(identity)
        for identity in sorted(self.due.intersection(self.open)):
            out.append(self._leave(identity))
        self.due.clear()
        self._forget(watermark)
        return out

    # -- candidates and components -------------------------------------------------

    def _candidates_of(self, obs: Timed):
        """Its candidates: among the open of the other side, ending near it."""
        end = _end_signal(self.params)
        reach = obs.end_us - end.offset_us if obs.side == "outbound" else obs.end_us + end.offset_us
        other = "inbound" if obs.side == "outbound" else "outbound"
        ordered = self.by_end[other]
        low = bisect.bisect_left(ordered, (reach - end.window_us,))
        high = bisect.bisect_left(ordered, (reach + end.window_us + 1,))
        near = [self.open[identity] for _, identity in ordered[low:high]]
        if not near:
            return []
        if obs.side == "outbound":
            return candidates([obs], near, self.params)
        return candidates(near, [obs], self.params)

    def _walk(self, start: Identity) -> tuple[list[Identity], bool]:
        """The unflagged members reached, and whether a flagged one was touched.

        Read by index up to the number open, so the walk ends whatever happens."""
        members = [start]
        reached = {start}
        touched = False
        for k in range(len(self.open)):
            if k == len(members):
                break
            for other in sorted(self.edges[members[k]]):
                if other in self.flagged:
                    touched = True
                elif other not in reached:
                    reached.add(other)
                    members.append(other)
        return members, touched

    def _decide(self, members: list[Identity]) -> dict[Identity, Verdict]:
        ups = sorted(m for m in members if self.open[m].side == "outbound")
        downs = sorted(m for m in members if self.open[m].side == "inbound")
        # Identities become ids in the batch's order: only their order counts.
        up_ids = {m: f"u{n:09d}" for n, m in enumerate(ups)}
        down_ids = {m: f"d{n:09d}" for n, m in enumerate(downs)}
        back = {v: k for k, v in (up_ids | down_ids).items()}
        decided = assign(
            up_ids.values(),
            down_ids.values(),
            [Edge(up_ids[u], down_ids[d], w) for u in ups for d, w in self.edges[u].items()],
            margin=self.params.margin_us,
            max_component=self.params.max_component,
        )
        out: dict[Identity, Verdict] = {}
        for d in (*decided.left, *decided.right):
            identity = back[d.id]
            if d.status is not Status.MATCHED:
                out[identity] = Verdict(Reason[d.status.name])
                continue
            partner = back[d.partner]
            if self._crowded(identity) or self._crowded(partner):
                out[identity] = Verdict(Reason.DENSE)
            else:
                out[identity] = Verdict(None, partner, self.edges[identity][partner])
        return out

    # -- the density guard ---------------------------------------------------------

    def _reference_of(self, obs: Timed) -> int:
        if obs.side == "outbound":
            return obs.start_us
        return _reference(self.params.signals[0].name, obs)

    def _crowded(self, identity: Identity) -> bool:
        span = self.params.local_span_us
        if not span:
            return False
        obs = self.open[identity]
        at = self._reference_of(obs)
        ordered = self.references[obs.side]
        others = bisect.bisect_right(ordered, at + span) - bisect.bisect_left(ordered, at - span)
        return _crowded(others - 1, self.params)

    def _forget(self, watermark: int) -> None:
        """Neighbours nothing open or to come can count any more."""
        span = self.params.local_span_us
        if not span:
            return
        ends = [side[0][0] for side in self.by_end.values() if side]
        oldest = min((*ends, watermark))
        limit = oldest - self.max_duration_us - span
        for ordered in self.references.values():
            del ordered[: bisect.bisect_left(ordered, limit)]

    # -- leaving -------------------------------------------------------------------

    def _leave(self, identity: Identity) -> Closing:
        self._remove(identity)
        return Closing((identity,), {identity: Verdict(Reason.OVERSIZED)})

    def _remove(self, identity: Identity) -> None:
        obs = self.open.pop(identity)
        ordered = self.by_end[obs.side]
        del ordered[bisect.bisect_left(ordered, (obs.end_us, identity))]
        for other in self.edges.pop(identity):
            del self.edges[other][identity]
        self.flagged.discard(identity)
        self.past.discard(identity)
