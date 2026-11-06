"""Derived links in a flow give what the batch gives, each decision once nothing
on time can change it. The batch path is the oracle: its verdict is what this
inherits, and only the moment of deciding is new here."""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation.assignment import Status as Batch
from reconcileflow.correlation.exact import Observation
from reconcileflow.correlation.flow import (
    Advance,
    FlowConfig,
    Kind,
    Seen,
    Status,
    _Flow,
    replay_flow,
)
from reconcileflow.correlation.flow_timed import Reason, horizon_of
from reconcileflow.correlation.timing import JoinParams, Signal, Timed, timed_links
from reconcileflow.correlation.topology import Evidence, load

POINT = {"host": "h", "visibility": "kernel", "clock": "h", "timestamps": "kernel_monotonic"}
TOPOLOGY = load(
    {
        "points": {
            "a": {**POINT, "inbound": [], "outbound": []},
            "b": {**POINT, "inbound": [], "outbound": []},
        },
        "hops": [{"from": "a", "to": "b", "pooling": True}],
    }
)
WHERE = "a -> b"
UP, DOWN = ("a", "outbound"), ("b", "inbound")
SOURCES = ("s",)
END = Signal("end", 2, 4)
START = Signal("start", 1, 3)
PARAMS = JoinParams((START, END), margin_us=1, max_component=4)
GUARDED = JoinParams((START, END), 1, 4, local_span_us=20, local_limit_permille=400)
FAR = 1_000_000


def config(params=PARAMS, duration=None, lateness=0, **extra):
    return FlowConfig(
        SOURCES,
        1,
        lateness,
        timing={WHERE: params},
        max_duration_us=duration,
        **extra,
    )


def seen(seq, start, end, place=UP, source="s"):
    return Seen(Observation(source, seq, place[0], place[1], {}), end, None, start, end)


def flush():
    return Advance("s", 10 * FAR)  # past any horizon, whatever the lateness


def finals(result):
    return {
        d.business_key[1]: d
        for d in result.decisions
        if d.status is Status.FINAL and d.business_key[0] == WHERE
    }


def verdicts(result):
    out = {}
    for identity, d in finals(result).items():
        if d.kind is Kind.LINKED:
            out[identity] = ("LINKED", d.partners[0], d.weight)
        else:
            out[identity] = (d.reason.name,)
    return out


def batch_verdicts(entries, params):
    observations = [
        Timed(e.observation.source, e.observation.seq, *_place(e), e.start_us, e.end_us)
        for e in entries
    ]
    result = timed_links(TOPOLOGY, observations, {WHERE: params})
    out = {}
    for link in result.links:
        out[link.upstream] = ("LINKED", link.downstream, link.weight)
        out[link.downstream] = ("LINKED", link.upstream, link.weight)
    for u in result.undecided:
        out[u.observation] = (Batch(u.status).name,)
    return out


def _place(entry):
    return entry.observation.point, entry.observation.side


RAW = st.lists(
    st.tuples(st.integers(0, 120), st.integers(0, 30), st.booleans()),
    max_size=30,
)
ALL_PARAMS = st.builds(
    JoinParams,
    st.sampled_from(
        (
            (END,),
            (START, END),
            (END, START),
            (Signal("end", -3, 2),),
            (Signal("end", 1, 3), Signal("duration", 0, 5)),
        )
    ),
    st.integers(0, 4),
    st.integers(1, 5),
    st.sampled_from((0, 10, 40)),
    st.sampled_from((100, 400, 1000)),
)


def entries_of(raw):
    return [
        seen(seq, start, start + length, UP if up else DOWN)
        for seq, (start, length, up) in enumerate(raw)
    ]


def in_end_order(entries):
    return sorted(entries, key=lambda e: e.at)


# -- the batch is the oracle ---------------------------------------------------


@settings(deadline=None, max_examples=500)
@given(raw=RAW, params=ALL_PARAMS)
def test_the_finals_are_the_batch_decisions_in_end_order(raw, params):
    entries = entries_of(raw)
    result = replay_flow(TOPOLOGY, [*in_end_order(entries), flush()], config(params, 30))
    assert verdicts(result) == batch_verdicts(entries, params)


@settings(deadline=None, max_examples=300)
@given(raw=RAW, params=ALL_PARAMS, seed=st.integers(0, 10**6))
def test_they_are_in_any_order_within_the_lateness(raw, params, seed):
    entries = entries_of(raw)
    mixed = list(entries)
    random.Random(seed).shuffle(mixed)
    result = replay_flow(TOPOLOGY, [*mixed, flush()], config(params, 30, lateness=FAR))
    assert verdicts(result) == batch_verdicts(entries, params)


@settings(deadline=None, max_examples=300)
@given(raw=RAW, params=ALL_PARAMS, every=st.integers(1, 4))
def test_they_are_with_the_flow_moving_between_entries(raw, params, every):
    entries = in_end_order(entries_of(raw))
    journal = []
    for k, entry in enumerate(entries):
        journal.append(entry)
        if k % every == 0:
            journal.append(Advance("s", entry.at))
    result = replay_flow(TOPOLOGY, [*journal, flush()], config(params, 30))
    assert verdicts(result) == batch_verdicts(entries, params)


@settings(deadline=None, max_examples=200)
@given(raw=RAW, params=ALL_PARAMS, seed=st.integers(0, 10**6))
def test_what_arrives_late_is_told_and_the_rest_is_the_batch_of_the_rest(raw, params, seed):
    entries = entries_of(raw)
    mixed = list(entries)
    random.Random(seed).shuffle(mixed)
    result = replay_flow(TOPOLOGY, [*mixed, flush()], config(params, 30))
    late = {d.business_key[1] for d in result.decisions if d.kind is Kind.LATE}
    kept = [e for e in entries if (e.observation.source, e.observation.seq) not in late]
    assert verdicts(result) == batch_verdicts(kept, params)
    assert set(verdicts(result)).isdisjoint(late)


@settings(deadline=None, max_examples=200)
@given(raw=RAW, limit=st.integers(1, 20))
def test_what_is_too_long_is_told_and_the_rest_is_the_batch_of_the_rest(raw, limit):
    entries = entries_of(raw)
    result = replay_flow(TOPOLOGY, [*in_end_order(entries), flush()], config(GUARDED, limit))
    kept = [e for e in entries if e.end_us - e.start_us <= limit]
    long = {verdict for k, verdict in verdicts(result).items() if verdict == ("TOO_LONG",)}
    assert long == ({("TOO_LONG",)} if len(kept) < len(entries) else set())
    decided = {k: v for k, v in verdicts(result).items() if v != ("TOO_LONG",)}
    assert decided == batch_verdicts(kept, GUARDED)


@settings(deadline=None, max_examples=100)
@given(raw=RAW, params=ALL_PARAMS)
def test_every_observation_leaves_with_one_final_and_nothing_stays(raw, params):
    entries = entries_of(raw)
    state = _Flow(TOPOLOGY, config(params, 30))
    for entry in [*in_end_order(entries), flush()]:
        state.feed(entry)
    join = state.timed[WHERE]
    assert not join.open and not join.edges and not join.flagged and not join.heap
    finals_by_key = {}
    for d in state.decisions:
        if d.status is Status.FINAL and d.business_key[0] == WHERE:
            assert d.business_key not in finals_by_key
            finals_by_key[d.business_key] = d
    assert len(finals_by_key) == len(entries)
    proved = [i for d in state.dossiers if d.where == WHERE for i in d.observations]
    assert sorted(proved) == sorted(k[1] for k in finals_by_key)


# -- when ------------------------------------------------------------------------


def test_the_horizon_takes_the_end_window_and_the_neighbours_delay():
    assert horizon_of(PARAMS, None) == 2 + 4
    assert horizon_of(JoinParams((Signal("end", -9, 4),), 0), None) == 13
    assert horizon_of(GUARDED, 100) == 20 + 100
    assert horizon_of(JoinParams((Signal("end", 500, 4),), 0, 4, 20), 100) == 504


def test_a_decision_stays_provisional_up_to_the_horizon_and_not_past_it():
    state = _Flow(TOPOLOGY, config())
    state.feed(seen(1, 11, 18, DOWN))
    state.feed(seen(0, 10, 20, UP))
    assert not [d for d in state.decisions if d.status is Status.FINAL]
    state.feed(Advance("s", 26))  # 20 + 6: not yet past it
    assert not [d for d in state.decisions if d.status is Status.FINAL]
    state.feed(Advance("s", 27))
    assert {d.kind for d in state.decisions if d.status is Status.FINAL} == {Kind.LINKED}


def test_a_group_waits_for_its_latest_member_not_its_first():
    state = _Flow(TOPOLOGY, config())
    state.feed(seen(0, 10, 20, UP))
    state.feed(seen(1, 12, 22, DOWN))  # candidates: gap end -2
    state.feed(Advance("s", 27))  # past the first's horizon, not the latest's (28)
    assert not [d for d in state.decisions if d.status is Status.FINAL]
    state.feed(Advance("s", 29))
    assert len([d for d in state.decisions if d.status is Status.FINAL]) == 2


def test_a_decision_is_written_only_when_it_changes():
    state = _Flow(TOPOLOGY, config())
    state.feed(seen(1, 11, 18, DOWN))
    state.feed(seen(0, 10, 20, UP))
    state.feed(seen(2, 24, 25, UP))  # no candidate of either: nothing changes for the pair
    pair = [d for d in state.decisions if d.business_key[1] in {("s", 0), ("s", 1)}]
    assert [d.kind for d in pair if d.business_key[1] == ("s", 1)] == [Kind.UNDECIDED, Kind.LINKED]
    assert [d.kind for d in pair if d.business_key[1] == ("s", 0)] == [Kind.LINKED]


def test_a_provisional_link_gives_way_to_a_rival():
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), seen(2, 11, 20, DOWN), flush()]
    result = replay_flow(TOPOLOGY, journal, config(JoinParams((START, END), 3, 4)))
    chain = [d for d in result.decisions if d.business_key == (WHERE, ("s", 0))]
    assert chain[0].kind is Kind.LINKED and chain[0].status is Status.PROVISIONAL
    final = chain[-1]
    assert (final.kind, final.reason, final.status) == (
        Kind.UNDECIDED,
        Reason.AMBIGUOUS,
        Status.FINAL,
    )
    assert final.supersedes == chain[-2].decision_id
    assert final.chain_version == len(chain)


# -- what is decided -------------------------------------------------------------


def test_a_derived_link_says_so_and_carries_its_weight_both_ways():
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), flush()]
    decisions = finals(replay_flow(TOPOLOGY, journal, config()))
    up, down = decisions["s", 0], decisions["s", 1]
    assert (up.kind, up.evidence, up.partners) == (Kind.LINKED, Evidence.DERIVED, (("s", 1),))
    assert (down.kind, down.evidence, down.partners) == (Kind.LINKED, Evidence.DERIVED, (("s", 0),))
    assert up.weight == down.weight == 1 + 3 + 4
    assert up.reason is None and up.key is None and up.value is None


def test_what_is_undecided_has_its_reason_and_no_partner():
    decision = finals(replay_flow(TOPOLOGY, [seen(0, 10, 20, UP), flush()], config()))["s", 0]
    assert (decision.kind, decision.reason) == (Kind.UNDECIDED, Reason.NO_CANDIDATE)
    assert decision.partners == () and decision.evidence is None and decision.weight is None


def test_a_dense_pair_is_withdrawn_both_sides():
    params = JoinParams((END,), 0, 8, local_span_us=20, local_limit_permille=100)
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), seen(2, 12, 30, UP), flush()]
    decisions = finals(replay_flow(TOPOLOGY, journal, config(params, 100)))
    assert decisions["s", 0].reason is Reason.DENSE
    assert decisions["s", 1].reason is Reason.DENSE


def test_too_long_is_final_at_once_and_counts_for_nobody():
    params = JoinParams((END,), 0, 8, local_span_us=20, local_limit_permille=100)
    state = _Flow(TOPOLOGY, config(params, 5))
    state.feed(seen(9, 0, 15, UP))
    decision = state.current[WHERE, ("s", 9)]
    assert (decision.kind, decision.reason, decision.status) == (
        Kind.UNDECIDED,
        Reason.TOO_LONG,
        Status.FINAL,
    )
    assert [d.observations for d in state.dossiers] == [(("s", 9),)]
    for entry in [seen(1, 15, 18, DOWN), seen(0, 16, 20, UP), flush()]:
        state.feed(entry)
    assert state.current[WHERE, ("s", 0)].kind is Kind.LINKED


def test_a_side_without_times_is_final_at_once():
    state = _Flow(TOPOLOGY, config())
    state.feed(Seen(Observation("s", 0, "a", "outbound", {}), 20))
    decision = state.current[WHERE, ("s", 0)]
    assert (decision.kind, decision.reason, decision.status) == (
        Kind.UNDECIDED,
        Reason.TIME_ABSENT,
        Status.FINAL,
    )
    assert not state.timed[WHERE].open


def test_a_point_outside_the_join_is_not_decided_here():
    state = _Flow(TOPOLOGY, config())
    state.feed(seen(0, 10, 20, ("a", "inbound")))
    assert (WHERE, ("s", 0)) not in state.current


def test_a_side_without_candidates_on_a_timed_flow_leaves_authored_joins_alone():
    topology = load(
        {
            "points": {
                "a": {**POINT, "inbound": [], "outbound": []},
                "b": {**POINT, "inbound": [], "outbound": ["traceparent"]},
                "c": {**POINT, "inbound": ["traceparent"], "outbound": []},
            },
            "hops": [{"from": "a", "to": "b", "pooling": True}, {"from": "b", "to": "c"}],
        }
    )
    journal = [
        seen(1, 11, 18, DOWN),
        Seen(Observation("s", 2, "b", "outbound", {"traceparent": "v"}), 18),
        Seen(Observation("s", 3, "c", "inbound", {"traceparent": "v"}), 19),
        seen(0, 10, 20, UP),
        flush(),
    ]
    result = replay_flow(topology, journal, config())
    effective = result.effective()
    assert effective[WHERE, ("s", 0)].kind is Kind.LINKED
    assert effective["b -> c", ("s", 2)].kind is Kind.LINKED
    assert effective["b -> c", ("s", 2)].evidence is Evidence.AUTHORED
    # No union: two proofs, one for each.
    wheres = {d.where: d.observations for d in result.dossiers if d.decisions}
    assert wheres[WHERE] == (("s", 0), ("s", 1))
    assert wheres[None] == (("s", 2), ("s", 3))


def test_a_value_told_after_its_group_still_enters_the_timed_join():
    topology = load(
        {
            "points": {
                "a": {**POINT, "inbound": [], "outbound": ["traceparent"]},
                "b": {**POINT, "inbound": [], "outbound": ["traceparent"]},
                "c": {**POINT, "inbound": ["traceparent"], "outbound": []},
            },
            "hops": [{"from": "a", "to": "b", "pooling": True}, {"from": "b", "to": "c"}],
        }
    )

    def carrying(seq, start, end, point, side):
        return Seen(Observation("s", seq, point, side, {"traceparent": "v"}), end, None, start, end)

    journal = [
        carrying(0, 0, 1, "b", "outbound"),
        carrying(1, 1, 2, "c", "inbound"),
        Advance("s", 100),
        seen(3, 102, 108, DOWN),
        carrying(2, 101, 110, "a", "outbound"),
        flush(),
    ]
    effective = replay_flow(topology, journal, config()).effective()
    assert effective["", ("s", 2)].kind is Kind.VALUE_AFTER_FINAL
    assert effective[WHERE, ("s", 2)].kind is Kind.LINKED


# -- too large a component -------------------------------------------------------


def crowd(count, step=2):
    """Every observation a candidate of the next: one component that never closes."""
    return [seen(k, k * step, k * step + 5, UP if k % 2 else DOWN) for k in range(count)]


def test_members_of_an_oversized_component_leave_past_the_horizon_alone():
    params = JoinParams((Signal("end", 0, 3),), 0, 3)
    state = _Flow(TOPOLOGY, config(params))
    largest = 0
    for entry in crowd(200):
        state.feed(entry)
        largest = max(largest, len(state.timed[WHERE].open))
    assert largest <= 3 + 6
    state.feed(flush())
    decisions = finals_of_state(state)
    assert {d.reason for d in decisions} == {Reason.OVERSIZED}
    assert len(decisions) == 200


def finals_of_state(state):
    return [d for d in state.decisions if d.status is Status.FINAL and d.business_key[0] == WHERE]


def test_each_of_them_has_its_own_proof():
    params = JoinParams((Signal("end", 0, 3),), 0, 3)
    result = replay_flow(TOPOLOGY, [*crowd(50), flush()], config(params))
    proofs = [d for d in result.dossiers if d.where == WHERE]
    assert len(proofs) == 50
    assert all(len(p.observations) == 1 and len(p.decisions) == 1 for p in proofs)


def test_oversized_counts_each_side_like_the_batch():
    params = JoinParams((Signal("end", 0, 50),), 0, 2)
    two_each = [seen(0, 0, 5), seen(2, 0, 5, DOWN), seen(1, 0, 6), seen(3, 0, 6, DOWN), flush()]
    reasons = {d.reason for d in finals(replay_flow(TOPOLOGY, two_each, config(params))).values()}
    assert Reason.OVERSIZED not in reasons
    three = [*two_each[:-1], seen(4, 0, 7), flush()]
    reasons = {d.reason for d in finals(replay_flow(TOPOLOGY, three, config(params))).values()}
    assert reasons == {Reason.OVERSIZED}


def test_a_member_past_its_horizon_when_the_component_grows_leaves_then():
    params = JoinParams((Signal("end", 0, 3),), 0, 2)
    state = _Flow(TOPOLOGY, config(params))
    journal = [seen(0, 0, 10, UP), seen(1, 0, 12, DOWN), seen(2, 0, 14, UP)]
    for entry in journal:
        state.feed(entry)
    state.feed(seen(3, 0, 16, DOWN))
    state.feed(seen(4, 0, 18, UP))
    assert ("s", 0) not in state.timed[WHERE].open
    assert state.current[WHERE, ("s", 0)].status is Status.FINAL


def test_a_new_neighbour_of_an_oversized_member_is_oversized():
    params = JoinParams((Signal("end", 0, 3),), 0, 2)
    state = _Flow(TOPOLOGY, config(params))
    for entry in crowd(6):
        state.feed(entry)
    state.feed(seen(50, 0, 15, DOWN))
    assert state.current[WHERE, ("s", 50)].reason is Reason.OVERSIZED


# -- the bound on what is kept ---------------------------------------------------


def test_neighbours_kept_follow_the_traffic_of_the_horizon():
    state = _Flow(TOPOLOGY, config(GUARDED, 10))
    most = 0
    for k in range(3000):
        state.feed(seen(k, k * 7, k * 7 + 5, UP if k % 2 else DOWN))
        join = state.timed[WHERE]
        most = max(most, len(join.references[UP[1]]) + len(join.references[DOWN[1]]))
    assert most <= 2 * (20 + 10 + 20 + 10) // 7 + 8


def test_the_candidates_of_an_entry_are_looked_for_near_its_end(monkeypatch):
    from reconcileflow.correlation import flow_timed

    sizes = []
    real = flow_timed.candidates

    def spying(ups, downs, params):
        ups, downs = list(ups), list(downs)
        sizes.append(len(ups) + len(downs))
        return real(ups, downs, params)

    monkeypatch.setattr(flow_timed, "candidates", spying)
    long = JoinParams((Signal("end", 0, 3),), 0, 4, 0)
    state = _Flow(TOPOLOGY, config(long, lateness=FAR))
    for k in range(500):  # all kept open by the lateness
        state.feed(seen(k, k * 2, k * 2 + 5, UP if k % 2 else DOWN))
    assert 2 <= max(sizes) <= 4


# -- proof and replay -------------------------------------------------------------


def test_the_proof_names_the_method_the_parameters_and_the_calibration():
    calibration = {WHERE: ("hash-1", "hash-2")}
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), flush()]
    state = _Flow(TOPOLOGY, config(calibration=calibration))
    for entry in journal:
        state.feed(entry)
    proof = next(d for d in state.dossiers if d.where == WHERE)
    assert proof.method == "nested_timing"
    assert proof.calibration == ("hash-1", "hash-2")
    assert proof.params_id == state.timed[WHERE].params_id
    other = _Flow(TOPOLOGY, config(GUARDED, 5))
    assert other.timed[WHERE].params_id != proof.params_id
    assert proof.observations == (("s", 0), ("s", 1))
    assert proof.topology_id == state.topology_id and proof.config_id == state.config_id
    assert set(proof.decisions) == {d.decision_id for d in finals_of_state(state)}


def test_the_proof_id_moves_with_the_parameters_and_the_calibration():
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), flush()]

    def proof(**kwargs):
        result = replay_flow(TOPOLOGY, journal, config(**kwargs))
        return next(d for d in result.dossiers if d.where == WHERE).dossier_id

    assert proof() == proof()
    assert proof() != proof(calibration={WHERE: ("x",)})


@settings(deadline=None, max_examples=100)
@given(raw=RAW, params=ALL_PARAMS)
def test_the_same_journal_gives_the_same_bytes(raw, params):
    journal = [*in_end_order(entries_of(raw)), flush()]
    assert replay_flow(TOPOLOGY, journal, config(params, 30)) == replay_flow(
        TOPOLOGY, journal, config(params, 30)
    )


@pytest.mark.parametrize("seed", range(5))
def test_the_final_decisions_and_proofs_do_not_depend_on_the_arrival_order(seed):
    rng = random.Random(seed)
    entries = [
        seen(k, s, s + rng.randrange(5, 12), UP if k % 2 else DOWN)
        for k, s in enumerate(rng.randrange(0, 400) for _ in range(60))
    ]
    mixed = list(entries)
    rng.shuffle(mixed)

    def outcome(journal):
        result = replay_flow(TOPOLOGY, [*journal, flush()], config(GUARDED, 30, lateness=FAR))
        return (
            {d.decision_id for d in result.decisions if d.status is Status.FINAL},
            {d.dossier_id for d in result.dossiers},
        )

    assert outcome(mixed) == outcome(in_end_order(entries))


def test_a_duplicate_counts_once_and_different_times_are_refused():
    entry = seen(0, 10, 20, UP)
    result = replay_flow(TOPOLOGY, [entry, entry, flush()], config())
    assert result.duplicates == 1
    other = seen(0, 11, 20, UP)
    with pytest.raises(ValueError, match="different content"):
        replay_flow(TOPOLOGY, [entry, other, flush()], config())


# -- what is refused ---------------------------------------------------------------


def test_nothing_declared_moves_no_fingerprint():
    plain = FlowConfig(SOURCES, 1)
    assert plain == FlowConfig(SOURCES, 1, 0, None, 32, None, None, None)
    assert config().config_id() != plain.config_id()
    assert config().config_id() != config(GUARDED, 5).config_id()
    assert config(GUARDED, 5).config_id() != config(GUARDED, 6).config_id()
    assert config().config_id() != config(calibration={WHERE: ("x",)}).config_id()


@pytest.mark.parametrize(
    ("params", "duration", "message"),
    [
        (JoinParams((START,), 0), None, "end signal"),
        (JoinParams((Signal("duration", 0, 3),), 0), None, "end signal"),
        (JoinParams((END,), 0, 4, 10), None, "max_duration_us"),
        (JoinParams((Signal("duration", 0, 3), END), 0, 4, 10), 5, "first signal"),
        (PARAMS, 0, "max_duration_us"),
        (PARAMS, True, "max_duration_us"),
        (PARAMS, 1.5, "max_duration_us"),
    ],
)
def test_a_join_that_cannot_be_bounded_is_refused(params, duration, message):
    with pytest.raises(ValueError, match=message):
        replay_flow(TOPOLOGY, [], config(params, duration))


def test_parameters_on_a_hop_that_is_not_timed_are_refused():
    topology = load(
        {
            "points": {
                "a": {**POINT, "inbound": [], "outbound": ["traceparent"]},
                "b": {**POINT, "inbound": ["traceparent"], "outbound": []},
            },
            "hops": [{"from": "a", "to": "b"}],
        }
    )
    with pytest.raises(ValueError, match="not a timing-derived hop"):
        replay_flow(topology, [], config())


def test_wrong_parameters_are_refused_as_in_the_batch():
    with pytest.raises(ValueError, match="window must be"):
        replay_flow(TOPOLOGY, [], config(JoinParams((Signal("end", 0, 0),), 0)))
    with pytest.raises(ValueError, match="margin"):
        replay_flow(TOPOLOGY, [seen(0, 0, 1), flush()], config(JoinParams((END,), -1)))


@pytest.mark.parametrize(
    ("start", "end", "at", "message"),
    [
        (10, None, 20, "together"),
        (None, 20, 20, "together"),
        (10, 20, 19, "at must be"),
        (21, 20, 20, "ends before"),
        ("10", 20, 20, "integer"),
        (10, "20", 20, "integer"),
        (True, 20, 20, "integer"),
    ],
)
def test_wrong_times_are_refused(start, end, at, message):
    entry = Seen(Observation("s", 0, "a", "outbound", {}), at, None, start, end)
    with pytest.raises(ValueError, match=message):
        replay_flow(TOPOLOGY, [entry], config())


# -- what mutation found missing -----------------------------------------------------

TWO_HOPS = load(
    {
        "points": {
            "a": {**POINT, "inbound": [], "outbound": []},
            "b": {**POINT, "inbound": [], "outbound": []},
            "c": {**POINT, "inbound": [], "outbound": []},
        },
        "hops": [
            {"from": "a", "to": "b", "pooling": True},
            {"from": "a", "to": "c", "pooling": True},
        ],
    }
)


def test_each_timed_join_takes_what_concerns_it():
    both = FlowConfig(SOURCES, 1, timing={WHERE: PARAMS, "a -> c": PARAMS})
    journal = [
        Seen(Observation("s", 9, "a", "outbound", {}), 5),  # no times: in both joins
        seen(1, 11, 18, ("c", "inbound")),  # outside the first join, inside the second
        seen(0, 10, 20, UP),
        flush(),
    ]
    effective = replay_flow(TWO_HOPS, journal, both).effective()
    assert effective[WHERE, ("s", 9)].reason is Reason.TIME_ABSENT
    assert effective["a -> c", ("s", 9)].reason is Reason.TIME_ABSENT
    assert effective["a -> c", ("s", 0)].partners == (("s", 1),)
    assert (WHERE, ("s", 1)) not in effective


def test_a_decision_is_not_written_again_when_its_component_grows_around_it():
    state = _Flow(TOPOLOGY, config(JoinParams((START, END), 0, 4)))
    state.feed(seen(1, 11, 18, DOWN))
    state.feed(seen(0, 10, 20, UP))
    state.feed(seen(2, 13, 21, DOWN))  # a weaker candidate of (s, 0)
    assert state.current[WHERE, ("s", 2)].reason is Reason.UNMATCHED
    chain = [d for d in state.decisions if d.business_key == (WHERE, ("s", 0))]
    assert [(d.kind, d.partners) for d in chain] == [(Kind.LINKED, (("s", 1),))]


def test_what_is_final_at_once_has_the_proof_of_its_join_and_is_not_forced():
    state = _Flow(TOPOLOGY, config(calibration={WHERE: ("x",)}))
    state.feed(Seen(Observation("s", 0, "a", "outbound", {}), 20))
    (proof,) = state.dossiers
    assert (proof.where, proof.method, proof.calibration) == (WHERE, "nested_timing", ("x",))
    assert proof.forced is False
    assert proof.decisions == (state.current[WHERE, ("s", 0)].decision_id,)


def test_a_proof_outside_timing_names_no_method():
    state = _Flow(TOPOLOGY, config())
    state.feed(seen(0, 10, 20, UP))
    state.feed(flush())
    (proof,) = [d for d in state.dossiers if d.where == WHERE]
    assert proof.forced is False
    plain = replay_flow(
        load(
            {
                "points": {
                    "b": {**POINT, "inbound": [], "outbound": ["traceparent"]},
                    "c": {**POINT, "inbound": ["traceparent"], "outbound": []},
                },
                "hops": [{"from": "b", "to": "c"}],
            }
        ),
        [
            Seen(Observation("s", 0, "b", "outbound", {"traceparent": "v"}), 1),
            Seen(Observation("s", 1, "c", "inbound", {"traceparent": "v"}), 2),
            flush(),
        ],
        FlowConfig(SOURCES, 1),
    )
    (proof,) = [d for d in plain.dossiers if d.decisions]
    assert (proof.where, proof.method, proof.params_id, proof.calibration) == (None, None, None, ())


def test_a_forced_watermark_forces_the_timed_decisions_and_their_proof():
    two = FlowConfig(("s", "t"), 1, 0, max_silence_us=50, timing={WHERE: PARAMS})
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), flush()]
    result = replay_flow(TOPOLOGY, journal, two)
    timed = [d for d in result.decisions if d.status is Status.FINAL]
    assert len(timed) == 2 and all(d.forced is True for d in timed)
    (proof,) = [d for d in result.dossiers if d.where == WHERE]
    assert proof.forced is True and proof.where == WHERE


def test_the_fingerprints_are_fixed_bytes():
    # Changing them on purpose is allowed; noticing it is the point.
    declared = config(GUARDED, 30, calibration={WHERE: ("x",)})
    assert declared.config_id() == "bd9ce297988f7d0d"
    journal = [seen(1, 11, 18, DOWN), seen(0, 10, 20, UP), flush()]
    (proof,) = [d for d in replay_flow(TOPOLOGY, journal, declared).dossiers if d.where]
    assert proof.dossier_id == "cd40a71af253177f"
    assert len(proof.params_id) == 16


def test_every_declared_part_moves_the_configuration():
    other_guard = JoinParams((START, END), 1, 4, local_span_us=20, local_limit_permille=500)
    assert config(GUARDED, 5).config_id() != config(other_guard, 5).config_id()
    assert config().config_id() != config(max_collision_members=5).config_id()


def test_a_refused_join_is_named():
    with pytest.raises(ValueError, match=f"^{WHERE}: a join in a flow needs an end signal"):
        replay_flow(TOPOLOGY, [], config(JoinParams((START,), 0)))


def test_margin_and_component_bound_are_refused_before_any_entry():
    with pytest.raises(ValueError, match=r"^margin must be"):
        replay_flow(TOPOLOGY, [], config(JoinParams((END,), -1)))
    with pytest.raises(ValueError, match=r"^max_component must be"):
        replay_flow(TOPOLOGY, [], config(JoinParams((END,), 0, 0)))


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (10, None, "^start_us and end_us are given together, or not at all$"),
        ("10", 20, "^start_us must be an integer"),
        (10, "20", "^end_us must be an integer"),
    ],
)
def test_wrong_times_are_named(start, end, message):
    entry = Seen(Observation("s", 0, "a", "outbound", {}), 20, None, start, end)
    with pytest.raises(ValueError, match=message):
        replay_flow(TOPOLOGY, [entry], config())


def test_a_component_bound_above_the_default_is_the_one_used():
    params = JoinParams((Signal("end", 0, 50),), 0, 40)
    entries = [seen(k, k, k) for k in range(33)] + [seen(99, 16, 16, DOWN)]
    result = replay_flow(TOPOLOGY, [*in_end_order(entries), flush()], config(params))
    assert verdicts(result)["s", 99] == ("LINKED", ("s", 16), 51)
    assert verdicts(result) == batch_verdicts(entries, params)


def test_the_upstream_neighbours_are_counted_by_start_whatever_the_first_signal():
    params = JoinParams((Signal("end", 0, 4),), 0, 4, local_span_us=20, local_limit_permille=200)
    entries = [seen(0, 0, 300), seen(1, 100, 310), seen(2, 200, 320), seen(3, 305, 310, DOWN)]
    result = replay_flow(TOPOLOGY, [*in_end_order(entries), flush()], config(params, 400))
    assert verdicts(result)["s", 3][0] == "LINKED"
    assert verdicts(result) == batch_verdicts(entries, params)


def test_only_the_open_within_the_end_window_are_candidates(monkeypatch):
    from reconcileflow.correlation import flow_timed

    seen_downs = []
    real = flow_timed.candidates

    def spying(ups, downs, params):
        seen_downs.append(sorted(d.seq for d in downs))
        return real(ups, downs, params)

    monkeypatch.setattr(flow_timed, "candidates", spying)
    params = JoinParams((Signal("end", 0, 3),), 0, 4)
    state = _Flow(TOPOLOGY, config(params, lateness=FAR))
    for seq, end in [(1, 96), (2, 97), (3, 103), (4, 104), (5, 110)]:
        state.feed(seen(seq, end - 1, end, DOWN))
    state.feed(seen(0, 99, 100, UP))
    assert seen_downs == [[2, 3]]
    assert state.timed[WHERE].open["s", 0] == Timed("s", 0, "a", "outbound", 99, 100)


def test_nothing_is_kept_about_those_gone():
    params = JoinParams((Signal("end", 0, 3),), 0, 2)
    state = _Flow(TOPOLOGY, config(params))
    for entry in [seen(0, 0, 10, UP), seen(1, 0, 12, DOWN), seen(2, 0, 14, UP)]:
        state.feed(entry)
    join = state.timed[WHERE]
    assert join._walk(("s", 0)) == ([("s", 0), ("s", 1), ("s", 2)], False)
    for entry in [seen(3, 0, 16, DOWN), seen(4, 0, 18, UP), flush()]:
        state.feed(entry)
    assert not join.open and not join.flagged and not join.past and not join.due
