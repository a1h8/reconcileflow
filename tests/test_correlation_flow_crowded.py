"""A value repeated without end must not hold everything open.

More observations never undo a collision, so past a declared size a join's
collision is definitive: its members go final as they pass the horizon, without
waiting for the rest. The verdicts are the ones with no limit; only the partner
lists stop, since N decisions naming N partners is what cost the most.
"""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation import flow as flow_module
from reconcileflow.correlation.exact import Observation
from reconcileflow.correlation.flow import (
    Advance,
    FlowConfig,
    Kind,
    Seen,
    Status,
    _digest,
    _Flow,
    replay_flow,
)
from reconcileflow.correlation.topology import load

TOPOLOGY = load(
    {
        "points": {
            "a": {
                "host": "h",
                "visibility": "kernel",
                "clock": "h",
                "timestamps": "kernel_monotonic",
                "outbound": ["traceparent"],
            },
            "b": {
                "host": "h",
                "visibility": "kernel",
                "clock": "h",
                "timestamps": "kernel_monotonic",
                "inbound": ["traceparent"],
                "outbound": ["traceparent"],
                "propagates": {"traceparent": "proxy"},
            },
            "c": {
                "host": "h",
                "visibility": "kernel",
                "clock": "h",
                "timestamps": "kernel_monotonic",
                "inbound": ["traceparent"],
            },
        },
        "hops": [{"from": "a", "to": "b"}, {"from": "b", "to": "c"}],
    }
)
PLACES = (("a", "outbound"), ("b", "inbound"), ("b", "outbound"), ("c", "inbound"))
SOURCE = "s"
HOP = "a -> b"
WINDOW = 10


def seen(seq, at, value="v", place=("a", "outbound")):
    return Seen(Observation(SOURCE, seq, place[0], place[1], {"traceparent": value}), at)


def config(limit=None, window=WINDOW, lateness=0):
    extra = {} if limit is None else {"max_collision_members": limit}
    return FlowConfig((SOURCE,), window, lateness, **extra)


def repeated(count, step=5):
    """One value, on one side of a hop, again and again."""
    return [seen(seq, seq * step) for seq in range(count)]


def finals(result):
    return {d.business_key: d for d in result.decisions if d.status is Status.FINAL}


def verdicts(result):
    return {key: (d.kind, d.key, d.value) for key, d in finals(result).items()}


def journal_of(raw):
    return [seen(seq, at, value, PLACES[place]) for seq, (at, value, place) in enumerate(raw)] + [
        Advance(SOURCE, 10_000)
    ]


JOURNALS = st.lists(
    st.tuples(
        st.integers(0, 60),
        st.sampled_from(("v", "w")),
        st.integers(0, len(PLACES) - 1),
    ),
    max_size=40,
)


@settings(deadline=None, max_examples=300)
@given(raw=JOURNALS, limit=st.integers(2, 4), window=st.integers(0, 20))
def test_the_verdicts_are_those_with_no_limit(raw, limit, window):
    journal = journal_of(raw)
    limited = replay_flow(TOPOLOGY, journal, config(limit, window))
    whole = replay_flow(TOPOLOGY, journal, config(10**9, window))
    assert verdicts(limited) == verdicts(whole)
    assert len(limited.decisions) <= len(whole.decisions) + len(finals(whole))


@settings(deadline=None, max_examples=100)
@given(raw=JOURNALS, limit=st.integers(2, 4))
def test_nothing_stays_open_and_each_observation_has_one_proof(raw, limit):
    state = _Flow(TOPOLOGY, config(limit))
    for entry in journal_of(raw):
        state.feed(entry)
    assert not state.open and not state.settled and not state.crowded and not state.holders
    proved = [i for d in state.dossiers for i in d.observations]
    assert len(proved) == len(set(proved))


def test_below_the_limit_a_collision_still_names_its_partners():
    journal = [seen(0, 0), seen(1, 1), seen(2, 2, place=("b", "inbound")), Advance(SOURCE, 100)]
    decision = finals(replay_flow(TOPOLOGY, journal, config()))[HOP, (SOURCE, 2)]
    assert decision.kind is Kind.COLLIDED
    assert decision.partners == ((SOURCE, 0), (SOURCE, 1))


def test_at_the_limit_it_still_does_past_it_the_value_stands_alone():
    def partners(members):
        result = replay_flow(
            TOPOLOGY, [*repeated(members, step=0), Advance(SOURCE, 100)], config(3)
        )
        return finals(result)[HOP, (SOURCE, 0)]

    assert partners(3).kind is Kind.COLLIDED
    alone = partners(4)
    assert (alone.kind, alone.key, alone.value, alone.partners) == (
        Kind.COLLIDED,
        "traceparent",
        "v",
        (),
    )


def test_the_limit_counts_both_sides_of_the_join():
    journal = [
        seen(0, 0),
        seen(1, 0),
        seen(2, 0, place=("b", "inbound")),
        seen(3, 0, place=("b", "inbound")),
        Advance(SOURCE, 100),
    ]
    assert finals(replay_flow(TOPOLOGY, journal, config(3)))[HOP, (SOURCE, 0)].partners == ()


def test_members_leave_as_they_pass_the_horizon_while_the_value_keeps_coming():
    state = _Flow(TOPOLOGY, config(4))
    largest = 0
    for entry in repeated(500):
        state.feed(entry)
        largest = max(largest, len(state.open))
    assert largest <= WINDOW // 5 + 6  # the window's traffic, and those before the limit


def test_a_member_is_not_final_before_its_horizon():
    state = _Flow(TOPOLOGY, config(2))
    for entry in repeated(10, step=1):  # all within the window
        state.feed(entry)
    assert not [d for d in state.decisions if d.status is Status.FINAL]
    assert len(state.open) == 10


def test_each_member_that_leaves_has_its_own_proof_citing_the_value():
    result = replay_flow(TOPOLOGY, [*repeated(50), Advance(SOURCE, 10_000)], config(4))
    assert len(result.dossiers) == 50
    assert all(len(d.observations) == 1 and len(d.decisions) == 1 for d in result.dossiers)
    assert {d.cites for d in result.dossiers} == {("traceparent", "v")}


def test_a_whole_group_cites_nothing():
    result = replay_flow(TOPOLOGY, [*repeated(3, step=0), Advance(SOURCE, 100)], config(4))
    assert [d.cites for d in result.dossiers] == [None]


def test_a_carrier_is_collided_while_the_value_is_open_and_told_once_it_closed():
    journal = [*repeated(10, step=5), seen(100, 50), Advance(SOURCE, 1_000), seen(101, 1_001)]
    decisions = finals(replay_flow(TOPOLOGY, journal, config(4)))
    assert decisions[HOP, (SOURCE, 100)].kind is Kind.COLLIDED
    assert decisions["", (SOURCE, 101)].kind is Kind.VALUE_AFTER_FINAL


def test_a_lone_later_carrier_on_the_other_side_is_collided_not_linked():
    journal = [*repeated(10), seen(100, 50, place=("b", "inbound")), Advance(SOURCE, 1_000)]
    decision = finals(replay_flow(TOPOLOGY, journal, config(4)))[HOP, (SOURCE, 100)]
    assert (decision.kind, decision.partners) == (Kind.COLLIDED, ())


def test_a_member_with_another_join_clean_waits_for_its_group():
    journal = [
        seen(100, 0, place=("b", "inbound")),
        seen(101, 0, place=("b", "outbound")),
        *repeated(40),
    ]
    state = _Flow(TOPOLOGY, config(4))
    for entry in journal:
        state.feed(entry)
    assert (SOURCE, 100) in state.open  # past its horizon, not all its joins collided
    state.feed(Advance(SOURCE, 1_000))
    decisions = {d.business_key: d for d in state.decisions}
    assert decisions[HOP, (SOURCE, 100)].kind is Kind.COLLIDED
    assert decisions["b", (SOURCE, 100)].kind is Kind.LINKED
    assert decisions["b", (SOURCE, 100)].partners == ((SOURCE, 101),)


def test_a_member_settled_after_its_horizon_leaves_as_soon_as_it_is():
    state = _Flow(TOPOLOGY, config(4))
    for entry in [seen(0, 0), seen(1, 100), seen(2, 101), seen(3, 102)]:
        state.feed(entry)
    assert (SOURCE, 0) in state.open  # past its horizon, but three members are no crowd
    state.feed(seen(4, 103))
    assert (SOURCE, 0) not in state.open


def test_a_member_with_two_values_never_leaves_alone():
    identifiers = ["traceparent", "tuple5", "h2_stream_id"]
    point = {
        "host": "h",
        "visibility": "kernel",
        "clock": "h",
        "timestamps": "kernel_monotonic",
    }
    topology = load(
        {
            "points": {
                "a": {**point, "outbound": identifiers},
                "b": {**point, "inbound": identifiers},
            },
            "hops": [{"from": "a", "to": "b", "pooling": True}],
        }
    )
    state = _Flow(topology, config(2))
    for seq in range(20):
        keys = {"traceparent": "v", "tuple5": f"p{seq}", "h2_stream_id": "1"}
        state.feed(Seen(Observation(SOURCE, seq, "a", "outbound", keys), seq * 5))
    assert state.crowded
    assert len(state.open) == 20
    assert not state.settled


def test_what_is_settled_is_not_evaluated_again(monkeypatch):
    sizes = []
    real = flow_module.exact_links

    def spying(topology, observations):
        observations = list(observations)
        sizes.append(len(observations))
        return real(topology, observations)

    monkeypatch.setattr(flow_module, "exact_links", spying)
    replay_flow(TOPOLOGY, repeated(200, step=0), config(4))
    assert max(sizes) <= 5


def test_finding_a_group_does_not_go_over_what_is_settled(monkeypatch):
    calls = []
    real = _Flow._values

    def spying(self, obs):
        calls.append(obs.seq)
        return real(self, obs)

    monkeypatch.setattr(_Flow, "_values", spying)
    state = _Flow(TOPOLOGY, config(4))
    for entry in repeated(300, step=0):
        state.feed(entry)
    assert len(state.settled) == 300
    calls.clear()
    state.feed(seen(1000, 0))
    assert len(calls) <= 20


def test_a_crowd_does_not_emit_a_decision_per_member_per_arrival():
    result = replay_flow(TOPOLOGY, [*repeated(500, step=0), Advance(SOURCE, 100)], config(4))
    assert len(result.decisions) < 3 * 500


@pytest.mark.parametrize("seed", range(3))
def test_the_final_decisions_do_not_depend_on_the_arrival_order(seed):
    entries = repeated(30, step=1) + [seen(100 + i, i, place=("b", "inbound")) for i in range(30)]
    shuffled = list(entries)
    random.Random(seed).shuffle(shuffled)
    window, lateness = 1_000, 1_000
    in_order = sorted(entries, key=lambda e: e.at)
    ids = [
        {
            d.decision_id
            for d in replay_flow(
                TOPOLOGY, [*journal, Advance(SOURCE, 100_000)], config(4, window, lateness)
            ).decisions
            if d.status is Status.FINAL
        }
        for journal in (in_order, shuffled)
    ]
    assert ids[0] == ids[1]
    dossiers = [
        {
            d.dossier_id
            for d in replay_flow(
                TOPOLOGY, [*journal, Advance(SOURCE, 100_000)], config(4, window, lateness)
            ).dossiers
        }
        for journal in (in_order, shuffled)
    ]
    assert dossiers[0] == dossiers[1]


def test_the_same_journal_gives_the_same_bytes():
    journal = [*repeated(100), Advance(SOURCE, 10_000)]
    assert replay_flow(TOPOLOGY, journal, config(4)) == replay_flow(TOPOLOGY, journal, config(4))


def test_the_default_limit_leaves_the_config_fingerprint_where_it_was():
    expected = _digest(
        "flow", "sources", SOURCE, "window", "10", "lateness", "0", "max_silence", "None"
    )
    assert config().config_id() == expected
    assert config(flow_module.DEFAULT_MAX_COLLISION_MEMBERS).config_id() == expected


def test_a_declared_limit_moves_the_config_fingerprint():
    assert config(4).config_id() != config().config_id()
    assert config(4).config_id() != config(5).config_id()


@pytest.mark.parametrize("bad", [0, 1, -1, True, 2.5, "3", None])
def test_a_wrong_limit_is_refused(bad):
    with pytest.raises(ValueError, match="max_collision_members"):
        replay_flow(TOPOLOGY, [], config(bad) if bad is not None else _none_limit())


def _none_limit():
    return FlowConfig((SOURCE,), 1, max_collision_members=None)


def test_a_declared_limit_enters_the_fingerprint_by_its_name_and_value():
    expected = _digest(
        "flow",
        "sources",
        SOURCE,
        "window",
        "10",
        "lateness",
        "0",
        "max_silence",
        "None",
        "max_collision",
        "4",
    )
    assert config(4).config_id() == expected


def test_a_proof_of_one_member_is_bound_to_the_value_it_cites():
    state = _Flow(TOPOLOGY, config(4))
    for entry in [*repeated(10), Advance(SOURCE, 10_000)]:
        state.feed(entry)
    dossier = state.dossiers[0]
    assert dossier.cites == ("traceparent", "v")
    assert dossier.dossier_id == _digest(
        "dossier",
        state.topology_id,
        state.config_id,
        f"{SOURCE}\x1f{dossier.observations[0][1]}",
        *dossier.decisions,
        "cites",
        "traceparent",
        "v",
    )
    assert dossier.topology_id == state.topology_id
    assert dossier.config_id == state.config_id
    assert dossier.engine_version == state.engine_version
    assert dossier.positions == (state.positions[dossier.observations[0]],)
    assert dossier.closed_at <= state.position
    assert dossier.closed_at >= 0


def test_a_member_leaving_under_a_silent_source_is_marked_forced_with_its_proof():
    def on(seq, at, source="fast"):
        keys = {"traceparent": "v"}
        return Seen(Observation(source, seq, "a", "outbound", keys), at)

    settings_ = FlowConfig(("fast", "silent"), WINDOW, max_silence_us=1, max_collision_members=2)
    # Leaving while the group is open, then with the group: both forced.
    journal = [Seen(on(0, 0, "silent").observation, 0), *[on(seq, seq * 5) for seq in range(1, 8)]]
    journal.append(Advance("fast", 1_000))
    result = replay_flow(TOPOLOGY, journal, settings_)
    leaving = [d for d in result.dossiers if d.cites is not None and d.forced]
    assert len(leaving) >= 2
    forced_ids = {i for d in leaving for i in d.decisions}
    assert all(d.forced for d in result.decisions if d.decision_id in forced_ids)
    assert forced_ids <= {d.decision_id for d in result.decisions}


def test_those_that_leave_are_forgotten_as_settled_and_the_crowd_with_the_group():
    state = _Flow(TOPOLOGY, config(4))
    for entry in repeated(30):
        state.feed(entry)
    assert len(state.settled) == len(state.open)
    assert state.crowded
    state.feed(Advance(SOURCE, 10_000))
    assert not state.settled
    assert not state.crowded
    assert ("traceparent", "v") in state.closed_values


def test_with_no_source_silent_nothing_that_leaves_is_marked_forced():
    result = replay_flow(TOPOLOGY, [*repeated(30), Advance(SOURCE, 10_000)], config(4))
    assert {d.forced for d in result.dossiers} == {False}
    assert {d.forced for d in result.decisions} == {False}
