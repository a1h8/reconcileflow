"""Timing path: candidates by window, then the assignment core decides."""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.timing import (
    Candidate,
    DerivedLink,
    JoinParams,
    Signal,
    Timed,
    candidates,
    timed_links,
)
from reconcileflow.correlation.topology import load

HOP = "a -> b"


def timing_only_point():
    return {
        "host": "lab",
        "visibility": "kernel",
        "clock": "lab",
        "timestamps": "kernel_monotonic",
        "inbound": [],
        "outbound": [],
    }


def topology(header_hop=False):
    points = {"a": timing_only_point(), "b": timing_only_point(), "c": timing_only_point()}
    if header_hop:
        points["b"]["outbound"] = ["traceparent"]
        points["c"]["inbound"] = ["traceparent"]
    hops = [{"from": "a", "to": "b", "pooling": True}, {"from": "b", "to": "c"}]
    return load({"points": points, "hops": hops})


def up(seq, start, end, source="s"):
    return Timed(source, seq, "a", "outbound", start, end)


def down(seq, start, end, source="t"):
    return Timed(source, seq, "b", "inbound", start, end)


END = JoinParams(signals=(Signal("end", 0, 100),), margin_us=0)
START_END = JoinParams(signals=(Signal("start", 50, 100), Signal("end", 20, 100)), margin_us=0)


# -- candidates: the window rule, checked against every pair one by one ------


def gap(name, a, b):
    if name == "start":
        return b.start_us - a.start_us
    if name == "end":
        return a.end_us - b.end_us
    return (a.end_us - a.start_us) - (b.end_us - b.start_us)


def oracle_candidates(ups, downs, params):
    """Every pair tested one by one, straight from the definition."""
    out = set()
    for a in ups:
        for b in downs:
            gaps = [(s, gap(s.name, a, b)) for s in params.signals]
            if all(abs(g - s.offset_us) <= s.window_us for s, g in gaps):
                weight = 1 + sum(s.window_us - abs(g - s.offset_us) for s, g in gaps)
                out.add(Candidate((a.source, a.seq), (b.source, b.seq), weight))
    return out


@pytest.mark.parametrize("seed", range(200))
def test_candidates_are_exactly_the_pairs_inside_every_window(seed):
    rng = random.Random(seed)
    names = rng.choice([("end",), ("start",), ("duration",), ("start", "end"), ("end", "duration")])
    params = JoinParams(
        signals=tuple(Signal(n, rng.randint(-200, 200), rng.randint(1, 300)) for n in names),
        margin_us=0,
    )
    ups, downs = [], []
    for k in range(rng.randint(0, 25)):
        s = rng.randint(0, 3_000)
        ups.append(up(k, s, s + rng.randint(0, 2_000)))
    for k in range(rng.randint(0, 25)):
        s = rng.randint(0, 3_000)
        downs.append(down(k, s, s + rng.randint(0, 2_000)))
    assert set(candidates(ups, downs, params)) == oracle_candidates(ups, downs, params)


def test_a_pair_exactly_at_the_window_edge_is_a_candidate_with_weight_one():
    params = JoinParams(signals=(Signal("end", 10, 100),), margin_us=0)
    a, inside, outside = up(1, 0, 1_000), down(1, 0, 890), down(2, 0, 889)
    assert candidates([a], [inside, outside], params) == [Candidate(("s", 1), ("t", 1), 1)]


def test_closer_to_the_usual_offset_weighs_more():
    a = up(1, 0, 1_000)
    near, far = down(1, 0, 1_000), down(2, 0, 950)
    weights = {c.downstream: c.weight for c in candidates([a], [near, far], END)}
    assert weights[("t", 1)] == 101 and weights[("t", 2)] == 51


# -- linking --------------------------------------------------------------------


def test_well_separated_requests_are_all_linked_to_their_own_partner():
    rng = random.Random(7)
    obs = []
    for k in range(200):
        t = k * 10_000  # 10 ms apart, noise below 30 µs, window 100 µs
        obs.append(up(k, t, t + 5_000))
        obs.append(down(k, t + 50 + rng.randint(-30, 30), t + 5_000 - 20 + rng.randint(-30, 30)))
    result = timed_links(topology(), obs, {HOP: START_END})
    assert [(x.upstream, x.downstream) for x in result.links] == [
        (("s", k), ("t", k)) for k in range(200)
    ]
    assert result.undecided == ()
    assert all(isinstance(x, DerivedLink) and x.where == HOP for x in result.links)


def test_two_indistinguishable_requests_are_ambiguous_and_never_linked():
    obs = [up(1, 0, 1_000), up(2, 0, 1_000), down(1, 0, 1_000), down(2, 0, 1_000)]
    result = timed_links(topology(), obs, {HOP: END})
    assert result.links == ()
    assert {(u.observation, u.side, u.status) for u in result.undecided} == {
        (("s", 1), "outbound", Status.AMBIGUOUS),
        (("s", 2), "outbound", Status.AMBIGUOUS),
        (("t", 1), "inbound", Status.AMBIGUOUS),
        (("t", 2), "inbound", Status.AMBIGUOUS),
    }


def test_a_near_tie_within_the_margin_is_ambiguous():
    # Up 1 ends 10 µs closer to down 1 than to down 2: decided at margin 0,
    # not at margin 20.
    obs = [up(1, 0, 1_000), up(2, 0, 1_010), down(1, 0, 1_000), down(2, 0, 1_010)]
    sharp = timed_links(topology(), obs, {HOP: END})
    assert len(sharp.links) == 2
    loose = JoinParams(signals=END.signals, margin_us=200)
    assert timed_links(topology(), obs, {HOP: loose}).links == ()


def test_no_partner_in_the_window_is_reported_not_dropped():
    obs = [up(1, 0, 1_000), down(1, 0, 5_000)]
    result = timed_links(topology(), obs, {HOP: END})
    assert result.links == ()
    assert {(u.observation, u.side, u.status) for u in result.undecided} == {
        (("s", 1), "outbound", Status.NO_CANDIDATE),
        (("t", 1), "inbound", Status.NO_CANDIDATE),
    }


def test_an_oversized_component_is_reported_not_resolved():
    obs = [up(k, 0, 1_000) for k in range(3)] + [down(k, 0, 1_000) for k in range(3)]
    params = JoinParams(signals=END.signals, margin_us=0, max_component=2)
    result = timed_links(topology(), obs, {HOP: params})
    assert result.links == ()
    assert {u.status for u in result.undecided} == {Status.OVERSIZED}
    assert len(result.undecided) == 6
    assert result.components == ((HOP, 6),)


def test_component_sizes_are_reported_per_hop():
    obs = [up(1, 0, 1_000), down(1, 0, 1_000), up(2, 50_000, 51_000), down(2, 50_000, 51_000)]
    obs += [up(3, 90_000, 91_000), down(3, 90_000, 91_000), down(4, 90_000, 91_000)]
    result = timed_links(topology(), obs, {HOP: END})
    assert result.components == ((HOP, 2), (HOP, 2), (HOP, 3))


def test_a_hop_without_params_is_left_alone():
    obs = [up(1, 0, 1_000), down(1, 0, 1_000)]
    assert timed_links(topology(), obs, {}) == timed_links(topology(), [], {})


def test_a_redelivered_observation_counts_once():
    obs = [up(1, 0, 1_000), up(1, 0, 1_000), up(1, 0, 1_000), down(1, 0, 1_000)]
    result = timed_links(topology(), obs, {HOP: END})
    assert result.duplicates_dropped == 2 and len(result.links) == 1


def test_a_link_carries_the_weight_of_its_pair():
    result = timed_links(topology(), [up(1, 0, 1_000), down(1, 0, 970)], {HOP: END})
    assert result.links == (DerivedLink(HOP, ("s", 1), ("t", 1), 71),)


def test_every_timed_hop_is_joined_and_reported():
    mid_out = Timed("u", 1, "b", "outbound", 0, 1_000)
    last_in = Timed("v", 1, "c", "inbound", 0, 1_000)
    obs = [up(1, 0, 1_000), down(1, 0, 1_000), mid_out, last_in]
    result = timed_links(topology(), obs, {HOP: END, "b -> c": END})
    assert [x.where for x in result.links] == [HOP, "b -> c"]
    assert result.components == ((HOP, 2), ("b -> c", 2))


def test_the_widest_windows_the_weight_allows_are_accepted():
    widest = JoinParams(signals=(Signal("end", 0, 10**9 - 1),), margin_us=0)
    assert len(timed_links(topology(), [up(1, 0, 1), down(1, 0, 1)], {HOP: widest}).links) == 1


# Up to 12 pairs: when every draw lands on one instant, the whole set is one
# dense component, solved exactly (assignment core), which is slow by design.
@settings(max_examples=100, deadline=None)
@given(st.randoms(use_true_random=False))
def test_same_result_whatever_the_order_of_the_observations(rng):
    obs = []
    for k in range(12):
        t = rng.randint(0, 20_000)
        obs.append(up(k, t, t + rng.randint(0, 3_000)))
        s = t + rng.randint(-100, 100)
        obs.append(down(k, s, s + rng.randint(0, 3_000)))
    reference = timed_links(topology(), obs, {HOP: START_END})
    shuffled = list(obs)
    rng.shuffle(shuffled)
    assert timed_links(topology(), shuffled, {HOP: START_END}) == reference


# -- refused inputs ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("obs", "reason"),
    [
        (Timed("s", 1, "nowhere", "outbound", 0, 1), "unknown point"),
        (Timed("s", 1, "a", "sideways", 0, 1), "side"),
        (Timed("s", 1, "a", "outbound", 5, 4), "ends before it starts"),
        (Timed("s", 1, "a", "outbound", 0.5, 1), "integer"),
        (Timed("s", True, "a", "outbound", 0, 1), "integer"),
        (Timed(1, 1, "a", "outbound", 0, 1), "source"),
    ],
)
def test_invalid_observations_are_refused(obs, reason):
    with pytest.raises(ValueError, match=reason):
        timed_links(topology(), [obs], {HOP: END})


def test_same_identity_with_different_content_is_refused():
    with pytest.raises(ValueError, match="different content"):
        timed_links(topology(), [up(1, 0, 1_000), up(1, 0, 2_000)], {HOP: END})


@pytest.mark.parametrize(
    ("params", "reason"),
    [
        (JoinParams(signals=(), margin_us=0), "one or two signals"),
        (JoinParams(signals=(Signal("end", 0, 1),) * 3, margin_us=0), "one or two signals"),
        (JoinParams(signals=(Signal("end", 0, 1), Signal("end", 0, 2)), margin_us=0), "twice"),
        (JoinParams(signals=(Signal("middle", 0, 1),), margin_us=0), "unknown signal"),
        (JoinParams(signals=(Signal("end", 0, 0),), margin_us=0), "window"),
        (JoinParams(signals=(Signal("end", 0.5, 1),), margin_us=0), "integer"),
        (JoinParams(signals=(Signal("end", 0, 1),), margin_us=-1), "margin must be an integer"),
        (JoinParams(signals=(Signal("end", 0, 1),), margin_us=0.5), "margin must be an integer"),
        (JoinParams(signals=(Signal("end", 0, 10**9),), margin_us=0), "too wide"),
    ],
)
def test_invalid_params_are_refused(params, reason):
    with pytest.raises(ValueError, match=reason):
        timed_links(topology(), [], {HOP: params})


def test_params_for_a_hop_that_is_not_timing_derived_or_does_not_exist_are_refused():
    with pytest.raises(ValueError, match="not a timing-derived hop"):
        timed_links(topology(header_hop=True), [], {"b -> c": END})
    with pytest.raises(ValueError, match="not a timing-derived hop"):
        timed_links(topology(), [], {"a -> z": END})
