"""The recipe inside a point (phase 4 spec 8.12): links forced by the calls a
request always makes and their order, from timings alone; never a score."""

import itertools
import random
import time

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.recipe import Recipe, learn_recipe, recipe_links
from reconcileflow.correlation.timing import Timed

BACK, SIDE = "hop -> back", "hop -> side"
RECIPE = Recipe("hop", (BACK, SIDE), ((BACK, SIDE),), 100, "test")


def req(seq, start, end):
    return Timed("in", seq, "hop", "inbound", start, end)


def call(hop, seq, start, end):
    return Timed(hop, seq, "hop", "outbound", start, end)


def overlapping_pair(T=0, drop_side_b=False):
    """A=[T, T+1000], B=[T+50, T+1050]. x1 starts before B: only A's.
    y1 starts before x2 ends: only after x1, so only A's."""
    requests = [req(0, T, T + 1000), req(1, T + 50, T + 1050)]
    calls = {
        BACK: [call(BACK, 0, T + 20, T + 300), call(BACK, 1, T + 150, T + 450)],
        SIDE: [call(SIDE, 0, T + 420, T + 620)]
        + ([] if drop_side_b else [call(SIDE, 1, T + 500, T + 700)]),
    }
    return requests, calls


def linked(result):
    return {(x.where, x.upstream): x.downstream for x in result.links}


def statuses(result):
    return {(u.where, u.observation): u.status for u in result.undecided}


def test_the_order_of_the_recipe_forces_both_requests():
    result = recipe_links(*overlapping_pair(), RECIPE)
    assert linked(result) == {
        (BACK, ("in", 0)): (BACK, 0),
        (SIDE, ("in", 0)): (SIDE, 0),
        (BACK, ("in", 1)): (BACK, 1),
        (SIDE, ("in", 1)): (SIDE, 1),
    }
    assert result.undecided == ()


def test_two_interchangeable_requests_are_ambiguous():
    requests = [req(0, 0, 1000), req(1, 0, 1000)]
    calls = {
        BACK: [call(BACK, 0, 100, 400), call(BACK, 1, 100, 400)],
        SIDE: [call(SIDE, 0, 500, 700), call(SIDE, 1, 500, 700)],
    }
    result = recipe_links(requests, calls, RECIPE)
    assert result.links == ()
    assert set(statuses(result).values()) == {Status.AMBIGUOUS}
    assert len(result.undecided) == 8  # 2 requests and 2 calls, on both hops


def test_a_missing_call_leaves_the_whole_group_incomplete():
    result = recipe_links(*overlapping_pair(drop_side_b=True), RECIPE)
    assert result.links == ()
    assert set(statuses(result).values()) == {Status.INCOMPLETE}


def test_a_chain_of_single_candidates_frees_the_next():
    requests, calls = [], {BACK: [], SIDE: []}
    for i, start in enumerate((0, 200, 400)):
        requests.append(req(i, start, start + 700))
        calls[BACK].append(call(BACK, i, start + 150, start + 250))
        calls[SIDE].append(call(SIDE, i, start + 600, start + 650))
    got = linked(recipe_links(requests, calls, RECIPE))
    assert [got[BACK, ("in", i)] for i in range(3)] == [(BACK, 0), (BACK, 1), (BACK, 2)]


def test_a_request_with_no_possible_call_is_set_aside_before_counting():
    # A truncated copy of a request, as an agent reports it: it starts just
    # before the call and ends before it; it can take no call.
    requests, calls = overlapping_pair()
    requests.append(req(9, 10, 30))
    result = recipe_links(requests, calls, RECIPE)
    assert len(result.links) == 4
    assert {u.status for u in result.undecided} == {Status.NO_CANDIDATE}
    assert {u.observation for u in result.undecided} == {("in", 9)}


def test_one_large_group_is_decided_quickly():
    # 20 000 requests, each overlapping the next: one group, one hop.
    one = Recipe("hop", (BACK,), (), 100, "test")
    requests = [req(i, 100 * i, 100 * i + 250) for i in range(20_000)]
    calls = {BACK: [call(BACK, i, 100 * i + 10, 100 * i + 60) for i in range(20_000)]}
    t0 = time.perf_counter()
    result = recipe_links(requests, calls, one)
    assert time.perf_counter() - t0 < 10
    assert len(result.links) == 20_000
    assert all(x.upstream[1] == x.downstream[1] for x in result.links)


def _every_assignment(requests, calls, recipe):
    """Brute force: the links held by every complete assignment, or None."""
    hops = recipe.calls
    options = {}
    for r in requests:
        sets = [[]]
        for h in hops:
            sets = [
                [*s, c]
                for s in sets
                for c in calls[h]
                if r.start_us <= c.start_us and c.end_us <= r.end_us and c not in s
            ]
        sets = [
            s
            for s in sets
            if all(s[hops.index(a)].end_us <= s[hops.index(b)].start_us for a, b in recipe.order)
        ]
        options[r] = sets
    common = None
    for choice in itertools.product(*(options[r] for r in requests)):
        used = [c for s in choice for c in s]
        if len(used) != len(set(used)):
            continue
        links = {
            (h, (r.source, r.seq)): (c.source, c.seq)
            for r, s in zip(requests, choice, strict=True)
            for h, c in zip(hops, s, strict=True)
        }
        common = links if common is None else {k: v for k, v in common.items() if links[k] == v}
    return common


def test_forced_links_are_held_by_every_assignment():
    rng = random.Random(11)
    one = Recipe("hop", (BACK,), (), 100, "test")
    for trial in range(300):
        recipe = RECIPE if trial % 2 else one
        n = rng.randint(1, 4)
        requests = []
        calls = {h: [] for h in recipe.calls}
        for i in range(n):
            s = rng.randint(0, 60)
            requests.append(req(i, s, s + rng.randint(40, 90)))
            for h in recipe.calls:
                c = rng.randint(0, 120)
                calls[h].append(call(h, i, c, c + rng.randint(1, 15)))
        got = linked(recipe_links(requests, calls, recipe))
        brute = _every_assignment(requests, calls, recipe)
        if brute is None:  # no complete assignment at all: decided group by group
            continue
        # Never a link that some assignment does not hold.
        assert all(brute.get(k) == v for k, v in got.items())
        if recipe is one:  # one hop: exactly the links held by every assignment
            assert got == brute


def test_no_assignment_is_no_solution():
    # Each request holds one back and one side call, but the order puts the
    # only side call before the only back call.
    requests = [req(0, 0, 1000)]
    calls = {BACK: [call(BACK, 0, 600, 800)], SIDE: [call(SIDE, 0, 100, 300)]}
    result = recipe_links(requests, calls, RECIPE)
    assert result.links == ()
    assert set(statuses(result).values()) == {Status.NO_SOLUTION}


def test_a_group_touching_a_declared_loss_is_incomplete():
    result = recipe_links(*overlapping_pair(), RECIPE, lossy=[(900, 950)])
    assert result.links == ()
    assert set(statuses(result).values()) == {Status.INCOMPLETE}


def test_the_result_does_not_depend_on_input_order():
    requests, calls = [], {BACK: [], SIDE: []}
    for k in range(6):
        r, c = overlapping_pair(T=k * 700)  # neighbouring pairs overlap
        requests += [req(2 * k + i, x.start_us, x.end_us) for i, x in enumerate(r)]
        for hop in (BACK, SIDE):
            calls[hop] += [call(hop, 2 * k + i, x.start_us, x.end_us) for i, x in enumerate(c[hop])]
    first = recipe_links(requests, calls, RECIPE)
    rng = random.Random(3)
    for _ in range(5):
        rng.shuffle(requests)
        for hop in calls:
            rng.shuffle(calls[hop])
        assert recipe_links(requests, calls, RECIPE) == first


def calibration(n=40, missing_side=None):
    out = []
    for k in range(n):
        T = k * 10_000
        got = {BACK: [call(BACK, k, T + 100, T + 400)]}
        got[SIDE] = [] if k == missing_side else [call(SIDE, k, T + 500, T + 700)]
        out.append((req(k, T, T + 1000), got))
    return out


def test_the_recipe_is_learnt_with_its_order():
    recipe = learn_recipe("hop", calibration(), source="calibration")
    assert recipe == Recipe("hop", (BACK, SIDE), ((BACK, SIDE),), 40, "calibration")


def test_a_call_missing_once_in_calibration_is_out_of_the_recipe():
    recipe = learn_recipe("hop", calibration(missing_side=7), source="calibration")
    assert recipe.calls == (BACK,) and recipe.order == ()


def test_too_few_calibration_requests_give_no_recipe():
    assert learn_recipe("hop", calibration(n=19), source="calibration") is None
