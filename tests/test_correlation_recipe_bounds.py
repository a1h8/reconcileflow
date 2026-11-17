"""Bounds of the recipe's rules (phase 4 spec 8.12, 8.17, 8.22), each test
written for a mutant that survived the mutation audit of 2026-11-17: exact
edges of the size window and signature, of a declared loss, and of what the
size rules are learnt from."""

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.recipe import (
    Lattice,
    Recipe,
    Sizes,
    learn_lattice,
    learn_window,
    recipe_links,
)
from reconcileflow.correlation.timing import Timed

HOP = "hop -> back"
ONE = Recipe("hop", (HOP,), (), 0, "declared")


def req(seq, start, end):
    return Timed("in", seq, "hop", "inbound", start, end)


def call(seq, start, end):
    return Timed(HOP, seq, "hop", "outbound", start, end)


def linked(result):
    return {x.upstream: x.downstream for x in result.links}


# -- T1: a signature whose lowest gap is not a multiple of half its step -------------


def test_the_signature_holds_exactly_the_gaps_on_its_step_from_its_low_end():
    lattice = Lattice(-30, 60, 22)
    assert {g for g in range(-100, 100) if g in lattice} == {-30, -8, 14, 36, 58}


# -- T2: the size window includes both of its ends ----------------------------------


def test_a_true_link_at_either_end_of_the_window_is_kept():
    # Both calls lie inside both requests; only sizes tell them apart, and
    # each request's own gap sits exactly on one end of the window.
    requests = [req(0, 0, 1000), req(1, 10, 1010)]
    calls = [call(0, 100, 400), call(1, 200, 500)]
    sizes = Sizes(
        inbound={("in", 0): 100, ("in", 1): 200},
        outbound={HOP: {(HOP, 0): 80, (HOP, 1): 200}},  # gaps 20 (high end), 0 (low end)
        windows={HOP: (0, 20)},
    )
    result = recipe_links(requests, {HOP: calls}, ONE, sizes=sizes)
    assert linked(result) == {("in", 0): (HOP, 0), ("in", 1): (HOP, 1)}


# -- T3: a group that meets a declared loss at an exact instant is refused --------------


def test_a_loss_that_starts_where_the_group_ends_refuses_it():
    result = recipe_links([req(0, 0, 1000)], {HOP: [call(0, 100, 400)]}, ONE, lossy=[(1000, 1200)])
    assert linked(result) == {}
    assert {u.status for u in result.undecided} == {Status.INCOMPLETE}


def test_a_loss_that_ends_where_the_group_starts_refuses_it():
    result = recipe_links([req(0, 0, 1000)], {HOP: [call(0, 100, 400)]}, ONE, lossy=[(-200, 0)])
    assert linked(result) == {}
    assert {u.status for u in result.undecided} == {Status.INCOMPLETE}


# -- T4: what the window and the signature are learnt from ---------------------------


def gaps_links(gaps):
    """One link per gap: request i answered 1000 + gap, its call 1000."""
    links = [(("in", i), (HOP, i)) for i in range(len(gaps))]
    inbound = {("in", i): 1000 + g for i, g in enumerate(gaps)}
    outbound = {(HOP, i): 1000 for i in range(len(gaps))}
    return links, inbound, outbound


def test_a_window_needs_its_minimum_of_links_and_no_more():
    assert learn_window(*gaps_links([0, 22] * 100)) == (-32, 54)  # 200 links: enough
    assert learn_window(*gaps_links([0, 22] * 99 + [0])) is None  # 199


def test_a_window_spread_over_exactly_its_span_is_learnt():
    assert learn_window(*gaps_links([0, 256] * 100)) == (-32, 288)
    assert learn_window(*gaps_links([0, 257] * 100)) is None


def test_the_spread_of_a_window_is_measured_between_its_gaps_not_from_zero():
    # Gaps far from zero but close to each other: a window.
    assert learn_window(*gaps_links([1000, 1100] * 100)) == (968, 1132)


def test_a_signature_ignores_links_whose_sizes_are_not_known():
    links, inbound, outbound = gaps_links([0, 22, 44] * 100)
    links.append((("in", "unknown"), (HOP, 0)))  # its request's size is not known
    assert learn_lattice(links, inbound, outbound) == Lattice(-22, 66, 22)


def test_a_signature_needs_a_step_of_at_least_eight_bytes():
    assert learn_lattice(*gaps_links([0, 8] * 100)) == Lattice(-32, 40, 8)
    assert learn_lattice(*gaps_links([0, 7] * 100)) is None


def test_the_signature_ends_on_its_step_inside_the_window():
    # Window (-32, 76) from gaps {0, 22, 44}: on the step of 22 from 0, the
    # highest gap inside it is 66, not past the window's end.
    lattice = learn_lattice(*gaps_links([0, 22, 44] * 100))
    assert (lattice.low, lattice.high) == (-22, 66)
