"""Links forced by the recipe (phase 4 spec 8.11): pure logic, no score."""

import sys

from test_jaeger_nesting import HARNESS, _module, request, span, trace_file

sys.path.insert(0, str(HARNESS))
fn = _module("forced_nesting")
jn = _module("jaeger_nesting")


def two_calls(k, start, back_at=100, side_at=500):
    """A request into 'front' that calls 'back' then 'side' (side after back ended)."""
    t = request(k, start, call_at=back_at, call_len=300)
    t.append(
        span(f"t{k:04d}", f"e{k}", f"s{k}", "front", "client", "/side.Call", start + side_at, 200)
    )
    return t


def calm(n=100):
    """Separated requests (first half): the recipe is learnt there."""
    return [two_calls(k, k * 10_000, back_at=100 + k % 7) for k in range(n)]


def server(k, start, total=1000):
    t = f"t{k:04d}"
    return span(t, f"s{k}", None, "front", "server", "GET /", start, total)


def call(k, sid, op, start, length):
    return span(f"t{k:04d}", sid, f"s{k}", "front", "client", op, start, length)


def links(tmp_path, traces, **kw):
    trace_file(tmp_path, "a.json", traces)
    spans = jn.load(tmp_path)
    found = jn.joins(spans)
    first, _ = jn.halves(spans)
    linked, stats = fn.forced_links(spans, found, first, "truth", **kw)
    by_key = {s["key"]: s for s in spans}
    named = {
        (by_key[p]["sid"], found[int(j[1:-1])][1][2]): by_key[c]["sid"]
        for (j, p), c in linked.items()
    }
    return named, stats


def overlapping_pair(T, k=900, drop_side_b=False):
    """A=[T, T+1000], B=[T+50, T+1050]. x1 starts before B: only A's.
    y1 starts at T+420, before x2 ends (T+450): only after x1, so only A's."""
    a = [
        server(k, T),
        call(k, "x1", "/back.Call", T + 20, 280),
        call(k, "y1", "/side.Call", T + 420, 200),
    ]
    b = [server(k + 1, T + 50), call(k + 1, "x2", "/back.Call", T + 150, 300)]
    if not drop_side_b:
        b.append(call(k + 1, "y2", "/side.Call", T + 500, 200))
    return [a, b]


def test_order_of_the_recipe_forces_both_requests(tmp_path):
    named, _ = links(tmp_path, calm() + overlapping_pair(2_000_000))
    assert named[("s900", "/back.Call")] == "x1"
    assert named[("s900", "/side.Call")] == "y1"
    assert named[("s901", "/back.Call")] == "x2"
    assert named[("s901", "/side.Call")] == "y2"


def test_two_interchangeable_requests_are_refused(tmp_path):
    T = 2_000_000
    twins = []
    for k, (x, y) in ((900, ("x1", "y1")), (901, ("x2", "y2"))):
        twins.append(
            [
                server(k, T),
                call(k, x, "/back.Call", T + 100, 300),
                call(k, y, "/side.Call", T + 500, 200),
            ]
        )
    named, stats = links(tmp_path, calm() + twins)
    assert not any(p in ("s900", "s901") for p, _ in named)
    assert stats["refused_groups"]["no_forced_link"] >= 1


def test_a_missing_call_refuses_the_whole_group(tmp_path):
    named, stats = links(tmp_path, calm() + overlapping_pair(2_000_000, drop_side_b=True))
    assert not any(p in ("s900", "s901") for p, _ in named)
    assert stats["refused_groups"]["incomplete"] >= 1


def test_a_call_type_missing_once_in_calibration_is_out_of_the_recipe(tmp_path):
    traces = calm()
    traces[10] = [s for s in traces[10] if s["spanID"] != "e10"]
    trace_file(tmp_path, "a.json", traces)
    spans = jn.load(tmp_path)
    first, _ = jn.halves(spans)
    recipes = fn.recipe(spans, jn.joins(spans), first, "truth")
    ((_, r),) = recipes.items()
    assert [c[2] for c in r["calls"]] == ["/back.Call"]


def test_a_chain_of_single_candidates_frees_the_next(tmp_path):
    T = 2_000_000
    chain = []
    # back calls: x1 only in A; x2 in A and B; x3 in B and C -> A x1, B x2, C x3.
    for i, (k, start) in enumerate(((900, T), (901, T + 200), (902, T + 400))):
        chain.append(
            [
                server(k, start, total=700),
                call(k, f"x{i + 1}", "/back.Call", start + 150, 100),
                call(k, f"y{i + 1}", "/side.Call", start + 600, 50),
            ]
        )
    named, _ = links(tmp_path, calm() + chain)
    assert [named.get((f"s{k}", "/back.Call")) for k in (900, 901, 902)] == ["x1", "x2", "x3"]


def test_the_search_budget_refuses_rather_than_guesses(tmp_path):
    named, stats = links(tmp_path, calm() + overlapping_pair(2_000_000), budget=1)
    assert not any(p in ("s900", "s901") for p, _ in named)
    assert stats["refused_groups"]["budget"] >= 1
