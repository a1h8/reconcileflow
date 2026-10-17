"""Topology model: evidence ceilings from the declaration alone, refusals at load time.

The expected ceilings of the catalogue (P1) and the refusals (P2) were fixed in
the private phase 1 spec before the code existed; they are not to be tuned to a
result.
"""

import copy
import random
import tomllib
from fractions import Fraction
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation.topology import (
    D_MAX,
    Evidence,
    LinkKind,
    Reason,
    TopologyError,
    load,
    loads,
)

CATALOGUE = Path(__file__).parent / "fixtures" / "topologies"

# Variant -> (hardest hop, expected ceiling of that hop).
EXPECTED = {
    "t1a": (("relay", "ingress"), Evidence.DERIVED),
    "t1b": (("relay", "ingress"), Evidence.AUTHORED),
    "t2a": (("ingress", "app"), Evidence.AUTHORED),
    "t2b": (("ingress", "app"), Evidence.OBSERVED),
    "t2c": (("ingress", "app"), Evidence.DERIVED),
    "t3a": (("sidecar", "app"), Evidence.OBSERVED),
    "t3b": (("sidecar", "app"), Evidence.DERIVED),
    "t4": (("egress", "external"), Evidence.DERIVED),
    "t5a": (("queue", "consumer"), Evidence.AUTHORED),
    "t5b": (("queue", "consumer"), Evidence.UNRESOLVED),
    "t6": (("relay", "ingress"), Evidence.DERIVED),
}


def declaration(name):
    return tomllib.loads((CATALOGUE / f"{name}.toml").read_text())


def ceilings(topology):
    return {(c.src, c.dst): c for c in topology.hops}


def reasons(excinfo):
    return {r.reason for r in excinfo.value.refusals}


def refused(decl):
    with pytest.raises(TopologyError) as excinfo:
        load(decl)
    return excinfo


# --- P1: the catalogue -------------------------------------------------------


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_catalogue_hardest_hop_has_the_expected_ceiling(name):
    hop, expected = EXPECTED[name]
    topology = loads((CATALOGUE / f"{name}.toml").read_text())
    by_hop = ceilings(topology)
    assert by_hop[hop].evidence is expected
    assert min(c.evidence for c in by_hop.values()) is expected  # it is the hardest


def test_catalogue_link_kinds():
    kinds = {name: ceilings(load(declaration(name)))[EXPECTED[name][0]].link for name in EXPECTED}
    assert kinds == {
        "t1a": LinkKind.NESTED_TIMING,
        "t1b": LinkKind.SHARED_HEADER,
        "t2a": LinkKind.SHARED_HEADER,
        "t2b": LinkKind.SAME_CONNECTION,
        "t2c": LinkKind.NESTED_TIMING,
        "t3a": LinkKind.SAME_SOCKET,
        "t3b": LinkKind.NESTED_TIMING,
        "t4": LinkKind.NESTED_TIMING,
        "t5a": LinkKind.MESSAGE_KEY,
        "t5b": LinkKind.NONE,
        "t6": LinkKind.NESTED_TIMING,
    }


def test_fanout_bound_is_kept():
    hop = ceilings(load(declaration("t6")))["relay", "ingress"]
    assert hop.max_fanout == 2


def test_crossings_inside_points():
    topology = load(declaration("t1b"))
    crossing = {c.point: c for c in topology.crossings}
    # Only points with a hop in and a hop out are crossed.
    assert set(crossing) == {"ingress"}
    assert (crossing["ingress"].evidence, crossing["ingress"].link) == (
        Evidence.AUTHORED,
        LinkKind.PROPAGATED,
    )


@pytest.mark.parametrize(
    ("visibility", "propagates", "expected"),
    [
        ("kernel", {}, (Evidence.OBSERVED, LinkKind.INSIDE_PROCESS)),
        ("log", {}, (Evidence.DERIVED, LinkKind.LOGGED_TIMING)),
        ("app", {}, (Evidence.DERIVED, LinkKind.LOGGED_TIMING)),
        ("none", {}, (Evidence.UNRESOLVED, LinkKind.NONE)),
        ("log", {"traceparent": "app"}, (Evidence.AUTHORED, LinkKind.PROPAGATED)),
    ],
)
def test_crossing_ceiling_by_visibility(visibility, propagates, expected):
    decl = declaration("t1b")
    mid = decl["points"]["ingress"]
    mid["visibility"] = visibility
    mid["propagates"] = propagates
    if visibility == "none":
        del mid["timestamps"], mid["clock"]
    crossing = {c.point: c for c in load(decl).crossings}["ingress"]
    assert (crossing.evidence, crossing.link) == expected


def test_a_point_that_sets_the_header_does_not_carry_it_through():
    decl = declaration("t2a")
    decl["points"]["front"] = {
        "host": "node-z",
        "visibility": "kernel",
        "clock": "ref-1",
        "timestamps": "kernel_monotonic",
        "outbound": ["traceparent"],
    }
    decl["hops"].append({"from": "front", "to": "ingress"})
    decl["points"]["ingress"]["sets"] = ["traceparent"]
    decl["points"]["ingress"]["propagates"] = {"traceparent": "app"}
    crossing = {c.point: c for c in load(decl).crossings}["ingress"]
    assert crossing.link is LinkKind.INSIDE_PROCESS


# --- R3: what an agent reports on a side whose header the point replaced ------


def test_header_set_by_a_kernel_observed_point_is_not_wire_truth():
    # Measured (TB3): an application that sets its own traceparent wins on the
    # wire, while the agent's outbound record still carries the incoming id.
    decl = declaration("t2a")
    decl["points"]["ingress"]["sets"] = ["traceparent"]
    del decl["points"]["ingress"]["propagates"]
    hop = ceilings(load(decl))["ingress", "app"]
    assert (hop.evidence, hop.link) == (Evidence.OBSERVED, LinkKind.SAME_CONNECTION)


def test_header_set_and_logged_by_the_point_itself_is_authored():
    decl = declaration("t2a")
    decl["points"]["ingress"]["sets"] = ["traceparent"]
    decl["points"]["ingress"]["visibility"] = "app"
    del decl["points"]["ingress"]["propagates"]
    assert ceilings(load(decl))["ingress", "app"].link is LinkKind.SHARED_HEADER


# --- P2: refusals ------------------------------------------------------------


def test_density_above_the_bound_is_refused_for_a_derived_hop():
    decl = declaration("t2c")
    # Kernel clocks on both sides (1 us): 1e6/s x 1 us = 1 is at the bound, accepted.
    decl["hops"][0]["rate"] = 1_000_000
    hop = ceilings(load(decl))["ingress", "app"]
    assert hop.density == D_MAX == 1
    decl["hops"][0]["rate"] = 1_000_001
    assert reasons(refused(decl)) == {Reason.DENSITY_ABOVE_BOUND}


def test_density_window_is_the_coarsest_timestamp_or_the_measured_noise():
    decl = declaration("t2c")
    decl["hops"][0]["rate"] = 100_000
    decl["hops"][0]["noise_us"] = 500  # measured hop-to-hop noise
    excinfo = refused(decl)
    (refusal,) = excinfo.value.refusals
    assert refusal.reason is Reason.DENSITY_ABOVE_BOUND
    assert refusal.where == "ingress -> app"
    decl["hops"][0]["noise_us"] = 10
    assert ceilings(load(decl))["ingress", "app"].density == 1
    decl["points"]["app"]["timestamps"] = "log_ms"  # 1 ms on one side: 100
    assert reasons(refused(decl)) == {Reason.DENSITY_ABOVE_BOUND}


def test_density_does_not_apply_above_derived():
    decl = declaration("t2a")
    decl["hops"][0]["rate"] = 10**9
    decl["hops"][0]["noise_us"] = 10**6
    hop = ceilings(load(decl))["ingress", "app"]
    assert (hop.evidence, hop.density) == (Evidence.AUTHORED, None)


def test_density_is_not_computed_without_a_rate():
    assert ceilings(load(declaration("t2c")))["ingress", "app"].density is None


@pytest.mark.parametrize(
    ("name", "expect", "missing"),
    [
        ("t5b", "authored", {Reason.NO_MESSAGE_KEY, Reason.NO_SHARED_HEADER}),
        (
            "t2c",
            "observed",
            {Reason.MISSING_NAT_MAPPING, Reason.MISSING_SOCKET_COOKIE, Reason.CROSS_HOST_SOCKET},
        ),
        ("t3b", "observed", {Reason.CROSS_HOST_SOCKET, Reason.MISSING_TUPLE5}),
    ],
)
def test_ceiling_below_what_is_expected_is_refused_with_what_is_missing(name, expect, missing):
    decl = declaration(name)
    hop = EXPECTED[name][0]
    for h in decl["hops"]:
        if (h["from"], h["to"]) == hop:
            h["expect"] = expect
    excinfo = refused(decl)
    (refusal,) = excinfo.value.refusals
    assert refusal.reason is Reason.CEILING_BELOW_EXPECTED
    assert refusal.where == f"{hop[0]} -> {hop[1]}"
    assert set(refusal.missing) == missing


def test_missing_stream_id_under_pooling_is_named():
    decl = declaration("t2b")
    decl["points"]["app"]["inbound"] = ["tuple5"]
    decl["hops"][0]["expect"] = "observed"
    (refusal,) = refused(decl).value.refusals
    assert Reason.MISSING_STREAM_ID in refusal.missing


def test_expected_derived_names_what_timing_lacks():
    decl = declaration("t5b")
    decl["hops"][1]["expect"] = "derived"
    (refusal,) = refused(decl).value.refusals
    assert Reason.ASYNC_RELATION in refusal.missing
    decl = declaration("t2c")
    decl["points"]["app"]["clock"] = "ref-2"
    decl["hops"][0]["expect"] = "derived"
    (refusal,) = refused(decl).value.refusals
    assert Reason.NO_SHARED_CLOCK in refusal.missing
    decl["points"]["app"] = {"host": "node-b", "visibility": "none", "inbound": ["tuple5"]}
    (refusal,) = refused(decl).value.refusals
    assert Reason.NOT_TIMESTAMPED in refusal.missing


def test_an_expectation_met_is_accepted():
    decl = declaration("t2b")
    decl["hops"][0]["expect"] = "observed"
    assert ceilings(load(decl))["ingress", "app"].evidence is Evidence.OBSERVED
    decl["hops"][0]["expect"] = "derived"  # at least derived: observed satisfies it
    load(decl)


def test_untrusted_entry_whose_agent_propagates_is_refused():
    # Measured (TB1): the agent re-injects an outside traceparent the edge stripped.
    decl = declaration("t2a")
    decl["points"]["ingress"]["trusts_incoming"] = False
    assert reasons(refused(decl)) == {Reason.UNTRUSTED_ENTRY_PROPAGATED}
    decl["points"]["ingress"]["sets"] = ["traceparent"]  # the entry sets its own
    load(decl)


def test_untrusted_entry_propagating_by_its_application_is_accepted():
    decl = declaration("t2a")
    decl["points"]["ingress"]["trusts_incoming"] = False
    decl["points"]["ingress"]["propagates"] = {"traceparent": "app"}
    load(decl)


def test_baggage_carried_by_the_agent_is_refused():
    # Measured: neither agent carries baggage (0/2000).
    decl = declaration("t2a")
    decl["points"]["ingress"]["propagates"] = {"traceparent": "agent", "baggage": "agent"}
    assert reasons(refused(decl)) == {Reason.AGENT_BAGGAGE}
    decl["points"]["ingress"]["propagates"] = {"traceparent": "agent", "baggage": "app"}
    load(decl)


def test_agent_on_a_point_without_kernel_visibility_is_refused():
    decl = declaration("t2a")
    decl["points"]["ingress"]["visibility"] = "log"
    assert reasons(refused(decl)) == {Reason.AGENT_NOT_PRESENT}


def test_unbounded_fanout_is_refused():
    decl = declaration("t6")
    del decl["hops"][0]["max_fanout"]
    assert reasons(refused(decl)) == {Reason.FANOUT_UNBOUNDED}


def test_max_fanout_only_on_a_fanout_hop():
    decl = declaration("t2a")
    decl["hops"][0]["max_fanout"] = 2
    assert reasons(refused(decl)) == {Reason.INVALID_VALUE}


A, P, D = "ingress -> app", "app", ""
U, V, ID = Reason.UNKNOWN_FIELD, Reason.UNKNOWN_VALUE, Reason.UNKNOWN_IDENTIFIER
M, X = Reason.MISSING_FIELD, Reason.INVALID_VALUE
NOT_DECLARED = [
    (Reason.UNKNOWN_POINT, A, "from = 'ingress' is not a declared point"),
    (Reason.UNKNOWN_POINT, A, "to = 'app' is not a declared point"),
]


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda d: d["points"]["app"].update(colour="red"), [(U, P, "unknown field 'colour'")]),
        (lambda d: d["hops"][0].update(colour="red"), [(U, A, "unknown field 'colour'")]),
        (lambda d: d.update(extra=1), [(U, D, "unknown field 'extra'")]),
        (
            lambda d: d["points"]["app"].update(visibility="xray"),
            [(V, P, "visibility = 'xray', expected one of ['app', 'kernel', 'log', 'none']")],
        ),
        (
            lambda d: d["points"]["app"].update(timestamps="sundial"),
            [
                (
                    V,
                    P,
                    "timestamps = 'sundial', expected one of ['kernel_monotonic', 'log_ms',"
                    " 'log_s', 'network_sync', 'precise_sync']",
                )
            ],
        ),
        (
            lambda d: d["hops"][0].update(relation="rpc"),
            [(V, A, "relation = 'rpc', expected one of ['async', 'call', 'fanout']")],
        ),
        (
            lambda d: d["hops"][0].update(nat="pat"),
            [(V, A, "nat = 'pat', expected one of ['dnat', 'none', 'snat']")],
        ),
        (
            lambda d: d["hops"][0].update(nat_mapping="guess"),
            [
                (
                    V,
                    A,
                    "nat_mapping = 'guess', expected one of ['conntrack', 'logged_both_sides',"
                    " 'none']",
                )
            ],
        ),
        (
            lambda d: d["hops"][0].update(expect="ambiguous"),
            [
                (
                    V,
                    A,
                    "expect = 'ambiguous', expected one of ['authored', 'derived', 'observed',"
                    " 'unresolved']",
                )
            ],
        ),
        (
            lambda d: d["points"]["ingress"].update(propagates={"traceparent": "magic"}),
            [(V, "ingress", "propagates.traceparent = 'magic'")],
        ),
        (
            lambda d: d["points"]["app"].update(inbound=["traceid"]),
            [(ID, P, "inbound: unknown identifier 'traceid'")],
        ),
        (
            lambda d: d["points"]["app"].update(sets=["tuple5"]),
            [(ID, P, "sets: unknown identifier 'tuple5'")],
        ),
        (
            lambda d: d["points"]["ingress"].update(propagates={"tuple5": "app"}),
            [(ID, "ingress", "propagates: unknown identifier 'tuple5'")],
        ),
        (
            lambda d: d["hops"][0].update(to="nowhere"),
            [
                (
                    Reason.UNKNOWN_POINT,
                    "ingress -> nowhere",
                    "to = 'nowhere' is not a declared point",
                )
            ],
        ),
        (
            lambda d: d["hops"][0].update({"from": "nowhere"}),
            [(Reason.UNKNOWN_POINT, "nowhere -> app", "from = 'nowhere' is not a declared point")],
        ),
        (lambda d: d["hops"][0].pop("to"), [(M, "ingress -> ?", "to is required")]),
        (lambda d: d["hops"][0].pop("from"), [(M, "? -> app", "from is required")]),
        (lambda d: d["points"]["app"].pop("host"), [(M, P, "host is required")]),
        (lambda d: d["points"]["app"].pop("visibility"), [(M, P, "visibility is required")]),
        (lambda d: d["points"]["app"].pop("clock"), [(M, P, "clock is required")]),
        (lambda d: d["points"]["app"].pop("timestamps"), [(M, P, "timestamps is required")]),
        (lambda d: d.pop("points"), [(M, D, "points is required"), *NOT_DECLARED]),
        (
            lambda d: d["hops"].append(dict(d["hops"][0])),
            [(Reason.DUPLICATE_HOP, A, "declared twice")],
        ),
        (
            lambda d: d["hops"][0].update(to="ingress"),
            [(X, "ingress -> ingress", "a hop joins two different points")],
        ),
        (
            lambda d: d["hops"][0].update(pooling="yes"),
            [(X, A, "pooling must be true or false, got 'yes'")],
        ),
        (lambda d: d["hops"][0].update(rate=0), [(X, A, "rate must be an integer >= 1, got 0")]),
        (
            lambda d: d["hops"][0].update(rate=1.5),
            [(X, A, "rate must be an integer >= 1, got 1.5")],
        ),
        (
            lambda d: d["hops"][0].update(rate=True),
            [(X, A, "rate must be an integer >= 1, got True")],
        ),
        (
            lambda d: d["hops"][0].update(noise_us=-1),
            [(X, A, "noise_us must be an integer >= 0, got -1")],
        ),
        (lambda d: d["points"]["app"].update(host=3), [(X, P, "host must be a string, got 3")]),
        (
            lambda d: d["points"]["app"].update(inbound="tuple5"),
            [(X, P, "inbound must be a list of identifiers")],
        ),
        (
            lambda d: d["points"]["app"].update(inbound=["tuple5", "tuple5"]),
            [(X, P, "inbound lists an identifier twice")],
        ),
        (
            lambda d: d["points"]["app"].update(trusts_incoming="no"),
            [(X, P, "trusts_incoming must be true or false, got 'no'")],
        ),
        (
            lambda d: d["points"]["app"].update(propagates=["traceparent"]),
            [(X, P, "propagates must be a table: identifier = who copies it")],
        ),
        (
            lambda d: d["points"].update(bad="not a table"),
            [(X, "bad", "a point must be a table")],
        ),
        (lambda d: d.update(hops={"from": "a"}), [(X, D, "hops must be a list of tables")]),
        (lambda d: d["hops"].append("not a table"), [(X, D, "a hop must be a table")]),
        (
            lambda d: d.update(points={}),
            [(X, D, "points must be a non-empty table"), *NOT_DECLARED],
        ),
    ],
)
def test_each_refusal_says_what_and_where(mutate, expected):
    decl = declaration("t2a")
    mutate(decl)
    assert [(r.reason, r.where, r.detail) for r in refused(decl).value.refusals] == expected


def test_refusal_message_counts_and_lists_every_refusal():
    decl = declaration("t2a")
    decl["points"]["app"]["colour"] = "red"
    assert str(refused(decl).value) == "1 refusal: UNKNOWN_FIELD at app: unknown field 'colour'"
    decl["colour"] = "blue"
    assert str(refused(decl).value) == (
        "2 refusals: UNKNOWN_FIELD at declaration: unknown field 'colour';"
        " UNKNOWN_FIELD at app: unknown field 'colour'"
    )


def test_rule_refusals_say_what_and_where():
    cases = []
    decl = declaration("t2c")
    decl["hops"][0].update(rate=100_000, noise_us=500)
    cases.append(
        (decl, (Reason.DENSITY_ABOVE_BOUND, A, "density 50 > 1: timing cannot carry this rate"))
    )
    decl = declaration("t2c")
    decl["hops"][0]["expect"] = "observed"
    cases.append(
        (
            decl,
            (
                Reason.CEILING_BELOW_EXPECTED,
                A,
                "expected observed, at most derived; missing: missing_nat_mapping,"
                " missing_socket_cookie, cross_host_socket",
            ),
        )
    )
    decl = declaration("t2a")
    decl["points"]["ingress"]["trusts_incoming"] = False
    cases.append(
        (
            decl,
            (
                Reason.UNTRUSTED_ENTRY_PROPAGATED,
                "ingress",
                "the agent re-injects an outside traceparent (measured); set it here instead",
            ),
        )
    )
    decl = declaration("t2a")
    decl["points"]["ingress"]["propagates"]["baggage"] = "agent"
    cases.append(
        (decl, (Reason.AGENT_BAGGAGE, "ingress", "the agent does not carry baggage (measured)"))
    )
    decl = declaration("t2a")
    decl["points"]["ingress"]["visibility"] = "log"
    cases.append(
        (
            decl,
            (Reason.AGENT_NOT_PRESENT, "ingress", "an agent propagates on a point it does not see"),
        )
    )
    decl = declaration("t6")
    del decl["hops"][0]["max_fanout"]
    cases.append(
        (decl, (Reason.FANOUT_UNBOUNDED, "relay -> ingress", "a fanout hop needs max_fanout"))
    )
    decl = declaration("t2a")
    decl["hops"][0]["max_fanout"] = 2
    cases.append((decl, (X, A, "max_fanout applies to a fanout hop only")))
    for decl, expected in cases:
        assert [(r.reason, r.where, r.detail) for r in refused(decl).value.refusals] == [expected]


def test_toml_refusal_is_about_the_declaration():
    with pytest.raises(TopologyError) as excinfo:
        loads("[points\n")
    (refusal,) = excinfo.value.refusals
    assert (refusal.reason, refusal.where) == (X, D)
    assert refusal.detail.startswith("not TOML: ")


def test_numeric_lower_bounds_are_inclusive():
    decl = declaration("t2c")
    decl["hops"][0].update(rate=1, noise_us=0)
    assert ceilings(load(decl))["ingress", "app"].density == Fraction(1, 1_000_000)
    decl = declaration("t6")
    decl["hops"][0]["max_fanout"] = 1
    assert ceilings(load(decl))["relay", "ingress"].max_fanout == 1


def test_every_bad_point_and_hop_is_reported():
    decl = declaration("t2a")
    decl["points"]["bad1"] = "x"
    decl["points"]["bad2"] = "y"
    decl["hops"] += ["not a table", "nor this", dict(decl["hops"][0]), dict(decl["hops"][0])]
    decl["hops"].append({"from": "app", "to": "nowhere"})
    got = [(r.reason, r.where) for r in refused(decl).value.refusals]
    assert got == [
        (X, D),
        (X, D),
        (X, "bad1"),
        (X, "bad2"),
        (Reason.UNKNOWN_POINT, "app -> nowhere"),
        (Reason.DUPLICATE_HOP, A),
        (Reason.DUPLICATE_HOP, A),
    ]


def test_a_declaration_without_hops_has_points_and_no_ceiling():
    decl = declaration("t2a")
    del decl["hops"]
    topology = load(decl)
    assert [p.name for p in topology.points] == ["app", "ingress"]
    assert (topology.hops, topology.crossings) == ((), ())


def test_points_are_kept_normalised():
    ingress = {p.name: p for p in load(declaration("t2a")).points}["ingress"]
    assert ingress.inbound == frozenset({"tuple5", "h2_stream_id", "traceparent"})
    assert ingress.propagates == (("traceparent", "agent"),)
    assert (ingress.host, ingress.clock, ingress.trusts_incoming) == ("node-a", "ref-1", True)


def test_relation_defaults_to_call():
    assert ceilings(load(declaration("t2a")))["ingress", "app"].relation == "call"
    assert ceilings(load(declaration("t5a")))["queue", "consumer"].relation == "async"


def test_a_header_replaced_by_one_point_does_not_hide_the_other():
    # ingress sets its own request_id (kernel: not wire truth) but traceparent
    # still crosses the wire unchanged.
    decl = declaration("t2a")
    decl["points"]["ingress"]["sets"] = ["request_id"]
    decl["points"]["ingress"]["outbound"].append("request_id")
    decl["points"]["app"]["inbound"].append("request_id")
    assert ceilings(load(decl))["ingress", "app"].link is LinkKind.SHARED_HEADER


def test_async_hop_expected_observed_names_the_relation():
    decl = declaration("t5b")
    decl["points"]["queue"]["outbound"] = ["tuple5", "socket_cookie"]
    decl["points"]["consumer"]["inbound"] = ["tuple5", "socket_cookie"]
    decl["points"]["consumer"]["host"] = "node-q"
    decl["hops"][1]["expect"] = "observed"
    (refusal,) = refused(decl).value.refusals
    assert refusal.missing == (Reason.ASYNC_RELATION,)


def test_density_window_uses_the_upstream_timestamps_too():
    decl = declaration("t2c")
    decl["hops"][0]["rate"] = 100_000
    decl["points"]["ingress"]["timestamps"] = "log_ms"
    assert reasons(refused(decl)) == {Reason.DENSITY_ABOVE_BOUND}


@pytest.mark.parametrize(
    ("carried", "point", "name"),
    [("request_id", "ingress", "t2a"), ("message_id", "queue", "t5a")],
)
def test_crossing_propagated_by_each_carried_identifier(carried, point, name):
    decl = declaration(name)
    if name == "t2a":
        decl["points"]["front"] = {"host": "z", "visibility": "none", "outbound": ["request_id"]}
        decl["hops"].append({"from": "front", "to": "ingress"})
        ingress = decl["points"]["ingress"]
        ingress["inbound"] = ["request_id"]
        ingress["outbound"] = ["request_id"]
        ingress["propagates"] = {carried: "app"}
    crossing = {c.point: c for c in load(decl).crossings}[point]
    assert crossing.link is LinkKind.PROPAGATED


def test_a_fanout_bound_must_be_a_positive_integer():
    decl = declaration("t6")
    decl["hops"][0]["max_fanout"] = 0
    assert reasons(refused(decl)) == {Reason.INVALID_VALUE}


def test_toml_syntax_errors_are_refused():
    with pytest.raises(TopologyError) as excinfo:
        loads("[points\n")
    assert reasons(excinfo) == {Reason.INVALID_VALUE}


# --- P3: every fault at once ---------------------------------------------------


def test_every_fault_is_reported_not_only_the_first():
    decl = declaration("t6")
    del decl["hops"][0]["max_fanout"]
    decl["points"]["app"]["visibility"] = "xray"
    decl["points"]["ingress"]["propagates"]["baggage"] = "agent"
    decl["hops"][1]["to"] = "nowhere"
    excinfo = refused(decl)
    assert reasons(excinfo) == {
        Reason.FANOUT_UNBOUNDED,
        Reason.UNKNOWN_VALUE,
        Reason.AGENT_BAGGAGE,
        Reason.UNKNOWN_POINT,
    }
    assert "4 refusals" in str(excinfo.value)


def test_refusals_are_listed_in_a_stable_order():
    decl = declaration("t6")
    del decl["hops"][0]["max_fanout"]
    decl["points"]["app"]["visibility"] = "xray"
    first = [(r.reason, r.where) for r in refused(decl).value.refusals]
    shuffled = copy.deepcopy(decl)
    shuffled["points"] = dict(reversed(list(shuffled["points"].items())))
    shuffled["hops"].reverse()
    assert [(r.reason, r.where) for r in refused(shuffled).value.refusals] == first


# --- P4: order of the declaration ---------------------------------------------


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_same_topology_whatever_the_order_of_the_declaration(name):
    reference = load(declaration(name))
    rng = random.Random(name)
    for _ in range(5):
        decl = declaration(name)
        points = list(decl["points"].items())
        rng.shuffle(points)
        decl["points"] = dict(points)
        rng.shuffle(decl["hops"])
        for point in decl["points"].values():
            for side in ("inbound", "outbound"):
                if side in point:
                    rng.shuffle(point[side])
        assert load(decl) == reference


# --- P5: removing a capability never raises a ceiling -------------------------

IDS = ["traceparent", "request_id", "message_id", "tuple5", "h2_stream_id", "socket_cookie"]


@st.composite
def two_point_declarations(draw):
    def point(sides):
        p = {
            "host": draw(st.sampled_from(["h1", "h2"])),
            "visibility": draw(st.sampled_from(["kernel", "log", "app", "none"])),
        }
        if p["visibility"] != "none":
            p["clock"] = draw(st.sampled_from(["c1", "c2"]))
            p["timestamps"] = draw(
                st.sampled_from(["kernel_monotonic", "precise_sync", "network_sync", "log_ms"])
            )
        for side in sides:
            p[side] = draw(st.lists(st.sampled_from(IDS), unique=True))
        if "outbound" in sides:
            p["sets"] = draw(st.lists(st.sampled_from(["traceparent", "request_id"]), unique=True))
        return p

    hop = {
        "from": "a",
        "to": "b",
        "relation": draw(st.sampled_from(["call", "async"])),
        "nat": draw(st.sampled_from(["none", "snat", "dnat"])),
        "nat_mapping": draw(st.sampled_from(["none", "conntrack", "logged_both_sides"])),
        "pooling": draw(st.booleans()),
    }
    return {"points": {"a": point(["outbound"]), "b": point(["inbound"])}, "hops": [hop]}


def weakened(decl, change):
    """The same declaration with one capability taken away."""
    decl = copy.deepcopy(decl)
    a, b, hop = decl["points"]["a"], decl["points"]["b"], decl["hops"][0]
    kind, arg = change
    if kind == "drop_out" and arg in a["outbound"]:
        a["outbound"].remove(arg)
    elif kind == "drop_in" and arg in b["inbound"]:
        b["inbound"].remove(arg)
    elif kind == "no_mapping":
        hop["nat_mapping"] = "none"
    elif kind == "pooling":
        hop["pooling"] = True
    elif kind == "other_host":
        b["host"] = "elsewhere"
    elif kind == "other_clock" and "clock" in b:
        b["clock"] = "elsewhere"
    elif kind == "blind":
        b.update(visibility="none")
        b.pop("clock", None), b.pop("timestamps", None)
    return decl


changes = st.one_of(
    st.tuples(st.sampled_from(["drop_out", "drop_in"]), st.sampled_from(IDS)),
    st.tuples(
        st.sampled_from(["no_mapping", "pooling", "other_host", "other_clock", "blind"]),
        st.none(),
    ),
)


@settings(max_examples=2000, deadline=None)
@given(two_point_declarations(), changes)
def test_taking_a_capability_away_never_raises_a_ceiling(decl, change):
    before = ceilings(load(decl))["a", "b"].evidence
    after = ceilings(load(weakened(decl, change)))["a", "b"].evidence
    assert after <= before
