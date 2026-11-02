"""Correlation as a stream, step B (observed links): a hop whose two ends both
record the connection, and the stream on a pooled one, has a second signal
next to the propagated identifier. One intent per test: two signals that agree
corroborate a link; two that disagree make no link and say why; a signal
missing on one end leaves the link standing but traced, never silent; the
connection alone links a hop without a header; a connection seen again after
its group closed starts a new group.
"""

import random

import pytest

from reconcileflow.correlation.exact import Collision, Link, Observation, observed_links
from reconcileflow.correlation.flow import (
    Advance,
    Corroboration,
    FlowConfig,
    Kind,
    Seen,
    Status,
    replay_flow,
)
from reconcileflow.correlation.topology import Evidence, load

CONN = "tuple5+h2_stream_id"
SOURCES = ("agent-c", "agent-p", "agent-s")
CFG = FlowConfig(sources=SOURCES, window_us=500)
EDGE = "proxy -> server"  # the hop the tests act on


def chain(pooling=True, nat="none", server_inbound=("traceparent", "tuple5", "h2_stream_id")):
    """client -> proxy -> server on one host, each end seen by an agent."""

    def point(**sides):
        return {
            "host": "h",
            "visibility": "kernel",
            "clock": "c",
            "timestamps": "kernel_monotonic",
        } | {side: list(keys) for side, keys in sides.items()}

    keys = ["traceparent", "tuple5", "h2_stream_id"]
    hop = {"pooling": pooling, "nat": nat} | ({"nat_mapping": "conntrack"} if nat != "none" else {})
    return load(
        {
            "points": {
                "client": point(outbound=keys),
                "proxy": point(inbound=keys, outbound=keys)
                | {"propagates": {"traceparent": "agent"}},
                "server": point(inbound=server_inbound),
            },
            "hops": [
                {"from": "client", "to": "proxy"} | hop,
                {"from": "proxy", "to": "server"} | hop,
            ],
        }
    )


CHAIN = chain()


def hop_of(topology, where):
    return next(h for h in topology.hops if f"{h.src} -> {h.dst}" == where)


def out(seq, **keys):
    return Observation("agent-p", seq, "proxy", "outbound", keys)


def inb(seq, **keys):
    return Observation("agent-s", seq, "server", "inbound", keys)


def conn(n, stream=1):
    return {"tuple5": f"10.0.0.1:{40000 + n}>10.0.0.2:443", "h2_stream_id": str(stream)}


def run(*observations, at=0, config=CFG):
    journal = [Seen(o, at) for o in observations] + [Advance(s, 10_000) for s in config.sources]
    return replay_flow(CHAIN, journal, config)


def at_edge(result):
    """Final decisions at the acted-on hop, by observation."""
    return {
        d.business_key[1]: d
        for d in result.effective().values()
        if d.business_key[0] == EDGE and d.status is Status.FINAL
    }


# -- topology: the observed signal of a hop ---------------------------------------


def test_a_pooled_hop_seen_by_both_ends_has_the_connection_and_stream_as_second_signal():
    hop = hop_of(CHAIN, EDGE)
    assert hop.observed == ("tuple5", "h2_stream_id")
    assert (hop.evidence, hop.key) == (Evidence.AUTHORED, "traceparent")  # the ceiling is unchanged


def test_a_hop_without_pooling_is_observed_on_the_connection_alone():
    assert hop_of(chain(pooling=False), EDGE).observed == ("tuple5",)


def test_a_hop_through_nat_has_no_observed_signal_even_with_a_mapping():
    assert hop_of(chain(nat="dnat"), EDGE).observed is None


def test_a_hop_whose_receiving_end_does_not_record_the_stream_has_no_observed_signal():
    assert hop_of(chain(server_inbound=("traceparent", "tuple5")), EDGE).observed is None


# -- the observed join -------------------------------------------------------------


def test_the_observed_join_links_on_the_connection_and_the_stream_together():
    result = observed_links(
        CHAIN, [out(0, **conn(1, 3)), inb(0, **conn(1, 3)), inb(1, **conn(1, 5))]
    )
    value = "10.0.0.1:40001>10.0.0.2:443\x1f3"
    assert result.links == (Link(EDGE, CONN, value, ("agent-p", 0), ("agent-s", 0)),)
    assert result.unlinked == ((EDGE, ("agent-s", 1)),)


def test_an_observation_missing_a_part_of_the_connection_key_is_outside_the_observed_join():
    result = observed_links(CHAIN, [out(0, tuple5=conn(1)["tuple5"]), inb(0, **conn(1))])
    assert result.links == ()
    assert result.unlinked == ((EDGE, ("agent-s", 0)),)  # the outbound one is absent, not unlinked


def test_one_connection_and_stream_on_two_requests_is_a_collision_of_the_observed_join():
    result = observed_links(CHAIN, [out(0, **conn(1)), inb(0, **conn(1)), inb(1, **conn(1))])
    value = "10.0.0.1:40001>10.0.0.2:443\x1f1"
    assert result.collisions == (
        Collision(EDGE, CONN, value, (("agent-p", 0),), (("agent-s", 0), ("agent-s", 1))),
    )


def test_the_observed_join_counts_a_redelivery_once_and_ignores_the_order():
    observations = [out(0, **conn(1)), inb(0, **conn(1)), out(0, **conn(1))]
    result = observed_links(CHAIN, observations)
    assert result.duplicates_dropped == 1
    assert observed_links(CHAIN, observations[::-1]) == result


# -- two signals on a hop ------------------------------------------------------------


def test_two_signals_designating_the_same_pair_corroborate_the_link():
    decisions = at_edge(
        run(out(0, traceparent="t1", **conn(1)), inb(0, traceparent="t1", **conn(1)))
    )
    for d in decisions.values():
        assert (d.kind, d.key, d.evidence, d.corroboration) == (
            Kind.LINKED,
            "traceparent",
            Evidence.AUTHORED,
            Corroboration.CORROBORATED,
        )
        assert d.missing_on == ()


def test_two_signals_designating_different_partners_make_no_link():
    # The id says proxy-out 0 went to server-in 0; the connection says server-in 1.
    decisions = at_edge(
        run(
            out(0, traceparent="t1", **conn(1)),
            inb(0, traceparent="t1", **conn(2)),
            inb(1, traceparent="t2", **conn(1)),
        )
    )
    assert all(d.kind is Kind.CONFLICTED for d in decisions.values())
    sender = decisions["agent-p", 0]
    assert sender.corroboration is Corroboration.SIGNAL_OTHER_PARTNER
    assert sender.partners == (("agent-s", 0), ("agent-s", 1))
    for receiver in (decisions["agent-s", 0], decisions["agent-s", 1]):
        assert receiver.corroboration is Corroboration.SIGNAL_UNMATCHED
        assert receiver.partners == (("agent-p", 0),)


def test_a_second_signal_recorded_on_both_ends_with_other_values_contradicts_the_link():
    decisions = at_edge(
        run(out(0, traceparent="t1", **conn(1)), inb(0, traceparent="t1", **conn(2)))
    )
    assert {(d.kind, d.corroboration) for d in decisions.values()} == {
        (Kind.CONFLICTED, Corroboration.SIGNAL_UNMATCHED)
    }


def test_a_second_signal_missing_on_one_end_leaves_the_link_traced_not_silent():
    receiver = inb(0, traceparent="t1", tuple5=conn(1)["tuple5"])  # no stream id recorded
    decisions = at_edge(run(out(0, traceparent="t1", **conn(1)), receiver))
    for d in decisions.values():
        assert (d.kind, d.evidence, d.corroboration) == (
            Kind.LINKED,
            Evidence.AUTHORED,
            Corroboration.SIGNAL_ABSENT,
        )
        assert d.missing_on == (("agent-s", 0),)


def test_the_connection_alone_links_a_hop_whose_header_was_not_recorded():
    decisions = at_edge(run(out(0, **conn(1)), inb(0, **conn(1))))
    for d in decisions.values():
        assert (d.kind, d.key, d.evidence, d.corroboration) == (
            Kind.LINKED,
            CONN,
            Evidence.OBSERVED,
            Corroboration.SIGNAL_ABSENT,
        )
        assert d.value == "10.0.0.1:40001>10.0.0.2:443\x1f1"
        assert d.missing_on == (("agent-p", 0), ("agent-s", 0))


def test_a_connection_link_contradicted_by_two_different_ids_makes_no_link():
    decisions = at_edge(
        run(out(0, traceparent="t1", **conn(1)), inb(0, traceparent="t9", **conn(1)))
    )
    assert {(d.kind, d.corroboration) for d in decisions.values()} == {
        (Kind.CONFLICTED, Corroboration.SIGNAL_UNMATCHED)
    }


def test_a_collision_on_the_connection_is_a_collision_even_where_the_id_links():
    decisions = at_edge(
        run(
            out(0, traceparent="t1", **conn(1)),
            inb(0, traceparent="t1", **conn(1)),
            inb(1, traceparent="t2", **conn(1)),
        )
    )
    assert decisions["agent-p", 0].kind is Kind.COLLIDED
    assert decisions["agent-p", 0].key == CONN


def test_nothing_linked_on_either_signal_is_unlinked():
    decisions = at_edge(run(out(0, traceparent="t1", **conn(1))))
    assert (decisions["agent-p", 0].kind, decisions["agent-p", 0].corroboration) == (
        Kind.UNLINKED,
        None,
    )


def test_a_hop_with_only_the_connection_declared_links_as_observed_without_corroboration():
    topology = chain(server_inbound=("tuple5", "h2_stream_id"))  # the server logs no header
    hop = hop_of(topology, EDGE)
    assert (hop.key, hop.observed) == (None, ("tuple5", "h2_stream_id"))
    journal = [Seen(out(0, traceparent="t1", **conn(1)), 0), Seen(inb(0, **conn(1)), 10)]
    result = replay_flow(topology, [*journal, *(Advance(s, 10_000) for s in SOURCES)], CFG)
    for d in at_edge(result).values():
        assert (d.kind, d.key, d.evidence, d.corroboration) == (
            Kind.LINKED,
            CONN,
            Evidence.OBSERVED,
            None,
        )


def test_a_join_with_one_declared_signal_carries_no_corroboration():
    client = Observation("agent-c", 0, "client", "outbound", {"traceparent": "t1", **conn(7)})
    proxy_in = Observation("agent-p", 9, "proxy", "inbound", {"traceparent": "t1", **conn(7)})
    result = run(client, proxy_in, out(0, traceparent="t1", **conn(1)))
    crossing = result.effective()["proxy", ("agent-p", 9)]
    assert (crossing.kind, crossing.evidence, crossing.corroboration) == (
        Kind.LINKED,
        Evidence.AUTHORED,
        None,
    )


# -- groups, finality ---------------------------------------------------------------


def test_observations_sharing_only_a_connection_close_in_one_group():
    result = run(out(0, traceparent="t1", **conn(1)), inb(0, **conn(1)))
    assert [d.observations for d in result.dossiers] == [(("agent-p", 0), ("agent-s", 0))]


def test_observations_on_different_connections_close_in_different_groups():
    result = run(out(0, **conn(1)), inb(0, **conn(1)), out(1, **conn(2)), inb(1, **conn(2)))
    assert [d.observations for d in result.dossiers] == [
        (("agent-p", 0), ("agent-s", 0)),
        (("agent-p", 1), ("agent-s", 1)),
    ]


def test_a_connection_seen_again_after_its_group_closed_opens_a_new_group():
    first = [Seen(out(0, **conn(1)), 0), Seen(inb(0, **conn(1)), 10)]
    later = [Seen(out(1, **conn(1)), 5000), Seen(inb(1, **conn(1)), 5010)]
    progress = [Advance(s, 2000) for s in SOURCES]
    result = replay_flow(
        CHAIN, [*first, *progress, *later, *(Advance(s, 10**5) for s in SOURCES)], CFG
    )
    assert all(d.kind is not Kind.VALUE_AFTER_FINAL for d in result.decisions)
    assert {d.business_key[1]: d.kind for d in at_edge(result).values()} == {
        ("agent-p", 0): Kind.LINKED,
        ("agent-s", 0): Kind.LINKED,
        ("agent-p", 1): Kind.LINKED,
        ("agent-s", 1): Kind.LINKED,
    }
    assert len(result.dossiers) == 2


def test_an_id_seen_again_after_its_group_closed_is_still_told():
    first = [
        Seen(out(0, traceparent="t1", **conn(1)), 0),
        Seen(inb(0, traceparent="t1", **conn(1)), 10),
    ]
    later = Seen(inb(1, traceparent="t1", **conn(2)), 5000)
    progress = [Advance(s, 2000) for s in SOURCES]
    result = replay_flow(
        CHAIN, [*first, *progress, later, *(Advance(s, 10**5) for s in SOURCES)], CFG
    )
    assert result.effective()["", ("agent-s", 1)].kind is Kind.VALUE_AFTER_FINAL


def test_a_contradicting_end_arriving_second_turns_the_provisional_decision_into_a_conflict():
    journal = [
        Seen(out(0, traceparent="t1", **conn(1)), 0),
        Seen(inb(0, traceparent="t1", **conn(2)), 10),
    ]
    result = replay_flow(CHAIN, [*journal, *(Advance(s, 10**4) for s in SOURCES)], CFG)
    chain_of = [d for d in result.decisions if d.business_key == (EDGE, ("agent-p", 0))]
    assert [(d.kind, d.status) for d in chain_of] == [
        (Kind.UNLINKED, Status.PROVISIONAL),
        (Kind.CONFLICTED, Status.PROVISIONAL),
        (Kind.CONFLICTED, Status.FINAL),
    ]


def test_the_corroboration_is_part_of_the_decision_fingerprint():
    corroborated = run(out(0, traceparent="t1", **conn(1)), inb(0, traceparent="t1", **conn(1)))
    absent = run(out(0, traceparent="t1", **conn(1)), inb(0, traceparent="t1"))
    ids = {d.decision_id for d in at_edge(corroborated).values()}
    assert ids.isdisjoint(d.decision_id for d in at_edge(absent).values())


def test_permutations_within_the_lateness_end_in_the_same_final_decisions():
    config = FlowConfig(sources=SOURCES, window_us=500, lateness_us=2000)
    rng = random.Random(0)
    entries = []
    for i in range(30):
        tid, c = f"t{i}", conn(i % 4, stream=i)
        if rng.random() < 0.2:
            tid = f"x{i}"  # the receiver reports another id: a conflict
        receiver = {"traceparent": tid, **c}
        if rng.random() < 0.2:
            del receiver["h2_stream_id"]  # the receiver missed the stream: absent
        entries += [
            Seen(out(i, traceparent=f"t{i}", **c), i * 1000),
            Seen(inb(i, **receiver), i * 1000 + 50),
        ]

    def finals_of(order):
        jitter = {id(e): rng.random() * 2000 for e in order}
        journal = sorted(order, key=lambda e: e.at + jitter[id(e)])
        result = replay_flow(CHAIN, [*journal, *(Advance(s, 10**6) for s in SOURCES)], config)
        return {
            k: (d.kind, d.key, d.value, d.partners, d.evidence, d.corroboration, d.missing_on)
            for k, d in result.effective().items()
        }

    first = finals_of(entries)
    assert {k for k, *_ in first.values()} >= {Kind.LINKED, Kind.CONFLICTED}
    for _ in range(20):
        assert finals_of(entries) == first


# -- series for observation (J4) ------------------------------------------------------


def test_signal_counts_give_the_final_decisions_by_join_and_corroboration():
    result = run(
        out(0, traceparent="t1", **conn(1)),
        inb(0, traceparent="t1", **conn(1)),
        out(1, traceparent="t2", **conn(2)),
        inb(1, traceparent="t2"),
        out(2, traceparent="t3", **conn(3)),
        inb(2, traceparent="t3", **conn(4)),
    )
    assert result.signal_counts() == {
        (EDGE, Corroboration.CORROBORATED): 2,
        (EDGE, Corroboration.SIGNAL_ABSENT): 2,
        (EDGE, Corroboration.SIGNAL_UNMATCHED): 2,
    }


@pytest.mark.parametrize("corroboration", list(Corroboration))
def test_every_corroboration_has_a_stable_name(corroboration):
    assert corroboration.value == corroboration.name
