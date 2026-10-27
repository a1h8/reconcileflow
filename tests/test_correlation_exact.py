"""Exact path: links by equal propagated identifiers, collisions, redeliveries.

Criteria E1-E4 were fixed in the private phase 2 spec before the code existed.
"""

import random
import tomllib
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation.exact import Collision, Link, Observation, exact_links
from reconcileflow.correlation.topology import load

CATALOGUE = Path(__file__).parent / "fixtures" / "topologies"


def topology(name):
    return load(tomllib.loads((CATALOGUE / f"{name}.toml").read_text()))


T1B = topology("t1b")  # relay -(request_id)-> ingress -(crossing: traceparent)-> -> app


def chain(n, source="obs"):
    """n requests seen at every side of T1b, each with its own ids."""
    out, seq = [], 0
    for i in range(n):
        rid, tid = f"rid-{i}", f"tid-{i}"
        for point, side, keys in (
            ("relay", "outbound", {"request_id": rid}),
            ("ingress", "inbound", {"request_id": rid, "traceparent": tid}),
            ("ingress", "outbound", {"traceparent": tid}),
            ("app", "inbound", {"traceparent": tid}),
        ):
            out.append(Observation(source, seq, point, side, keys))
            seq += 1
    return out


def pairs(result, where):
    return {(link.upstream, link.downstream) for link in result.links if link.where == where}


def test_every_request_is_linked_on_every_authored_join():
    result = exact_links(T1B, chain(3))
    assert {link.where for link in result.links} == {
        "relay -> ingress",
        "ingress",
        "ingress -> app",
    }
    assert len(result.links) == 9
    assert result.collisions == () and result.unlinked == () and result.duplicates_dropped == 0
    first = [link for link in result.links if link.value == "rid-0"]
    assert first == [Link("relay -> ingress", "request_id", "rid-0", ("obs", 0), ("obs", 1))]
    crossing = [link for link in result.links if link.where == "ingress" and link.value == "tid-0"]
    assert crossing == [Link("ingress", "traceparent", "tid-0", ("obs", 1), ("obs", 2))]


def test_a_hop_without_an_authored_ceiling_produces_no_link():
    t2b = topology("t2b")
    obs = [
        Observation("s", 0, "ingress", "outbound", {"tuple5": "a"}),
        Observation("s", 1, "app", "inbound", {"tuple5": "a"}),
    ]
    assert exact_links(t2b, obs).links == ()


def test_an_observation_without_the_join_key_does_not_take_part():
    obs = chain(1)
    obs[3] = Observation("obs", 3, "app", "inbound", {})
    result = exact_links(T1B, obs)
    assert pairs(result, "ingress -> app") == set()
    assert result.unlinked == (("ingress -> app", ("obs", 2)),)


def test_an_observation_with_no_partner_is_listed_where_it_waits():
    obs = [o for o in chain(2) if not (o.point == "relay" and o.keys["request_id"] == "rid-1")]
    result = exact_links(T1B, obs)
    assert result.unlinked == (("relay -> ingress", ("obs", 5)),)


def test_one_value_seen_twice_on_one_side_is_a_collision_and_links_nothing():
    obs = chain(2)
    # request 1's relay record carries request 0's id: two upstream records share it.
    obs[4] = Observation("obs", 4, "relay", "outbound", {"request_id": "rid-0"})
    result = exact_links(T1B, obs)
    assert result.collisions == (
        Collision(
            "relay -> ingress", "request_id", "rid-0", (("obs", 0), ("obs", 4)), (("obs", 1),)
        ),
    )
    assert not [link for link in result.links if link.value == "rid-0"]
    # request 1's ingress record lost its partner, and is listed as waiting.
    assert ("relay -> ingress", ("obs", 5)) in result.unlinked


def test_a_collision_on_the_downstream_side_is_reported_too():
    obs = chain(2)
    obs[7] = Observation("obs", 7, "app", "inbound", {"traceparent": "tid-0"})
    (collision,) = exact_links(T1B, obs).collisions
    assert (collision.where, collision.upstream, collision.downstream) == (
        "ingress -> app",
        (("obs", 2),),
        (("obs", 3), ("obs", 7)),
    )


def test_a_redelivered_observation_is_dropped_not_a_collision():
    obs = chain(2)
    result = exact_links(T1B, [*obs, obs[0], obs[5], obs[5]])
    assert result.collisions == ()
    assert result.duplicates_dropped == 3
    assert result.links == exact_links(T1B, obs).links


def test_same_identity_with_different_content_is_refused():
    obs = chain(1)
    clash = Observation("obs", 0, "relay", "outbound", {"request_id": "other"})
    with pytest.raises(
        ValueError, match="observation \\('obs', 0\\) delivered with different content"
    ):
        exact_links(T1B, [*obs, clash])


@pytest.mark.parametrize(
    ("observation", "reason"),
    [
        (Observation("s", 0, "nowhere", "inbound", {}), "unknown point 'nowhere'"),
        (Observation("s", 0, "app", "sideways", {}), "side must be inbound or outbound"),
        (Observation("s", 0, "app", "inbound", {"traceid": "x"}), "unknown identifier 'traceid'"),
        (Observation("s", 0, "app", "inbound", {"traceparent": ""}), "empty value"),
        (Observation("s", 0, "app", "inbound", {"traceparent": 7}), "empty value"),
        (Observation("s", "0", "app", "inbound", {}), "seq must be an integer"),
        (Observation("s", True, "app", "inbound", {}), "seq must be an integer"),
        (Observation(3, 0, "app", "inbound", {}), "source must be a string"),
    ],
)
def test_invalid_observations_are_refused(observation, reason):
    with pytest.raises(ValueError, match=reason):
        exact_links(T1B, [observation])


# --- E1: injected duplicate identifiers -----------------------------------------


@settings(max_examples=300, deadline=None)
@given(st.integers(2, 40), st.data())
def test_injected_duplicates_all_collide_and_never_link(n, data):
    obs = chain(n)
    victims = data.draw(st.lists(st.integers(1, n - 1), unique=True, min_size=1))
    injected = set()
    for i in victims:
        # A donor keeps its own record: otherwise its original id would sit once
        # on each side and make a real wrong link, not a collision.
        donor = data.draw(st.sampled_from([j for j in range(i) if j not in victims]))
        k = 4 * i  # request i's relay record takes request `donor`'s id
        obs[k] = Observation("obs", k, "relay", "outbound", {"request_id": f"rid-{donor}"})
        injected.add(f"rid-{donor}")
    result = exact_links(T1B, obs)
    collided = {c.value for c in result.collisions if c.where == "relay -> ingress"}
    assert collided == injected
    assert not [link for link in result.links if link.value in injected]


# --- E2: injected redeliveries ----------------------------------------------------


@settings(max_examples=300, deadline=None)
@given(st.integers(1, 40), st.data())
def test_injected_redeliveries_raise_no_collision(n, data):
    obs = chain(n)
    again = data.draw(st.lists(st.sampled_from(obs), min_size=1, max_size=3 * n))
    result = exact_links(T1B, obs + again)
    assert result.collisions == ()
    assert result.duplicates_dropped == len(again)
    assert result.links == exact_links(T1B, obs).links


# --- E3: order of the observations -------------------------------------------------


def test_same_result_whatever_the_order_of_the_observations():
    obs = chain(30)
    obs[8] = Observation("obs", 8, "relay", "outbound", {"request_id": "rid-0"})  # a collision
    del obs[50]  # an unlinked record
    reference = exact_links(T1B, obs)
    rng = random.Random(0)
    for _ in range(20):
        rng.shuffle(obs)
        assert exact_links(T1B, obs) == reference
