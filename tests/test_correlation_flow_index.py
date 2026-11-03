"""Finding groups through an index decides nothing differently from going over
everything open: the reference below is the rule itself, written the long way,
and every journal must give it the same bytes."""

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from reconcileflow.correlation.exact import Observation
from reconcileflow.correlation.flow import Advance, FlowConfig, FlowResult, Seen, _Flow, replay_flow
from reconcileflow.correlation.topology import load

TOPOLOGY = load(
    {
        "points": {
            "a": {
                "host": "h",
                "visibility": "kernel",
                "clock": "h",
                "timestamps": "kernel_monotonic",
                "outbound": ["traceparent", "tuple5", "h2_stream_id"],
            },
            "b": {
                "host": "h",
                "visibility": "kernel",
                "clock": "h",
                "timestamps": "kernel_monotonic",
                "inbound": ["traceparent", "tuple5", "h2_stream_id"],
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
        "hops": [{"from": "a", "to": "b", "pooling": True}, {"from": "b", "to": "c"}],
    }
)
PLACES = (("a", "outbound"), ("b", "inbound"), ("b", "outbound"), ("c", "inbound"))
SOURCES = ("s1", "s2")


class Reference(_Flow):
    """Every group recomputed from all that is open, at every step."""

    def _groups(self):
        parent = {identity: identity for identity in self.open}

        def root(identity):
            return identity if parent[identity] == identity else root(parent[identity])

        holder = {}
        for identity in sorted(self.open):
            for value in self._values(self.open[identity].observation):
                other = holder.setdefault(value, identity)
                parent[root(identity)] = root(other)
        groups = {}
        for identity in sorted(parent):
            groups.setdefault(root(identity), []).append(identity)
        return sorted(groups.values())

    def _group_of(self, identity):
        return next(g for g in self._groups() if identity in g)

    def _ripe_groups(self, watermark):
        return [
            g
            for g in self._groups()
            if max(self.open[i].at for i in g) + self.config.window_us < watermark
        ]


def bytes_of(flow, journal):
    for entry in journal:
        flow.feed(entry)
    return repr((flow.decisions, flow.dossiers, flow.duplicates))


entries = st.lists(
    st.one_of(
        st.tuples(
            st.sampled_from(SOURCES),
            st.integers(0, 40),  # few identities: duplicates happen
            st.sampled_from(PLACES),
            st.integers(0, 5),  # few values: groups merge and collide
            st.none() | st.integers(0, 3),  # a connection, or none
            st.integers(0, 60),
        ),
        st.tuples(st.sampled_from(SOURCES), st.integers(0, 120)),
    ),
    max_size=60,
)


def journal_of(raw):
    journal, content = [], {}
    for item in raw:
        if len(item) == 2:
            journal.append(Advance(*item))
            continue
        source, seq, (point, side), value, connection, at = item
        if (source, seq) in content:  # same identity: delivered again, same content
            journal.append(content[source, seq])
            continue
        keys = {"traceparent": f"v{value}"}
        if connection is not None and point != "c" and (point, side) != ("b", "outbound"):
            keys |= {"tuple5": f"c{connection}", "h2_stream_id": str(value)}
        content[source, seq] = Seen(Observation(source, seq, point, side, keys), at)
        journal.append(content[source, seq])
    return journal


@settings(max_examples=300, deadline=None)
@given(raw=entries, window=st.integers(0, 15), lateness=st.integers(0, 5))
def test_the_index_gives_the_same_bytes_as_going_over_everything(raw, window, lateness):
    config = FlowConfig(SOURCES, window, lateness_us=lateness)
    journal = journal_of(raw)
    assert bytes_of(_Flow(TOPOLOGY, config), journal) == bytes_of(
        Reference(TOPOLOGY, config), journal
    )


@settings(max_examples=100, deadline=None)
@given(raw=entries, bound=st.integers(1, 30))
def test_so_does_a_silent_source_left_out_and_back(raw, bound):
    config = FlowConfig((*SOURCES, "s3"), 5, max_silence_us=bound)
    journal = journal_of(raw)
    assert bytes_of(_Flow(TOPOLOGY, config), journal) == bytes_of(
        Reference(TOPOLOGY, config), journal
    )


def test_a_group_closes_only_with_its_latest_member():
    def seen(seq, at):
        return Seen(Observation("s1", seq, "a", "outbound", {"traceparent": "v"}), at)

    config = FlowConfig(("s1",), 10)
    flow = _Flow(TOPOLOGY, config)
    flow.feed(seen(0, 0))
    flow.feed(seen(1, 8))
    flow.feed(Advance("s1", 11))  # past the first, not past the second
    assert not flow.dossiers
    assert len(flow.open) == 2
    flow.feed(Advance("s1", 19))
    assert len(flow.dossiers) == 1
    assert flow.dossiers[0].observations == (("s1", 0), ("s1", 1))


def test_what_closes_leaves_nothing_behind_in_the_index():
    flow = _Flow(TOPOLOGY, FlowConfig(("s1",), 10))
    for seq in range(5):
        keys = {"traceparent": f"v{seq % 2}", "tuple5": "c", "h2_stream_id": str(seq)}
        flow.feed(Seen(Observation("s1", seq, "a", "outbound", keys), seq))
    assert flow.holders and flow.pending
    flow.feed(Advance("s1", 100))
    assert flow.open == {}
    assert flow.holders == {}
    assert flow.pending == []


@pytest.mark.parametrize("seed", range(3))
def test_the_cost_of_an_entry_does_not_grow_with_what_is_open(seed):
    # Every observation stands alone, all held open: going over everything would
    # read each of them at every entry.
    draw = random.Random(seed)
    calls = 0

    class Counting(_Flow):
        def _values(self, obs):
            nonlocal calls
            calls += 1
            return super()._values(obs)

    flow = Counting(TOPOLOGY, FlowConfig(("s1", "s2"), 10))
    n = 2_000
    for seq in range(n):
        keys = {"traceparent": f"{draw.getrandbits(64):016x}"}
        flow.feed(Seen(Observation("s1", seq, "a", "outbound", keys), seq))
    assert len(flow.open) == n
    assert calls <= 3 * n


def test_replay_flow_still_returns_the_flows_results():
    journal = [Seen(Observation("s1", 0, "a", "outbound", {"traceparent": "v"}), 0)]
    result = replay_flow(TOPOLOGY, [*journal, Advance("s1", 100)], FlowConfig(("s1",), 10))
    assert isinstance(result, FlowResult)
    assert len(result.dossiers) == 1
