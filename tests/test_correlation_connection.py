"""Connection path: the timing path inside one connection (phase 4)."""

import random
from dataclasses import replace

import pytest

from reconcileflow.correlation.assignment import Status
from reconcileflow.correlation.connection import Conn, Connected, connection_links
from reconcileflow.correlation.timing import JoinParams, Signal, Timed, timed_links
from reconcileflow.correlation.topology import load

HOP = "a -> b"
END = JoinParams(signals=(Signal("end", 0, 100),), margin_us=0)


def topology():
    point = {
        "host": "lab",
        "visibility": "kernel",
        "clock": "lab",
        "timestamps": "kernel_monotonic",
        "inbound": [],
        "outbound": [],
    }
    return load({"points": {"a": point, "b": dict(point)}, "hops": [{"from": "a", "to": "b"}]})


def conn(port):
    return Conn("10.0.0.1", port, "10.0.0.2", 443)


def up(seq, start, end, c, stream=0):
    return Connected(Timed("s", seq, "a", "outbound", start, end), c, stream)


def down(seq, start, end, c, stream=0):
    return Connected(Timed("t", seq, "b", "inbound", start, end), c, stream)


def pairs(result):
    return {(link.upstream[1], link.downstream[1]) for link in result.links}


def statuses(result):
    return {(u.side, u.observation[1]): u.status for u in result.undecided}


def two_connections_ending_together():
    """Requests k on two connections end at the same instant: by time alone
    each upstream has two candidates at the same distance."""
    obs = []
    for k in range(5):
        t = 10_000 * k
        for n, c in ((2 * k, conn(5001)), (2 * k + 1, conn(5002))):
            obs += [up(n, t, t + 1_000, c), down(n, t + 50, t + 1_000, c)]
    return obs


def test_timing_alone_cannot_tell_two_connections_apart():
    timing = timed_links(
        topology(), [o.timed for o in two_connections_ending_together()], {HOP: END}
    )
    assert not timing.links
    assert {u.status for u in timing.undecided} == {Status.AMBIGUOUS}


def test_the_connection_links_every_request_to_its_own_partner():
    result = connection_links(topology(), two_connections_ending_together(), {HOP: END})
    assert pairs(result) == {(n, n) for n in range(10)}
    assert not result.undecided
    assert result.key == "connection"


def test_a_lost_observation_leaves_its_partner_alone_and_shifts_nothing():
    # K2: no rank is counted, so one loss cannot move the rest of the connection.
    obs = [
        o
        for o in two_connections_ending_together()
        if o.timed.seq != 4 or o.timed.side != "inbound"
    ]
    result = connection_links(topology(), obs, {HOP: END})
    assert pairs(result) == {(n, n) for n in range(10) if n != 4}
    assert statuses(result) == {("outbound", 4): Status.NO_CANDIDATE}


GUARDED = JoinParams(signals=(Signal("end", 0, 100),), margin_us=0, local_span_us=1_000)


def back_to_back(n=30, stream=0):
    """n requests one after the other on one connection, 150 µs apart: each has a
    single candidate in its window, but some twenty neighbours within the guard's span."""
    c = conn(5001)
    obs = []
    for k in range(n):
        t = 150 * k
        obs += [up(k, t, t + 100, c, stream), down(k, t + 10, t + 100, c, stream)]
    return obs


def test_inside_one_http1_connection_the_density_guard_is_off():
    # 4a': one request at a time on the connection, so its neighbours cannot be mistaken for it.
    obs = back_to_back()
    by_time = timed_links(topology(), [o.timed for o in obs], {HOP: GUARDED})
    assert Status.DENSE in {u.status for u in by_time.undecided}
    result = connection_links(topology(), obs, {HOP: GUARDED})
    assert pairs(result) == {(k, k) for k in range(30)}
    assert not result.undecided
    assert dict(result.params)[HOP].local_span_us == 0  # the parameters actually applied


def test_the_stream_key_keeps_the_density_guard():
    obs = back_to_back(stream=1)  # one stream reused: not decided for 4b
    result = connection_links(topology(), obs, {HOP: GUARDED}, key="h2-stream")
    assert Status.DENSE in {u.status for u in result.undecided}
    assert dict(result.params)[HOP] == GUARDED


def test_two_requests_at_once_on_one_connection_stay_ambiguous_without_the_guard():
    c = conn(5001)
    obs = [up(0, 0, 1_000, c), down(0, 50, 1_000, c), up(1, 5, 1_000, c), down(1, 55, 1_000, c)]
    result = connection_links(topology(), obs, {HOP: GUARDED})
    assert not result.links
    assert set(statuses(result).values()) == {Status.AMBIGUOUS}


def test_the_stream_key_counts_neighbours_on_the_same_key_only():
    # One pair alone on its connection, crowded by 20 requests on others.
    obs = [up(0, 0, 5_000, conn(6000), 1), down(0, 10, 5_000, conn(6000), 1)]
    # Their starts are within the span of the pair's; their ends are far from its window.
    for k in range(1, 21):
        obs += [
            up(k, 40 * k, 9_000 + 40 * k, conn(6000 + k), 1),
            down(k, 40 * k + 10, 9_000 + 40 * k, conn(6000 + k), 1),
        ]
    by_time = timed_links(topology(), [o.timed for o in obs], {HOP: GUARDED})
    assert (0, 0) not in {(x.upstream[1], x.downstream[1]) for x in by_time.links}
    assert (0, 0) in pairs(connection_links(topology(), obs, {HOP: GUARDED}, key="h2-stream"))


@pytest.mark.parametrize(
    "bad",
    [
        None,
        Conn("", 5001, "10.0.0.2", 443),
        Conn("10.0.0.1", 0, "10.0.0.2", 443),
        Conn("10.0.0.1", 5001, "", 443),
        Conn("10.0.0.1", 5001, "10.0.0.2", 65_536),
    ],
)
def test_an_unusable_four_tuple_is_no_key_and_never_linked(bad):
    # C3: a span with empty addresses and made-up ports must not be trusted.
    obs = [
        up(0, 0, 1_000, bad),
        down(0, 50, 1_000, conn(5001)),
        up(1, 0, 1_000, conn(5002)),
        down(1, 50, 1_000, conn(5002)),
    ]
    result = connection_links(topology(), obs, {HOP: END})
    assert pairs(result) == {(1, 1)}
    assert statuses(result) == {("outbound", 0): Status.NO_KEY, ("inbound", 0): Status.NO_CANDIDATE}


def test_on_one_connection_the_result_is_the_timing_path_without_its_guard():
    # K4: with a single four-tuple, phase 3's path, less the density guard (4a').
    rng = random.Random(4)
    guarded = JoinParams(
        signals=(Signal("start", 50, 120), Signal("end", 0, 120)), margin_us=5, local_span_us=2_000
    )
    for _ in range(30):
        obs = []
        for n in range(rng.randint(0, 25)):
            start = rng.randint(0, 20_000)
            end = start + rng.randint(300, 3_000)
            obs.append(up(n, start, end, conn(7000)))
            if rng.random() < 0.9:
                obs.append(
                    down(n, start + rng.randint(0, 100), end - rng.randint(0, 100), conn(7000))
                )
        alone = timed_links(
            topology(), [o.timed for o in obs], {HOP: replace(guarded, local_span_us=0)}
        )
        result = connection_links(topology(), obs, {HOP: guarded})
        assert (result.links, result.undecided, result.components) == (
            alone.links,
            alone.undecided,
            alone.components,
        )


def test_same_result_whatever_the_order_of_the_observations():
    obs = two_connections_ending_together()
    expected = connection_links(topology(), obs, {HOP: END})
    for seed in range(10):
        shuffled = list(obs)
        random.Random(seed).shuffle(shuffled)
        assert connection_links(topology(), shuffled, {HOP: END}) == expected


# -- the HTTP/2 stream number (4b) -------------------------------------------


def concurrent_streams(n=8):
    """n requests on one connection, in flight together and ending at the same
    instant: by time alone, one ambiguous component."""
    c = conn(5001)
    obs = []
    for k in range(n):
        obs += [up(k, k, 1_000, c, stream=2 * k + 1), down(k, 50 + k, 1_000, c, stream=2 * k + 1)]
    return obs


def test_the_stream_number_links_requests_in_flight_together():
    obs = concurrent_streams()
    assert not timed_links(topology(), [o.timed for o in obs], {HOP: END}).links
    result = connection_links(topology(), obs, {HOP: END}, key="h2-stream")
    assert pairs(result) == {(k, k) for k in range(8)}
    assert result.key == "h2-stream"


def test_a_shifted_stream_number_beyond_the_window_is_a_key_time_conflict():
    # K3: numbers + 2 on one side; requests 10 ms apart, windows of 100 µs.
    c = conn(5001)
    obs = []
    for k in range(5):
        t = 10_000 * k
        obs += [
            up(k, t, t + 1_000, c, stream=2 * k + 1),
            down(k, t + 50, t + 1_000, c, stream=2 * k + 3),
        ]
    result = connection_links(topology(), obs, {HOP: END}, key="h2-stream")
    assert not result.links
    s = statuses(result)
    assert s[("outbound", 0)] == Status.NO_CANDIDATE  # stream 1: nothing downstream
    assert s[("inbound", 4)] == Status.NO_CANDIDATE  # stream 11: nothing upstream
    assert all(
        s[side, k] == Status.KEY_TIME_CONFLICT
        for side, k in [("outbound", 1), ("outbound", 4), ("inbound", 0), ("inbound", 3)]
    )


def test_a_shifted_stream_number_inside_the_window_is_not_caught():
    # The limit of K3, written down: time only catches a shift wider than its window.
    obs = concurrent_streams(3)
    shifted = [
        down(o.timed.seq, o.timed.start_us, o.timed.end_us, o.conn, o.stream + 2)
        if o.timed.side == "inbound"
        else o
        for o in obs
    ]
    result = connection_links(topology(), shifted, {HOP: END}, key="h2-stream")
    assert pairs(result) == {(1, 0), (2, 1)}  # wrong links: the stream number is trusted


def test_a_missing_stream_number_is_no_key():
    c = conn(5001)
    obs = [
        up(0, 0, 1_000, c, stream=0),
        down(0, 50, 1_000, c, stream=1),
        up(1, 0, 1_000, c, stream=3),
        down(1, 50, 1_000, c, stream=3),
    ]
    result = connection_links(topology(), obs, {HOP: END}, key="h2-stream")
    assert pairs(result) == {(1, 1)}
    assert statuses(result) == {("outbound", 0): Status.NO_KEY, ("inbound", 0): Status.NO_CANDIDATE}


def test_a_reused_four_tuple_and_stream_is_told_apart_by_time():
    c = conn(5001)
    far = [
        up(0, 0, 1_000, c, 1),
        down(0, 50, 1_000, c, 1),
        up(1, 60_000, 61_000, c, 1),
        down(1, 60_050, 61_000, c, 1),
    ]
    assert pairs(connection_links(topology(), far, {HOP: END}, key="h2-stream")) == {(0, 0), (1, 1)}
    close = [
        up(0, 0, 1_000, c, 1),
        down(0, 50, 1_000, c, 1),
        up(1, 5, 1_000, c, 1),
        down(1, 55, 1_000, c, 1),
    ]
    result = connection_links(topology(), close, {HOP: END}, key="h2-stream")
    assert not result.links
    assert set(statuses(result).values()) == {Status.AMBIGUOUS}


# -- the TCP stream offset (4c) ----------------------------------------------

WIDE = JoinParams(signals=(Signal("end", 0, 50_000),), margin_us=0, local_span_us=1_000)


def at(obs, offset):
    """The same observation, with the offset of its first byte in the TCP stream."""
    return Connected(obs.timed, obs.conn, obs.stream, offset)


def with_offsets(obs, size=100):
    """Request k on its connection starts at byte k * size, on both sides."""
    return [at(o, o.timed.seq * size) for o in obs]


def frozen(obs):
    """An uneven freeze: even downstream observations dated 250 µs late, odd ones
    10 µs, so each even one is seen after the next request's (150 µs apart)."""
    out = []
    for o in obs:
        t = o.timed
        if t.side == "inbound":
            d = 250 if t.seq % 2 == 0 else 10
            t = Timed(t.source, t.seq, t.point, t.side, t.start_us + d, t.end_us + d)
        out.append(Connected(t, o.conn, o.stream, o.offset))
    return out


def test_the_tcp_offset_links_messages_that_time_refuses_in_a_freeze():
    obs = frozen(with_offsets(back_to_back()))
    by_time = connection_links(topology(), obs, {HOP: WIDE})
    assert not by_time.links
    assert {u.status for u in by_time.undecided} == {Status.AMBIGUOUS}
    result = connection_links(topology(), obs, {HOP: WIDE}, key="tcp-seq")
    assert pairs(result) == {(k, k) for k in range(30)}
    assert not result.undecided
    assert result.key == "tcp-seq"


def test_a_lost_message_shifts_nothing_with_the_tcp_offset():
    obs = [o for o in with_offsets(back_to_back()) if (o.timed.side, o.timed.seq) != ("inbound", 7)]
    result = connection_links(topology(), obs, {HOP: WIDE}, key="tcp-seq")
    assert pairs(result) == {(k, k) for k in range(30) if k != 7}
    assert statuses(result) == {("outbound", 7): Status.NO_CANDIDATE}


def test_one_offset_is_one_message_so_the_density_guard_is_off():
    result = connection_links(topology(), with_offsets(back_to_back()), {HOP: WIDE}, key="tcp-seq")
    assert dict(result.params)[HOP].local_span_us == 0


def test_a_shifted_offset_beyond_the_window_is_a_key_time_conflict():
    # K5: downstream offsets one message ahead; requests 10 ms apart, windows of 100 µs.
    c = conn(5001)
    obs = []
    for k in range(5):
        t = 10_000 * k
        obs += [
            at(up(k, t, t + 1_000, c), 100 * k),
            at(down(k, t + 50, t + 1_000, c), 100 * (k + 1)),
        ]
    result = connection_links(topology(), obs, {HOP: END}, key="tcp-seq")
    assert not result.links
    s = statuses(result)
    assert s[("outbound", 0)] == Status.NO_CANDIDATE  # offset 0: nothing downstream
    assert s[("inbound", 4)] == Status.NO_CANDIDATE  # offset 500: nothing upstream
    assert all(s["outbound", k] == Status.KEY_TIME_CONFLICT for k in range(1, 5))


def test_a_shifted_offset_inside_the_window_is_not_caught():
    # The limit of K5, written down: time only catches a shift wider than its window.
    obs = with_offsets(back_to_back(5))
    shifted = [at(o, o.offset + 100) if o.timed.side == "inbound" else o for o in obs]
    result = connection_links(topology(), shifted, {HOP: WIDE}, key="tcp-seq")
    assert pairs(result) == {(k + 1, k) for k in range(4)}  # wrong links: the offset is trusted


def test_a_missing_offset_is_no_key_and_offset_zero_is_usable():
    c = conn(5001)
    obs = [
        at(up(0, 0, 1_000, c), 0),
        at(down(0, 50, 1_000, c), 0),
        up(1, 5_000, 6_000, c),
        at(down(1, 5_050, 6_000, c), 100),
    ]
    result = connection_links(topology(), obs, {HOP: END}, key="tcp-seq")
    assert pairs(result) == {(0, 0)}
    assert statuses(result) == {("outbound", 1): Status.NO_KEY, ("inbound", 1): Status.NO_CANDIDATE}


def test_the_offset_is_ignored_by_the_other_keys():
    obs = two_connections_ending_together()
    assert connection_links(topology(), with_offsets(obs), {HOP: END}) == connection_links(
        topology(), obs, {HOP: END}
    )


@pytest.mark.parametrize("offset", [-1, True, "0", 1.0])
def test_an_invalid_offset_is_refused(offset):
    with pytest.raises(ValueError, match="offset"):
        connection_links(topology(), [at(up(0, 0, 1, conn(5001)), offset)], {HOP: END})


def test_same_identity_with_a_different_offset_is_refused():
    a, b = at(up(0, 0, 1_000, conn(5001)), 0), at(up(0, 0, 1_000, conn(5001)), 100)
    with pytest.raises(ValueError, match="different content"):
        connection_links(topology(), [a, b], {HOP: END})


# -- inputs --------------------------------------------------------------------


def test_a_redelivered_observation_counts_once():
    obs = two_connections_ending_together()
    result = connection_links(topology(), obs + obs[:3], {HOP: END})
    assert result.duplicates_dropped == 3
    assert pairs(result) == {(n, n) for n in range(10)}


def test_same_identity_with_a_different_connection_or_stream_is_refused():
    a = up(0, 0, 1_000, conn(5001), 1)
    for b in (up(0, 0, 1_000, conn(5002), 1), up(0, 0, 1_000, conn(5001), 3)):
        with pytest.raises(ValueError, match="different content"):
            connection_links(topology(), [a, b], {HOP: END})


@pytest.mark.parametrize(
    "obs, reason",
    [
        (up(0, 0, 1, Conn("10.0.0.1", "5001", "10.0.0.2", 443)), "integers"),
        (up(0, 0, 1, Conn(None, 5001, "10.0.0.2", 443)), "strings"),
        (up(0, 0, 1, conn(5001), stream=-1), "stream"),
        (up(0, 0, 1, conn(5001), stream=True), "stream"),
        (up(0, 0, 1, "10.0.0.1:5001"), "Conn"),
        (Connected(Timed("s", 0, "x", "outbound", 0, 1), conn(5001)), "unknown point"),
    ],
)
def test_invalid_observations_are_refused(obs, reason):
    with pytest.raises(ValueError, match=reason):
        connection_links(topology(), [obs], {HOP: END})


def test_an_unknown_key_and_bad_params_are_refused_even_without_observations():
    with pytest.raises(ValueError, match="key"):
        connection_links(topology(), [], {HOP: END}, key="tcp")
    with pytest.raises(ValueError, match="window"):
        connection_links(
            topology(), [], {HOP: JoinParams(signals=(Signal("end", 0, 0),), margin_us=0)}
        )


@pytest.mark.parametrize("client, server", [(1, 443), (65_535, 443), (5001, 1), (5001, 65_535)])
def test_ports_at_both_ends_of_their_range_are_usable(client, server):
    c = Conn("10.0.0.1", client, "10.0.0.2", server)
    result = connection_links(topology(), [up(0, 0, 1_000, c), down(0, 50, 1_000, c)], {HOP: END})
    assert pairs(result) == {(0, 0)}


@pytest.mark.parametrize("client, server", [(0, 443), (65_536, 443), (5001, 0), (5001, 65_536)])
def test_ports_just_outside_their_range_are_no_key(client, server):
    c = Conn("10.0.0.1", client, "10.0.0.2", server)
    result = connection_links(topology(), [up(0, 0, 1_000, c), down(0, 50, 1_000, c)], {HOP: END})
    assert not result.links
    assert set(statuses(result).values()) == {Status.NO_KEY}


@pytest.mark.parametrize("stream, linked", [(1, True), (2**31 - 1, True), (2**31, False)])
def test_the_stream_number_is_31_bits(stream, linked):
    c = conn(5001)
    obs = [up(0, 0, 1_000, c, stream), down(0, 50, 1_000, c, stream)]
    result = connection_links(topology(), obs, {HOP: END}, key="h2-stream")
    assert bool(result.links) is linked


def test_components_are_reported_for_every_connection():
    result = connection_links(topology(), two_connections_ending_together(), {HOP: END})
    assert result.components == tuple((HOP, 2) for _ in range(10))


def test_observations_off_the_hop_are_skipped_wherever_they_come():
    # An inbound observation at "a" and an outbound one at "b" are on no join of a -> b.
    off = [
        Connected(Timed("x", 0, "a", "inbound", 0, 1_000), conn(5001)),
        Connected(Timed("x", 1, "b", "outbound", 0, 1_000), conn(5001)),
    ]
    result = connection_links(topology(), off + two_connections_ending_together(), {HOP: END})
    assert pairs(result) == {(n, n) for n in range(10)}
    assert not result.undecided
