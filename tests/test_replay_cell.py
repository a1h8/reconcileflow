"""Replaying a kept harness cell through the stream correlation: what enters the
journal, when a hop gets its second signal, and how the outcome is judged
against a truth the engine never sees."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from reconcileflow.correlation.flow import Advance, Corroboration, Kind, Seen, replay_flow

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
spec = importlib.util.spec_from_file_location("replay_cell", HARNESS / "replay_cell.py")
rc = importlib.util.module_from_spec(spec)
sys.modules["replay_cell"] = rc  # dataclasses look the module up
spec.loader.exec_module(rc)

HOP_PORTS = [41001, 41002]
UPSTREAM_PORT = 42000
HOST_START_US = 1_000_000
GAP_US = 10_000
WINDOW_US = 5_000


def truth(i):
    return f"{i + 1:032x}"


def write_cell(root, requests=6, hops=1, stream=True, port_ok=True, masked=False, edit=None):
    """A cell in the harness's own file formats: one chain of nested requests."""
    cell = root / f"n{hops}"
    cell.mkdir()
    (cell / "cell.json").write_text(
        json.dumps(
            {
                "hops": hops,
                "fake_upstream_port": UPSTREAM_PORT,
                "hop_ports": HOP_PORTS[:hops],
                "hop_kinds": ["relay"] * hops,
                "lg_mask": int(masked),
                "context_propagation": "headers",
            }
        )
    )
    rows, spans = [], []
    for i in range(requests):
        base = HOST_START_US + i * GAP_US
        duration = (2 * hops + 3) * 2 * 100
        rows.append(
            {"trace_id": truth(i), "timestamp_ns": base * 1000, "duration_ns": duration * 1000}
        )
        ends = []
        for k in range(1, hops + 1):
            entry = HOP_PORTS[k - 1] if port_ok else 48736
            client_port = 50000 if k == 1 else 51000 + k - 1  # the previous point's
            next_port = HOP_PORTS[k] if k < hops else UPSTREAM_PORT
            ends.append(("SERVER", f"hop-{k}", "127.0.0.1", client_port, entry))
            ends.append(("CLIENT", f"127.0.0.1:{next_port}", f"hop-{k}", 51000 + k, next_port))
        ends.append(("SERVER", "fake-upstream", "127.0.0.1", 51000 + hops, UPSTREAM_PORT))
        for depth, (kind, server, client, client_port, server_port) in enumerate(ends, start=1):
            attributes = {"url": f"/truth-{truth(i)}", "serverAddr": server, "clientAddr": client}
            if stream:
                attributes["http2StreamId"] = str(2 * i + 1)
            record = {
                "kind": f"SPAN_KIND_{kind}",
                "traceID": truth(i),
                "peerPort": str(client_port),
                "hostPort": str(server_port),
                "start": str(base + depth * 100),
                "handlerStart": str(base + depth * 100),
                "end": str(base + duration - depth * 100),
                "attributes": attributes,
            }
            if edit is not None:
                edit(record, i, server if kind == "SERVER" else client, kind)
            spans.append(record)
    (cell / "lg.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    (cell / "agent.out").write_text("not a span line\n" + json.dumps(spans) + "\n")
    return cell


def replay(cell, **faults):
    loaded = rc.load_cell(cell, WINDOW_US, rc.Faults(**faults))
    return loaded, replay_flow(loaded.topology, loaded.journal, loaded.config)


def report(cell, **faults):
    return rc.analyze_cell(cell, WINDOW_US, rc.Faults(**faults))


# -- what enters the journal ---------------------------------------------------


def test_a_clean_cell_links_every_join_with_nothing_wrong(tmp_path):
    out = report(write_cell(tmp_path, hops=2))
    assert set(out["joins"]) == {
        "load-gen -> hop-1",
        "hop-1",
        "hop-1 -> hop-2",
        "hop-2",
        "hop-2 -> fake-upstream",
    }
    for join in out["joins"].values():
        assert join == {
            "linked": 12,
            "correct": 12,
            "wrong": 0,
            "collided": 0,
            "unlinked": 0,
            "conflicted": 0,
        }
    assert out["wrong"] == 0
    assert out["open"] == 0
    assert out["after_window"] == 0
    assert out["late"] == 0
    assert out["measured"] == 6


def test_an_observation_happens_when_the_request_ends_in_microseconds(tmp_path):
    loaded, _ = replay(write_cell(tmp_path, requests=1))
    seen = [e for e in loaded.journal if isinstance(e, Seen)]
    load_gen = next(e for e in seen if e.observation.point == "load-gen")
    assert load_gen.at == HOST_START_US + 1000  # timestamp_ns + duration_ns, in µs
    upstream = next(e for e in seen if e.observation.point == "fake-upstream")
    assert upstream.at == HOST_START_US + 1000 - 300


def test_the_journal_is_in_event_order_and_ends_past_the_window(tmp_path):
    loaded, _ = replay(write_cell(tmp_path))
    seen = [e for e in loaded.journal if isinstance(e, Seen)]
    assert [e.at for e in seen] == sorted(e.at for e in seen)
    assert len(seen) == 6 * 4  # load-gen, hop-1 twice, fake-upstream
    last = max(e.at for e in seen)
    closing = loaded.journal[len(seen) :]
    assert {e.source for e in closing} == set(loaded.config.sources)
    assert all(isinstance(e, Advance) and e.until == last + WINDOW_US + 1 for e in closing)
    assert loaded.config.window_us == WINDOW_US


def test_warm_up_and_tail_traffic_never_enters(tmp_path):
    cell = write_cell(tmp_path, requests=3)
    (cell / "warmup-1.jsonl").write_text(json.dumps({"trace_id": truth(0)}) + "\n")
    (cell / "tail.jsonl").write_text(json.dumps({"trace_id": truth(1)}) + "\n")
    loaded, _ = replay(cell)
    agent = [
        e.observation
        for e in loaded.journal
        if isinstance(e, Seen) and e.observation.source == "agent.out"
    ]
    assert {o.keys["traceparent"] for o in agent} == {truth(2)}


def test_a_span_without_a_measured_truth_or_a_place_never_enters(tmp_path):
    def edit(record, i, _who, kind):
        if i == 0:
            record["attributes"]["url"] = "/health"
        elif i == 1 and kind == "CLIENT":
            record["attributes"]["clientAddr"] = "elsewhere"

    loaded, _ = replay(write_cell(tmp_path, requests=2, edit=edit))
    agent = [
        e.observation
        for e in loaded.journal
        if isinstance(e, Seen) and e.observation.source == "agent.out"
    ]
    assert sorted((o.point, o.side) for o in agent) == [
        ("fake-upstream", "inbound"),
        ("hop-1", "inbound"),
    ]


def test_a_masked_cell_gives_load_gen_no_key(tmp_path):
    loaded, _ = replay(write_cell(tmp_path, masked=True))
    load_gen = [
        e.observation
        for e in loaded.journal
        if isinstance(e, Seen) and e.observation.point == "load-gen"
    ]
    assert load_gen and all(o.keys == {} for o in load_gen)


def test_an_all_zero_trace_id_is_no_key(tmp_path):
    def edit(record, _i, _who, _kind):
        record["traceID"] = "0" * 32

    loaded, _ = replay(write_cell(tmp_path, requests=1, edit=edit))
    agent = [
        e.observation
        for e in loaded.journal
        if isinstance(e, Seen) and e.observation.source == "agent.out"
    ]
    assert agent and all("traceparent" not in o.keys for o in agent)


def test_the_truth_stays_out_of_the_observations(tmp_path):
    def edit(record, _i, _who, _kind):
        record["traceID"] = "a" * 32  # the url still holds the truth

    loaded, _ = replay(write_cell(tmp_path, requests=2, edit=edit))
    written = repr(
        [e for e in loaded.journal if isinstance(e, Seen) and e.observation.source == "agent.out"]
    )
    assert truth(0) not in written and truth(1) not in written
    assert set(loaded.truth.values()) == {truth(0), truth(1)}


# -- the second signal, hop by hop ---------------------------------------------


def test_right_ports_and_stream_numbers_corroborate_the_hop(tmp_path):
    loaded, result = replay(write_cell(tmp_path, hops=2))
    assert loaded.signals == {
        "load-gen -> hop-1": rc.NO_REAL_STREAM,
        "hop-1 -> hop-2": None,
        "hop-2 -> fake-upstream": None,
    }
    counts = result.signal_counts()
    assert counts["hop-1 -> hop-2", Corroboration.CORROBORATED] == 12
    assert counts["hop-2 -> fake-upstream", Corroboration.CORROBORATED] == 12
    assert not [c for w, c in counts if w == "load-gen -> hop-1"]


def test_the_connection_key_is_ports_only_and_carries_the_stream(tmp_path):
    loaded, _ = replay(write_cell(tmp_path, requests=1, hops=2))
    keys = {
        (e.observation.point, e.observation.side): e.observation.keys
        for e in loaded.journal
        if isinstance(e, Seen)
    }
    assert keys["hop-1", "outbound"]["tuple5"] == f"51001>{HOP_PORTS[1]}"
    assert keys["hop-1", "outbound"]["h2_stream_id"] == "1"
    assert keys["hop-2", "inbound"]["tuple5"] == f"51001>{HOP_PORTS[1]}"
    assert "tuple5" not in keys["load-gen", "outbound"]
    assert "tuple5" not in keys["hop-1", "inbound"]  # its hop is load-gen's


def test_a_false_entry_port_refuses_the_second_signal_with_its_reason(tmp_path):
    loaded, result = replay(write_cell(tmp_path, hops=2, port_ok=False))
    assert loaded.signals["hop-1 -> hop-2"] == rc.FALSE_ENTRY_PORT
    assert loaded.signals["hop-2 -> fake-upstream"] is None
    keys = [
        e.observation.keys
        for e in loaded.journal
        if isinstance(e, Seen)
        and e.observation.point == "hop-2"
        and e.observation.side == "inbound"
    ]
    assert keys and all("tuple5" not in k for k in keys)
    decisions = result.effective()
    linked = [d for (w, _), d in decisions.items() if w == "hop-1 -> hop-2"]
    assert linked and all(d.kind is Kind.LINKED and d.corroboration is None for d in linked)


def test_one_false_entry_port_is_enough_to_refuse(tmp_path):
    def edit(record, i, who, kind):
        if i == 3 and who == "hop-2" and kind == "SERVER":
            record["hostPort"] = "48736"

    loaded, _ = replay(write_cell(tmp_path, hops=2, edit=edit))
    assert loaded.signals["hop-1 -> hop-2"] == rc.FALSE_ENTRY_PORT


def test_a_constant_but_right_port_keeps_the_second_signal(tmp_path):
    # A pooled connection has one pair of ports: constancy alone is no defect.
    loaded, _ = replay(write_cell(tmp_path, hops=2))
    assert loaded.signals["hop-1 -> hop-2"] is None


def test_no_stream_number_refuses_the_second_signal(tmp_path):
    loaded, result = replay(write_cell(tmp_path, stream=False))
    assert loaded.signals["hop-1 -> fake-upstream"] == rc.NO_STREAM
    assert result.signal_counts() == {}


def test_a_stream_number_of_zero_is_none(tmp_path):
    def edit(record, _i, _who, _kind):
        record["attributes"]["http2StreamId"] = "0"

    loaded, _ = replay(write_cell(tmp_path, edit=edit))
    assert loaded.signals["hop-1 -> fake-upstream"] == rc.NO_STREAM


def test_a_stream_number_on_one_end_only_refuses_the_second_signal(tmp_path):
    def edit(record, _i, who, _kind):
        if who == "fake-upstream":
            del record["attributes"]["http2StreamId"]

    loaded, _ = replay(write_cell(tmp_path, edit=edit))
    assert loaded.signals["hop-1 -> fake-upstream"] == rc.NO_STREAM


def test_a_cell_without_listening_ports_gets_no_second_signal(tmp_path):
    cell = write_cell(tmp_path, hops=2)
    meta = json.loads((cell / "cell.json").read_text())
    del meta["hop_ports"]
    (cell / "cell.json").write_text(json.dumps(meta))
    loaded, _ = replay(cell)
    assert loaded.signals["hop-1 -> hop-2"] == rc.NO_LISTENING_PORT
    # fake-upstream's listening port is always declared; a relay's may not be.
    assert loaded.signals["hop-2 -> fake-upstream"] is None


def test_a_missing_stream_number_on_one_request_is_traced_absent(tmp_path):
    def edit(record, i, who, _kind):
        if i == 2 and who == "fake-upstream":
            del record["attributes"]["http2StreamId"]

    _, result = replay(write_cell(tmp_path, edit=edit))
    counts = result.signal_counts()
    assert counts["hop-1 -> fake-upstream", Corroboration.SIGNAL_ABSENT] == 2
    assert counts["hop-1 -> fake-upstream", Corroboration.CORROBORATED] == 10


def test_two_requests_trading_ids_on_a_hop_are_conflicted_not_linked(tmp_path):
    def edit(record, i, who, _kind):
        if who == "fake-upstream" and i in (1, 2):
            record["traceID"] = truth(3 - i)

    out = report(write_cell(tmp_path, edit=edit))
    join = out["joins"]["hop-1 -> fake-upstream"]
    assert join["conflicted"] == 4
    assert join["wrong"] == 0


# -- the journal's order, the replay ------------------------------------------


def test_replaying_twice_gives_the_same_bytes(tmp_path):
    cell = write_cell(tmp_path, hops=2)
    first, second = report(cell), report(cell)
    assert first["digest"] == second["digest"]
    assert len(first["digest"]) == 64


def test_a_different_cell_gives_different_bytes(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    assert (
        report(write_cell(tmp_path / "a", requests=2))["digest"]
        != report(write_cell(tmp_path / "b", requests=3))["digest"]
    )


def test_a_bounded_disorder_changes_no_final_decision(tmp_path):
    cell = write_cell(tmp_path, requests=40, hops=2)
    plain = report(cell)
    shifted, _ = replay(cell, shift_us=3_000, seed=7)
    seen = [e for e in shifted.journal if isinstance(e, Seen)]
    assert [e.at for e in seen] != sorted(e.at for e in seen)  # it did reorder
    assert shifted.config.lateness_us == 3_000
    out = report(cell, shift_us=3_000, seed=7)
    assert out["joins"] == plain["joins"]
    assert out["late"] == 0
    assert out["finals"] == plain["finals"]


def test_the_same_seed_gives_the_same_disorder_and_another_does_not(tmp_path):
    cell = write_cell(tmp_path, requests=40)
    one, _ = replay(cell, shift_us=3_000, seed=1)
    same, _ = replay(cell, shift_us=3_000, seed=1)
    other, _ = replay(cell, shift_us=3_000, seed=2)
    assert one.journal == same.journal
    assert one.journal != other.journal


def test_the_closing_advance_covers_the_tolerated_lateness(tmp_path):
    loaded, result = replay(write_cell(tmp_path), shift_us=3_000, seed=1)
    seen = [e for e in loaded.journal if isinstance(e, Seen)]
    closing = [e for e in loaded.journal if isinstance(e, Advance)]
    assert {e.until for e in closing} == {max(e.at for e in seen) + WINDOW_US + 3_000 + 1}
    assert all(d.status.value == "FINAL" for d in result.effective().values())


# -- what falls outside is told, never dropped --------------------------------


def test_a_side_past_the_window_is_told_and_links_nothing(tmp_path):
    def edit(record, i, who, _kind):
        if i == 0 and who == "fake-upstream":
            record["end"] = str(int(record["end"]) + 10 * WINDOW_US)
            record["start"] = str(int(record["start"]) + 10 * WINDOW_US)

    out = report(write_cell(tmp_path, edit=edit))
    assert out["after_window"] == 1
    assert out["joins"]["hop-1 -> fake-upstream"]["unlinked"] == 1
    assert out["joins"]["hop-1 -> fake-upstream"]["wrong"] == 0
    assert out["open"] == 0


def test_a_reused_id_is_never_a_link(tmp_path):
    # Every third request carries the previous one's id, past load-gen.
    out = report(write_cell(tmp_path, requests=9, hops=2), reuse_every=3)
    assert out["reuse"]["injected"] == 2  # requests 3 and 6
    assert out["reuse"]["linked"] == 0
    assert out["reuse"]["collided"] > 0
    assert out["wrong"] == 0


def test_the_reuse_fault_leaves_load_gen_alone(tmp_path):
    loaded, _ = replay(write_cell(tmp_path, requests=9), reuse_every=3)
    load_gen = sorted(
        e.observation.keys["traceparent"]
        for e in loaded.journal
        if isinstance(e, Seen) and e.observation.point == "load-gen"
    )
    assert load_gen == sorted(truth(i) for i in range(9))


def test_a_reused_id_with_no_stream_collides_on_the_hop(tmp_path):
    out = report(write_cell(tmp_path, requests=9, stream=False), reuse_every=3)
    assert out["joins"]["hop-1 -> fake-upstream"]["collided"] > 0
    assert out["joins"]["hop-1 -> fake-upstream"]["wrong"] == 0


def test_a_hidden_side_is_a_missing_link_in_its_exact_place(tmp_path):
    out = report(write_cell(tmp_path, requests=5, hops=2), hide=("hop-1", "outbound"))
    joins = out["joins"]
    assert joins["load-gen -> hop-1"]["linked"] == 10  # untouched
    assert joins["hop-1"]["linked"] == 0 and joins["hop-1"]["unlinked"] == 5
    assert joins["hop-1 -> hop-2"]["linked"] == 0 and joins["hop-1 -> hop-2"]["unlinked"] == 5
    assert joins["hop-2"]["linked"] == 10  # untouched
    assert out["wrong"] == 0


def test_a_hidden_side_must_exist(tmp_path):
    with pytest.raises(ValueError, match="nothing to hide"):
        replay(write_cell(tmp_path), hide=("hop-9", "outbound"))


# -- the report ---------------------------------------------------------------


def test_the_report_states_what_was_declared_and_the_signals(tmp_path):
    out = report(write_cell(tmp_path, hops=2, port_ok=False))
    assert out["window_us"] == WINDOW_US
    assert out["lateness_us"] == 0
    assert out["hops"] == 2
    assert out["signals"]["hop-1 -> hop-2"] == rc.FALSE_ENTRY_PORT
    assert out["signals"]["hop-2 -> fake-upstream"] is None
    assert out["corroboration"] == {"CORROBORATED": 12}


def test_a_wrong_link_is_counted_never_hidden(tmp_path):
    # Equality cannot see it: two ids traded, no second signal on this hop.
    def edit(record, i, who, _kind):
        if who == "fake-upstream" and i in (1, 2):
            record["traceID"] = truth(3 - i)

    out = report(write_cell(tmp_path, stream=False, edit=edit))
    assert out["joins"]["hop-1 -> fake-upstream"]["wrong"] == 4
    assert out["joins"]["hop-1 -> fake-upstream"]["correct"] == 8
    assert out["wrong"] == 4


def test_the_command_line_scores_a_run(tmp_path, capsys):
    run = tmp_path / "run"
    run.mkdir()
    write_cell(run)
    rc.main([str(run), "--window-us", str(WINDOW_US)])
    text = capsys.readouterr().out
    assert "n1" in text and "hop-1 -> fake-upstream" in text and "wrong links in the run: 0" in text


def test_the_command_line_gives_json(tmp_path, capsys):
    run = tmp_path / "run"
    run.mkdir()
    write_cell(run)
    rc.main([str(run), "--window-us", str(WINDOW_US), "--json"])
    data = json.loads(capsys.readouterr().out)
    assert data["n1"]["wrong"] == 0
    assert "finals" not in data["n1"]
