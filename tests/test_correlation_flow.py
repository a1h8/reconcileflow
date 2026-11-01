"""Correlation as a stream, step A (authored links): one intent per test, the
done criterion of the private spec made exact: replay gives the same bytes;
another order within the lateness gives the same final proofs; an identifier
seen twice is a collision, never a link, even when it comes after the link
looked made; a masked hop leaves a typed unlinked observation at its place;
every final link joins observations of one request; nothing card-shaped.
"""

import random
import re
import tomllib
from pathlib import Path

import pytest

from reconcileflow.correlation.exact import Observation
from reconcileflow.correlation.flow import (
    Advance,
    FlowConfig,
    Kind,
    Seen,
    Status,
    replay_flow,
    topology_id,
)
from reconcileflow.correlation.topology import load

CATALOGUE = Path(__file__).parent / "fixtures" / "topologies"
DECLARATION = tomllib.loads((CATALOGUE / "t1b.toml").read_text())
T1B = load(DECLARATION)  # relay -(request_id)-> ingress -(crossing: traceparent)-> -> app
SOURCES = ("edge-log", "agent-a", "agent-b")
CFG = FlowConfig(sources=SOURCES, window_us=500)
FINAL, PROVISIONAL, SUPERSEDED = Status.FINAL, Status.PROVISIONAL, Status.SUPERSEDED

# Who sees each side of T1b, and when, after the request starts.
SIDES = (
    ("edge-log", "relay", "outbound", ("request_id",), 0),
    ("agent-a", "ingress", "inbound", ("request_id", "traceparent"), 10),
    ("agent-a", "ingress", "outbound", ("traceparent",), 20),
    ("agent-b", "app", "inbound", ("traceparent",), 30),
)


def requests(n, start=0, gap=1000):
    """n requests through T1b, one every ``gap`` microseconds, each with its own
    ids; returns the entries and, for each identity, the request it belongs to."""
    entries, truth, seq = [], {}, dict.fromkeys(SOURCES, 0)
    for i in range(n):
        ids = {"request_id": f"rid-{start + i}", "traceparent": f"tid-{start + i}"}
        t = (start + i) * gap
        for source, point, side, keys, offset in SIDES:
            obs = Observation(source, seq[source], point, side, {k: ids[k] for k in keys})
            truth[(source, seq[source])] = start + i
            seq[source] += 1
            entries.append(Seen(obs, t + offset))
    return entries, truth


def done(until):
    return [Advance(s, until) for s in SOURCES]


def finals(result):
    """Final decisions by content: what was decided, not when."""
    return {
        d.business_key: (d.kind, d.key, d.value, d.partners, d.forced)
        for d in result.effective().values()
        if d.status is FINAL
    }


# -- F1: the same journal, the same bytes ---------------------------------------


def test_f1_the_same_journal_replays_to_the_same_decisions_and_dossiers():
    entries, _ = requests(5)
    first = replay_flow(T1B, [*entries, *done(10_000)], CFG)
    second = replay_flow(T1B, [*entries, *done(10_000)], CFG)
    assert first == second
    assert len(first.dossiers) == 5  # one closed group, one proof, per request


def test_f1_every_request_is_linked_at_every_authored_join():
    entries, _ = requests(1)
    result = replay_flow(T1B, [*entries, *done(10_000)], CFG)
    kinds = {(where, d[0]) for (where, _), d in finals(result).items()}
    assert kinds == {
        ("relay -> ingress", Kind.LINKED),
        ("ingress", Kind.LINKED),
        ("ingress -> app", Kind.LINKED),
    }
    (dossier,) = result.dossiers
    assert dossier.observations == (("agent-a", 0), ("agent-a", 1), ("agent-b", 0), ("edge-log", 0))
    assert len(dossier.decisions) == 6  # two sides at each of three joins
    assert not dossier.forced


# -- F2: another order within the lateness, the same final proofs ---------------


def shuffled_journal(entries, rng, lateness):
    """Arrivals out of order by less than the lateness, with progress signals at
    the latest time seen, so groups close in the middle of the journal."""
    jitter = {id(e): rng.random() * lateness for e in entries}
    journal, latest = [], 0
    for entry in sorted(entries, key=lambda e: e.at + jitter[id(e)]):
        journal.append(entry)
        latest = max(latest, entry.at)
        if rng.random() < 0.3:
            journal += [Advance(s, latest) for s in SOURCES]
    return journal


def test_f2_permutations_within_the_lateness_end_in_the_same_final_proofs():
    config = FlowConfig(sources=SOURCES, window_us=500, lateness_us=2000)
    rng = random.Random(0)
    for _ in range(50):
        entries, _ = requests(12, gap=rng.choice([100, 400, 1000]))
        a, b = (
            replay_flow(T1B, [*shuffled_journal(entries, rng, 2000), *done(10**6)], config)
            for _ in range(2)
        )
        assert all(d.kind is not Kind.LATE for d in (*a.decisions, *b.decisions))
        assert finals(a) == finals(b)
        assert {d.dossier_id for d in a.dossiers} == {d.dossier_id for d in b.dossiers}


# -- F3: an identifier seen twice is a collision, never a link -------------------


def test_f3_a_reused_id_arriving_after_the_link_supersedes_it_with_a_collision():
    entries, _ = requests(1)
    reused = Observation("edge-log", 1, "relay", "outbound", {"request_id": "rid-0"})
    journal = [*entries, Seen(reused, 200), *done(10_000)]
    result = replay_flow(T1B, journal, CFG)
    # The relay record arrives first, alone: unlinked, then linked, then collided.
    chain = [d for d in result.decisions if d.business_key == ("relay -> ingress", ("edge-log", 0))]
    assert [(d.kind, d.status) for d in chain] == [
        (Kind.UNLINKED, PROVISIONAL),
        (Kind.LINKED, PROVISIONAL),
        (Kind.COLLIDED, PROVISIONAL),
        (Kind.COLLIDED, FINAL),
    ]
    assert result.status(chain[1]) is SUPERSEDED
    assert not [
        d
        for d in result.decisions
        if d.kind is Kind.LINKED and d.value == "rid-0" and d.status is FINAL
    ]


# -- F4: a masked hop leaves a typed unlinked observation at its place -----------


def test_f4_a_missing_observation_leaves_its_partner_unlinked_at_that_join():
    entries, _ = requests(2)
    masked = [e for e in entries if e.observation.source != "agent-b" or e.observation.seq != 1]
    result = replay_flow(T1B, [*masked, *done(10_000)], CFG)
    waiting = finals(result)[("ingress -> app", ("agent-a", 3))]
    assert waiting == (Kind.UNLINKED, "traceparent", "tid-1", (), False)
    # The rest of request 1's chain is linked: the gap is at one join, not everywhere.
    assert finals(result)[("relay -> ingress", ("agent-a", 2))][0] is Kind.LINKED


# -- F5: every final link joins observations of one request ----------------------


def test_f5_no_final_link_joins_two_requests():
    rng = random.Random(1)
    entries, truth = requests(40, gap=50)  # dense: many groups open together
    journal = shuffled_journal(entries, rng, 200)
    result = replay_flow(T1B, [*journal, *done(10**6)], FlowConfig(SOURCES, 500, lateness_us=200))
    linked = [d for d in result.effective().values() if d.kind is Kind.LINKED]
    assert len(linked) == 40 * 6
    for d in linked:
        own = truth[d.business_key[1]]
        assert {truth[p] for p in d.partners} == {own}


# -- F6: redeliveries ------------------------------------------------------------


def test_f6_a_redelivered_observation_is_counted_once():
    entries, _ = requests(2)
    result = replay_flow(T1B, [*entries, entries[0], entries[5], *done(10_000)], CFG)
    assert result.duplicates == 2
    assert finals(result) == finals(replay_flow(T1B, [*entries, *done(10_000)], CFG))


def test_f6_the_same_identity_with_another_content_is_refused():
    entries, _ = requests(1)
    clash = Observation("edge-log", 0, "relay", "outbound", {"request_id": "other"})
    with pytest.raises(ValueError, match="delivered with different content"):
        replay_flow(T1B, [*entries, Seen(clash, 0)], CFG)


# -- F7: an observation under the watermark is LATE and changes no final ---------


def test_f7_a_late_observation_is_typed_and_leaves_finals_alone():
    entries, _ = requests(1)
    before = replay_flow(T1B, [*entries, *done(5000)], CFG)
    late = Observation("edge-log", 1, "relay", "outbound", {"request_id": "rid-0"})
    after = replay_flow(T1B, [*entries, *done(5000), Seen(late, 100)], CFG)
    (typed,) = [d for d in after.decisions if d.kind is Kind.LATE]
    assert (typed.business_key, typed.status) == (("", ("edge-log", 1)), FINAL)
    assert {k: v for k, v in finals(after).items() if k[0]} == finals(before)


# -- F8: a silent source holds everything, unless a bound is declared ------------


def test_f8_a_declared_source_that_never_reports_keeps_everything_open():
    entries, _ = requests(2)
    config = FlowConfig(sources=(*SOURCES, "agent-c"), window_us=500)
    result = replay_flow(T1B, [*entries, *done(10_000)], config)
    assert finals(result) == {} and result.dossiers == ()


def test_f8_past_the_silence_bound_groups_close_and_are_marked_forced():
    entries, _ = requests(2)
    config = FlowConfig(sources=(*SOURCES, "agent-c"), window_us=500, max_silence_us=3000)
    result = replay_flow(T1B, [*entries, *done(10_000)], config)
    assert len(result.dossiers) == 2 and all(d.forced for d in result.dossiers)
    assert all(v[4] for v in finals(result).values())


# -- F9: nothing card-shaped in a journal or a proof (Q6, as a test) -------------

_CARD = re.compile(r"(?<![0-9A-Za-z])(?:\d[ -]?){12,18}\d(?![0-9A-Za-z])")


def _luhn(digits):
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d) * (2 if i % 2 else 1)
        total += n - 9 if n > 9 else n
    return total % 10 == 0


def card_shaped(text):
    return [m for m in _CARD.findall(text) if _luhn(re.sub(r"\D", "", m))]


def test_f9_the_detector_finds_a_known_test_number_and_not_ids():
    assert card_shaped("pan=4111 1111 1111 1111;") == ["4111 1111 1111 1111"]
    assert card_shaped("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01") == []


def test_f9_no_journal_or_dossier_of_these_tests_is_card_shaped():
    entries, _ = requests(50)
    result = replay_flow(T1B, [*entries, *done(10**6)], CFG)
    assert card_shaped(repr(entries)) == [] and card_shaped(repr(result)) == []


# -- F10: what a dossier carries ------------------------------------------------


def test_f10_the_topology_fingerprint_does_not_depend_on_declaration_order():
    reordered = dict(reversed(list(DECLARATION.items())))
    reordered["points"] = dict(reversed(list(DECLARATION["points"].items())))
    for point in reordered["points"].values():
        if "inbound" in point:
            point["inbound"] = list(reversed(point["inbound"]))
    assert topology_id(load(reordered)) == topology_id(T1B)
    assert topology_id(load(tomllib.loads((CATALOGUE / "t1a.toml").read_text()))) != topology_id(
        T1B
    )


def test_f10_a_dossier_names_its_topology_settings_engine_and_positions():
    entries, _ = requests(1)
    (dossier,) = replay_flow(T1B, [*entries, *done(10_000)], CFG).dossiers
    assert dossier.topology_id == topology_id(T1B)
    assert dossier.config_id == CFG.config_id()
    assert dossier.engine_version
    # agent-a 0 and 1, agent-b 0, edge-log 0 sit at journal positions 1, 2, 3, 0.
    assert dossier.positions == (1, 2, 3, 0)


def test_f10_every_setting_changes_the_config_fingerprint():
    configs = [
        CFG,
        FlowConfig(sources=(*SOURCES, "agent-c"), window_us=500),
        FlowConfig(sources=SOURCES, window_us=600),
        FlowConfig(sources=SOURCES, window_us=500, lateness_us=1),
        FlowConfig(sources=SOURCES, window_us=500, max_silence_us=1),
    ]
    assert len({c.config_id() for c in configs}) == len(configs)


# -- refusals -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (FlowConfig(sources=(), window_us=500), "sources"),
        (FlowConfig(sources=("a", "a"), window_us=500), "sources"),
        (FlowConfig(sources=SOURCES, window_us=-1), "window_us"),
        (FlowConfig(sources=SOURCES, window_us=500, lateness_us=-1), "lateness_us"),
        (FlowConfig(sources=SOURCES, window_us=500, max_silence_us=0), "max_silence_us"),
    ],
)
def test_settings_out_of_range_are_refused(config, message):
    with pytest.raises(ValueError, match=message):
        replay_flow(T1B, [], config)


def test_an_observation_from_an_undeclared_source_is_refused():
    obs = Observation("stranger", 0, "relay", "outbound", {"request_id": "rid-0"})
    with pytest.raises(ValueError, match="undeclared source 'stranger'"):
        replay_flow(T1B, [Seen(obs, 0)], CFG)
    with pytest.raises(ValueError, match="undeclared source 'stranger'"):
        replay_flow(T1B, [Advance("stranger", 0)], CFG)


def test_a_topology_with_a_density_has_a_stable_fingerprint_too():
    declaration = tomllib.loads((CATALOGUE / "t2c.toml").read_text())
    declaration["hops"][0]["rate"] = 100_000
    assert topology_id(load(declaration)) == topology_id(load(declaration))
    declaration["hops"][0]["rate"] = 200_000
    assert topology_id(load(declaration)) != topology_id(
        load(tomllib.loads((CATALOGUE / "t2c.toml").read_text()))
    )


def test_a_final_decision_that_nothing_replaces_keeps_its_status():
    entries, _ = requests(1)
    result = replay_flow(T1B, [*entries, *done(10_000)], CFG)
    last = result.decisions[-1]
    assert result.status(last) is FINAL


@pytest.mark.parametrize("entry", [object(), Observation("edge-log", 0, "relay", "outbound", {})])
def test_an_entry_that_is_neither_seen_nor_advance_is_refused(entry):
    with pytest.raises(ValueError, match="unknown journal entry"):
        replay_flow(T1B, [entry], CFG)


@pytest.mark.parametrize("bad", [1.5, "10", True])
def test_times_must_be_integer_microseconds(bad):
    obs = Observation("edge-log", 0, "relay", "outbound", {"request_id": "rid-0"})
    with pytest.raises(ValueError, match="at must be an integer of microseconds"):
        replay_flow(T1B, [Seen(obs, bad)], CFG)
    with pytest.raises(ValueError, match="until must be an integer of microseconds"):
        replay_flow(T1B, [Advance("edge-log", bad)], CFG)


# -- F3b: a value of a closed group, on time but past the window -----------------


def test_f3b_a_value_seen_again_after_its_group_closed_is_typed_not_linked():
    entries, _ = requests(1)
    again = [
        Seen(Observation("edge-log", 1, "relay", "outbound", {"request_id": "rid-0"}), 3000),
        Seen(Observation("agent-a", 2, "ingress", "inbound", {"request_id": "rid-0"}), 3010),
    ]
    before = replay_flow(T1B, [*entries, *done(2000)], CFG)
    result = replay_flow(T1B, [*entries, *done(2000), *again, *done(10_000)], CFG)
    typed = {
        d.business_key: (d.kind, d.status) for d in result.decisions if d.business_key[0] == ""
    }
    assert typed == {
        ("", ("edge-log", 1)): (Kind.VALUE_AFTER_FINAL, FINAL),
        ("", ("agent-a", 2)): (Kind.VALUE_AFTER_FINAL, FINAL),
    }
    assert not [
        d
        for d in result.decisions
        if d.kind is Kind.LINKED and d.business_key[1] in {("edge-log", 1), ("agent-a", 2)}
    ]
    # The closed group's proof is untouched.
    assert result.dossiers == before.dossiers


# -- mutation testing: what the tests above left unpinned ------------------------

from dataclasses import replace  # noqa: E402

from reconcileflow.stream import _engine_version  # noqa: E402

EL, A0, A1, B0 = ("edge-log", 0), ("agent-a", 0), ("agent-a", 1), ("agent-b", 0)
RI, IN, IA = "relay -> ingress", "ingress", "ingress -> app"
P, F = "PROVISIONAL", "FINAL"


def test_every_decision_of_one_request_is_pinned_whole():
    # Positions: the four observations 0-3, the three progress signals 4-6; the
    # group closes on the last one. Identifiers are pinned as bytes.
    entries, _ = requests(1)
    result = replay_flow(T1B, [*entries, *done(10_000)], CFG)
    got = [
        (
            d.business_key,
            d.chain_version,
            d.status.name,
            d.kind.name,
            d.key,
            d.value,
            d.partners,
            d.decided_at,
            d.final_at,
            d.forced,
            d.decision_id,
            d.supersedes,
        )
        for d in result.decisions
    ]
    assert got == [
        (
            (RI, EL),
            1,
            P,
            "UNLINKED",
            "request_id",
            "rid-0",
            (),
            0,
            None,
            False,
            "fc63ad745ccd5c81",
            None,
        ),
        (
            (IN, A0),
            1,
            P,
            "UNLINKED",
            "traceparent",
            "tid-0",
            (),
            1,
            None,
            False,
            "3a572a4a50516859",
            None,
        ),
        (
            (RI, A0),
            1,
            P,
            "LINKED",
            "request_id",
            "rid-0",
            (EL,),
            1,
            None,
            False,
            "84bf12ab3849ec02",
            None,
        ),
        (
            (RI, EL),
            2,
            P,
            "LINKED",
            "request_id",
            "rid-0",
            (A0,),
            1,
            None,
            False,
            "faae1f8e3ef278ab",
            "fc63ad745ccd5c81",
        ),
        (
            (IN, A0),
            2,
            P,
            "LINKED",
            "traceparent",
            "tid-0",
            (A1,),
            2,
            None,
            False,
            "187af8d2107cb818",
            "3a572a4a50516859",
        ),
        (
            (IN, A1),
            1,
            P,
            "LINKED",
            "traceparent",
            "tid-0",
            (A0,),
            2,
            None,
            False,
            "fc8cbbea7cbeb75e",
            None,
        ),
        (
            (IA, A1),
            1,
            P,
            "UNLINKED",
            "traceparent",
            "tid-0",
            (),
            2,
            None,
            False,
            "e3b7b1c576b35b55",
            None,
        ),
        (
            (IA, A1),
            2,
            P,
            "LINKED",
            "traceparent",
            "tid-0",
            (B0,),
            3,
            None,
            False,
            "16fabffbb9cbaaef",
            "e3b7b1c576b35b55",
        ),
        (
            (IA, B0),
            1,
            P,
            "LINKED",
            "traceparent",
            "tid-0",
            (A1,),
            3,
            None,
            False,
            "119aa148ead891d4",
            None,
        ),
        (
            (IN, A0),
            3,
            F,
            "LINKED",
            "traceparent",
            "tid-0",
            (A1,),
            6,
            6,
            False,
            "dedffee3f724346d",
            "187af8d2107cb818",
        ),
        (
            (RI, A0),
            2,
            F,
            "LINKED",
            "request_id",
            "rid-0",
            (EL,),
            6,
            6,
            False,
            "dac71e7c967500e9",
            "84bf12ab3849ec02",
        ),
        (
            (IN, A1),
            2,
            F,
            "LINKED",
            "traceparent",
            "tid-0",
            (A0,),
            6,
            6,
            False,
            "2846f028b2428714",
            "fc8cbbea7cbeb75e",
        ),
        (
            (IA, A1),
            3,
            F,
            "LINKED",
            "traceparent",
            "tid-0",
            (B0,),
            6,
            6,
            False,
            "b3a35e65a6b4213b",
            "16fabffbb9cbaaef",
        ),
        (
            (IA, B0),
            2,
            F,
            "LINKED",
            "traceparent",
            "tid-0",
            (A1,),
            6,
            6,
            False,
            "943a39f5963a63af",
            "119aa148ead891d4",
        ),
        (
            (RI, EL),
            3,
            F,
            "LINKED",
            "request_id",
            "rid-0",
            (A0,),
            6,
            6,
            False,
            "c5b83be65b8854cf",
            "faae1f8e3ef278ab",
        ),
    ]
    (dossier,) = result.dossiers
    assert dossier == replace(
        dossier,
        dossier_id="2d03877311ed933f",
        observations=(A0, A1, B0, EL),
        decisions=(
            "2846f028b2428714",
            "943a39f5963a63af",
            "b3a35e65a6b4213b",
            "c5b83be65b8854cf",
            "dac71e7c967500e9",
            "dedffee3f724346d",
        ),
        forced=False,
        topology_id="39b23e5b8d25d615",
        config_id="964457471600987f",
        engine_version=_engine_version(),
        positions=(1, 2, 3, 0),
        closed_at=6,
    )
    assert dossier.dossier_id == "2d03877311ed933f" and dossier.closed_at == 6


def test_forced_identifiers_are_pinned():
    entries, _ = requests(1)
    config = FlowConfig(sources=(*SOURCES, "agent-c"), window_us=500, max_silence_us=3000)
    result = replay_flow(T1B, [*entries, *done(10_000)], config)
    assert [d.decision_id for d in result.decisions if d.forced][:2] == [
        "6c01133f603ef777",
        "105a7f9fa997db71",
    ]
    assert result.dossiers[0].dossier_id == "2d90377d061bfa9f"


def test_collisions_and_reused_values_carry_their_key_and_value():
    entries, _ = requests(1)
    reused = Observation("edge-log", 1, "relay", "outbound", {"request_id": "rid-0"})
    result = replay_flow(T1B, [*entries, Seen(reused, 200), *done(10_000)], CFG)
    collided = {
        d.business_key: (d.key, d.value, d.partners)
        for d in result.effective().values()
        if d.kind is Kind.COLLIDED
    }
    assert collided == {
        (RI, EL): ("request_id", "rid-0", (A0,)),
        (RI, ("edge-log", 1)): ("request_id", "rid-0", (A0,)),
        (RI, A0): ("request_id", "rid-0", (EL, ("edge-log", 1))),
    }
    again = Seen(Observation("edge-log", 2, "relay", "outbound", {"request_id": "rid-0"}), 20_000)
    after = replay_flow(T1B, [*entries, *done(10_000), again, *done(30_000)], CFG)
    (typed,) = [d for d in after.decisions if d.kind is Kind.VALUE_AFTER_FINAL]
    assert (typed.key, typed.value) == ("request_id", "rid-0")


def test_a_density_changes_the_topology_fingerprint():
    declaration = tomllib.loads((CATALOGUE / "t2c.toml").read_text())
    declaration["hops"][0]["rate"] = 100_000
    one = topology_id(load(declaration))
    declaration["hops"][0]["rate"] = 200_000
    assert topology_id(load(declaration)) != one


def test_progress_never_moves_back():
    entries, _ = requests(1)
    back = [Advance("edge-log", 100)]  # after 10 000: ignored
    late = Seen(Observation("edge-log", 1, "relay", "outbound", {"request_id": "x"}), 200)
    result = replay_flow(T1B, [*entries, *done(10_000), *back, late], CFG)
    assert result.decisions[-1].kind is Kind.LATE


def test_boundaries_of_lateness_closing_and_silence():
    # An observation exactly at the watermark is on time.
    entries, _ = requests(1)
    exact = Seen(Observation("edge-log", 1, "relay", "outbound", {"request_id": "x"}), 10_000)
    assert (
        replay_flow(T1B, [*entries, *done(10_000), exact], CFG).decisions[-1].kind is Kind.UNLINKED
    )
    # A group whose horizon (30 + 500) equals the watermark stays open.
    assert replay_flow(T1B, [*entries, *done(530)], CFG).dossiers == ()
    assert len(replay_flow(T1B, [*entries, *done(531)], CFG).dossiers) == 1


def test_a_source_lagging_by_exactly_the_bound_still_holds_the_watermark():
    from reconcileflow.correlation.flow import _Flow

    flow = _Flow(T1B, FlowConfig(sources=SOURCES, window_us=500, max_silence_us=9_000))
    flow.first_at = 0
    flow.progress = {"edge-log": 10_000, "agent-a": 10_000, "agent-b": 1_000}
    assert flow._watermark() == (1_000, False)  # lag 9 000: holding
    flow.progress["agent-b"] = 999
    assert flow._watermark() == (10_000, True)  # lag 9 001: silent, left out


def test_the_smallest_valid_settings_are_accepted():
    replay_flow(T1B, [], FlowConfig(sources=SOURCES, window_us=0, max_silence_us=1))


@pytest.mark.parametrize("sources", [("",), (1,), ("a", "")])
def test_source_names_must_be_non_empty_strings(sources):
    with pytest.raises(ValueError, match="sources"):
        replay_flow(T1B, [], FlowConfig(sources=sources, window_us=500))


def test_progress_signals_take_a_position():
    entries, _ = requests(1)
    result = replay_flow(T1B, [*done(0), *entries, *done(10_000)], CFG)
    assert [d.decided_at for d in result.decisions[:2]] == [3, 4]  # after three signals
