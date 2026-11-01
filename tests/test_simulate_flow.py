"""The N-exchange simulator (private spec, correlation in flow §7.6): the same
seed gives the same journal; without faults every exchange is linked at every
join and nothing is wrong; each injected fault gives its expected outcome;
nothing is ever linked across two exchanges."""

import importlib.util
import sys
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"
sys.path.insert(0, str(HARNESS))
spec = importlib.util.spec_from_file_location("simulate_flow", HARNESS / "simulate_flow.py")
sim = importlib.util.module_from_spec(spec)
sys.modules["simulate_flow"] = sim  # dataclasses look the module up
spec.loader.exec_module(sim)

from reconcileflow.correlation.flow import replay_flow  # noqa: E402


def run(plan):
    simulation = sim.simulate(plan)
    result = replay_flow(simulation.topology, simulation.journal, simulation.config)
    return simulation, result, sim.score(simulation, result)


def test_the_same_seed_gives_the_same_journal():
    plan = sim.Plan(exchanges=50, hops=3, faults=sim.Faults(reuse=0.1, loss=0.05, duplicates=0.1))
    assert sim.simulate(plan).journal == sim.simulate(plan).journal
    other = sim.Plan(exchanges=50, hops=3, seed=1, faults=plan.faults)
    assert sim.simulate(other).journal != sim.simulate(plan).journal


@pytest.mark.parametrize("hops", [1, 2, 5])
def test_without_faults_every_exchange_is_linked_at_every_join(hops):
    _, _, report = run(sim.Plan(exchanges=200, hops=hops))
    joins = 2 * hops + 1  # hops + 1 hops between points, plus a crossing per proxy
    assert report["linked"] == 200 * joins * 2  # both sides of every join
    assert report["wrong"] == 0
    assert report["dossiers"] == 200
    assert report["open"] == 0  # everything final at the end


def test_a_reused_trace_id_is_never_linked():
    _, _, report = run(sim.Plan(exchanges=300, hops=3, faults=sim.Faults(reuse=0.1)))
    assert report["reuse"]["injected"] > 10
    assert report["reuse"]["linked"] == 0
    assert report["reuse"]["collided"] + report["reuse"]["after_final"] > 0
    assert report["wrong"] == 0


def test_a_lost_observation_leaves_its_partners_unlinked_at_its_joins():
    _, _, report = run(sim.Plan(exchanges=300, hops=3, faults=sim.Faults(loss=0.05)))
    assert report["loss"]["injected"] > 10
    assert report["loss"]["unlinked_found"] == report["loss"]["unlinked_expected"] > 0
    assert report["wrong"] == 0


def test_disorder_within_the_lateness_changes_no_final_decision():
    plan = sim.Plan(exchanges=200, hops=3)
    _, _, ordered = run(plan)
    shuffled = sim.Plan(exchanges=200, hops=3, faults=sim.Faults(disorder_us=1_500))
    _, _, report = run(shuffled)
    assert report["finals"] == ordered["finals"]
    assert report["late"]["found"] == 0


def test_duplicates_are_counted_once():
    _, result, report = run(sim.Plan(exchanges=200, hops=2, faults=sim.Faults(duplicates=0.1)))
    assert report["duplicates"]["injected"] > 10
    assert result.duplicates == report["duplicates"]["injected"]
    assert report["wrong"] == 0 and report["linked"] == 200 * 5 * 2


def test_late_observations_are_typed_late():
    _, _, report = run(sim.Plan(exchanges=200, hops=2, faults=sim.Faults(late=0.05)))
    assert report["late"]["injected"] > 5
    assert report["late"]["found"] == report["late"]["injected"]
    assert report["wrong"] == 0


def test_all_faults_together_give_no_unexplained_wrong_link():
    # A reuse whose donor lost its observation at a join leaves the reused value
    # unique there: a wrong link equality cannot see, explained and counted.
    faults = sim.Faults(reuse=0.05, loss=0.03, duplicates=0.05, late=0.02, disorder_us=1_500)
    explained = 0
    for seed in range(5):
        _, _, report = run(sim.Plan(exchanges=300, hops=4, faults=faults, seed=seed))
        assert report["wrong_unexplained"] == 0
        explained += report["wrong_explained"]["reuse_with_donor_loss"]
    assert explained > 0  # the case does occur: not vacuous


def test_a_swap_of_trace_ids_gives_wrong_links_exactly_where_it_starts():
    # Each id stays unique per side: equality alone cannot see a swap. This is the
    # limit the per-hop oracle exists for; the simulator shows it, it does not hide it.
    _, _, report = run(sim.Plan(exchanges=300, hops=3, faults=sim.Faults(swap=0.05)))
    assert report["swap"]["injected"] > 3
    assert report["swap"]["wrong_at_start"] == 4 * report["swap"]["injected"]
    assert report["wrong_explained"]["swap"] == report["wrong"]
    assert report["wrong_unexplained"] == 0


def test_with_every_fault_each_expectation_is_met():
    faults = sim.Faults(
        reuse=0.05, swap=0.02, loss=0.02, duplicates=0.05, late=0.01, disorder_us=1_500
    )
    _, result, report = run(sim.Plan(exchanges=500, hops=5, faults=faults))
    assert report["loss"]["unlinked_found"] == report["loss"]["unlinked_expected"] > 0
    assert report["late"]["found"] == report["late"]["injected"] > 0
    assert result.duplicates == report["duplicates"]["injected"] > 0
    assert report["wrong_unexplained"] == 0 and report["open"] == 0
