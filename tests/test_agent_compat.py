"""OBI and Beyla are both supported agents: pin that the analyses read both.

The fixtures are real server spans captured from the A2a runs (OBI v0.14.0,
Beyla v3.38.0), one each, with the load-gen record of the same request.
"""

import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "m0-harness"
FIXTURES = Path(__file__).parent / "fixtures" / "agent-json"
AGENTS = ("obi-v0.14.0", "beyla-v3.38.0")


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


load_module("analyze_multihop")  # imported by analyze_run
analyze_run = load_module("analyze_run")
h1 = load_module("h1_timestamps")


def fixture(agent):
    return json.loads((FIXTURES / f"{agent}.json").read_text())


def test_both_agents_emit_the_same_span_fields():
    obi, beyla = (fixture(a)["span"] for a in AGENTS)
    assert sorted(obi) == sorted(beyla)
    assert sorted(obi["attributes"]) == sorted(beyla["attributes"])


@pytest.mark.parametrize("agent", AGENTS)
def test_propagation_scoring_reads_the_agent_span(agent):
    data = fixture(agent)
    line = json.dumps([data["span"]]) + "\n"
    result = analyze_run.evaluate_json([data["load_gen_row"]], [line], data["port"])
    assert result["status"] == "VALID"
    assert result["correct"] == 1


@pytest.mark.parametrize("agent", AGENTS)
def test_timestamp_analysis_reads_the_agent_span(agent):
    data = fixture(agent)
    spans = h1.obi_server_spans([json.dumps([data["span"]]) + "\n"], data["port"])
    assert len(spans) == 1
    result = h1.score_obi([data["load_gen_row"]], spans)
    assert result["paired"] == 1
    for field in ("start", "handlerStart", "end"):
        assert int(data["span"][field]) > 0  # microseconds since the epoch, as a string


AGENT_SH = HARNESS / "scripts" / "agent.sh"
BUILDINFO = {
    "obi": "go.opentelemetry.io/obi/pkg/buildinfo",
    "beyla": "github.com/grafana/beyla/v3/pkg/buildinfo",
}


@pytest.mark.skipif(shutil.which("go") is None, reason="needs the go tool")
@pytest.mark.parametrize("kind", ["obi", "beyla", None])
def test_agent_kind_is_detected_from_buildinfo(kind, tmp_path):
    # A stand-in binary linked with the same -X flags as the real agents: the
    # detection reads the link flags recorded in the build info, nothing else.
    (tmp_path / "go.mod").write_text("module stand.in/agent\n\ngo 1.21\n")
    (tmp_path / "main.go").write_text("package main\n\nfunc main() {}\n")
    flags = [] if kind is None else [f"-ldflags=-X '{BUILDINFO[kind]}.Version=v0.0.0'"]
    subprocess.run(
        ["go", "build", *flags, "-o", "agent", "."], cwd=tmp_path, check=True, capture_output=True
    )
    probe = subprocess.run(
        ["bash", "-c", f'source "{AGENT_SH}"; agent_kind "{tmp_path / "agent"}"'],
        capture_output=True,
        text=True,
    )
    if kind is None:
        assert probe.returncode != 0
    else:
        assert probe.stdout.strip() == kind
        prefix = subprocess.run(
            ["bash", "-c", f'source "{AGENT_SH}"; agent_env_prefix {kind}'],
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert prefix == {"obi": "OTEL_EBPF", "beyla": "BEYLA"}[kind]
