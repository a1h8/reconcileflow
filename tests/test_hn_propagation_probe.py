"""hn-propagation.sh with PROBE_TCP_SEQ=1 (phase 4, 4c (a)).

The bench then runs stream-probe on fake-upstream's port next to the agent
and fake-upstream logs each request's stream position (-stream-offsets).
What runs as root is checked by the user's runs; this pins what can be
checked without it: settings refused before anything is built, and the
command lines a cell would run.
"""

import os
import shutil
import subprocess

import pytest

from test_agent_compat import HARNESS

SCRIPT = HARNESS / "scripts" / "hn-propagation.sh"

pytestmark = [
    pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash"),
    pytest.mark.skipif(os.geteuid() == 0, reason="phase 1 refuses root first"),
]


def phase1(tmp_path, **env):
    """Run phase 1 with env; it must stop before building anything."""
    run_dir = tmp_path / "run"
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=HARNESS,
        env={
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path),
            "NO_AGENT": "1",
            "RUN_DIR": str(run_dir),
            **env,
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0, result.stdout
    assert not run_dir.exists(), "refused settings must stop before the build"
    return result.stderr


def headers(tmp_path):
    d = tmp_path / "bpf"
    (d / "vendor").mkdir(parents=True)
    (d / "vmlinux.h").write_text("")
    return str(d)


def test_probe_setting_must_be_0_or_1(tmp_path):
    assert "PROBE_TCP_SEQ must be 0 or 1" in phase1(tmp_path, PROBE_TCP_SEQ="2")


def test_probe_needs_http1_towards_fake_upstream(tmp_path):
    err = phase1(
        tmp_path, PROBE_TCP_SEQ="1", HOP_UPSTREAM_PROTO="h2", BPF_HEADERS=headers(tmp_path)
    )
    assert "PROBE_TCP_SEQ=1 needs HOP_UPSTREAM_PROTO=h1" in err


@pytest.mark.parametrize("missing", ["vmlinux.h", "vendor"])
def test_probe_needs_the_kernel_headers(tmp_path, missing):
    d = headers(tmp_path)
    target = os.path.join(d, missing)
    (os.rmdir if missing == "vendor" else os.remove)(target)
    err = phase1(tmp_path, PROBE_TCP_SEQ="1", HOP_UPSTREAM_PROTO="h1", BPF_HEADERS=d)
    assert "BPF_HEADERS" in err and "vmlinux.h" in err


def cell_commands(**env):
    """The pieces of a cell's command lines, from the script's own functions."""
    script = (
        f"source {SCRIPT}; "
        'echo "fu: ${FU_FLAGS[*]}"; '
        'probe_args /run/n1 /run/bin 18443; echo "probe: ${PROBE_ARGS[*]}"; '
        'decode_args /run/n1 18443; echo "decode: ${DECODE_ARGS[*]}"'
    )
    result = subprocess.run(
        ["bash", "-c", script],
        cwd=HARNESS,
        env={"PATH": os.environ["PATH"], **env},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return dict(
        line.split(": ", 1) if ": " in line else (line.rstrip(":"), "")
        for line in result.stdout.splitlines()
    )


def test_with_the_probe_a_cell_runs_it_on_fake_upstream_and_leaves_the_server_as_is():
    # fake-upstream counting its stream hides its connection from the agent's Go
    # instrumentation (seen 2026-11-09: server spans with no address): apart.
    got = cell_commands(PROBE_TCP_SEQ="1")
    assert got["fu"] == ""
    # the probe only copies into its journal under load; the cell decodes it afterwards
    assert got["probe"] == "-obj /run/bin/probe.o -port 18443 -journal /run/n1/journal"
    assert got["decode"] == "-decode -journal /run/n1/journal -port 18443 -out /run/n1/probe.jsonl"


def test_the_servers_stream_positions_are_a_setting_of_their_own():
    assert cell_commands(PROBE_TCP_SEQ="1", FU_STREAM_OFFSETS="1")["fu"] == "-stream-offsets"


def test_stream_positions_setting_must_be_0_or_1(tmp_path):
    assert "FU_STREAM_OFFSETS must be 0 or 1" in phase1(tmp_path, FU_STREAM_OFFSETS="2")


def test_stream_positions_need_http1_towards_fake_upstream(tmp_path):
    err = phase1(tmp_path, FU_STREAM_OFFSETS="1", HOP_UPSTREAM_PROTO="h2")
    assert "FU_STREAM_OFFSETS=1 needs HOP_UPSTREAM_PROTO=h1" in err


def test_without_the_probe_a_cell_is_unchanged():
    assert cell_commands()["fu"] == ""
