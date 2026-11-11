"""eval-recipe-series.sh (phase 4 spec 8.14): what can be checked without
root: the settings it refuses before running anything."""

import os
import shutil
import subprocess

import pytest

from test_agent_compat import HARNESS

SCRIPT = HARNESS / "scripts" / "eval-recipe-series.sh"

pytestmark = [
    pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash"),
    pytest.mark.skipif(os.geteuid() == 0, reason="refuses root first"),
]


def refused(tmp_path, *args, **env):
    result = subprocess.run(
        ["bash", str(SCRIPT), *args],
        cwd=HARNESS,
        env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), **env},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    return result.stderr


def test_it_needs_a_prefix(tmp_path):
    assert "usage" in refused(tmp_path)


def test_it_needs_the_agents_and_traefik(tmp_path):
    assert "BEYLA_BIN must point to an executable" in refused(tmp_path, "evalr-x")


def test_a_first_day_list_must_exist(tmp_path):
    assert "no such runs list" in refused(tmp_path, "evalr-x", str(tmp_path / "none.txt"))


def test_it_needs_the_kernel_headers(tmp_path):
    exe = shutil.which("true")
    err = refused(
        tmp_path, "evalr-x", BEYLA_BIN=exe, OBI_BIN=exe, TRAEFIK_BIN=exe, BPF_HEADERS=str(tmp_path)
    )
    assert "BPF_HEADERS needs vmlinux.h" in err
