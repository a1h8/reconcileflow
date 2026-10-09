"""analyze_obi_log.py and analyze_multihop.py against OBI's own printer output.

The reference line is OBI's golden output for its text trace printer
(pkg/export/debug/debug_test.go, TestTracePrinterResolve_PrinterText), with the
timestamp prefix its test strips. Deriving fixtures from the regexes under test
would only show that each regex matches itself.
"""

import importlib.util
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "m0-harness"


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, HARNESS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


obi_log = load_module("analyze_obi_log")
multihop = load_module("analyze_multihop")
SCRIPTS = [obi_log, multihop]

TID = "01020300000000000000000000000000"
OTHER = "f" * 32
UPSTREAM = (
    "2026-09-29 10:00:00.12345 "
    "(25µs[20µs]) HTTP(subType=0) 200 method path(route) [peer as peername.otherns:1234]->"
    "[host as hostname.foo:5678] contentLen:1024B responseLen:2048B svc=[foo/bar go]"
    f" traceparent=[00-{TID}-0102030000000000[0102040000000000]-01]"
)


def client(line, tid=TID, dest="hostname.foo:5678"):
    return (
        line.replace("HTTP(subType=", "HTTPClient(subType=")
        .replace(TID, tid)
        .replace("hostname.foo:5678", dest)
    )


# Valid printer output the regexes cannot read: OBI omits "(route)" when the
# route is empty, prints traceparent=[] without a valid trace ID, and leaves a
# double space when the method is empty.
UNREADABLE = {
    "no route": UPSTREAM.replace("path(route)", "path"),
    "no traceparent": UPSTREAM.split(" traceparent=")[0] + " traceparent=[]",
    "no method": UPSTREAM.replace("200 method path", "200  path"),
}


def test_reads_upstream_line():
    assert obi_log.analyze([UPSTREAM], {TID}) == {("foo/bar", "HTTP"): [1, 0]}
    assert multihop.analyze([UPSTREAM], {TID}) == {("foo/bar", "HTTP", "hostname.foo:5678"): [1, 0]}


@pytest.mark.parametrize("script", SCRIPTS)
def test_trace_id_absent_from_ground_truth_is_self_authored(script):
    lines = [UPSTREAM, client(UPSTREAM, tid=OTHER), UPSTREAM.replace("HTTP(", "GRPC(")]
    totals = {key[:2]: counts for key, counts in script.analyze(lines, {TID}).items()}
    assert totals == {("foo/bar", "HTTP"): [1, 0], ("foo/bar", "HTTPClient"): [1, 1]}


def test_both_scripts_agree_once_destinations_are_summed():
    lines = [UPSTREAM, client(UPSTREAM), client(UPSTREAM, OTHER, "traefik:9081")]
    summed = {}
    for (svc, event, _), (total, bad) in multihop.analyze(lines, {TID}).items():
        tb = summed.setdefault((svc, event), [0, 0])
        tb[0] += total
        tb[1] += bad
    assert summed == obi_log.analyze(lines, {TID})


@pytest.mark.parametrize("script", SCRIPTS)
@pytest.mark.parametrize("line", UNREADABLE.values(), ids=UNREADABLE.keys())
def test_unreadable_http_line_is_counted_not_dropped(script, line):
    lines = [UPSTREAM, line, "unrelated log line"]
    assert script.count_unparsed(lines) == 1
    assert script.count_unparsed([UPSTREAM, client(UPSTREAM)]) == 0


def write_inputs(tmp_path, lines):
    log = tmp_path / "obi.log"
    log.write_text("".join(f"{line}\n" for line in lines))
    gt = tmp_path / "load-gen.jsonl"
    gt.write_text(f'{{"trace_id": "{TID}"}}\n\n')
    return str(log), str(gt)


@pytest.mark.parametrize("script", SCRIPTS)
def test_main_fails_when_its_window_holds_an_unreadable_line(tmp_path, capsys, script):
    log, gt = write_inputs(tmp_path, [UPSTREAM, client(UPSTREAM, OTHER), UNREADABLE["no route"]])
    assert script.main([log, gt, "0", "2"]) == 0
    assert "self_authored=    1 (100.0%)" in capsys.readouterr().out
    assert script.main([log, gt, "0", "3"]) == 1
    assert "INVALID: 1 HTTP/HTTPClient" in capsys.readouterr().err


def test_obi_log_window_defaults_to_the_whole_log(tmp_path, capsys):
    log, gt = write_inputs(tmp_path, [UPSTREAM, UPSTREAM])
    assert obi_log.main([log, gt]) == 0
    assert "total=    2 self_authored=    0" in capsys.readouterr().out
