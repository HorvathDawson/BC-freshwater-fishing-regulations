"""P05: THE CARD GATE RUNS ON THE SHIPPED ANSWERS AND BLOCKS SHIPPING.

`deliver all` runs `card_check --all` after `answers` and before `store`. The gate decodes the answers file from its
bytes and proves every number on every card against the bundle's rules (`card_check.check_shipped`). It writes a stamp
(`card-check.json`, holding the sha256 of the bytes it proved), and the store refuses answers without a passing stamp
for exactly those bytes (`card_check.stamp_problems`).

The gate is only worth something if it reads the FILE. The old `check_built` rebuilt the rows from the bundle, so a
number changed in the shipped file could never fail it (memory "checks seeded from input launder"). The
`needs_bundle` test pins that by mutation: one quota number changed in the live answers fails, and the file as built
passes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from pipeline.deliver import __main__ as D
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.store.common import StoreError


# ---- the chain: card_check sits between answers and store, and a failure stops it ------------------------------

def _run_all(monkeypatch, tmp_path, fail_at=None):
    ran = []

    def step(args):
        ran.append(args[0])
        if args[0] == fail_at:
            raise subprocess.CalledProcessError(1, args)
    monkeypatch.setattr(D, "_step", step)
    monkeypatch.setattr("pipeline.common.vintage.report", lambda *a, **k: (True, "ok"))
    try:
        D.main(["all", "--out-dir", str(tmp_path)])
    except subprocess.CalledProcessError:
        pass
    return ran


def test_deliver_all_runs_the_card_gate_after_answers_and_before_store(monkeypatch, tmp_path):
    ran = _run_all(monkeypatch, tmp_path)
    assert ran == ["bundle", "verdicts", "status_index", "export", "answers", "card_check", "store"]


def test_a_failing_card_gate_stops_the_chain_before_store(monkeypatch, tmp_path):
    ran = _run_all(monkeypatch, tmp_path, fail_at="card_check")
    assert ran[-1] == "card_check" and "store" not in ran


# ---- the stamp: the store packages only bytes the gate proved -------------------------------------------------

def _stamp(dirpath: Path, raw: bytes, problems=0, parts=10, sha=None):
    st = {"card_check": {"problems": problems, "parts_checked": parts, "keys_checked": 3,
                         "answers_sha256": sha or hashlib.sha256(raw).hexdigest(), "code_sha": "x"}}
    (dirpath / CC.STAMP_NAME).write_text(json.dumps(st))


def test_answers_without_a_passing_stamp_for_their_bytes_are_refused(tmp_path):
    ans = tmp_path / "ui-rules-answers.json"
    raw = b'{"about": {}}'
    ans.write_bytes(raw)
    assert any("no card-gate stamp" in p for p in CC.stamp_problems(ans, raw))
    _stamp(tmp_path, raw, problems=2)
    assert any("found 2 problems" in p for p in CC.stamp_problems(ans, raw))
    _stamp(tmp_path, raw, sha="0" * 64)
    assert any("other answers bytes" in p for p in CC.stamp_problems(ans, raw))
    _stamp(tmp_path, raw, parts=None)
    assert any("not a whole-file run" in p for p in CC.stamp_problems(ans, raw))
    _stamp(tmp_path, raw)
    assert CC.stamp_problems(ans, raw) == []
    # one byte changed after the gate ran: refused
    assert CC.stamp_problems(ans, raw + b" ")


def test_the_store_refuses_unproved_answers_before_reading_the_bundle(tmp_path, monkeypatch):
    from pipeline.deliver.store import build as SB
    ans = tmp_path / "ui-rules-answers.json"
    ans.write_bytes(b"{}")
    monkeypatch.setattr(SB, "load_inputs", lambda a, e: (b"{}", {}, {}, {}))
    with pytest.raises(StoreError, match="not proved by the card gate"):
        SB.build(ans, tmp_path, tmp_path / "no-bundle.sqlite", tmp_path / "regs.sqlite")


# ---- the live file, mutation-pinned ----------------------------------------------------------------------------

def _live():
    from pipeline.deliver.bundle import read as R
    from pipeline.tools.export_ui_rules import OUT
    return R.BUNDLE, OUT.parent


def _vedder_keys(wire, n=1):
    from pipeline.tests.test_answers_28_live import VEDDER
    return sorted({k for k in wire["parts"][VEDDER] if k is not None})[:n]


@pytest.mark.needs_bundle
def test_a_quota_number_changed_in_the_shipped_answers_fails_the_gate(request, tmp_path):
    from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
    bundle, exp = _live()
    need(request, "bundle", bundle, BUNDLE_HINT)
    need(request, "bundle", exp / "ui-rules-answers.json", EXPORT_HINT)
    from pipeline.deliver.answers.cli import _raw
    wire = json.loads((exp / "ui-rules-answers.json").read_bytes())
    keys = _vedder_keys(wire)
    # the file as built: proved
    ok = CC.check_shipped(bundle, exp, keys=keys, log=lambda *_: None)
    assert ok["groups"] > 0 and ok["problems"] == [], ok["problems"][:5]
    # one headline number changed in a rows group the Vedder's cards use
    bad = copy.deepcopy(wire)
    sec = bad["sections"]["rows"]
    used = {i for k in keys for ref in sec["at"][k] for i in sec["frames"][ref][2]}
    row = next(i for i in sorted(used) if sec["rows"][i].get("group"))
    sec["rows"][row]["group"]["headline"]["n"] += 1
    for f in ("ui-rules-export.json", "ui-rules-guide.json"):
        (tmp_path / f).symlink_to(exp / f)
    (tmp_path / "ui-rules-answers.json").write_bytes(_raw(bad))
    got = CC.check_shipped(bundle, tmp_path, keys=keys, log=lambda *_: None)
    assert got["problems"], "the gate passed a changed quota number: it is not reading the file"
    assert got["answers_sha256"] != ok["answers_sha256"]
