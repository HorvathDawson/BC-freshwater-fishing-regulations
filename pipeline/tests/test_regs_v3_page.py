"""The app page renders what the pipeline hands it and computes nothing.

`app/design/regs-v3.html` used to resolve precedence in the browser — four ladders, each a
second opinion on the pipeline's. It now carries a second JSON block, `t`, written by
`pipeline/tools/emit_regs_v3_tables.py` from the same `provenance.section` /
`method_provenance.section` the artifacts render. These tests pin three things:

  1. the ladders are gone, and stay gone;
  2. every stretch in `d` has its table in `t`, and the `d` block — what `corpus.section_rules`,
     `comply` and the delta invariant read — is untouched by the emitter;
  3. the emitted tables are the pipeline's CURRENT answer, not a stale one: a sample of
     stretches re-resolved here must match what the page carries, counter for counter.
"""
from __future__ import annotations
import json
import re

import pytest

PAGE = "app/design/regs-v3.html"
LADDERS = ["kindBeaten", "authBeaten", "tribBeaten", "shutAll", "retentionTable(", "gearTable(",
           "closureCal(", "closureBlock(", "strictRank(", "sameQuestion(", "authRank("]


def _blocks():
    H = open(PAGE, encoding="utf-8").read()
    d = re.search(r'<script id="d" type="application/json">(.*?)</script>', H, re.S)
    t = re.search(r'<script id="t" type="application/json">(.*?)</script>', H, re.S)
    assert d and t, "the page needs both its rules (d) and its resolved tables (t)"
    return H, json.loads(d.group(1)), json.loads(t.group(1).replace("<\\/", "</"))


def test_the_browser_no_longer_resolves_precedence():
    H, _, _ = _blocks()
    code = re.sub(r'<script id="[dt]" type="application/json">.*?</script>', "", H, flags=re.S)
    left = [w for w in LADDERS if w in code]
    assert not left, f"browser-side precedence survived: {left}"
    assert "LEDGER.quota(" in code and "LEDGER.gear(" in code


def test_every_stretch_has_its_resolved_tables():
    _, D, T = _blocks()
    waters = [n for n in D if not n.startswith("_")]
    for w in waters:
        runs = D[w].get("runs") or []
        assert len(T["quota"].get(w, [])) == len(runs), (w, "quota tables != stretches")
        assert len(T["gear"].get(w, [])) == len(runs), (w, "gear tables != stretches")
        for i, s in enumerate(T["quota"][w]):
            assert s["water"] == w and s["stretch"] == i + 1
            for r in s["rows"]:
                for k in r["counters"] + r["behind"]:
                    assert isinstance(k, int) and 0 <= k < len(T["pool"])
                # every counter names who wrote it, in words, with the sentence
                for k in r["counters"]:
                    src = T["pool"][k]["source"]
                    assert src["tag"] and src["words"] and src["scope"], (w, i, r["heading"])
    assert len(T["pool"]) > 500 and len(T["rules"]) > 200


def test_the_page_carries_the_pipelines_current_answer():
    """A stale `t` block would be a page quietly disagreeing with the pipeline. Re-resolve a
    few stretches of different shapes and compare counter for counter."""
    from pipeline.regs.table import provenance, method_provenance
    from pipeline.tools.emit_regs_v3_tables import compact_quota, compact_gear, Pool
    _, D, T = _blocks()
    pool = T["pool"]
    sample = [("Fraser River", 1), ("Shuswap Lake", 0), ("Yakoun River", 0), ("Fording River", 0), ("Kootenay River", 3)]
    checked = 0
    for w, i in sample:
        if w not in D or i >= len(D[w].get("runs") or []):
            continue
        fp = Pool()
        fresh = compact_quota(provenance.section(w, i), fp)
        page = T["quota"][w][i]
        assert [r["key"] for r in fresh["rows"]] == [r["key"] for r in page["rows"]], (w, i, "rows differ")
        for fr, pr in zip(fresh["rows"], page["rows"]):
            assert sorted(json.dumps(fp.items[k], sort_keys=True) for k in fr["counters"]) == \
                   sorted(json.dumps(pool[k], sort_keys=True) for k in pr["counters"]), (w, i, fr["heading"], "counters differ")
            checked += len(pr["counters"])
        gp = Pool(); g = compact_gear(method_provenance.section(w, i), gp)
        pg_ = T["gear"][w][i]
        assert [gp.items[k]["method"] for k in g["rows"]] == [pool[k]["method"] for k in pg_["rows"]], (w, i, "gear rows differ")
        for fk, pk in zip(g["rows"], pg_["rows"]):
            assert gp.items[fk]["verdict"] == pool[pk]["verdict"] and gp.items[fk]["calendar"] == pool[pk]["calendar"], (w, i, pool[pk]["method"])
    assert checked > 50


def test_the_emitter_leaves_the_rules_block_alone():
    """`corpus.section_rules` reads the `d` block; the emitter must only ever add or replace `t`."""
    from pipeline.tools.emit_regs_v3_tables import BLOCK, AFTER
    H, _, _ = _blocks()
    d_before = re.search(r'<script id="d" type="application/json">(.*?)</script>', H, re.S).group(1)
    new = BLOCK.sub(lambda m: m.group(1) + "{}" + m.group(2), H, count=1)
    d_after = re.search(r'<script id="d" type="application/json">(.*?)</script>', new, re.S).group(1)
    assert d_before == d_after
    assert AFTER.search(H)
