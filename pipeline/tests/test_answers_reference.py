"""The answers layer against the consumer page v35, field by field (pipeline/deliver/answers/reference).

The golden side is what the page itself shows: `reference/regenerate.sh` runs the page's own script
(`page_v35.js`, in node) over the live export. The data test makes it itself, once per export and
tooling vintage, into `data/generated/scratch/answers_reference/<stamp>/` (git-ignored; ~6.5 min with
4 node workers, so the test is `slow`), and FAILS — never skips — when the bundle, the export or
node is missing.

v35 is the comparator, not v36: page v36 (`page_v36.js`, `page_data.py`) DECIDES NOTHING — it
renders the answers file it is handed — so comparing the answers layer with v36 would compare the
file with itself. v35 is the page whose own ladder and card the answers layer ports.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.deliver.answers.reference.compare import compare, diff_values, summary
from pipeline.tests.conftest import BUNDLE_HINT, EXPORT_HINT, need

ROOT = Path(__file__).resolve().parents[2]
REF = ROOT / "pipeline" / "deliver" / "answers" / "reference"
#: page_v35.js is the page's <script>, byte for byte; a change here means it is no longer the page
PAGE_SHA256 = "d6855ef640287127f0350de322cba10138fc0edb3a6c6ce6c3d633af546411f2"
#: every file the golden outputs are made with: a change to any of them is a new golden vintage
GOLDEN_TOOLING = tuple(REF / f for f in (
    "regenerate.sh", "convert.py", "sample.py", "harness.js", "golden.js", "manifest.js",
    "page_v35.js", "page_v35_inputs.json")) + (ROOT / "pipeline" / "tools" / "export_codec.py",)
#: the export files the golden outputs read (convert.load)
EXPORT_FILES = ("ui-rules-export.json", "ui-rules-guide.json")
NODE_HINT = "install node >= 18 (regenerate.sh runs the page's own script in node)"


def test_page_script_is_verbatim():
    assert hashlib.sha256((REF / "page_v35.js").read_bytes()).hexdigest() == PAGE_SHA256


def test_diff_values_reports_each_field():
    page = {"status": "keep", "daily": 2, "roles": {"a": ["governs", None]}, "lines": [{"t": "rel", "b": 30}]}
    same = {"status": "keep", "daily": 2, "roles": {"a": ["governs", None]}, "lines": [{"t": "rel", "b": 30}]}
    assert diff_values(page, same) == []
    got = {"status": "keep", "daily": 4, "roles": {"a": ["also", None], "b": ["moot", "a"]}, "lines": []}
    d = diff_values(page, got)
    paths = {p for p, _, _ in d}
    assert ("daily",) in paths and ("roles", "a", 0) in paths and ("roles", "b") in paths
    assert ("lines", "len") in paths
    assert (("daily",), 2, 4) in d


def test_compare_counts_missing_extra_and_differing_keys():
    want = {"answer": {("w", "p", "07-01", "RB", "wild"): {"daily": 1}, ("w", "p", "07-01", "CT", "wild"): {"daily": 2}}}
    got = {"answer": {("w", "p", "07-01", "RB", "wild"): {"daily": 3}, ("w", "p", "07-01", "EB", "wild"): {"daily": 2}}}
    r = compare(want, got, ["answer"])["answer"]
    assert (r["n_only_page"], r["n_only_pipeline"], r["n_differ"]) == (1, 1, 1)
    assert "answer" in summary({"answer": r})


def test_port_classes_explain_only_their_own_difference():
    """The port's classifier (reference/port.py): a documented page bug explains a difference
    only when its own feature is on the record; anything else is `unexplained`."""
    from pipeline.deliver.answers.reference.port import classify_answer, classify_row
    page = {"status": "keep", "daily": 1, "winner": "w", "narrow": None,
            "lines": [{"t": "annual", "r": "p"}], "roles": {"w": ["governs", None], "p": ["season", None]}}
    ours = {**page, "lines": [{"t": "possession_cap", "r": "p"}],
            "roles": {"w": ["governs", None], "p": ["possession_cap", None]}}
    assert classify_answer(page, page) == set()
    assert classify_answer(page, ours) == {"F6"}
    assert classify_answer(page, {**ours, "daily": 2}) == {"unexplained"}
    prow = {"kind": "keep", "pool": "p", "win": None, "members": ["CT", "RB"],
            "all_members": ["CT", "RB"], "daily": 4, "real_daily": None,
            "everyone": [["cap", "c", ["CT", "RB"]]], "groups": []}
    orow = {**prow, "groups": [[["CT", "RB"], [["origin2", "x"]]]]}
    full = {"members": ["CT", "RB"], "daily": 4, "groups": [], "conds": [], "items": []}
    assert classify_row(prow, orow, full) == {"F2"}
    assert classify_row(prow, {**orow, "daily": 3}, full) == {"F2", "unexplained"}
    # a real daily limit that moved with no F1/F3/F4/F5 feature on the row is not explained
    assert classify_row(prow, {**prow, "real_daily": [4, True, 2, 2, False]}, full) == {"unexplained"}


def test_convert_gives_the_page_a_caught_condition_as_while():
    """v35 predates `caught` (answers 2.3, Q38: a snagged fish is released): read raw, the snag
    rule is `take: 0` on ALL_FIN_FISH with no condition the page knows, so the page released every
    fish in B.C. The converter hands the page the condition in its own word (`while`, which its
    `release` test reads, page_v35.js `n.release`); a rule without `caught` is untouched."""
    from pipeline.deliver.answers.reference.convert import rule_out
    prov = {"rank": 4, "who": "Provincial", "authority": "province", "binds_to": "region",
            "uncertain": False, "why": None, "entry_name": "Everywhere in B.C.", "scope": "area"}
    snag = {"entry_id": "zp:prohibited_methods", "rule_id": "prohibited_methods.r3",
            "type": "retention_limit", "family": "retention", "dimension": "daily@caught=foul_hooked",
            "label": "x", "parts": {}, "verbatim": "v", "binds": "sections", "provenance": prov,
            "fields": {"caught": ["foul_hooked"], "species": ["ALL_FIN_FISH"], "take": 0,
                       "period": "daily", "may_target": True}}
    got = rule_out(snag)
    assert got["fields"] == {"while": ["foul_hooked"], "species": ["ALL_FIN_FISH"], "take": 0,
                             "period": "daily", "may_target": True}
    assert snag["fields"]["caught"] == ["foul_hooked"] and "while" not in snag["fields"]  # not mutated
    plain = {**snag, "fields": {"species": ["RB"], "take": 0, "period": "daily"}}
    assert rule_out(plain)["fields"] == {"species": ["RB"], "take": 0, "period": "daily"}
    both = {**snag, "fields": {"caught": ["foul_hooked"], "while": ["angling"], "take": 0}}
    with pytest.raises(ValueError, match="both"):
        rule_out(both)


def test_port_explains_a_restated_rule_only_where_ours_agrees_with_the_winner():
    """ROUND15 3 (rows.py, owner): a rule of the winner's own row saying the very same thing is the
    same statement printed twice: it AGREES with the winner (no line), where the page says `also`
    (Columbia River's two "bass, no limit" rules, r5 and r9). Anything else stays unexplained."""
    from pipeline.deliver.answers.reference.port import classify_answer, classify_row, restated
    page = {"status": "nolimit", "daily": "Infinity", "winner": "w", "narrow": None,
            "lines": [{"t": "also", "r": "p"}], "roles": {"w": ["governs", None], "p": ["also", None]}}
    ours = {**page, "lines": [], "roles": {"w": ["governs", None], "p": ["agrees", "w"]}}
    assert restated(page, ours) == {"p"}
    assert classify_answer(page, ours) == {"R15.3"}
    # MUTATION: agreeing with another rule than the winner, or a number that moved, is not it
    assert classify_answer(page, {**ours, "roles": {"w": ["governs", None], "p": ["agrees", "x"]}}) == {"unexplained"}
    assert classify_answer(page, {**ours, "daily": 3}) == {"unexplained"}
    assert restated(page, {**ours, "roles": {"w": ["governs", None], "p": ["agrees", "x"]}}) == set()
    prow = {"kind": "nolimit", "pool": "w", "win": None, "members": ["LMB", "SMB"],
            "all_members": ["LMB", "SMB"], "daily": "Infinity", "real_daily": None,
            "everyone": [["also", "p", ["LMB", "SMB"]]], "groups": []}
    orow = {**prow, "everyone": []}
    full = {"members": ["LMB", "SMB"], "daily": None, "groups": [], "conds": [], "items": []}
    assert classify_row(prow, orow, full, {"p"}) == {"R15.3"}
    assert classify_row(prow, orow, full) == {"unexplained"}          # p not restated on the card
    # the restated line accounted for, a different line left over: still unexplained
    assert classify_row(prow, {**orow, "everyone": [["also", "q", ["LMB"]]]}, full, {"p"}) == \
        {"R15.3", "unexplained"}


def test_port_explains_a_shared_count_only_with_its_feature():
    """F11 (rows.DECISIONS): a count limit shared by several kinds ("3 Dolly Varden and lake trout
    combined", Region 6) gives them that many TOGETHER, so the real daily limit moves; the page
    summed each kind's own. Explained only where our row names the shared count."""
    from pipeline.deliver.answers.reference.port import classify_row
    prow = {"kind": "keep", "pool": "z6", "win": None, "members": ["CT", "DV", "RB"],
            "all_members": ["CT", "DV", "RB"], "daily": 5, "real_daily": [5, False, None, 6, False],
            "everyone": [], "groups": []}
    orow = {**prow, "real_daily": [5, False, None, 3, False]}
    full = {"members": ["CT", "DV", "RB"], "daily": 5, "groups": [], "conds": [], "items": [],
            "real_daily": {"shared_count": [{"take": 3, "by": "z6.r3"}]}}
    assert classify_row(prow, orow, full) == {"F11"}
    assert classify_row(prow, orow, {**full, "real_daily": {}}) == {"unexplained"}


def _kitimat_row(rest_bands):
    """A Kitimat River-like keep row (Region 6 trout/char, Jul 1-Oct 31): steelhead by
    cross-reference, lake trout under a sub-limit (a condition naming it), rainbow in a fact group;
    brook, brown and cutthroat trout named by nothing, so the page lumps them into its `rest` item.
    `rest_bands` gives our items for those three: {members: bands}."""
    items = [{"members": ["ST"], "bands": None, "xref": True},
             {"members": ["LT"], "bands": [[30, None, 3]]},
             {"members": ["RB"], "bands": [[30, 50, 5], [50, None, 1]]}]
    items += [{"members": list(ms), "bands": b} for ms, b in rest_bands.items()]
    return {"members": ["CT", "EB", "GB", "LT", "RB", "ST"],
            "all_members": ["RB", "ST", "LT", "EB", "GB", "CT"], "daily": 5,
            "groups": [{"members": ["ST"], "facts": [{"t": "xref", "r": "kitimat.r5", "members": ["ST"]}]},
                       {"members": ["RB"], "facts": [{"t": "steel", "r": "z6.r9", "members": ["RB"]}]}],
            "conds": [{"c": "group", "who": ["LT"], "sub": 3}],
            "everyone": [], "items": items,
            "real_daily": {"n": 5, "all": False, "sum": None, "capped_sum": 6, "rb": False,
                           "capped": ["ST", "LT", "GB", "CT"], "open": ["RB", "EB"]}}


def test_port_explains_the_pages_rest_item_as_f1_only_where_its_fish_differ():
    """F1 (rows.DECISIONS): page_v35.js `speciesItems` puts every keepable fish no group or
    condition names into ONE trailing `rest` item, and `quotaLines` gives it the stream share only
    when the share covers EVERY member. Kitimat River, Jul 1-Oct 31: rest = brook, brown and
    cutthroat trout; Region 6's '1 trout from streams' (except char) covers brown and cutthroat but
    not brook trout, so the page kept 5 for all three (real daily 5, ours 6: brown/cutthroat 1)."""
    from pipeline.deliver.answers.reference.port import classify_row
    prow = {"kind": "keep", "pool": "z6.r1", "win": None, "members": ["CT", "EB", "GB", "LT", "RB", "ST"],
            "all_members": ["RB", "ST", "LT", "EB", "GB", "CT"], "daily": 5,
            "real_daily": [5, False, None, 5, False], "everyone": [], "groups": []}
    orow = {**prow, "real_daily": [5, False, None, 6, False]}
    split = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    assert classify_row(prow, orow, split) == {"F1"}
    # MUTATION, outside Jul 1-Oct 31: no stream share, the rest fish have the same range (one item)
    same = _kitimat_row({("EB", "GB", "CT"): [[30, 50, 5], [50, None, 1]]})
    assert classify_row(prow, orow, same) == {"unexplained"}
    # MUTATION, the share covers every rest fish (brook trout named by a group, so not in rest):
    # what is left in the page's rest item has one range, so the rest item is not the cause
    covered = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    covered["groups"].append({"members": ["EB"], "facts": [{"t": "tnote", "r": "z6.r7", "members": ["EB"]}]})
    assert classify_row(prow, orow, covered) == {"unexplained"}
    # MUTATION: fish with different ranges that a group or condition names are not the rest item
    named = _kitimat_row({("GB", "CT"): [[30, None, 1]], ("EB",): [[30, 50, 5], [50, None, 1]]})
    named["conds"].append({"c": "group", "who": ["GB", "CT"], "sub": 1})
    assert classify_row(prow, orow, named) == {"unexplained"}


def test_port_explains_an_unread_orphan_count_clause_as_f16_only_with_the_orphan():
    """F16 (rows.DECISIONS, RULINGS Q2): an ORPHAN count clause (another table's, still speaking)
    caps its fish's bands like a sub-cap; page_v35.js `quotaLines` caps only from outercap/subcap
    and never reads `orphan`. Gotchen Lake, 'Trout/char daily quota = 2' under Region 5's
    '1 Dolly Varden/bull trout': ours caps DV at 1 (real daily [2, F, null, 1, F]), the page none."""
    from pipeline.deliver.answers.reference.port import PAGE_BUGS, classify_row
    assert PAGE_BUGS["F16"] == "an orphan count clause (another table's, still speaking) not read as " \
                               "a cap on its fish (rows.DECISIONS F16, RULINGS Q2)"
    prow = {"kind": "keep", "pool": "gotchen.r1", "win": None, "members": ["DV", "EB", "RB"],
            "all_members": ["DV", "EB", "RB"], "daily": 2, "real_daily": None,
            "everyone": [["orphan", "z5.r4", ["DV"]]], "groups": []}
    orow = {**prow, "real_daily": [2, False, None, 1, False]}
    orphan = {"t": "orphan", "r": "z5.r4", "members": ["DV"], "rules": ["z5.r4"], "take": 1,
              "binds": ["DV"]}
    # a fact naming some of the row's fish sits in their fact group (rows.build_model)
    full = {"members": ["DV", "EB", "RB"], "all_members": ["DV", "EB", "RB"], "daily": 2,
            "groups": [{"members": ["DV"], "facts": [orphan]}], "conds": [], "everyone": [],
            "items": [{"members": ["DV"], "bands": [[0, None, 1]]},
                      {"members": ["EB", "RB"], "bands": [[0, None, 2]]}],
            "real_daily": {"n": 2, "all": False, "sum": None, "capped_sum": 1, "rb": False,
                           "capped": ["DV"], "open": ["EB", "RB"]}}
    assert classify_row(prow, orow, full) == {"F16"}
    # one naming every fish of the row is said to everyone: the same
    said = {**full, "everyone": [{**orphan, "members": ["DV", "EB", "RB"]}],
            "groups": [{"members": ["DV"], "facts": [{"t": "tnote", "r": "z5.r8", "members": ["DV"]}]}]}
    assert classify_row(prow, orow, said) == {"F16"}

    def with_fact(l):
        return {**full, "groups": [{"members": ["DV"], "facts": [l]}]}
    # MUTATION: no orphan fact; an orphan with no take; an orphan on a fish the limit does not cap
    assert classify_row(prow, orow, with_fact({"t": "tnote", "r": "z5.r8", "members": ["DV"]})) \
        == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "take": None})) == {"unexplained"}
    assert classify_row(prow, orow, {**full, "groups": [
        {"members": ["DV"], "facts": [{"t": "tnote", "r": "z5.r8", "members": ["DV"]}]},
        {"members": ["EB"], "facts": [{**orphan, "members": ["EB"], "binds": ["EB"]}]}]}) \
        == {"unexplained"}
    # MUTATION: the orphan is not what caps the fish — its take is above the fish's range (z5's
    # '3 char' over a DV capped at 1 by something else), or its own gate (water, dates, species)
    # does not hold for the fish, so rows.fish_bands never capped it with the orphan
    assert classify_row(prow, orow, with_fact({**orphan, "take": 3})) == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "binds": []})) == {"unexplained"}
    assert classify_row(prow, orow, with_fact({**orphan, "binds": ["EB"]})) == {"unexplained"}


def test_port_attributes_polley_lakes_orphan_caps_to_f16_alone():
    """Polley Lake (the gate fixture that pins DECISIONS F16 on our side,
    test_answers_28_gate_classes): Region 5's '1 Dolly Varden' and '2 lake trout' under the lake's
    'trout 8'. The page, not reading orphans, shows no real daily limit; the port puts that on F16
    and on nothing else (its rest fish share one range, so not F1)."""
    from pipeline.deliver.answers import cli
    from pipeline.deliver.answers.reference.port import classify_row, orphan_takes
    from pipeline.tests.test_answers_28_gate_classes import JUL15, card

    class Ids(dict):
        def __missing__(self, k):
            return k
    out, P = card("polley", JUL15)
    out = orphan_takes(json.loads(json.dumps(out, default=str)), P)
    row = next(r for r in out["rows"] if r.get("pool") == "r5:polley_lake@5-2::polley_lake.r1")
    ours = cli.row_projection(row, Ids())
    assert ours["real_daily"] == [8, False, None, 3, False]
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"F16"}
    orphans = [l for g in row["groups"] for l in g["facts"]] + row["everyone"]
    assert sorted((l["take"], l["binds"]) for l in orphans if l["t"] == "orphan") \
        == [(1, ["DV"]), (2, ["LT"])]
    for l in orphans:
        if l["t"] == "orphan":
            l["take"] += 1                                          # MUTATION: above the fish's cap
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"unexplained"}
    for l in orphans:
        if l["t"] == "orphan":
            l["take"] = None                                       # MUTATION: no clause number
    assert classify_row({**ours, "real_daily": None}, ours, row) == {"unexplained"}


def test_port_gives_orphan_facts_their_clauses_take():
    """Our rows ship an orphan fact without a number (the rule holds it): the port reads the take
    off the rule, and the fish it BINDS — rows.fish_bands' own orphan gate (water None or the
    part's kind, its dates hold, it covers the fish) over the fact's fish (else the row's) — so F16
    can require that the orphan is what caps the fish. Other facts are left as they are."""
    from types import SimpleNamespace as NS
    from pipeline.deliver.answers.reference.port import orphan_takes

    def rule(take=None, water=None, species=("DV",), except_=(), dated=True):
        return NS(f={"take": take, "water": water, "dated": dated}, species=list(species),
                  except_=list(except_))
    P = NS(kind="lake", in_dates=lambda r: r.f["dated"], by_key={
        "z5::r4": rule(1), "z5::r9": rule(), "z5::r5": rule(2, "lake", ("DV", "LT")),
        "z5::r6": rule(2, "stream"), "z5::r7": rule(2, dated=False),
        "z5::r8": rule(2, species=("DV", "LT"), except_=("LT",))})
    out = {"rows": [{"members": ["DV", "LT"],
                     "everyone": [{"t": "orphan", "r": "z5::r4"}, {"t": "cap", "r": "z5::r4"},
                                  {"t": "orphan", "r": "z5::r5"}],
                     "groups": [{"members": ["DV"], "facts": [
                         {"t": "orphan", "r": "z5::r9", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r6", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r7", "members": ["DV"]},
                         {"t": "orphan", "r": "z5::r8", "members": ["DV", "LT"]}]}]},
                    {"members": [], "everyone": [], "groups": []}]}
    orphan_takes(out, P)
    row = out["rows"][0]
    assert row["everyone"] == [{"t": "orphan", "r": "z5::r4", "take": 1, "binds": ["DV"]},
                               {"t": "cap", "r": "z5::r4"},
                               {"t": "orphan", "r": "z5::r5", "take": 2, "binds": ["DV", "LT"]}]
    assert [(l["take"], l["binds"]) for l in row["groups"][0]["facts"]] == [
        (None, ["DV"]),     # no take: F16 refuses it on the take
        (2, []),            # another water's clause
        (2, []),            # out of its dates
        (2, ["DV"])]        # LT is excepted
    assert out["rows"][1] == {"members": [], "everyone": [], "groups": []}


def test_port_keys_our_rows_in_the_pages_word():
    """The page keys a row by ITS kind word ("nolimit"); ours says "no_limit" (cli.row_projection
    maps the value, cli.page_view the key). Keyed raw, every card with a no-limit row read as a row
    order/set difference."""
    from pipeline.deliver.answers.reference.port import row_key
    ids = ["a::r1", "b::r2"]
    assert row_key({"kind": "no_limit", "pool": None, "win": 1}, ids) == ("nolimit", "b::r2")
    assert row_key({"kind": "keep", "pool": 0, "win": 1}, ids) == ("keep", "a::r1")
    assert row_key({"kind": "release", "pool": None, "win": 0}, ids) == ("release", "a::r1")


# ------------------------------------------------------------------------------- the golden cache
def golden_stamp(export_dir: Path) -> str:
    """The golden vintage: the export pair it reads and every file it is made with."""
    h = hashlib.sha256()
    for p in [Path(export_dir) / f for f in EXPORT_FILES] + list(GOLDEN_TOOLING):
        h.update(p.name.encode() + b"\0" + hashlib.sha256(p.read_bytes()).digest())
    return h.hexdigest()[:16]


def golden_for(export_dir: Path, cache_root: Path, run=subprocess.run) -> Path:
    """The golden outputs of `export_dir` (`<cache_root>/<stamp>/golden`), made by regenerate.sh
    when this vintage has none (written beside, then renamed: a broken run leaves no cache)."""
    d = Path(cache_root) / golden_stamp(export_dir)
    if (d / "golden" / "manifest.json").exists():
        return d / "golden"
    tmp = d.with_name(d.name + f".tmp-{os.getpid()}")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    run(["bash", str(REF / "regenerate.sh"), str(tmp), str(export_dir)], cwd=ROOT, check=True,
        env={**os.environ, "PY": sys.executable})
    if not (tmp / "golden" / "manifest.json").exists():
        raise AssertionError(f"regenerate.sh wrote no golden manifest into {tmp}")
    shutil.rmtree(d, ignore_errors=True)
    tmp.rename(d)
    return d / "golden"


def _fake_export(where: Path, text: str = "{}") -> Path:
    x = where / "regs"
    x.mkdir(parents=True, exist_ok=True)
    for f in EXPORT_FILES:
        (x / f).write_text(text)
    return x


def test_golden_stamp_moves_with_the_export(tmp_path):
    """MUTATION: a changed export file is a new golden vintage (a stale cache would compare the
    port with another export's page); the same bytes elsewhere are the same vintage."""
    a = golden_stamp(_fake_export(tmp_path / "a"))
    assert golden_stamp(_fake_export(tmp_path / "c")) == a
    for f in EXPORT_FILES:
        x = _fake_export(tmp_path / f)
        (x / f).write_text("[]")
        assert golden_stamp(x) != a, f


def test_golden_for_makes_once_then_reads_the_cache(tmp_path):
    x = _fake_export(tmp_path)
    calls = []

    def run(cmd, **kw):
        calls.append(cmd)
        out = Path(cmd[2])
        (out / "golden").mkdir(parents=True)
        (out / "golden" / "manifest.json").write_text("{}")

    g = golden_for(x, tmp_path / "cache", run)
    assert g == tmp_path / "cache" / golden_stamp(x) / "golden" and (g / "manifest.json").exists()
    assert golden_for(x, tmp_path / "cache", run) == g
    assert len(calls) == 1 and calls[0][1] == str(REF / "regenerate.sh") and calls[0][3] == str(x)


def test_golden_for_leaves_no_cache_when_the_run_writes_nothing(tmp_path):
    """MUTATION: a run that produced no manifest is refused, and no half cache is kept."""
    x = _fake_export(tmp_path)
    with pytest.raises(AssertionError, match="no golden manifest"):
        golden_for(x, tmp_path / "cache", lambda cmd, **kw: None)
    assert not (tmp_path / "cache" / golden_stamp(x)).exists()


# ------------------------------------------------------------------------------- the data test
@pytest.fixture(scope="module")
def port_inputs():
    """(bundle, export dir, golden dir): the live bundle and its export pair, and the page's
    golden outputs of that export. Missing data or node FAILS, naming the command."""
    from pipeline.common.curated import GENERATED
    bundle = need(None, "bundle", os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite",
                  BUNDLE_HINT)
    export = need(None, "bundle", os.environ.get("ANSWERS_EXPORT_DIR") or bundle.parent.parent / "regs",
                  EXPORT_HINT)
    for f in EXPORT_FILES:
        need(None, "bundle", export / f, EXPORT_HINT)
    need(None, "bundle", bundle.with_name("verdicts.sqlite"), BUNDLE_HINT)
    if shutil.which("node") is None:
        pytest.fail(f"no node on PATH — {NODE_HINT}", pytrace=False)
    golden = golden_for(export, GENERATED.scratch / "answers_reference")
    return bundle, export, golden


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_port_on_the_pages_own_ladder(port_inputs):
    """THE PORT, ISOLATED: rows.py on the page's own golden ladder reproduces the page's card on
    every golden card, answer and row, except where a documented page bug shows (port.PAGE_BUGS) —
    pinned at ZERO unexplained differences."""
    from pipeline.deliver.answers.reference import port
    bundle, export, golden = port_inputs
    manifest = json.loads((golden / "manifest.json").read_text())
    rep = port.run(str(bundle), str(export), str(golden), "all")
    st = rep["stats"]
    assert st["cards"] == manifest["counts"]["card"] > 0, (st, manifest["counts"])
    assert st["answers"] > 0 and st["rows"] > 0, st
    assert not st.get("spp_differ"), st
    bad = {k: v for k, v in rep["classes"].items() if "unexplained" in k}
    assert not bad, (bad, {k: rep["examples"][k] for k in bad})
    assert {k.split()[1] for k in rep["classes"]} <= set(port.PAGE_BUGS), rep["classes"]
