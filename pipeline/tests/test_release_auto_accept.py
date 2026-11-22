"""T993 item 8: what the release lets through on its own, said — a golden re-cut that only renumbers source indexes
(`release_follows.renumbered_only`) and a binding diff every change of which a curated row changed since LATEST's commit
explains (`release_attribute`). Anything else still stops (the orchestration: test_release_command.py)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline import release_attribute as A
from pipeline import release_follows as RF


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path, monkeypatch) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.invalid")
    _git(root, "config", "user.name", "t")
    monkeypatch.setattr(RF, "REPO_ROOT", root)
    monkeypatch.setattr(A, "REPO_ROOT", root)
    cur = root / "data" / "curated"
    monkeypatch.setattr(A, "CURATED", SimpleNamespace(
        base=cur, waters=SimpleNamespace(**{k: cur / "waters" / f"{k}.json" for k in (
            "name_variants", "found_names", "continuations", "unmapped_waters", "added_streams")}),
        regulations=SimpleNamespace(book_name_links=cur / "regulations" / "book_name_links.json"),
        dfo_salmon=SimpleNamespace(places=cur / "dfo_salmon" / "places")))
    return root


def _write(root: Path, rel: str, doc) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=1, sort_keys=True))


def _commit(root: Path, msg: str = "c") -> str:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", msg)
    return _git(root, "rev-parse", "HEAD")


PAGE = "app/packages/ui-native/src/regs/fixtures/p.json"
PAGE2 = "app/packages/ui-native/src/regs/fixtures/q.json"


def _page(src, words="Release all trout", sheet=None):
    return {"keep": {"trout": {"rows": [{"src": src, "sheet": {"src": sheet or src}, "text": words}], "src": src}},
            "gear": {"src": {"keys": ["zp:bait::bait.r1"], "who": "Province ›"}}}


# ----------------------------------------------------------------------------------- renumber-only golden pages

def test_a_page_whose_sources_only_renumbered_is_a_renumber(repo):
    _write(repo, PAGE, _page([1363, 3144]))
    _write(repo, PAGE2, _page([1363, 1364]))
    _commit(repo)
    _write(repo, PAGE, _page([1364, 3145]))
    _write(repo, PAGE2, _page([1364, 1365]))
    before = {1363: {"rule_id": "kla.r1"}, 1364: {"rule_id": "kla.r2"}, 3144: {"rule_id": "possession_quota.r1"}}
    after = {1364: {"rule_id": "kla.r1"}, 1365: {"rule_id": "kla.r2"}, 3145: {"rule_id": "possession_quota.r1"}}
    ok, note = RF.renumbered_only([PAGE, PAGE2], rules=(before, after))
    assert ok and "2 golden page(s) differ only in source indexes" in note and "1363->1364" in note
    # with no source table to read (this repo has no fixture release) it is never a renumber
    ok, why = RF.renumbered_only([PAGE, PAGE2])
    assert not ok and "source table could not be read" in why


def test_mutation_two_sources_that_swap_places_are_not_a_renumber(repo):
    """Review 2026-11-21: 1->2 and 2->1 is one-to-one and consistent, yet each index now names the OTHER rule."""
    _write(repo, PAGE, _page([1, 2], sheet=[1, 2]))
    _commit(repo)
    _write(repo, PAGE, _page([2, 1], sheet=[2, 1]))
    same = {1: {"rule_id": "a.r1"}, 2: {"rule_id": "b.r1"}}
    ok, why = RF.renumbered_only([PAGE], rules=(same, same))
    assert not ok and "is another source" in why
    swapped = {1: {"rule_id": "b.r1"}, 2: {"rule_id": "a.r1"}}           # the rules moved with the indexes: a renumber
    assert RF.renumbered_only([PAGE], rules=(same, swapped))[0]


def test_the_source_table_is_the_committed_regs_fixtures_rules():
    """The table the indexes read decodes (zdict blobs), at a commit and on disk; no data needed (it is committed)."""
    import pipeline.release_follows as RF0
    on_disk, at_head = RF0.fixture_rules(None), RF0.fixture_rules("HEAD")
    assert on_disk and on_disk.keys() == at_head.keys() and all("rule_id" in r for r in list(on_disk.values())[:50])


def test_mutation_a_changed_word_is_not_a_renumber(repo):
    _write(repo, PAGE, _page([1363]))
    _commit(repo)
    _write(repo, PAGE, _page([1364], words="Release all trout and char"))
    ok, why = RF.renumbered_only([PAGE])
    assert not ok and "text" in why


def test_a_source_that_maps_two_ways_is_not_a_renumber(repo):
    """One source table: 1363 cannot be 1364 on one page and 1365 on another (that is another source, not a move)."""
    _write(repo, PAGE, _page([1363]))
    _write(repo, PAGE2, _page([1363]))
    _commit(repo)
    _write(repo, PAGE, _page([1364]))
    _write(repo, PAGE2, _page([1365]))
    ok, why = RF.renumbered_only([PAGE, PAGE2])
    assert not ok and "source 1363 is 1364 elsewhere, 1365 here" in why


def test_two_sources_becoming_one_is_not_a_renumber(repo):
    _write(repo, PAGE, _page([1, 2], sheet=[1, 2]))
    _commit(repo)
    _write(repo, PAGE, _page([5, 5], sheet=[5, 5]))
    ok, why = RF.renumbered_only([PAGE])
    assert not ok and "not one-to-one" in why


def test_a_source_added_or_an_int_outside_src_is_not_a_renumber(repo):
    _write(repo, PAGE, {**_page([1]), "daily": 2})
    _commit(repo)
    _write(repo, PAGE, {**_page([1, 2]), "daily": 2})
    assert not RF.renumbered_only([PAGE])[0]
    _write(repo, PAGE, {**_page([1]), "daily": 3})                 # a limit, not a source index
    assert not RF.renumbered_only([PAGE])[0]


def test_a_new_page_is_never_a_renumber(repo):
    _write(repo, PAGE, _page([1]))
    _commit(repo)
    _write(repo, PAGE2, _page([1]))
    ok, why = RF.renumbered_only([PAGE2])
    assert not ok and "added" in why


# ----------------------------------------------------------------------------- an explained binding diff

SYN = "data/curated/regulations/synopsis/region-2.json"
LOCAL = "data/curated/regulations/local_closures/region-2.json"
VARIANTS = "data/curated/waters/name_variants.json"
NOTES = "data/curated/waters/shore_held.json"                       # mentions waters; not a name / match file
DFO = "data/curated/dfo_salmon/places/region-2.json"


def _entry(eid: str, rules=None, matched=("wbk:1",)) -> dict:
    return {"entry_id": eid, "matched": list(matched),
            "rules": rules if rules is not None else [{"rule_id": f"{eid.split(':')[1].split('@')[0]}.r1", "take": 2}]}


def test_the_rows_changed_since_a_commit_are_the_binding_files_only(repo):
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8"), _entry("r2:b@2-8", matched=("lake:7",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"}]})
    _write(repo, NOTES, {"rows": [{"item": "lake:7", "note": "a"}]})
    since = _commit(repo)
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "take": 0}]),
                                   _entry("r2:b@2-8", matched=("lake:7",))]})
    _write(repo, LOCAL, {"entries": [_entry("r2:lost_lagoon_local@2-8", matched=("wbk:329532481",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"},
                                                         {"name": "Beaver Pond", "item": "lake:7"}]})
    _write(repo, NOTES, {"rows": [{"item": "lake:7", "note": "b"}]})
    _commit(repo)
    ch = A.changed_rows(since)
    assert set(ch["entries"]) == {"r2:a@2-8", "r2:lost_lagoon_local@2-8"}                # b did not change
    f, before, after = ch["entries"]["r2:lost_lagoon_local@2-8"]
    assert f == LOCAL and before is None and after["matched"] == ["wbk:329532481"]
    assert [(f, s) for f, names, s in ch["rows"]] == [(VARIANTS, "+")]                   # NOTES is no binding input
    assert "lake:7" in ch["rows"][0][1] and ch["heads"]["r2:b@2-8"]["matched"] == ["lake:7"]


def _changes(**kw) -> dict:
    return {"entries": {}, "rows": [], "files": {}, "heads": {}, "dfo_root": "data/curated/dfo_salmon/places/", **kw}


def _diff(**kw) -> dict:
    return {"rules": {}, "unresolved": {}, "licensing": {}, "dfo": {}, "side": {}, "diagnostics": {}, **kw}


GAINED, LOST = {"gained": 1, "lost": 0}, {"gained": 0, "lost": 3}


def test_an_added_rule_that_gains_is_explained_by_its_own_entry():
    """0.8.5 stop 7: Lost Lagoon's local-closure entry, added by T981 — its rule added, its binding +1."""
    e = _entry("r2:lost_lagoon_local@2-8", rules=[{"rule_id": "lost_lagoon_local.r1"}])
    ch = _changes(entries={e["entry_id"]: (LOCAL, None, e)}, files={LOCAL: ["7ce79dcf0"]})
    ok, said, bad = A.attribute(_diff(rules={"r2:lost_lagoon_local@2-8::lost_lagoon_local.r1": GAINED}), "b", changes=ch)
    assert ok and bad == [] and "rule lost_lagoon_local.r1 added in" in said[0] and "7ce79dcf0" in said[0]


def test_mutation_an_added_rule_whose_binding_lost_or_moved_both_ways_stops():
    e = _entry("r2:x@2-8", rules=[{"rule_id": "x.r1"}])
    ch = _changes(entries={e["entry_id"]: (SYN, None, e)})
    ok, _said, bad = A.attribute(_diff(rules={"r2:x@2-8::x.r1": LOST}), "b", changes=ch)
    assert not ok and "only grew, yet the binding LOST 3" in bad[0]
    assert "moved both ways" in A.attribute(_diff(rules={"r2:x@2-8::x.r1": {"gained": 2, "lost": 1}}), "b",
                                            changes=ch)[2][0]


def test_mutation_a_wording_or_date_edit_explains_nothing():
    """Review 2026-11-21 (b): only the rule's binding fields count."""
    b = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "verbatim": "Jan 1-Mar 31", "when": {"m": 1},
                                   "extents": [{"op": "whole"}]}])
    a = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "verbatim": "Jan 1-Apr 30", "when": {"m": 2},
                                   "extents": [{"op": "whole"}]}])
    ch = _changes(entries={"r2:a@2-8": (SYN, b, a)}, heads={"r2:a@2-8": a})
    ok, said, bad = A.attribute(_diff(rules={"r2:a@2-8::a.r1": GAINED}), "b", changes=ch)
    assert not ok and said == [] and "none of its own binding inputs" in bad[0]


def test_an_extent_added_explains_a_gain_and_never_a_loss():
    b = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "extents": [{"op": "whole"}]}])
    a = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "extents": [{"op": "whole"}, {"op": "within", "area_id": "x"}]}])
    ch = _changes(entries={"r2:a@2-8": (SYN, b, a)})
    assert A.attribute(_diff(rules={"r2:a@2-8::a.r1": GAINED}), "b", changes=ch)[0]
    assert not A.attribute(_diff(rules={"r2:a@2-8::a.r1": LOST}), "b", changes=ch)[0]
    swapped = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "extents": [{"op": "within", "area_id": "x"}]}])
    ok, _said, bad = A.attribute(_diff(rules={"r2:a@2-8::a.r1": GAINED}), "b",
                                 changes=_changes(entries={"r2:a@2-8": (SYN, b, swapped)}))
    assert not ok and "input moved both ways" in bad[0]                  # an extent replaced: no direction


def test_mutation_a_sibling_rules_edit_never_explains_another_rule():
    b = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "extents": [{"op": "whole"}]}, {"rule_id": "a.r2"}])
    a = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "extents": [{"op": "whole"}, {"op": "x"}]}, {"rule_id": "a.r2"}])
    ch = _changes(entries={"r2:a@2-8": (SYN, b, a)}, heads={"r2:a@2-8": a})
    ok, said, bad = A.attribute(_diff(rules={"r2:a@2-8::a.r1": GAINED, "r2:a@2-8::a.r2": GAINED}), "b", changes=ch)
    assert not ok and len(said) == 1 and "a.r1's extents edited" in said[0]
    assert len(bad) == 1 and bad[0].startswith("rule r2:a@2-8::a.r2: none of its own binding inputs")


def test_a_name_row_explains_its_own_direction_only():
    e = _entry("r2:b@2-8", matched=("lake:7",))
    added = _changes(heads={"r2:b@2-8": e}, rows=[(VARIANTS, frozenset({"Beaver Pond", "lake:7"}), "+")])
    ok, said, _bad = A.attribute(_diff(rules={"r2:b@2-8::b.r1": GAINED}), "b", changes=added)
    assert ok and f"a row of {VARIANTS} naming lake:7 changed" in said[0]
    assert not A.attribute(_diff(rules={"r2:b@2-8::b.r1": LOST}), "b", changes=added)[0]


def test_mutation_a_row_outside_the_binding_files_or_naming_only_the_entry_stops(repo):
    """Review 2026-11-21 (c): a file that merely mentions the water explains nothing (changed_rows never lists it)."""
    e = _entry("r2:b@2-8", matched=("lake:7",))
    _write(repo, SYN, {"entries": [e]})
    _write(repo, NOTES, {"rows": [{"item": "lake:7", "note": "a"}]})
    since = _commit(repo)
    _write(repo, NOTES, {"rows": [{"item": "lake:7", "note": "b"}]})
    _commit(repo)
    ok, _said, bad = A.attribute(_diff(rules={"r2:b@2-8::b.r1": GAINED}), since)
    assert not ok and "none of its own binding inputs" in bad[0]
    only_entry = _changes(heads={"r2:b@2-8": e}, rows=[(VARIANTS, frozenset({"r2:b@2-8"}), "+")])
    assert not A.attribute(_diff(rules={"r2:b@2-8::b.r1": GAINED}), "b", changes=only_entry)[0]


def test_unresolved_diagnostic_and_side_changes_never_pass():
    e = _entry("r2:x@2-8", rules=[{"rule_id": "x.r1"}])
    ch = _changes(entries={e["entry_id"]: (SYN, None, e)})
    assert not A.attribute(_diff(unresolved={"r2:x@2-8::x.r1": {}}), "b", changes=ch)[0]
    assert not A.attribute(_diff(diagnostics={"r2:x@2-8::x.r1 unplaced p": {}}), "b", changes=ch)[0]
    ok, _said, bad = A.attribute(_diff(side={"tidal": {"changed": 3}}), "b", changes=ch)
    assert not ok and "never attributable" in bad[0]


def test_dfo_places_need_their_own_place_row_one_way_and_the_same_outcome():
    ch = _changes(rows=[(DFO, frozenset({"loc:chilliwack-1"}), "+")])
    same = {"outcome": ["open", "open"], "gained": 2, "lost": 0}
    assert A.attribute(_diff(dfo={"loc:chilliwack-1": same}), "b", changes=ch)[0]
    assert not A.attribute(_diff(dfo={"loc:vedder-2": same}), "b", changes=ch)[0]
    assert not A.attribute(_diff(dfo={"loc:chilliwack-1": {**same, "outcome": ["open", "closed"]}}), "b",
                           changes=ch)[0]


# ------------------------------------------------- the code / atlas gate: no auto-accept at all (review (a))

def _runs(tmp: Path, handles=("h1", "h1"), registry=("g1", "g1")) -> dict:
    out = {}
    for k, h, g in zip(("before", "after"), handles, registry):
        d = tmp / k
        d.mkdir(parents=True)
        (d / "report.json").write_text(json.dumps({"handles": h, "registry_digest": g}))
        out[k] = str(d)
    return out


def test_no_auto_accept_when_the_binding_code_moved(tmp_path, monkeypatch):
    from pipeline import release as R
    monkeypatch.setattr(R, "reach_code_files", lambda: {"pipeline/regs/parsing/io.py"})
    monkeypatch.setattr(R, "committed_since", lambda rev: {"data/curated/regulations/synopsis/region-2.json"})
    assert R.binding_moved("base0", _runs(tmp_path)) == []
    monkeypatch.setattr(R, "committed_since", lambda rev: {"data/curated/regulations/synopsis/region-2.json",
                                                           "pipeline/regs/parsing/io.py"})
    got = R.binding_moved("base0", _runs(tmp_path / "x"))
    assert len(got) == 1 and "the binding code changed since base0" in got[0] and "io.py" in got[0]
    monkeypatch.setattr(R, "committed_since", lambda rev: {"pipeline/atlas/registry/build.py"})
    assert R.binding_moved("base0", _runs(tmp_path / "y"))                # anything under pipeline/atlas/


def test_no_auto_accept_when_the_atlas_moved_or_cannot_be_read(tmp_path, monkeypatch):
    from pipeline import release as R
    monkeypatch.setattr(R, "reach_code_files", lambda: set())
    monkeypatch.setattr(R, "committed_since", lambda rev: set())
    assert "the atlas changed between the runs (handles h1 -> h2)" in R.binding_moved(
        "b", _runs(tmp_path, handles=("h1", "h2")))[0]
    assert R.binding_moved("b", _runs(tmp_path / "r", registry=("g1", "g9")))
    assert "cannot be read" in R.binding_moved("b", {"before": str(tmp_path / "none"), "after": str(tmp_path)})[0]

    def boom():
        raise RuntimeError("no manifest")
    monkeypatch.setattr(R, "reach_code_files", boom)
    assert "a code change cannot be ruled out" in R.binding_moved("b", _runs(tmp_path / "z"))[0]


def test_mutation_a_code_change_and_a_same_water_edit_together_stop(tmp_path, monkeypatch):
    """Review (a): the entry edit would explain the move, but code moved too — the diff stops, saying why."""
    from pipeline import release as R
    rep = {**_diff(rules={"r2:x@2-8::x.r1": GAINED}), **_runs(tmp_path)}
    js = tmp_path / "binding_diff.json"
    js.write_text(json.dumps(rep))
    monkeypatch.setattr(R, "git_has_commit", lambda rev: True)
    monkeypatch.setattr(R, "reach_code_files", lambda: {"pipeline/atlas/reach/classify.py"})
    monkeypatch.setattr(R, "committed_since", lambda rev: {"pipeline/atlas/reach/classify.py", SYN})
    e = _entry("r2:x@2-8", rules=[{"rule_id": "x.r1"}])
    monkeypatch.setattr(A, "changed_rows", lambda since, rev="HEAD": _changes(entries={e["entry_id"]: (SYN, None, e)}))
    ok, said, bad = R.attribute_diff(js, "base0")
    assert not ok and said == [] and bad[0].startswith("NO auto-accept: the binding code changed")
    monkeypatch.setattr(R, "committed_since", lambda rev: {SYN})        # the same edit, no code: explained
    assert R.attribute_diff(js, "base0")[0]
