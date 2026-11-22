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
    monkeypatch.setattr(A, "CURATED", SimpleNamespace(base=root / "data" / "curated"))
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
CUTS = "data/curated/waters/cuts.json"


def _entry(eid: str, rules=None, matched=("wbk:1",)) -> dict:
    return {"entry_id": eid, "matched": list(matched),
            "rules": rules if rules is not None else [{"rule_id": f"{eid.split(':')[1].split('@')[0]}.r1", "take": 2}]}


def test_the_rows_changed_since_a_commit(repo):
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8"), _entry("r2:b@2-8", matched=("lake:7",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"}]})
    since = _commit(repo)
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "take": 0}]),
                                   _entry("r2:b@2-8", matched=("lake:7",))]})
    _write(repo, LOCAL, {"entries": [_entry("r2:lost_lagoon_local@2-8", matched=("wbk:329532481",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"},
                                                         {"name": "Beaver Pond", "item": "lake:7"}]})
    _commit(repo)
    ch = A.changed_rows(since)
    assert set(ch["entries"]) == {"r2:a@2-8", "r2:lost_lagoon_local@2-8"}                # b did not change
    f, before, after = ch["entries"]["r2:lost_lagoon_local@2-8"]
    assert f == LOCAL and before is None and after["matched"] == ["wbk:329532481"]
    assert any(f == VARIANTS and "lake:7" in names for f, names in ch["rows"])
    assert not any("lake:9" in names for f, names in ch["rows"])                         # an unchanged row
    assert ch["heads"]["r2:b@2-8"]["matched"] == ["lake:7"] and len(ch["files"][LOCAL]) == 1


def _changes(**kw) -> dict:
    return {"entries": {}, "rows": [], "files": {}, "heads": {}, **kw}


def _diff(**kw) -> dict:
    return {"rules": {}, "unresolved": {}, "licensing": {}, "dfo": {}, "side": {}, "diagnostics": {}, **kw}


GAINED = {"gained": 1, "lost": 0}


def test_an_added_rule_that_gains_is_explained_by_its_own_entry():
    """0.8.5 stop 7: Lost Lagoon's local-closure entry, added by T981 — its rule added, its binding +1."""
    e = _entry("r2:lost_lagoon_local@2-8", rules=[{"rule_id": "lost_lagoon_local.r1"}])
    ch = _changes(entries={e["entry_id"]: (LOCAL, None, e)}, files={LOCAL: ["7ce79dcf0"]})
    ok, said, bad = A.attribute(_diff(rules={"r2:lost_lagoon_local@2-8::lost_lagoon_local.r1": GAINED}), "b", changes=ch)
    assert ok and bad == [] and "rule lost_lagoon_local.r1 added in" in said[0] and "7ce79dcf0" in said[0]


def test_mutation_an_added_rule_whose_binding_lost_sections_stops():
    e = _entry("r2:x@2-8", rules=[{"rule_id": "x.r1"}])
    ch = _changes(entries={e["entry_id"]: (SYN, None, e)})
    ok, _said, bad = A.attribute(_diff(rules={"r2:x@2-8::x.r1": {"gained": 0, "lost": 4}}), "b", changes=ch)
    assert not ok and "was added" in bad[0] and "LOST 4" in bad[0]


def test_mutation_a_sibling_rules_edit_never_explains_another_rule():
    """Review 2026-11-21: any edit to the entry excused every rule of it."""
    b = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "take": 2}, {"rule_id": "a.r2", "take": 1}])
    a = _entry("r2:a@2-8", rules=[{"rule_id": "a.r1", "take": 0}, {"rule_id": "a.r2", "take": 1}])
    ch = _changes(entries={"r2:a@2-8": (SYN, b, a)}, heads={"r2:a@2-8": a})
    ok, said, bad = A.attribute(_diff(rules={"r2:a@2-8::a.r1": GAINED, "r2:a@2-8::a.r2": GAINED}), "b", changes=ch)
    assert not ok and len(said) == 1 and "a.r1 edited" in said[0]
    assert len(bad) == 1 and bad[0].startswith("rule r2:a@2-8::a.r2: none of its own curated inputs")


def test_mutation_a_code_only_move_stops():
    """No curated input of the rule changed (a code or atlas change moved it): never explained."""
    e = _entry("r6:skeena@6-10")
    ch = _changes(heads={"r6:skeena@6-10": e}, rows=[(VARIANTS, frozenset({"lake:99", "r6:skeena@6-10"}))])
    ok, said, bad = A.attribute(_diff(rules={"r6:skeena@6-10::skeena.r1": {"gained": 30, "lost": 2}}), "b", changes=ch)
    assert not ok and said == [] and "none of its own curated inputs" in bad[0]


def test_a_row_naming_only_the_entry_explains_nothing():
    e = _entry("r2:b@2-8", matched=("lake:7",))
    ch = _changes(heads={"r2:b@2-8": e}, rows=[("data/curated/regulations/book_name_links.json",
                                                  frozenset({"r2:b@2-8", "Beaver"}))])
    assert not A.attribute(_diff(rules={"r2:b@2-8::b.r1": GAINED}), "b", changes=ch)[0]


def test_a_changed_row_naming_a_matched_water_or_a_cut_the_rule_names_is_explained():
    e = _entry("r2:b@2-8", rules=[{"rule_id": "b.r1", "extents": [{"op": "upstream_of", "splits": ["split-n2"]}]},
                                  {"rule_id": "b.r2"}], matched=("lake:7",))
    variants = _changes(heads={"r2:b@2-8": e}, rows=[(VARIANTS, frozenset({"Beaver Pond", "lake:7"}))])
    ok, said, _bad = A.attribute(_diff(unresolved={"r2:b@2-8::b.r1": {}}), "b", changes=variants)
    assert ok and f"a row of {VARIANTS} naming lake:7" in said[0]
    cut = _changes(heads={"r2:b@2-8": e}, rows=[(CUTS, frozenset({"split-n2", "Campbell River"}))])
    assert A.attribute(_diff(rules={"r2:b@2-8::b.r1": GAINED}), "b", changes=cut)[0]
    assert not A.attribute(_diff(rules={"r2:b@2-8::b.r2": GAINED}), "b", changes=cut)[0]   # b.r2 names no cut


def test_matched_waters_explain_in_their_direction():
    b, a = _entry("r2:c@2-8", matched=("lake:1",)), _entry("r2:c@2-8", matched=("lake:1", "lake:2"))
    ch = _changes(entries={"r2:c@2-8": (SYN, b, a)})
    assert A.attribute(_diff(rules={"r2:c@2-8::c.r1": GAINED}), "b", changes=ch)[0]
    ok, _said, bad = A.attribute(_diff(rules={"r2:c@2-8::c.r1": {"gained": 0, "lost": 3}}), "b", changes=ch)
    assert not ok and "only gained matched waters" in bad[0]


def test_dfo_places_need_a_row_naming_them_and_the_side_tables_never_pass():
    ch = _changes(rows=[("data/curated/dfo_salmon/places/region-2.json", frozenset({"loc:chilliwack-1"}))])
    assert A.attribute(_diff(dfo={"loc:chilliwack-1": {}}), "b", changes=ch)[0]
    assert not A.attribute(_diff(dfo={"loc:vedder-2": {}}), "b", changes=ch)[0]
    ok, _said, bad = A.attribute(_diff(side={"tidal": {"changed": 3}}), "b", changes=ch)
    assert not ok and "never attributable" in bad[0]
