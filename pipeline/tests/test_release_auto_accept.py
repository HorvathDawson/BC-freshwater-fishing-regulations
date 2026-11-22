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
    ok, note = RF.renumbered_only([PAGE, PAGE2])
    assert ok and "2 golden page(s) differ only in source indexes" in note and "1363->1364" in note


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


def _entry(eid: str, verbatim: str = "No Fishing", matched=("wbk:1",)) -> dict:
    return {"entry_id": eid, "regs_verbatim": verbatim, "matched": list(matched), "rules": [{"rule_id": "a.r1"}]}


def test_the_rows_changed_since_a_commit(repo):
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8"), _entry("r2:b@2-8", matched=("lake:7",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"}]})
    since = _commit(repo)
    _write(repo, SYN, {"entries": [_entry("r2:a@2-8", verbatim="No Fishing Jan 1-Mar 31"), _entry("r2:b@2-8",
                                                                                                matched=("lake:7",))]})
    _write(repo, LOCAL, {"entries": [_entry("r2:lost_lagoon_local@2-8", matched=("wbk:329532481",))]})
    _write(repo, VARIANTS, {"_about": "x", "variants": [{"name": "Old Lagoon", "item": "lake:9"},
                                                         {"name": "Beaver Pond", "item": "lake:7"}]})
    _commit(repo)
    ch = A.changed_rows(since)
    assert ch["entries"] == {"r2:a@2-8": SYN, "r2:lost_lagoon_local@2-8": LOCAL}          # b did not change
    assert any(f == VARIANTS and "lake:7" in names for f, names in ch["rows"])
    assert not any("lake:9" in names for f, names in ch["rows"])                         # an unchanged row
    assert ch["entry_matched"]["r2:b@2-8"] == ["lake:7"] and len(ch["files"][LOCAL]) == 1


def _changes(**kw) -> dict:
    return {"entries": {}, "rows": [], "files": {}, "entry_matched": {}, **kw}


def _diff(**kw) -> dict:
    return {"rules": {}, "unresolved": {}, "licensing": {}, "dfo": {}, "side": {}, "diagnostics": {}, **kw}


def test_a_change_on_a_changed_entry_is_explained():
    ch = _changes(entries={"r2:lost_lagoon_local@2-8": LOCAL}, files={LOCAL: ["7ce79dcf0"]})
    ok, said, bad = A.attribute(_diff(rules={"r2:lost_lagoon_local@2-8::lost_lagoon_local.r1": {}}), "base0", changes=ch)
    assert ok and bad == [] and "entry r2:lost_lagoon_local@2-8 changed in" in said[0] and "7ce79dcf0" in said[0]


def test_a_change_named_by_a_changed_row_is_explained_by_the_entrys_matched_id():
    ch = _changes(rows=[(VARIANTS, frozenset({"Beaver Pond", "lake:7"}))], entry_matched={"r2:b@2-8": ["lake:7"]})
    ok, said, _bad = A.attribute(_diff(unresolved={"r2:b@2-8::b.r1": {}}), "base0", changes=ch)
    assert ok and f"a row of {VARIANTS} naming lake:7 changed" in said[0]


def test_mutation_a_change_nothing_names_is_not_explained():
    ch = _changes(entries={"r2:a@2-8": SYN}, rows=[(VARIANTS, frozenset({"lake:7"}))])
    ok, said, bad = A.attribute(_diff(rules={"r2:a@2-8::a.r1": {}, "r6:skeena@6-10::skeena.r1": {}}), "base0",
                                changes=ch)
    assert not ok and len(said) == 1 and bad == ["rule r6:skeena@6-10::skeena.r1: no curated row changed since base0 "
                                                 "names entry r6:skeena@6-10"]


def test_dfo_places_need_a_row_naming_them_and_the_side_tables_never_pass():
    ch = _changes(rows=[("data/curated/dfo_salmon/places/region-2.json", frozenset({"loc:chilliwack-1"}))])
    assert A.attribute(_diff(dfo={"loc:chilliwack-1": {}}), "b", changes=ch)[0]
    assert not A.attribute(_diff(dfo={"loc:vedder-2": {}}), "b", changes=ch)[0]
    ok, _said, bad = A.attribute(_diff(side={"tidal": {"changed": 3}}), "b", changes=ch)
    assert not ok and "never attributable" in bad[0]
