"""Curation review phase 2 (T133, curation-review PLAN §4): one vintage on screen.

An entry is BUILT when the reach run, the bundle and the release were all made from it as it is on
disk now, on the served atlas, with every split it reads built as authored (`vintage.entry_state`).
Until then it reads "awaiting build": T and R can be marked, a verified mark is refused.

The acceptance, each against artifacts written to disk by the pipeline's own writers where one
exists (`io.entry_digests`, `reach.io.write_run`, `bundle.rules.write`, `release.build.ship`,
`splits.write_anchor_record`) and read back by the one reader the tool uses (`vintage.read_built`):
  * after an entry edit, THAT entry reads "awaiting build" until reach AND deliver have run, then
    clears; every other entry stays current throughout;
  * a split added mid-review (owner 2026-10-23) puts the entries on its water in "awaiting build"
    until the atlas, reach and deliver have run, then clears — re-read from disk, no restart;
  * the Tacheeda / Twin / Main Lake shape (the bundle binds one lake of two, labels identical) is
    reported as a MISMATCH;
  * no bundle is "awaiting build" (T97 owner Q4), so no verified mark is taken without one.
The key behaviours are pinned by mutation, and each decision has one function (no second copy).
"""
from __future__ import annotations

import copy
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

import pytest

from pipeline.atlas.splits import splits as S
from pipeline.deliver.release import manifest as MAN
from pipeline.regs.parsing import io
from pipeline.regs.review import fingerprint as F
from pipeline.regs.review import vintage as V
from pipeline.tests.test_review_marks import ADAM, AMOR, BIG, SCHEMA, corpus, make_bundle, splits_doc

REPO = Path(__file__).resolve().parents[2]

# --------------------------------------------------------------------------------------------
# A world on disk: an atlas, a reach run, a bundle and a release, each "built" from the entries
# and splits.json as they are when the step runs
# --------------------------------------------------------------------------------------------


class World:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.entries = corpus()             # the catalogue on disk
        self.splits = splits_doc()          # splits.json on disk
        self.atlas = tmp / "atlas"
        self.reach = tmp / "reaches" / "full"
        self.bundle = tmp / "bundle.sqlite"
        self.release = tmp / "release"
        self.atlas.mkdir()
        self.reach.mkdir(parents=True)
        self.release.mkdir()
        self._atlas_n = 0

    # -- the pipeline's steps -------------------------------------------------------------
    def build_atlas(self, *, record: bool = True, renumber: bool = True) -> None:
        """An atlas build: new section handles (`renumber`: an atlas build is not deterministic),
        the registry, every split resolved, and (`record`) the anchors it was cut from."""
        if renumber:
            self._atlas_n += 1
        (self.atlas / "section_handles.txt").write_text(
            "".join(f"{self._atlas_n}:{i}\n" for i in range(5)))
        (self.atlas / "registry.json").write_text(json.dumps({"items": [
            {"id": i, "name": i, "kind": "stream", "section_ids": []}
            for i in ("gnis:28136", "gnis:5480", "gnis:99")]}))
        (self.atlas / "splits.resolved.json").write_text(json.dumps(
            [{"split_id": sid, "source": "curated"} for sid in S.split_index(self.splits)]))
        rec = self.atlas / S.ANCHORS_FILE
        if record:
            S.write_anchor_record(self.splits, self.atlas)
        elif rec.exists():
            rec.unlink()

    def run_reach(self, *, digests: bool = True, code: str = "") -> None:
        """A reach run: report.json as `reach.io.write_run` stamps it (handles and registry of the
        atlas it ran on, the corpus, and — the T133 field — each entry's digest). `code`: the
        builder changed, so the same corpus binds differently (another bindings `digest`)."""
        from pipeline.common.section_handles import digest_for, registry_digest_for
        rep = {"handles": digest_for(self.atlas), "registry_digest": registry_digest_for(self.atlas),
               "digest": (code + io.corpus_digest(self.entries)[::-1])[:16],
               "entries_digest": io.corpus_digest(self.entries)}
        if digests:
            rep["entry_digests"] = io.entry_digests(self.entries)
        (self.reach / "report.json").write_text(json.dumps(rep))

    def deliver(self, *, digests: bool = True, release: bool = True) -> None:
        """`pipeline.deliver all`: the bundle (its meta as `bundle.build` + `rules.write` stamp it)
        from the reach run on disk, then the release cut from it by `release.build.ship`."""
        rep = json.loads((self.reach / "report.json").read_text())
        make_bundle(self.bundle, self.entries)
        meta = {"section_handles": rep["handles"], "registry_digest": rep["registry_digest"],
                "reach_digest": rep["digest"], "entries_digest": rep["entries_digest"]}
        if digests:
            meta["entry_digests"] = json.dumps(io.entry_digests(self.entries))
        db = sqlite3.connect(self.bundle)
        db.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", sorted(meta.items()))
        db.commit()
        db.close()
        if release:
            self.ship(meta)

    def ship(self, bundle_meta: dict | None) -> Path:
        from pipeline.deliver.release.build import ship
        work = self.tmp / f"work{time.monotonic_ns()}"
        work.mkdir()
        time.sleep(0.01)                                        # a later manifest is a newer one
        return ship(work, {}, "h", self.release, bundle=bundle_meta,
                    data_release=f"r-{time.monotonic_ns()}")

    # -- what the tool reads ----------------------------------------------------------------
    def built(self) -> V.Built:
        return V.read_built(self.atlas, self.reach, self.bundle, self.release)

    def ctx(self) -> F.Context:
        return F.Context(splits=self.splits)

    def state(self, eid: str) -> dict:
        e = self.entries[eid]
        ctx = self.ctx()
        anchors = {sid: S.anchor_digest(ctx.split_index[sid])
                   for sid in F.entry_split_ids(e, ctx) if sid in ctx.split_index}
        return V.entry_state(eid, io.entry_digest(e), anchors, self.built())

    def states(self) -> dict:
        return {eid: (s["state"], s["reasons"]) for eid in self.entries
                for s in (self.state(eid),)}

    def split(self, sid: str) -> dict:
        return V.split_state(sid, S.anchor_digest(self.ctx().split_index[sid]), self.built())


@pytest.fixture
def world(tmp_path):
    w = World(tmp_path)
    w.build_atlas()
    w.run_reach()
    w.deliver()
    return w


CUR = (V.CURRENT, [])
ALL_CURRENT = {ADAM: CUR, AMOR: CUR, BIG: CUR}
ENTRY_FIX = ["scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles"]


def _edit(w: World) -> None:
    w.entries[ADAM]["rules"][1]["verbatim"] = "Artificial fly only (all year)"
    w.entries[ADAM]["regs_verbatim"] += " (all year)"


def test_a_fresh_build_is_current(world):
    assert world.states() == ALL_CURRENT
    assert world.state(ADAM)["splits_checked"] == "anchors"


# --------------------------------------------------------------------------------------------
# Acceptance: an entry edit awaits reach and deliver, then clears
# --------------------------------------------------------------------------------------------

def edit_awaits_reach_and_deliver(w: World) -> list:
    """The PLAN §4 phase-2 acceptance, step by step: what each entry reads after the edit, after
    the reach run, after deliver."""
    _edit(w)
    seen = [w.states()]
    w.run_reach()
    seen.append(w.states())
    w.deliver()
    seen.append(w.states())
    return seen


def test_after_an_entry_edit_that_entry_awaits_reach_and_deliver_then_clears(world):
    edited, reached, delivered = edit_awaits_reach_and_deliver(world)
    assert edited == {**ALL_CURRENT, ADAM: (V.AWAITING, [V.Waiting.reach_entry.value,
                                                         V.Waiting.bundle_entry.value])}
    assert reached == {**ALL_CURRENT, ADAM: (V.AWAITING, [V.Waiting.bundle_entry.value])}
    assert delivered == ALL_CURRENT
    _edit(world)
    assert world.state(ADAM)["commands"] == ENTRY_FIX


def test_the_edit_check_is_pinned_by_mutation(world, monkeypatch):
    """Comparing the entry with the BUNDLE is what holds it until deliver: a vintage that only
    asks the reach run clears it one step early."""
    real = V.entry_state

    def reach_only(eid, digest_now, anchors, built):
        return real(eid, digest_now, anchors, V.Built(**{**built.__dict__, "bundle": {
            **(built.bundle or {}), "entry_digests": (built.reach or {}).get("entry_digests")}}))
    monkeypatch.setattr(V, "entry_state", reach_only)
    edited, reached, _ = edit_awaits_reach_and_deliver(world)
    assert reached[ADAM] == CUR, "the mutation must make the reach run alone clear it"


# --------------------------------------------------------------------------------------------
# Acceptance (owner 2026-10-23): a split added mid-review, then the rebuild — no restart
# --------------------------------------------------------------------------------------------

NEW_SPLIT = {"id": "adam__falls", "label": "the falls", "kind": "point",
             "anchor": {"type": "point", "coord": [-126.2, 50.3], "is_lonlat": True}, "note": ""}


def split_added_then_rebuilt(w: World) -> list:
    w.splits["waterbodies"][0]["splits"].append(copy.deepcopy(NEW_SPLIT))
    seen = [(w.states(), w.split("adam__falls")["state"])]
    w.build_atlas()                        # atlas → (binding diff) → promote
    seen.append((w.states(), w.split("adam__falls")["state"]))
    w.run_reach()
    seen.append((w.states(), w.split("adam__falls")["state"]))
    w.deliver()
    seen.append((w.states(), w.split("adam__falls")["state"]))
    return seen


def test_a_split_added_mid_review_awaits_the_rebuild_then_clears(world):
    added, atlas, reached, delivered = split_added_then_rebuilt(world)
    # only the water the split is on waits; it says why, which split, and what to run
    assert added == ({**ALL_CURRENT, ADAM: (V.AWAITING, [V.Waiting.split_unbuilt.value])},
                     V.AWAITING)
    # (a promoted atlas is a new sid space: until the reach run and the bundle are made on it,
    # every entry waits — its bindings are the old atlas's)
    assert atlas[0] == {e: (V.AWAITING, [V.Waiting.reach_atlas.value, V.Waiting.bundle_atlas.value])
                        for e in ALL_CURRENT} and atlas[1] == V.CURRENT
    assert reached[0] == {e: (V.AWAITING, [V.Waiting.bundle_atlas.value]) for e in ALL_CURRENT}
    assert delivered == (ALL_CURRENT, V.CURRENT)


def test_the_split_check_names_the_split_and_the_atlas_commands(world):
    world.splits["waterbodies"][0]["splits"].append(copy.deepcopy(NEW_SPLIT))
    st = world.state(ADAM)
    assert st["splits"] == ["adam__falls"]
    assert st["commands"][0] == "scripts/heavy.sh atlas 30 -- python -m pipeline build --atlas"
    assert "reach.cli" in st["commands"][1] and "binding_diff" in st["commands"][2]
    assert st["commands"][3] == "scripts/heavy.sh build 30 -- python -m pipeline build --promote"


def test_a_moved_split_awaits_the_atlas(world):
    world.splits["waterbodies"][0]["splits"][1]["anchor"]["coord"] = [-126.2, 50.4]
    assert world.states() == {**ALL_CURRENT, ADAM: (V.AWAITING, [V.Waiting.split_moved.value])}
    assert world.split("adam__bridge")["state"] == V.AWAITING
    world.build_atlas(renumber=False)          # the atlas cut from it (same sections, say)
    assert world.states() == ALL_CURRENT
    world.splits["waterbodies"][0]["splits"][1]["label"] = "renamed"      # a label moves nothing
    world.splits["waterbodies"][0]["splits"][1]["_note"] = "checked"      # nor does a note
    assert world.states() == ALL_CURRENT


def test_an_atlas_without_the_anchor_record_sees_an_added_split_not_a_moved_one(world):
    """An atlas built before `split_anchors.json` (today's) can say which split IDS it holds, not
    where: a moved split is not seen until the next atlas build writes the record — and the state
    says which check it made (`splits_checked`)."""
    world.build_atlas(record=False, renumber=False)
    world.splits["waterbodies"][0]["splits"][1]["anchor"]["coord"] = [-126.2, 50.4]
    st = world.state(ADAM)
    assert (st["state"], st["splits_checked"]) == (V.CURRENT, "ids")
    world.splits["waterbodies"][0]["splits"].append(copy.deepcopy(NEW_SPLIT))
    assert world.state(ADAM)["reasons"] == [V.Waiting.split_unbuilt.value]


def test_the_split_check_is_pinned_by_mutation(world, monkeypatch):
    monkeypatch.setattr(V, "_split_reasons", lambda anchors, built: (set(), []))
    added = split_added_then_rebuilt(world)[0]
    assert added[0][ADAM] == CUR, "with no split check, an unbuilt split must read built"


# --------------------------------------------------------------------------------------------
# Every artifact counts, and a missing one waits
# --------------------------------------------------------------------------------------------

def test_a_reach_rerun_of_the_same_corpus_holds_every_entry_until_deliver(world):
    """Review (T133): a reach-code (or steelhead-list) change reruns reach on the SAME corpus. No
    entry digest moves, yet every binding the builder shows now may differ from the bundle's —
    the 2026-10-22 "reach run 17:08, bundle 15:49" pair. Every entry waits for deliver."""
    world.run_reach(code="v2")
    assert world.states() == {e: (V.AWAITING, [V.Waiting.bundle_reach.value]) for e in ALL_CURRENT}
    assert world.state(BIG)["commands"] == ENTRY_FIX
    world.deliver()
    assert world.states() == ALL_CURRENT


def test_the_rebound_check_is_pinned_by_mutation(world, monkeypatch):
    monkeypatch.setattr(V, "bundle_rebound", lambda reach, bundle: False)
    world.run_reach(code="v2")
    assert world.states() == ALL_CURRENT, "the mutation must read the old bundle as this run's"


def test_no_bundle_is_awaiting_build(world):
    """T97 owner Q4: a mark made with no bundle holds no bundle half and could never go stale, so
    with no bundle every entry awaits its build and no verified mark is taken."""
    world.bundle.unlink()
    st = world.state(BIG)
    assert st["state"] == V.AWAITING and V.Waiting.no_bundle.value in st["reasons"]
    with pytest.raises(ValueError, match="awaiting build"):
        V.refuse_unbuilt("entry", {k: "y" for k in "TRPA"}, st)


def test_a_release_cut_from_another_bundle_waits(world):
    other = {"entries_digest": "0" * 16, "reach_digest": "1" * 16}
    world.ship(other)
    assert world.states() == {e: (V.AWAITING, [V.Waiting.release_bundle.value]) for e in ALL_CURRENT}
    world.deliver()
    assert world.states() == ALL_CURRENT


def test_the_release_check_is_pinned_by_mutation(world, monkeypatch):
    monkeypatch.setattr(V, "release_is_of", lambda release, bundle: True)
    world.ship({"entries_digest": "0" * 16, "reach_digest": "1" * 16})
    assert world.states() == ALL_CURRENT, "the mutation must hide the other bundle's release"


def test_artifacts_that_predate_the_digests_wait(world):
    world.run_reach(digests=False)
    world.deliver(digests=False, release=False)
    world.ship(None)                                        # a manifest without `bundle`
    assert world.state(BIG)["reasons"] == [V.Waiting.reach_predates.value,
                                           V.Waiting.bundle_predates.value,
                                           V.Waiting.release_predates.value]


def test_nothing_built_says_so(tmp_path):
    w = World(tmp_path)
    assert w.state(BIG)["reasons"] == [V.Waiting.no_reach_run.value, V.Waiting.no_bundle.value,
                                       V.Waiting.no_release.value]


# --------------------------------------------------------------------------------------------
# What may be marked while awaiting build
# --------------------------------------------------------------------------------------------

def test_while_awaiting_build_only_t_and_r_may_be_answered(world):
    _edit(world)
    st = world.state(ADAM)
    for answers in ({k: "y" for k in "TRPA"}, {"T": "y", "R": "y", "P": "y"}, {"A": "n"}):
        with pytest.raises(ValueError, match="only T and R") as ex:
            V.refuse_unbuilt("entry", answers, st)
        assert "pipeline build --no-tiles" in str(ex.value)
    for answers in ({"T": "y", "R": "?"}, {"T": "n", "R": "y"}, {"T": "y"}):
        V.refuse_unbuilt("entry", answers, st)                       # T and R: allowed
        V.refuse_unbuilt("rule", answers, st)
    with pytest.raises(ValueError, match="split"):
        V.refuse_unbuilt("split", {"P": "y"}, st)
    # a split mark answers only P: even one that answers nothing (deferred, with a note) is a
    # call on the built map, so it waits too — while an entry's may (review T133: pinned)
    with pytest.raises(ValueError, match="split"):
        V.refuse_unbuilt("split", {}, st)
    V.refuse_unbuilt("entry", {}, st)
    V.refuse_unbuilt("entry", {k: "y" for k in "TRPA"}, world.state(BIG))   # current: anything


# --------------------------------------------------------------------------------------------
# Bundle matches: per-rule section counts, not only labels (Tacheeda / Twin / Main Lake)
# --------------------------------------------------------------------------------------------

TACHEEDA = "r7:tacheeda_lakes_north_and_south@7-16"
T_RULES = ("tacheeda_lakes.r1", "tacheeda_lakes.r2", "tacheeda_lakes.r3")


def _tacheeda_bundle(path: Path, lakes_bound: int) -> Path:
    """The real schema; Tacheeda's three rules each bind `lakes_bound` of its two lakes."""
    db = sqlite3.connect(path)
    db.executescript(SCHEMA.read_text())
    db.execute("INSERT INTO entry (entry_id, matched) VALUES (?, ?)",
               (TACHEEDA, json.dumps(["wbk:329468810", "wbk:329468815"])))
    set_id = 0
    for rid in T_RULES:
        db.execute("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts) "
                   "VALUES (?,?,?,?,?,?,?)", (TACHEEDA, rid, "retention_limit", "keep", "daily",
                                              f"label of {rid}", "{}"))
        set_id += 1
        db.execute("INSERT INTO ruleset VALUES (?,?,?,?)", (set_id, TACHEEDA, rid, "reach"))
        for lake in range(lakes_bound):
            db.execute("INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (?,?,0)",
                       (set_id * 10 + lake, set_id))
    db.commit()
    db.close()
    return path


def tacheeda_matches(tmp: Path, lakes_bound: int) -> dict:
    db = sqlite3.connect(_tacheeda_bundle(tmp / f"t{lakes_bound}.sqlite", lakes_bound))
    try:
        counts = F.rule_section_counts(db, TACHEEDA)
    finally:
        db.close()
    labels = {rid: f"label of {rid}" for rid in T_RULES}
    return V.bundle_matches(labels, dict(labels), {rid: 2 for rid in T_RULES}, counts)


def test_the_tacheeda_shape_is_reported_as_a_mismatch(tmp_path):
    """The labels agree, the bundle binds one lake of the two the builder binds now: that is a
    mismatch, rule by rule (it read "matches" while only labels were compared)."""
    got = tacheeda_matches(tmp_path, 1)
    assert got["matches"] is False
    assert got["rules"] == {rid: ["sections: 1 in the bundle, 2 now"] for rid in T_RULES}
    assert tacheeda_matches(tmp_path, 2) == {"matches": True, "rules": {}, "unchecked": []}


def test_the_count_check_is_pinned_by_mutation(tmp_path, monkeypatch):
    real = V.bundle_matches
    monkeypatch.setattr(V, "bundle_matches", lambda ln, lb, cn, cb: real(ln, lb, {}, {}))
    got = tacheeda_matches(tmp_path, 1)
    assert got["matches"] is True and got["unchecked"] == list(T_RULES), \
        "labels alone must call it a match — the counts are what catch it"


def test_bundle_matches_names_labels_missing_rules_and_what_it_could_not_check():
    got = V.bundle_matches({"a": "x", "b": "y", "c": "z"}, {"a": "x", "b": "Y", "d": "w"},
                           {"a": None, "b": 3, "c": 1}, {"b": 3})
    assert got == {"matches": False, "unchecked": ["a"],
                   "rules": {"b": ["label"], "c": ["not in the bundle"], "d": ["not in the file"]}}


def test_the_review_app_panel_reports_the_tacheeda_mismatch(tmp_path, monkeypatch):
    """The tool's bundle panel (`answer.entry_in_bundle`) over the same bundle: the same verdict,
    with each rule's count in the bundle and now."""
    monkeypatch.syspath_prepend(str(REPO / "curation-review" / "backend"))
    sys.modules.pop("answer", None)
    import answer
    monkeypatch.setattr(answer.R, "BUNDLE", str(_tacheeda_bundle(tmp_path / "b.sqlite", 1)))
    answer.bundle_meta.cache_clear()
    got = answer.entry_in_bundle(TACHEEDA, {rid: f"label of {rid}" for rid in T_RULES},
                                 {rid: 2 for rid in T_RULES})
    answer.bundle_meta.cache_clear()
    assert got["in_bundle"] is True and got["bundle_matches"] is False
    assert [(r["n_sections"], r["n_sections_now"]) for r in got["rules"]] == [(1, 2)] * 3
    assert "entry_digests" not in got["bundle"]


# --------------------------------------------------------------------------------------------
# The writers: reach, bundle, release, atlas
# --------------------------------------------------------------------------------------------

def test_the_reach_run_writes_every_entrys_digest_and_the_corpus_digest_is_unchanged(tmp_path):
    from pipeline.atlas.reach.io import write_run
    from pipeline.tests.test_reach_builder import _result
    entries = corpus()
    write_run(tmp_path / "run", _result([(ADAM, "adam_river.r1", ["s1"], None)]), entries)
    rep = json.loads((tmp_path / "run" / "report.json").read_text())
    assert rep["entry_digests"] == io.entry_digests(entries) and len(rep["entry_digests"]) == 3
    # check_corpus and the build stage keys read `entries_digest`: its value is what it was
    import hashlib
    old = hashlib.sha256(json.dumps([entries[k] for k in sorted(entries)], sort_keys=True,
                                    separators=(",", ":"), ensure_ascii=False)
                         .encode("utf-8")).hexdigest()[:16]
    assert rep["entries_digest"] == io.corpus_digest(entries) == old
    # an entry's digest moves with its content and with nothing else's
    moved = copy.deepcopy(entries)
    moved[ADAM]["regs_verbatim"] += "."
    a, b = io.entry_digests(entries), io.entry_digests(moved)
    assert {k for k in a if a[k] != b[k]} == {ADAM}


def test_the_bundle_records_every_entrys_digest(tmp_path):
    from pipeline.deliver.bundle import rules as bundle_rules
    from pipeline.tests.test_bundle_licensing import _Cov, _rules_fixture
    db, run, entries, build = _rules_fixture(tmp_path, [("r1:x@1-1", "x.r1", "s:2")], [])
    bundle_rules.write(db, run, entries, _Cov(), build_dir=build)
    meta = dict(db.execute("SELECT k, v FROM meta"))
    on_disk = io.read_entryfile(entries / "region-1.json")
    assert json.loads(meta["entry_digests"]) == io.entry_digests(on_disk)
    assert meta["entries_digest"] == io.corpus_digest(on_disk)


def test_the_release_manifest_states_its_bundle(world, tmp_path):
    rel = MAN.latest(world.release)
    m = json.loads((rel / "manifest.json").read_text())
    meta = dict(sqlite3.connect(world.bundle).execute("SELECT k, v FROM meta"))
    assert m["bundle"] == {"entries_digest": meta["entries_digest"],
                           "reach_digest": meta["reach_digest"]}
    with pytest.raises(SystemExit, match="states no"):
        MAN.bundle_block({"reach_digest": "x"})
    assert MAN.manifest("r-x", {}, "h")  # no bundle given: no `bundle` key, as before
    assert "bundle" not in MAN.manifest("r-x", {}, "h")
    assert MAN.latest(tmp_path / "nowhere") is None


def test_the_release_id_covers_the_bundle_it_names(tmp_path):
    from pipeline.deliver.release.build import ship
    ids = []
    for n, b in enumerate(({"entries_digest": "a", "reach_digest": "b"},
                           {"entries_digest": "c", "reach_digest": "b"})):
        work = tmp_path / f"w{n}"
        work.mkdir()
        ids.append(ship(work, {}, "h", tmp_path / "out", bundle=b).name)
    assert ids[0] != ids[1]


def test_the_newest_manifest_is_the_release_on_disk(tmp_path):
    for name in ("r-b", "r-a"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "manifest.json").write_text("{}")
        time.sleep(0.01)
    os.utime(tmp_path / "r-b" / "manifest.json")
    assert MAN.latest(tmp_path).name == "r-b"


def test_the_atlas_records_the_anchors_the_fingerprint_holds(tmp_path):
    doc = splits_doc()
    S.write_anchor_record(doc, tmp_path)
    rec = S.read_anchor_record(tmp_path)
    ctx = F.Context(splits=doc)
    assert set(rec) == {"adam__eve", "adam__bridge", "amor__falls"}
    for sid, d in rec.items():
        assert F.split_fingerprints(sid, ctx)["records"][0]["binding"] == d
    S.write_anchor_record(None, tmp_path)            # --no-splits: none recorded
    assert S.read_anchor_record(tmp_path) == {}
    assert S.read_anchor_record(tmp_path / "old") is None


def test_an_entry_reads_the_splits_its_binding_reads():
    ctx = F.Context(splits=splits_doc())
    es = corpus()
    assert F.entry_split_ids(es[ADAM], ctx) == {"adam__eve", "adam__bridge"}
    assert F.entry_split_ids(es[AMOR], ctx) == {"amor__falls"}
    assert F.entry_split_ids(es[BIG], ctx) == set()


# --------------------------------------------------------------------------------------------
# One decision, one function
# --------------------------------------------------------------------------------------------

def _py_sources():
    for p in sorted(list((REPO / "pipeline").rglob("*.py")) +
                    list((REPO / "curation-review" / "backend").rglob("*.py"))):
        if "tests" not in p.parts:
            yield p, p.read_text(encoding="utf-8")


def test_no_second_copy_of_the_vintage_the_digests_or_the_counts():
    hits = {"def entry_digest(": [], "def entry_state(": [], "def refuse_unbuilt(": [],
            "def bundle_matches(": [], "def split_anchor(": [], "def anchor_digest(": [],
            "ON s.set_id = r.set_id GROUP BY r.entry_id, r.rule_id": [], "def latest(release_root": [],
            "<newest-release picker>": []}
    for p, text in _py_sources():
        for k in hits:
            if k in text:
                hits[k].append(str(p.relative_to(REPO)))
        if 'glob("r-*")' in text and "st_mtime" in text:       # picking the newest release folder
            hits["<newest-release picker>"].append(str(p.relative_to(REPO)))
    assert hits["def entry_digest("] == ["pipeline/regs/parsing/io.py"]
    assert hits["def entry_state("] == ["pipeline/regs/review/vintage.py"]
    assert hits["def refuse_unbuilt("] == ["pipeline/regs/review/vintage.py"]
    assert hits["def bundle_matches("] == ["pipeline/regs/review/vintage.py"]
    assert hits["def split_anchor("] == ["pipeline/atlas/splits/splits.py"]
    assert hits["def anchor_digest("] == ["pipeline/atlas/splits/splits.py"]
    assert hits["ON s.set_id = r.set_id GROUP BY r.entry_id, r.rule_id"] == ["pipeline/regs/review/fingerprint.py"]
    assert hits["<newest-release picker>"] == ["pipeline/deliver/release/manifest.py"]
    assert hits["def latest(release_root"] == ["pipeline/deliver/release/manifest.py"]
    # nor in the shell: `ls -t data/release/r-*` was a second answer (by folder mtime) in
    # test-full.sh and update-hydat.sh (review T133) — they ask `latest` too
    sh = {str(p.relative_to(REPO)) for p in sorted((REPO / "scripts").glob("*.sh"))
          if re.search(r"\bls\b[^\n|]*-\w*t\w*[^\n|]*release/r-\*", p.read_text(encoding="utf-8"))}
    assert sh == set(), sh
    # the review app never decides the vintage itself: it reads digests and asks `vintage`
    reuse = (REPO / "curation-review" / "backend" / "reuse.py").read_text(encoding="utf-8")
    assert "entry_digests" not in reuse and "Waiting." not in reuse
    assert len(re.findall(r"V\.entry_state\(", reuse)) == 1
