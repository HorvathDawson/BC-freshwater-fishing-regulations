"""Curation review phase 4 (T135) THROUGH THE REVIEW APP, in REVIEW MODE: the one queue, the
pre-checks on screen, the ticket export, and a refused mark said in the reviewer's words (T142).

The FastAPI app runs with `TestClient` over TEMP COPIES of the catalogue, splits.json and the marks
store, the real atlas and synopsis (read only), and a FIXTURE REACH RUN written here — two real
diagnostics of the corpus (Ash River's ambiguous cut, Frog Lake's unresolved rules) — so what the
queue says of the run is pinned whatever the live run is doing.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.tests.conftest import ATLAS_HINT, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "review", "evidence")
ASH, FROG = "r1:ash_river@1-7", "r5:frog_lake@5-6"
ADAM = "r1:adam_river_except_eve_river@1-10"     # binds through splits.json's first waterbody
AMB = {"also_at": [12419.76], "candidates": [12419.76, 14994.7], "extent": 0,
       "split_id": "ash_river__dickson_lake", "toward": "ash_river__elsie_lake", "used": 14994.7}

pytestmark = [pytest.mark.needs_atlas, pytest.mark.needs_source]


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    root = tmp_path_factory.mktemp("queue")
    cat = root / "catalogue"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    shutil.copy2(CURATED.waters.splits, root / "splits.json")
    (root / "marks.jsonl").write_text("")
    run = root / "run"
    run.mkdir()
    (run / "report.json").write_text(json.dumps({"digest": "fixture-run"}))
    (run / "rule_diagnostic.jsonl").write_text(json.dumps(
        {"entry_id": ASH, "kind": "ambiguous_cut", "payload": json.dumps(AMB),
         "rule_id": "ash_river.r1"}) + "\n")
    (run / "rule_unresolved.jsonl").write_text("".join(json.dumps(
        {"detail": "no scoped item is in the registry: []", "entry_id": FROG,
         "reason": "no_sections_for_items", "rule_id": rid}) + "\n"
        for rid in ("frog_lake.r1", "frog_lake.r2")))
    mp = pytest.MonkeyPatch()
    for k, v in {"CURATION_ENTRIES_DIR": cat,
                 "CURATION_SPLITS_JSON": root / "splits.json", "CURATION_MARKS": root / "marks.jsonl",
                 "CURATION_BACKUP_DIR": root / "backups", "CURATION_REACH_DIR": run}.items():
        mp.setenv(k, str(v))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    assert reuse.REACH_DIR == run
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "root": root, "reuse": reuse}
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


@pytest.fixture(scope="module")
def queue(env):
    return env["client"].get("/api/entries").json()


def test_one_queue_across_every_region_in_the_review_order(env, queue):
    """No region: the whole corpus, tier by tier (P1 Z P2 P3 P4 sweep), each tier led by its most
    urgent risk factor; the statuses say what the parse left, never "unreviewed"."""
    from pipeline.regs.review.tiers import RISKS, TIERS
    assert len(queue) == 1515  # 1525 before the zone daily-quota merge (6335f73d)
    assert len({r["region"] for r in queue}) > 5
    # owner 2026-10-26: severity first (one chip per entry), then the tier order, each tier led by its most urgent risk
    from pipeline.regs.review import severity as S
    order = [(S.rank(r["severity"]), TIERS.index(r["tier"])) for r in queue]
    assert order == sorted(order)
    for lv in {r["severity"] for r in queue}:
        for t in ("P1", "P3"):
            first = [RISKS.index(r["risks"][0]) for r in queue if r["tier"] == t and r["severity"] == lv]
            assert first == sorted(first), (lv, t)
    assert next(r for r in queue if r["tier"] == "P1")["risks"][0] == "no match"
    assert {r["status"] for r in queue} == {"no_registry", "parser-uncertain", "unused_splits",
                                            "plain", "zone"}
    assert all(set(r["prechecks"]) == {"A", "B"} for r in queue)
    c = env["client"]
    assert c.get("/api/entries", params={"order": "attention"}).status_code == 422
    assert c.get("/api/entries", params={"status": "flagged"}).status_code == 422


def test_the_run_s_facts_reach_the_queue_and_the_screen(env, queue):
    """Ash River's ambiguous cut and Frog Lake's unresolved rules, from the fixture run: risk
    factors on their rows, pre-checks and the cut's candidates on Ash River's screen."""
    rows = {r["entry_id"]: r for r in queue}
    assert "ambiguous cut" in rows[ASH]["risks"] and "unresolved" in rows[FROG]["risks"]
    v = env["client"].get(f"/api/entries/{ASH}/review").json()
    assert "ambiguous_cuts" not in v      # T189: the cut is its finding, never a second list
    assert v["run"] == {"run": "fixture-run", "why": None}
    amb = [f for f in v["prechecks"] if f["code"] == "ambiguous_cut"]
    assert amb == [{"code": "ambiguous_cut", "severity": "A", "rule_id": "ash_river.r1",
                    "what": "a cut matched several places on its line; the builder took one by "
                            "ruling E16 — check the pick: ash_river__dickson_lake at 12,420 m, "
                            "14,995 m; took 14,995 m (toward ash_river__elsie_lake)"}]
    assert v["parse_findings"] == []
    frog = env["client"].get(f"/api/entries/{FROG}/review").json()
    assert [(f["code"], f["rule_id"]) for f in frog["prechecks"] if f["code"] == "unresolved"] == [
        ("unresolved", "frog_lake.r1"), ("unresolved", "frog_lake.r2")]
    p = env["client"].get("/api/review/progress").json()
    assert p["facts"] == {"run": "fixture-run", "why": None}


def test_a_split_added_mid_review_is_checked_through_its_entries(env, queue):
    """Owner 2026-10-23 (T135 acceptance), re-read by T184 (2026-10-24, entries only): a split
    added after review began is NO review target of its own — no split card, no S0 tier, progress
    counts entries alone — and it is checked by proxy: the entry on its water re-queues (its mark
    stale, "binding moved"), and no other entry moves."""
    reuse = env["reuse"]
    path = Path(reuse.SPLITS_JSON_PATH)
    original = path.read_text()
    store = Path(reuse.MARKS_PATH)
    marks_before = store.read_text()
    c = env["client"]
    for target in (ADAM, ASH):
        r = c.post("/api/marks", json={"scope": "entry", "target": target,
                                       "answers": {"T": "y", "R": "n"}, "problem": "dates",
                                       "note": "before the split was added", "reviewer": "tester"})
        assert r.status_code == 200, r.text
    marked = [r for r in c.get("/api/entries").json() if r["entry_id"] in (ADAM, ASH)]
    queue = [dict(r, verify="wrong") if r["entry_id"] in (ADAM, ASH) else r for r in queue]
    try:
        data = json.loads(original)
        wb = data["waterbodies"][0]
        new = {**wb["splits"][0], "id": "t135_added_split", "label": "added mid-review",
               "_note": "Verify: added by hand 2026-10-23 — confirm on the built map"}
        wb["splits"].append(new)
        path.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n")
        c = env["client"]
        assert c.get("/api/review/splits").status_code == 404           # no split cards
        after = c.get("/api/entries").json()
        p = c.get("/api/review/progress").json()
        assert [t["tier"] for t in p["tiers"]] == ["P1", "Z", "P2", "P3", "P4", "sweep"]
        assert p["overall"]["total"] == len(after)                      # entries alone
        # T135 adversarial review: with an empty store nothing COULD go stale, so "no entry
        # re-queues" was vacuous. With marks: the entry on the split's own water (Adam River —
        # `fingerprint.entry_binding_fp` holds every split on the entry's waters, T133) goes stale,
        # "binding moved"; every other mark stands, and no row moves tier or place
        assert [r["verify"] for r in marked] == ["wrong", "wrong"]
        now = {r["entry_id"]: r for r in after}
        assert (now[ADAM]["verify"], now[ADAM]["mark_reasons"], now[ASH]["verify"]) == (
            "stale", ["binding moved"], "wrong")
        key = lambda rows: [(r["entry_id"], r["tier"],  # noqa: E731
                             "stale" if r["entry_id"] == ADAM else r["verify"]) for r in rows]
        assert key(after) == key(queue)
    finally:
        path.write_text(original)
        store.write_text(marks_before)


def test_a_wrong_mark_becomes_a_ticket_that_says_what_the_reviewer_said(env):
    from pipeline.regs.review.tickets import SEPARATOR, parse_ticket
    c = env["client"]
    body = {"scope": "entry", "target": ASH, "answers": {"T": "y", "R": "n"}, "problem": "dates",
            "note": "the row says Jun 15,\nwe store Jun 1", "reviewer": "tester"}
    r = c.post("/api/marks", json=body)
    assert r.status_code == 200 and r.json()["verdict"] == "wrong", r.text
    got = c.get("/api/review/tickets").json()
    assert [t["name"] for t in got] == ["review-wrong-entry-r1-ash-river-1-7.md"]
    fields = parse_ticket(got[0]["text"])
    assert {k: fields[k] for k in ("scope", "target", "rule_id", "verdict", "answers", "problem",
                                   "note", "reviewer")} == {
        "scope": "entry", "target": ASH, "rule_id": None, "verdict": "wrong",
        "answers": {"T": "y", "R": "n"}, "problem": "dates", "note": "the row says Jun 15,\nwe store Jun 1",
        "reviewer": "tester"}
    md = c.get("/api/review/tickets.md")
    assert md.headers["content-type"].startswith("text/markdown") and SEPARATOR not in md.text
    assert md.text == got[0]["text"]


def test_a_refused_mark_is_said_in_the_reviewer_s_words(env):
    """T142: what to do, never pydantic's "Value error, …" and never a field path."""
    from pipeline.regs.review.marks import REFUSAL_WORDS, unanswered_words
    words = dict(REFUSAL_WORDS)
    c = env["client"]

    def refused(**kw):
        body = {"scope": "entry", "target": ASH, "answers": {"T": "y", "R": "n"},
                "reviewer": "tester", **kw}
        r = c.post("/api/marks", json=body)
        assert r.status_code == 422, r.text
        return r.json()["detail"]

    assert refused() == [{"path": "", "msg": words["wrong needs a problem category and a note"]}]
    # the fixture run states no entry digests, so every entry AWAITS ITS BUILD: P and A cannot be
    # answered, and "answer every check" would ask for what the screen holds back (T135/T142
    # adversarial review) — T y R y is no verdict there (Q96)
    reuse = env["reuse"]
    assert reuse.awaiting_build("entry", ASH) is True
    held = ('Until it is built only T and R can be answered and P and A wait, so "all fine" '
            'waits for the build: record it wrong (answer n) or unsure (answer ?), or defer it '
            'with a note.')
    assert refused(answers={"T": "y", "R": "y"}) == [{"path": "", "msg": held}]
    assert refused(answers={"T": "y"}) == [{"path": "", "msg": held}]
    assert unanswered_words("entry", awaiting=True) == held
    built = pytest.MonkeyPatch()
    built.setattr(reuse, "awaiting_build", lambda scope, target: False)
    try:
        assert refused(answers={"T": "y"}) == [{"path": "", "msg": unanswered_words("entry")}]
    finally:
        built.undo()
    assert unanswered_words("entry") == ("Answer every check (T R P A) with y, n or ? before "
                                         "recording the mark, or defer it with a note.")
    assert refused(reviewer="", problem="dates", note="x") == [
        {"path": "", "msg": "Type your name first: every mark names its reviewer."}]
    assert refused(verdict="deferred", answers={}) == [
        {"path": "", "msg": "To defer, write a note saying why."}]


def test_a_ticket_says_what_changed_since_and_the_cli_says_the_same(env):
    """T135 adversarial review: the API's tickets said "changed since marked" and the CLI's did not
    (it never asked the marks' status). Both now ask `tickets.statuses_now` over the vintage on
    disk (`fingerprint.Context.on_disk`). Edit the marked entry's rule: its ticket says so."""
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import tickets as TK
    reuse, c = env["reuse"], env["client"]
    region = Path(reuse.ENTRIES_DIR) / "region-5.json"
    original = region.read_text()
    store = Path(reuse.MARKS_PATH)
    marks_before = store.read_text()
    try:
        r = c.post("/api/marks", json={"scope": "entry", "target": FROG, "answers": {"T": "y", "R": "n"},
                                       "problem": "dates", "note": "frog", "reviewer": "tester"})
        assert r.status_code == 200, r.text
        data = json.loads(original)
        e = next(x for x in data["entries"] if x["entry_id"] == FROG)
        e["rules"][0]["verbatim"] = str(e["rules"][0].get("verbatim") or "") + " (edited)"
        region.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n")
        got = {t["name"]: t["text"] for t in c.get("/api/review/tickets").json()}
        frog = next(t for n, t in got.items() if "frog" in n)
        assert "- **Now:** changed since marked (" in frog, frog
        # the CLI's computation, over the same files
        ctx = reuse._review_context()
        entries = {x["entry_id"]: x for _, x in reuse._all_entries()}
        marks = M.load(reuse.MARKS_PATH)
        st = TK.statuses_now(marks, entries=entries, ctx=ctx)
        assert st[("entry", FROG, None)]["status"] == "stale"
        assert dict(TK.export(marks, entries=entries, splits=TK.split_index(reuse._load_splits()),
                              statuses=st)) == got
    finally:
        region.write_text(original)
        store.write_text(marks_before)
