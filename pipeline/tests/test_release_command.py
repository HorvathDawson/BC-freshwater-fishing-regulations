"""`python -m pipeline release` (T295/T297, T408): the orchestration, with every heavy step mocked — the order, stop on
a failure, resume, the binding-diff gate, and production refusing anything but a human. T666 (one release path):
preflight lists every failure, fast-fail is keyed on the tree's content, the follows stage refreshes what follows the
release on the candidate, the flags are remembered, the rehearsal walks every stage's checks."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline import release as R
from pipeline import release_version as RV

LIVE, NEW = "r-live00000000", "r-new000000000"


def _release(root: Path, rid: str, reach: str, data: bytes = b"data", extra: dict | None = None,
             stamp: str | None = "0.5.0") -> None:
    d = root / rid
    d.mkdir(parents=True)
    (d / "water.bin.gz").write_bytes(data)
    (d / "manifest.json").write_text(json.dumps({
        "format": "app-manifest/2", "data_release": rid, **({"minAppVersion": stamp} if stamp else {}),
        "files": {"water": {"url": f"{rid}/water.bin.gz", "gz": len(data), "format": "water/1"}, **(extra or {})},
        "bundle": {"entries_digest": "e", "reach_digest": reach}}))


def _reach(d: Path, digest: str) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "report.json").write_text(json.dumps({"digest": digest}))
    (d / "rule_section.jsonl.gz").write_bytes(digest.encode())


#: (T555 item 4) the app-reads table these tests judge by: 0.3.0 was the first to read paint/5, 0.5.0 salmon/4
APPS = {"0.2.0": {"water": ["water/1"], "paint": ["paint/4"], "salmon": ["salmon/2"]},
        "0.3.0": {"water": ["water/1"], "paint": ["paint/5", "paint/4"], "salmon": ["salmon/2"]},
        "0.5.0": {"water": ["water/1"], "paint": ["paint/5", "paint/4"], "salmon": ["salmon/4"]}}


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    root, reaches = tmp_path / "release", tmp_path / "reaches" / "full"
    _release(root, LIVE, "reach-live")
    (root / "LATEST").write_text(LIVE + "\n")
    _reach(reaches, "reach-live")
    w = {"calls": [], "fail": set(), "dirty": [], "diffs": 0, "differs": False, "root": root, "reaches": reaches,
         "pushed": True, "fast_fail_rc": 0, "fast_fails": 0, "new_stamp": "0.5.0", "tree": "t1", "rc": {}}

    def run_cmd(cmd, env=None):
        name = "inseason" if "pipeline.regs.inseason.resolve" in cmd else \
            "data-tier" if any("pytest-chunked" in c for c in cmd) else \
            "recut" if any("regs-recut/sync.mjs" in c for c in cmd) else \
            "taps" if any("regs-recut/taps.py" in c for c in cmd) else \
            "follow-py" if "pytest" in cmd else "follow-worker" if "workers/feeds" in cmd else \
            "follow-app" if "vitest" in cmd else "app-gate" if cmd[:4] == ["pnpm", "-C", "app", "run"] else \
            "build" if "build" in cmd else \
            "upload" if any("upload.sh" in c for c in cmd) else "smoke" if any("smoke-staging" in c for c in cmd) else \
            "publish" if any("publish" in c for c in cmd) else " ".join(cmd)
        w["calls"].append((name, env))
        for c in cmd:                       # (T680) a check's report, as the real tool would write it
            if name in w.get("reports", {}) and c.startswith(("--outputFile.json=", "--junitxml=")):
                Path(c.split("=", 1)[1]).write_text(w["reports"][name])
        if name in w["rc"]:
            return w["rc"][name]
        if name in w["fail"]:
            return 3
        if name == "build" and not (root / NEW).exists():       # the build writes the release and CANDIDATE
            digest = w.get("new_digest", "reach-new")
            _reach(reaches, digest)
            if "new_dfo" in w:                                     # (T631) DFO places, outside the rules digest
                (reaches / "dfo_salmon.jsonl.gz").write_bytes(w["new_dfo"])
            _release(root, NEW, digest, extra=w.get("extra"), stamp=w["new_stamp"])
            (root / "CANDIDATE").write_text(NEW + "\n")
        return 0

    def diff(before, after):
        w["diffs"] += 1
        return {"before": str(before), "after": str(after), "rules_changed": int(w["differs"]),
                "unresolved_changed": 0, "licensing_changed": 0, "rules": {}, "unresolved": {}, "licensing": {}}

    monkeypatch.setattr(R, "release_root", lambda: root)
    monkeypatch.setattr(R, "reach_dir", lambda: reaches)
    monkeypatch.setattr(R, "HOME", tmp_path / "home")
    monkeypatch.setattr(R, "LEDGER", tmp_path / "home" / "ledger.json")
    monkeypatch.setattr(R, "git_dirty", lambda: list(w["dirty"]))
    monkeypatch.setattr(R, "git_head", lambda: w.get("head", "abc123"))
    monkeypatch.setattr(R, "run_cmd", run_cmd)

    def fast_fail(base=None, env=None):
        w["fast_fails"] += 1
        w["fast_fail_base"], w["fast_fail_env"] = base, env
        return w["fast_fail_rc"], list(w.get("fast_fail_failed", ["a step that failed"] if w["fast_fail_rc"] else []))

    monkeypatch.setattr(R, "run_fast_fail", fast_fail)
    # (T666 item 5) the tree by content, what was uncommitted at a pass, and what differs from a commit
    monkeypatch.setattr(R, "tree_key", lambda: "key-" + w["tree"])
    w["dirt"], w["changed_since"] = [], set()
    monkeypatch.setattr(R, "dirty_paths", lambda: list(w["dirt"]))
    monkeypatch.setattr(R, "changed_since", lambda rev: set(w["changed_since"]))
    # (T666) the fresh-copy test run: a clean clone of HEAD, the no-data tier (heavy: stood in for)
    w["fresh_copies"], w["fresh_copy_rc"], w["committed_since"] = 0, 0, set()

    def fresh_copy():
        w["fresh_copies"] += 1
        return w["fresh_copy_rc"]
    monkeypatch.setattr(R, "run_fresh_copy", fresh_copy)
    monkeypatch.setattr(R, "head_tree", lambda: "committed-" + w["tree"])
    monkeypatch.setattr(R, "committed_since", lambda rev: set(w["committed_since"]))
    w["ci"] = (0, "ci-status: ok    app-ci.yml: green on abc123")
    w["ci_skips"] = []

    def ci(skip=None):
        w["ci_skips"].append(skip)
        return w["ci"]
    monkeypatch.setattr(R, "ci_status", ci)
    monkeypatch.setattr(R, "git_has_commit", lambda rev: rev in w.get("commits", ()))
    def pushed():
        if w["pushed"] == "no-branch":                       # (T995) origin has no release/<version>
            raise R.NoReleaseBranch(R.release_branch())
        if w["pushed"] in ("behind", "diverged"):            # (T995 review) HEAD is not the branch's tip
            raise R.ReleaseBranchMoved(w["pushed"])
        return w["pushed"]
    monkeypatch.setattr(R, "git_pushed", pushed)
    monkeypatch.setattr(R, "current_branch", lambda: w.get("branch", "release/" + w["version"]))
    # (T995) the app version names the release branch and the `v<version>` tag. The tags: w["tag_at"] local,
    # w["remote_tags"] on origin (a push copies a local tag there)
    w["version"], w["tag_at"], w["remote_tags"], w["made_tags"], w["pushed_tags"] = "0.9.0", {}, {}, [], []
    monkeypatch.setattr(R, "app_version", lambda: w["version"])
    # (T998) what the commits compute: by default the version app.json names (a feat since the cutover)
    monkeypatch.setattr(R, "version_decision", lambda: w.get("decision") or RV.decide(
        "0.8.5", "the cutover b5dd8ade3", [_commit(f"feat(T998): what {w['version']} brings")]))
    monkeypatch.setattr(R, "tag_commit", lambda tag: w["tag_at"].get(tag))
    monkeypatch.setattr(R, "remote_tag_commit", lambda tag: w["remote_tags"].get(tag))
    monkeypatch.setattr(R, "is_ancestor", lambda a, b: (a, b) in w.get("ancestors", set()))
    # (T1062) the release candidates on origin, read from the same remote tags; two commits are one tree apart from the
    # build numbers when they are one commit or w["same_tree"] says so
    monkeypatch.setattr(R, "remote_rcs", lambda v: {n: c for t, c in w["remote_tags"].items()
                                                    if (n := R.rc_number(v, f"refs/tags/{t}"))})   # the real pattern
    monkeypatch.setattr(R, "beyond_build_numbers", lambda a, b: [] if a == b or (a, b) in w.get("same_tree", set())
                        else ["app/apps/mobile/src/a.tsx"])
    monkeypatch.setattr(R, "fetch_branch", lambda b: w.setdefault("fetched_branches", []).append(b))

    def make_tag(tag, commit, message):
        w["made_tags"].append((tag, commit))
        w["tag_at"][tag] = commit
        return w["rc"].get("tag", 0)
    monkeypatch.setattr(R, "make_tag", make_tag)

    def push_tags(tags):
        w["pushed_tags"].append(list(tags))
        rc = w["rc"].get("push-tags", 0)
        if not rc:
            w["remote_tags"].update({x: w["tag_at"][x] for x in tags})
        return rc
    monkeypatch.setattr(R, "push_tags", push_tags)
    w["landed"] = (True, "review marks: every mark in force has landed")
    monkeypatch.setattr(R, "marks_landed", lambda: w["landed"])
    w["wrong_saves"] = (True, "wrong saves: no place ships on a wrong or unsure save with no fix (0 allowed)")
    monkeypatch.setattr(R, "wrong_saves", lambda: w["wrong_saves"])
    # (T841) the classified-waters gate: the corpus half at preflight, the candidate's bundle at checks (pinned in
    # test_classified_waters.py)
    w["classified"] = (True, "classified waters: 73 book rows, 74 designations — every one in the corpus as printed")
    monkeypatch.setattr(R, "classified", lambda bundle=None: w["classified"])
    w["classified_problems"] = ""
    monkeypatch.setattr(R, "classified_problems", lambda rid: w["classified_problems"])
    w["atlas_behind"] = []
    monkeypatch.setattr(R, "atlas_behind", lambda: list(w["atlas_behind"]))
    # (T777) the dry run's informational review line reads the served atlas's registry and the corpus on disk
    # (`pending.pending_on_disk`, pinned in test_curation_review_vintage.py): this world has nothing waiting on a rebuild
    from pipeline.regs.review import pending as PENDING
    monkeypatch.setattr(PENDING, "pending_on_disk", lambda target=None: {"entries": [], "splits": []})
    monkeypatch.setattr(R, "binding_diff", diff)
    # (T555 item 4) what each app version reads: a small table in place of the committed one (pinned in test_app_reads.py),
    # so these releases' made-up formats are judged the same whatever the real apps read today
    from pipeline.deliver.release import app_gate as AG
    monkeypatch.setattr(AG, "table", lambda: APPS)
    # (T555 item 5) the ids gone with no alias: reads both releases' core files (pinned in test_release_gone_ids.py)
    w["gone"] = []

    def gone(before, after):
        rep = {"before": 3, "kept": 3 - len(w["gone"]), "aliased": [], "gone": list(w["gone"])}
        return rep, f"item ids, {before} -> {after}: {len(w['gone'])} gone with no alias"
    monkeypatch.setattr(R, "gone_ids", gone)
    # (T667 slice 6) the live counts: read a release folder, a reach run and the corpus (pinned in test_release_live_counts.py)
    w["counts"] = {"items": 3}
    monkeypatch.setattr(R, "live_counts", lambda before, after, led: (
        dict(w["counts"]), f"live counts, {before} -> {after}: items {w['counts']['items']}"))
    # (T1047) the gauge level records moved onto a new zero: read git + the datum file (pinned in test_datum_changes.py)
    w["gauge_records"], w["gauge_records_since"] = [], []

    def gauge_records(since):
        w["gauge_records_since"].append(since)
        return list(w["gauge_records"]), "gauge level records: " + ("; ".join(w["gauge_records"]) or "none moved")
    monkeypatch.setattr(R, "gauge_records", gauge_records)
    # (T1029) the one-bank tributaries kept with no side read: read the reach run's report.json (pinned below)
    w["one_bank"] = []
    monkeypatch.setattr(R, "one_bank_unread", lambda: (
        list(w["one_bank"]), "one-bank tributaries with no side read: " + ("; ".join(w["one_bank"]) or "none")))
    monkeypatch.setattr(R, "diff_summary", lambda rep: f"{rep['rules_changed']} rule(s) bind different sections")
    # the manifest check's real body reads the size gate and publish.plan over real release files: pinned elsewhere
    monkeypatch.setattr(R, "manifest_problems", lambda rid: list(w.get("manifest_problems", [])))
    # the live staging site (network): `site_gate` is pinned by its own tests below
    w["site_min_apps"] = []

    def site(min_app, manifest=None):
        w["site_min_apps"].append(min_app)
        w.setdefault("site_manifests", []).append(manifest)
        return list(w.get("site_problems", [])), "site serves app 9.9.9"
    monkeypatch.setattr(R, "site_problems", site)
    # (T993 item 2) the code's formats, judged by APPS (pinned in test_release_min_app.py): water/1, which every app reads
    w["code_files"] = {"water": {"format": "water/1"}}
    monkeypatch.setattr(R, "code_manifest", lambda: {"files": dict(w["code_files"])})
    # (T788 #2) the pieces gate (the network): pinned in test_release_min_app.py
    w["pieces"], w["pieces_asked"] = [], []

    def pieces(rid):
        w["pieces_asked"].append(rid)
        return list(w["pieces"])
    monkeypatch.setattr(R, "pieces_problems", pieces)
    # (T993 item 9) the size headroom reads a real manifest's sizes (pinned in test_release_min_app.py)
    w["headroom"] = [("water", 2_750_000, 2_850_000, 0.035), ("core", 1_500_000, 1_650_000, 0.091)]
    monkeypatch.setattr(R, "size_headroom", lambda rid: list(w["headroom"]))
    # (T993 item 8) the auto-accepts read git (pinned in test_release_auto_accept.py): by default nothing is explained
    w["attributed"] = (False, [], ["rule r2:x::x.r1: no curated row changed since abc names entry r2:x"])
    w["attribute_asked"] = []

    def attribute(js, since):
        w["attribute_asked"].append(since)
        return w["attributed"]
    monkeypatch.setattr(R, "attribute_diff", attribute)
    w["renumbered"] = (False, "a page changed")
    monkeypatch.setattr(R, "recut_renumbered", lambda pages: w["renumbered"])
    # (T537; T679 #11) the in-season resolve writes the generated resolution only: no commit of its own
    # (T666 items 1-2) the follows stage: the re-cut's changed paths, its commit, the tests found by rule, the push
    w["recut_changed"], w["recuts_committed"], w["unpushed"], w["pushes"] = [], [], [], 0
    monkeypatch.setattr(R, "recut_changed", lambda: list(w["recut_changed"]))
    monkeypatch.setattr(R, "recut_token", lambda paths: "tok-" + str(len(paths)))

    def commit_recut(rid, kind):
        w["recuts_committed"].append((rid, kind))
        w["recut_changed"], w["pushed"], w["tree"] = [], False, w["tree"] + "+fixtures"
        w["unpushed"].append("f1xture" + "0" * 33)
        return "f1xture" + "0" * 33
    monkeypatch.setattr(R, "commit_recut", commit_recut)
    monkeypatch.setattr(R, "follow_tests", lambda: (["pipeline/tests/test_x.py"], ["tools/x.release.test.ts"]))
    monkeypatch.setattr(R, "app_check_steps", lambda: ["typecheck"])      # (T680) one gate stands for check:fast's
    monkeypatch.setattr(R, "git_unpushed", lambda: list(w["unpushed"]))

    def push():
        w["pushes"] += 1
        w["pushed"], w["unpushed"] = True, []
        return 0
    monkeypatch.setattr(R, "git_push", push)
    w["staging_prechecks"] = []
    monkeypatch.setattr(R, "staging_prechecks", lambda rid: list(w["staging_prechecks"]))
    w["inseason_check"] = ("same", "in-season matches on r-x: unchanged")
    monkeypatch.setattr(R, "inseason_check", lambda rid: w["inseason_check"])
    w["goldens_paired"] = True
    monkeypatch.setattr(R, "goldens_paired", lambda d: (w["goldens_paired"], "said"))
    w["staging_serves"] = NEW
    monkeypatch.setattr(R, "staging_release", lambda: w["staging_serves"])
    monkeypatch.setattr(R, "after_chores", lambda rid: f"after the release: chores for {rid}")
    # (T1161) the Mac: macOS on power, a fake pmset and a fake caffeinate that only records its pid (nothing here
    # reads the real power source or holds the real Mac awake)
    w["pmset"], w["caffeinated"] = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged; 0:00 remaining", []
    monkeypatch.setattr(R, "is_macos", lambda: True)
    monkeypatch.setattr(R, "pmset_batt", lambda: w["pmset"])
    monkeypatch.setattr(R, "start_caffeinate", lambda pid: w["caffeinated"].append(pid))
    return w


def _commit(message: str, paths=("app/apps/mobile/src/a.tsx",), sha: str = "c0ffee0000000000") -> RV.Commit:
    """(T998) one commit as the version decision reads it (an app path unless told otherwise)."""
    return RV.Commit(sha, ("p",), message, tuple(paths))


def _staging(**kw) -> int:
    return R.main(["--staging", *sum(([f"--{k.replace('_', '-')}", str(v)] for k, v in kw.items()), [])])


def _latest(w) -> str:
    return (w["root"] / "LATEST").read_text().strip()


ALL = ["build", "inseason", "recut", "follow-py", "follow-app", "follow-worker", "app-gate", "data-tier", "upload",
       "smoke"]


def _env_of(w, name) -> dict:
    return [env for c, env in w["calls"] if c == name][0]


def test_the_stages_run_in_order_and_ship_the_candidate(world, capsys):
    assert _staging() == 0
    out = capsys.readouterr().out
    assert R.STAGES == ("preflight", "baseline", "build", "checks", "follows", "promote", "staging")
    marks = [out.index(f"[{i}/7] {s} ok") for i, s in enumerate(R.STAGES, 1)]
    assert marks == sorted(marks)
    assert [c for c, _ in world["calls"]] == ALL
    assert _env_of(world, "upload") == {"MIN_APP_VERSION": "0.5.0", "SMOKE": "0"}   # live stamp
    assert _latest(world) == NEW and not (world["root"] / "CANDIDATE").exists()
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["binding_diff"] == "same" and rec["staged_at"] and rec["commit"] == "abc123"


def test_the_live_counts_are_printed_recorded_and_never_stop_the_release(world, capsys):
    """(T667 slice 6, D3) the counts tests used to pin are printed in the release check and kept on the candidate's
    ledger record (the next release diffs against them); a big jump still ships."""
    world["counts"] = {"items": 999_999}
    assert _staging() == 0
    out = capsys.readouterr().out
    assert f"live counts, {LIVE} -> {NEW}: items 999999" in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["counts"] == {"items": 999_999}
    assert (R.HOME / "checks" / NEW / "live_counts.txt").read_text() == f"live counts, {LIVE} -> {NEW}: items 999999\n"


def test_a_moved_gauge_level_record_is_its_own_line_and_never_stops_the_release(world, capsys):
    """(T1047, owner 2026-11-24 "easy to see") each gauge level record the release moves onto a new zero is printed as its
    own line in the checks, written beside them and kept on the ledger, compared against the live release's commit."""
    line = "08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note, 2025-06-05)"
    world["gauge_records"] = [line]
    R.save_ledger({"releases": {LIVE: {"commit": "c0ffee"}}})
    assert _staging() == 0
    out = capsys.readouterr().out
    assert line in out and "1 gauge level record(s) moved" in out
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["gauge_records"] == [line]
    assert line in (R.HOME / "checks" / NEW / "gauge_records.txt").read_text()
    assert world["gauge_records_since"] == ["c0ffee"]                     # the live release's commit


def test_a_one_bank_tributary_with_no_side_read_is_warned_and_never_stops_the_release(world, capsys):
    """(T1029, owner 2026-11-27: "keep it closed ... but ... we should add a thing that warns us if this is ever a thing")
    each one-bank place keeping a tributary with no side read is its own WARNING line in the checks, in the stage's
    summary, written beside the checks and kept on the ledger; the release still ships."""
    from pipeline.atlas.reach.dfo_salmon import ONE_BANK_UNREAD_WARNING
    line = f"{ONE_BANK_UNREAD_WARNING} 6:kit:bank: 2 tributary section(s) KEPT with no side read …: c:0, d:0"
    world["one_bank"] = [line]
    assert _staging() == 0
    out = capsys.readouterr().out
    assert line in out and "WARNING: 1 one-bank place(s) keep a tributary with no side read (T1029, see above)" in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["one_bank_unread"] == [line]
    assert line in (R.HOME / "checks" / NEW / "one_bank_unread.txt").read_text()


def test_no_one_bank_warning_when_every_side_is_read(world, capsys):
    assert _staging() == 0
    out = capsys.readouterr().out
    assert "one-bank place(s) keep a tributary" not in out and "one-bank tributaries NOT counted" not in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["one_bank_unread"] == []


def test_one_bank_unread_reads_the_reach_runs_tally(tmp_path, monkeypatch):
    """The real `one_bank_unread` over the candidate's reach report.json `dfo_one_bank`: a place holding a tributary with
    no side read fires one WARNING line naming it; today's shape (1 place, 12 kept, none unread) fires none; a report
    with no tally (unreadable, or from before T1029) is said and recorded, never read as none."""
    from pipeline.atlas.reach.dfo_salmon import ONE_BANK_UNREAD_WARNING
    monkeypatch.setattr(R, "reach_dir", lambda: tmp_path)
    rep = tmp_path / "report.json"

    def tally(und: dict) -> None:
        rep.write_text(json.dumps({"digest": "d", "dfo_one_bank": {
            "places": 1, "kept": 12, "tributary_other_bank": 0,
            "tributary_side_undetermined": sum(len(v) for v in und.values()), "undetermined_by_place": und}}))
    tally({"6:kitimat-river:on-west-bank": ["360639090:0", "360639090:1519"]})
    lines, txt = R.one_bank_unread()
    assert len(lines) == 1 and lines[0].startswith(f"{ONE_BANK_UNREAD_WARNING} 6:kitimat-river:on-west-bank: 2 ")
    assert "360639090:0, 360639090:1519" in lines[0] and "1 place(s) WARNED, kept in the place under its rule" in txt
    tally({})
    assert R.one_bank_unread() == ([], "one-bank tributaries with no side read (T1029): none (1 place(s) with "
                                       "tributaries, 12 kept, 0 on the other bank)")
    rep.write_text(json.dumps({"digest": "d"}))                           # a reach run from before T1029
    lines, txt = R.one_bank_unread()
    assert lines[0].startswith(R.ONE_BANK_UNCOUNTED) and "KeyError" in lines[0] and lines[0] in txt
    # a tally that lacks a key, or is not a mapping, is "could not be counted", never none (W46)
    rep.write_text(json.dumps({"digest": "d", "dfo_one_bank": {"places": 1, "kept": 12, "tributary_other_bank": 0}}))
    lines, _ = R.one_bank_unread()
    assert lines[0].startswith(R.ONE_BANK_UNCOUNTED) and "KeyError: 'undetermined_by_place'" in lines[0]
    rep.write_text(json.dumps({"digest": "d", "dfo_one_bank": {"undetermined_by_place": {}}}))      # no counts
    lines, _ = R.one_bank_unread()
    assert lines[0].startswith(R.ONE_BANK_UNCOUNTED) and "KeyError: 'places'" in lines[0]
    for bad in (None, [], {"places": 1, "kept": 0, "tributary_other_bank": 0, "undetermined_by_place": ["x:0"]}):
        rep.write_text(json.dumps({"digest": "d", "dfo_one_bank": bad}))
        lines, _ = R.one_bank_unread()
        assert len(lines) == 1 and lines[0].startswith(R.ONE_BANK_UNCOUNTED), bad
    rep.unlink()
    lines, _ = R.one_bank_unread()
    assert lines[0].startswith(R.ONE_BANK_UNCOUNTED) and "FileNotFoundError" in lines[0]


def test_an_uncounted_one_bank_tally_ships_said_in_the_stage_summary(world, capsys):
    world["one_bank"] = [f"{R.ONE_BANK_UNCOUNTED} reaches/full/report.json gives no `dfo_one_bank` tally (KeyError)"]
    assert _staging() == 0
    assert "one-bank tributaries NOT counted (see above)" in capsys.readouterr().out


def test_gauge_records_reads_the_datum_file_at_the_live_commit(monkeypatch):
    """The real `gauge_records`: the file at the live release's commit against the file now; no commit in this clone ->
    every accepted change listed, said so."""
    from pipeline.gauges.feed import datum
    now = [{"station": "08HD006", "effective": "2025-06-05", "offset_m": -5.0, "source": "hydat_remark",
            "status": "accepted"}]
    monkeypatch.setattr(datum, "load", lambda: now)
    monkeypatch.setattr(datum, "rows_at", lambda commit, repo: None)
    monkeypatch.setattr(R, "git_has_commit", lambda rev: rev == "abc")
    want = "08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note, 2025-06-05)"
    lines, txt = R.gauge_records("abc")
    assert lines == [want] and "against the live release's commit abc (no datum file then): 1 moved" in txt
    monkeypatch.setattr(datum, "rows_at", lambda commit, repo: now)
    assert R.gauge_records("abc") == ([], "gauge level records (T1047 datum changes), against the live release's "
                                          "commit abc: none moved")
    lines, txt = R.gauge_records(None)
    assert lines == [want] and "every accepted change listed" in txt


def test_gauge_records_that_cannot_be_compared_are_said_and_recorded_not_a_stop(monkeypatch):
    """A git failure or a schema refusal of the datum file at the live commit is never a halt of stage 4 and never hidden:
    it is the one recorded line "could not be compared: <reason>"."""
    import subprocess
    from pipeline.gauges.feed import datum
    monkeypatch.setattr(datum, "load", lambda: [])
    monkeypatch.setattr(R, "git_has_commit", lambda rev: True)

    def git_fails(commit, repo):
        raise subprocess.CalledProcessError(128, ["git", "show"], stderr=b"fatal: bad object abc")
    monkeypatch.setattr(datum, "rows_at", git_fails)
    lines, txt = R.gauge_records("abc")
    assert lines == [f"{R.GAUGE_UNCOMPARED} the datum file at the live release's commit abc (CalledProcessError: "
                     "fatal: bad object abc)"] and lines[0] in txt

    def refused(commit, repo):
        raise datum.DatumFileError("08HD006: status 'maybe' is not one of the statuses")
    monkeypatch.setattr(datum, "rows_at", refused)
    lines, _ = R.gauge_records("abc")
    assert lines[0].startswith(R.GAUGE_UNCOMPARED) and "DatumFileError: 08HD006: status 'maybe'" in lines[0]


def test_an_uncompared_gauge_record_ships_said_in_the_stage_and_on_the_ledger(world, capsys):
    line = f"{R.GAUGE_UNCOMPARED} the datum file at the live release's commit c0ffee (CalledProcessError: fatal)"
    world["gauge_records"] = [line]
    assert _staging() == 0
    out = capsys.readouterr().out
    assert line in out and "gauge level records NOT compared" in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["gauge_records"] == [line]
    assert line in (R.HOME / "checks" / NEW / "gauge_records.txt").read_text()


def test_ids_gone_with_no_alias_are_listed_and_kept_and_never_stop_the_release(world, capsys):
    world["gone"] = [{"id": "wbk:3", "name": "Lost Pond"}]
    assert _staging() == 0
    out = capsys.readouterr().out
    assert f"item ids, {LIVE} -> {NEW}: 1 gone with no alias" in out and "1 item id(s) gone with no alias" in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["gone_ids"] == 1
    kept = json.loads((R.HOME / "checks" / NEW / "gone_ids.json").read_text())
    assert kept["gone"] == [{"id": "wbk:3", "name": "Lost Pond"}]


def test_uncommitted_code_stops_before_anything_runs(world, capsys):
    world["dirty"] = ["pipeline/build.py"]
    assert _staging() == 1
    assert world["calls"] == [] and _latest(world) == LIVE
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "pipeline/build.py" in err


def test_an_atlas_behind_its_curated_inputs_stops_naming_the_file(world, capsys):
    """T547: a cut / name / area edited after the atlas build stops the release before anything runs, naming the file;
    --ship-stale-atlas REASON goes on and prints the reason."""
    world["atlas_behind"] = ["cuts.json (curated:waters.cuts): changed since the atlas was built — rebuild: x"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "cuts.json (curated:waters.cuts)" in err and "--ship-stale-atlas" in err
    assert world["calls"] == [] and _latest(world) == LIVE
    with pytest.raises(SystemExit):
        _staging(ship_stale_atlas=" ")
    world["atlas_behind"].append("pipeline/atlas/graph/names.py (code): changed since the atlas was built")
    assert _staging(ship_stale_atlas="owner: names only, rebuild next week") == 0
    out = capsys.readouterr().out
    assert "SHIPPED ANYWAY (--ship-stale-atlas): owner: names only, rebuild next week" in out
    # (T1171, third review of #286, M3) every reason it shipped behind is printed, not a count
    assert "cuts.json (curated:waters.cuts): changed" in out and "pipeline/atlas/graph/names.py (code): changed" in out


def test_the_dry_run_says_whether_the_atlas_is_behind(world, capsys):
    world["atlas_behind"] = ["slough_calls.json (curated:waters.slough_calls): changed since the atlas was built"]
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "WOULD STOP: the atlas NEEDS REBUILDING: 1 reason(s)" in out and "slough_calls.json" in out
    world["atlas_behind"] = []
    assert R.main(["--staging", "--dry-run"]) == 0
    assert "ok: the atlas is current with its curated inputs" in capsys.readouterr().out


def test_a_failed_build_leaves_latest_and_uploads_nothing(world, capsys):
    world["fail"] = {"build"}
    assert _staging() == 1
    assert [c for c, _ in world["calls"]] == ["build"] and _latest(world) == LIVE
    assert "STOPPED at build" in capsys.readouterr().err


def test_a_failed_check_leaves_latest_untouched(world, capsys):
    world["manifest_problems"] = ["files.water: missing"]
    assert _staging() == 1
    assert _latest(world) == LIVE and (world["root"] / "CANDIDATE").read_text().strip() == NEW
    assert [c for c, _ in world["calls"]] == ["build"]
    assert "STOPPED at checks" in capsys.readouterr().err


def test_the_app_gate_refuses_a_format_the_oldest_app_cannot_read(world, capsys):
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    assert _staging(min_app_version="0.4.0") == 1
    err = capsys.readouterr().err
    assert "STOPPED at checks" in err and "salmon/4 needs app >= 0.5.0" in err
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ["build"]
    # (T666 item 4) the flag is remembered: a bare rerun asks the same thing again; a new value replaces it
    assert _staging() == 1 and "salmon/4 needs app >= 0.5.0" in capsys.readouterr().err
    assert _staging(min_app_version="0.5.0") == 0


def test_the_diff_gate_stops_and_only_its_own_json_lets_it_pass(world, tmp_path, capsys):
    world["differs"] = True
    assert _staging() == 1
    err = capsys.readouterr()
    assert "STOPPED at checks" in err.err and "--accept-diff" in err.err and "1 rule(s)" in err.out
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ["build"]
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    assert js.is_file()
    stale = tmp_path / "stale.json"
    stale.write_text("{}")
    assert _staging(accept_diff=stale) == 1                  # another diff is not this one
    assert _latest(world) == LIVE
    assert _staging(accept_diff=js) == 0
    assert _latest(world) == NEW
    assert world["diffs"] == 1                                # the diff ran once; reruns read the ledger
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["checks"]["binding_diff"] == "accepted"


def test_a_dfo_only_change_is_diffed_though_the_rules_digest_is_unchanged(world, capsys):
    """T631 item 1: the reach digest covers the rules only. A run whose DFO places moved (21 places on 2026-11-11, the
    Morice lost 11 sections) with the same digest was called "same reach run" and never diffed."""
    world["new_digest"], world["new_dfo"], world["differs"] = "reach-live", b"21 places moved", True
    assert _staging() == 1
    assert world["diffs"] == 1 and "STOPPED at checks" in capsys.readouterr().err
    assert _latest(world) == LIVE


def test_the_same_run_by_digest_and_files_is_not_diffed(world):
    world["new_digest"] = "reach-live"
    assert _staging() == 0
    assert world["diffs"] == 0
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["binding_diff"]["note"] == \
        "same reach run (digest and files)"


def test_a_rerun_resumes_where_it_stopped(world, capsys):
    world["fail"] = {"upload"}
    assert _staging() == 1
    assert "STOPPED at staging" in capsys.readouterr().err
    assert _latest(world) == NEW                              # checks passed and promoted before the upload failed
    world["fail"] = set()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ALL[:-1] + ["build", "upload", "smoke"]
    assert world["diffs"] == 1                                # checked once; the rerun read the ledger
    assert _staging() == 0                                    # done: the upload is not repeated
    assert [c for c, _ in world["calls"]][-1] == "build"
    assert "already on staging" in capsys.readouterr().out


def test_a_rerun_after_the_promote_keeps_the_first_runs_min_app(world, capsys):
    """T484 (r-1b4f54fb6ed9, 2026-11-07): the upload failed after the promote; the rerun took the default min app from
    LATEST — by then the new, unstamped release — so the gate saw "any" and refused paint/5 + salmon/4."""
    world["new_stamp"] = None                                 # the build does not stamp; the upload does
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    world["fail"] = {"upload"}
    assert _staging() == 1
    assert "STOPPED at staging" in capsys.readouterr().err and _latest(world) == NEW
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["checks"]["min_app_version"] == "0.5.0" and rec["live_before"] == LIVE
    world["fail"] = set()
    assert _staging() == 0, capsys.readouterr().err
    ups = [env for c, env in world["calls"] if c == "upload"]
    assert [u["MIN_APP_VERSION"] for u in ups] == ["0.5.0", "0.5.0"]


def test_a_rerun_with_no_checks_recorded_falls_back_to_the_release_before_the_promote(world):
    led = {"releases": {NEW: {"live_before": LIVE}}}
    assert R.resume_min_app(led, NEW, NEW) == "0.5.0"         # LIVE's stamp, not NEW's (LATEST now)
    led["releases"][NEW]["checks"] = {"min_app_version": "0.6.0"}
    assert R.resume_min_app(led, NEW, NEW) == "0.6.0"
    assert R.resume_min_app({"releases": {}}, None, LIVE) == "0.5.0"     # a first run: the live stamp


def test_fast_fail_runs_first_and_stops_the_release(world, capsys):
    world["fast_fail_rc"] = 1
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "fast-fail.sh failed" in err
    assert world["calls"] == [] and _latest(world) == LIVE and world["fast_fails"] == 1


def test_preflight_runs_every_check_and_lists_every_failure_together(world, capsys):
    """T666 item 3 (build 27: six runs, each stopped by the next single failure). An instant failure is said in its
    first second (T535) and the rest still run — fast-fail too — so one stop carries the whole list."""
    world["dirty"], world["pushed"] = ["pipeline/build.py"], False
    world["site_problems"] = ["site 0.6.9 < 0.7.0"]
    world["landed"] = (False, "review marks: 1 NOT LANDED in the curated files:\n  would dfo dfo|2:some-river:x|: y")
    world["atlas_behind"] = ["cuts.json (curated:waters.cuts): changed since the atlas was built"]
    world["ci"] = (1, "ci-status: STOP  pipeline-ci.yml: RED on abc123 (failure): https://github.com/x/runs/7")
    world["fast_fail_rc"], world["fast_fail_failed"] = 1, ["app gates (pnpm check:fast)", "format pairs"]
    world["fresh_copy_rc"] = 1
    assert _staging() == 1
    io = capsys.readouterr()
    assert "[1/7] preflight FAILED: 8 problems — ALL of them" in io.err and "STOPPED at preflight" in io.err
    for said in ("pipeline/build.py", "cuts.json (curated:waters.cuts)", "dfo|2:some-river:x|", "not on origin/",
                 "CI is red", "site 0.6.9 < 0.7.0", "- app gates (pnpm check:fast)", "- format pairs",
                 "the fresh-copy test run failed"):
        assert said in io.err, said
    assert [f"  {k}. " in io.err for k in range(1, 9)] == [True] * 8 and io.err.count("     next: ") == 8
    assert "6 check(s) failed already" in io.out and world["fast_fails"] == 1      # it ran to complete the list
    assert world["fresh_copies"] == 1
    assert io.out.index("STOP: 1 uncommitted path(s)") < io.out.index("6 check(s) failed already")   # said at once
    assert world["calls"] == [] and _latest(world) == LIVE
    assert "fast_fail" not in json.loads(R.LEDGER.read_text())                      # a failed run records no pass


def test_one_failure_reads_as_it_always_did(world, capsys):
    world["pushed"] = False
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "[1/7] preflight FAILED: HEAD abc123 is not on origin/release/0.9.0" in err
    assert "problems" not in err and "  next: git push origin release/0.9.0 (after" in err


def test_a_rerun_skips_fast_fail_on_the_same_tree_content(world, capsys):
    """T535: fast-fail ran again on every resume. T666 item 5: the pass is keyed on the tree's CONTENT, not on HEAD —
    a commit that changes no content (or the same tree under a new HEAD) does not rerun it; any edit does."""
    world["fail"] = {"upload"}
    assert _staging() == 1 and world["fast_fails"] == 1
    assert world["calls"][0] == ("build", None)       # (T661) heavy.sh runs no fast-fail: nothing is handed to it
    world["fail"] = set()
    R.git_head = lambda: "def456"                             # a new HEAD over the same content: not rerun
    try:
        assert _staging() == 0
        assert world["fast_fails"] == 1 and "fast-fail passed for this tree's content (key-t1" in capsys.readouterr().out
        done = json.loads(R.LEDGER.read_text())["fast_fail"]
        assert (done["head"], done["key"]) == ("abc123", "key-t1")
        world["tree"] = "t2"                                  # an edit, committed or not: it runs again
        assert _staging() == 0 and world["fast_fails"] == 2
        assert json.loads(R.LEDGER.read_text())["fast_fail"]["key"] == "key-t2"
    finally:
        R.git_head = lambda: "abc123"


def test_after_a_pass_only_what_changed_since_is_checked(world, capsys):
    """T666 item 5 (build 27: the fixtures re-cut was a new commit and fast-fail ran its whole 8 minutes again): the
    base is the commit it last passed on, so a fixtures or docs commit reruns no test tier."""
    world["commits"] = {"abc123", "c0ffee"}
    R.save_ledger({"releases": {LIVE: {"commit": "c0ffee"}}})
    assert _staging() == 0 and world["fast_fail_base"] == "c0ffee"            # first pass: everything since the ship
    world["tree"], world["changed_since"] = "t2", {"app/fixtures/golden/taps.json"}
    capsys.readouterr()
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"
    assert "fast-fail base: abc123, the commit it last passed on" in capsys.readouterr().out
    # a file that was uncommitted at that pass and is back at the commit's content was never checked there: ship base
    led = json.loads(R.LEDGER.read_text())
    led["fast_fail"]["dirty"] = ["app/x.ts"]
    R.LEDGER.write_text(json.dumps(led))
    world["tree"] = "t3"
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"            # NEW is live now: the commit it was built from
    out = capsys.readouterr().out
    assert "1 file(s) uncommitted at its last pass are back at abc123's content (app/x.ts)" in out
    assert f"fast-fail base: abc123, the commit {NEW} was built from" in out
    world["tree"], world["changed_since"] = "t4", {"app/x.ts", "docs.md"}     # still different from that commit: usable
    led = json.loads(R.LEDGER.read_text())
    led["fast_fail"]["dirty"] = ["app/x.ts"]
    R.LEDGER.write_text(json.dumps(led))
    assert _staging() == 0 and world["fast_fail_base"] == "abc123"
    assert "the commit it last passed on" in capsys.readouterr().out


def test_red_ci_stops_at_preflight_and_the_escape_prints_its_reason(world, capsys):
    """T533: CI was red from 10-29 to 11-09 and no release asked GitHub."""
    world["ci"] = (1, "ci-status: STOP  pipeline-ci.yml: RED on abc123 (failure): https://github.com/x/runs/7")
    assert _staging() == 1
    io = capsys.readouterr()
    assert "STOPPED at preflight" in io.err and "CI is red" in io.err and "runs/7" in io.out
    assert world["calls"] == [] and world["fast_fails"] == 1      # (T666 item 3) the rest still ran
    world["ci"] = (2, "ci-status: STOP  app-ci.yml: CANNOT TELL — gh: not logged in")
    assert _staging() == 1 and "could not read CI" in capsys.readouterr().err
    world["ci"] = (0, "ci-status: SKIPPED (not asked): CI down for T999")
    with pytest.raises(SystemExit):
        _staging(no_ci_check=" ")
    assert _staging(no_ci_check="CI down for T999") == 0
    assert world["ci_skips"][-1] == "CI down for T999"
    assert _env_of(world, "upload")["CI_CHECK_SKIP"] == "release --no-ci-check: CI down for T999"


def test_fast_fail_checks_everything_since_the_commit_latest_was_built_from(world, capsys):
    """T533: with HEAD pushed, fast-fail's "changed" (not on origin) was nothing; the base is now LATEST's commit."""
    R.save_ledger({"releases": {LIVE: {"commit": "c0ffee"}}})
    world["commits"] = {"c0ffee"}
    assert _staging() == 0 and world["fast_fail_base"] == "c0ffee"
    assert "fast-fail base: c0ffee, the commit r-live00000000 was built from" in capsys.readouterr().out


def test_no_recorded_base_is_said_not_hidden(world, capsys):
    R.save_ledger({"releases": {LIVE: {"commit": "gone99"}}})          # recorded, but not in this clone
    assert _staging() == 0 and world["fast_fail_base"] is None
    assert "fast-fail base: no commit recorded for r-live00000000" in capsys.readouterr().out


def test_no_fast_fail_skips_it_and_hands_heavy_nothing(world):
    """(T661) heavy.sh runs no fast-fail of its own (the T535 gate was dropped): the skip is the release's alone."""
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 0
    assert world["fast_fails"] == 0 and world["calls"][0] == ("build", None)


def test_the_dry_run_runs_the_site_gate_and_the_app_gate(world, capsys):
    """T535: the dry run omitted the site gate the real preflight stops on."""
    world["site_problems"] = ["the staging site is app 0.6.9, older than 0.7.0"]
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}   # in the candidate the build's dry run names
    assert R.main(["--staging", "--dry-run", "--min-app-version", "0.7.0"]) == 0
    out = capsys.readouterr().out
    assert "WOULD STOP: the staging site is app 0.6.9" in out and world["site_min_apps"] == ["0.7.0"]
    assert f"app-version gate on {LIVE} (min app 0.7.0): ok" in out
    world["site_problems"] = []
    assert R.main(["--staging", "--dry-run", "--min-app-version", "0.4.0"]) == 0
    out = capsys.readouterr().out
    assert "ok: site serves app 9.9.9" in out and f"app-version gate on {NEW} (min app 0.4.0): WOULD STOP: salmon/4" in out


def test_no_fast_fail_needs_a_reason_and_prints_it(world, capsys):
    world["fast_fail_rc"] = 1
    with pytest.raises(SystemExit):
        _staging(no_fast_fail=" ")
    assert world["fast_fails"] == 0 and world["calls"] == []
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 0
    assert "fast-fail SKIPPED (--no-fast-fail): tsc is down for the T999 refactor" in capsys.readouterr().out
    assert world["fast_fails"] == 0


def test_an_unpushed_head_stops_at_preflight_not_at_the_upload(world, capsys):
    """T484: found at stage 6 on 2026-11-07 (after the build, checks and promote); it belongs in preflight."""
    world["pushed"] = False
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "not on origin/release/0.9.0" in err and "git push" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_marks_that_have_not_landed_stop_at_preflight_even_without_fast_fail(world, capsys):
    """T545: two owner marks read "saved" and were in no curated file; no release looked."""
    world["landed"] = (False, "review marks: 1 NOT LANDED in the curated files:\n  would dfo dfo|2:some-river:x|: y")
    for kw in ({}, {"no_fast_fail": "fast-fail is down"}):
        assert _staging(**kw) == 1
        err = capsys.readouterr().err
        assert "STOPPED at preflight" in err and "dfo|2:some-river:x|" in err and "review.apply" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_the_baseline_is_kept_before_the_build_overwrites_the_reach_run(world):
    world["differs"] = True
    assert _staging() == 1
    base = R.HOME / "baseline" / LIVE
    assert json.loads((base / "report.json").read_text())["digest"] == "reach-live"
    assert json.loads((world["reaches"] / "report.json").read_text())["digest"] == "reach-new"
    assert world["diffs"] == 1


def test_an_accepted_diff_is_compared_by_content_not_by_its_paths(world, tmp_path, capsys):
    """caught (2026-11-09): `--accept-diff` refused a diff identical in content because its `after` path was relative
    in one and absolute in the other (the file's bytes were compared)."""
    world["differs"] = True
    assert _staging() == 1
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    rep = json.loads(js.read_text())
    same = tmp_path / "same.json"                             # the same diff, written with other path strings
    same.write_text(json.dumps({**rep, "before": "data/generated/release/baseline/x", "after": "reaches/full"}))
    other = tmp_path / "other.json"                           # another diff: one more rule moved
    other.write_text(json.dumps({**rep, "rules_changed": 2, "rules": {"e::r": {"gained": 1}}}))
    capsys.readouterr()
    assert _staging(accept_diff=other) == 1 and _latest(world) == LIVE
    assert "is not this diff" in capsys.readouterr().err
    assert _staging(accept_diff=same) == 0 and _latest(world) == NEW
    assert R.diff_content_sha(same) == R.diff_content_sha(js) != R.diff_content_sha(other)


def test_a_ledger_row_from_before_content_keys_still_takes_its_own_json(world):
    world["differs"] = True
    assert _staging() == 1
    led = json.loads(R.LEDGER.read_text())
    del led["releases"][NEW]["binding_diff"]["content"]       # written by the byte-compare version
    R.LEDGER.write_text(json.dumps(led))
    assert _staging(accept_diff=R.HOME / "checks" / NEW / "binding_diff.json") == 0


def test_an_old_staging_site_stops_at_preflight_not_after_the_upload(world, capsys):
    """caught (2026-11-09): a 0.7.0 release went up under a 0.6.9 staging site; the smoke test failed after upload."""
    world["site_problems"] = ["the staging site is app 0.6.9, older than 0.7.0 … deploy/staging/site.sh"]
    assert _staging(min_app_version="0.7.0") == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "site.sh" in err
    assert world["calls"] == [] and _latest(world) == LIVE
    assert world["site_min_apps"] == ["0.7.0"]
    world["site_problems"] = []
    assert _staging() == 0 and world["site_min_apps"][-1] == "0.7.0"     # (T666 item 4) remembered from the first run


def test_the_live_stamp_is_the_min_app_when_none_was_ever_given(world):
    assert _staging() == 0 and world["site_min_apps"][-1] == "0.5.0"


def test_keep_baseline_copies_once_and_says_when_the_run_is_gone(world):
    kept, msg = R.keep_baseline(LIVE)
    assert kept and "copied" in msg
    assert json.loads((R.HOME / "baseline" / LIVE / "report.json").read_text())["digest"] == "reach-live"
    _reach(world["reaches"], "reach-other")                  # overwritten after the copy: the copy stands
    assert R.keep_baseline(LIVE) == (True, f"kept: {R.HOME / 'baseline' / LIVE} (reach reach-live)")
    import shutil
    shutil.rmtree(R.HOME / "baseline")
    kept, msg = R.keep_baseline(LIVE)
    assert not kept and "reach-other" in msg and "no copy" in msg


def test_no_baseline_and_a_moved_reach_run_stops_with_a_plain_message(world, capsys):
    _reach(world["reaches"], "reach-other")
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at baseline" in err and "--baseline" in err and world["calls"] == []


# ------------------------------------------------------------------------------- T666: what follows the release

def test_a_renumber_only_recut_is_committed_with_the_release_id_and_pushed_when_asked(world, capsys):
    """T666 item 2 / T667 D5 (build 27: the goldens could only be re-cut AFTER the promote, by hand, as a new commit):
    the release re-cuts on the CANDIDATE and commits it; with --push it pushes its own commits and goes on."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json", "app/packages/data/dev/regs-release/r-x/manifest.json",
                              "app/packages/data/dev/section-lines.json"]
    assert R.main(["--staging", "--push"]) == 0
    out = capsys.readouterr().out
    assert world["recuts_committed"] == [(NEW, "renumber only")] and world["pushes"] == 1
    assert "the re-cut changed 3 path(s): goldens 1, regs-fixture 2" in out
    assert "pushed the 1 commit(s) the release made (--push)" in out and _latest(world) == NEW
    names = [c for c, _ in world["calls"]]
    assert names.index("recut") < names.index("follow-py") < names.index("upload")   # before the promote and the upload
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]
    assert rec["recut"]["kind"] == "renumber only" and rec["cut_for"] == NEW and rec["commits"] == ["f1xture" + "0" * 33]
    assert R.recut_cmd(NEW)[:3] == ["node", "app/tools/regs-recut/sync.mjs", "--release"]
    assert R.recut_cmd(NEW)[3] == str(world["root"] / NEW)                             # the candidate, never LATEST


def test_a_changed_golden_page_stops_uncommitted_until_its_own_token_is_accepted(world, capsys):
    """T667 D5: the release stops only when an ANSWER changed (a golden page's words)."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/glenn_501.json"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "changed 1 golden page(s)" in err and "glenn_501.json" in err
    assert "nothing was committed" in err and "--accept-fixtures tok-2" in err
    assert world["recuts_committed"] == [] and _latest(world) == LIVE
    assert "follow-py" not in [c for c, _ in world["calls"]]                # no test ran on an unaccepted re-cut
    assert _staging(accept_fixtures="tok-9") == 1                           # another re-cut's token is not this one
    assert "--accept-fixtures tok-9 is not this re-cut" in capsys.readouterr().err and world["recuts_committed"] == []
    assert _staging(accept_fixtures="tok-2") == 1                           # accepted and committed: the push gate
    assert world["recuts_committed"] == [(NEW, "golden pages accepted")]
    assert "1 the release made" in capsys.readouterr().err
    world["pushed"], world["unpushed"] = True, []
    assert _staging() == 0 and _latest(world) == NEW


def test_a_moved_answer_pin_stops_with_the_pages_and_a_moved_count_is_committed(world, capsys, monkeypatch):
    """(T1135) the re-cut records the release pins: a moved `answers: false` pin (a count) is committed with the release
    id, no stop; a moved `answers: true` pin (words an angler reads) stops like a golden page, named, until the same
    --accept-fixtures token (one token covers the pages and the pins)."""
    pins = "app/fixtures/golden/release-pins.json"
    moved = {pins: []}
    monkeypatch.setattr(R.RF, "moved_answer_pins", lambda p, rev="HEAD": moved.get(p, []))
    world["recut_changed"] = ["app/fixtures/golden/taps.json", pins]
    assert R.main(["--staging", "--push"]) == 0                              # a count moved: committed, no stop
    assert world["recuts_committed"] == [(NEW, "renumber only")] and "pins 1" in capsys.readouterr().out


def test_a_moved_sentence_pin_stops_until_its_token(world, capsys, monkeypatch):
    pins = "pipeline/tests/fixtures/release-pins.json"
    monkeypatch.setattr(R.RF, "moved_answer_pins", lambda p, rev="HEAD": ["answers26.williston.applies"] if p == pins else [])
    world["recut_changed"] = ["app/fixtures/golden/taps.json", pins]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "nothing was committed" in err and "--accept-fixtures tok-2" in err
    assert f"{pins} — answer pin(s) moved: answers26.williston.applies" in err and "git diff -- " in err
    assert world["recuts_committed"] == []
    assert _staging(accept_fixtures="tok-2") == 1 and world["recuts_committed"] == [(NEW, "golden pages accepted")]


def test_a_moved_answer_pin_still_stops_when_the_golden_pages_only_renumbered(world, capsys, monkeypatch):
    """(T1135 review) the renumber auto-accept covers golden pages only: a moved answer pin beside them still stops."""
    pins, page = "app/fixtures/golden/release-pins.json", "app/packages/ui-native/src/regs/fixtures/glenn_501.json"
    monkeypatch.setattr(R.RF, "moved_answer_pins", lambda p, rev="HEAD": ["sourceLine.rowPages.glenn_501"] if p == pins else [])
    asked = []
    monkeypatch.setattr(R, "recut_renumbered", lambda pages: asked.append(list(pages)) or (True, "1 page renumbered"))
    world["recut_changed"] = [pins, page]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert asked == [[page]] and f"{pins} — answer pin(s) moved: sourceLine.rowPages.glenn_501" in err
    assert "STOPPED at follows" in err and world["recuts_committed"] == []


def test_a_broken_pins_file_is_a_stop_never_a_traceback_or_an_accept(world, capsys, monkeypatch):
    pins = "pipeline/tests/fixtures/release-pins.json"

    def broken(p, rev="HEAD"):
        raise R.RF.PinsFileError(f"{p}: pin 'x' is \"y\", not {{...}}; it is written by `{R.RF.REFRESH_PINS}`")
    monkeypatch.setattr(R.RF, "moved_answer_pins", broken)
    world["recut_changed"] = [pins]
    for kw in ({}, {"accept_fixtures": "tok-1"}):
        assert _staging(**kw) == 1
        err = capsys.readouterr().err
        assert "STOPPED at follows" in err and "a release-pins file that is not one" in err and "pin 'x'" in err
        assert "node app/tools/regs-recut/sync.mjs --release" in err and "Traceback" not in err
        assert world["recuts_committed"] == []


def test_push_never_pushes_a_commit_the_release_did_not_make(world, capsys):
    world["recut_changed"] = ["app/fixtures/golden/taps.json"]
    world["unpushed"] = ["0ther" + "0" * 35]                                 # another lane committed during the build
    assert R.main(["--staging", "--push"]) == 1
    err = capsys.readouterr().err
    assert world["pushes"] == 0 and "1 made by someone else since preflight (0ther0000000)" in err
    assert "--push pushes only the release's own commits" in err and _latest(world) == LIVE


def test_the_tests_that_ask_for_the_release_run_on_the_candidate_before_the_promote(world, capsys):
    """T666 item 2 (build 27: 13 tests compared with LATEST and were red from the build to the promote)."""
    world["rc"] = {"follow-py": 1, "follow-app": 1}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "pipeline: rc=1" in err and "app: rc=1" in err and "LATEST was not moved" in err
    assert "2 failure(s) on the candidate" in err and "(no failed test in its report" in err   # no report: the rc stands
    assert _latest(world) == LIVE and "upload" not in [c for c, _ in world["calls"]]
    for name in ("follow-py", "follow-app", "follow-worker", "app-gate"):    # every check ran: one stop, all named
        env = _env_of(world, name)
        assert env["RELEASE_DIR"] == str(world["root"] / NEW) and env["REGS_STORE_REQUIRED"] == "1"
    assert "pipeline/tests/test_x.py" in [c for c in R.follow_tests_cmds(R.follow_tests()[0], R.HOME)[0][1]]
    with pytest.raises(SystemExit):
        _staging(no_follow_tests=" ")
    world["calls"].clear()
    assert _staging(no_follow_tests="perf suite on a loaded machine, T999") == 0
    assert "tests on the candidate SKIPPED (--no-follow-tests): perf suite on a loaded machine, T999" in capsys.readouterr().out
    assert "follow-py" not in [c for c, _ in world["calls"]]


def test_every_failure_on_the_candidate_is_listed_by_name_in_one_stop(world, capsys):
    """T680 (r-483b295de57c: the re-cut moved section 578 to 586 and changed a golden page's words; WaterMode.test.tsx
    and one-part-one-page.test.ts failed in CI one round at a time, after the push): after the re-cut the WHOLE app
    check runs on the candidate — the app suite, the worker suite, each check:fast gate — and the one stop names every
    failing test, from every suite, before anything is pushed."""
    app_json = json.dumps({"testResults": [
        {"name": str(R.REPO_ROOT / "app/packages/ui-native/src/WaterMode.test.tsx"), "status": "failed",
         "assertionResults": [{"status": "failed", "fullName": "one water screen > (T53) an unnamed reach"},
                              {"status": "passed", "fullName": "fine"}]},
        {"name": str(R.REPO_ROOT / "app/tools/one-part-one-page.test.ts"), "status": "failed",
         "assertionResults": [{"status": "failed", "fullName": "(T387) every tap on a part gives the same page"}]},
        {"name": str(R.REPO_ROOT / "app/tools/broken.test.ts"), "status": "failed", "message": "SyntaxError: x\nmore",
         "assertionResults": []}]})
    junit = ('<testsuites><testsuite><testcase classname="pipeline.tests.test_x" name="test_a"><failure/></testcase>'
             '<testcase classname="pipeline.tests.test_x" name="test_ok"/></testsuite></testsuites>')
    world["reports"] = {"follow-app": app_json, "follow-py": junit}
    world["rc"] = {"follow-py": 1, "follow-app": 1, "app-gate": 2}
    world["recut_changed"] = ["app/packages/data/dev/section-lines.json"]
    assert R.main(["--staging", "--push"]) == 1
    err = capsys.readouterr().err
    assert "5 failure(s) on the candidate" in err and "ALL of them" in err
    for line in ("pipeline: pipeline.tests.test_x::test_a",
                 "app: app/packages/ui-native/src/WaterMode.test.tsx › one water screen > (T53) an unnamed reach",
                 "app: app/tools/one-part-one-page.test.ts › (T387) every tap on a part gives the same page",
                 "app: app/tools/broken.test.ts: SyntaxError: x", "app gate typecheck: rc=2"):
        assert line in err
    assert "test_ok" not in err and "fine" not in err
    assert world["pushes"] == 0 and _latest(world) == LIVE                  # stopped before the push gate
    names = [c for c, _ in world["calls"]]
    assert names.index("recut") < names.index("follow-py") < names.index("follow-app") < names.index("follow-worker") \
        < names.index("app-gate")
    assert "tests" not in json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]   # not recorded as passed


def test_the_app_gates_are_read_from_check_fast_one_by_one():
    """T680: each check:fast gate runs on its own (the && chain stops at the first failure), read from its one home;
    the worker typecheck is one of them (it was in no check)."""
    from pipeline import release_follows as RF
    steps = RF.app_check_steps()
    assert steps[:2] == ["tickets", "anchors"] and "typecheck" in steps and "typecheck:worker" in steps
    assert RF.app_check_cmd("anchors") == ["pnpm", "-C", "app", "run", "-s", "anchors"]


def test_the_rehearsal_recuts_runs_everything_and_puts_the_fixtures_back(world, capsys, tmp_path, monkeypatch):
    """T680: `--dry-run --rehearse-follows` on a built candidate re-cuts in place, runs every check, lists every
    failure and the golden pages with the token the real run would take — and leaves the fixtures byte for byte."""
    repo = tmp_path / "repo"
    (repo / "app/fixtures/golden").mkdir(parents=True)
    (repo / "app/fixtures/golden/taps.json").write_text("before")
    pages = repo / "app/packages/ui-native/src/regs/fixtures"
    pages.mkdir(parents=True)
    (pages / "glenn_501.json").write_text("old words")
    monkeypatch.setattr(R, "REPO_ROOT", repo)
    real = R.run_cmd

    def run_cmd(cmd, env=None):
        rc = real(cmd, env)
        if any("regs-recut/sync.mjs" in c for c in cmd):               # the re-cut writes in place
            (repo / "app/fixtures/golden/taps.json").write_text("after")
            (pages / "glenn_501.json").write_text("new words")
            (pages / "new_1.json").write_text("+")
        return rc
    monkeypatch.setattr(R, "run_cmd", run_cmd)
    # what differs from "HEAD", read from the disk (so the put-back is seen)
    monkeypatch.setattr(R, "recut_changed", lambda: [p for p, head in (
        ("app/fixtures/golden/taps.json", "before"), ("app/packages/ui-native/src/regs/fixtures/glenn_501.json",
                                                     "old words")) if (repo / p).read_text() != head])
    assert R.main(["--staging"]) == 1 and "STOPPED at follows" in capsys.readouterr().err   # built: a CANDIDATE
    world["calls"].clear()
    (repo / "app/fixtures/golden/taps.json").write_text("before")      # the stopped real run's re-cut, undone
    (pages / "glenn_501.json").write_text("old words")
    (pages / "new_1.json").unlink()
    world["rc"] = {"follow-worker": 1}
    with pytest.raises(SystemExit):
        R.main(["--staging", "--rehearse-follows"])                   # a rehearsal only
    assert R.main(["--staging", "--dry-run", "--rehearse-follows"]) == 0
    out = capsys.readouterr().out
    assert "the re-cut would change 2 path(s)" in out and "1 golden page(s) change" in out and "--accept-fixtures tok-2" in out
    assert "1 failure(s) on the candidate" in out and "worker: rc=1" in out
    names = [c for c, _ in world["calls"]]
    assert names.count("recut") == 1 and {"follow-py", "follow-app", "follow-worker", "app-gate"} <= set(names)
    assert (repo / "app/fixtures/golden/taps.json").read_text() == "before"            # put back, byte for byte
    assert (pages / "glenn_501.json").read_text() == "old words" and not (pages / "new_1.json").exists()
    assert world["recuts_committed"] == [] and world["pushes"] == 0 and _latest(world) == LIVE
    assert "WARNING" not in out


def test_fast_fail_reads_the_candidate_once_the_fixtures_are_cut_for_it(world):
    """Between the re-cut and the promote the goldens pair with the CANDIDATE: a rerun's fast-fail is told so
    (RELEASE_DIR), and not before the re-cut (they still pair with LATEST then)."""
    world["rc"] = {"inseason": 3}
    assert _staging() == 1 and world["fast_fail_env"] == NEXT                  # no candidate yet
    world["tree"] = "t2"
    assert _staging() == 1 and world["fast_fail_env"] == NEXT                  # a candidate, fixtures not re-cut on it
    world["rc"], world["recut_changed"], world["tree"] = {}, ["app/fixtures/golden/taps.json"], "t3"
    assert _staging() == 1                                                   # re-cut + committed: the push gate
    world["pushed"], world["unpushed"] = True, []
    assert _staging() == 0
    # (T993 item 15) the candidate's tests passed on the run before: the follows stage will not run them again, so
    # fast-fail keeps its app gates and worker suite (no FOLLOWS_NEXT)
    assert world["fast_fail_env"] == {"RELEASE_DIR": str(world["root"] / NEW), R.FRESH_COPY_NEXT: "1"}
    assert json.loads(R.LEDGER.read_text())["fast_fail"]["release_dir"] == str(world["root"] / NEW)


#: (T791) the fresh copy runs next in the same preflight: fast-fail leaves it the whole tier; (T993 item 15) and the
#: follows stage runs the app gates and the worker suite on the candidate: fast-fail leaves those to it
NEXT = {R.FRESH_COPY_NEXT: "1", R.FOLLOWS_NEXT: "1"}


def test_fast_fail_leaves_the_whole_tier_to_the_fresh_copy_that_follows(world):
    """T791 item 3 (owner 2026-11-16 "Yes, as proposed"): the same 6,490 tests ran in place inside fast-fail, then on the
    clean clone minutes later. Fast-fail is told the fresh copy follows; with --no-fresh-copy it is not (it runs them)."""
    assert _staging() == 0 and world["fast_fail_env"] == NEXT and world["fresh_copies"] == 1
    world["tree"] = "t2"
    assert _staging(no_fresh_copy="the clone is down") == 0 and world["fast_fail_env"] == {R.FOLLOWS_NEXT: "1"}


def test_fast_fail_sh_skips_the_in_place_tier_and_its_subsets_when_told():
    """The script's side: the three steps that run the whole no-data tier or a subset of it check the variable first."""
    from pipeline.common.curated import REPO_ROOT
    sh = (REPO_ROOT / "scripts" / "fast-fail.sh").read_text(encoding="utf-8")
    assert f'${{{R.FRESH_COPY_NEXT}:-}} == 1' in sh
    for fn in ("as_ci", "source_scanners", "review_tests"):
        body = sh[sh.index(f"{fn}() {{"):]
        body = body[:body.index("\n}\n")]
        assert "fresh_copy_next && return 0" in body, fn


def test_the_command_remembers_its_flags_and_a_rerun_needs_none(world, tmp_path, capsys):
    """T666 item 4 (build 27: `--baseline` / `--accept-diff` had to be retyped, and were wrong after the promote)."""
    kept = tmp_path / "full_prev"
    _reach(kept, "reach-live")
    _reach(world["reaches"], "reach-other")                  # the live run was overwritten before this release began
    world["differs"] = True
    assert _staging(baseline=kept) == 1                      # the diff stops it
    assert json.loads((R.HOME / "baseline" / LIVE / "report.json").read_text())["digest"] == "reach-live"   # kept
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    world["fail"] = {"upload"}
    assert _staging(accept_diff=js, min_app_version="0.6.0") == 1 and _latest(world) == NEW
    run = json.loads(R.LEDGER.read_text())["run"]
    assert run["promoted"] == NEW and run["flags"] == {"min_app": "0.6.0"}     # the promote spent the other two
    world["fail"] = set()
    capsys.readouterr()
    assert _staging() == 0                                   # the bare command
    out = capsys.readouterr().out
    assert "remembered from an earlier run of this release" in out and "--min-app-version 0.6.0" in out
    assert _env_of(world, "smoke") is None and [u["MIN_APP_VERSION"] for c, u in world["calls"] if c == "upload"] == ["0.6.0"] * 2
    assert "run" not in json.loads(R.LEDGER.read_text())     # on staging: the next release starts clean


def test_a_baseline_flag_left_over_from_before_the_promote_is_ignored_out_loud(world, tmp_path, capsys):
    kept = tmp_path / "full_prev"
    _reach(kept, "reach-live")
    world["fail"] = {"upload"}
    assert _staging(baseline=kept) == 1 and _latest(world) == NEW
    world["fail"] = set()
    capsys.readouterr()
    assert _staging(baseline=kept) == 0                      # the first run's command line, typed again
    assert "left over from before the promote, IGNORED" in capsys.readouterr().out
    world2 = tmp_path / "wrong"
    _reach(world2, "reach-wrong")
    R.save_ledger({"releases": {}})
    (world["root"] / "LATEST").write_text(LIVE + "\n")       # a first run with a wrong baseline is still refused
    _reach(world["reaches"], "reach-live")
    assert _staging(baseline=world2) == 1 and "STOPPED at baseline" in capsys.readouterr().err


def test_forget_flags_drops_what_was_remembered(world, capsys):
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/4"}}
    assert _staging(min_app_version="0.4.0") == 1
    capsys.readouterr()
    assert R.main(["--staging", "--forget-flags"]) == 0      # back to the live stamp (0.5.0)
    assert "--forget-flags: dropped --min-app-version" in capsys.readouterr().out


def test_an_escape_is_never_remembered(world, capsys):
    world["fast_fail_rc"] = 1
    world["fail"] = {"upload"}
    assert _staging(no_fast_fail="tsc is down for the T999 refactor") == 1 and world["fast_fails"] == 0
    assert json.loads(R.LEDGER.read_text())["run"]["flags"] == {}
    assert _staging() == 1 and world["fast_fails"] == 1      # the bare rerun runs the guard again
    assert "STOPPED at preflight" in capsys.readouterr().err


def test_a_workflow_that_would_not_start_stops_saying_only_the_upload_is_left(world, capsys):
    """T666 item 6 (build 27: GitHub refused the start and the only retry was the whole command)."""
    world["rc"] = {"upload": R.RC_WORKFLOW_NOT_STARTED}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at staging" in err and "did not start the run-timing workflow" in err and "only the upload runs" in err
    assert _latest(world) == NEW and "uploaded_at" not in json.loads(R.LEDGER.read_text())["releases"][NEW]
    world["rc"] = {}
    world["calls"].clear()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ["build", "upload", "smoke"] and world["fast_fails"] == 1
    up = (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
    assert "scripts/start-workflow.sh" in up and "gh workflow run" not in up       # the one start, with its retry
    assert 'exit "$rc"' in up                                                      # its 75 reaches the release


def test_a_dispatch_accepted_whose_run_cannot_be_read_says_check_it_never_redispatch(world, capsys):
    """T993 lane G3: start-workflow.sh exits 76 when GitHub accepted the dispatch but its run cannot be read (it may
    be running). Read as a plain upload failure, a rerun would dispatch a second run."""
    world["rc"] = {"upload": R.RC_WORKFLOW_UNREADABLE}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at staging" in err and "dispatched (GitHub accepted it) but its run could not be read" in err
    assert "check it, don't re-dispatch" in err and "failed (rc=76)" not in err
    assert _latest(world) == NEW and "uploaded_at" not in json.loads(R.LEDGER.read_text())["releases"][NEW]


def test_the_uploads_first_refusals_are_asked_before_it_starts(world, capsys):
    world["staging_prechecks"] = ["`gh auth status` failed (rc=1)", "the rclone remote `r2:` is not configured"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "[7/7] staging FAILED: 2 problems" in err and "gh auth status" in err and "rclone remote" in err
    assert "upload" not in [c for c, _ in world["calls"]] and _latest(world) == NEW


def test_the_rehearsal_walks_every_stages_checks_and_lists_the_stops(world, capsys):
    """T666 item 6: `--dry-run` predicts the stops. It reads the SAME preflight table as the real run."""
    assert R.main(["--staging", "--dry-run"]) == 0           # before the build: preflight, baseline, the build's plan
    out = capsys.readouterr().out
    assert "REHEARSAL" in out and "rehearsal: no stop found before the build" in out
    assert world["fast_fails"] == 1 and "ok: fast-fail passed" in out          # fast-fail is a check: it ran
    assert json.loads(R.LEDGER.read_text())["fast_fail"]["key"] == "key-t1"     # … and the real run will skip it
    assert [c for c, _ in world["calls"]] == ["build"] and _latest(world) == LIVE   # the build's own --dry-run only
    assert "run" not in json.loads(R.LEDGER.read_text())                        # a rehearsal remembers no flag
    # the mock's build dry run left a CANDIDATE: now every later stage is rehearsed on it
    world["dirty"], world["pushed"] = ["pipeline/build.py"], False
    world["manifest_problems"] = ["files.water: missing"]
    world["inseason_check"] = ("unresolved", "in-season matches on r-new: WOULD CHANGE, 1 unresolved")
    world["goldens_paired"], world["rc"] = False, {"taps": 1}
    world["staging_prechecks"] = ["the rclone remote `r2:` is not configured"]
    world["calls"].clear()
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert world["fast_fails"] == 1                                             # same tree: not rerun
    for said in ("WOULD STOP: 1 uncommitted path(s) the build reads: pipeline/build.py",
                 "WOULD STOP: HEAD abc123 is not on origin/release/0.9.0",
                 f"manifest sanity on {NEW}: WOULD STOP: files.water: missing",
                 "WOULD STOP: reach run on disk is reach-new and no copy is kept (--baseline DIR)",
                 "binding diff: not rehearsed: no baseline copy is kept",
                 f"in-season matches: WOULD STOP: a match does not resolve on {NEW}",
                 f"fixtures: WOULD STOP: the re-cut's first step (taps.py) fails on {NEW}",
                 f"the upload's first refusals, on {NEW}: WOULD STOP: the rclone remote",
                 "rehearsal: the real run would STOP 7 time(s) as things are:"):
        assert said in out, said
    assert [c for c, _ in world["calls"]] == ["build", "taps"]                  # nothing re-cut, committed or sent
    assert _env_of(world, "taps") is None and _latest(world) == LIVE and world["recuts_committed"] == []
    assert "follows" not in json.loads(R.LEDGER.read_text()).get("releases", {}).get(NEW, {})


def test_the_rehearsal_and_the_run_read_one_preflight_table(world, capsys):
    """By construction: a check added to PREFLIGHT is in both. Every check's failure shows in the rehearsal with the
    words the real run stops with."""
    assert [n for n, _ in R.PREFLIGHT] == ["committed", "atlas", "marks", "wrong-saves", "classified", "pushed",
                                           "version", "ci", "live", "min-app", "site", "pieces", "fast-fail",
                                           "fresh-copy"]
    world["pushed"], world["site_problems"], world["fast_fail_rc"] = False, ["site 0.6.9 < 0.7.0"], 1
    assert _staging() == 1
    real = capsys.readouterr()
    assert R.main(["--staging", "--dry-run"]) == 0
    rehearsed = capsys.readouterr().out
    for line in [ln.strip()[len("STOP: "):] for ln in real.out.splitlines() if ln.strip().startswith("STOP: ")]:
        assert "WOULD STOP: " + line in rehearsed, line
    assert real.out.count("    STOP: ") == 3 and "rehearsal: the real run would STOP 3 time(s)" in rehearsed


def test_the_fresh_copy_test_run_happens_once_in_the_release_pre_check(world, capsys):
    """T666, owner 2026-11-11: the clean-checkout run is the release pre-check's, once — not every quick check's. It
    is not repeated on a resume, nor after the release's own fixtures / matches commits; another commit runs it again."""
    world["fail"] = {"upload"}
    assert _staging() == 1 and world["fresh_copies"] == 1
    assert "ok: fresh-copy test run passed on a clean clone of HEAD abc123" in capsys.readouterr().out
    assert json.loads(R.LEDGER.read_text())["fresh_copy"] == {
        "tree": "committed-t1", "head": "abc123", "at": json.loads(R.LEDGER.read_text())["fresh_copy"]["at"]}
    assert _staging() == 1 and world["fresh_copies"] == 1                      # a resume: same tree
    world["commits"] = {"abc123"}
    world["tree"] = "t2"                                                       # the release's own re-cut commit
    world["committed_since"] = {"app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/a.json"}
    capsys.readouterr()
    assert _staging() == 1 and world["fresh_copies"] == 1
    assert "the commits since touch only what the release refreshes (fixtures, matches): not rerun" in capsys.readouterr().out
    world["committed_since"] |= {"pipeline/deliver/x.py"}                      # a code commit: it runs again
    assert _staging() == 1 and world["fresh_copies"] == 2
    assert "1 path(s) committed since its pass at abc123 (pipeline/deliver/x.py): it runs again" in capsys.readouterr().out


def test_a_failed_fresh_copy_stops_before_the_build_and_the_escape_prints_its_reason(world, capsys):
    world["fresh_copy_rc"] = 1
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "on a clean clone of HEAD abc123" in err and "--no-fresh-copy REASON" in err
    assert world["calls"] == [] and "fresh_copy" not in json.loads(R.LEDGER.read_text())
    with pytest.raises(SystemExit):
        _staging(no_fresh_copy=" ")
    assert _staging(no_fresh_copy="GitHub CI ran the same tier green on this commit") == 0
    assert "fresh-copy test run SKIPPED (--no-fresh-copy): GitHub CI ran the same tier green" in capsys.readouterr().out
    assert world["fresh_copies"] == 1
    assert R.run_fresh_copy.__name__ == "fresh_copy"


def test_the_fresh_copy_runs_ci_s_tier_on_a_clone_through_heavy(monkeypatch):
    seen = []
    monkeypatch.setattr(R, "run_cmd", lambda cmd, env=None: seen.append(cmd) or 0)
    assert R.run_fresh_copy() == 0
    assert seen == [["bash", "scripts/heavy.sh", "release-fresh-copy", "30", "--", "bash", "scripts/fresh-copy-tests.sh"]]
    sh = (Path(R.REPO_ROOT) / "scripts/fresh-copy-tests.sh").read_text()
    assert "git clone -q --shared --no-checkout" in sh and "scripts/pytest-chunked.sh" in sh
    assert "MARKERS=$(no_data_markers)" in sh and "ci_no_build" not in sh      # CI's own markers (test-env.sh); no build to hide


# ------------------------------------------------------------------------------------------------------------- prod

@pytest.fixture
def staged(world, monkeypatch):
    assert _staging() == 0
    world["remote_tags"]["v0.9.0"] = world["tag_at"]["v0.9.0"] = "abc123"   # (T1062) `release --final` on rc.1
    world["calls"].clear()
    for k in ("CLAUDECODE", "AI_AGENT", "CLAUDE_CODE_ENTRYPOINT", "CONFIRM_PROD"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(R, "is_tty", lambda: True)
    return world


def test_prod_refuses_without_the_human_confirmation(staged, capsys):
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    err = capsys.readouterr().err
    assert "CONFIRM_PROD is not" in err and "deploy/prod/upload.sh" in err
    assert staged["calls"] == []


def test_prod_refuses_an_agent_even_with_the_confirmation(staged, monkeypatch, capsys):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    monkeypatch.setenv("CLAUDECODE", "1")
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    assert "agent environment" in capsys.readouterr().err and staged["calls"] == []


def test_prod_refuses_without_a_terminal_or_the_typed_id(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    monkeypatch.setattr(R, "is_tty", lambda: False)
    assert R.prod(NEW, ask=lambda _: NEW) == 2
    monkeypatch.setattr(R, "is_tty", lambda: True)
    assert R.prod(NEW, ask=lambda _: "r-typo") == 2
    assert staged["calls"] == []


def test_prod_ships_only_a_checked_staged_unchanged_release(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", LIVE)
    assert R.prod(LIVE, ask=lambda _: LIVE) == 1              # never through release --staging
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    (staged["root"] / NEW / "water.bin.gz").write_bytes(b"changed")
    assert R.prod(NEW, ask=lambda _: NEW) == 1                # not the artifact that was checked
    assert staged["calls"] == []


def test_prod_runs_the_prod_upload_for_a_human(staged, monkeypatch):
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    # the stamp the staging upload adds does not change the artifact
    m = json.loads((staged["root"] / NEW / "manifest.json").read_text())
    (staged["root"] / NEW / "manifest.json").write_text(json.dumps({**m, "published": "2026-11-05"}))
    assert R.prod(NEW, ask=lambda _: NEW) == 0
    # (T404) the one production upload, deploy/prod/upload.sh, given its own confirmation only after release --prod's
    assert staged["calls"] == [("upload", {"CONFIRM_PROD": "canifishthis.ca"})]
    assert R.prod_cmd(NEW) == ["bash", "deploy/prod/upload.sh", NEW]


def test_prod_refuses_when_staging_serves_another_release(staged, monkeypatch, capsys):
    """T537: deploy/prod/upload.sh promotes what staging serves; a later release --staging replaced it silently."""
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    staged["staging_serves"] = "r-other0000000"
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert "staging serves r-other0000000, not r-new000000000" in capsys.readouterr().err and staged["calls"] == []
    assert R.main(["--prod", NEW, "--dry-run"]) == 0
    assert "WOULD STOP: staging serves r-other0000000" in capsys.readouterr().out

    def unreadable():
        raise OSError("offline")
    monkeypatch.setattr(R, "staging_release", unreadable)
    assert R.prod(NEW, ask=lambda _: NEW) == 1 and "cannot read what staging serves" in capsys.readouterr().err


def test_a_failed_inseason_resolve_stops_before_the_promote(world, capsys):
    """T537: the resolve ran mid-upload, after LATEST moved; a failed match now stops at the checks."""
    world["fail"] = {"inseason"}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at follows" in err and "do not all resolve" in err and "LATEST was not moved" in err
    assert _latest(world) == LIVE and "upload" not in [c for c, _ in world["calls"]]


def test_the_inseason_resolve_makes_no_commit_and_the_run_stops_once_for_the_push(world, capsys):
    """T537, T666, and T679 #11: the resolve writes the GENERATED resolution (never committed), so the release's one
    commit is the fixtures re-cut, and the stop for the push comes once, at the END of the follows stage."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json"]
    assert _staging() == 1
    io = capsys.readouterr()
    assert "in-season resolved on" in io.out and "committed c0mmit" not in io.out
    assert world["recuts_committed"] == [(NEW, "renumber only")]
    assert "STOPPED at follows" in io.err and "1 commit(s) are not on origin/release/0.9.0: 1 the release made" in io.err
    assert "next: git push origin HEAD:release/0.9.0, then rerun" in io.err and "--push" in io.err
    assert _latest(world) == LIVE and [c for c, _ in world["calls"]] == ALL[:8]
    world["pushed"], world["unpushed"] = True, []                                    # the human pushed
    assert _staging() == 0                                     # resolved, re-cut and tested for NEW: none run again
    names = [c for c, _ in world["calls"]]
    assert (names.count("inseason"), names.count("recut"), names.count("follow-py"), names.count("follow-app")) == (1, 1, 1, 1)
    assert world["pushes"] == 0 and _latest(world) == NEW

def test_a_smoke_failure_reruns_only_the_smoke_and_the_chores_are_named(world, capsys):
    world["fail"] = {"smoke"}
    assert _staging() == 1
    io = capsys.readouterr()
    assert "STOPPED at staging" in io.err and "only the smoke runs again" in io.err
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["uploaded_at"] and "staged_at" not in rec
    world["fail"] = set()
    world["calls"].clear()
    assert _staging() == 0
    assert [c for c, _ in world["calls"]] == ["build", "smoke"]                  # no second upload
    out = capsys.readouterr().out
    assert "only the smoke runs" in out and f"after the release: chores for {NEW}" in out
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["smoked_at"] == rec["staged_at"]


def test_replacing_a_release_handed_for_prod_is_said(world, capsys):
    R.save_ledger({"releases": {LIVE: {"staged_at": "2026-11-09T17:57:16", "commit": "x"}}})
    assert _staging() == 0
    assert f"this replaces {LIVE} on staging" in capsys.readouterr().out
    assert json.loads(R.LEDGER.read_text())["releases"][LIVE]["replaced_by"] == NEW


def test_after_chores_name_the_exact_commands():
    text = R.after_chores("r-x")
    assert "goldens:" in text and "CONFIRM_PROD=r-x python -m pipeline release --prod r-x" in text
    assert "deploy/staging/site-all.sh" in text and "deploy/staging/feeds.sh" in text


# ----------------------------------------------------------------------------------------------- the pieces it uses

def test_the_build_holds_latest_when_asked(monkeypatch, tmp_path):
    atlas = tmp_path / "atlas" / "full"                       # the build dir is only named in the commands
    monkeypatch.setattr(B, "HOLD_LATEST", True)
    assert B.commands(atlas)["deliver"][-1] == "--hold-latest"
    monkeypatch.setattr(B, "HOLD_LATEST", False)
    assert "--hold-latest" not in B.commands(atlas)["deliver"]
    assert R.build_cmd()[:5] == ["bash", "scripts/heavy.sh", "release", str(R.HEAVY_MIN_FREE), "--"]
    assert "--hold-latest" in R.build_cmd()


def test_prune_keeps_the_candidate(tmp_path):
    from pipeline.deliver.release import prune
    from pipeline.deliver.release.manifest import write_candidate, write_latest
    for i, rid in enumerate(("r-a", "r-b", "r-c", "r-d")):
        (tmp_path / rid).mkdir()
        (tmp_path / rid / "manifest.json").write_text("{}")
    write_latest(tmp_path, "r-d")
    write_candidate(tmp_path, "r-a")
    kept, _gone = prune.plan(tmp_path)
    assert "r-a" in {d.name for d in kept} and (tmp_path / "LATEST").read_text().strip() == "r-d"


def test_the_app_gate_is_the_uploads_rule():
    from pipeline.deliver.release.app_gate import problems
    m = {"files": {"salmon": {"format": "salmon/4"}, "paint": {"format": "paint/5"}}}
    assert problems(m, "0.5.0", APPS) == []
    assert problems(m, "0.4.0", APPS) == ["salmon/4 needs app >= 0.5.0 but the oldest app allowed is 0.4.0; "
                                          "rerun with MIN_APP_VERSION=0.5.0 (or newer)"]
    assert len(problems(m, None, APPS)) == 2                  # no stamp, no MIN_APP_VERSION: refused
    assert "app_gate" in (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()


# ------------------------------------------------------------------------------- the staging site's version (T483)

def _site(index: str, bundle: str):
    def fetch(url, timeout=30):
        if url.endswith("/"):
            return index
        if "/_expo/static/js/web/index-" in url:
            return bundle
        raise AssertionError(url)
    return fetch


IDX = '<script src="/_expo/static/js/web/index-5524ce5c42e7529fe22800c74906f952.js" defer></script>'
JS = 'x;const b=(0,n.resolveAppVersion)({version:"0.6.9",build:"19",sha:"13b31d3f1ddb",date:"2026-11-08"});y'


def test_the_site_gate_reads_the_live_bundles_version_stamp():
    from pipeline.deliver.release import site_gate as SG
    v, how = SG.site_version("https://staging.example", _site(IDX, JS))
    assert v == "0.6.9" and "build 19" in how
    assert SG.site_version("https://s", _site("<html></html>", JS))[0] is None
    assert SG.site_version("https://s", _site(IDX, "no stamp here"))[0] is None

    def down(url, timeout=30):
        raise OSError("offline")
    v, how = SG.site_version("https://s", down)
    assert v is None and "offline" in how


def test_the_site_gate_refuses_a_site_older_than_the_release_and_names_the_redeploy(monkeypatch):
    from pipeline.deliver.release import site_gate as SG
    monkeypatch.delenv("SITE_CHECK_SKIP", raising=False)
    m = {"data_release": "r-x", "minAppVersion": "0.7.0", "files": {"salmon": {"format": "salmon/4"}}}
    bad, _ = SG.check(m, None, site="https://s", fetch=_site(IDX, JS), rid="r-x")
    assert len(bad) == 1 and "0.6.9, older than 0.7.0" in bad[0]
    assert "deploy/staging/site-all.sh" in bad[0]       # (T544) one command, not three typed from memory
    assert SG.check(m, None, site="https://s", fetch=_site(IDX, JS.replace("0.6.9", "0.7.0")))[0] == []
    assert SG.check(m, "0.6.0", site="https://s", fetch=_site(IDX, JS))[0] == []    # MIN_APP_VERSION wins
    # unstamped: the formats' own needs still hold (salmon/4 needs 0.5.0)
    assert SG.required({"files": {"salmon": {"format": "salmon/4"}}}, None) == "0.5.0"
    assert SG.check({"files": {}}, None, site="https://s", fetch=_site(IDX, JS.replace("0.6.9", "0.4.0")))[0] == []
    # an unreadable site is a refusal, never a pass; SITE_CHECK_SKIP says why it is skipped
    assert SG.check(m, None, site="https://s", fetch=_site("", ""))[0]
    monkeypatch.setenv("SITE_CHECK_SKIP", "site down for the T999 move")
    bad, said = SG.check(m, None, site="https://s", fetch=_site("", ""))
    assert bad == [] and "T999" in said


def test_the_upload_runs_the_site_gate_before_anything_is_sent():
    up = (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
    assert up.index("pipeline.deliver.release.site_gate") < up.index("rclone copy")


def test_heavy_runs_a_scripts_precheck_before_it_takes_the_lock(tmp_path):
    """caught (2026-11-09): `session.sh --build` with MLN_LOCAL_MAVEN and no TESTBED_APK_TAG failed only after waiting
    for the heavy lock; heavy.sh now runs a `heavy-precheck: yes` script's checks first, unlocked."""
    import os
    import subprocess
    lock = tmp_path / "heavy-job.lock"   # T536: only the two lane names exist; HEAVY_LANES_DIR keeps it off ~/.claude
    # (T534/T661) session.sh refuses an MLN_LOCAL_MAVEN that does not hold exactly one AAR: a real check (the precheck
    # finds it by name), so the experiment repo here holds one stand-in AAR
    aar = tmp_path / "m2/org/maplibre/gl/android-sdk-vulkan/0-test/android-sdk-vulkan-0-test.aar"
    aar.parent.mkdir(parents=True)
    aar.write_bytes(b"")
    env = {k: v for k, v in os.environ.items() if k not in ("TESTBED_APK_TAG", "HEAVY_HELD", "HEAVY_PRECHECK")}
    env.update(HEAVY_LOCK=str(lock), HEAVY_LANES_DIR=str(tmp_path), HEAVY_PAUSE=str(tmp_path / "no-pause"),
               MLN_LOCAL_MAVEN=str(tmp_path / "m2"))
    r = subprocess.run(["bash", "scripts/heavy.sh", "testbed", "30", "--", "bash", "app/tools/testbed/session.sh",
                        "--build", "--", "true"], cwd=R.REPO_ROOT, env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "needs TESTBED_APK_TAG" in r.stdout and "the lock was not taken" in r.stderr
    assert not lock.exists()
    ok = subprocess.run(["bash", "app/tools/testbed/session.sh", "--build", "--", "false"], cwd=R.REPO_ROOT,
                        env={**env, "HEAVY_PRECHECK": "1", "TESTBED_APK_TAG": "t"}, capture_output=True, text=True,
                        timeout=60)
    assert ok.returncode == 0 and "precheck ok" in ok.stdout


def test_t786_a_wrong_save_stops_the_preflight_naming_the_place(world, capsys):
    """T786 (owner 2026-11-17: "do not ship \"wrong\" places"): the preflight stops on the wrong-saves gate, with the
    place, its mark line, its verdict and "fix and re-save it"; --no-fast-fail does not skip it."""
    world["wrong_saves"] = (False, "wrong saves: 1 place(s) …:\n  dfo 2:some-river:x: mark line 41, verdict wrong, "
                                   "no extent fix (…) — fix and re-save it")
    assert _staging(no_fast_fail="docs only") == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "dfo 2:some-river:x: mark line 41, verdict wrong" in err
    assert "fix and re-save" in err and "WRONG_SAVE_OK" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_t786_mutation_without_the_gate_in_the_table_the_wrong_save_ships(world, monkeypatch, capsys):
    """The mutation: with `check_wrong_saves` out of PREFLIGHT the same wrong save goes through the whole release —
    the gate is the only thing that stops it (no other check reads the verdicts)."""
    world["wrong_saves"] = (False, "wrong saves: 1 place(s) …:\n  dfo 2:some-river:x: mark line 41, verdict wrong")
    monkeypatch.setattr(R, "PREFLIGHT", tuple((n, c) for n, c in R.PREFLIGHT if n != "wrong-saves"))
    assert _staging() == 0
    assert "STOPPED" not in capsys.readouterr().err and _latest(world) != LIVE


def test_t841_a_classified_water_missing_from_the_corpus_stops_the_preflight(world, capsys):
    """T841 (owner 2026-11-17: "make sure we capture all classified waters"): the preflight stops on the corpus half of
    the classified-waters gate, naming the row and its page; --no-fast-fail does not skip it."""
    world["classified"] = (False, "classified waters (corpus): 1 problem(s) — …:\n  p.53 SKEENA RIVER (mainstem only) "
                                  "(Region 6): the book prints Class II, Class II; r6:skeena@6-10 carries Class II")
    assert _staging(no_fast_fail="docs only") == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "p.53 SKEENA RIVER (mainstem only)" in err and "T841" in err
    assert world["calls"] == [] and _latest(world) == LIVE


def test_t841_a_candidate_shipping_a_classified_water_without_its_licence_stops_at_checks(world, capsys):
    """The bundle half runs on the candidate after the build: a water that ships without the Classified Waters Licence
    stops the release before anything is promoted."""
    world["classified_problems"] = ("classified waters (bundle): 1 problem(s) — …:\n  r6:telkwa_river@6-9#telkwa_river "
                                    "(p.54 TELKWA RIVER): 3 of its 41 section(s) ship WITHOUT the Classified Waters Licence")
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at checks" in err and "TELKWA RIVER" in err and "Classified Waters Licence" in err
    assert _latest(world) == LIVE


def test_t841_mutation_without_the_gate_in_the_table_the_gap_ships(world, monkeypatch, capsys):
    """The mutation: with `check_classified` out of PREFLIGHT the same corpus gap goes through the whole release."""
    world["classified"] = (False, "classified waters (corpus): 1 problem(s)")
    monkeypatch.setattr(R, "PREFLIGHT", tuple((n, c) for n, c in R.PREFLIGHT if n != "classified"))
    assert _staging() == 0
    assert "STOPPED" not in capsys.readouterr().err and _latest(world) != LIVE


# ---------------------------------------------------------------- (T993 lane G2) the minimum app, derived before the build

#: APPS with a newer app that reads a new salmon format (salmon/5): the code that writes it needs app 0.6.0
APPS_060 = {**APPS, "0.6.0": {"water": ["water/1"], "paint": ["paint/5", "paint/4"], "salmon": ["salmon/5", "salmon/4"]}}


def test_the_min_app_is_derived_from_the_codes_formats_and_its_rise_is_said(world, capsys, monkeypatch):
    """T993 item 2 (0.8.5 stop 3: `--min-app-version 0.8.4` was not passed though the formats always needed it): with no
    flag, the release goes up stamped for what the CODE's formats need — said in preflight, with the recorded apps the
    rise leaves behind. Mutation pin: the old default (the live stamp alone) uploads 0.5.0 and the app gate stops."""
    from pipeline.deliver.release import app_gate as AG
    monkeypatch.setattr(AG, "table", lambda: APPS_060)
    world["code_files"] = {"water": {"format": "water/1"}, "salmon": {"format": "salmon/5"}}
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/5"}}
    assert _staging() == 0
    out = capsys.readouterr().out
    assert _env_of(world, "upload")["MIN_APP_VERSION"] == "0.6.0"
    assert "ok: min app 0.6.0 (derived: the live stamp raised to the code's formats; the code's formats need 0.6.0" in out
    assert "RAISED from 0.5.0 to 0.6.0: recorded app(s) 0.5.0 stop applying new releases" in out
    assert world["site_min_apps"][0] == "0.6.0" and world["site_manifests"][0] == {"files": world["code_files"]}


def test_the_old_default_would_have_stopped_at_the_app_gate(world, capsys, monkeypatch):
    """The mutation of the test above: `default_min_app` as it was (the live stamp only) stops the checks."""
    from pipeline.deliver.release import app_gate as AG
    monkeypatch.setattr(AG, "table", lambda: APPS_060)
    monkeypatch.setattr(R, "default_min_app", R.live_stamp)
    world["code_files"] = {"salmon": {"format": "salmon/5"}}
    world["extra"] = {"salmon": {"url": "x", "format": "salmon/5"}}
    assert _staging() == 1 and "salmon/5 needs app >= 0.6.0" in capsys.readouterr().err


def test_a_min_app_flag_below_the_codes_formats_stops_before_the_build(world, capsys, monkeypatch):
    from pipeline.deliver.release import app_gate as AG
    monkeypatch.setattr(AG, "table", lambda: APPS_060)
    world["code_files"] = {"salmon": {"format": "salmon/5"}}
    assert _staging(min_app_version="0.5.0") == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "--min-app-version 0.5.0 is older than the code's formats need: 0.6.0" in err
    assert world["calls"] == []                                   # nothing built


def test_a_format_no_recorded_app_reads_stops_before_the_build(world, capsys):
    world["code_files"] = {"water": {"format": "water/9"}}
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "the code writes 1 format(s) no recorded app reads: water/9" in err and world["calls"] == []


def test_the_site_gate_reads_the_candidates_formats_once_it_is_built(world, capsys):
    world["differs"] = True
    assert _staging() == 1                                        # built, stopped at the binding diff (CANDIDATE)
    world["site_manifests"].clear()
    assert _staging() == 1
    assert world["site_manifests"][0]["data_release"] == NEW      # the candidate's manifest, not the code's


def test_the_pieces_gate_is_asked_before_the_build_and_on_the_candidate_before_the_promote(world, capsys):
    """T788 #2: the gate was asked only inside the upload, after the promote."""
    world["pieces"] = ["the release binds 2 closed-area piece(s); the feeds worker does not emit them"]
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at preflight" in err and "deploy/staging/feeds.sh" in err and world["calls"] == []
    world["pieces"] = []
    assert _staging() == 0 and world["pieces_asked"][:3] == [LIVE, LIVE, NEW]   # preflight on LATEST, checks on NEW


def test_the_rehearsal_says_what_it_cannot_see_before_a_build_and_offers_the_build(world, capsys):
    """T993 item 4: a pre-build rehearsal promised "every stop the real run would hit" and saw preflight only."""
    world["rc"]["build"] = 0
    assert R.main(["--staging", "--dry-run"]) == 0                # the build's dry run makes no candidate here
    out = capsys.readouterr().out
    assert "NOT SEEN before a build: the size gates on the new files" in out
    assert "python -m pipeline release --staging --dry-run --build --rehearse-follows" in out
    assert "1. bash scripts/heavy.sh release 50 -- " in out and "--hold-latest" in out
    # (T993 item 9) LATEST's headroom, tightest first, WARN under 5 %
    assert f"size headroom on {LIVE}" in out and "3.5%  WARN" in out and "1 under 5%" in out


def test_dry_run_build_builds_the_candidate_and_rehearses_on_it(world, capsys):
    assert R.main(["--staging", "--dry-run", "--build"]) == 0
    out = capsys.readouterr().out
    builds = [(c, e) for c, e in world["calls"] if c == "build"]
    assert len(builds) == 1 and builds[0][1] is None   # the real build (T661: heavy.sh is handed no fast-fail word)
    assert f"manifest sanity on {NEW}: ok" in out and "NOT SEEN before a build" not in out
    assert _latest(world) == LIVE and (R.HOME / "baseline" / LIVE / "report.json").is_file()
    with pytest.raises(SystemExit):
        R.main(["--staging", "--build"])                           # a rehearsal flag only


# ------------------------------------------------------------------- (T993 item 8, T788 #8) what is safe passes, said

def test_a_diff_every_change_of_which_a_committed_row_explains_passes_with_a_note(world, capsys):
    """0.8.5 stop 7: Lost Lagoon +1, the T981 entry added on purpose. Explained by the rows changed since LATEST's
    commit, it passes — printed, and the ledger keeps what explained it."""
    R.save_ledger({"releases": {LIVE: {"commit": "base0"}}})
    world["commits"], world["differs"] = {"base0"}, True
    world["attributed"] = (True, ["rule r2:lost_lagoon_local@2-8::lost_lagoon_local.r1: entry r2:lost_lagoon_local@2-8 "
                                  "changed in data/curated/regulations/local_closures/region-2.json (7ce79dcf0)"], [])
    assert _staging() == 0 and world["attribute_asked"] == ["base0"]
    out = capsys.readouterr().out
    assert "binding diff AUTO-ACCEPTED (T993 item 8)" in out and "entry r2:lost_lagoon_local@2-8 changed" in out
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["binding_diff"]["status"] == "accepted" and rec["binding_diff"]["auto"] == "attributable"
    assert rec["checks"]["binding_diff"] == "accepted"


def test_mutation_a_change_nothing_explains_still_stops(world, capsys):
    R.save_ledger({"releases": {LIVE: {"commit": "base0"}}})
    world["commits"], world["differs"] = {"base0"}, True
    world["attributed"] = (False, ["rule a::a.r1: entry a changed"], ["rule b::b.r1: no curated row changed since base0"])
    assert _staging() == 1
    got = capsys.readouterr()
    assert "STOPPED at checks" in got.err and "--accept-diff" in got.err
    assert "not explained by a curated row changed since LATEST's commit" in got.out and "rule b::b.r1" in got.out
    assert _latest(world) == LIVE


def test_an_accepted_diff_keeps_its_record_on_a_rerun_after_the_promote(world, capsys):
    """T788 #8: r-741815c6090d's accept (Lost Lagoon +1) read "same reach run" after a rerun past the promote."""
    world["differs"] = True
    assert _staging() == 1
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    world["fail"] = {"upload"}
    assert _staging(accept_diff=js) == 1 and _latest(world) == NEW          # promoted, the upload failed
    world["fail"] = set()
    assert _staging() == 0                                                  # the rerun after the promote
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["binding_diff"]["status"] == "accepted" and rec["binding_diff"]["json"] == str(js)
    assert rec["checks"]["binding_diff"] == "accepted"
    assert "binding diff: as checked before the promote (accepted" in capsys.readouterr().out


def test_mutation_without_the_keep_the_rerun_overwrites_the_accept(world, capsys, monkeypatch):
    """The mutation of T788 #8's fix: the key alone decides, and the post-promote rerun writes "same"."""
    world["differs"] = True
    assert _staging() == 1
    js = R.HOME / "checks" / NEW / "binding_diff.json"
    world["fail"] = {"upload"}
    assert _staging(accept_diff=js) == 1
    led = json.loads(R.LEDGER.read_text())
    led["releases"][NEW]["binding_diff"].pop("status")                     # what the fix keeps on, taken away
    R.save_ledger(led)
    world["fail"] = set()
    assert _staging() == 0
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["binding_diff"]["status"] == "same"


def test_golden_pages_that_only_renumber_their_sources_are_committed_with_a_note(world, capsys):
    """0.8.5 stop 8: nine pages, all renumbered source indexes, stopped for --accept-fixtures."""
    world["recut_changed"] = ["app/fixtures/golden/taps.json", "app/packages/ui-native/src/regs/fixtures/glenn_501.json"]
    world["renumbered"] = (True, "1 golden page(s) differ only in source indexes renumbered one for one (2 index(es): "
                                 "1363->1364, 3144->3145)")
    assert R.main(["--staging", "--push"]) == 0
    out = capsys.readouterr().out
    assert "AUTO-ACCEPTED as a renumber (T993 item 8)" in out and "1363->1364" in out
    assert world["recuts_committed"] == [(NEW, "renumber only (source indexes)")]
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]
    assert rec["recut"]["kind"] == "renumber only (source indexes)" and "1363->1364" in rec["renumbered"]
    # the mutation (the same re-cut, not a renumber, stops) is test_a_changed_golden_page_stops_uncommitted_…


# ------------------------------------------------- (T993 items 15, 16; T788 #10) each check once per release, no more

def test_fast_fail_sh_leaves_the_app_gates_and_the_worker_suite_to_the_follows_when_told():
    from pipeline.common.curated import REPO_ROOT
    sh = (REPO_ROOT / "scripts" / "fast-fail.sh").read_text(encoding="utf-8")
    assert f'${{{R.FOLLOWS_NEXT}:-}} == 1' in sh
    for fn in ("feeds_worker_tests", "app_gates"):
        body = sh[sh.index(f"{fn}() {{"):]
        assert body[:body.index("\n")].count("follows_next && return 0") == 1, fn
    assert 'step "app gates (pnpm check:fast: every pnpm check gate but vitest)" app_gates' in sh


def test_a_bare_rehearsal_keeps_the_app_gates_in_fast_fail_and_a_narrower_pass_does_not_cover_it(world):
    """The real run leaves them to the follows stage; a rehearsal with no follows run keeps them (so it still lists
    their failures before any build), and a pass that left them out is not taken for one that ran them."""
    world["fail"] = {"upload"}
    assert _staging() == 1 and world["fast_fail_env"].get(R.FOLLOWS_NEXT) == "1"
    assert json.loads(R.LEDGER.read_text())["fast_fail"]["scope"] == "follows-next"
    n = world["fast_fails"]
    assert R.main(["--staging", "--dry-run"]) == 0
    assert world["fast_fails"] == n + 1 and R.FOLLOWS_NEXT not in world["fast_fail_env"]   # rerun, and in full
    assert R.main(["--staging", "--dry-run"]) == 0 and world["fast_fails"] == n + 1         # a full pass covers it


def test_the_rehearsal_on_a_candidate_with_rehearse_follows_leaves_them_to_it(world):
    world["differs"] = True
    assert _staging() == 1                                                  # a candidate, stopped at the diff
    world["tree"] = "t2"
    assert R.main(["--staging", "--dry-run", "--rehearse-follows"]) == 0
    assert world["fast_fail_env"].get(R.FOLLOWS_NEXT) == "1"


def test_the_live_data_tier_runs_once_on_the_candidate_in_chunks(world):
    """T791 #4 / T993 item 16 (T931: ~55 stale pins in the data tier, which nothing ran): a follows row, run with the
    other checks on the candidate (RELEASE_DIR), the files found by rule deselected (they run whole)."""
    assert _staging() == 0
    tier = [e for c, e in world["calls"] if c == "data-tier"]
    assert len(tier) == 1 and tier[0]["RELEASE_DIR"] == str(world["root"] / NEW)
    cmd = R.data_tier_cmd()
    assert "scripts/pytest-chunked.sh" in cmd and "--deselect=pipeline/tests/test_x.py" in cmd
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["follows"]["tests"]["data_tier"] is True
    assert "data-tier" in [r.name for r in R.RF.FOLLOWS]


def test_the_data_tier_selects_every_needs_marker_and_never_slow_or_no_data():
    from _pytest.mark.expression import Expression
    from pipeline.tests.tiers import data_markers
    expr = Expression.compile(R.RF.data_tier_markers())
    for m in data_markers():
        if m != "slow":
            assert expr.evaluate(lambda name, /, m=m, **kw: name == m), m
    assert not expr.evaluate(lambda name, /, **kw: False)                                # a no-data test: CI's
    assert not expr.evaluate(lambda name, /, **kw: name in ("slow", "needs_bundle"))     # slow stays out


def test_a_rerun_does_not_rebuild_what_this_release_built(world, capsys):
    """T788 #10: r-741815c6090d's own in-season step reran deliver for 9 minutes for the same release id. A candidate
    this release built, with nothing a build reads committed since, is not rebuilt; a code commit rebuilds it."""
    world["commits"] = {"abc123"}
    world["recut_changed"] = ["app/packages/ui-native/src/regs/fixtures/glenn_501.json"]
    assert _staging() == 1 and [c for c, _ in world["calls"]].count("build") == 1     # stopped at the golden page
    world["committed_since"] = {"app/fixtures/golden/taps.json", "docs/STATUS.md"}
    capsys.readouterr()
    assert _staging(accept_fixtures="tok-1") == 1 and [c for c, _ in world["calls"]].count("build") == 1
    assert "nothing a build reads was committed since — not rebuilt (T788 #10" in capsys.readouterr().out
    world["committed_since"] |= {"pipeline/deliver/x.py"}                            # the mutation: a build input
    world["pushed"], world["unpushed"] = True, []
    assert _staging() == 0 and [c for c, _ in world["calls"]].count("build") == 2


# ------------------------------------------------------------------------------- release branches and tags (T995)

def test_preflight_wants_the_release_branch_of_app_jsons_version(world, capsys):
    """(T995) the release ships from release/<app.json's version>; no such branch on origin says how to cut it."""
    world["pushed"] = "no-branch"
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "there is no release/0.9.0 on origin: a release ships from its release branch, cut from main" in err
    assert "next: python -m pipeline release --cut 0.9.0, then build from it (git switch release/0.9.0)" in err
    assert world["calls"] == [] and _latest(world) == LIVE
    world["pushed"], world["version"] = False, "1.4.2"                    # the branch follows app.json
    assert _staging() == 1 and "not on origin/release/1.4.2" in capsys.readouterr().err


def test_preflight_wants_head_to_be_the_branch_tip_and_never_says_push_from_another_branch(world, capsys):
    """(T995 review) an ancestor of the tip (a pre-cut main commit, a stale checkout) passed: HEAD must BE the tip.
    The hint never pushes another branch's HEAD onto the release branch."""
    for state in ("behind", "diverged"):
        world["pushed"] = state
        assert _staging() == 1
        err = capsys.readouterr().err
        assert f"HEAD abc123 is not the tip of origin/release/0.9.0 ({state}:" in err
        assert "next: git switch release/0.9.0 && git pull --ff-only" in err and "HEAD:release" not in err
    world["pushed"], world["branch"] = False, "main"                         # ahead, but on main
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "next: git switch release/0.9.0 && git pull --ff-only (HEAD is on main" in err and "git push" not in err
    world["branch"] = "release/0.9.0"                                        # ahead on the branch itself: push it
    assert _staging() == 1 and "next: git push origin release/0.9.0" in capsys.readouterr().err
    assert world["calls"] == []


def test_the_ship_tags_data_and_the_next_release_candidate_and_names_the_merge_back(world, capsys):
    """(T995, T1062) at ship: data/<r-id> and v<version>-rc.1 on the uploaded commit, pushed, recorded; the merge-back
    PR printed. A rerun at the same commit tags nothing; one at a new HEAD (a fix cherry-picked, the same r-id) tags
    rc.2 on it, never touching rc.1; a HEAD that is the newest rc apart from the build numbers (a Play build's
    versionCode commit) tags nothing new. No v0.9.0 is made at ship. Mutations: drop the tagging from stage 7, tag
    `newest` instead of `newest + 1`, or skip the build-number comparison: this fails."""
    assert _staging() == 0
    out = capsys.readouterr().out
    assert world["made_tags"] == [(f"data/{NEW}", "abc123"), ("v0.9.0-rc.1", "abc123")]
    assert world["pushed_tags"] == [[f"data/{NEW}", "v0.9.0-rc.1"]]
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["tags"] == {f"data/{NEW}": "abc123", "v0.9.0-rc.1": "abc123"} and rec["ship_commit"] == "abc123"
    assert f"tagged data/{NEW} at abc123, v0.9.0-rc.1 at abc123, pushed data/{NEW}, v0.9.0-rc.1" in out
    assert rec["uploaded_commit"] == "abc123" and "v0.9.0" not in world["remote_tags"]
    assert ('gh pr create --base main --head release/0.9.0 --title "chore(release): merge release/0.9.0 back into '
            'main"') in out
    world["made_tags"].clear()
    assert _staging() == 0 and world["made_tags"] == []                   # a rerun on staging tags nothing again
    assert "tagged nothing new, pushed nothing (on origin already); v0.9.0-rc.1 (abc123) is abc123 apart from the " \
           "build numbers: no new rc" in capsys.readouterr().out
    world["head"] = "f1x0000"                                               # a fix cherry-picked: the next rc on it
    assert _staging() == 0 and world["made_tags"] == [("v0.9.0-rc.2", "f1x0000")]
    assert world["remote_tags"] == {f"data/{NEW}": "abc123", "v0.9.0-rc.1": "abc123", "v0.9.0-rc.2": "f1x0000"}
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["ship_commit"] == "abc123"   # the data's commit stays
    world["made_tags"].clear()
    world["head"], world["same_tree"] = "c0de45", {("f1x0000", "c0de45")}  # only a versionCode commit since rc.2
    assert _staging() == 0 and world["made_tags"] == []
    assert "v0.9.0-rc.2 (f1x0000) is c0de45 apart from the build numbers: no new rc" in capsys.readouterr().out


def test_a_version_tagged_final_already_gets_no_release_candidate(world, capsys):
    """(T1062 transition) v0.9.0 was made by the old code at an earlier ship (no rc of it exists): the ship tags only
    data/<r-id>, makes no rc, says why; the old tag stays where it is. Production then takes the old rule (below).
    Mutation: drop the `final_at` branch and an rc is made for a released version: this fails."""
    world["remote_tags"]["v0.9.0"] = world["tag_at"]["v0.9.0"] = "0ld0000"
    assert _staging() == 0
    out = capsys.readouterr().out
    assert world["made_tags"] == [(f"data/{NEW}", "abc123")] and world["pushed_tags"] == [[f"data/{NEW}"]]
    assert ("no release candidate made: v0.9.0 is tagged final already at 0ld0000 (made before release candidates, "
            "T1062); a change after it ships as the next version") in out
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["tags"] == {f"data/{NEW}": "abc123"}
    assert world["remote_tags"]["v0.9.0"] == "0ld0000"


def test_a_data_tag_on_another_commit_stops_and_a_failed_tag_push_reruns_only_the_tags(world, capsys):
    world["remote_tags"][f"data/{NEW}"] = "e1sewhere"
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "STOPPED at staging" in err
    assert f"the tag data/{NEW} exists on origin already at e1sewhere, not at the uploaded commit abc123" in err
    assert world["made_tags"] == [] and "tags" not in json.loads(R.LEDGER.read_text())["releases"][NEW]
    del world["remote_tags"][f"data/{NEW}"]
    world["rc"]["push-tags"] = 1
    assert _staging() == 1 and "only the tags are left" in capsys.readouterr().err
    assert world["remote_tags"] == {}                                        # made here, never assumed pushed
    world["rc"]["push-tags"], world["head"] = 0, "n3whead"                   # (T995 review) HEAD moved since the upload
    world["calls"].clear()
    assert _staging() == 0 and [c for c, _ in world["calls"]] == ["build"]   # no second upload or smoke
    assert world["pushed_tags"][-1] == [f"data/{NEW}", "v0.9.0-rc.1"]       # the tags made before are pushed now
    assert world["remote_tags"] == {f"data/{NEW}": "abc123", "v0.9.0-rc.1": "abc123"}   # at the UPLOADED commit
    assert json.loads(R.LEDGER.read_text())["releases"][NEW]["ship_commit"] == "abc123"


def test_a_local_only_tag_elsewhere_stops_and_one_at_the_shipped_commit_is_pushed(world, capsys):
    """(T995 review) a local tag is never taken as pushed: at the uploaded commit it is pushed, elsewhere it stops."""
    world["tag_at"]["v0.9.0-rc.1"] = "st4le"
    assert _staging() == 1
    assert "a local-only tag v0.9.0-rc.1 points at st4le, not at abc123, the commit it tags" in capsys.readouterr().err
    world["tag_at"]["v0.9.0-rc.1"] = "abc123"
    assert _staging() == 0 and world["made_tags"] == [(f"data/{NEW}", "abc123")]
    assert world["remote_tags"] == {f"data/{NEW}": "abc123", "v0.9.0-rc.1": "abc123"}


def test_a_release_with_no_upload_commit_is_not_tagged_at_a_guess(world, capsys):
    assert _staging() == 0
    led = json.loads(R.LEDGER.read_text())
    for k in ("tags", "uploaded_commit"):
        led["releases"][NEW].pop(k)
    R.save_ledger(led)
    world["made_tags"].clear()
    assert _staging() == 1 and "has no recorded upload commit" in capsys.readouterr().err
    assert world["made_tags"] == []


def test_prod_checks_the_tags_point_at_the_shipped_commit(staged, monkeypatch, capsys):
    """(T995) production ships only what its tags name. Mutation: drop `tag_problems` from prod and the upload runs.
    (T1062 transition) with no rc of the version on origin (v0.9.0 made before release candidates), the old rule: the
    version tag at the ship commit or before it."""
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    del staged["remote_tags"]["v0.9.0-rc.1"]                               # a version tagged before T1062
    del staged["remote_tags"][f"data/{NEW}"]                                # only the local tag is left
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    err = capsys.readouterr().err
    assert f"no tag data/{NEW} on origin (only a local one: git push origin refs/tags/data/{NEW})" in err
    assert staged["calls"] == []
    del staged["tag_at"][f"data/{NEW}"]
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert f"no tag data/{NEW} on origin: git tag -a data/{NEW}" in capsys.readouterr().err
    staged["remote_tags"][f"data/{NEW}"] = "m0ved"
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert f"data/{NEW} points at m0ved, not at the commit {NEW} shipped from (abc123)" in capsys.readouterr().err
    staged["remote_tags"][f"data/{NEW}"], staged["remote_tags"]["v0.9.0"] = "abc123", "later99"
    assert R.prod(NEW, ask=lambda _: NEW) == 1 and "v0.9.0 points at later99" in capsys.readouterr().err
    staged["ancestors"] = {("later99", "abc123")}                          # a version tag before the ship commit
    assert R.prod(NEW, ask=lambda _: NEW) == 0 and staged["calls"] == [("upload", {"CONFIRM_PROD": "canifishthis.ca"})]
    assert R.main(["--prod", NEW, "--dry-run"]) == 0 and "tags (T995): ok" in capsys.readouterr().out


def test_prod_refuses_a_release_never_tagged_at_ship(staged, monkeypatch, capsys):
    led = json.loads(R.LEDGER.read_text())
    led["releases"][NEW].pop("ship_commit")
    R.save_ledger(led)
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    assert R.prod(NEW, ask=lambda _: NEW) == 1 and "was never tagged at ship" in capsys.readouterr().err
    assert staged["calls"] == []


@pytest.fixture
def cutting(monkeypatch):
    w = {"cuts": [], "remote": set(), "fetched": 0, "commits": [],
         "decision": RV.decide("0.8.5", "the cutover b5dd8ade3", [_commit("feat(T985): a new layer")])}
    monkeypatch.setattr(R, "fetch_trunk", lambda: w.__setitem__("fetched", w["fetched"] + 1))
    monkeypatch.setattr(R, "remote_ref_exists", lambda ref: ref in w["remote"])
    monkeypatch.setattr(R, "trunk_decision", lambda: w["decision"])
    monkeypatch.setattr(R, "version_commit", lambda v, d, reads_moved=None: w["commits"].append(v) or "ab12cd34ef5600000")
    w["rows"] = []                                   # (T1048) the app_reads row: its own real-git tests below
    monkeypatch.setattr(R, "app_reads_row", lambda v, base=None, reads_moved=None: w["rows"].append(v) or (
        "{}", f"app {v}: the same formats as 0.8.5", {"reads": {"water": ["water/3"]}}))
    monkeypatch.setattr(R, "cut_branch", lambda b, commit=None: w["cuts"].append((b, commit)) or 0)
    monkeypatch.setattr(R, "is_tty", lambda: True)
    monkeypatch.setattr(R, "today", lambda: "2026-11-22")
    return w


def test_cut_computes_the_version_prints_its_commits_and_asks_the_owner(cutting, capsys):
    """(T998) a bare --cut: the computed version and the commits that decided it, then the owner types it; the branch
    is made at the version commit (app.json, package.json, the changelog entry), and pushed."""
    asked = []
    assert R.cut(None, ask=lambda q: asked.append(q) or "0.9.0") == 0
    out = capsys.readouterr().out
    assert asked == ["type 0.9.0 to cut release/0.9.0 with it: "]
    assert "0.9.0: 0.8.5 + minor, from 1 app commit(s) since the cutover b5dd8ade3" in out
    assert "c0ffee000000 feat(T985): a new layer" in out
    assert cutting["fetched"] == 1 and cutting["commits"] == ["0.9.0"]
    assert cutting["cuts"] == [("release/0.9.0", "ab12cd34ef5600000")]
    assert "release: release/0.9.0 cut from origin/main and pushed. Next: git switch release/0.9.0" in out
    assert "its first commit (ab12cd34ef56) sets expo.version 0.9.0 (app.json + package.json), opens the anglers' " \
           "entry in app/packages/core/src/changelog.ts (EMPTY" in out
    assert R.cut(None, ask=lambda q: "0.9.1") == 2 and "did not match; nothing was made" in capsys.readouterr().err
    assert len(cutting["cuts"]) == 1


def test_a_hand_set_version_that_disagrees_is_refused_naming_both(cutting, capsys):
    """(T998) Mutation: drop the comparison (`RV.accepts`) and 0.9.1 / 0.8.6 are cut: this fails."""
    for typed in ("0.9.1", "0.8.6", "1.0.1"):
        assert R.main(["--cut", typed]) == 1
        err = capsys.readouterr().err
        assert f"release --cut: refused: {typed} is set by hand, but the commits compute 0.9.0 (0.8.5 + minor)" in err
    assert cutting["cuts"] == [] and cutting["commits"] == []
    assert R.main(["--cut", "0.9.0"]) == 0 and cutting["cuts"] == [("release/0.9.0", "ab12cd34ef5600000")]
    assert "release --cut: 0.9.0 is the version the commits compute" in capsys.readouterr().out
    assert R.main(["--cut", "1.0.0"]) == 0                                   # the owner's launch version (T998)
    assert "1.0.0: the first open Play / App Store release, the owner's call" in capsys.readouterr().out


def test_cut_says_when_there_is_no_app_release_and_never_cuts_without_the_owner(cutting, capsys, monkeypatch):
    cutting["decision"] = RV.decide("0.8.5", "the cutover b5dd8ade3", [_commit("docs(T1): a"), _commit("ci(T2): b")])
    assert R.main(["--cut"]) == 1
    err = capsys.readouterr().err
    assert "release --cut: no app release: nothing since the cutover b5dd8ade3 bumps the app" in err
    assert "python -m pipeline release --cut --data-only" in err and cutting["cuts"] == []
    cutting["decision"] = RV.decide("0.8.5", "x", [_commit("fix(T1): a")])
    monkeypatch.setattr(R, "is_tty", lambda: False)                         # no terminal: say the command, cut nothing
    assert R.main(["--cut"]) == 2 and "run: python -m pipeline release --cut 0.8.6" in capsys.readouterr().err
    assert R.main(["--cut", "--dry-run"]) == 0
    assert "would cut release/0.8.6 once you confirm it; nothing was made" in capsys.readouterr().out
    cutting["decision"] = RV.decide("0.8.5", "x", [_commit("T994 fix x")])
    assert R.main(["--cut", "0.8.6"]) == 1 and "not in the format" in capsys.readouterr().out
    assert cutting["cuts"] == []


def test_cut_refuses_an_existing_branch_a_shipped_version_and_a_bad_version(cutting, capsys):
    cutting["remote"] = {"refs/heads/release/0.9.0"}
    assert R.main(["--cut", "0.9.0"]) == 1 and "release/0.9.0 exists on origin already" in capsys.readouterr().err
    cutting["remote"] = {"refs/tags/v0.9.0"}
    assert R.main(["--cut", "0.9.0"]) == 1 and "v0.9.0 is tagged already" in capsys.readouterr().err
    assert R.main(["--cut", "0.9"]) == 2 and "is not x.y.z" in capsys.readouterr().err
    assert cutting["cuts"] == [] and cutting["fetched"] == 2


def test_a_data_only_cut_makes_the_days_branch_at_main_and_refuses_app_changes(cutting, capsys):
    """(T998 item 6) release/data-<date> from origin/main itself (no version commit), only when no app commit bumps."""
    assert R.main(["--cut", "--data-only"]) == 1
    assert "refused: app commits since the cutover b5dd8ade3 bump the app to 0.9.0" in capsys.readouterr().err
    cutting["decision"] = RV.decide("0.8.5", "the cutover b5dd8ade3", [
        _commit("fix(T931): a pipeline fix", paths=("pipeline/regs/x.py",)), _commit("docs(T1): notes")])
    assert R.main(["--cut", "--data-only"]) == 0
    out = capsys.readouterr().out
    assert cutting["cuts"] == [("release/data-2026-11-22", None)] and cutting["commits"] == []
    assert cutting["rows"] == []                       # (T1048 review) app.json stays: no app_reads row is computed
    assert "a data-only release: app.json stays 0.8.5; the ship tags only data/<r-id>" in out
    cutting["remote"] = {"refs/heads/release/data-2026-11-22"}
    assert R.main(["--cut", "--data-only"]) == 1 and "exists on origin already" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        R.main(["--cut", "0.9.0", "--data-only"])


@pytest.fixture
def cut_repo(tmp_path, monkeypatch):
    """(T998, T1048) a real git repo holding the files the version commit reads, its HEAD as origin/main, and REPO_ROOT
    pointed at it: (run(git args) -> stdout, the repo, the app_reads table it holds). The table's newest row is what
    this tree's app reads (whatever version the checkout carries), so a cut of it moves nothing."""
    import subprocess
    from pipeline.tools.emit import app_reads as AR
    env = {k: v for k, v in __import__("os").environ.items() if not k.startswith("GIT_")}
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main", "-c",
           "commit.gpgsign=false"]
    here = tmp_path / "here"
    subprocess.run([*git, "init", "-q", str(here)], check=True, env=env)
    run = lambda *a: subprocess.run([*git, *a], cwd=here, check=True, capture_output=True, text=True,   # noqa: E731
                                    env=env).stdout.strip()
    real = Path(R.REPO_ROOT)
    for rel in (R.APP_JSON_REL, R.APP_PKG_REL, R.CHANGELOG_TS, AR.PACK_TS, AR.MANIFEST_TS, AR.PAINT_TS):
        (here / rel).parent.mkdir(parents=True, exist_ok=True)
        (here / rel).write_text((real / rel).read_text())
    table = AR.load()
    newest = max(table["apps"], key=AR.ver)
    table["apps"][newest] = {"reads": AR.reads_in(lambda rel: (here / rel).read_text())}
    (here / AR.TABLE_REL).parent.mkdir(parents=True, exist_ok=True)
    (here / AR.TABLE_REL).write_text(AR.dump(table))
    # the code under test runs its own git (commit-tree) without the -c flags above: the fixture repo carries the
    # identity itself, as a CI runner has none of its own (no global user.name / user.email)
    for key, value in (("user.email", "t@t"), ("user.name", "t"), ("commit.gpgsign", "false")):
        run("config", key, value)
    run("add", ".")
    run("commit", "-qm", "base", "--no-verify")
    run("update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(R, "REPO_ROOT", str(here))
    for k in [k for k in __import__("os").environ if k.startswith("GIT_")]:
        monkeypatch.delenv(k)
    return run, here, table


def test_the_version_commit_sets_the_version_opens_the_entry_and_writes_the_changelog(cut_repo):
    """(T998) on real git, with no checkout: one commit on origin/main changing exactly the five files; the version
    strings and nothing else; an empty draft entry first; CHANGELOG.md grouped by type; (T1048) the new version's
    app_reads row. Nothing else moves."""
    from pipeline.tools.emit import app_reads as AR
    run, here, table = cut_repo
    head = run("rev-parse", "HEAD")
    old = R.CP.app_version(here / R.APP_JSON_REL)
    d = RV.decide(old, "the cutover b5dd8ade3", [_commit("feat(T985): a layer"), _commit("fix(T1): a bug")])
    sha = R.version_commit("9.9.9", d, day="2026-11-22")
    assert run("rev-parse", "HEAD") == head and run("status", "--porcelain") == ""          # nothing else moved
    assert run("rev-parse", f"{sha}^") == head
    assert sorted(run("diff", "--name-only", head, sha).split()) == sorted(
        [R.APP_JSON_REL, R.APP_PKG_REL, R.CHANGELOG_TS, R.CHANGELOG_MD, AR.TABLE_REL])
    diff = run("diff", "-U0", head, sha, "--", R.APP_JSON_REL, R.APP_PKG_REL)
    assert [ln for ln in diff.splitlines() if ln[:1] in "+-" and ln[:3] not in ("+++", "---")] == [
        f'-    "version": "{old}",', '+    "version": "9.9.9",', f'-  "version": "{old}",', '+  "version": "9.9.9",']
    ts = run("show", f"{sha}:{R.CHANGELOG_TS}")
    entry = ts[ts.index(R.CHANGELOG_OPEN) + len(R.CHANGELOG_OPEN):].split("  },", 1)[0]
    assert 'version: "9.9.9", date: "2026-11-22", draft: true, notes: [\n    ],' in entry
    md = run("show", f"{sha}:{R.CHANGELOG_MD}")
    assert md.startswith(RV.CHANGELOG_TITLE) and "## 9.9.9 (2026-11-22)" in md and "### Features" in md
    msg = run("log", "-1", "--format=%B", sha)
    newest = max(table["apps"], key=AR.ver)
    assert f"{AR.TABLE_REL} (T1048): app 9.9.9: the same formats as {newest}." in msg
    assert msg.startswith("chore(release): 9.9.9, its version in app.json and package.json, a draft changelog entry")
    assert RV.parse(msg).type == "chore" and not RV.is_ignored(msg) and len(RV.header_of(msg)) <= 100
    assert R.with_changelog_entry(ts, "9.9.9", "x") == ts                   # an entry already there stays
    with pytest.raises(RuntimeError, match="its version is not written once"):
        R.with_version('{"expo": {"version": "1.0.0"}, "x": {"version": "1.0.0"}}', R.APP_JSON_REL, "2.0.0")


def test_the_cuts_version_commit_adds_the_new_versions_app_reads_row(cut_repo):
    """(T1048) the 0.9.0 cut left the row out (6ea5b344a added it by hand). The version commit's app_reads.json is its
    base's with ONE row added, the new version reading what the newest recorded version reads, and `--check` passes on
    it. Mutations: drop the table from the version commit's files, or record the row under the old version: this
    fails."""
    from pipeline.tools.emit import app_reads as AR
    run, here, table = cut_repo
    head = run("rev-parse", "HEAD")
    newest = max(table["apps"], key=AR.ver)
    d = RV.decide(R.CP.app_version(here / R.APP_JSON_REL), "x", [_commit("fix(T1): a bug")])
    sha = R.version_commit("9.9.9", d, day="2026-11-22")
    after = json.loads(run("show", f"{sha}:{AR.TABLE_REL}"))
    assert after["apps"]["9.9.9"] == {"reads": table["apps"][newest]["reads"]}
    assert {v: r for v, r in after["apps"].items() if v != "9.9.9"} == table["apps"] and after["_about"] == table["_about"]
    diff = run("diff", "-U0", head, sha, "--", AR.TABLE_REL).splitlines()
    added = [ln for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    want = [f'+  "{newest}"', '+  "9.9.9"']               # the row, and a comma on the last; by each one's own length:
    assert [ln[:len(w)] for ln, w in zip(added, want, strict=True)] == want   # a fixed 10 cut "0.10.0" (the 0.10.0 cut)
    assert [ln[1:] + "," for ln in removed] == [ln[1:] for ln in added[:1]]
    reads = AR.reads_in(lambda rel: run("show", f"{sha}:{rel}"))
    assert AR.check_problems(after, "9.9.9", reads) == []                 # the release branch's `--check` passes


def test_the_cut_stops_when_what_the_app_reads_moved_and_its_dry_run_prints_the_row(cut_repo, monkeypatch, capsys):
    """(T1048) a format that moved since the newest recorded version stops the cut, naming the file, old -> new, before
    anything is made; `--reads-moved WHY` records the new formats (the owner's call). `--cut --dry-run` prints the row
    and makes nothing: no ref, no object. Mutations: skip the comparison, or return before the row in the dry run:
    this fails."""
    import re
    from pipeline.tools.emit import app_reads as AR
    run, here, table = cut_repo
    newest = max(table["apps"], key=AR.ver)
    nxt = ".".join(newest.split(".")[:2] + [str(int(newest.split(".")[2]) + 1)])  # the cut is the patch after the newest row
    monkeypatch.setattr(R, "fetch_trunk", lambda: None)
    monkeypatch.setattr(R, "trunk_decision", lambda: RV.decide(newest, "x", [_commit("fix(T1): a bug")]))
    monkeypatch.setattr(R, "remote_ref_exists", lambda ref: False)
    made = []
    monkeypatch.setattr(R, "cut_branch", lambda b, commit=None: made.append((b, commit)) or 0)
    state = lambda: (run("for-each-ref"), run("count-objects", "-v"))                          # noqa: E731
    before = state()
    assert R.main(["--cut", "--dry-run"]) == 0 and R.main(["--cut", nxt, "--dry-run"]) == 0
    out = capsys.readouterr().out
    row = json.dumps({"reads": table["apps"][newest]["reads"]}, sort_keys=True)
    assert out.count(f"app_reads.json (T1048): app {nxt}: the same formats as {newest}; the version commit adds\n"
                     f'  "{nxt}": {row}') == 2
    assert f"would cut release/{nxt}" in out and state() == before and made == []

    pack = here / AR.PACK_TS                                             # water's format moves on main
    text = pack.read_text()
    old_fmt = re.search(r'"water": \{\s*"format": "([^"]+)"', text).group(1)
    pack.write_text(text.replace(f'"format": "{old_fmt}"', '"format": "water/99"', 1))
    run("commit", "-qam", "water moves", "--no-verify")
    run("update-ref", "refs/remotes/origin/main", "HEAD")
    before = state()
    for argv in (["--cut", nxt, "--dry-run"], ["--cut", nxt]):
        assert R.main(argv) == 1
        err = capsys.readouterr().err
        assert f"release --cut: refused: {AR.TABLE_REL}: what the app reads moved since {newest} (water: " \
               f"['{old_fmt}'] -> ['water/99'])" in err
        assert "--reads-moved" in err and "Nothing was made" in err
    assert state() == before and made == []
    assert R.main(["--cut", nxt, "--reads-moved", f"water/99 ships in {nxt}"]) == 0
    assert "the formats moved since" in capsys.readouterr().out and len(made) == 1
    after = json.loads(run("show", f"{made[0][1]}:{AR.TABLE_REL}"))
    assert after["apps"][nxt]["reads"]["water"] == ["water/99"] and after["apps"][newest] == table["apps"][newest]
    with pytest.raises(SystemExit):
        R.main(["--cut", "--data-only", "--reads-moved", "x"])
    # a row already recorded for the version: kept when it is what the app reads, refused when it is not
    doc, what = AR.for_cut(after, nxt, after["apps"][nxt]["reads"])
    assert doc == after and "already recorded" in what
    with pytest.raises(AR.CutRefused, match="recorded already reading other formats"):
        AR.for_cut(after, nxt, table["apps"][newest]["reads"])


def test_reads_in_reads_from_a_tree_what_current_reads_reads_from_the_checkout():
    """(T1048 review) the cut's row comes from `reads_in` over origin/main's files; on this tree it must be what
    `current_reads` (contract.py + ALSO_READS, the `--check` side) says. The cut_repo fixture builds its newest row with
    `reads_in` itself, so only this pins it. Mutation: `reads_in` drops ALSO_READS -> paint loses paint/4, fails."""
    from pipeline.tools.emit import app_reads as AR
    root = Path(R.REPO_ROOT)
    assert AR.reads_in(lambda rel: (root / rel).read_text(encoding="utf-8")) == AR.current_reads()


def test_the_cut_refuses_an_unmerged_shipped_release_a_bare_table_and_an_idle_reads_moved(cut_repo, capsys):
    """(T1048 review) a `v*` tag below the cut with no row on the base (release/<it> not merged back: cutting 0.9.1
    then would compare against 0.8.5's row, and the two merge-backs conflict) stops the cut naming it; a table with no
    `_about` is refused (never a default header swapped in); `--reads-moved` when nothing moved is refused (its reason
    would be dropped). Mutations: skip any of the three checks -> fails."""
    from pipeline.tools.emit import app_reads as AR
    run, here, table = cut_repo
    run("tag", "v9.0.0")
    with pytest.raises(AR.CutRefused, match=r"v9\.0\.0 shipped, but the base has no row for 9\.0\.0: merge "
                                            r"release/9\.0\.0 back"):
        R.app_reads_row("9.9.9")
    assert R.app_reads_row("8.9.9")[1].startswith("app 8.9.9: the same formats as")   # a tag above the cut: no bar
    run("tag", "-d", "v9.0.0")
    with pytest.raises(AR.CutRefused, match="--reads-moved was given .'x'., but nothing the app reads moved since"):
        R.app_reads_row("9.9.9", reads_moved="x")
    bare = {k: v for k, v in table.items() if k != "_about"}
    (here / AR.TABLE_REL).write_text(json.dumps(bare))
    run("commit", "-qam", "no about", "--no-verify")
    run("update-ref", "refs/remotes/origin/main", "HEAD")
    with pytest.raises(AR.CutRefused, match="app_reads.json has no `_about`"):
        R.app_reads_row("9.9.9")


def test_the_release_pushes_go_to_the_release_branch_and_skip_the_hook_saying_why(monkeypatch):
    """(T995) the real commands: HEAD to release/<version>, the cut from origin/main, the tags by ref; each tells the
    pre-push hook why it skips (scripts/before_push.py BEFORE_PUSH_SKIP)."""
    ran = []
    monkeypatch.setattr(R, "run_cmd", lambda cmd, env=None: ran.append((cmd, env)) or 0)
    monkeypatch.setattr(R, "app_version", lambda: "0.9.0")
    assert R.git_push() == 0 and R.cut_branch("release/0.9.0") == 0 and R.push_tags(["data/r-x", "v0.9.0"]) == 0
    assert [c for c, _ in ran] == [["git", "push", "origin", "HEAD:release/0.9.0"],
                                   ["git", "branch", "--no-track", "release/0.9.0", "origin/main"],
                                   ["git", "push", "-u", "origin", "refs/heads/release/0.9.0"],
                                   ["git", "push", "origin", "refs/tags/data/r-x", "refs/tags/v0.9.0"]]
    pushes = [env for c, env in ran if c[1] == "push"]
    assert len(pushes) == 3 and all(e and e["BEFORE_PUSH_SKIP"].strip() for e in pushes)


def test_git_pushed_reads_the_release_branch_on_a_real_remote(tmp_path, monkeypatch):
    """(T995) on real git: no release branch on origin raises NoReleaseBranch; HEAD on it is pushed; a commit past it
    is not."""
    import subprocess
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main"]
    origin, here = tmp_path / "origin.git", tmp_path / "here"
    subprocess.run([*git, "init", "-q", "--bare", str(origin)], check=True)
    subprocess.run([*git, "init", "-q", str(here)], check=True)
    run = lambda *a: subprocess.run([*git, *a], cwd=here, check=True, capture_output=True)   # noqa: E731
    (here / "f").write_text("x")
    run("add", "f")
    run("commit", "-qm", "one")
    run("remote", "add", "origin", str(origin))
    run("push", "-q", "origin", "HEAD:refs/heads/main")
    monkeypatch.setattr(R, "REPO_ROOT", str(here))
    monkeypatch.setattr(R, "app_version", lambda: "0.9.0")
    with pytest.raises(R.NoReleaseBranch):
        R.git_pushed()
    run("push", "-q", "origin", "HEAD:refs/heads/release/0.9.0")
    assert R.git_pushed() is True and R.current_branch() == "main"
    (here / "f").write_text("y")
    run("commit", "-qam", "two")
    assert R.git_pushed() is False and len(R.git_unpushed()) == 1          # ahead: commits to push
    run("push", "-q", "origin", "HEAD:refs/heads/release/0.9.0")
    run("checkout", "-q", "HEAD~1")                                          # (T995 review) a pre-cut / stale commit
    with pytest.raises(R.ReleaseBranchMoved, match="behind"):
        R.git_pushed()
    (here / "g").write_text("z")
    run("add", "g")
    run("commit", "-qm", "off the branch")
    with pytest.raises(R.ReleaseBranchMoved, match="diverged"):
        R.git_pushed()
    # (T995 review) tags are read on origin, annotated or not; a local-only tag is not there
    run("tag", "-a", "data/r-a", "-m", "a")
    run("tag", "v9.9.9")
    run("push", "-q", "origin", "refs/tags/data/r-a")
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=here, capture_output=True, text=True).stdout.strip()
    assert R.remote_tag_commit("data/r-a") == head and R.tag_commit("data/r-a") == head
    assert R.remote_tag_commit("v9.9.9") is None and R.tag_commit("v9.9.9") == head
    run("push", "-q", "origin", "refs/tags/v9.9.9")
    assert R.remote_tag_commit("v9.9.9") == head


def test_the_rehearsal_names_the_tags_and_the_merge_back(world, capsys):
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert ("then (T995) tags data/<r-id> + v0.9.0-rc.N, the next release candidate (T1062: none when HEAD is the "
            "newest rc) on the pushed HEAD, pushes them") in out
    assert "gh pr create --base main --head release/0.9.0" in out


def test_the_upload_wants_head_to_be_the_release_branch_tip():
    """(T995 review) upload.sh's own refusal matches preflight's: HEAD == origin/<release branch>, not an ancestor."""
    text = (Path(R.REPO_ROOT) / "deploy/staging/upload.sh").read_text()
    assert '[[ $(git rev-parse HEAD) == "$(git rev-parse "origin/$GH_REF")" ]]' in text
    assert 'merge-base --is-ancestor HEAD "origin/$GH_REF"' not in text
    assert "GH_REPO=$(python3 scripts/ci_paths.py repo); GH_REF=$(python3 scripts/ci_paths.py release-branch)" in text


# ------------------------------------------------------------------------- the version and data-only releases (T998)

def test_preflight_holds_app_json_to_the_version_the_commits_compute(world, capsys):
    """(T998) the hand-set version that disagrees stops at preflight, naming both and the commits that decided it.
    Mutation: drop the comparison in check_version and this fails."""
    world["decision"] = RV.decide("0.8.5", "the cutover b5dd8ade3", [_commit("fix(T1): a bug")])
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "app.json says 0.9.0, the commits compute 0.8.6: 0.9.0 is set by hand, but the commits compute 0.8.6 " \
           "(0.8.5 + patch)" in err
    assert "c0ffee000000 fix(T1): a bug" in err and world["calls"] == []
    world["decision"] = RV.decide("0.8.5", "x", [_commit("T994 fix x")])
    assert _staging() == 1 and "not in the format: c0ffee000000 T994 fix x" in capsys.readouterr().err


def test_the_rehearsal_prints_the_computed_version_and_the_commits_that_decided_it(world, capsys):
    assert R.main(["--staging", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "ok: 0.9.0 is the version the commits compute: 0.9.0: 0.8.5 + minor, from 1 app commit(s) since the " \
           "cutover b5dd8ade3" in out
    assert "c0ffee000000 feat(T998): what 0.9.0 brings" in out


def test_a_data_only_release_ships_from_its_days_branch_and_tags_only_its_data(world, capsys):
    """(T998 item 6) on release/data-<date>: preflight takes that branch (app.json still names the shipped version,
    whose own branch is finished), the ship tags only data/<r-id>, the merge-back names the data branch, and prod
    does not ask for an app tag."""
    world["branch"], world["version"] = "release/data-2026-11-22", "0.8.5"
    world["decision"] = RV.decide("0.8.5", "the cutover b5dd8ade3", [_commit("fix(T931): x", paths=("pipeline/a.py",))])
    assert R.release_branch() == "release/data-2026-11-22"
    assert _staging() == 0
    out = capsys.readouterr().out
    assert "ok: a data-only release on release/data-2026-11-22, app 0.8.5 unchanged" in out
    assert world["made_tags"] == [(f"data/{NEW}", "abc123")] and world["pushed_tags"] == [[f"data/{NEW}"]]
    assert "v0.8.5 not tagged here: a data-only release (release/data-2026-11-22), app 0.8.5 unchanged" in out
    assert "gh pr create --base main --head release/data-2026-11-22" in out
    rec = json.loads(R.LEDGER.read_text())["releases"][NEW]
    assert rec["tags"] == {f"data/{NEW}": "abc123"} and rec["data_only"] is True
    world["remote_tags"][f"data/{NEW}"] = "abc123"
    assert R.tag_problems(NEW, rec) == []                                  # no v0.8.5 tag asked of a data release


def test_a_data_only_branch_with_an_app_change_stops_at_preflight(world, capsys):
    world["branch"], world["version"] = "release/data-2026-11-22", "0.8.5"
    world["decision"] = RV.decide("0.8.5", "the cutover b5dd8ade3", [_commit("feat(T985): a layer")])
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "release/data-2026-11-22 is a data-only release, but app commits since the cutover b5dd8ade3 bump the app " \
           "to 0.9.0" in err and world["calls"] == []
    world["decision"], world["version"] = RV.decide("0.8.5", "x", []), "0.8.6"
    assert _staging() == 1 and "but app.json says 0.8.6, not the shipped 0.8.5" in capsys.readouterr().err


def test_the_shipping_branch_is_named_once():
    """(T998) scripts/ci_paths.py names the branch upload.sh's workflow runs (`release-branch`) as preflight does."""
    assert R.CP.shipping_branch("release/data-2026-11-22", "0.8.5") == "release/data-2026-11-22"
    assert R.CP.shipping_branch("main", "0.8.5") == "release/0.8.5"
    assert R.CP.shipping_branch("release/data-tomorrow", "0.8.5") == "release/0.8.5"
    assert R.CP.data_branch("2026-11-22") == "release/data-2026-11-22"


# ── (T1062) release candidates: the final tag, production's check, and the real git under them ──────────────────────

def test_final_tags_the_newest_rc_and_refuses_everything_else(world, capsys):
    """(T1062) `--final X.Y.Z` puts v<x.y.z> on the newest rc's commit, pushed — refused, saying why, with no rc, a
    version other than app.json's, HEAD or origin's branch past the rc beyond the build numbers, a final tag already
    there (on origin or only here), a data-only branch, or --dry-run. It never moves or deletes a tag. Mutations: drop
    the comparison with the rc, or the exists check: this fails."""
    assert R.main(["--final", "0.9.0"]) == 1
    assert "refused: no release candidate of 0.9.0 on origin: the staging ship tags one" in capsys.readouterr().err
    assert _staging() == 0 and world["remote_tags"]["v0.9.0-rc.1"] == "abc123"
    capsys.readouterr()
    world["made_tags"].clear()
    assert R.main(["--final", "0.9.1"]) == 1
    assert "refused: 0.9.1 is not app.json's version (0.9.0, on release/0.9.0)" in capsys.readouterr().err
    assert R.main(["--final", "0.9.0"]) == 1                              # HEAD has a change the rc does not hold
    err = capsys.readouterr().err
    assert ("refused: HEAD and origin/release/0.9.0 not v0.9.0-rc.1 (abc123) apart from the build numbers (HEAD: "
            "app/apps/mobile/src/a.tsx; origin/release/0.9.0: app/apps/mobile/src/a.tsx): tag a new rc first "
            "(python -m pipeline release --staging --push)") in err
    world["same_tree"] = {("abc123", "HEAD")}                              # only origin's branch moved on
    assert R.main(["--final", "0.9.0"]) == 1 and "refused: origin/release/0.9.0 not v0.9.0-rc.1" in capsys.readouterr().err
    world["same_tree"].add(("abc123", "origin/release/0.9.0"))
    world["tag_at"]["v0.9.0"] = "0ldrc00"                                  # (review) a local-only final elsewhere
    assert R.main(["--final", "0.9.0"]) == 1
    assert ("refused: a local-only tag v0.9.0 points at 0ldrc00, not at the newest release candidate v0.9.0-rc.1 "
            "(abc123): read it (git show v0.9.0); it is not on origin, so: git tag -d v0.9.0") in capsys.readouterr().err
    del world["tag_at"]["v0.9.0"]
    with pytest.raises(SystemExit):
        R.main(["--final", "0.9.0", "--dry-run"])
    assert world["made_tags"] == [] and world["pushed_tags"] == [[f"data/{NEW}", "v0.9.0-rc.1"]]
    assert R.main(["--final", "0.9.0"]) == 0
    assert "release --final: v0.9.0 at abc123 (= v0.9.0-rc.1), pushed." in capsys.readouterr().out
    assert world["made_tags"] == [("v0.9.0", "abc123")] and world["pushed_tags"][-1] == ["v0.9.0"]
    assert world["remote_tags"]["v0.9.0"] == "abc123" and world["fetched_branches"][-1] == "release/0.9.0"
    assert R.main(["--final", "0.9.0"]) == 1                               # once only: a tag is never moved
    assert "refused: v0.9.0 exists already (on origin at abc123): a tag is never moved" in capsys.readouterr().err
    world["branch"], world["version"] = "release/data-2026-11-25", "0.9.0"
    assert R.main(["--final", "0.9.0"]) == 1 and "is a data-only release" in capsys.readouterr().err
    assert world["made_tags"] == [("v0.9.0", "abc123")]


def test_prod_wants_the_final_tag_on_the_newest_rc_holding_the_data(staged, monkeypatch, capsys):
    """(T1062) with rcs on origin: v<version> must be AT the newest rc's commit (before it is no longer enough), and
    the commit the data shipped from at that commit or before it. Mutations: keep the old "v at or before the ship"
    rule when rcs exist, or skip the data-in-rc check: this fails."""
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    del staged["remote_tags"]["v0.9.0"]
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert ("no tag v0.9.0 on origin: the final tag goes on the newest release candidate v0.9.0-rc.1 (abc123) once the "
            "owner passed it: python -m pipeline release --final 0.9.0") in capsys.readouterr().err
    staged["remote_tags"]["v0.9.0"], staged["ancestors"] = "0ther00", {("0ther00", "abc123")}
    assert R.prod(NEW, ask=lambda _: NEW) == 1                            # before the ship: the old rule, not enough
    assert "v0.9.0 points at 0ther00, not at the newest release candidate v0.9.0-rc.1 (abc123)" in capsys.readouterr().err
    staged["remote_tags"]["v0.9.0-rc.2"] = "f1x0000"                       # a later rc: the final must be on it
    staged["remote_tags"]["v0.9.0"] = "abc123"
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert "v0.9.0 points at abc123, not at the newest release candidate v0.9.0-rc.2 (f1x0000)" in capsys.readouterr().err
    staged["remote_tags"]["v0.9.0"] = "f1x0000"
    assert R.prod(NEW, ask=lambda _: NEW) == 1
    assert (f"{NEW} shipped from abc123, which is not v0.9.0-rc.2 (f1x0000), nor before it, nor it apart from the "
            "build numbers: the newest rc does not hold the code") in capsys.readouterr().err
    assert staged["calls"] == []
    staged["same_tree"] = {("f1x0000", "abc123")}                          # (review) only the build numbers differ
    assert R.prod(NEW, ask=lambda _: NEW) == 0
    staged["calls"].clear()
    staged["same_tree"] = set()
    staged["ancestors"] = {("abc123", "f1x0000")}                          # rc.2 is the data's commit plus a fix
    assert R.prod(NEW, ask=lambda _: NEW) == 0 and staged["calls"] == [("upload", {"CONFIRM_PROD": "canifishthis.ca"})]


def test_release_candidates_on_real_git(tmp_path, monkeypatch, capsys):
    """(T1062) on real git: `remote_rcs` lists only this version's rcs ON ORIGIN (annotated peeled; 0.9.10's and a
    local-only one are not); `beyond_build_numbers` lets only app.json's versionCode / buildNumber differ; `rc_check`
    (the store builds' precheck) passes HEAD = newest rc + a versionCode commit and refuses anything more, naming
    the command; and the version rule ignores rc tags: the next version counts from the last x.y.z tag, and rcs are
    not shipped versions. Mutations: match the rc pattern on a prefix only, compare app.json whole, or let
    VERSION take a suffix: this fails."""
    import subprocess
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main"]
    origin, here = tmp_path / "origin.git", tmp_path / "here"
    subprocess.run([*git, "init", "-q", "--bare", str(origin)], check=True)
    subprocess.run([*git, "init", "-q", str(here)], check=True)
    run = lambda *a: subprocess.run([*git, *a], cwd=here, check=True, capture_output=True, text=True).stdout.strip()  # noqa: E731
    app = here / R.APP_JSON_REL
    app.parent.mkdir(parents=True)

    def commit(code=1, version="0.9.0", other=None):
        app.write_text(json.dumps({"expo": {"version": version, "ios": {"buildNumber": str(code)},
                                            "android": {"versionCode": code}}}, indent=2) + "\n")
        if other:
            (here / "x.txt").write_text(other)
        run("add", "-A")
        run("commit", "-qm", f"c {code} {other}")
        return run("rev-parse", "HEAD")
    c1 = commit()
    run("remote", "add", "origin", str(origin))
    run("push", "-q", "origin", "HEAD:refs/heads/main", "HEAD:refs/heads/release/0.9.0")
    monkeypatch.setattr(R, "REPO_ROOT", str(here))
    monkeypatch.setattr(RV, "REPO_ROOT", str(here))
    monkeypatch.setattr(R, "app_version", lambda: "0.9.0")
    assert R.remote_rcs("0.9.0") == {} and R.rc_check() == 1
    assert ("no release candidate of 0.9.0 on origin: tag one first, the staging ship does: python -m pipeline release "
            "--staging --push") in capsys.readouterr().out
    run("tag", "-a", "v0.9.0-rc.1", "-m", "rc 1", c1)
    c2 = commit(other="a fix")
    run("tag", "v0.9.0-rc.2", c2)
    run("tag", "v0.9.10-rc.5", c2)
    run("tag", "v0.9.0-rc.2-fix", c1)                                      # not an rc name: never read as rc.2
    run("tag", "v0.8.6", c1)
    run("push", "-q", "origin", "refs/tags/v0.9.0-rc.1", "refs/tags/v0.9.0-rc.2", "refs/tags/v0.9.10-rc.5",
        "refs/tags/v0.9.0-rc.2-fix")
    run("tag", "v0.9.0-rc.3", c2)                                          # local only: not a release candidate
    assert R.remote_rcs("0.9.0") == {1: c1, 2: c2} and R.newest_rc("0.9.0") == ("v0.9.0-rc.2", c2)
    c3 = commit(code=2, other="a fix")                                    # a Play build's versionCode commit
    assert R.beyond_build_numbers(c2, c3) == [] and R.beyond_build_numbers(c1, c3) == ["x.txt"]
    assert R.rc_check() == 0 and f"HEAD is v0.9.0-rc.2 ({c2[:12]}) apart from the build numbers" in capsys.readouterr().out
    c4 = commit(code=2, version="0.9.1", other="a fix")
    assert R.beyond_build_numbers(c3, c4) == [f"{R.APP_JSON_REL} beyond its build numbers"]
    commit(code=2, version="0.9.0", other="another fix")
    assert R.rc_check() == 1
    assert (f"HEAD is not v0.9.0-rc.2 ({c2[:12]}) apart from the build numbers (x.txt): tag a new rc first: python -m "
            "pipeline release --staging --push") in capsys.readouterr().out
    assert RV.last_tag() == ("v0.8.6", "0.8.6") and R.shipped_versions() == ("0.8.6",)


@pytest.fixture
def rc_repo(tmp_path, monkeypatch):
    """(T1062 review) a real clone on release/0.10.0 with a bare origin, the release command pointed at it: tags are
    made, pushed and read by the real functions (`tag_release`, `final`, `tag_problems`); only the ledger write is
    skipped. `commit(code, other)` = a commit setting app.json's build numbers to `code` (and x.txt to `other`)."""
    import subprocess
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main"]
    origin, here = tmp_path / "origin.git", tmp_path / "here"
    subprocess.run([*git, "init", "-q", "--bare", str(origin)], check=True)
    subprocess.run([*git, "init", "-q", str(here)], check=True)
    run = lambda *a: subprocess.run([*git, *a], cwd=here, check=True, capture_output=True, text=True).stdout.strip()  # noqa: E731
    app = here / R.APP_JSON_REL
    app.parent.mkdir(parents=True)

    def commit(code=1, other=None):
        app.write_text(json.dumps({"expo": {"version": "0.10.0", "ios": {"buildNumber": str(code)},
                                            "android": {"versionCode": code}}}, indent=2) + "\n")
        if other:
            (here / "x.txt").write_text(other)
        run("add", "-A")
        run("commit", "-qm", f"c {code} {other}")
        run("push", "-q", "origin", "HEAD:refs/heads/release/0.10.0")
        return run("rev-parse", "HEAD")
    run("remote", "add", "origin", str(origin))
    run("checkout", "-q", "-b", "release/0.10.0")
    for k, v in (("GIT_COMMITTER_NAME", "t"), ("GIT_COMMITTER_EMAIL", "t@t"), ("GIT_AUTHOR_NAME", "t"),
                 ("GIT_AUTHOR_EMAIL", "t@t")):
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(R, "REPO_ROOT", str(here))
    monkeypatch.setattr(R, "app_version", lambda: "0.10.0")
    monkeypatch.setattr(R, "save_ledger", lambda led: None)
    monkeypatch.setattr(R, "PUSH_ENV", {})
    hook = origin / "hooks" / "pre-receive"

    def refuse(tag: str | None) -> None:                  # origin refuses a push of this tag (None: accepts all)
        if tag is None:
            hook.unlink(missing_ok=True)
            return
        hook.write_text(f"#!/bin/sh\nwhile read o n ref; do [ \"$ref\" = refs/tags/{tag} ] && exit 1; done\nexit 0\n")
        hook.chmod(0o755)
    import types
    return types.SimpleNamespace(run=run, commit=commit, refuse=refuse, ctx=types.SimpleNamespace(led={}))


def test_real_git_a_data_release_shipped_from_a_versioncode_commit_goes_to_production(rc_repo):
    """(T1062 review, finding 1) r-1 ships from X (rc.1); a Play build commits the versionCode bump B; r-2 (new data,
    no code) ships from B: no new rc, `--final` puts v0.10.0 on X, and production takes r-2 (B is X apart from the
    build numbers). A code change after the rc is still refused. Mutation: drop the build-number clause from
    `tag_problems`: r-2 is refused, this fails."""
    g = rc_repo
    x = g.commit()
    r1 = {"uploaded_commit": x}
    assert R.tag_release(g.ctx, "r-1", r1).startswith(f"tagged data/r-1 at {x[:12]}, v0.10.0-rc.1 at {x[:12]}")
    b = g.commit(code=2)                                                    # the versionCode commit, pushed
    r2 = {"uploaded_commit": b}
    said = R.tag_release(g.ctx, "r-2", r2)
    assert "v0.10.0-rc.1" in said and "no new rc" in said and R.remote_rcs("0.10.0") == {1: x}
    assert R.final("0.10.0") == 0 and R.remote_tag_commit("v0.10.0") == x
    assert R.tag_problems("r-1", r1) == [] and R.tag_problems("r-2", r2) == []
    c = g.commit(code=2, other="a code change")
    r3 = {"uploaded_commit": c, "ship_commit": c, "app_version": "0.10.0"}
    g.run("tag", "-a", "data/r-3", "-m", "d", c)
    g.run("push", "-q", "origin", "refs/tags/data/r-3")
    assert any("does not hold the code this data release shipped with" in p for p in R.tag_problems("r-3", r3))


def test_real_git_a_final_whose_push_failed_is_pushed_by_the_rerun_or_refused_when_stale(rc_repo, capsys):
    """(T1062 review, finding 2) `--final`'s push refused: the local tag stays; the rerun pushes it (it is on the newest
    rc). The staging ship stops on a local-only final, plainly. A local-only final on an OLDER rc (a new rc tagged
    since) is refused, naming `git tag -d`, and never pushed. Mutations: refuse every local final, or push one that is
    not on the newest rc: this fails."""
    g = rc_repo
    x = g.commit()
    r1 = {"uploaded_commit": x}
    R.tag_release(g.ctx, "r-1", r1)
    g.refuse("v0.10.0")
    assert R.final("0.10.0") == 1 and R.tag_commit("v0.10.0") == x and R.remote_tag_commit("v0.10.0") is None
    assert "was made here but the push failed" in capsys.readouterr().err
    g.refuse(None)
    with pytest.raises(R.Stop, match="a local-only tag v0.10.0 points at"):
        R.tag_release(g.ctx, "r-1", r1)                                    # a rerun of the ship stops on it
    assert R.final("0.10.0") == 0 and R.remote_tag_commit("v0.10.0") == x
    assert f"v0.10.0 at {x[:12]} (= v0.10.0-rc.1), made before, pushed." in capsys.readouterr().out
    # the stale case: a final made on rc.1 whose push is refused, then a fix tags rc.2 (reset the scratch origin first)
    g.run("tag", "-d", "v0.10.0")
    g.run("push", "-q", "origin", ":refs/tags/v0.10.0")
    g.refuse("v0.10.0")
    assert R.final("0.10.0") == 1 and R.tag_commit("v0.10.0") == x
    g.refuse(None)
    g.run("tag", "-d", "v0.10.0")                                          # the ship would stop on it: set it aside
    y = g.commit(other="a fix")
    assert "v0.10.0-rc.2 at" in R.tag_release(g.ctx, "r-1", r1)
    g.run("tag", "v0.10.0", x)                                             # the stale local final comes back
    assert R.final("0.10.0") == 1
    err = capsys.readouterr().err
    assert (f"refused: a local-only tag v0.10.0 points at {x[:12]}, not at the newest release candidate v0.10.0-rc.2 "
            f"({y[:12]})") in err and "git tag -d v0.10.0" in err
    assert R.remote_tag_commit("v0.10.0") is None


def test_real_git_rc_9_is_followed_by_rc_10(rc_repo):
    """(T1062 review) the next rc is one past the newest BY NUMBER: rc.10 after rc.9, never rc.10 read as before rc.9
    and never a leading zero. Mutation: take the newest by name (string order): this fails."""
    g = rc_repo
    x = g.commit()
    for n in range(1, 10):
        g.run("tag", f"v0.10.0-rc.{n}", x)
    g.run("tag", "v0.10.0-rc.011", x)                                      # not an rc name
    g.run("push", "-q", "origin", "--tags")
    y = g.commit(other="a fix")
    assert R.tag_release(g.ctx, "r-1", {"uploaded_commit": y}).startswith(f"tagged data/r-1 at {y[:12]}, "
                                                                          f"v0.10.0-rc.10 at {y[:12]}")
    assert max(R.remote_rcs("0.10.0")) == 10 and R.newest_rc("0.10.0") == ("v0.10.0-rc.10", y)
    z = g.commit(other="another fix")                                     # and after rc.10, rc.11 (not "9" + 1)
    assert f"v0.10.0-rc.11 at {z[:12]}" in R.tag_release(g.ctx, "r-1", {"uploaded_commit": y, "tags": {"x": y}})


def test_a_comparison_that_fails_at_the_ship_is_a_stop_not_a_traceback(world, capsys, monkeypatch):
    """(T1062 review, finding 3) the rerun's comparison with the newest rc fetches a missing commit: a git failure there
    (after the upload) is a Stop naming it, never a traceback. Mutation: move the call out of the try: this fails."""
    import subprocess
    assert _staging() == 0
    capsys.readouterr()

    def down(a, b):
        raise subprocess.CalledProcessError(128, ["git", "fetch"])
    monkeypatch.setattr(R, "beyond_build_numbers", down)
    assert _staging() == 1
    err = capsys.readouterr().err
    assert "could not compare abc123 with v0.9.0-rc.1" in err and "only the tags are left" in err


BATT = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t80%; discharging; 3:00 remaining"


def test_a_long_run_keeps_the_mac_awake_on_power(world, capsys):
    """(T1161) --staging on AC starts caffeinate bound to this process's pid and says so, lid open. Mutation: drop the
    start_caffeinate call in stay_awake: this fails."""
    import os
    assert _staging() == 0
    assert world["caffeinated"] == [os.getpid()]
    assert "keeping the Mac awake while this runs (caffeinate -i -s): keep the lid open, on power" in capsys.readouterr().out


def test_an_unattended_run_on_battery_is_refused(world, monkeypatch, capsys):
    """(T1161) no terminal + battery: refused before any stage, caffeinate not started, nothing built. Mutation: drop the
    battery refusal in stay_awake: this fails."""
    world["pmset"] = BATT
    monkeypatch.setattr(R, "is_tty", lambda: False)
    with pytest.raises(SystemExit) as e:
        _staging()
    assert "refused on battery for an unattended run" in str(e.value) and "--on-battery" in str(e.value)
    assert world["caffeinated"] == [] and not world["calls"]


def test_on_battery_with_a_reason_goes_on_and_prints_it(world, monkeypatch, capsys):
    monkeypatch.setattr(R, "is_tty", lambda: False)
    world["pmset"] = BATT
    assert _staging(on_battery="the AC adapter is in the car") == 0
    assert len(world["caffeinated"]) == 1
    assert "ON BATTERY, goes on (--on-battery): the AC adapter is in the car" in capsys.readouterr().out


def test_an_attended_run_on_battery_warns_and_goes_on(world, monkeypatch, capsys):
    monkeypatch.setattr(R, "is_tty", lambda: True)
    world["pmset"] = BATT
    assert _staging() == 0
    assert "WARNING: on battery" in capsys.readouterr().out and len(world["caffeinated"]) == 1


def test_an_empty_on_battery_reason_is_refused(world):
    with pytest.raises(SystemExit):
        _staging(on_battery="  ")


@pytest.mark.parametrize("said", ["", "pmset: something new\n", "Now drawing from 'UPS Power'\n"])
def test_an_unreadable_pmset_is_refused_not_passed(world, monkeypatch, said):
    """(T1161) a first line that is neither AC nor Battery Power is a refusal, attended or not (never a silent pass)."""
    world["pmset"] = said
    monkeypatch.setattr(R, "is_tty", lambda: True)
    with pytest.raises(SystemExit) as e:
        _staging()
    assert "cannot read the power source" in str(e.value) and world["caffeinated"] == []


def test_a_failing_pmset_is_refused(monkeypatch):
    import subprocess
    monkeypatch.setattr(R.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 1, "", "pmset: boom"))
    with pytest.raises(SystemExit) as e:
        R.pmset_batt()
    assert "cannot read the power source" in str(e.value) and "boom" in str(e.value)


def test_which_runs_hold_the_mac_awake(world):
    """(T1161) --staging, --prod and a rehearsal that builds or re-cuts hold it; a bare --dry-run (cheap checks) does not."""
    assert R.main(["--staging", "--dry-run"]) == 0 and world["caffeinated"] == []
    assert R.main(["--staging", "--dry-run", "--build"]) == 0 and len(world["caffeinated"]) == 1
    R.main(["--staging", "--dry-run", "--rehearse-follows"])
    assert len(world["caffeinated"]) == 2
    R.main(["--prod", NEW, "--dry-run"])                                   # prod's dry run only reads: no hold
    assert len(world["caffeinated"]) == 2


def test_not_macos_has_nothing_to_keep_awake(world, monkeypatch):
    monkeypatch.setattr(R, "is_macos", lambda: False)
    assert R.stay_awake() == "not macOS: nothing to keep awake" and world["caffeinated"] == []


def test_caffeinate_is_detached_from_the_releases_streams(monkeypatch):
    """(T1161 review) the helper inherits none of the release's stdin/stdout/stderr. Mutation: drop the DEVNULLs: fails."""
    import subprocess
    seen = {}
    monkeypatch.setattr(R.subprocess, "Popen", lambda cmd, **kw: seen.update(cmd=cmd, **kw))
    R.start_caffeinate(4242)
    assert seen["cmd"] == ["caffeinate", "-i", "-s", "-w", "4242"]
    assert seen["stdin"] == seen["stdout"] == seen["stderr"] == subprocess.DEVNULL


def test_a_prod_run_that_passes_its_checks_holds_the_mac_awake(staged, monkeypatch):
    """(T1161 review) the prod hold starts inside prod(), after its checks. Mutation: drop the call: this fails."""
    import os
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    staged["caffeinated"].clear()
    assert R.prod(NEW, ask=lambda _: NEW) == 0
    assert staged["caffeinated"] == [os.getpid()]


def test_an_unattended_prod_run_on_battery_gets_the_human_only_refusal(staged, monkeypatch, capsys):
    """(T1161 review) no terminal on battery: prod's own refusal (rc 2), not the battery one; no hold started."""
    monkeypatch.setenv("CONFIRM_PROD", NEW)
    monkeypatch.setattr(R, "is_tty", lambda: False)
    staged["pmset"] = BATT
    staged["caffeinated"].clear()
    assert R.main(["--prod", NEW]) == 2
    assert "no interactive terminal" in capsys.readouterr().err and staged["caffeinated"] == []
