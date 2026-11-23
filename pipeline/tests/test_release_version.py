"""(T998) THE APP VERSION FROM THE COMMITS: `pipeline/release_version.py`.

The ticket's fixture histories (fix only -> 0.8.6; one feat -> 0.9.0; `feat!` before 1.0 -> 0.9.0, after it -> 2.0.0;
docs / ci only -> no app release), the shared parse contract with commitlint (`app/tools/commits-contract.json`, which
app/tools/check-commits.test.ts holds commitlint to), only the app's commits counting, a hand-set version refused,
and the reading of real git history: since the last v* tag, else since the cutover, never the exempt history."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from pipeline import release_version as RV

ROOT = Path(__file__).resolve().parents[2]
APP = ("app/apps/mobile/src/screen.tsx",)
_n = iter(range(10_000))


def c(message: str, paths=APP, parents=1) -> RV.Commit:
    return RV.Commit(f"{next(_n):040x}", tuple(f"p{i}" for i in range(parents)), message, tuple(paths))


def test_the_tickets_fixture_histories():
    assert RV.decide("0.8.5", "x", [c("fix(T1): a"), c("docs(T1): b")]).version == "0.8.6"
    assert RV.decide("0.8.5", "x", [c("fix(T1): a"), c("feat(T2): b"), c("fix: c")]).version == "0.9.0"
    assert RV.decide("0.8.5", "x", [c("feat(T2)!: b")]).version == "0.9.0"               # before 1.0: minor
    assert RV.decide("0.8.5", "x", [c("fix: a\n\nBREAKING CHANGE: old apps")]).version == "0.9.0"
    assert RV.decide("1.0.0", "v1.0.0", [c("feat(T2)!: b")]).version == "2.0.0"          # after it: the full rule
    assert RV.decide("1.4.2", "v1.4.2", [c("feat: b")]).version == "1.5.0"
    assert RV.decide("1.4.2", "v1.4.2", [c("perf: b")]).version == "1.4.3"
    none = RV.decide("0.8.5", "the cutover b5dd8ade3", [c("docs(T1): a"), c("ci(T2): b"), c("chore: c"), c("test: d")])
    assert none.version is None and none.level == "none"
    assert none.said().startswith("no app release: none of the 4 app commit(s) since the cutover b5dd8ade3 bumps the "
                                  "app") and "it stays 0.8.5, a data-only release (--cut --data-only)" in none.said()


def test_the_decision_names_the_commits_that_decided_it():
    feat = c("feat(T985): a new layer")
    d = RV.decide("0.8.5", "the cutover b5dd8ade3", [c("fix(T1): a"), feat, c("docs: b")])
    assert d.deciding == [feat] and d.level == "minor"
    said = d.said().splitlines()
    assert said[0] == "0.9.0: 0.8.5 + minor, from 3 app commit(s) since the cutover b5dd8ade3"
    assert said[1:3] == ["  decided by (minor):", f"    {feat.sha[:12]} feat(T985): a new layer"]
    assert "1 minor, 1 none, 1 patch" in said[3]


def test_only_the_apps_commits_bump_the_app():
    """A pipeline / data / docs / worker / dev-tool commit is the data release's: a `fix` there bumps nothing."""
    for paths in (("pipeline/release.py",), ("data/curated/x.json",), ("app/workers/feeds/src/a.ts",),
                  ("app/tools/check-x.mjs",), ("app/fixtures/golden/taps.json",), ("app/packages/core/src/a.test.ts",),
                  ("app/MAP.md",), ("app/packages/ui-native/src/regs/fixtures/a.json",),
                  # (T1021) dev tooling at the top of app/ and the data package's fixture release
                  ("app/knip.jsonc",), ("app/layers.json",), ("app/vitest.config.ts",),
                  ("app/packages/data/dev/mini-release/water.json",)):
        d = RV.decide("0.8.5", "x", [c("fix(T1): a", paths=paths)])
        assert d.version is None and len(d.not_app) == 1, paths
    for paths in (("app/packages/core/src/a.ts",), ("app/apps/mobile/app.json",), ("app/tools/seo/build.ts",),
                  ("pipeline/x.py", "app/packages/map/src/a.ts"),
                  ("app/tools/maplibre-patch/feature-state-deltas-lean.patch",),
                  ("app/package.json",), ("app/packages/data/src/a.ts",)):   # the dependency list and real data code ship
        assert RV.decide("0.8.5", "x", [c("fix(T1): a", paths=paths)]).version == "0.8.6", paths


def test_merges_count_by_what_they_merged_and_a_bad_app_commit_stops():
    d = RV.decide("0.8.5", "x", [c("Merge branch 'main' into feat/x", parents=2), c("merge origin/main into x", parents=2),
                                 c("fix(T1): a (#5)", parents=2)])
    assert d.version == "0.8.6" and not d.bad and len(d.counted["merge"]) == 1 and len(d.counted["ignored"]) == 1
    bad = c("T994 fix x")
    d = RV.decide("0.8.5", "x", [c("feat: a"), bad, c("T1 free-form, data only", paths=("pipeline/a.py",))])
    assert d.version is None and d.bad == [bad]
    assert d.said().splitlines()[:2] == ["CANNOT COMPUTE the version: 1 app commit(s) since x are not Conventional Commits",
                                         f"  not in the format: {bad.sha[:12]} T994 fix x"]


def test_a_hand_set_version_must_be_the_computed_one_but_for_the_launch():
    d = RV.decide("0.8.5", "the cutover", [c("feat: a")])
    assert RV.accepts("0.9.0", d) == (True, "0.9.0 is the version the commits compute")
    assert RV.accepts("0.8.6", d) == (False, "0.8.6 is set by hand, but the commits compute 0.9.0 (0.8.5 + minor)")
    assert RV.accepts("1.0.0", d)[0] and "the first open Play / App Store release" in RV.accepts("1.0.0", d)[1]
    none = RV.decide("0.8.5", "the cutover", [c("docs: a")])
    assert RV.accepts("0.8.6", none) == (False, "0.8.6 is set by hand, but no app commit since the cutover bumps the app "
                                                "(it stays 0.8.5)")
    assert not RV.accepts("1.0.1", none)[0] and not RV.accepts("2.0.0", RV.decide("1.2.0", "v1.2.0", [c("feat: a")]))[0]


def test_the_parse_contract_with_commitlint():
    """Every message of the contract reads here as commitlint reads it (check-commits.test.ts holds commitlint to the
    same file), and as the bump the contract names."""
    contract = json.loads((ROOT / "app" / "tools" / "commits-contract.json").read_text())["messages"]
    assert len(contract) > 20
    for m in contract:
        p = RV.parse(m["message"])
        got = {"type": p.type if p else None, "scope": p.scope if p else None, "breaking": bool(p and p.breaking),
               "ignored": RV.is_ignored(m["message"]), "bump": RV.reading(c(m["message"]))}
        assert got == {k: m[k] for k in got}, m["message"]


def test_the_types_are_the_ones_commitlint_allows():
    """One list (app/tools/versioning.json `bump`): commitlint's type-enum is built from the same keys."""
    assert set(RV.BUMP) == {"feat", "fix", "perf", "docs", "chore", "ci", "test", "refactor", "build", "style"}
    assert {k for k, v in RV.BUMP.items() if v} == {"feat", "fix", "perf"}
    assert "Object.keys(VERSIONING.bump)" in (ROOT / "app" / "tools" / "commitlint.config.mjs").read_text()


def test_the_developer_changelog_groups_by_type_and_replaces_its_own_entry():
    d = RV.decide("0.8.5", "the cutover", [c("feat(T985): a layer"), c("fix(T1): a bug"), c("feat!: x"),
                                           c("docs(handoff): y"), c("Merge branch 'main' into z", parents=2)])
    sec = RV.changelog_section("0.9.0", "2026-11-22", d)
    assert sec.startswith("## 0.9.0 (2026-11-22)\n\nFrom 0.8.5: 4 app commit(s) since the cutover.\n")
    order = [sec.index(h) for h in ("### Breaking changes", "### Features", "### Fixes", "### Other")]
    assert order == sorted(order) and "- **T985** a layer (`" in sec and "Merge branch" not in sec
    text = RV.changelog_with(None, sec, "0.9.0")
    assert text.startswith(RV.CHANGELOG_TITLE) and text.count("## 0.9.0") == 1
    again = RV.changelog_with(text, sec.replace("a layer", "the layer"), "0.9.0")
    assert again.count("## 0.9.0") == 1 and "the layer" in again
    newer = RV.changelog_with(again, RV.changelog_section("0.9.1", "2026-11-23", d), "0.9.1")
    assert newer.index("## 0.9.1") < newer.index("## 0.9.0")


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A real repository: history before a cutover, an exempt side history, then commits that touch the app or not."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main", "-c",
           "commit.gpgsign=false"]
    here = tmp_path / "here"
    subprocess.run([*git, "init", "-q", str(here)], check=True, env=env)

    def run(*a):
        return subprocess.run([*git, *a], cwd=here, check=True, capture_output=True, text=True, env=env).stdout.strip()

    def commit(message, path=None):
        if path:
            f = here / path
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(f.read_text() + "x" if f.exists() else "x")
            run("add", path)
        run("commit", "-q", "--allow-empty", "--no-verify", "-m", message)
        return run("rev-parse", "HEAD")
    commit("free-form before the cutover", "app/apps/mobile/a.ts")
    cut = commit("docs(T998): the cutover")
    run("checkout", "-q", "--orphan", "old")
    run("rm", "-rq", "--cached", ".")
    old = commit("fix(in-season): the old main, superseded", "app/apps/mobile/b.ts")
    run("checkout", "-q", "-f", "main")
    run("merge", "-q", "--no-verify", "--allow-unrelated-histories", "-m", "Merge old into main", "old")
    monkeypatch.setattr(RV, "REPO_ROOT", str(here))
    monkeypatch.setattr(RV, "VERSIONING", {**RV.VERSIONING, "cutover": cut, "exempt": {old: "why"}, "base": "0.8.5"})
    return run, commit


def test_the_history_is_read_since_the_cutover_then_since_the_last_tag(repo):
    run, commit = repo
    d = RV.decide_at("HEAD")
    assert d.version is None and d.since.startswith("the cutover") and "fix(in-season)" not in d.said()
    fix = commit("fix(T1): a bug", "app/apps/mobile/a.ts")
    commit("fix(T2): the pipeline", "pipeline/x.py")
    d = RV.decide_at("HEAD")
    assert (d.version, [x.sha for x in d.deciding], len(d.not_app)) == ("0.8.6", [fix], 1)
    assert d.counted["ignored"][0].subject == "Merge old into main"        # the merge that brought the exempt history
    run("tag", "-a", "v0.8.6", "-m", "app 0.8.6")
    run("tag", "v0.10.0-not-a-version")
    assert RV.last_tag() == ("v0.8.6", "0.8.6")
    assert RV.decide_at("HEAD").version is None                            # nothing since the tag
    commit("feat(T3): a layer", "app/packages/map/src/a.ts")
    d = RV.decide_at("HEAD")
    assert (d.base, d.since, d.version) == ("0.8.6", "v0.8.6", "0.9.0")
    # mutation: without the exemptions, the old main's fix and the free-form history are read
    RV.VERSIONING["exempt"] = {}
    assert any("fix(in-season)" in x.subject for x in RV.commits_since("HEAD", [RV.VERSIONING["cutover"]]))


def test_the_release_texts_name_runbooks_that_exist():
    """(T1021) `release --prod`'s hand-over names the one human runbook and the reference page, both files that exist; the
    post-release hints do not cite a memory note (a note is not in the repo)."""
    from pipeline import release as REL
    root = Path(__file__).resolve().parents[2]
    text = REL.prod_instructions("r-x", {})
    for rel in ("handoff/PLAN/runbooks/release-by-hand.md", "handoff/PLAN/release/PRODUCTION.md"):
        assert rel in text and (root / rel).is_file(), rel
    assert "(memory " not in Path(REL.__file__).read_text(encoding="utf-8")
