"""NO SILENT SKIPS (P2, 2026-10-09). A test that cannot find its data FAILS, naming the file and the
command that makes it; it never skips.

A test that reads generated or fetched data says so with a marker (registered in `pytest.ini`,
`--strict-markers`), and the no-data tier deselects by marker — CI runs
`-m "not slow and not needs_bundle and not needs_atlas and ..."`, a machine with the data runs
everything. The marker is the ONLY way a data test leaves a run: `need()` fails when its path is
missing, and ALSO fails when the calling test does not carry the matching `needs_<kind>` marker, so
a marker cannot drift away from the read it declares.

    needs_bundle     data/generated/bundle (bundle, verdicts, status index) and the regs export/answers
    needs_atlas      data/generated/atlas (graph, registry, section handles)
    needs_reach_run  data/generated/reaches
    needs_tiles      data/generated/tiles
    needs_source     fetched source data (the gpkg, synopsis rows, municipal sources)
    needs_cache      cache/ (the DFO history)

`predates(msg)` is the corpus-vintage gate (moved from test_lift_decisions.py): a bundle built
before the ruling a test pins FAILS; `UI_EXPORT_ALLOW_PREDATING=1` is the user's deliberate switch
for a run against an old side build, and only then does it skip.

`test_no_silent_skips.py` refuses any skip outside its allowlist.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

# (T995, 2026-11-22) git's REPOSITORY variables leave the test session before any test runs. Run from a git hook in a
# linked worktree, GIT_DIR names that worktree's gitdir; a test's `git init <tmp>` then re-initialised the REAL
# repository as bare (core.bare = true: every worktree broke). The hook and before_push.py strip them too.
for _k in subprocess.run(["git", "rev-parse", "--local-env-vars"], capture_output=True, text=True).stdout.split() + [
        "GIT_QUARANTINE_PATH"]:
    os.environ.pop(_k, None)

KINDS = ("bundle", "atlas", "reach_run", "tiles", "source", "cache")

#: The test item being run (set at setup, so a module-scoped fixture sees the test that asked for it).
_CURRENT: list = []


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    _CURRENT[:] = [item]


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item):
    _CURRENT[:] = []


def _node(request_or_node):
    """The node whose markers decide. A pytest node is used as given; a fixture's `request` (or
    None) means the running test — for a module-scoped fixture, the test that asked for it."""
    if request_or_node is not None and hasattr(request_or_node, "get_closest_marker") \
            and not hasattr(request_or_node, "_pyfuncitem"):
        return request_or_node
    if _CURRENT:
        return _CURRENT[0]
    if request_or_node is None:
        return None
    return getattr(request_or_node, "_pyfuncitem", None) or getattr(request_or_node, "node", None)


def need(request_or_node, kind: str, path, hint: str) -> Path:
    """Fail unless `path` exists — and fail unless the test carries `needs_<kind>`.

    `request_or_node` is a fixture's `request`, a pytest node, or None (the running test).
    Returns the path, so `p = need(request, "bundle", BUNDLE, ...)` reads naturally."""
    if kind not in KINDS:
        raise ValueError(f"unknown data kind {kind!r}; one of {KINDS}")
    marker = f"needs_{kind}"
    node = _node(request_or_node)
    if node is None or node.get_closest_marker(marker) is None:
        where = getattr(node, "nodeid", "<no test>")
        pytest.fail(f"{where} reads {kind} data ({path}) without the `{marker}` marker — "
                    f"add `@pytest.mark.{marker}` (or `pytestmark = pytest.mark.{marker}`)",
                    pytrace=False)
    p = Path(path)
    if not p.exists():
        pytest.fail(f"missing {kind}: {p} — make it with: {hint}", pytrace=False)
    return p


def sandbox_tree(dst: Path, *, fresh_review: bool = True) -> Path:
    """T422: a whole copy of this process's curated tree at `dst` (`curated.make_root`): in a test run that is the
    session sandbox (`machine_state`, T777: the tracked files, review empty), for a data test the live tree.
    `fresh_review` (the default) makes it a review with nothing in it, as the per-file temp stores were: the marks store
    empty, no closed list (`CURATED.review.closed` absent = nothing closed) and no closed-areas file
    (`dfo_salmon/closed_areas.json` is written from the marks alone and born with the first confirm, so with no marks it
    is absent — a copy of the live one would be rows no mark in the empty store explains). Returns the root."""
    from pipeline.common import curated as C
    from pipeline.tests.machine_state import fresh_review as _fresh
    root = C.make_root(dst)
    return _fresh(root) if fresh_review else root


def curated_root(mp: pytest.MonkeyPatch, dst: Path, *, fresh_review: bool = True) -> Path:
    """T422: THE review-app sandbox — `sandbox_tree(dst)` and `CURATED_ROOT` pointed at it through `mp`, so every
    curated path the process reads is the copy (never a mix). Returns the root; a test reads the copy's paths
    through `CURATED` AFTER this call."""
    from pipeline.common.curated import ROOT_ENV
    root = sandbox_tree(dst, fresh_review=fresh_review)
    mp.setenv(ROOT_ENV, str(root))
    return root


def generated_root(mp: pytest.MonkeyPatch, dst: Path, *names: str) -> Path:
    """T1192: THE generated sandbox, the twin of `curated_root`: `curated.make_generated_root(dst, names)` (the named
    parts of this process's generated tree, none = empty) and `GENERATED_ROOT` pointed at it through `mp`, so every
    generated path the process reads is the copy, never a mix. A test writes its own inputs at the reader's own path
    (`tidal.path()`), asked AFTER this call. Returns the root."""
    from pipeline.common import curated as C
    from pipeline.common.curated import GENERATED_ROOT_ENV, make_generated_root
    mp.setattr(C, "_GENERATED_ROOT_ALLOWED", True)          # the opt-in (`allow_generated_root`), undone with `mp`
    root = make_generated_root(dst, names)
    mp.setenv(GENERATED_ROOT_ENV, str(root))
    return root


def no_served_build(mp: pytest.MonkeyPatch, dst: Path) -> Path:
    """(T777) THE REVIEW APP WITH NO ATLAS TO SERVE, as CI runs it: `CURATION_BUILD` named to a folder under `dst` that
    does not exist, set through `mp` BEFORE the backend is imported (`reuse._BUILD` is read at import). A no-data test
    of the app then never reads this machine's served build (the registry `applied_keys` resolves new points on,
    `splits.resolved.json`); without it the test read the live atlas wherever one was built, and the suite's guard
    (`machine_state`) refuses that. Returns the path."""
    p = Path(dst) / "no-served-atlas"
    mp.setenv("CURATION_BUILD", str(p))
    return p


@pytest.fixture(autouse=True)
def _refused_saves_to_temp(tmp_path, monkeypatch):
    """No test writes the review app's real refused-saves log (owner 2026-10-28)."""
    monkeypatch.setenv("CURATION_REFUSED_LOG", str(tmp_path / "refused-saves.jsonl"))


@pytest.fixture(autouse=True)
def _the_marked_data(request):
    """The safety net under the marker: a test marked `needs_bundle` / `needs_atlas` FAILS with the
    make-command when the bundle / atlas build is absent, even where it reads it without `need`."""
    from pipeline.common.curated import GENERATED
    if request.node.get_closest_marker("needs_bundle"):
        need(request, "bundle", os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite",
             BUNDLE_HINT)
    if request.node.get_closest_marker("needs_atlas"):
        need(request, "atlas", os.environ.get("ATLAS_BUILD") or GENERATED.build(), ATLAS_HINT)


_LIVE_STORES: dict = {}


@pytest.fixture(scope="session")
def live_store_of(tmp_path_factory):
    """THE LIVE REGS STORE, built ONCE per session (T63: test_release and test_regs_store each built it, ~50 s apiece).
    No regs.sqlite is kept (P20), so it is made here from the answers + export pair on disk, as `deliver all` does —
    card-gate stamp required, as there. `of(export_dir, bundle)` -> the store's path; the caller has already `need`ed
    its inputs."""
    def of(export_dir: Path, bundle: Path) -> Path:
        key = (str(export_dir), str(bundle))
        if key not in _LIVE_STORES:
            from pipeline.deliver.store.build import build
            out = tmp_path_factory.mktemp("live-store") / "regs.sqlite"
            build(Path(export_dir) / "ui-rules-answers.json", Path(export_dir), Path(bundle), out)
            _LIVE_STORES[key] = out
        return _LIVE_STORES[key]
    return of


def release_root() -> Path:
    """The release root paired with the suite's bundle: a side set's `deliver all --out-dir` dir (`UI_EXPORT_DIR`)
    writes its releases (and LATEST) to `<dir>/release`; without it, the live `data/release`."""
    from pipeline.common.curated import ROOTS
    side = os.environ.get("UI_EXPORT_DIR")
    return Path(side) / "release" if side else Path(ROOTS.release)


def predates(msg: str) -> None:
    """A bundle built before the ruling this test pins FAILS — the canonical gate and a side build
    alike — unless `UI_EXPORT_ALLOW_PREDATING=1` asks for the old skip."""
    if os.environ.get("UI_EXPORT_ALLOW_PREDATING") == "1":
        pytest.skip(msg)
    pytest.fail(msg)


def one_vintage(atlas: Path, release: Path) -> None:
    """(T1001) A test that resolves on the release under test from the served atlas (the review app's In-season
    resolution: `inseason.resolve` refuses a release not built from its atlas) FAILS with the reason when the two are
    different vintages — the state between an atlas promote and the release that ships it, expected and never a bug:
    one line naming both, and what to run. As `predates` does for an old bundle."""
    import json
    from pipeline.common import section_handles as H
    have = H.digest_for(atlas)
    core = (json.loads((Path(release) / "manifest.json").read_text(encoding="utf-8")).get("files") or {}).get("core") or {}
    rel = str(core.get("section_handles") or "")
    if rel != have:
        pytest.fail(f"atlas build {Path(atlas).name} (section handles {have}) is not the release under test "
                    f"{Path(release).name}'s (tiles {rel or 'unstated'}): between a promote and a release that is "
                    "expected — release it (python -m pipeline release --staging), or point RELEASE_DIR at the "
                    "candidate", pytrace=False)


def part_at(data: dict, item: str, *ends: str) -> int:
    """The index of `item`'s ONE export part with a range ending at every one of `ends` (an
    endpoint label such as "mouth" or "chilliwack_vedder_rivers__vedder_crossing_bridge", or a
    prefix of one ending in "*"): a part found by what it covers, not by its position, so a pin
    survives an atlas rebuild that adds or drops a part elsewhere on the water."""
    def hit(label, want):
        return label.startswith(want[:-1]) if want.endswith("*") else label == want
    got = [i for i, p in enumerate(data["waters"][item]["parts"])
           if all(any(hit(r[0], e) or hit(r[1], e) for r in p[3]) for e in ends)]
    assert len(got) == 1, (item, ends, got, data["waters"][item]["parts"])
    return got[0]


BUNDLE_HINT = "python -m pipeline.deliver (or set UI_EXPORT_BUNDLE to a built bundle)"
EXPORT_HINT = "python -m pipeline.deliver export (or set ANSWERS_EXPORT_DIR to a written export)"
ATLAS_HINT = ("python -m pipeline.atlas.build --full --splits data/curated/waters/cuts.json "
              "--out data/generated/atlas/full (~18 min, AGENTS.md 17)")
REACH_HINT = "the reach step of python -m pipeline.deliver (writes data/generated/reaches/<run>)"
TILES_HINT = "python -m pipeline.deliver.tiles"
GPKG_HINT = "python -m data.fetch_data (the FWA geopackage, not in git)"
ROWS_HINT = "python -m pipeline.regs.extraction.extract_synopsis"
DFO_CACHE_HINT = "python -m pipeline.regs.dfo_salmon.churn"
#: (T667 slice 8) the COMMITTED mini release (`under_test.mini()`): no marker — it is in git, always here — and when it
#: is missing or stale this is what makes it again
MINI_HINT = "python -m pipeline.tools.mini_release --write (--check says whether the committed copy is stale)"
