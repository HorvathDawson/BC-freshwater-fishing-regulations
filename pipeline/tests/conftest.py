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
from pathlib import Path

import pytest

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


def predates(msg: str) -> None:
    """A bundle built before the ruling this test pins FAILS — the canonical gate and a side build
    alike — unless `UI_EXPORT_ALLOW_PREDATING=1` asks for the old skip."""
    if os.environ.get("UI_EXPORT_ALLOW_PREDATING") == "1":
        pytest.skip(msg)
    pytest.fail(msg)


BUNDLE_HINT = "python -m pipeline.deliver (or set UI_EXPORT_BUNDLE to a built bundle)"
EXPORT_HINT = "python -m pipeline.deliver export (or set ANSWERS_EXPORT_DIR to a written export)"
ATLAS_HINT = ("python -m pipeline.atlas.build --full --splits data/curated/waters/splits.json "
              "--out data/generated/atlas/full (~18 min, AGENTS.md 17)")
REACH_HINT = "the reach step of python -m pipeline.deliver (writes data/generated/reaches/<run>)"
TILES_HINT = "python -m pipeline.deliver.tiles"
GPKG_HINT = "python -m data.fetch_data (the FWA geopackage, not in git)"
ROWS_HINT = "python -m pipeline.regs.extraction.extract_synopsis"
DFO_CACHE_HINT = "python -m pipeline.regs.dfo_salmon.churn"
