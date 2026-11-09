"""AS CI SEES IT (fast-fail, AGENTS 20a): a pytest plugin (`-p pipeline.tests.ci_no_build`) that hides this machine's
builds — the atlas, the bundle, the reach runs, the tiles — so a test that reads one without its `needs_*` marker fails
here as it does on CI (no data there; the marked tiers are deselected).

caught (2026-11-09, CI run 42717971280): T522 resolved the live atlas before the chain fakes' `step_declared` stub ran
(13 deliver tests), and the review app's 503 test read the registry before the failure it pins; every local run passed
because the build is here."""
from __future__ import annotations

import os

_GONE = "/nonexistent/ci-no-build"

for _k in ("ATLAS_BUILD", "UI_EXPORT_BUNDLE"):
    os.environ.pop(_k, None)
os.environ["CURATION_BUILD"] = f"{_GONE}/atlas/full"

from pipeline.common.curated import GENERATED  # noqa: E402

_g = GENERATED._loader()                      # the cached tree every reader gets; frozen, so set past pydantic
object.__setattr__(_g.atlas, "builds", type(_g.atlas.builds)(f"{_GONE}/atlas"))
for _f in ("bundle", "reaches", "tiles"):
    object.__setattr__(_g, _f, type(getattr(_g, _f))(f"{_GONE}/{_f}"))
assert not GENERATED.build().exists() and GENERATED.atlas.builds == _g.atlas.builds
