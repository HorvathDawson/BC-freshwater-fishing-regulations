"""Facts that more than one module needs must have ONE definition.

Not a style rule. Each of these was declared twice, and in every case the two copies
disagreed about something a user would see:

  * the mainstem edge set decided which water a rule reaches;
  * name normalisation decided whether two spellings were one water, and the tile folded
    ~3,000 shouting duplicates the bundle kept — so the same query returned different
    results depending on which index answered it.
"""

from __future__ import annotations

import pathlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_mainstem_edge_kinds_is_defined_once():
    """What counts as "still the same river", in one place.

    This used to check that TWO tributary modules both imported the canonical set rather
    than declaring their own. There is one module now — `atlas/reach/tributaries.py` was
    absorbed into `atlas/graph/tributaries.py` — so the check is that the surviving one
    imports it and that no module anywhere writes the literal again.
    """
    from pipeline.atlas.graph.tributaries import MAINSTEM_EDGE_KINDS as a
    from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS as canonical

    assert a is canonical, "the tributary module is redeclaring the mainstem set"

    # This file included, which is why it is excluded — the search string appears in the
    # assertion below it.
    declared = [p for p in (ROOT / "pipeline").rglob("*.py")
                if "archive" not in p.parts
                and p != pathlib.Path(__file__).resolve()
                and 'frozenset({"continuation"' in p.read_text()]
    assert declared == [ROOT / "pipeline/common/models/graph.py"], \
        f"the mainstem set is declared in {[str(p) for p in declared]}"


def test_there_is_one_tributary_module():
    """`atlas/reach/tributaries.py` is gone, and nothing should reintroduce it.

    It was a second answer to "which water does this rule cover" — the pipeline's most
    expensive question — missing the Strahler guard that stopped McLennan Creek absorbing
    20.6% of British Columbia, and it had no production caller. A tributary walk is a graph
    operation; there is one, and it lives in the graph package.
    """
    assert not (ROOT / "pipeline/atlas/reach/tributaries.py").exists()
    assert (ROOT / "pipeline/atlas/graph/tributaries.py").exists()


def test_the_bundle_normalises_names_the_way_the_tiles_do():
    src = (ROOT / "pipeline/deliver/bundle/build.py").read_text()
    assert "from pipeline.deliver.tiles.names import normalise" in src, (
        "the bundle is comparing names raw again; the tile folds case and punctuation, so "
        "the two indexes would disagree about what counts as a distinct name")


def test_normalise_folds_the_case_that_caused_it():
    from pipeline.deliver.tiles.names import normalise

    # The actual bug: the registry carries both cases of ~3,000 names.
    assert normalise("EAST WHITE RIVER") == normalise("East White River")
    assert normalise("Chilliwack  River") == normalise("Chilliwack River")


def test_normalise_does_not_yet_fold_a_possessive():
    """A known limit, pinned so it is a decision rather than a surprise.

    Punctuation becomes a SPACE, so "St. Mary's Lake" and "St Marys Lake" are two names to
    this function. They are one lake to a person, and someone typing either should find it.
    Changing it would change the tile's search haystack as well as the bundle's — which is
    now the point: there is one function, so it is one decision, made once.
    """
    from pipeline.deliver.tiles.names import normalise

    assert normalise("St. Mary's Lake") != normalise("St Marys Lake")


def test_the_trust_bands_agree_across_the_language_boundary():
    """The pipeline decides a band; the app decides what a band MEANS. They must not each
    decide where a band starts.

    THIS TEST WAS RIGHT AND NOT ENOUGH, which is worth recording. It pinned `TRUST_FLOOR` in
    `flow.ts` to `TRUST_BANDS` in `shed.py`, and it passed the whole time the app was ALSO
    carrying `gaugeTrust()` — a second implementation that banded `reach / gauge`
    asymmetrically, with no watershed gate — and `build-fixture.mjs` was carrying a third
    with a `none: 0` band the pipeline has never written. Equal thresholds, three different
    rules. Pinning the CONSTANTS never had a chance of catching that.

    So the floors are no longer written by hand on the TypeScript side at all: they are
    GENERATED from `TRUST_BANDS` by `pipeline.tools.emit_gauge_policy`, and
    `pipeline/tests/test_gauge_policy.py` fails if the committed output is stale. What is
    left here is the assertion that the app still gets its floors from that generated file
    and has not quietly reintroduced a literal.
    """
    import re

    from pipeline.gauges.consume.shed import TRUST_BANDS

    core = Path(__file__).resolve().parents[2] / "app/packages/core/src"
    if not core.exists():                       # the pipeline may be checked out alone
        import pytest

        pytest.skip("app/ not present")

    flow = (core / "flow.ts").read_text(encoding="utf-8")
    assert "gauge-policy.generated" in flow, (
        "flow.ts no longer reads the generated policy — if the floors moved, they must "
        "still come from pipeline/gauges/consume/shed.py via emit_gauge_policy")
    assert "function gaugeTrust" not in flow, (
        "a second implementation of the trust rule is back in flow.ts. The app reads "
        "`section_gauge.trust`; it cannot band anything itself, because the drainage gate "
        "needs FWA watershed codes that never ship to a client.")

    m = re.search(r"TRUST_FLOOR\s*=\s*\{([^}]*)\}",
                  (core / "gauge-policy.generated.ts").read_text(encoding="utf-8"))
    assert m, "TRUST_FLOOR is gone from the generated policy"
    ts = {k: float(v) for k, v in re.findall(r"(\w+)\s*:\s*([\d.]+)", m.group(1))}

    assert ts == {band: floor for band, floor in TRUST_BANDS}
