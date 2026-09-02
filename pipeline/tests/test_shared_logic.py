"""Facts that more than one module needs must have ONE definition.

Not a style rule. Each of these was declared twice, and in every case the two copies
disagreed about something a user would see:

  * the mainstem edge set decided which water a rule reaches;
  * name normalisation decided whether two spellings were one water, and the tile folded
    ~3,000 shouting duplicates the bundle kept — so the same query returned different
    results depending on which index answered it.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_mainstem_edge_kinds_is_defined_once():
    from pipeline.graph.tributaries import _MAINSTEM_EDGE_KINDS as a
    from pipeline.models.graph import MAINSTEM_EDGE_KINDS as canonical
    from pipeline.reach.tributaries import MAINSTEM_EDGE_KINDS as b

    assert a is canonical and b is canonical, "a module is redeclaring the mainstem set"

    for mod in ("pipeline/graph/tributaries.py", "pipeline/reach/tributaries.py"):
        src = (ROOT / mod).read_text()
        assert "frozenset({\"continuation\"" not in src, f"{mod} declares its own copy"


def test_the_bundle_normalises_names_the_way_the_tiles_do():
    src = (ROOT / "pipeline/bundle/build.py").read_text()
    assert "from pipeline.tiles.names import normalise" in src, (
        "the bundle is comparing names raw again; the tile folds case and punctuation, so "
        "the two indexes would disagree about what counts as a distinct name")


def test_normalise_folds_the_case_that_caused_it():
    from pipeline.tiles.names import normalise

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
    from pipeline.tiles.names import normalise

    assert normalise("St. Mary's Lake") != normalise("St Marys Lake")


def test_the_trust_bands_agree_across_the_language_boundary():
    """The pipeline decides a band; the app decides what a band MEANS. They must not each
    decide where a band starts.

    `pipeline/hydro/shed.py` writes `trust` into the bundle. `app/packages/core/src/flow.ts`
    has `TRUST_FLOOR` and re-derives the same band from raw magnitudes for anything the
    bundle did not precompute. Two copies of one set of thresholds is exactly the drift
    that made a gauge speak for the wrong river in v1, so this pins them together — if you
    move a floor, move both and this test tells you which one you forgot.
    """
    import re
    from pathlib import Path

    from pipeline.hydro.shed import TRUST_BANDS

    flow = Path(__file__).resolve().parents[2] / "app/packages/core/src/flow.ts"
    if not flow.exists():                       # the pipeline may be checked out alone
        import pytest

        pytest.skip("app/ not present")

    m = re.search(r"TRUST_FLOOR\s*=\s*\{([^}]*)\}", flow.read_text(encoding="utf-8"))
    assert m, "TRUST_FLOOR is gone from flow.ts — find where the app puts the floors now"
    ts = {k: float(v) for k, v in re.findall(r"(\w+)\s*:\s*([\d.]+)", m.group(1))}

    assert ts == {band: floor for band, floor in TRUST_BANDS}
