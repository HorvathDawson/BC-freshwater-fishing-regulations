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
    from pathlib import Path

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
