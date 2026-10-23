"""The trust policy is generated, and the generated copies must not drift.

THE FAILURE THIS REPLACES. The rule was implemented three times: `pipeline/gauges/consume/shed.py`
(symmetric ratio, then a watershed-descent gate), `app/packages/core/src/flow.ts`
(asymmetric, no gate, plus a fourth band), and `app/tools/build-fixture.mjs` (its own
`BAND_FLOOR` literal, with `none: 0`). The last one wrote 1,785 `trust = 'none'` rows into
the fixture bundle that three test files then asserted against — so the suite was green
about a contract the province bundle has never satisfied.

Only Python decides a band now. These tests keep the two generated renderings honest.
"""

from __future__ import annotations

import json

import pytest

from pipeline.gauges.consume.shed import TRUST_BANDS
from pipeline.tools import emit_gauge_policy as E


def test_the_generated_typescript_is_committed_and_current():
    assert E.OUT.exists(), f"{E.OUT} missing — run pipeline.tools.emit_gauge_policy"
    assert E.OUT.read_text(encoding="utf-8") == E.render(), (
        "app/packages/core/src/gauge-policy.generated.ts is stale.\n"
        "  run: python -m pipeline.tools.emit_gauge_policy")


def test_the_generated_json_is_committed_and_current():
    # Read by `app/tools/build-fixture.mjs`, which is a plain node script and cannot import
    # the TypeScript package. Two renderings, one source.
    assert E.OUT_JSON.exists(), f"{E.OUT_JSON} missing — run pipeline.tools.emit_gauge_policy"
    assert E.OUT_JSON.read_text(encoding="utf-8") == E.render_json(), (
        "app/packages/core/src/gauge-policy.generated.json is stale.\n"
        "  run: python -m pipeline.tools.emit_gauge_policy")


def test_check_mode_agrees_with_what_is_on_disk(monkeypatch):
    monkeypatch.setattr("sys.argv", ["emit_gauge_policy", "--check"])
    assert E.main() == 0


def test_the_two_renderings_carry_the_same_bands():
    got = json.loads(E.render_json())
    assert got["bands"] == [b for b, _ in TRUST_BANDS]
    assert got["floor"] == {b: f for b, f in TRUST_BANDS}
    for band, floor in TRUST_BANDS:
        assert f'"{band}"' in E.render()
        assert f"{band}: {floor}" in E.render()


def test_no_band_means_no_gauge():
    # A reach nothing is entitled to speak for gets NO ROW. A band spelled `none` is a row
    # asserting a station and then retracting it -- a different and worse claim, and the one
    # the fixture builder used to make 1,785 times.
    assert "none" not in [b for b, _ in TRUST_BANDS]
    assert "none" not in json.loads(E.render_json())["bands"]
    # The union type must not offer it either. (The word appears in the file's prose, saying
    # why it is absent, so this reads the declaration rather than the whole text.)
    union = E.render().split("export type GaugeTrust =")[1].split(";")[0]
    assert "none" not in union


def test_the_temperature_bands_are_the_publishers():
    # P12: the Conditions legend read "under 18 °C / 18–20 °C / 20 °C and over" from literals typed into Shell.tsx,
    # while the publisher classed every reading by `_TEMP_BANDS`. Both renderings now carry the publisher's bands,
    # ceilings in order, the last open-ended (null) -- one place to change when the real thresholds are curated.
    from pipeline.gauges.feed.publish import _TEMP_BANDS
    want = [[name, None if ceiling == float("inf") else ceiling] for ceiling, name in _TEMP_BANDS]
    assert json.loads(E.render_json())["temperature"] == want
    ts = E.render().split("export const TEMP_BANDS")[1].split("];")[0]
    assert ts == (': readonly (readonly [TempBand, number | null])[] = [\n'
                  '  ["cool", 18],\n'
                  '  ["warm", 20],\n'
                  '  ["critical", null],\n')
    union = E.render().split("export type TempBand =")[1].split(";")[0]
    assert union == ' "cool" | "warm" | "critical"'


def test_the_knots_the_panel_floor_and_the_forecast_days_are_the_pipelines():
    """T125 item 6: emitted whole, from their one Python home each; the publisher's own ranking reads the knots from
    the climatology too (mutation: re-type `[0.10, 0.25, ...]` in `percentile_of` and the source pin fails)."""
    import inspect
    from pipeline.atlas.gauges.panel import MIN_TOTAL_WEIGHT
    from pipeline.gauges.feed import publish
    from pipeline.gauges.feed.climatology import PCTILES
    got = json.loads(E.render_json())
    assert (got["knots"], got["minTotalWeight"], got["forecastDays"]) == (
        [0.1, 0.25, 0.5, 0.75, 0.9], 0.05, [1, 3, 5, 10])
    assert [round(k * 100) for k in got["knots"]] == list(PCTILES)
    assert (got["minTotalWeight"], got["forecastDays"]) == (MIN_TOTAL_WEIGHT, list(publish.HORIZONS))
    assert "export const PERCENTILE_KNOTS = [0.1, 0.25, 0.5, 0.75, 0.9] as const;" in E.render()
    assert "export const MIN_TOTAL_WEIGHT = 0.05;" in E.render()
    assert "export const FORECAST_DAYS = [1, 3, 5, 10] as const;" in E.render()
    src = inspect.getsource(publish.percentile_of)
    assert "PCTILES" in src and "0.25" not in src
    assert publish.percentile_of(30, [10, 20, 30, 40, 50]) == 0.5
