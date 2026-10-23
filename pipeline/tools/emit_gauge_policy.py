"""Emit the gauge trust policy as TypeScript, so the app cannot hold a second opinion.

    python -m pipeline.tools.emit_gauge_policy          # write it
    python -m pipeline.tools.emit_gauge_policy --check  # fail if it is stale (CI)

WHY THIS EXISTS. The trust rule was implemented twice. `pipeline/gauges/consume/shed.py` banded the
SYMMETRIC ratio `min(mag)/max(mag)` and then refused any reach outside the gauge's watershed;
`app/packages/core/src/flow.ts` banded the ASYMMETRIC `reach/gauge` with no drainage gate at
all, plus a fourth band the pipeline has no concept of. The app's own fixture used the second
one, so the suite asserted bands the province bundle would never produce — the Slesse Creek
failure, still live on the TypeScript side of the boundary, months after it was fixed on the
Python side.

WHAT IS SHARED AND WHAT IS NOT, because "unify" does not mean "port the function".

    the ALGORITHM   Python only. It needs stream magnitudes for two million nodes and the
                    FWA watershed codes to test drainage. A client has neither, and the one
                    time it pretended to, it invented answers. It is not shared because it
                    CANNOT be shared -- the client's job is to read the band the pipeline
                    already decided, never to re-derive it.

    the POLICY      Both. The band names and their floors are the vocabulary the two sides
                    have to agree on: the pipeline writes `fair` into a column and the app
                    renders a sentence for `fair`. That agreement is what this file makes
                    mechanical instead of remembered.

So changing the algorithm is a one-file change in `shed.py`. Changing the BANDS changes
`TRUST_BANDS` there, and this regenerates the TypeScript from it; `--check` in CI is what
stops the two drifting again.

THE TEMPERATURE BANDS ride along (P12). The publisher classes every water-temperature reading by
`_TEMP_BANDS` in `pipeline/gauges/feed/publish.py` and the map colours the dots by that class; the
legend words the same ceilings (`TEMP_LEGEND` in core) instead of typing 18 and 20 a second time.

THE EXPECTED-CHANGE POLICY rides along too (T52). The publisher measures each run's change against named thresholds and
carries the ratio coded for the donor pathway (`pipeline/gauges/feed/change.py`); the app decodes that number, words it
against the same thresholds and colours it on stops placed at them. Typed twice, a "2x" in the words could sit beside a
colour that turns at 2.5x.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.gauges.consume.shed import TRUST_BANDS
from pipeline.gauges.feed import change as _change
from pipeline.gauges.feed.publish import _TEMP_BANDS
from pipeline.common.curated import REPO_ROOT

ROOT = REPO_ROOT
OUT = ROOT / "app" / "packages" / "core" / "src" / "gauge-policy.generated.ts"

#: The same constant as JSON, for the build tools. `app/tools/build-fixture.mjs` is a plain
#: node script and cannot import a workspace TypeScript package, and the literal it used to
#: carry instead was a THIRD copy of the vocabulary — the one that invented a `none: 0` band
#: and wrote 1,785 rows of it into the fixture bundle. Two renderings, one source.
OUT_JSON = ROOT / "app" / "packages" / "core" / "src" / "gauge-policy.generated.json"


def _temp_bands() -> list[tuple[str, float | int | None]]:
    """The publisher's `_TEMP_BANDS` as (name, ceiling) in order; the open-ended last band's ceiling is None.

    P12: the Conditions legend typed 18 and 20 into `Shell.tsx` while the publisher classed every reading by these. The
    legend now reads them from here, so the words and the dots' colours cannot answer from two different thresholds.
    Integral ceilings are written as integers so the TypeScript reads `18`, not `18.0`.
    """
    out: list[tuple[str, float | int | None]] = []
    for ceiling, name in _TEMP_BANDS:
        c = None if ceiling == float("inf") else (int(ceiling) if float(ceiling).is_integer() else ceiling)
        out.append((name, c))
    return out


def _num(v: float | int) -> float | int:
    """An integral value as an integer, so the TypeScript reads `2`, not `2.0`."""
    return int(v) if float(v).is_integer() else v


def _change_policy() -> dict:
    """The publisher's expected-change constants, under the names the app reads them by."""
    return {"days": _change.CHANGE_DAYS, "riseRatio": _num(_change.RISE_RATIO),
            "fallRatio": _num(_change.FALL_RATIO), "bigPoints": _num(_change.BIG_POINTS),
            "spikeBack": _num(_change.SPIKE_BACK), "scale": _num(_change.CHANGE_SCALE),
            "codeFloor": _num(_change.CODE_FLOOR), "staleHours": _num(_change.STALE_HOURS),
            "order": list(_change.CHANGE_ORDER)}


def render_json() -> str:
    import json
    return json.dumps({
        "_generated_by": "python -m pipeline.tools.emit_gauge_policy",
        "_source": "pipeline/gauges/consume/shed.py :: TRUST_BANDS; "
                   "pipeline/gauges/feed/publish.py :: _TEMP_BANDS; "
                   "pipeline/gauges/feed/change.py :: the change policy",
        "bands": [b for b, _ in TRUST_BANDS],
        "floor": {b: f for b, f in TRUST_BANDS},
        "temperature": [[b, c] for b, c in _temp_bands()],
        "change": _change_policy(),
    }, indent=2) + "\n"


def render() -> str:
    """The TypeScript, byte for byte."""
    names = " | ".join(f'"{b}"' for b, _ in TRUST_BANDS)
    floors = "\n".join(f"  {b}: {f}," for b, f in TRUST_BANDS)
    bands = "\n".join(f'  ["{b}", {f}],' for b, f in TRUST_BANDS)
    temp = _temp_bands()
    temp_names = " | ".join(f'"{b}"' for b, _ in temp)
    temp_rows = "\n".join(f'  ["{b}", {"null" if c is None else c}],' for b, c in temp)
    cp = _change_policy()
    order = ", ".join(f'"{m}"' for m in cp["order"])
    change_rows = "\n".join(f"  {k}: {v}," for k, v in cp.items() if k != "order")
    return f'''// GENERATED by `python -m pipeline.tools.emit_gauge_policy`. Do not edit.
//
// The source of truth is `TRUST_BANDS` in pipeline/gauges/consume/shed.py, which is also where the
// algorithm lives. The app never computes a band: it reads the one the bundler wrote into
// `section_gauge.trust`. These constants exist so the two sides agree on the VOCABULARY --
// what the words are and where the floors sit -- not so the rule can be run twice.

/** The bands the pipeline can write. A reach below the last floor gets NO ROW at all,
 *  which is why there is no "none" here: absence is the fourth outcome. */
export type GaugeTrust = {names};

/** Fraction of the gauge's own drainage that a reach accounts for, at each band's floor. */
export const TRUST_FLOOR = {{
{floors}
}} as const;

/** The same thing in order, worst floor last -- for rendering a scale or a legend. */
export const TRUST_BANDS: readonly (readonly [GaugeTrust, number])[] = [
{bands}
];

// The water-temperature classes, from `_TEMP_BANDS` in pipeline/gauges/feed/publish.py -- the publisher writes one
// onto every temperature reading (`temperatureBand`), and the Conditions legend words them (`TEMP_LEGEND` in core).
// POLICY awaiting curation, not physics, and never a citation of a closure order.

/** A temperature class the publisher can write. */
export type TempBand = {temp_names};

/** Each band holds readings BELOW its ceiling in °C, in order; the last is open-ended (null). */
export const TEMP_BANDS: readonly (readonly [TempBand, number | null])[] = [
{temp_rows}
];

// The expected-change policy (T52), from pipeline/gauges/feed/change.py. The publisher measures each forecast run's
// largest move over `days` against these thresholds and carries the ratio coded as Φ(log2(ratio) / scale), kept inside
// (codeFloor, 1 − codeFloor), so it rides the donor panels; the app decodes, words and colours it by the same numbers.
export const CHANGE_POLICY = {{
{change_rows}
  order: [{order}],
}} as const;
'''


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="exit 1 if the committed file differs (for CI)")
    a = ap.parse_args()

    stale = [(p, want) for p, want in ((OUT, render()), (OUT_JSON, render_json()))
             if not p.exists() or p.read_text(encoding="utf-8") != want]
    if not stale:
        print(f"gauge policy up to date: {OUT.name}, {OUT_JSON.name}")
        return 0
    if a.check:
        for p, _ in stale:
            print(f"STALE: {p.relative_to(ROOT)} does not match "
                  "pipeline/gauges/consume/shed.py :: TRUST_BANDS / "
                  "pipeline/gauges/feed/publish.py :: _TEMP_BANDS / "
                  "pipeline/gauges/feed/change.py", file=sys.stderr)
        print("  run: python -m pipeline.tools.emit_gauge_policy", file=sys.stderr)
        return 1
    for p, want in stale:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(want, encoding="utf-8")
        print(f"wrote {p.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
