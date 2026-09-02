"""BC River Forecast Centre model forecasts — what the river is expected to do next.

WHY THIS IS A SEPARATE SOURCE FROM EVERYTHING ELSE IN `pipeline/hydro`. The rest of this
package reads Environment and Climate Change Canada: an observation is a measurement of
something that happened. A forecast is a MODEL OUTPUT from a different agency (the Province
of British Columbia), it carries its own attribution and disclaimer, and it can be wrong in
ways an observation cannot. Mixing the two into one number would put a prediction and a
reading behind the same word.

THREE MODELS, EACH SEASONAL, AND NONE OF THEM ALWAYS RUNNING:

    CLEVER   freshet, hourly, 10 days   — spring melt, the high-flow question
    COFFEE   fall floods, daily, 5 days — autumn rain
    ELF      low flow, daily, 30 days   — summer drought, the one anglers care about

Outside its season a model publishes nothing, so a station with no forecast today is the
NORMAL case and never an error. Where two models both speak for a station, the one with the
more recent issue time wins — they disagree at the shoulders of their seasons, and the
fresher run is the one the Centre stands behind.

ONE REQUEST PER MODEL, NOT ONE PER STATION. The Centre publishes a per-station CSV as well,
and v1 fetched all of them; that is ~450 requests every half hour against a provincial
endpoint for data that the summary layer already carries. The summary gives the forecast
minimum, average and maximum over the model's horizon, which is what a person actually reads
("it will fall to about 8 m3/s over the next month"). Three requests is polite; four hundred
and fifty is a habit that gets an open dataset closed.

ATTRIBUTION IS NOT OPTIONAL. `ATTRIBUTION` below is the exact string the Province requires,
confirmed with the River Forecast Centre, and it must be displayed verbatim wherever a
forecast appears.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# The Centre's ArcGIS org. Each map hub is one FeatureServer layer of per-station rows.
_ARCGIS = "https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services"

_UA = "BC-freshwater-fishing-regulations/1.0 (+https://canifishthis.ca)"

ATTRIBUTION = (
    "Forecast data provided by the BC River Forecast Centre, "
    "Province of British Columbia, and used under the Province's "
    "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). "
    "Users should use the information on this website with caution and "
    "at their own risk."
)

# model -> where its numbers live in the service's own column names.
#
# `rep` is which of min/ave/max is THE number for that model, and it is not a formatting
# choice: a flood model is asked "how high", a low-flow model "how low". Showing the average
# of an ELF run would smooth away the entire question it exists to answer.
MODELS: dict[str, dict] = {
    "CLEVER": {
        "service": "CLM_MapHub_forecast", "horizon_days": 10, "rep": "max",
        "obs": "Latest_Reading", "issued": "Issued_at",
        "fmax": "Forecast_maximum_in_5_days",
    },
    "COFFEE": {
        "service": "coffee_MapHub_forecast", "horizon_days": 5, "rep": "max",
        "obs": "Latest_Reading", "issued": "Issued_at",
        "fmin": "Forecast_minimum_in_5_days",
        "fave": "Forecast_average_in_5_days",
        "fmax": "Forecast_maximum_in_5_days",
    },
    "ELF": {
        "service": "MapHub_ELF_Forecast", "horizon_days": 30, "rep": "min",
        "obs": "Qobs_m3_s_", "issued": "Issued_at",
        "fmin": "Qfor_MIN_30_Days_m3_s_",
        "fave": "Qfor_AVE_30_Days_m3_s_",
        "fmax": "Qfor_MAX_30_Days_m3_s_",
    },
}


def _num(v) -> float | None:
    """The first number in whatever the service put in the cell.

    These columns are typed as text and arrive as "12.4", "12.4 m3/s", "" and null in the
    same layer. A parse failure is None — never 0.0, which would read as a river that has
    stopped.
    """
    if v is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", str(v))
    return float(m.group()) if m else None


def _issued(v) -> str | None:
    """When this run was made, as ISO-8601 — from whichever of three forms the layer used.

    NOT COSMETIC. Two models overlap at the shoulders of their seasons and the fresher run
    wins, which is a comparison — and comparing "Updated at: 09:38 AM Tue 2026-09-01"
    against "2026-09-02T00:00:00" as strings sorts on the letter U and picks the older run
    every time. Normalising here is what makes that comparison mean what it says.

    An unrecognised form is returned as it stands rather than dropped: a reader is still
    better off seeing the Centre's own words than nothing, and the tie-break falls back to
    model order, which is stated.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, timezone.utc).isoformat(timespec="seconds")
    text = str(v).strip()
    if not text:
        return None
    m = re.search(r"(\d{1,2}):(\d{2})\s*([AP]M).*?(\d{4}-\d{2}-\d{2})", text, re.I)
    if m:
        hour = int(m.group(1)) % 12 + (12 if m.group(3).upper() == "PM" else 0)
        return f"{m.group(4)}T{hour:02d}:{m.group(2)}:00"
    m = re.search(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?", text)
    return m.group(0).replace(" ", "T") if m else text


def _query(service: str, timeout: float = 30.0) -> list[dict]:
    """Every row of one FeatureServer layer, paged."""
    url = f"{_ARCGIS}/{service}/FeatureServer/0/query"
    out: list[dict] = []
    offset, page = 0, 1000
    while True:
        q = urllib.parse.urlencode({
            "where": "1=1", "outFields": "*", "returnGeometry": "false",
            "f": "json", "resultOffset": offset, "resultRecordCount": page,
        })
        req = urllib.request.Request(f"{url}?{q}", headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
        feats = data.get("features", [])
        out.extend(f.get("attributes", {}) for f in feats)
        if len(feats) < page or not data.get("exceededTransferLimit"):
            return out
        offset += page


def fetch(models: list[str] | None = None) -> dict[str, dict]:
    """`{station: forecast}` across every model that is currently running.

    A model that fails is SKIPPED with a note, not raised. This runs inside a 30-minute cron
    beside the observations; losing a seasonal forecast must not cost the map its readings.
    """
    out: dict[str, dict] = {}
    for name in (models or list(MODELS)):
        cfg = MODELS[name]
        try:
            rows = _query(cfg["service"])
        except Exception as exc:                      # noqa: BLE001 — see docstring
            print(f"  {name}: unavailable ({exc})", file=sys.stderr)
            continue
        kept = 0
        for a in rows:
            station = str(a.get("Station_ID") or "").strip()
            if not station:
                continue
            lo = _num(a.get(cfg.get("fmin"))) if cfg.get("fmin") else None
            av = _num(a.get(cfg.get("fave"))) if cfg.get("fave") else None
            hi = _num(a.get(cfg.get("fmax"))) if cfg.get("fmax") else None
            rep = {"min": lo, "ave": av, "max": hi}[cfg["rep"]]
            rep = rep if rep is not None else (hi if hi is not None else av if av is not None else lo)
            if rep is None:
                continue                # a row with no forecast in it is not a forecast
            row = {
                "model": name,
                "issuedAt": _issued(a.get(cfg.get("issued"))),
                "horizonDays": cfg["horizon_days"],
                # The number to show, and which end of the range it is. A client that only
                # printed `value` would say "8 m3/s" for a flood warning and a drought alike.
                "value": rep, "extreme": cfg["rep"],
                "min": lo, "ave": av, "max": hi,
                "observed": _num(a.get(cfg.get("obs"))),
                "unit": "m3/s",
            }
            prev = out.get(station)
            # Two models overlap at the shoulders of their seasons. The fresher run wins;
            # with no issue time to compare, the first model listed keeps the slot rather
            # than being replaced by an arbitrary later one.
            if prev is None or (row["issuedAt"] or "") > (prev["issuedAt"] or ""):
                out[station] = row
            kept += 1
        print(f"  {name:7} {kept:>4} station forecasts")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch BC River Forecast Centre forecasts")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--models", nargs="*", choices=sorted(MODELS))
    a = ap.parse_args()
    got = fetch(a.models)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(
        {"fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "attribution": ATTRIBUTION, "stations": got},
        separators=(",", ":")), encoding="utf-8")
    print(f"wrote {a.out}  ({len(got)} stations)")


if __name__ == "__main__":
    main()
