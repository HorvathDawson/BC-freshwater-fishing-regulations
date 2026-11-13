"""The ONLY writer of the frozen chart -> lake match file: the owner's accept (T634 item 5).

    python -m pipeline.bathymetry.accept --by OWNER --ids 00001BRID 00034SANJ         # accept these proposals
    python -m pipeline.bathymetry.accept --by OWNER --rule clean --evidence equal      # accept a whole bucket
    python -m pipeline.bathymetry.accept --by OWNER --ids 00099LARL --refuse "no such lake"

Reads `GENERATED.base/bathymetry/proposals.json` (pipeline.bathymetry.propose) and appends to the frozen file
`CURATED.base/bathymetry/matches.json` (created by the first accept). Rules (owner, standing):
  * nothing is pre-accepted: a row enters only through this command, with who and when;
  * an accepted row is NEVER rewritten: accepting an id that is already in the file refuses (the gauge lesson, a rerun
    once demoted 25 reviewed gauges); a deliberate change is `--replace`, which prints the old row first;
  * a refusal is a row too (`refused: <reason>`), so the same question is not asked every year;
  * only a BULK_ACCEPTABLE rule (`clean`) may be taken whole with `--rule`; every review rule (variant_circular
    included, owner 2026-11-11) is accepted one id at a time with `--ids`;
  * a proposal whose rule is not `clean` / `not_in_app` and has no single proposed lake needs `--wbk` naming the lake
    the owner chose (two named lakes, unnamed, no lake) — the program never guesses it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence


#: the proposal sets this command accepts from: (proposals file, frozen file). `contours` and `stocking` are built on
#: "Access Only" records (owner says OGL, unconfirmed: T634); their rows carry the licence flag through.
SETS = {"charts": ("bathymetry/proposals.json", "bathymetry/matches.json"),
        "contours": ("bathymetry/contour_proposals.json", "bathymetry/contour_matches.json"),
        "stocking": ("stocking/proposals.json", "stocking/matches.json")}


def frozen_path(which: str = "charts") -> Path:
    from pipeline.common.curated import CURATED
    return Path(CURATED.base) / SETS[which][1]


def proposals_path(which: str = "charts") -> Path:
    from pipeline.common.curated import GENERATED
    return GENERATED.base / SETS[which][0]


def load_frozen(path: Path) -> dict:
    if not path.exists():
        return {"_about": "Chart -> lake matches ACCEPTED by the owner (T634). Written ONLY by "
                          "`python -m pipeline.bathymetry.accept`; the build and the app read it and match nothing "
                          "themselves. Rows keep the chart's titles as printed.",
                "matches": []}
    return json.loads(path.read_text())


def accept(frozen: dict, proposals: Sequence[dict], ids: Sequence[str], *, by: str, today: str,
           refuse: Optional[str] = None, wbk: Optional[str] = None, replace: bool = False) -> List[str]:
    """Append rows for `ids`; returns the log lines. Raises on anything that would rewrite or guess."""
    by_id: Dict[str, dict] = {p["chart_id"]: p for p in proposals}
    have = {m["chart_id"]: i for i, m in enumerate(frozen["matches"])}
    log = []
    for cid in ids:
        p = by_id.get(cid)
        if p is None:
            raise SystemExit(f"{cid}: not in the proposals file")
        if cid in have and not replace:
            raise SystemExit(f"{cid}: already accepted ({frozen['matches'][have[cid]]}); --replace to change it")
        if refuse:
            wbks: List[str] = []
        elif wbk:
            wbks = [wbk]
        elif p["wbks"] and p["rule"] in ("clean", "not_in_app"):
            wbks = list(p["wbks"])
        elif p["wbks"]:
            wbks = list(p["wbks"])
            log.append(f"{cid}: accepting a {p['rule']} proposal as proposed ({wbks})")
        else:
            raise SystemExit(f"{cid}: rule {p['rule']} proposes no lake; name it with --wbk wbk:<key>")
        row = {"chart_id": cid, "wbks": wbks, "gnis_id": p.get("gnis_id") if not wbk else None,
               "titles": p["titles"], "rule": p["rule"], "evidence": p["evidence"],
               "accepted_by": by, "accepted_on": today}
        if p.get("licence"):
            row["licence"] = p["licence"]
        if refuse:
            row["refused"] = refuse
        if cid in have:
            log.append(f"{cid}: REPLACING {frozen['matches'][have[cid]]}")
            frozen["matches"][have[cid]] = row
        else:
            frozen["matches"].append(row)
        log.append(f"{cid}: {'refused' if refuse else 'accepted'} {wbks}")
    frozen["matches"].sort(key=lambda m: m["chart_id"])
    return log


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="accept chart -> lake proposals into the frozen file (owner only)")
    ap.add_argument("--by", required=True)
    ap.add_argument("--set", default="charts", choices=sorted(SETS), help="which proposals: charts | contours | stocking")
    ap.add_argument("--ids", nargs="*", default=[])
    ap.add_argument("--rule", help="accept every proposal with this rule …")
    ap.add_argument("--evidence", help="… and this name evidence (equal, word_order, variant, contains)")
    ap.add_argument("--refuse", help="record a refusal with this reason instead")
    ap.add_argument("--wbk", help="the lake the owner chose (wbk:<key>), for a proposal that names none")
    ap.add_argument("--replace", action="store_true")
    a = ap.parse_args(argv)
    props = json.loads(proposals_path(a.set).read_text())["proposals"]
    ids = list(a.ids)
    if a.rule:
        from pipeline.bathymetry.propose import BULK_ACCEPTABLE
        if a.rule not in BULK_ACCEPTABLE:
            raise SystemExit(f"--rule {a.rule}: only {sorted(BULK_ACCEPTABLE)} is accepted in bulk; "
                             "the owner reviews the rest one id at a time (--ids)")
        ids += [p["chart_id"] for p in props if p["rule"] == a.rule
                and (a.evidence is None or p["evidence"].get("name_evidence") == a.evidence)]
    if not ids:
        raise SystemExit("nothing named: --ids or --rule")
    path = frozen_path(a.set)
    frozen = load_frozen(path)
    for line in accept(frozen, props, ids, by=a.by, today=dt.date.today().isoformat(), refuse=a.refuse,
                       wbk=a.wbk, replace=a.replace):
        print(line)
    from pipeline.common.curated_write import write_any       # T600: atomic, backed up and logged
    write_any(path, json.dumps(frozen, indent=1, ensure_ascii=False) + "\n", tool="pipeline.bathymetry.accept",
              by=a.by, source=f"{a.set}: {len(ids)} id(s)")
    print(f"wrote {path} ({len(frozen['matches'])} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
