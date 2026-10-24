"""Build the T67 braid-cut review page (owner checks every cut on a phone).

    python3 handoff/PLAN/braids/v2/page/build_page.py  ->  handoff/PLAN/braids/v2/page/braid_cuts.html

Reads ../cuts.json and ../ditches.json (the build lane's review data) and inlines a slimmed copy into the template.
Marks are saved to the artifact's db (collection "marks", one doc per item), the same way the slough page does.
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
V2 = HERE.parent


def r5(o):
    if isinstance(o, float):
        return round(o, 5)
    if isinstance(o, list):
        return [r5(x) for x in o]
    if isinstance(o, dict):
        return {k: r5(v) for k, v in o.items()}
    return o


def rule_row(r):
    return {"rule": r.get("rule_id"), "label": r.get("label") or "", "delta": round(r.get("delta_m") or 0),
            "causes": {k: round(v) for k, v in (r.get("delta_by_cause_m") or {}).items() if round(v)}}


def signature(c):
    """What the owner looked at: the gate, the braid cut points and each piece's side."""
    return json.dumps([c["gate"], sorted(map(tuple, (b.get("point") or [] for b in c.get("braid_cuts") or []))),
                       sorted((p.get("section"), p.get("side")) for p in c.get("pieces") or [])])


def slim_cut(c, before):
    sig = signature(c)
    return r5({
        "why": c.get("why") or "", "changed": before.get(c["split_id"]) not in (None, sig) if before else False,
        "id": c["split_id"], "label": c.get("label") or c["split_id"], "water": c.get("water") or "",
        "source": c.get("source"), "gate": c["gate"], "anchor": c.get("anchor_type"),
        "pt": c.get("cut_point"), "line": c.get("cut_line") or [], "host": c.get("host") or [],
        "pieces": [{"name": p.get("name") or "", "side": p.get("side"), "len": round(p.get("length_m") or 0),
                    "line": p.get("line") or []} for p in c.get("pieces") or []],
        "lens": {k: round(v) for k, v in (c.get("length_m") or {}).items()},
        "cuts": [b.get("point") for b in c.get("braid_cuts") or []],
        "straddle": bool(c.get("residual_straddle")),
        "rules": [rule_row(r) for r in c.get("rules") or [] if r.get("changed") or round(r.get("delta_m") or 0)],
        "rules_all": len(c.get("rules") or []),
    })


def main():
    d = json.loads((V2 / "cuts.json").read_text())
    dd = json.loads((V2 / "ditches.json").read_text())
    prev = V2 / "cuts.r2.json"     # the round the owner marked: a card whose cut moved since is flagged
    before = {}
    if prev.exists():
        for c in json.loads(prev.read_text())["cuts"]:
            before[c["split_id"]] = signature(c)
    cuts = [slim_cut(c, before) for c in d["cuts"]]
    ditches = [r5({"id": x["section"], "name": x.get("name") or "(unnamed)", "len": round(x.get("length_m") or 0),
                   "rules": len(x.get("rules_before") or []),
                   "named_rules": [r for r in x.get("rules_before") or [] if not r.startswith("z")],
                   "line": x.get("line") or x.get("geometry") or []}) for x in dd["ditches"]]
    lc = d["length_cuts"]
    data = {
        "cuts": cuts, "ditches": ditches,
        "length": {k: lc.get(k) for k in ("old", "new", "unchanged", "moved", "dropped", "added",
                                          "braid_spanned_junctions_skipped")}
        | {"sweep_braid_cuts": lc.get("sweep_on_length_cuts", {}).get("braid_cuts"),
           "home_moves": lc.get("region_home_moves", {})},
        "residual": d.get("residual_straddles", []),
        "gate_failures": d.get("reach_gate_failures", []),
        "gauge_gone": [{"id": g["split_id"], "why": g.get("gate_now")} for g in d.get("gauge_cuts_no_longer_made", [])],
    }
    blob = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    html = (HERE / "page_template.html").read_text().replace("/*__DATA__*/null", blob)
    out = HERE / "braid_cuts.html"
    out.write_text(html)
    print(f"{out} {out.stat().st_size / 1e6:.2f} MB · {len(cuts)} cuts · {len(ditches)} ditches")


if __name__ == "__main__":
    main()
