"""One-off migration: collapse locator `anchor_kind` to split-anchor types, add
`resolver_hint`, drop `split_id` (folding any authored-split link into `notes`).

Run once:  .venv/bin/python -m stream_sections.oneoff.migrate_curation_schema
Idempotent: re-running on an already-migrated file is a no-op.
"""

from __future__ import annotations

import json
from pathlib import Path

_DOC = Path("stream_sections/docs/14-locators-to-curate.json")
_SPLITS = Path("stream_sections/splits.json")

# old anchor_kind -> (new anchor_kind == split anchor.type, resolver_hint)
KIND_MAP = {
    "coordinate": ("point", "coordinate"),
    "falls_canyon_obstacle": ("point", "falls_obstacle"),
    "dam_weir_fence": ("point", "dam_weir_fence"),
    "bridge_road_km": ("point", "bridge_road_km"),
    "boundary_signs_generic": ("point", "boundary_signs"),
    "confluence_tributary": ("confluence", "tributary"),
    "lake_reach": ("lake", "lake_reach"),
    "lake_inlet_outlet": ("lake_io", "inlet_outlet"),
    "radius_buffer": ("buffer", "radius"),
    "line_between_signs": ("line", "between_signs"),
    "area_park_polygon": ("area_boundary", "park_polygon"),
    "except_negative": ("not_a_split", "except_negative"),
    "map_or_vague": ("not_a_split", "map_or_vague"),
    "other_reach": ("unclassified", "other_reach"),
}

# new anchor_kind -> maps to which split anchor.type (docs surfaced in-file)
ANCHOR_KINDS_DOC = {
    "point": "split anchor.type=point (coord cut perpendicular to the mainstem)",
    "confluence": "split anchor.type=confluence (cut at a tributary mouth, keyed on tributary_wsc)",
    "line": "split anchor.type=line (author >=2 endpoints)",
    "lake": "split anchor.type=lake (lake polygon boundary; often already splits the BLK)",
    "area_boundary": "split anchor.type=area_boundary (polygon boundary, e.g. a park)",
    "mu_boundary": "split anchor.type=mu_boundary (shared line between two MUs)",
    "lake_io": "NEW lake_io op (immediate lake_inlets union lake_outlets) — not yet built",
    "buffer": "NEW buffer op (point + radius) — not yet built",
    "not_a_split": "NOT a curated split: whole-water / tributary-SET / EXCEPT-exclusion / map-only",
    "unclassified": "reach whose split type is not yet decided — pick one during curation",
}

RESOLVER_HINTS_DOC = {
    "coordinate": "exact coord already in the reg text (DMS/UTM) — easiest",
    "falls_obstacle": "point via FISS obstacles layer (falls/canyon)",
    "dam_weir_fence": "point at a dam/weir/counting fence (infrastructure)",
    "bridge_road_km": "point at a bridge / road / km marker",
    "boundary_signs": "point located from fishing boundary signs on a map",
    "tributary": "confluence at the named tributary's mouth",
    "lake_reach": "the subject is a lake (whole-lake / lake-split / lake-internal)",
    "inlet_outlet": "immediate inlet/outlet streams of a lake",
    "radius": "point plus a radius buffer",
    "between_signs": "a line drawn between two boundary signs",
    "park_polygon": "a park/protected-area polygon boundary",
    "except_negative": "an EXCEPT / negative exclusion, not a positive cut",
    "map_or_vague": "no clean anchor — map-only / vague wording",
    "other_reach": "generic reach; resolver/type still to be decided",
}

# desired key order for each locator row
_ORDER = [
    "id", "name_verbatim", "region", "mus", "src", "locator_text",
    "full_regulation", "anchor_kind", "resolver_hint", "status",
    "target", "coord", "label", "notes",
]


def _reorder(row: dict) -> dict:
    out = {k: row[k] for k in _ORDER if k in row}
    for k, v in row.items():  # keep any stragglers at the end
        if k not in out:
            out[k] = v
    return out


def main() -> None:
    doc = json.loads(_DOC.read_text())
    split_ids = {
        s["id"]
        for s in json.loads(_SPLITS.read_text())["splits"]
        if isinstance(s, dict) and "id" in s
    }

    migrated = 0
    linked = 0
    for x in doc["locators"]:
        old = x.get("anchor_kind")
        if old in KIND_MAP:
            new_kind, hint = KIND_MAP[old]
            x["anchor_kind"] = new_kind
            x["resolver_hint"] = hint
            migrated += 1
        elif "resolver_hint" not in x:
            x["resolver_hint"] = ""

        sid = x.pop("split_id", "")
        if sid:
            # fold the authored-split link into notes as provenance
            for token in [s.strip() for s in sid.replace("+", " ").split()]:
                if token in split_ids:
                    note = f"authored split: {token}"
                    if note not in (x.get("notes") or ""):
                        x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + note
                        linked += 1

    doc["locators"] = [_reorder(x) for x in doc["locators"]]

    # refresh top-level docs
    schema = doc.get("_schema", {})
    schema.pop("split_id", None)
    schema["anchor_kind"] = "split anchor.type this locator maps to; see anchor_kinds"
    schema["resolver_hint"] = "how to FIND the point (falls_obstacle/dam_weir_fence/...); see resolver_hints"
    doc["_schema"] = schema
    doc["anchor_kinds"] = ANCHOR_KINDS_DOC
    doc["resolver_hints"] = RESOLVER_HINTS_DOC

    _DOC.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    print(f"migrated {migrated} rows; folded {linked} split links into notes")


if __name__ == "__main__":
    main()
