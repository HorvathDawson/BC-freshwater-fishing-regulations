"""One-off: classify the 63 `lake` todo locators (lake_split / whole_lake / lake_internal /
reservoir / multi_lake / mis-bucketed) and write each row back to disk IMMEDIATELY.

Incremental: the doc is saved after EVERY row, so an interrupted run keeps all completed rows.
Idempotent: rows already carrying a `[lake-class: ...]` note are skipped.

Run:  .venv/bin/python -m stream_sections.oneoff.classify_lakes
"""

from __future__ import annotations

import json
from pathlib import Path

_DOC = Path("stream_sections/docs/14-locators-to-curate.json")

# id -> (klass, new_status, new_anchor_kind|None, new_resolver_hint|None, note)
# lake_split  -> stays todo (needs wbk lookup); note names the bounding lake.
# whole_lake  -> not_applicable (matched by lake name, no cut).
# lake_internal / reservoir -> deferred (lake-internal & reservoir reaches unsupported, docs/15).
# multi_lake  -> not_applicable (set-of-lakes / whole-watershed, matched by area).
# mis-bucket  -> reclassify anchor_kind + keep todo.
PLAN: dict[str, tuple[str, str, str | None, str | None, str]] = {
    # ---- lake_split: the named lake's polygon edge splits the stream (author a `lake` anchor)
    "ash-river-f11aa9": ("lake_split", "todo", None, None,
        "bounded by Elsie Lake (up) and Dickson Lake (down); author two `lake` anchors."),
    "big-bar-creek-ce0182": ("lake_split", "todo", None, None, "outlet of Big Bar Lake."),
    "bonaparte-river-98b62b": ("lake_split", "todo", None, None, "outlet of Bonaparte Lake."),
    "bridge-river-3fa485": ("lake_split", "todo", None, None, "head of Downton Lake (reservoir)."),
    "chimney-creek-04b2dc": ("lake_split", "todo", None, None, "outlet of Brunson Lake."),
    "columbia-river-73fe32": ("lake_split", "todo", None, None,
        "bounded by Mud Lake and Columbia Lake; two `lake` anchors."),
    "davie-river-f757c8": ("lake_split", "todo", None, None, "outlet of Schoen Lake."),
    "deadman-river-aa98fb": ("lake_split", "todo", None, None, "outlet of Mowich Lake."),
    "eagle-river-a52a4f": ("lake_split", "todo", None, None, "outlet of Griffin Lake."),
    "forsyth-creek-3adc4a": ("lake_split", "todo", None, None,
        "Connor Lake outlet + 3 km offset downstream (lake anchor + offset)."),
    "guichon-creek-9a81b6": ("lake_split", "todo", None, None, "outlet of Mamit Lake."),
    "jewel-creek-b23cd8": ("lake_split", "todo", None, None,
        "Jewel Lake outlet + ~1.5 km offset downstream."),
    "mckinley-creek-9d6931": ("lake_split", "todo", None, None, "outlet of McKinley Lake."),
    "moberly-river-b2a8ec": ("lake_split", "todo", None, None, "outlet of Moberly Lake."),
    "nahatlatch-river-1d1eaf": ("lake_split", "todo", None, None, "outlet of Nahatlatch Lake."),
    "nahatlatch-river-0bb710": ("lake_split", "todo", None, None, "outlet of Nahatlatch Lake."),
    "nahmint-river-66b328": ("lake_split", "todo", None, None, "inlet/head of Nahmint Lake."),
    "nahmint-river-786f4c": ("lake_split", "todo", None, None,
        "up & downstream of Nahmint Lake — two lake edges."),
    "nanaimo-river-76bf11": ("lake_split", "todo", None, None,
        "westernmost of the two Nanaimo (First/Second) lakes — that lake edge."),
    "nicola-river-aba357": ("lake_split", "todo", None, None, "inlet/head of Nicola Lake."),
    "nicola-river-4b9f64": ("lake_split", "todo", None, None, "outlet of Nicola Lake."),
    "paul-creek-downstream-of-paul-lake-b809c0": ("lake_split", "todo", None, None, "outlet of Paul Lake."),
    "pennask-creek-c98c41": ("lake_split", "todo", None, None, "inlet/head of Pennask Lake."),
    "pitt-river-564cfe": ("lake_split", "todo", None, None, "head/inlet of Pitt Lake."),
    "seton-river-includes-bc-hydro-power-canal-ups-5f4cbe": ("lake_split", "todo", None, None, "outlet of Seton Lake."),
    "seton-river-includes-bc-hydro-power-canal-ups-8eb4d1": ("lake_split", "todo", None, None, "outlet of Seton Lake."),
    "seymour-river-db4eb4": ("lake_split", "todo", None, None, "outlet of Seymour Lake."),
    "shuswap-river-521556": ("lake_split", "todo", None, None, "head/inlet of Sugar Lake."),
    "silverhope-silver-creek-015132": ("lake_split", "todo", None, None, "head/inlet of Silver Lake."),
    "thompson-river-upstream-of-kamloops-lake-e0c70b": ("lake_split", "todo", None, None, "head/inlet of Kamloops Lake."),
    "whatshan-river-bcc368": ("lake_split", "todo", None, None, "head/inlet of Whatshan Lake."),
    "yakoun-river-eace82": ("lake_split", "todo", None, None,
        "Yakoun Lake outlet + offset downstream."),
    "harrison-river-from-the-fraser-river-upstream-f8ecc6": ("lake_split", "todo", None, None,
        "Harrison Lake edge (up) + Fraser confluence (down) — lake anchor + confluence."),
    "kootenay-river-downstream-of-idaho-border-c2529b": ("lake_split", "todo", None, None,
        "Kootenay Lake edge (up) + Idaho border (down) — authored split kootenay_idaho_border already covers the border."),
    "ruby-creek-ad1f2c": ("lake_split", "todo", None, None,
        "Ruby Lake outlet (up) + boundary signs (down) — lake anchor + point."),
    "fran-ois-lake-9c8274": ("lake_split", "todo", None, None,
        "at the outlet of Francois Lake (map-described) — lake outlet edge."),
    "zymoetz-copper-river-f226e7": ("lake_split", "todo", None, None,
        "McDonell Lake outlet + ~3 km offset downstream (lake anchor + offset)."),
    "atnarko-bella-coola-rivers-includes-tributari-985001": ("lake_split", "todo", None, None,
        "Tenas Lake edge (up) + boundary signs (down) — lake anchor + point."),

    # ---- whole_lake: quota/bait/gear only, matched by lake name, no cut
    "high-lake-unnamed-lake-approx-4-km-north-of-b-0ec39d": ("whole_lake", "not_applicable", None, None,
        "whole-lake quota/bait reg, matched by (unnamed) lake — no cut."),
    "teepee-lake-adjacent-to-west-road-river-a62e7b": ("whole_lake", "not_applicable", None, None,
        "whole-lake reg on Teepee Lake — no cut."),
    "downton-lake-reservoir-73beda": ("whole_lake", "not_applicable", None, None,
        "whole reservoir (Downton Lake) — matched by name, no cut."),
    "idlewild-lake-old-cranbrook-reservoir-85e69f": ("whole_lake", "not_applicable", None, None,
        "whole lake (Idlewild / old Cranbrook Reservoir) — no cut."),
    "grizzly-lake-unnamed-lake-approx-4-5-km-upstr-81a537": ("whole_lake", "not_applicable", None, None,
        "whole (unnamed) lake — matched by description, no cut."),

    # ---- lake_internal: a bridge/line dividing a LAKE (docs/15 lake-internal split, unsupported)
    "mara-lake-70c173": ("lake_internal", "deferred", None, None,
        "lake-internal divide at the CPR bridge (docs/15) — unsupported."),
    "prudhomme-lake-south-of-the-hwy-16-bridge-eccc4b": ("lake_internal", "deferred", None, None,
        "lake-internal divide at the Hwy 16 bridge (docs/15) — unsupported."),
    "rosemond-lake-061373": ("lake_internal", "deferred", None, None,
        "lake-internal divide at the CPR bridge (docs/15) — unsupported."),
    "pitt-lake-47a388": ("lake_internal", "deferred", None, None,
        "lake-internal line at boundary signs (docs/15) — unsupported."),
    "premier-lake-895b0d": ("lake_internal", "deferred", None, None,
        "lake-internal line at boundary signs (docs/15) — unsupported."),
    "shannon-lake-netted-off-portion-on-the-south--c375a0": ("lake_internal", "deferred", None, None,
        "netted-off lake portion (docs/15 lake-internal) — unsupported."),
    "shumway-lake-02ae86": ("lake_internal", "deferred", None, None,
        "lake-internal line at boundary signs (docs/15) — unsupported."),
    "mahood-lake-see-map-on-page-28-for-area-closu-79ec2d": ("lake_internal", "deferred", None, None,
        "lake-internal area closure within boundary signs (see map, docs/15) — unsupported."),
    "chilko-lake-83dc6f": ("lake_internal", "deferred", None, None,
        "lake-internal (Big Lagoon, west side of Chilko Lake) — unsupported."),

    # ---- reservoir: reservoir zone/reach between landmarks (complex)
    "upper-arrow-lake-drawdown-area-f15974": ("reservoir", "deferred", None, None,
        "reservoir drawdown reach (Hwy 1 bridge <-> Akolkolex Narrows power line) — complex."),
    "williston-lake-in-zone-a-includes-waters-500--456568": ("reservoir", "deferred", None, None,
        "reservoir zone (Williston Zone A, ±500 m Causeway Rd) — complex."),
    "williston-lake-in-zone-a-includes-waters-500--7972e6": ("reservoir", "deferred", None, None,
        "reservoir zone (Williston Zone A) — complex."),
    "williston-lake-in-zone-a-includes-waters-500--3be97e": ("reservoir", "deferred", None, None,
        "reservoir zone (Williston Zone A) — complex."),

    # ---- multi_lake / whole-watershed / vague map: matched by area, no single cut
    "bluey-lake-potholes-d4ca7c": ("multi_lake", "not_applicable", None, None,
        "set of unnamed lakes within 2 km of Bluey Lake — area membership, no cut."),
    "unnamed-lakes-located-immediately-north-and-s-3aca70": ("multi_lake", "not_applicable", None, None,
        "set of unnamed lakes N/S of Bluey Lake — area membership, no cut."),
    "liard-river-watershed-see-map-on-page-63-772040": ("multi_lake", "not_applicable", None, None,
        "whole Liard watershed (all lakes & streams, see map) — no single cut."),
    "shuswap-lake-see-maps-on-page-28-includes-lit-80ee82": ("multi_lake", "deferred", None, None,
        "multi-basin (Shuswap + Little Shuswap + arms, see maps) — needs map-driven splits."),
    "echoes-lake-near-kimberley-4adc24": ("multi_lake", "deferred", None, None,
        "'from both lakes' (Echoes Lake pair) — ambiguous, needs map."),

    # ---- mis-bucketed: not a lake at all -> reclassify
    "asher-creek-4b601a": ("mis_bucket", "todo", "confluence", "tributary",
        "NOT a lake: 'downstream of South Fork' is a fork confluence, not a lake reach."),
    "thompson-river-downstream-of-signs-at-kamloop-38fc02": ("mis_bucket", "todo", "point", "boundary_signs",
        "NOT a lake cut: boundary signs at Kamloops Lake outlet on the Thompson — a point."),
}


def main() -> None:
    doc = json.loads(_DOC.read_text())
    by_id = {x["id"]: x for x in doc["locators"]}

    applied = skipped = 0
    counts: dict[str, int] = {}
    for lid, (klass, status, kind, hint, note) in PLAN.items():
        x = by_id.get(lid)
        if x is None:
            print(f"  ! missing id {lid}")
            continue
        if "[lake-class:" in (x.get("notes") or ""):
            skipped += 1
            continue
        x["status"] = status
        if kind:
            x["anchor_kind"] = kind
        if hint:
            x["resolver_hint"] = hint
        tag = f"[lake-class: {klass}] {note}"
        x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + tag
        _DOC.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")  # save AFTER each row
        applied += 1
        counts[klass] = counts.get(klass, 0) + 1

    print(f"applied {applied}, skipped {skipped} (already classified)")
    for k in sorted(counts):
        print(f"  {k:14s} {counts[k]}")
    total = len(PLAN)
    print(f"plan covers {total}/63 lake rows")


if __name__ == "__main__":
    main()
