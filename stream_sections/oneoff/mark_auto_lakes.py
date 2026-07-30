"""One-off: mark lake_split locators that are FULLY handled by the universal combine-phase
lake split (docs/04) as status `auto`, and flag the offset/mixed ones that still need an
authored point. Saves after EVERY row (incremental).

Per docs/04: a BLK is cut at its lake fids automatically, so "X River upstream/downstream of
Y Lake" sections fall out for free and the reg attaches by location_identifier — NO authored
split is required. That applies to reaches bounded ONLY by lake edges (and natural mouths /
the auto border split). Reaches with an OFFSET ("N km below the outlet") or a second non-lake
endpoint (boundary signs) still need an authored point, so they stay `todo`.

Run:  .venv/bin/python -m stream_sections.oneoff.mark_auto_lakes
"""

from __future__ import annotations

import json
from pathlib import Path

_DOC = Path("stream_sections/docs/14-locators-to-curate.json")

_AUTO_TAG = ("[auto-lake-split] Bounded only by lake edge(s) (and/or natural mouth / the auto "
            "border split). The combine phase already cuts the BLK at the lake, so the reg "
            "attaches via location_identifier per docs/04 — NO authored split needed.")

# Fully handled by the universal lake split -> status `auto`
AUTO = [
    "ash-river-f11aa9", "big-bar-creek-ce0182", "bonaparte-river-98b62b",
    "bridge-river-3fa485", "chimney-creek-04b2dc", "columbia-river-73fe32",
    "davie-river-f757c8", "deadman-river-aa98fb", "eagle-river-a52a4f",
    "guichon-creek-9a81b6", "mckinley-creek-9d6931", "moberly-river-b2a8ec",
    "nahatlatch-river-1d1eaf", "nahatlatch-river-0bb710", "nahmint-river-66b328",
    "nahmint-river-786f4c", "nanaimo-river-76bf11", "nicola-river-aba357",
    "nicola-river-4b9f64", "paul-creek-downstream-of-paul-lake-b809c0",
    "pennask-creek-c98c41", "pitt-river-564cfe",
    "seton-river-includes-bc-hydro-power-canal-ups-5f4cbe",
    "seton-river-includes-bc-hydro-power-canal-ups-8eb4d1", "seymour-river-db4eb4",
    "shuswap-river-521556", "silverhope-silver-creek-015132",
    "thompson-river-upstream-of-kamloops-lake-e0c70b", "whatshan-river-bcc368",
    "fran-ois-lake-9c8274", "harrison-river-from-the-fraser-river-upstream-f8ecc6",
    "kootenay-river-downstream-of-idaho-border-c2529b",
]

# Lake bound is auto, but the reach ALSO needs an authored point -> stays todo, just note it.
NEEDS_POINT = {
    "forsyth-creek-3adc4a": "OFFSET: lake bound (Connor Lake outlet) is auto, but the +3 km "
        "downstream point needs an authored point (lake anchor + offset, or a point).",
    "jewel-creek-b23cd8": "OFFSET: Jewel Lake outlet is auto; the ~1.5 km downstream point needs authoring.",
    "yakoun-river-eace82": "OFFSET: Yakoun Lake outlet is auto; the downstream offset point needs authoring.",
    "zymoetz-copper-river-f226e7": "OFFSET: McDonell Lake outlet is auto; the ~3 km downstream point needs authoring.",
    "ruby-creek-ad1f2c": "MIXED: Ruby Lake outlet is auto; the downstream boundary-signs point needs authoring.",
    "atnarko-bella-coola-rivers-includes-tributari-985001": "MIXED: Tenas Lake edge is auto; the "
        "downstream boundary-signs point needs authoring.",
}


def main() -> None:
    doc = json.loads(_DOC.read_text())
    by_id = {x["id"]: x for x in doc["locators"]}

    auto_n = point_n = missing = 0
    for lid in AUTO:
        x = by_id.get(lid)
        if x is None:
            print(f"  ! missing {lid}")
            missing += 1
            continue
        if x["status"] == "auto":
            continue
        x["status"] = "auto"
        if _AUTO_TAG not in (x.get("notes") or ""):
            x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + _AUTO_TAG
        _DOC.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
        auto_n += 1

    for lid, note in NEEDS_POINT.items():
        x = by_id.get(lid)
        if x is None:
            print(f"  ! missing {lid}")
            missing += 1
            continue
        tag = f"[lake-bound-auto] {note}"
        if tag not in (x.get("notes") or ""):
            x["notes"] = (x["notes"] + " — " if x.get("notes") else "") + tag
            _DOC.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
            point_n += 1

    print(f"marked {auto_n} auto, annotated {point_n} needs-point, {missing} missing")


if __name__ == "__main__":
    main()
