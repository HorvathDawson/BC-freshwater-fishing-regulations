"""Page v36's data blocks, cut from a RELEASE: the input the page oracle (`page-oracle.mjs`) runs page_v36.js over.

    .venv/bin/python app/tools/regs-recut/page_blocks.py --release data/release/r-… [--taps app/fixtures/golden/taps.json]
    -> stdout: {"data": {...}, "answers": {...}}

T667 slice 5b (owner D1, 2026-11-11): the page oracle is no longer page v36's FROZEN data (`page_v36.html` #data /
#answers, which needed a hand amendment per approved ruling: the retired `v36-rulings.json`), but page v36's own CODE
run over the release under test. This module is the Python half of that: it reads the release through the Python
reference decoder (`pipeline.deliver.store.release.province_store`: `Store.answers()` is the answers file, held to its
SHA-256; `Store.export_subset()` the export slice the app reads) — never through the app's TypeScript read path, which
is what the parities test — and writes the two blocks the page reads:

  data     `rules` (keyed "entry::rule"), `rid` (rix -> key: the export's `rule_ids` order, the store's rule.rix),
           `licensing` / `lid` likewise, `species`, `licences`, `entries`, `conduct`, and `waters`: the golden waters
           (taps.json's items) with each part's rules as `[[rix, "reach" | "trib"]]` from the part's ruleset — the
           page's `D.waters[].parts[].rules`, in the export's part order (= the answers' part index)
  answers  the answers file as the release holds it (every section, verbatim)

Nothing is re-interned or re-ordered: the page's own decode reads the release's values. Read-only; no network.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.release import province_store  # noqa: E402


def blocks(release: Path, items: list[str]) -> dict:
    s = province_store(release)
    A = s.answers()
    X = s.export_subset()
    ix = {k: i for i, k in enumerate(X["rule_ids"])}
    waters = []
    for item in items:
        w = X["waters"].get(item)
        if w is None:
            raise SystemExit(f"page_blocks: the release has no water {item}")
        parts = []
        keys = A["parts"].get(item) or []
        for p in w["parts"]:
            if p.get("ruleset") is None:
                # an unkeyed part (tidal water, T278): no rules, and the answers hold no key for it either
                if len(keys) > len(parts) and keys[len(parts)] is not None:
                    raise SystemExit(f"page_blocks: {item} part {len(parts)} has no ruleset but answers key {keys[len(parts)]}")
                parts.append(None)
                continue
            rs = X["rulesets"][p["ruleset"]]
            unknown = set(rs) - {"sections", "reach", "trib"}
            if unknown:
                raise SystemExit(f"page_blocks: ruleset {p['ruleset']} has fields {sorted(unknown)} this cut does not know")
            # the export drops an empty list (AGENTS 58: defaults dropped), so an absent `trib` is a ruleset with none
            rules = sorted([(ix[k], "reach") for k in rs.get("reach", [])] + [(ix[k], "trib") for k in rs.get("trib", [])])
            if len({r for r, _ in rules}) != len(rules):
                raise SystemExit(f"page_blocks: {item} part {len(parts)}: a rule both reach and trib in ruleset {p['ruleset']}")
            parts.append({"rules": [list(r) for r in rules]})
        if len(parts) != len(A["parts"].get(item) or []):
            raise SystemExit(f"page_blocks: {item}: {len(parts)} export parts, {len(A['parts'].get(item) or [])} answers parts")
        waters.append({"id": item, "name": w["name"], "kind": w["kind"], "parts": parts})
    data = {
        "about": {"release": release.name},
        "rules": X["rules"], "rid": {str(i): k for k, i in ix.items()},
        "licensing": X["licensing"], "lid": {str(i): k for i, k in enumerate(X["licensing_ids"])},
        "licences": X["licences"], "species": X["species"], "entries": X["entries"], "conduct": X["conduct"],
        "waters": waters,
    }
    return {"data": data, "answers": A}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", required=True, help="the release folder (the release under test)")
    ap.add_argument("--taps", default=str(REPO / "app/fixtures/golden/taps.json"))
    a = ap.parse_args()
    taps = json.loads(Path(a.taps).read_text())
    items = sorted({t["item"] for t in taps["states"].values()})
    json.dump(blocks(Path(a.release), items), sys.stdout, separators=(",", ":"))


if __name__ == "__main__":
    main()
