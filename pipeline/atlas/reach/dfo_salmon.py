"""THE DFO SALMON LOCATIONS, PLACED (T211): every curated DFO location with an authored extent, resolved to sections by
the SAME `build_reach` the provincial rules go through (`entries.to_reach_input` shapes it; nothing here re-implements a
cut, a walk or a clip).

    data/curated/regulations/entries/dfo_salmon/region-*.json   the curated geography (read only)
      └─ to_reach_input → build_reach ─▶ <reach run>/dfo_salmon.jsonl.gz   one line per location

Written beside the reach run's tables and OUTSIDE the rules' digest (like `tidal`, `steelhead_presence`): a DFO location
is not a provincial rule, and adding the salmon to a run must not move the rules' digest. The bundle reads it
(`pipeline.deliver.salmon`) and never resolves again.

A LOCATION WITH NO EXTENT IS WRITTEN TOO, with `sections: []` and `mapped: false` — the app says "location not yet mapped"
for it on the card, and never draws it. A VARIANT (`duplicate_of`, the same reach reworded) takes its target's extent and
water when it has none of its own: it is the same place.

`regional=False`: a DFO row's extent is its own words (a region default says `within area:region:6`, a closure
`within_area`), and a water's own row applies along the whole water (user ruling 2026-09-25) — the provincial region
limit is a provincial rule's, not DFO's.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

FILE = "dfo_salmon.jsonl.gz"


def _slugs() -> List[str]:
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    return [s for s in ALL_SLUGS if not PAGES[s].is_stub]


def resolve_all(registry, graph, *, entries_dir: Optional[Path] = None, outside=None) -> List[dict]:
    """One row per curated location, sorted by location_id: `{location_id, region, mapped, outcome, reason, sections}`."""
    from pipeline.atlas.reach.build import build_reach
    from pipeline.atlas.reach.models import Outcome
    from pipeline.atlas.reach.outside import outside_bc
    from pipeline.regs.dfo_salmon import entries as E

    if outside is None:
        outside = outside_bc(registry, graph)
    rows: List[dict] = []
    for slug in _slugs():
        ef = E.load(slug, entries_dir or E.ENTRIES_DIR, registry=registry)
        by_id = {x.location_id: x for x in ef.locations}
        for loc in ef.locations:
            target = loc
            if not loc.binding.resolved and loc.is_variant and loc.duplicate_of in by_id:
                target = by_id[loc.duplicate_of]
            water = ef.water(target.water_id) if target.water_id else None
            row = {"location_id": loc.location_id, "region": slug, "mapped": False,
                   "outcome": "no_extent", "reason": None, "sections": []}
            if target.binding.resolved:
                entry, rules = E.to_reach_input(target, [{}], water)
                entry = {**entry, "entry_id": loc.location_id}
                binding, _ = build_reach(entry, rules[0], registry, graph, outside=outside, regional=False)
                row["outcome"] = binding.outcome.value
                if binding.outcome is Outcome.bound:
                    row["mapped"] = True
                    row["sections"] = sorted(binding.sections)
                else:
                    row["reason"] = binding.reason.value if binding.reason else None
            rows.append(row)
    rows.sort(key=lambda r: r["location_id"])
    return rows


def write(out_dir: Path, rows: Iterable[dict]) -> str:
    """Write `dfo_salmon.jsonl.gz` (gzip mtime 0: byte-identical reruns) and return its digest."""
    body = "".join(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
    raw = body.encode("utf-8")
    p = Path(out_dir) / FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(gzip.compress(raw, compresslevel=6, mtime=0))
    return hashlib.sha256(raw).hexdigest()[:16]


def read(run_dir: Path) -> Iterator[dict]:
    """The run's DFO placements. RAISES when the run predates them — a run with no file is not a run with no salmon."""
    p = Path(run_dir) / FILE
    if not p.exists():
        raise SystemExit(f"{p} is missing — the reach run predates the DFO salmon placements (T211). Re-run the "
                         f"reach builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {run_dir}")
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def entries_digest(entries_dir: Optional[Path] = None) -> str:
    """The curated DFO entry files by content — the reach stage's key must move when a curator places a location."""
    from pipeline.regs.dfo_salmon import entries as E
    root = Path(entries_dir or E.ENTRIES_DIR)
    h = hashlib.sha256()
    for p in sorted(root.glob("region-*.json")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]
