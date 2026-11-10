"""T224: WRITE THE ACCEPTED MINED NAMES (`data/curated/waters/found_names.json`) from the candidates + the owner's
decisions. Deterministic; rerun after the owner marks more rows in the review app's Names tab.

Accepted = (the auto-accepted rows, owner 2026-10-28) + (the owner's single-row calls, `waters/found_name_calls.json`
via `names.owner_calls`, T327) + (every row whose latest name mark is verified `accept`) − (every row whose latest name mark is verified `reject`). The name
marks' decisions are computed in memory from marks.jsonl (`review.export.load`, T408).

Each accepted row becomes a `name_variants`-shaped entry: its target and one name per source, each with its REAL
source code, label, ref and evidence (`names.name_records`). Nothing else is written.

    python -m pipeline.tools.mined_names [--check]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pipeline.common.curated import CURATED
from pipeline.regs.review import marks as M
from pipeline.regs.review import names as N


def decisions(decided, stale: list | None = None) -> dict[str, tuple[str, str]]:
    """candidate id -> (call, who) from the latest VERIFIED, non-stale name mark — the marks' decisions
    (`review.export.load`; a marks path is decided in memory first). A mark whose candidate is gone or changed
    decides nothing and is appended to `stale` as {target, why} (T545: said on its item, as a stale slough is)."""
    from pipeline.regs.review import export as E
    if not isinstance(decided, E.Decided):
        decided = E.decide(M.load(decided))
    out = {}
    for m in decided.scope("name"):
        if m.scope != "name" or m.verdict != "verified" or m.call not in ("accept", "reject"):
            continue
        now = N.prints_of(m.target)
        st = None if now is None else M.status(m, M.Prints.model_validate(now))
        if st is None or st["status"] != "verified":
            if stale is not None:
                stale.append({"target": m.target, "why": "candidate gone (not in the name candidates)" if st is None
                              else "stale: " + ", ".join(st["reasons"])})
            continue                      # stale: the candidate changed since the mark
        out[m.target] = (m.call, f"owner mark {m.date} ({m.reviewer})")
    return out


def stale(decided) -> list[dict]:
    """The accepted names that are NOT written, each said on its item: a name mark whose candidate is gone or changed,
    and an accepted name the atlas cannot apply (`NOT_APPLIED`) — [{target, why}]."""
    got: list[dict] = []
    build(decided, got)
    return got


#: T545 item 7: the atlas hangs a found name on BLUE LINES only (`atlas.registry.mined.add_mined_names` reads
#: `target.blks`); a name accepted on a lake polygon or a registry item would be written and never applied
NOT_APPLIED = ("accepted, but NOT written: the atlas applies a found name to blue lines only, and this one is on {what} "
               "(`registry.mined.add_mined_names`) — it would never show")


def build(decided, skipped: list | None = None) -> list[dict]:
    """The entries to write. `skipped` collects every accepted name that is not written, as {target, why} (`stale`)."""
    doc = N.load()
    marked = decisions(decided, skipped)
    owner = N.owner_calls()
    entries = []
    for i in sorted(doc["by_id"]):
        c = doc["by_id"][i]
        call, who = marked.get(i, (None, ""))
        if call == "reject":
            continue
        if call == "accept":
            by = who
        elif i in owner and owner[i]["call"] == "accept":
            by = owner[i]["why"]
        elif i in owner and owner[i]["call"] == N.HOLD:
            continue                                      # owner 2026-10-29: held, not applied
        elif c.get("auto_accept_eligible"):
            by = N.COVERAGE_ACCEPTED
        else:
            continue
        tgt = N.target_entry(c)
        if not tgt:
            continue
        if "blks" not in tgt:
            if skipped is not None:
                skipped.append({"target": i, "why": NOT_APPLIED.format(
                    what="a lake polygon" if "wbks" in tgt else "a registry item")})
            continue
        entries.append({"target": tgt, "names": N.name_records(i, c, call_by=by)})
    return entries


#: (owner 2026-10-28) the names whose source is OpenStreetMap are offered under the ODbL; the record rides in the file.
LICENCE_APPLIES_TO = ("every `names[]` record whose `source` is \"osm\" (its name, target and evidence). Records with "
                      "another `source` are not OpenStreetMap data: `municipal` names are under the city's own open "
                      "government licence, `gauge` names under the Open Government Licence – Canada.")


#: (T324) the file's header: what it holds, who writes it, who reads it
ABOUT = ("THE FOUND NAMES (was mined_names.json; T324): water names found in city stream layers, OpenStreetMap and ECCC "
         "gauges that the owner accepted (an auto rule, or a Names-tab mark / found_name_calls.json call); each name "
         "carries its real `source`. PROMOTED: written only by `python -m pipeline.tools.mined_names` and the review "
         "app's write-through (`pipeline.regs.review.apply`) — never edit by hand; change found_name_calls.json or mark "
         "the candidate instead. Read by the atlas build (`load_name_variants`, CURATED.waters.found_names) as name "
         "variants on their waters.")


def document(got: list[dict]) -> dict:
    """The file: the header, the ODbL record, then the entries (readers take them through `licence.entries`)."""
    from pipeline.common.licence import odbl
    return {"_about": ABOUT, "_licence": odbl(LICENCE_APPLIES_TO), "entries": got}


def main(argv: list[str]) -> int:
    # T545: ONE WRITE PATH — the names domain of `review.apply` (atomic, utf-8, the shrink guard, dropped rows named)
    from pipeline.regs.review import apply as AP
    if "--check" in argv:
        got = AP.run(["names"], dry_run=True)
        same = got["rc"] == 0 and not got["files"]
        print("found_names.json is current" if same else
              "found_names.json is OUT OF DATE — python -m pipeline.tools.mined_names")
        return 0 if same else 1
    return AP.run(["names"])["rc"]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
