"""T224: WRITE THE ACCEPTED MINED NAMES (`data/curated/waters/mined_names.json`) from the candidates + the owner's
decisions. Deterministic; rerun after the owner marks more rows in the review app's Names tab.

Accepted = (the auto-accepted rows, owner 2026-10-28) + (the owner's single-row decisions, `names.OWNER`) + (every
row whose latest name mark is verified `accept`) − (every row whose latest name mark is verified `reject`).

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


def decisions(marks_path: Path) -> dict[str, tuple[str, str]]:
    """candidate id -> (call, who) from the latest VERIFIED, non-stale name mark."""
    out = {}
    for m in M.latest(M.load(marks_path)).values():
        if m.scope != "name" or m.verdict != "verified" or m.call not in ("accept", "reject"):
            continue
        now = N.prints_of(m.target)
        if now is None or M.status(m, M.Prints.model_validate(now))["status"] != "verified":
            continue                      # stale: the candidate changed since the mark
        out[m.target] = (m.call, f"owner mark {m.date} ({m.reviewer})")
    return out


def build(marks_path: Path) -> list[dict]:
    doc = N.load()
    marked = decisions(marks_path)
    entries = []
    for i in sorted(doc["by_id"]):
        c = doc["by_id"][i]
        call, who = marked.get(i, (None, ""))
        if call == "reject":
            continue
        if call == "accept":
            by = who
        elif i in N.OWNER and N.OWNER[i]["call"] == "accept":
            by = N.OWNER[i]["why"]
        elif c.get("auto_accept_eligible"):
            by = "auto 2026-10-28 (owner rule: high confidence, one candidate, no rival)"
        else:
            continue
        tgt = N.target_entry(c)
        if not tgt:
            continue
        recs = N.name_records(i, c, call_by=by)
        if refused(recs):
            continue                      # (owner 2026-10-28) a slur is never a name we ship, whoever accepted it
        entries.append({"target": tgt, "names": recs})
    return entries


def refused(records: list[dict]) -> list[str]:
    """The names in `records` the deny-list refuses (`pipeline.common.offensive_names`)."""
    from pipeline.common.offensive_names import offensive
    return [r["name"] for r in records if offensive(r.get("name"))]


#: (owner 2026-10-28) the names whose source is OpenStreetMap are offered under the ODbL; the record rides in the file.
LICENCE_APPLIES_TO = ("every `names[]` record whose `source` is \"osm\" (its name, target and evidence). Records with "
                      "another `source` are not OpenStreetMap data: `municipal` names are under the city's own open "
                      "government licence, `gauge` names under the Open Government Licence – Canada.")


def document(got: list[dict]) -> dict:
    """The file: the ODbL record, then the entries (readers take them through `licence.entries`)."""
    from pipeline.common.licence import odbl
    return {"_licence": odbl(LICENCE_APPLIES_TO), "entries": got}


def main(argv: list[str]) -> int:
    from pipeline.common.curated import CURATED as C
    marks_path = C.regulations.review.marks
    out = CURATED.waters.mined_names
    got = build(marks_path)
    text = json.dumps(document(got), indent=1, ensure_ascii=False) + "\n"
    if "--check" in argv:
        same = out.read_text() == text
        print("mined_names.json is current" if same else "mined_names.json is OUT OF DATE")
        return 0 if same else 1
    out.write_text(text)
    print(f"{len(got)} accepted names -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
