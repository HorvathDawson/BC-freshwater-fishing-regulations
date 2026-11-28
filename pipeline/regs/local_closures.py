"""WATERS A LOCAL AUTHORITY CLOSES (T981): a city, park board or other local authority's no-fishing water, not the book's.

    python -m pipeline.regs.local_closures --check        # the accepted rows hold their shape (seconds)

THE SOURCE. `CURATED.regulations.local_closures` holds `region-<n>.json` files (`_about`, `region`, `entries`), the
synopsis's own file shape, so it is a second ENTRY SOURCE beside `synopsis` (`parsing.io.entry_sources`): the reach run
binds its entries and the bundle writes them with no other change. Only the owner's ACCEPTED rows live there (memory
`everything-added-is-a-proposal`); the inventory of the others is in the ticket and in
`data/generated/review/t981_local_closures_proposals.json` (read by nothing; removed from git 2026-11-27, in history at 9337319f7).

ONE SHAPE, held by `check_entry`: an entry is one water's whole closure — a water row (`r<n>:<slug>_local@<mu>`, see
below), `matched` exactly one item, `extents` whole, no synopsis page, its `authority` cited (`LocalAuthority`), and ONE
rule: "No Fishing" (`retention_limit`, every game fish, take 0, not to be fished for) whole, `authority: superior`. A
superior authority sits outside the ladder (`bundle.read.source`, rank -1): nothing in the book reopens what the city
closed, the way a national park's closure stands (`zp:superior_closures`).

THE ENTRY ID: a water row's (`r<n>:`), slug ending `_local`. The water kind is what every reader already treats as "this
water's own row" (the reach binding, the ladder's scope, the export's kind) — a new kind would have every one of the ~70
kind tests in the readers to revisit; the `_local` suffix keeps the id apart from any row the book might print for the
same water (`r2:lost_lagoon@2-8`), which `check_against_book` then refuses.

NEVER BESIDE A BOOK ROW (`check_against_book`): a water the synopsis has a water row for is refused. The two would answer
the angler from two documents with no ruling on which; such a water is an owner call, kept in the ticket.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.common import entry_id as EID

#: the directory name under the entries root: the source's name in `parsing.io.entry_sources`
SOURCE = "local_closures"
#: the slug suffix of every entry id here (see the module note)
SLUG_SUFFIX = "_local"
#: the one rule an entry holds, field by field (`rule_id`, `verbatim` and `extents` checked apart)
CLOSURE = {"type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0, "may_target": False,
           "authority": "superior"}
VERBATIM = "No Fishing"
WHOLE = [{"op": "whole"}]


class LocalClosureError(ValueError):
    """A local-closures entry out of its one shape, or beside a book row."""


def root() -> Path:
    """The source directory (`CURATED.regulations.local_closures`). RAISES when it is missing (AGENTS 37): an absent
    source would silently reopen every water it closes."""
    from pipeline.common.curated import CURATED
    d = Path(CURATED.regulations.local_closures)
    if not d.is_dir():
        raise FileNotFoundError(f"{d}: the local-closures source is missing (T981) — restore it from git")
    return d


def check_entry(e: dict, region: str) -> None:
    """One entry's shape (the module note); `region` is its file's. Raises `LocalClosureError` naming the entry."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    eid = e.get("entry_id") or "?"

    def bad(why: str):
        raise LocalClosureError(f"local closure {eid}: {why}")

    ce = CatalogueEntry.model_validate(e)          # the corpus model first (`authority` and pages: `_authority_cites`)
    head = EID.head_of(eid)
    if not EID.is_water_row(head) or EID.region_of(head) != region:
        bad(f"its id must be a Region {region} water row (r{region}:<slug>{SLUG_SUFFIX}@<mu>)")
    if not EID.parse(eid).slug.endswith(SLUG_SUFFIX):
        bad(f"its slug must end {SLUG_SUFFIX!r}, apart from any row the book prints for the water")
    if ce.region != region:
        bad(f"region {ce.region!r} in a region-{region} file")
    if ce.authority is None:
        bad("cites no `authority` (body, instrument, url, quote)")
    if len(ce.matched) != 1:
        bad(f"must match exactly one water (matched {list(ce.matched)})")
    if list(ce.extents) != WHOLE:
        bad(f"closes the whole water (extents {list(ce.extents)})")
    if ce.licensing or ce.see or ce.includes_tributaries:
        bad("holds only its closure: no licensing, pointer or tributaries")
    if len(e.get("rules") or []) != 1:
        bad(f"holds exactly one rule, its closure ({len(e.get('rules') or [])} rules)")
    r = e["rules"][0]
    wrong = {k: r.get(k) for k, v in CLOSURE.items() if r.get(k) != v}
    if wrong or r.get("verbatim") != VERBATIM or (r.get("extents") or []) != WHOLE:
        bad(f"its rule is not the whole-water '{VERBATIM}' closure of a superior authority "
            f"({wrong or {'verbatim': r.get('verbatim'), 'extents': r.get('extents')}})")
    if e.get("regs_verbatim") != VERBATIM:
        bad(f"regs_verbatim must be {VERBATIM!r}")


def check_file(path: Path) -> dict[str, dict]:
    """One region file: its shape and every entry's -> {entry_id: entry}. Raises on a missing or bad file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"no local-closures region file at {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    region = path.stem.split("region-", 1)[1]
    if not data.get("_about"):
        raise LocalClosureError(f"{path}: no `_about` (AGENTS 36a)")
    if str(data.get("region")) != region:
        raise LocalClosureError(f"{path}: `region` {data.get('region')!r} in region-{region}.json")
    if "entries" not in data:
        raise LocalClosureError(f"{path}: no `entries`")
    out: dict[str, dict] = {}
    for e in data["entries"]:
        check_entry(e, region)
        if e["entry_id"] in out:
            raise LocalClosureError(f"{path}: {e['entry_id']} twice")
        out[e["entry_id"]] = e
    return out


def load(d: Path | None = None) -> dict[str, dict]:
    """Every accepted local closure -> {entry_id: entry}, each checked (`check_file`). RAISES on a missing source."""
    d = Path(d) if d is not None else root()
    if not d.is_dir():
        raise FileNotFoundError(f"{d}: the local-closures source is missing (T981) — restore it from git")
    out: dict[str, dict] = {}
    for p in sorted(d.glob("region-*.json")):
        out.update(check_file(p))
    return out


def check_against_book(merged: dict[str, dict], origin: dict[str, str]) -> None:
    """THE GUARD: a water a local closure matches must have no water row from any other source (the synopsis). `merged`
    / `origin`: every entry of every source and the source each came from (`parsing.io.read_all_entries`)."""
    local = {eid: e for eid, e in merged.items() if origin.get(eid) == SOURCE}
    if not local:
        return
    rows: dict[str, list[str]] = {}
    for eid, e in merged.items():
        if origin.get(eid) == SOURCE or not EID.is_water_row(EID.head_of(eid)):
            continue
        for item in e.get("matched") or []:
            rows.setdefault(item, []).append(eid)
    clash = [f"{eid} closes {item}, which the book's {', '.join(rows[item])} already rules"
             for eid, e in sorted(local.items()) for item in e.get("matched") or [] if item in rows]
    if clash:
        raise SystemExit("local closures beside a synopsis row (T981: an owner call, never both):\n  "
                         + "\n  ".join(clash))


def check_source(merged: dict[str, dict], origin: dict[str, str], d: Path | None = None) -> None:
    """What the corpus read (`read_all_entries`) asks: the source is there (RAISES when missing), every one of its
    entries holds its shape, and none sits beside a book row."""
    want = load(d)
    got = {eid for eid, src in origin.items() if src == SOURCE}
    if got != set(want):
        raise SystemExit(f"the corpus read {sorted(got)} from {SOURCE} but the source holds {sorted(want)}")
    check_against_book(merged, origin)


def write_region(region: str, entries: list[dict], *, about: str, source: str, d: Path | None = None) -> dict | None:
    """Write one region file through the curated writer (AGENTS 36b), every entry checked first."""
    from pipeline.common.curated_write import write
    d = Path(d) if d is not None else root()
    path = d / f"region-{region}.json"
    for e in entries:
        check_entry(e, region)
    text = json.dumps({"_about": about, "region": region, "entries": entries}, ensure_ascii=False, indent=1) + "\n"
    return write(path, text, tool="pipeline.regs.local_closures", source=source,
                 validate=lambda t: [check_entry(e, region) for e in json.loads(t)["entries"]] and None)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--check", action="store_true", help="check the accepted rows' shape and print them")
    a = ap.parse_args(argv)
    if not a.check:
        ap.error("--check is the only action")
    got = load()
    for eid, e in got.items():
        print(f"  {eid}  {e['matched'][0]}  closed by {e['authority']['body']} ({e['authority']['instrument']})")
    print(f"ok: {len(got)} local closure(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
