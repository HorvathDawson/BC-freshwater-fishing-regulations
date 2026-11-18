"""T841: EVERY CLASSIFIED WATER THE BOOK PRINTS SHIPS CLASSIFIED, WITH ITS CLASSIFIED WATERS LICENCE — a gate (owner
2026-11-17 20:27: "make sure the classified water symbols are maintained properly … Make sure we capture all classified
waters whether labeled by symbol or by tributary of something").

    python -m pipeline.regs.classified_waters --check                     # the corpus against the book (instant)
    python -m pipeline.regs.classified_waters --check --bundle [PATH]     # + the built bundle (~15 s)
    python -m pipeline.regs.classified_waters --extraction PATH           # the book table against an extraction

    exit 0: every book-classified water ships classified; 1: a gap, each named with its printed page; 2: could not run

THE BOOK (`BOOK`, below): every row of the 2025-27 synopsis's water tables that is a Classified Water — printed with the
classified-water glyph ("CW", p.6: "A (CW) symbol means that this is a Classified [water]"), or printing "Class I/II
water" without it (the West Road, RULINGS Li10) — with the classes it prints, whether each reaches the tributaries (the
row's ✱, "[Includes Tributaries]" on the class phrase, or "including tributaries"), and its PRINTED page. "Tributaries
of X" classifications are rows too: "ELK RIVER'S TRIBUTARIES" (p.36: "ALL tributaries … are Class II waters") is
`tributaries="only"`, and every "including tributaries" is `tributaries=True`. A glyph row that prints no class of its
own (a pointer — "A tributary of Wigwam River; see Wigwam River", "See Ksi X'anmas River" — or a closed part) names the
row whose designation classifies it (`via`). The rows that print "NOT a Classified Water" are `NOT_CLASSIFIED`. Cut by
hand from the extraction (`synopsis_raw_data.json`: the glyph is its `Classified` symbol) and read against the PDF,
2026-11-17 (T841 audit); the p.33 map (Region 4's hatched Class II waters) and p.34 / p.42 (the region pages) list no
water the tables do not. `--extraction` re-reads an extraction against it (a new edition, an extractor change).

THE CORPUS CHECK (`corpus_problems`, instant — fast-fail and the release preflight):
  * every BOOK row has its entry (region + printed name), with the page among its `source_pages`;
  * that entry's designations print the row's classes, no more, no fewer (the Dean: I, I, II; the Skeena: II, II);
  * each designation reaches the tributaries exactly when the book says (its own `includes_tributaries`, else its
    entry's — AGENTS 9; `tributaries_only` for "X's tributaries");
  * the entry carries the glyph exactly when the book prints it (`symbols`);
  * a `via` row has no designation, and the row it names does, reaching its tributaries (or holding the same water);
  * every entry with a designation, the glyph, or a printed "Class I/II water" is a BOOK row — a new one is read on its
    page and added here, never passed over;
  * every NOT_CLASSIFIED row carries its `not_classified` record, and no other entry does.

THE BUNDLE CHECK (`bundle_problems`, `--bundle` — the release's checks stage, on the candidate's bundle):
  * every corpus designation is in the bundle, placed on sections, at least one of them its own water's (`via` reach;
    a tributary's, for "X's tributaries"); a whole-water designation on a whole-water row covers every section of
    that water (tidal and out-of-B.C. sections aside);
  * ON EVERY SECTION A DESIGNATION BINDS, on the first day of its period (all year: Sep 15), the Licence tab's answer for
    a B.C. resident aged 16+ (the profile every other answer adds to) lists the Classified Waters Licence — the reader the
    answers are built from (`answers.licence.holds` / `documents`). Only a designation the book puts to sleep is let
    off: one with `suspended_while` whose closure binds that day (the Atnarko: "Classified Waters Licence … not required
    until reopened to steelhead fishing");
  * on the day before a dated period starts, a section no designation is in force on does not list it ("nowhere it
    should not");
  * every requirement satisfied by the Classified Waters Licence holds `on: classified_period` (so it can hold only
    where a designation is in force);
  * every section of a `via` row's water carries a designation (tidal and out-of-B.C. sections aside).

READ-ONLY. `scripts/fast-fail.sh` runs the corpus check; the release preflight (`pipeline/release.py`
`check_classified`) runs it, and its checks stage runs the bundle check on the candidate.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

#: what every refusal ends with
FIX_WORDS = ("read the printed page; fix the corpus (a parse defect: the human re-runs the parser) or, when the book "
             "itself changed, the BOOK table in pipeline/regs/classified_waters.py")

#: THE DOCUMENT the gate is about
CWL = "classified_waters_licence"
#: the profile the bundle check asks for: a B.C. resident, 16 or older, not guided, no status (answers.licence)
PROFILE = {"residency": "resident", "age": "16_plus", "guidance": "non_guided", "status": "none"}
#: the day an all-year ("when open") designation is asked on
ALL_YEAR_DAY = (9, 15)


@dataclass(frozen=True)
class Row:
    """One book row that is a Classified Water. `classes`: each "Class I/II water" it prints, in print order.
    `tributaries`: True / False for every designation of the row, or "only" ("X's tributaries"). `glyph`: the row
    prints the CW glyph. `via`: (region, printed name) of the row whose designation classifies this one, for a glyph
    row that prints no class of its own; `why` says how."""
    region: str
    page: int
    name: str
    classes: tuple = ()
    tributaries: object = None
    glyph: bool = True
    via: Optional[tuple] = None
    why: str = ""


#: THE BOOK, 2025-27: 72 rows with the CW glyph (69 printing their classes, 3 classified through another row) and one
#: printing its class without it. Printed pages (AGENTS: printed = PDF − 2 to p.40, PDF − 6 after).
BOOK: tuple[Row, ...] = (
    # ---- Region 1 (pp. 14-19) — every one Class II, with its own dated period and Steelhead Stamp clause
    Row("1", 14, "AHNUHATI RIVER", ("II",), True),
    Row("1", 15, "COPPER CREEK", ("II",), True),
    Row("1", 15, "DATLAMEN CREEK", ("II",), True),
    Row("1", 15, "DEENA CREEK", ("II",), False),
    Row("1", 16, "HONNA RIVER", ("II",), True),
    Row("1", 16, "KAKWEIKEN RIVER", ("II",), True),
    Row("1", 16, "KINGCOME RIVER", ("II",), True),
    Row("1", 17, "MAMIN RIVER", ("II",), True),
    Row("1", 17, "PALLANT CREEK", ("II",), True),
    Row("1", 18, "SEYMOUR RIVER", ("II",), True),
    Row("1", 18, "TLELL RIVER", ("II",), True),
    Row("1", 19, "WAKEMAN RIVER", ("II",), True),
    Row("1", 19, "YAKOUN RIVER", ("II",), True),
    # ---- Region 3 (p.32)
    Row("3", 32, "THOMPSON RIVER (downstream of signs at Kamloops Lake outlet to the confluence with Fraser River)",
        ("II",), False),
    # ---- Region 4 (pp. 35-40) — "Class II water when open, including tributaries"; the p.33 map hatches these
    Row("4", 35, "ABRUZZI CREEK", ("II",), True),
    Row("4", 35, "ALEXANDER CREEK (downstream of the easternmost Hwy 3 bridge)", ("II",), True),
    Row("4", 35, "ALEXANDER CREEK (upstream of the easternmost Hwy 3 bridge)", ("II",), True),
    Row("4", 35, "BIGHORN (Ram) CREEK", via=("4", "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the "
                                              "Bighorn (Ram) Forest Service Road)"),
        why="prints only \"A tributary of Wigwam River; see Wigwam River\": the Wigwam's tributaries are Class II"),
    Row("4", 35, "BULL RIVER", ("II",), True),
    Row("4", 35, "CADORNA CREEK", ("II",), True),
    Row("4", 36, "ELK RIVER (downstream of Elko Dam)", ("II",), True),
    Row("4", 36, "ELK RIVER'S TRIBUTARIES (see exceptions)", ("II",), "only"),
    Row("4", 36, "ELK RIVER (upstream of Elko Dam)", ("II",), True),
    Row("4", 36, "FORDING RIVER (downstream of Josephine Falls)", ("II",), True),
    Row("4", 36, "FORDING RIVER (upstream of Josephine Falls)", via=("4", "FORDING RIVER (downstream of Josephine Falls)"),
        why="prints only \"No Fishing\": the lower Fording's \"including tributaries\" and the Elk's carry it"),
    Row("4", 36, "FORSYTH CREEK", ("II",), True),
    Row("4", 37, "HELLROARING CREEK", ("II",), True),
    Row("4", 37, "KOOTENAY RIVER (upstream of Koocanusa Reservoir)", ("II",), True),
    Row("4", 38, "LODGEPOLE CREEK (downstream of falls near the km 26 post on Lodgepole Road)", ("II",), True),
    Row("4", 38, "LODGEPOLE CREEK (upstream of falls)", ("II",), True),
    Row("4", 38, "MICHEL CREEK (downstream of the easternmost Hwy 3 bridge)", ("II",), True),
    Row("4", 38, "MICHEL CREEK (upstream of the easternmost Hwy 3 bridge)", ("II",), True),
    Row("4", 38, "MORRISSEY CREEK", ("II",), True),
    Row("4", 39, "NORTH (Fork) WHITE RIVER", ("II",), True),
    Row("4", 39, "PERRY CREEK", ("II",), True),
    Row("4", 39, "QUINN CREEK", ("II",), True),
    Row("4", 39, "SKOOKUMCHUCK CREEK", ("II",), True),
    Row("4", 39, "ST. MARY RIVER", ("II",), True),
    Row("4", 40, "WHITE RIVER (see also east White & North White Rivers)", ("II",), True),
    Row("4", 40, "WIGWAM RIVER (downstream of the access road adjacent to km 42 on the Bighorn (Ram) Forest Service "
                 "Road)", ("II",), True),
    Row("4", 40, "WIGWAM RIVER (upstream of the Forest Service recreation site adjacent to km 42 on the Bighorn (Ram) "
                 "Forest Service Road)", ("II",), True),
    # ---- Region 5 (pp. 43-47; the Dean's box on p.42)
    Row("5", 43, "ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Creek upstream of Sitkatapa "
                 "Creek, Hunlen Creek upstream of Hunlen Falls, and Young Creek upstream of Hwy 20 (see separate entries "
                 "for these three waters)", ("II",), True),
    Row("5", 43, "BURNT BRIDGE CREEK (upstream of Sitkatapa Creek)", ("II",), True),
    Row("5", 43, "CHILCOTIN RIVER", ("II",), True),
    Row("5", 44, "CHILKO RIVER", ("II",), True),
    Row("5", 44, "CHUCKWALLA RIVER", ("II",), True),
    Row("5", 44, "DEAN RIVER", ("I", "I", "II"), True),
    Row("5", 45, "HORSEFLY RIVER (from Quesnel Lake to Horsefly River Falls)", ("II",), True),
    Row("5", 45, "KILBELLA RIVER", ("II",), True),
    Row("5", 46, "NEKITE RIVER", ("II",), True),
    Row("5", 47, "WEST ROAD (\"Blackwater\") RIVER", ("II",), False, glyph=False),     # "Mainstem only"; RULINGS Li10
    # ---- Region 6 (pp. 50-54)
    Row("6", 50, "BABINE RIVER", ("I",), True),
    Row("6", 50, "BULKLEY RIVER", ("II",), True),
    Row("6", 50, "DAMDOCHAX CREEK", ("II",), True),
    Row("6", 51, "ECSTALL RIVER", ("II",), True),
    Row("6", 51, "GITNADOIX RIVER", ("I",), True),
    Row("6", 51, "KISPIOX RIVER", ("II",), True),
    Row("6", 52, "KITSEGUECLA RIVER", ("II",), True),
    Row("6", 52, "KITSUMKALUM (Kalum) RIVER", ("II",), True),
    Row("6", 52, "KITWANGA RIVER", ("II",), True),
    Row("6", 52, "KLUATANTAN RIVER", ("II",), True),
    Row("6", 52, "KSI X'ANMAS RIVER (formerly Kwinamass River)", ("II",), True),
    Row("6", 52, "KWINAGEESE RIVER", ("II",), True),
    Row("6", 52, "KWINAMASS RIVER", via=("6", "KSI X'ANMAS RIVER (formerly Kwinamass River)"),
        why="the old name's pointer (\"See Ksi X'anmas River\"): the same water"),
    Row("6", 52, "LAKELSE RIVER", ("I",), True),
    Row("6", 52, "MORICE RIVER", ("II",), True),
    Row("6", 53, "SKEENA RIVER (mainstem only)", ("II", "II"), False),    # "Skeena River 2" and "Section 4"
    Row("6", 53, "STELLAKO RIVER", ("II",), True),
    Row("6", 53, "SUSKWA (Bear) RIVER", ("I",), True),
    Row("6", 53, "SUSTUT RIVER", ("I",), True),
    Row("6", 54, "TELKWA RIVER", ("II",), True),
    Row("6", 54, "ZYMOETZ (Copper) RIVER", ("I", "II"), True),            # "Zymoetz River A" / "B"
    # ---- Region 7A (p.61): the Stellako's row printed again
    Row("7", 61, "STELLAKO RIVER", ("II",), True),
)

#: The rows that print "NOT a Classified Water" (each an asserted absence, a `not_classified` record).
NOT_CLASSIFIED: tuple[tuple[str, int, str], ...] = (
    ("4", 35, "COAL CREEK (downstream of Old MF&M Railway bridge 7 km upstream of Elk River)"),
    ("4", 37, "JOSEPH CREEK"),
)

#: "Class I water", "Class 1 water", "Class II waters" — a printed class (the row's own words, never a zone table's)
_CLASS_PRINTED = re.compile(r"\bClass\s+(I{1,2}|[12])\s+waters?\b", re.I)


def _key(region: str, name: str) -> tuple[str, str]:
    return (str(region), " ".join(name.split()))


def is_water_row(entry: Mapping) -> bool:
    """A row of a region's water table, not a zone or province table (`common.entry_id.is_water_row`)."""
    from pipeline.common import entry_id as EID
    return EID.is_water_row(entry.get("entry_id", ""))


def load_corpus(root: Optional[Path] = None) -> dict[str, dict]:
    """Every entry of the corpus, raw (`io.read_entryfile`, no registry: ids as the file holds them). A missing or empty
    corpus raises — never an empty pass."""
    from pipeline.regs.parsing.io import read_entryfile
    if root is None:
        from pipeline.common.curated import CURATED
        root = Path(CURATED.regulations.synopsis)
    files = sorted(Path(root).glob("region-*.json"))
    if not files:
        raise FileNotFoundError(f"no region files under {root}")
    out: dict[str, dict] = {}
    for f in files:
        out.update(read_entryfile(f, require_entries=True))
    return out


def designations(entry: Mapping) -> list[dict]:
    return [x for x in entry.get("licensing") or [] if x.get("kind") == "designation"]


def reaches_tributaries(entry: Mapping, d: Mapping) -> object:
    """What a designation does about tributaries: "only", True or False — its own field, else its entry's (AGENTS 9)."""
    if d.get("tributaries_only"):
        return "only"
    own = d.get("includes_tributaries")
    return bool(entry.get("includes_tributaries") if own is None else own)


def _row_name(r: Row) -> str:
    return f"p.{r.page} {r.name} (Region {r.region})"


def corpus_problems(entries: Mapping[str, Mapping], book: Sequence[Row] = BOOK,
                    not_classified: Sequence[tuple] = NOT_CLASSIFIED) -> list[str]:
    """Every way the corpus disagrees with the book (see the module doc). [] when it agrees."""
    out: list[str] = []
    by_name: dict[tuple, list[dict]] = {}
    for e in entries.values():
        if is_water_row(e):
            by_name.setdefault(_key(e.get("region", ""), e.get("name", "")), []).append(e)
    rows = {_key(r.region, r.name): r for r in book}
    if len(rows) != len(book):
        out.append("the BOOK table lists a row twice")

    def entry_of(region: str, name: str, page: int, what: str) -> Optional[dict]:
        got = by_name.get(_key(region, name), [])
        if len(got) != 1:
            out.append(f"{what} p.{page} {name} (Region {region}): {len(got)} corpus entries carry this printed name "
                       f"— want exactly 1")
            return None
        if page not in (got[0].get("source_pages") or []):
            out.append(f"{what} p.{page} {name}: its entry {got[0]['entry_id']} says pages "
                       f"{got[0].get('source_pages')}")
        return got[0]

    for r in book:
        e = entry_of(r.region, r.name, r.page, "book row")
        if e is None:
            continue
        ds = designations(e)
        glyph = "Classified" in (e.get("symbols") or [])
        if glyph != r.glyph:
            out.append(f"{_row_name(r)}: the book {'prints' if r.glyph else 'does not print'} the CW glyph; "
                       f"{e['entry_id']} symbols {e.get('symbols')}")
        if r.via is not None:
            if ds:
                out.append(f"{_row_name(r)}: prints no class of its own, yet {e['entry_id']} carries "
                           f"{len(ds)} designation(s) — the book table says it is classified through {r.via[1]}")
            v = by_name.get(_key(*r.via), [])
            vds = designations(v[0]) if len(v) == 1 else []
            same_water = len(v) == 1 and set(e.get("matched") or []) & set(v[0].get("matched") or [])
            if not vds or not (same_water or any(reaches_tributaries(v[0], d) for d in vds)):
                out.append(f"{_row_name(r)} is classified through {r.via[1]} ({r.why}), but that row "
                           + ("carries no designation" if not vds else
                              "neither holds the same water nor reaches its tributaries"))
            continue
        got = [d.get("classified") for d in ds]
        if Counter(got) != Counter(r.classes):
            out.append(f"{_row_name(r)}: the book prints {_classes(r.classes)}; {e['entry_id']} carries "
                       f"{_classes(got) if got else 'no designation'}")
        for d in ds:
            t = reaches_tributaries(e, d)
            if t != r.tributaries:
                out.append(f"{_row_name(r)}: designation {d.get('id')} "
                           f"{_trib_words(t)}; the book: {_trib_words(r.tributaries)}")
    # the reverse: a classified-looking entry the book table does not list
    for e in sorted(entries.values(), key=lambda x: x.get("entry_id", "")):
        if not is_water_row(e) or _key(e.get("region", ""), e.get("name", "")) in rows:
            continue
        says = []
        if designations(e):
            says.append(f"{len(designations(e))} designation(s)")
        if "Classified" in (e.get("symbols") or []):
            says.append("the CW glyph")
        if _CLASS_PRINTED.search(_without_not_classified(e.get("regs_verbatim") or "")):
            says.append("a printed \"Class I/II water\"")
        if says:
            out.append(f"{e['entry_id']} (p.{(e.get('source_pages') or ['?'])[0]} {e.get('name')}) has "
                       + " and ".join(says) + " but is not a BOOK row")
    # NOT a Classified Water
    want = set()
    for region, page, name in not_classified:
        e = entry_of(region, name, page, "\"NOT a Classified Water\" row")
        if e is None:
            continue
        want.add(e["entry_id"])
        if not any(x.get("kind") == "not_classified" for x in e.get("licensing") or []):
            out.append(f"p.{page} {name}: prints \"NOT a Classified Water\"; {e['entry_id']} carries no "
                       f"not_classified record")
    for e in entries.values():
        if e["entry_id"] not in want and any(x.get("kind") == "not_classified" for x in e.get("licensing") or []):
            out.append(f"{e['entry_id']} carries a not_classified record but is not a NOT_CLASSIFIED row")
    return out


def _without_not_classified(text: str) -> str:
    return re.sub(r"not a classified water", "", text, flags=re.I)


def _classes(cs: Iterable) -> str:
    cs = list(cs)
    return ", ".join(f"Class {c}" for c in cs) if cs else "no class"


def _trib_words(t: object) -> str:
    return {"only": "covers only the tributaries", True: "reaches the tributaries",
            False: "does not reach the tributaries"}.get(t, repr(t))


# ------------------------------------------------------------------------------------------------- the bundle check
def _first_day(when: Optional[Mapping]) -> Optional[tuple[int, int]]:
    ds = (when or {}).get("dates") or []
    return (ds[0]["from_month"], ds[0]["from_day"]) if ds else None


def _day_before(md: tuple[int, int]) -> tuple[int, int]:
    import datetime
    d = datetime.date(2024, *md) - datetime.timedelta(days=1)      # a leap year, so Mar 1 -> Feb 29
    return (d.month, d.day)


def _whole(extents: Optional[Sequence[Mapping]]) -> bool:
    """No extents, or only `whole` ones with nothing narrowing them: the row's (or record's) whole water."""
    return all(x.get("op") == "whole" and set(x) <= {"op"} for x in extents or [])


def _item_sections(db: sqlite3.Connection, item: str) -> list[tuple[int, bool, bool]]:
    """(sid, tidal, outside B.C.) for every section of a water, by its id or an id the bundle aliases to it."""
    return [(sid, bool(t), bool(o)) for sid, t, o in db.execute(
        "SELECT s.sid, (SELECT 1 FROM tidal t WHERE t.sid = s.sid), (SELECT 1 FROM outside_bc o WHERE o.sid = s.sid) "
        "FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.item_id = "
        "COALESCE((SELECT item_id FROM item_alias WHERE alias = ?), ?)", (item, item))]


class _Asker:
    """The Licence tab's answer for PROFILE on a section and day, through the reader the answers are built from
    (`answers.licence.holds` + `documents`), memoised on the section's LICENCE KEY — the bundle's part's, as the answers
    build reads it (`answers.licence.part_licence_key`: every section of a key answers alike); a section in no part is
    asked on its own."""

    def __init__(self, db: sqlite3.Connection):
        from pipeline.deliver.answers import licence as L
        from pipeline.deliver.bundle.derived import parts, unnamed_parts
        self.L, self.db = L, db
        self.C = L.corpus(db)
        sleepy = L.sleepy_sets(db)
        named = {(p.item_id, p.part_ix): L.part_licence_key(p, sleepy) for ps in parts(db).values() for p in ps}
        self.keys: dict[int, object] = {}
        for item, ix, sid in db.execute("SELECT i.item_id, s.part_ix, s.sid FROM part_section s JOIN item i "
                                        "ON i.ord = s.ord ORDER BY i.item_id, s.part_ix"):
            self.keys.setdefault(sid, named[(item, ix)])
        loose = {u.up: L.part_licence_key(u, sleepy) for u in unnamed_parts(db)}
        for sid, up in db.execute("SELECT sid, up FROM section_ruleset WHERE up IS NOT NULL"):
            self.keys.setdefault(sid, loose[up])
        self.memo: dict = {}

    def answer(self, sid: int, md: tuple[int, int]) -> dict:
        k = (self.keys.get(sid, ("section", sid)), md)
        if k not in self.memo:
            L, C = self.L, self.C
            h = L.holds(self.db, C, sid, md)
            docs = L.documents(C, h, L._alternatives_here(self.db, C, sid), PROFILE, C.index.__getitem__)
            self.memo[k] = {"designations": set(h["designations"]),
                            "cwl": any(x["doc"] == CWL for x in docs["documents"]),
                            "wrong_water": list(h["wrong_water"]), "displaced": dict(h["displaced"])}
        return self.memo[k]


def bundle_problems(bundle: Path, entries: Mapping[str, Mapping], book: Sequence[Row] = BOOK) -> list[str]:
    """Every way the built bundle ships a classified water without its licence (see the module doc). [] when none."""
    from pipeline.deliver.answers.common import connect
    if not Path(bundle).is_file():
        raise FileNotFoundError(f"no bundle at {bundle} — build it: python -m pipeline.deliver bundle")
    db = connect(str(bundle))
    out: list[str] = []
    try:
        corpus_des = {(e["entry_id"], d["id"]): (e, d) for e in entries.values() for d in designations(e)}
        shipped = {(e, i): (placement, json.loads(rec)) for e, i, placement, rec in db.execute(
            "SELECT entry_id, designation_id, placement, record FROM designation")}
        for k in sorted(set(corpus_des) - set(shipped)):
            out.append(f"designation {k[0]}#{k[1]} is in the corpus but not in the bundle (the bundle is stale)")
        for k in sorted(set(shipped) - set(corpus_des)):
            out.append(f"designation {k[0]}#{k[1]} is in the bundle but not in the corpus (the bundle is stale)")
        ask = _Asker(db)
        for (eid, did) in sorted(set(corpus_des) & set(shipped)):
            placement, rec = shipped[(eid, did)]
            e = corpus_des[(eid, did)][0]
            where = f"{eid}#{did} (p.{(e.get('source_pages') or ['?'])[0]} {e.get('name')})"
            rows = db.execute("SELECT sid, via FROM designation_section WHERE entry_id = ? AND designation_id = ?",
                              (eid, did)).fetchall()
            own = ("reach", "trib") if rec.get("tributaries_only") else ("reach",)
            if placement != "sections" or not any(v in own for _, v in rows):
                out.append(f"{where}: placed {placement!r} on {len(rows)} section(s), none of them "
                           f"{'a tributary' if rec.get('tributaries_only') else 'its own water'} — it ships on no water")
                continue
            # A WHOLE-WATER DESIGNATION ON A WHOLE-WATER ROW covers every section of the row's water (tidal and
            # out-of-B.C. ones aside): a section of it that lost the designation is a water shipped half classified
            if not rec.get("tributaries_only") and _whole(rec.get("extents")) and _whole(e.get("extents")):
                bound = {sid for sid, _ in rows}
                for item in e.get("matched") or []:
                    bare = [sid for sid, t, o in _item_sections(db, item) if sid not in bound and not t and not o]
                    if bare:
                        out.append(f"{where}: {len(bare)} section(s) of its own water {item} do not carry it "
                                   f"(e.g. section {bare[0]})")
            start = _first_day(rec.get("when"))
            md = start or ALL_YEAR_DAY
            asleep = 0
            missing: Counter = Counter()
            example: dict = {}
            for sid, _via in rows:
                a = ask.answer(sid, md)
                if a["cwl"]:
                    continue
                if f"{eid}#{did}" not in a["designations"] and rec.get("suspended_while"):
                    asleep += 1                    # the book's own "not required until reopened"
                    continue
                why = ("not in force" if f"{eid}#{did}" not in a["designations"] else
                       f"dropped for the other kind of water {a['wrong_water']}" if a["wrong_water"] else
                       f"displaced by {sorted(a['displaced'])}" if a["displaced"] else "not asked for")
                missing[why] += 1
                example.setdefault(why, sid)
            for why, n in sorted(missing.items()):
                out.append(f"{where}: {n} of its {len(rows)} section(s) ship WITHOUT the Classified Waters Licence on "
                           f"{md[0]}/{md[1]} — {why} (e.g. section {example[why]})")
            if start is not None:
                before = _day_before(start)
                extra = [sid for sid, _ in rows
                         if not ask.answer(sid, before)["designations"] and ask.answer(sid, before)["cwl"]]
                if extra:
                    out.append(f"{where}: {len(extra)} section(s) ask for the Classified Waters Licence on "
                               f"{before[0]}/{before[1]}, outside its period, with no designation in force "
                               f"(e.g. section {extra[0]})")
        # a requirement satisfied by the licence must wait for a designation
        for eid, rid, rec in db.execute("SELECT entry_id, req_id, record FROM requirement"):
            r = json.loads(rec)
            if any(CWL in (p.get("hold") or []) for p in r.get("satisfied_by") or []) \
                    and r.get("on") != "classified_period":
                out.append(f"requirement {eid}#{rid} asks for the Classified Waters Licence without "
                           f"`on: classified_period` — it can hold where no water is classified")
        # a `via` row's water is classified all along
        by_name = {_key(e.get("region", ""), e.get("name", "")): e for e in entries.values() if is_water_row(e)}
        for r in book:
            if r.via is None or _key(r.region, r.name) not in by_name:
                continue                                     # corpus_problems names a missing row
            e = by_name[_key(r.region, r.name)]
            designated = {sid for (sid,) in db.execute("SELECT DISTINCT sid FROM designation_section")}
            for item in e.get("matched") or []:
                got = _item_sections(db, item)
                if not got:
                    out.append(f"{_row_name(r)}: its water {item} has no section in the bundle")
                    continue
                bare = [sid for sid, t, o in got if sid not in designated and not t and not o]
                if bare:
                    out.append(f"{_row_name(r)}: {len(bare)} of {len(got)} section(s) of {item} carry no designation "
                               f"— the book classifies it through {r.via[1]} (e.g. section {bare[0]})")
    finally:
        db.close()
    return out


# ------------------------------------------------------------------------------------------------- the extraction
def extraction_problems(rows: Sequence[Mapping], book: Sequence[Row] = BOOK,
                        not_classified: Sequence[tuple] = NOT_CLASSIFIED) -> list[str]:
    """The BOOK table against an extraction (`synopsis_raw_data.json`'s pages, or their rows): every row with the
    Classified glyph or a printed class is a BOOK row on its page, and every BOOK row is in the extraction."""
    flat = [r for p in rows for r in (p.get("rows") or [])] if rows and "rows" in rows[0] else list(rows)
    seen = {}
    for r in flat:
        region = re.match(r"REGION\s+(\d+)", r.get("region") or "")
        seen[_key(region.group(1) if region else "", r.get("water") or "")] = r
    out = []
    want = {_key(r.region, r.name): r for r in book}
    for k, r in sorted(seen.items()):
        glyph = "Classified" in (r.get("symbols") or [])
        printed = bool(_CLASS_PRINTED.search(_without_not_classified(r.get("raw_regs") or "")))
        if (glyph or printed) and k not in want:
            out.append(f"extraction p.{r.get('page')} {r.get('water')}: "
                       f"{'the CW glyph' if glyph else 'a printed class'}, but not a BOOK row")
        elif k in want and (want[k].page != r.get("page") or want[k].glyph != glyph):
            out.append(f"extraction p.{r.get('page')} {r.get('water')}: glyph {glyph}; the BOOK row says "
                       f"p.{want[k].page}, glyph {want[k].glyph}")
    for k, r in want.items():
        if k not in seen:
            out.append(f"BOOK row {_row_name(r)} is not in the extraction")
    for region, page, name in not_classified:
        if _key(region, name) not in seen:
            out.append(f"NOT_CLASSIFIED row p.{page} {name} is not in the extraction")
    return out


# ------------------------------------------------------------------------------------------------- the command
def check(*, corpus_root: Optional[Path] = None, bundle: Optional[Path] = None) -> dict:
    entries = load_corpus(corpus_root)
    probs = corpus_problems(entries)
    bprobs = bundle_problems(bundle, entries) if bundle is not None else None
    n_des = sum(len(designations(e)) for e in entries.values())
    return {"ok": not probs and not bprobs, "corpus": probs, "bundle": bprobs, "rows": len(BOOK),
            "designations": n_des}


def say(got: Mapping) -> tuple[int, str]:
    """(exit code, what to print): 0 when the corpus (and the bundle, when asked) agree with the book, else 1."""
    lines = []
    for what in ("corpus", "bundle"):
        ps = got.get(what)
        if ps:
            lines.append(f"classified waters ({what}): {len(ps)} problem(s) — {FIX_WORDS}:")
            lines += [f"  {p}" for p in ps]
    tail = (f"classified waters: {got['rows']} book rows, {got['designations']} designations — every one "
            f"{'ships classified with its licence' if got.get('bundle') is not None else 'in the corpus as printed'}")
    if lines:
        return 1, "\n".join(lines)
    return 0, tail


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.classified_waters", description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="the corpus against the book (and the bundle with --bundle)")
    ap.add_argument("--bundle", nargs="?", const="", default=None, type=str,
                    help="also check this built bundle (default: the generated bundle)")
    ap.add_argument("--corpus", type=Path, default=None, help="the corpus directory (default: the curated one)")
    ap.add_argument("--extraction", type=Path, default=None,
                    help="check the BOOK table against an extraction (synopsis_raw_data.json)")
    a = ap.parse_args(argv)
    if not a.check and a.extraction is None:
        ap.error("say --check and/or --extraction PATH")
    try:
        rc = 0
        if a.extraction is not None:
            ps = extraction_problems(json.loads(Path(a.extraction).read_text(encoding="utf-8")))
            print("\n".join(["book table vs extraction: " + (f"{len(ps)} problem(s)" if ps else "agree")]
                            + [f"  {p}" for p in ps]))
            rc = 1 if ps else 0
        if a.check:
            bundle = None
            if a.bundle is not None:
                if a.bundle:
                    bundle = Path(a.bundle)
                else:
                    from pipeline.common.curated import GENERATED
                    bundle = GENERATED.bundle / "bundle.sqlite"
            code, said = say(check(corpus_root=a.corpus, bundle=bundle))
            print(said)
            rc = max(rc, code)
        return rc
    except Exception as ex:  # noqa: BLE001 — said and exit 2: a gate that could not read is never a pass
        print(f"classified waters: could not run ({type(ex).__name__}: {ex})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
