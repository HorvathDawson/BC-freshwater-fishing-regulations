"""Emit THE TROUT TABLE VECTORS (trout table plan, T810 ticket V-py; contract 2.8): the decoded trout cells of the case
waters, as the app's reader must decode them.

    python -m pipeline.tools.emit.trout_vectors          # write app/fixtures/trout/trout-vectors.generated.json
    python -m pipeline.tools.emit.trout_vectors --check  # fail if it is stale against the committed trout fixture

WHY THIS EXISTS (AGENTS 40, the `dfo_vectors` pattern). Python decides every word, number and date of the trout table;
the app only draws it. The one pin between the two is a shared committed fixture: for each CASE (a water, by stream key
and metre or by its lake, on a day) the `TroutFrame` the cell holds, decoded by Python from the committed fixture
release. The app's V test decodes the same cell from the same release and must get the same value; the hand cases
(`trout-hand.json`) are frames both sides must REFUSE (`test_trout_vectors.py` holds the Python half).

WHERE THE VECTORS COME FROM, SO CI CAN CHECK THEM. A full answers build cannot run in CI (AGENTS 39). The vectors are
decoded from THE TROUT FIXTURE RELEASE, `app/fixtures/trout/regs-release/` (+ `section-lines.json` beside it): the regs
fixture (`pipeline.deliver.release.regs_fixture --trout-cases`) cut from a SIDE release built with `--trout-table`,
keeping exactly the waters of `CASES`. `--check` decodes them again and compares: it proves vectors = fixture, never
vectors = builder; a builder change reaches both only through `scripts/trout-refresh.sh` (side release -> fixture ->
these vectors -> the golden sync), which nobody runs by hand step by step.

WHY A FIXTURE OF ITS OWN (until the switch). The section ships only behind `--trout-table` (owner 17:40: "only at the
switch"), and the app's committed regs fixture (`app/packages/data/dev/regs-release/`, re-cut from every normal release
by `regs-recut/sync.mjs`) must stay the normal release's until the app reads the section (its M0): a regs fixture holding
`trout` moves `taps.json`'s versions, which the app's conformance holds to `KNOWN_VERSIONS`. So the side-cut copy lives
under `app/fixtures/trout/`; at the cutover (ticket L) the normal release carries the section and the two can be one.

A CASE: `name` (unique, `<water>-<mmmdd>`), `water` (its name, for people), `item`, `anchor` (`{"stream": blue line
key, "m": a section's start metre}` or `{"lake": item}`: the lake's one part; never a part number, AGENTS W46), `day`
("MM-DD"), `weekday` (the app's `segmentIndex` names one where a day reads by weekday), `pins` (what it is there for,
contract 2.8's list). A vector adds `s` (the segment index), `frame` (the decoded `TroutFrame`, or null: no table,
Q-D3) and `composed` (the `{day}` slot filled: "today" and "on Jul 15").

`drafts` (contract 2.8 "before the builder exists"): hand-written tables for layout work. The builder exists, so it
is empty; a draft added later is never the source of a word.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.common.calendar_spec import MONTHS, WEEKDAYS as CALENDAR_WEEKDAYS
from pipeline.common.curated import REPO_ROOT

FORMAT = "trout-vectors/1"
HERE = REPO_ROOT / "app" / "fixtures" / "trout"
OUT = REPO_ROOT / "app" / "fixtures" / "trout" / "trout-vectors.generated.json"
#: the hand cases both sides refuse (one shared file, never a copy per side)
HAND = HERE / "trout-hand.json"
#: the trout fixture release root (one `r-*` folder: the regs fixture cut from a side release, `--trout-cases`)
FIXTURE = HERE / "regs-release"
#: its stream lines ([handle, blue_line_key, from_m, to_m) of every stream section of the kept waters)
LINES = HERE / "section-lines.json"

_BY = "python -m pipeline.tools.emit.trout_vectors"
#: the calendar spec's own tables (gate 4: spelled once); a case names its weekday by its first three letters
WEEKDAYS = {w[:3].lower(): w for w in CALENDAR_WEEKDAYS}


@dataclass(frozen=True)
class Case:
    name: str
    water: str
    item: str
    anchor: Tuple                    # ("stream", blue line key, start metre) | ("lake",)
    day: Tuple[int, int]
    pins: str
    weekday: str = "wed"

    def json(self) -> dict:
        a = {"stream": self.anchor[1], "m": self.anchor[2]} if self.anchor[0] == "stream" else {"lake": self.item}
        return {"name": self.name, "water": self.water, "item": self.item, "anchor": a,
                "day": f"{self.day[0]:02d}-{self.day[1]:02d}", "weekday": self.weekday, "pins": self.pins}


def _s(name, water, item, blk, m, day, pins):
    return Case(name, water, item, ("stream", blk, m), day, pins)


def _l(name, water, item, day, pins):
    return Case(name, water, item, ("lake",), day, pins)


#: THE CASES (contract 2.8's list; the stream anchors are each part's longest section's start, r-46e4355a7277). The
#: regs fixture keeps these waters (`case_items`), so the two lists cannot disagree.
CASES: Tuple[Case, ...] = (
    _s("skeena-sedan-mill-jul15", "Skeena River", "gnis:2936", 360887278, 208304, (7, 15),
       "one hatchery-only steelhead row with the label; stop duty 'this river'; own 'only 3'; shared band"),
    _s("skeena-sedan-mill-sep15", "Skeena River", "gnis:2936", 360887278, 208304, (9, 15), "row, band and strip dates"),
    _s("skeena-sedan-mill-nov15", "Skeena River", "gnis:2936", 360887278, 208304, (11, 15), "row, band and strip dates"),
    _l("lois-jul15", "Lois Lake", "wbk:329197058", (7, 15),
       "two boxes counted apart; top.second = together; hatchery-only char and steelhead"),
    _l("khartoum-jul15", "Khartoum Lake", "wbk:329197063", (7, 15), "the same body as Lois Lake, interned once"),
    _l("windsor-jul15", "Windsor Lake", "wbk:329197074", (7, 15), "char hatchery only (C5)"),
    _l("dodd-jul15", "Dodd Lake", "wbk:329197061", (7, 15),
       "HARD (owner, T810 G2 Q9): hatchery rows first, the four wild rows under one white box 'only 2 wild in all'"),
    _l("horseshoe-jul15", "Horseshoe Lake", "wbk:329197066", (7, 15), "char hatchery only (C5)"),
    _l("nanton-jul15", "Nanton Lake", "wbk:329197073", (7, 15), "char hatchery only (C5)"),
    _l("ireland-jul15", "Ireland Lake", "wbk:329197113", (7, 15), "char hatchery only (C5)"),
    _s("shamrock-jul15", "Shamrock Creek", "blk:356196726", 356196726, 0, (7, 15), "no 20-30 cm steelhead (T805 D)"),
    _s("kilgard-jul15", "Kilgard Creek", "blk:356314888", 356314888, 0, (7, 15), "no 20-30 cm steelhead (T805 D)"),
    _l("echoes-jul15", "Echoes Lakes", "wbk:329524034", (7, 15), "together 'from both lakes together' (T805 D)"),
    _l("bowron-jul15", "Bowron Lake", "wbk:329058703", (7, 15), "park possession in the strip"),
    _s("istsi-jul15", "Istsi Creek", "gnis:10793", 356255673, 0, (7, 15), "possession that came and went"),
    _l("nation-lakes-jul15", "Nation Lakes (Tchentlo)", "wbk:329424457", (7, 15),
       "possession below the day's number: the possession band and its quiet line"),
    _l("cunningham-jul15", "Cunningham Lake", "wbk:329537190", (7, 15), "possession below the day's number"),
    _l("tagish-jul15", "Tagish Lake", "wbk:328961735", (7, 15), "possession with a size clause; 'from these 2 together'"),
    _l("takla-jul15", "Takla Lake", "wbk:328961757", (7, 15), "Region 7 '1 day's limit'"),
    _s("buckinghorse-jul15", "Buckinghorse River", "gnis:10118", 359013222, 0, (7, 15), "bull trout possession = 1"),
    _s("fraser-r2-mission-jul15", "Fraser River (Region 2, Mission)", "gnis:39325", 355991887, 0, (7, 15),
       "box-level HATCHERY ONLY label; 30 cm; no wild rows under Release"),
    _s("fraser-r3-sep15", "Fraser River (Region 3)", "gnis:39325", 355999845, 0, (9, 15),
       "box date 'until Sep 30, then closed to trout and char'"),
    _s("fraser-r3-oct15", "Fraser River (Region 3)", "gnis:39325", 355999845, 0, (10, 15),
       "closed-today form; DateSet.top 'reopens Nov 1'"),
    _s("fraser-r3-nov15", "Fraser River (Region 3)", "gnis:39325", 355999845, 0, (11, 15), "reopened"),
    _s("bish-may20", "Bish Creek", "gnis:3480", 360227777, 0, (5, 20),
       "only steelhead closed: a closed group of one; its closed box says 'reopens'"),
    _s("adams-jul15", "Adams River", "gnis:39257", 355991580, 0, (7, 15), "'rule changes Jul 31'; summed tier never drawn"),
    _s("adams-jul31", "Adams River", "gnis:39257", 355991580, 0, (7, 31), "80 cm on the row; the one-day sheet line"),
    _l("kamloops-lake-jan15", "Kamloops Lake", "wbk:329563838", (1, 15), "the summed 'only 3' never drawn"),
    _l("kamloops-lake-jul15", "Kamloops Lake", "wbk:329563838", (7, 15), "DV and LT share 'only 1 of these 2'"),
    _s("elk-nov15", "Elk River (mouth to Elko Dam)", "gnis:16880", 356368637, 0, (11, 15),
       "all-release table, fold 'Release', the year bar"),
    _s("elk-apr15", "Elk River (mouth to Elko Dam)", "gnis:16880", 356368637, 0, (4, 15),
       "a whole-water closed day: no table (null)"),
    _s("elk-jul15", "Elk River (mouth to Elko Dam)", "gnis:16880", 356368637, 0, (7, 15), "'Up to 1'"),
    _l("kootenay-main-nov15", "Kootenay Lake (main body)", "wbk:-20", (11, 15), "one table with two keep boxes"),
    _s("glenn-jul15", "Glenn Creek", "gnis:10457", 356549109, 0, (7, 15), "top line 'or 5 if they're rainbow trout'"),
    _s("madden-jul15", "Madden Creek", "gnis:12076", 356568153, 0, (7, 15), "top line with three numbers"),
    _s("kaslo-jul15", "Kaslo River", "gnis:11345", 356567321, 5243, (7, 15),
       "Dolly Varden inside the 2 from streams (no lift printed)"),
    _s("keen-jul15", "Keen Creek", "gnis:11410", 356562809, 14384, (7, 15), "as Kaslo River"),
    _s("vedder-jul15", "Vedder River", "gnis:3062", 355999181, 0, (7, 15), "the box label; 'or 4 if they're hatchery'"),
    _l("ross-lake-jul15", "Ross Lake", "wbk:328961736", (7, 15), "boxes counted apart in the top line"),
    _s("kitimat-jul15", "Kitimat River", "gnis:3225", 360230142, 0, (7, 15),
       "twin wild and hatchery rows; the wild row inside 'only 1 of these 3'"),
    _s("kitimat-nov01", "Kitimat River", "gnis:3225", 360230142, 0, (11, 1), "number and size changes dated"),
    _s("humphrys-jul15", "Humphrys Creek", "gnis:2046", 360246200, 0, (7, 15), "as the Kitimat, on a tributary"),
    _s("humphrys-nov01", "Humphrys Creek", "gnis:2046", 360246200, 0, (11, 1), "number and size changes dated"),
    _l("ruby-jul15", "Ruby Lake", "wbk:329197064", (7, 15), "a wild size naming its fish"),
    _l("cultus-jul15", "Cultus Lake", "wbk:329083342", (7, 15), "a wild quota of its own"),
    _s("bella-coola-mar15", "Bella Coola River", "gnis:11611", 360222840, 0, (3, 15), "a looser-only change dated"),
    _s("andi-jul15", "Andi Creek", "gnis:10031", 360846730, 3231, (7, 15),
       "the strip date is never given when a row's change is later"),
    _s("denetiah-jun30", "Denetiah Creek", "gnis:39298", 359001543, 0, (6, 30), "a short gap still dated"),
    _l("shuswap-jul15", "Shuswap Lake", "wbk:329518145", (7, 15), "a simple case"),
    _s("bruer-jul15", "Bruer Creek", "gnis:10061", 356570610, 0, (7, 15), "a loose row inside the table (t670 K4/K5)"),
    _s("mat-jul15", "Mat Creek", "gnis:14254", 356559564, 0, (7, 15), "a loose row inside the table"),
    _s("burnt-jan15", "Burnt River", "gnis:10657", 359004052, 0, (1, 15), "a mixed trout and kokanee row split"),
    _s("nahwitti-jul15", "Nahwitti River", "gnis:29435", 354154451, 0, (7, 15),
       "Region 1 base only: z1 trout_quota.r7 'Release all char' is a row with no pool (EB/DV/LT, no group, no "
       "parent) beside the trout keep pool (r1: trout and char except char, 4); the char land in the Release box "
       "with covers (0,1), drawn exactly once, never alone under other fish (owner, staging r-46e4355a7277 key 106; "
       "the shape is 513 keys on 289 waters)"),
    _l("nitinat-jul15", "Nitinat Lake", "wbk:329504244", (7, 15), "tidal: no table (null)"),
)


def case_items(cases: Sequence[Case] = CASES) -> List[str]:
    """The waters the trout fixture keeps (`regs_fixture --trout-cases`)."""
    return sorted({c.item for c in cases})


# ============================================================================================
# The fixture release, read cell by cell (a fixture is a subset: the store's whole-file decoder refuses it)
# ============================================================================================

class Fixture:
    """One regs fixture release (`root/r-*`), its regulation files reassembled (`store.release.province_store`)."""

    def __init__(self, root: Path, lines: Path):
        from pipeline.deliver.store.release import province_store
        rels = sorted(p for p in Path(root).glob("r-*") if (p / "manifest.json").exists())
        if len(rels) != 1:
            raise SystemExit(f"{_BY}: {root} holds {len(rels)} releases, not one (cut it with scripts/trout-refresh.sh)")
        self.dir = rels[0]
        self.manifest = json.loads((self.dir / "manifest.json").read_text())
        if "trout" not in ((self.manifest.get("regulations") or {}).get("versions") or {}):
            raise SystemExit(f"{_BY}: {self.dir.name} holds no trout section: it was not cut from a side release "
                             "built with --trout-table")
        self.st = province_store(self.dir, verify=False)
        con = self.st.con
        self.moments = self.st.static("answers.moments")
        self.seglist = {i: json.loads(t) for i, t in con.execute("SELECT seg, starts FROM seglist")}
        self.segmom = {i: json.loads(t) for i, t in con.execute("SELECT mom, ix FROM segmom")}
        self.akeys = self.st.section_akeys()
        strs = dict(con.execute("SELECT id, text FROM str"))
        self.ord_of = {strs[w]: o for o, w in con.execute("SELECT item_ord, item_id FROM item")}
        # where each section lies: the water file's own `part_section` (not a store table)
        f = self.manifest["files"]["water"]
        with tempfile.TemporaryDirectory(prefix="trout-vectors-") as tmp:
            p = Path(tmp) / "water.sqlite"
            p.write_bytes(gzip.decompress((self.dir.parent / f["url"]).read_bytes()))
            wc = sqlite3.connect(str(p))
            self.part_sids: Dict[Tuple[int, int], List[int]] = {}
            for o, sid, ix in wc.execute("SELECT ord, sid, part_ix FROM part_section ORDER BY sid"):
                self.part_sids.setdefault((o, ix), []).append(sid)
            #: each kept water's receiving waters (`water_exit`: the fixture keeps them too, T459)
            self.down: Dict[int, set] = {}
            for o, d in wc.execute("SELECT ord, down_ord FROM water_exit"):
                self.down.setdefault(o, set()).add(d)
            #: the side release the fixture was cut from (every fixture file's meta says it: `regs_fixture`)
            self.of = dict(wc.execute("SELECT k, v FROM meta")).get("fixture_of")
            wc.close()
        self.sids_of: Dict[int, List[int]] = {}
        for (o, _ix), sids in sorted(self.part_sids.items()):
            self.sids_of.setdefault(o, []).extend(sids)
        self.parts_of: Dict[int, List[Optional[int]]] = {}
        for o, ix, a in con.execute("SELECT item_ord, part_ix, akey FROM part ORDER BY item_ord, part_ix"):
            self.parts_of.setdefault(o, []).append(a)
        d = json.loads(Path(lines).read_text())
        if self.of is None or d.get("release") != self.of:
            raise SystemExit(f"{_BY}: {lines} was cut from {d.get('release')}, the fixture from {self.of}: re-run "
                             "scripts/trout-refresh.sh")
        self.lines = d["lines"]

    def close(self) -> None:
        self.st.close()

    def section(self, c: Case) -> int:
        """The case's anchor -> its section handle, inside the case's own water (a moved section fails by name)."""
        o = self.ord_of.get(c.item)
        if o is None:
            raise SystemExit(f"{_BY}: case {c.name}: the fixture holds no water {c.item}")
        mine = set(self.sids_of.get(o, ()))
        if c.anchor[0] == "lake":
            ps = self.parts_of.get(o, [])
            if len(ps) != 1 or not mine:
                raise SystemExit(f"{_BY}: case {c.name}: {c.item} is not one lake part ({len(ps)} parts)")
            return min(mine)
        _k, blk, m = c.anchor
        hit = [h for h, b, lo, hi in self.lines if b == blk and lo == m and h in mine]
        if len(hit) != 1:
            raise SystemExit(f"{_BY}: case {c.name}: no section of {c.item} starts at {m} m on line {blk} "
                             f"({len(hit)} found): the water moved; re-anchor the case")
        return hit[0]

    def key(self, akey: int) -> Tuple[List[int], Optional[List[dict]]]:
        seg, mom = self.st.con.execute("SELECT seg, mom FROM akey WHERE akey = ?", (akey,)).fetchone()
        return self.seglist[seg], (None if mom is None else [self.moments[i] for i in self.segmom[mom]])

    def frame(self, akey: int, s: int) -> Optional[dict]:
        """The cell's TroutFrame, decoded as `encode.TroutCodec.decode_frame` decodes a whole file's."""
        con = self.st.con
        (ref,) = con.execute("SELECT trout FROM cell WHERE akey = ? AND s = ?", (akey, s)).fetchone()
        if ref is None:
            return None
        one = lambda t, i: self.st.val(t, con.execute(f"SELECT j FROM {t} WHERE id = ?", (i,)).fetchone()[0])
        t, d, covers = one("trout_frame", ref)
        body = one("trout_table", t)
        table = {**body, "boxes": [{**b, "rows": [{**r, "sheet": one("trout_sheet", r["sheet"])} for r in b["rows"]]}
                                   for b in body["boxes"]]}
        return {"table": table, "dates": None if d is None else one("trout_dates", d), "covers": covers}


def vector(fx: Fixture, c: Case) -> dict:
    from pipeline.deliver import calendar as CAL
    from pipeline.deliver.answers import encode as E
    from pipeline.deliver.answers.model.trout import TroutFrame
    sid = fx.section(c)
    akey = fx.akeys.get(sid)
    if not akey:
        raise SystemExit(f"{_BY}: case {c.name}: section {sid} has no answer key")
    starts, moms = fx.key(akey)
    s = E.segment_index(starts, CAL.day_of(c.day), moms, WEEKDAYS[c.weekday], inside=False)
    frame = fx.frame(akey, s)
    composed = None
    if frame is not None:
        TroutFrame.model_validate_json(json.dumps(frame, ensure_ascii=False))
        lead = frame["table"]["top"]["lead"]
        on = f"on {MONTHS[c.day[0] - 1]} {c.day[1]}"
        composed = {"lead_today": lead.replace("{day}", "today"), "lead_on": lead.replace("{day}", on)}
    return {**c.json(), "s": s, "frame": frame, "composed": composed}


def build(root: Path = FIXTURE, lines: Path = LINES, cases: Sequence[Case] = CASES) -> dict:
    names = [c.name for c in cases]
    if len(set(names)) != len(names):
        raise SystemExit(f"{_BY}: a case name is used twice")
    fx = Fixture(root, lines)
    try:
        vecs = [vector(fx, c) for c in cases]
        src = fx.of
    finally:
        fx.close()
    return {
        "format": FORMAT,
        "_about": f"GENERATED by `{_BY}` from the trout fixture release beside this file (cut from side release "
                  f"{src}): per case the trout cell the app's reader must decode. Do not edit; the refresh is "
                  "scripts/trout-refresh.sh. trout-hand.json holds the frames both sides refuse.",
        "fixture": {"release": fx.dir.name, "of": src},
        "cases": vecs,
        "drafts": [],
    }


def render(root: Path = FIXTURE, lines: Path = LINES, cases: Sequence[Case] = CASES) -> str:
    return json.dumps(build(root, lines, cases), ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="fail if the committed file is stale")
    ap.add_argument("--fixture", type=Path, default=FIXTURE, help="the trout fixture release root (default: committed)")
    ap.add_argument("--lines", type=Path, default=LINES)
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args(argv)
    try:
        text = render(args.fixture, args.lines)
    except SystemExit as e:                     # a fixture that cannot be read: said, and this generator fails alone
        print(e, file=sys.stderr)
        return 1
    if args.check:
        if not args.out.exists() or args.out.read_text(encoding="utf-8") != text:
            print(f"{args.out} is stale: the trout fixture decodes to other cells than the committed vectors say. Run "
                  "scripts/trout-refresh.sh (never the steps by hand); the app's trout vector test must then pass on "
                  "the new file too.", file=sys.stderr)
            return 1
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    print(f"wrote {args.out} ({len(text):,} bytes, {len(CASES)} cases)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
