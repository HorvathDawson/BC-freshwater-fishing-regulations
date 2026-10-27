"""How this was decided (answers 2.6): each rule's book pages and why it applies here (`applies_here`,
`book_pages`), the words for why a rule lost (`REASON_WORDS`), and what a water inherits by the walk."""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.common import BOOK_TITLE, AnswersError, cap, join, range_txt
from pipeline.deliver.bundle.rules import closure_grade
from pipeline.deliver.answers.display.rule import kind_of, unless_opened


#: Why a rule LOST, in words, per reader reason (`read.LOSS_REASONS`): `lost` said of the loser,
#: `won` of the rule that beat it. "{by}" / "{rule}" are the other rule (the app writes its label
#: and row there).
REASON_WORDS = {
    "lifted": ("Lifted here by {by}.", "Lifts {rule} here."),
    "ladder": ("Replaced by {by}: closer to this water, or names the fish.", "Replaces {rule}."),
    "own_row": ("Replaced by this water’s own row: {by}.", "This water’s own row replaces {rule}."),
    "nearer_row": ("Replaced by {by}: its water is nearer this stream.",
                   "Its water is nearer this stream, so it replaces {rule}."),
    "water_dates": ("Replaced on these dates by {by}, which prints its own dates.",
                    "Its own dates replace {rule}."),
    "zone_release": ("Silenced by {by}: the table releases these fish.",
                     "Releases these fish, so {rule} is silent."),
    "closure": ("Out while {by} closes the water.", "Closes the water, so {rule} is out."),
    "water_closure": ("Out while this water’s own closure holds: {by}.",
                      "This water’s own closure silences {rule}."),
    "size_release": ("Covered by {by}, a release for those sizes.",
                     "Releases those sizes, so {rule} is silent."),
    "water_release": ("Silenced by {by}: this water releases these fish.",
                      "This water releases these fish, so {rule} is silent."),
    "same_row_release": ("Silent on the dates its own row releases or closes: {by}.",
                         "Its dates silence {rule} of the same row."),
    "moot_size_clause": ("Doesn’t matter today: {by} releases every one.",
                         "Releases every one, so {rule} doesn’t matter today."),
    "stricter_region": ("Replaced by {by}, the other region’s stricter rule.",
                        "Stricter than {rule} of the other region."),
    "same_as_peer": ("Same as {by}, the other region’s identical line.",
                     "Says what {rule} says, for the other region."),
}


def reasons_table() -> Dict[str, dict]:
    """`display.reasons`: {reason: {lost, won}} for every reader reason — refused if one is missing
    (a reason the reader gains must get its words here)."""
    from pipeline.deliver.bundle import read
    missing = sorted(set(read.LOSS_REASONS) - set(REASON_WORDS))
    if missing:
        raise AnswersError(f"display: no words for the reader's reasons {missing}")
    return {r: {"lost": REASON_WORDS[r][0], "won": REASON_WORDS[r][1]} for r in read.LOSS_REASONS}


def _region_of(entry_id: str) -> Optional[str]:
    reg = EID.zone_region(entry_id)
    return EID.region_words([reg]) if reg else None


def _lc_place(name: str, region: Optional[str] = None) -> str:
    """An entry name inside a sentence: "Every stream in Region 1" -> "every stream in Region 1";
    the region itself (Region 2's table, "Region 2") -> "all of Region 2"."""
    name = name.rstrip(". ")
    if region and name in (region, region.replace("Region 7", "Zone ")):
        return f"all of {region}"
    return re.sub(r"^(Every|Everywhere|Lakes|Streams|All)\b", lambda m: m.group(1).lower(), name)


def trib_owner(x: dict, entries: Optional[dict] = None) -> str:
    """The water whose row a walked rule comes from, as `applies_trib` names it ("Inherited from
    Quinsam River: …"): its entry's name (the export's entries, else the bundle rule's own
    `entry_name`)."""
    E = (entries or {}).get(x.get("entry_id") or x.get("entry")) or {}
    prov = x.get("provenance") or {}
    return E.get("name") or prov.get("entry_name") or x.get("entry_name") or \
        x.get("entry_id") or x.get("entry")


def _fx(x: dict) -> dict:
    """A rule's fields flat, from the export's form ({fields, type, …}) or the bundle's (flat)."""
    return {**x, **(x.get("fields") or {})}


def _parts(x: dict) -> dict:
    p = x.get("parts") or {}
    return json.loads(p) if isinstance(p, str) else p


def _days(when) -> set:
    from pipeline.deliver.calendar import when_vector
    return {d for d, v in enumerate(when_vector(when), start=1) if v}


def _days_txt(days: set) -> str:
    from pipeline.deliver.calendar import DAYS, LEAP, runs
    if len(days - {LEAP}) >= DAYS - 1:
        return "all year"
    return ", ".join(range_txt(a * 100 + b, c * 100 + d) for a, b, c, d in runs(days))


def inherited_kind(t: dict) -> Optional[str]:
    """What a lifted rule is, for an inherited lift's line: a closure ("Open …"), a release ("… may be
    kept …"), a gear rule ("No bait ban …"); None for a quota (the card names those lifts)."""
    f = _fx(t)
    k = kind_of(f)
    if k in ("gear", "conduct", "vessel"):
        return "gear"
    if k == "gate":
        return "closure" if closure_grade(f) is not None else "release"
    return None


def inherited_line(lifter: str, lifted: Sequence[str], doc: dict) -> str:
    """RULINGS P12: a lift the tributary walk carries from a ✱ row, in words — whose rule it is (the
    water `applies_trib` names), what it lifts and when: "Open Jul 15–Aug 31: the Quinsam River’s
    exemption from the summer stream closure (MUs 1-1 to 1-6) covers its tributaries." `doc`: the
    export ({rules, entries}) or the bundle's rules by id ({rules})."""
    from pipeline.deliver.answers.common import lc_names
    x = doc["rules"][lifter]
    ts = [doc["rules"][t] for t in lifted]
    fx = _fx(x)
    owner = trib_owner(x, doc.get("entries"))
    from pipeline.deliver.answers.common import the_name
    whose = f"{the_name(owner)}’s"                     # RULINGS P24: "the Quinsam River’s", "Trout Lake’s"
    days: set = set()
    for t in ts:
        days |= _days(_fx(t).get("when"))
    days &= _days(fx.get("when"))
    when = _days_txt(days)
    t0 = ts[0]
    # what it lifts: the row's own words ("Summer stream closure lifted — …"), else the lifted line
    label = x.get("label") or ""
    if re.search(r" lifted\b", label):
        what = re.split(r" lifted\b", label)[0].strip()
        first = what.split()[0] if what else ""
        tE = (doc.get("entries") or {}).get(t0.get("entry_id") or t0.get("entry")) or {}
        place = " ".join([tE.get("name") or "", t0.get("entry_name") or ""])
        what = "the " + (what if first and first in place else what[:1].lower() + what[1:])
    else:
        what = f"“{_parts(t0).get('what') or t0.get('label')}”"
    where = (_parts(t0).get("where") or "")
    if where.startswith("within "):
        what += " (" + where[len("within "):].replace("Management Units", "MUs") + ")"
    kind = inherited_kind(t0)
    sp = [c for c in fx.get("species") or [] if c != "ALL_GAME_FISH"]
    fish = lc_names(sp) if sp else None
    if kind == "closure":
        lead = "Open" + (f" for {fish}" if fish else "") + f" {when}"
    elif kind == "release":
        lead = f"{cap(fish or lc_names(_fx(t0).get('species') or []))} may be kept {when}"
    elif kind == "gear":
        lead = f"No {_parts(t0).get('what', '').lower() or 'such rule'} {when}"
    else:
        lead = cap(when)
    return f"{lead}: {whose} exemption from {what} covers its tributaries."


def applies_here(x: dict, doc: dict) -> Tuple[str, Optional[str]]:
    """(`applies`, `applies_trib`): one line saying why a rule binds where it does — its scope, by
    the export's provenance rank and entry — and, for a water row's rule the tributary walk can
    carry, the line said where it arrives by the walk ("Inherited from …"). A lift adds what it
    lifts."""
    prov = x.get("provenance") or {}
    rank = prov.get("rank")
    E = doc["entries"].get(x["entry_id"]) or {}
    name = E.get("name") or prov.get("entry_name") or x["entry_id"]
    region = _region_of(x["entry_id"])
    if region is None and rank == 2:
        # a water table's AREA row (the Liard watershed, Strathcona Park waters): its chapter
        ch = str(E.get("chapter") or "")
        region = EID.region_words([EID.region_of(ch)]) if EID.is_water_row(ch) and EID.region_of(ch) else None
    trib = None
    if rank is None:
        raise AnswersError(f"display: rule {x['id']} ships no provenance rank")
    name = trib_owner(x, doc["entries"])
    if rank < 0:
        say = f"Set by a federal or park authority — {_lc_place(name)}."
    elif rank == 4:
        say = f"A B.C.-wide rule — {_lc_place(name)}."
    elif rank == 3:
        say = f"From the {region} table — {_lc_place(name, region)}." if region else \
            f"A regional rule — {_lc_place(name)}."
    elif rank == 2:
        say = f"For an area — {_lc_place(name)}" + (f", in {region}." if region else ".")
    elif E.get("kind") == "water":
        # RULINGS P41: the book named by its title
        say = f"This water’s own row in the {BOOK_TITLE}: {name}."
        trib = f"Inherited from {name}: its row covers its tributaries."
    else:
        # a table's line about named waters (Zone B's "all from Williston Lake and its
        # tributaries"): for, and inherited from, the waters it names
        def water(i):                          # a lake part is said as its lake (Williston)
            w = doc["waters"].get(i) or {}
            return (doc["waters"].get(w.get("part_of")) or w).get("name") or i
        waters = join(list(dict.fromkeys(water(e["item_id"])
                                         for e in (x.get("fields") or {}).get("extents") or []
                                         if e.get("item_id"))))
        say = f"From the {region} table, for {waters or _lc_place(name, region)}." if region \
            else f"A B.C. rule for {waters or _lc_place(name)}."
        trib = (f"Inherited from {waters or _lc_place(name)}: the "
                f"{region + ' table' if region else 'B.C. rule'} covers its tributaries.")
    lifts = [(e.get("entry_id"), e.get("rule_id")) for e in (x.get("fields") or {}).get("exempts") or []]
    if lifts:
        words = []
        for e, r in lifts:
            t = doc["rules"].get(f"{e}::{r}")
            if t is None:
                raise AnswersError(f"display: {x['id']} lifts {e}::{r}, which the export lacks")
            tn = (doc["entries"].get(e) or {}).get("name") or e
            words.append(f"“{t['label']}” ({_lc_place(tn)})")
        lift = " It lifts " + join(words) + " here."
        say += lift
        trib = trib + lift if trib else None
    # THE AREA CAVEAT (T100, owner Q43 default): a rule bound to the whole water that holds only in
    # an undrawn part says so wherever the page says why it applies — the export's sentence, once
    nym = x.get("not_yet_mapped")
    if nym:
        say += " " + nym["says"]
    return say, trib


def book_pages(x: dict, doc: dict) -> List[List[int]]:
    """[[printed page, PDF page]] of the rule's entry: the PRINTED page (the book's own address,
    the entry's `pages`) and the 1-based PDF page it is on (`guide.pdf_pages`, the one map —
    `pipeline.common.synopsis_pages`), for opening the official synopsis at `#page=N`."""
    E = doc["entries"].get(x["entry_id"]) or {}
    pages = E.get("pages") or []
    pdf = (doc.get("guide") or {}).get("pdf_pages") or {}
    if not pages:
        raise AnswersError(f"display: rule {x['id']} — its entry {x['entry_id']} prints no page")
    # T100: a rule in an undrawn part the book draws on a map (the export's `not_yet_mapped.map`,
    # `pipeline.deliver.export.read.book_map`) cites that map's page too — Shuswap's rows on p.32, maps on p.28
    mp = (x.get("not_yet_mapped") or {}).get("map")
    if mp and mp["page"] not in pages:
        pages = [*pages, mp["page"]]
    out = []
    for p in pages:
        q = pdf.get(str(p))
        if q is None:
            raise AnswersError(f"display: printed page {p} ({x['entry_id']}) is not in the "
                               f"guide's pdf_pages")
        out.append([int(p), int(q)])
    return out


_BOXES: dict = {}


def _row_boxes() -> dict:
    """T215: the synopsis rows' boxes, `{(printed page, water): [{pdf, box, size}]}`, from the extractor's own
    `row_boxes.json` sidecar (`pipeline.regs.extraction.row_boxes`) — and only when it was cut from THE synopsis PDF the
    release ships (its `pdf_sha256`); a sidecar of another PDF, or none, gives no boxes (the page opens unhighlighted)."""
    if "rows" in _BOXES:
        return _BOXES["rows"]
    out: dict = {}
    try:
        from pipeline.common.synopsis_pages import default_pdf
        from pipeline.regs.extraction.row_boxes import default_out, sha256
        side = json.loads(default_out().read_text(encoding="utf-8"))
        if side.get("pdf_sha256") == sha256(default_pdf()):
            for r in side["rows"].values():
                out.setdefault((int(r["printed"]), r["water"]), []).append(
                    {"pdf": int(r["pdf_page"]), "box": [float(v) for v in r["box"]],
                     "size": [float(v) for v in r["page_size"]]})
    except (OSError, ValueError, KeyError):
        out = {}
    _BOXES["rows"] = out
    return out


def book_boxes(x: dict, doc: dict) -> List[dict]:
    """T215: where the rule's entry is printed on its PDF page — `[{pdf, box [x0, top, x1, bottom], size [w, h]}]`
    in points, top-left origin — for the app's highlight annotation. A water row only (the zone and province tables
    are not rows in the sidecar): [] where the entry has no row box."""
    E = doc["entries"].get(x["entry_id"]) or {}
    name = E.get("official_name") or E.get("name")
    rows = _row_boxes()
    return [b for p in E.get("pages") or [] for b in rows.get((int(p), name), [])]


def licensing_books(doc: dict, lic_ids: Sequence[str]) -> List[dict]:
    """answers 2.7 (A3): every licensing record's book pages ({book}, aligned with the export's
    `licensing_ids`) — the page cites a licensing record ("Source" on a Classified Water, an
    exemption) as it cites a rule, so it opens the synopsis at the same `#page=N`."""
    out = []
    for k in lic_ids:
        x = (doc.get("licensing") or {}).get(k)
        if x is None:
            raise AnswersError(f"display: licensing record {k} is not in the decoded export")
        out.append({"book": book_pages({"entry_id": x["entry_id"], "id": k}, doc)})
    return out


def add_sources(facts: List[dict], doc: dict, order: Sequence[str]) -> None:
    """Each rule's book pages and why it applies (`book`, `applies`, `applies_trib`) onto its
    display facts, in the export's rule order (`order`: rule ids). Only the rules a part reaches
    by the tributary walk somewhere carry `applies_trib`."""
    walked = {k for rs in doc["rulesets"].values() for k in rs.get("trib") or []}
    for f, rid in zip(facts, order):
        x = doc["rules"][rid]
        f["book"] = book_pages(x, doc)
        boxes = book_boxes(x, doc)
        if boxes:
            f["book_box"] = boxes
        say, trib = applies_here(x, doc)
        f["applies"] = say
        if rid in walked:
            if trib is None:
                raise AnswersError(f"display: {rid} reaches a part by the walk but is bound to "
                                   f"no water (provenance rank {x['provenance'].get('rank')})")
            f["applies_trib"] = trib


def _unless_words(ctx, rid: str) -> Optional[str]:
    got = ctx.cache.setdefault("display_unless", {})
    if rid not in got:
        got[rid] = unless_opened(ctx.by_id[rid], ctx.B.rules)
    return got[rid]
