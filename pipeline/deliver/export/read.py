"""Reading the bundle into the export's model (`read`).

Entries, rules (keyed `entry_id::rule_id`), licensing records (`entry_id#record_id`), the document register,
the interned rule and licensing sets, every named water with the sets its parts carry, and the splits by
name. Everything comes from `bundle.sqlite`; nothing is settled here. Section handles never leave the
bundle (AGENTS 5), so membership is exported per set and per named water, never per section.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle.place_names import display_case
from pipeline.regs.parsing import catalogue as C
from pipeline.deliver.bundle.read import source_of
from pipeline.common import entry_id as EID
from pipeline.deliver.export.contract import _need
from pipeline.deliver.export.guide_words import NOT_PLACED, TIDAL_GUIDE
from pipeline.deliver.export.steelhead import add_steelhead_rules


# --------------------------------------------------------------------------------------------
# Reading the bundle
# --------------------------------------------------------------------------------------------

def _j(s, empty=None):
    return json.loads(s) if s else empty


def _rows(db, sql, *args):
    cur = db.execute(sql, args)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _entry_kind(entry_id: str, extents: list) -> str:
    """province (`zp:`), water (`r<n>:`), or — for a zone chapter (`z<n>:`) — `area` when the
    entry's own extents name a place smaller than a region, `zone` otherwise."""
    if EID.is_province(entry_id):
        return "province"
    if EID.is_water_row(entry_id):
        return "water"
    for x in extents:
        if x.get("op") != "within":
            continue
        if (x.get("area_id") and not x["area_id"].startswith("area:region:")) or \
                x.get("area_kind") not in (None, "", "region"):
            return "area"
    return "zone"


#: Bundle columns that are the rule's own model fields, under their model (alias) names.
_RULE_COLUMNS = (("when_", "when"), ("while_", "while"), ("species", "species"),
                 ("species_except", "species_except"), ("take", "take"),
                 ("may_target", "may_target"), ("standing", "standing"),
                 ("exempts", "exempts"), ("extent_text", "extent_text"),
                 ("undrawn_part", "undrawn_part"))
#: Of those, the ones the bundle stores as JSON text.
_JSON_COLUMNS = frozenset({"when_", "while_", "species", "species_except", "exempts"})


def _rule_record(r: dict, entry_name: str, entry: dict | None = None) -> dict:
    """One rule as the bundle ships it: its columns and its `conditions`, both under the model's
    own field names, empty values left out."""
    fields = {}
    for col, name in _RULE_COLUMNS:
        v = r[col]
        if col in _JSON_COLUMNS:
            v = _j(v)
        if col == "may_target" and v is not None:
            v = bool(v)
        if col == "standing":
            if not v:
                continue
            v = True
        if v is None or v == "" or v == [] or v == {}:
            continue
        fields[name] = v
    for k, v in _j(r["conditions"], {}).items():
        if k in fields:
            raise SystemExit(f"{r['entry_id']}::{r['rule_id']}: `{k}` is both a column and a "
                             f"condition — the bundle says it twice")
        fields[k] = v
    s = source_of({"entry": r["entry_id"], "rule": r["rule_id"], "entry_name": entry_name,
                   "extents": fields.get("extents") or [],
                   "authority": fields.get("authority"),
                   "extent_text": r["extent_text"], "undrawn_part": r["undrawn_part"],
                   "verbatim": r["verbatim"]})
    return {
        "id": f"{r['entry_id']}::{r['rule_id']}",
        "entry_id": r["entry_id"], "rule_id": r["rule_id"],
        "type": r["type"], "family": r["family"], "dimension": r["dimension"],
        "label": r["label"], "parts": _j(r["parts"], {}), "verbatim": r["verbatim"],
        "binds": _binds(r),
        **({"not_yet_mapped": not_yet_mapped(r, r["verbatim"] or "", entry)}
           if _binds(r) == "sections_in_part" else {}),
        "fields": dict(sorted(fields.items())),
        "provenance": {
            "entry_name": entry_name,
            "region": EID.region_of(r["entry_id"]),
            "authority": s.authority.value,
            "binds_to": s.scope.value,
            "rank": s.rank,
            "who": s.words(),
            "scope": r["scope"],
            "uncertain": bool(r["uncertain"]),
            "why": r["unresolved"],
        },
    }


#: THE RECORD DUTY OF AN ANNUAL QUOTA, linked once, from the book. A page rendering "The annual
#: province-wide quota for hatchery steelhead is 10" added its own "Record each one you keep on
#: your licence" under it, beside the printed zp:steelhead.r4 "You must immediately record your
#: retention of hatchery steelhead on your basic angling licence" — the same duty twice, one of
#: them invented. The link is DERIVED here, never authored: a rule carrying `record_retention: true` is an annual
#: quota's record duty when it names the same fish (groups expanded, `species_except` ignored —
#: no annual quota or record rule carries one), the same origin or none, the same dates or none,
#: and is IN FORCE WHEREVER THE QUOTA IS (in every rule set that holds the quota). Exactly one such
#: rule links (`recorded_by` on the quota, `records_for` on the record rule); two would be
#: ambiguous and are refused by `problems`; none leaves the quota without a record line.
def _fish(codes) -> set:
    out: set = set()
    for c in codes or ():
        out |= set(C.SPECIES_GROUPS.get(c) or (c,))
    return out


def _held(rulesets: dict) -> dict:
    held = defaultdict(set)
    for sid, s in rulesets.items():
        for via, ids in s.items():
            if via != "sections":
                for i in ids:
                    held[i].add(sid)
    return held


def record_candidates(rules: dict, rulesets: dict) -> dict[str, list[str]]:
    """Every annual quota -> the record-duty rules that qualify as its own (sorted)."""
    held = _held(rulesets)
    quotas = sorted(k for k, x in rules.items() if x["fields"].get("period") == "annual")
    records = sorted(k for k, x in rules.items() if x["fields"].get("record_retention"))
    out = {}
    for q in quotas:
        fq = rules[q]["fields"]
        out[q] = [r for r in records
                  if _fish(fq.get("species")) & _fish(rules[r]["fields"].get("species"))
                  and rules[r]["fields"].get("origin") in (None, fq.get("origin"))
                  and rules[r]["fields"].get("when") in (None, fq.get("when"))
                  and held[q] and held[q] <= held[r]]
    return out


def record_link_problems(doc: dict) -> list[str]:
    """Every annual quota whose record duty qualifies is linked to it, both ways, and nothing
    else is linked. Run on the OUTPUT."""
    R, out = doc["rules"], []
    cands = record_candidates(R, doc["rulesets"])
    for q, c in cands.items():
        got = R[q].get("recorded_by")
        if len(c) > 1:
            out.append(f"annual quota {q}: record duty is ambiguous ({', '.join(c)})")
        elif c and got != c[0]:
            out.append(f"annual quota {q}: not linked to its record duty {c[0]}")
        elif not c and got:
            out.append(f"annual quota {q}: linked to {got}, which is not its record duty")
    for i, x in R.items():
        if "recorded_by" in x and i not in cands:
            out.append(f"{i}: recorded_by on a rule that is not an annual quota")
        for q in x.get("records_for") or []:
            if (R.get(q) or {}).get("recorded_by") != i:
                out.append(f"{i}: records_for {q}, which is not recorded by it")
        by = x.get("recorded_by")
        if by and i not in ((R.get(by) or {}).get("records_for") or []):
            out.append(f"{i}: recorded_by {by}, which does not list it in records_for")
    return out


def _binds(r: dict) -> str:
    if r["uncertain"]:
        return "nowhere"
    return "sections_in_part" if r["undrawn_part"] else "sections"


def not_yet_mapped(r: dict, verbatim: str = "", entry: dict | None = None) -> dict:
    """THE FLAG A RULE IN AN UNDRAWN PART CARRIES (user ruling 2026-09-26): it holds only in a part
    of its water that nothing draws, so it is placed on the whole water to be SEEN there — at the
    top, marked as a place not yet mapped — and it never decides the water (`read.not_yet_mapped`:
    it never competes, displaces, lifts or suspends).

    THE AREA CAVEAT (T100, RULINGS K5, owner Q43 default 2026-10-23): such a rule keeps binding the
    WHOLE water, and `says` is the caveat line every page shows beside it (the answers' `applies`).
    Where the book prints a map of the part (`book_map`: Shuswap's maps A/B/C and Mahood's area
    closure on p.28, Canim's on p.42, François's map 1 on p.51), `map` names it and `says` cites
    it, so the page never overstates the part as the whole lake and points to the book's drawing."""
    part = str(r["undrawn_part"]).strip()
    identified = C.part_identifies_place(part)
    mp = book_map(part, verbatim, entry or {})
    return {"display": "prominent", "part": part, "identified": identified,
            **({"map": mp} if mp else {}),
            "says": _not_yet_mapped_says(part, mp) if identified else _unidentified_says(part)}


#: A MAP THE BOOK NAMES FOR A PART (T100). The part or the rule's line names it ("(map A)",
#: "described on map 1"); the rule's line or the row's heading says where it is printed ("(see
#: map on page 28)", "SHUSWAP LAKE (see maps on page 28)", "MAHOOD LAKE (see map on page 28 for area
#: closure)"), or that it is printed with the row ("map 1, (see below)", "(see map below)"). Book
#: page numbers are PRINTED pages (the book's own address, as `entries[].pages`).
_MAP_NAMED = re.compile(r"\bmap\s+([A-Z]|\d{1,2})\b")
_MAP_TAG = re.compile(r"\s*\(\s*map\s+(?:[A-Z]|\d{1,2})\s*\)")
_SEE_MAP_ON_PAGE = re.compile(r"\bsee\s+(maps?)\s+(?:[A-Z]\s+|\d{1,2}\s+)?on\s+page\s+(\d{1,3})\b", re.I)
_SEE_MAP_BELOW = re.compile(r"\bsee\s+(maps?)\s+below\b|\bmap\s+(?:[A-Z]|\d{1,2}),?\s*\(see\s+below\)", re.I)

#: A ROW'S MAP THAT DOES NOT DRAW EVERY PART ON THE ROW (T100 review). A row heading's "(see map on
#: page N)" is the row's, not each rule's: a part the printed map does not show must not cite it.
#: (row name prefix, part pattern) — each verified against the printed map and pinned by
#: test_t100_area_caveat (every pair still matches a widened rule).
#:  * MAHOOD LAKE (p.31, map p.28 "for area closure"): the map shows only the western tip at the
#:    Canim River mouth (the Jan 1-June 30 sign line); the Mahood River outlet is at the lake's far
#:    (east) end, off the map.
ROW_MAP_DOES_NOT_DRAW = (
    ("MAHOOD LAKE", re.compile(r"\bMahood River outlet\b", re.I)),
)


def book_map(part: str, verbatim: str, entry: dict) -> dict | None:
    """THE BOOK'S MAP OF AN UNDRAWN PART, or None (T100: the one place this is decided). Only a
    part that names a place has one (an unidentified "on parts" is drawn nowhere, so no map is cited
    for it even where its row prints one — the Cowichan River's "see map below"). The map's page is
    the first of: "see map(s) … on page N" in the rule's own line, the same in the row's heading,
    the row's own (single) printed page where it says the map is "below". A map NAMED but with no
    page found is None here and refused by `undrawn_map_problems`. A part the row's map is known
    not to draw (`ROW_MAP_DOES_NOT_DRAW`: Mahood's outlet) cites nothing.

    `{"name": "map A" | None, "page": <printed page>, "cite": "map A on p.28" | "the map on p.42" |
    "the maps on p.28"}` — `cite` is the words `says` uses."""
    if not str(part or "").strip() or not C.part_identifies_place(part):
        return None
    row = str(entry.get("full_name") or "").upper()
    if any(row.startswith(n) and p.search(part) for n, p in ROW_MAP_DOES_NOT_DRAW):
        return None
    named = _MAP_NAMED.search(part) or _MAP_NAMED.search(verbatim or "")
    name = f"map {named.group(1)}" if named else None
    page = plural = None
    for text in (verbatim or "", entry.get("full_name") or ""):
        m = _SEE_MAP_ON_PAGE.search(text)
        if m:
            page, plural = int(m.group(2)), m.group(1).lower() == "maps"
            break
    if page is None:
        pages = entry.get("pages") or []
        for text in (entry.get("full_name") or "", entry.get("printed") or ""):
            m = _SEE_MAP_BELOW.search(text)
            if m and len(pages) == 1:
                page, plural = int(pages[0]), (m.group(1) or "").lower() == "maps"
                break
    if page is None:
        return None
    cite = f"{name} on p.{page}" if name else f"the {'maps' if plural else 'map'} on p.{page}"
    return {"name": name, "page": page, "cite": cite}


def undrawn_map_problems(doc: dict) -> list[str]:
    """T100: a part that names a map must cite the page it is printed on, and every cited page
    must open the PDF (`guide.pdf_pages`) — the page links to it."""
    pdf = doc["guide"].get("pdf_pages") or {}
    out = []
    for kind in ("rules", "licensing"):
        for k, x in doc[kind].items():
            n = x.get("not_yet_mapped")
            if not n:
                continue
            mp = n.get("map")
            if mp is None and n.get("identified") and _MAP_NAMED.search(n["part"]):
                out.append(f"{k}: its part names a map ({n['part']!r}) but no page is found for it "
                           f"in its line or its row (`book_map`)")
            if mp is not None and str(mp["page"]) not in pdf:
                out.append(f"{k}: cites {mp['cite']}, which guide.pdf_pages does not map")
    return out


def _not_yet_mapped_says(part: str, mp: dict | None = None) -> str:
    # "applies only {part}" read "applies only waters lying west of …" / "only the area at …" for
    # about half the 131 parts; the part is quoted as a phrase instead. A part the book draws on a
    # map cites it (T100); its "(map A)" tag is then said by the cite, not twice.
    if mp:
        shown = _MAP_TAG.sub("", part).strip() if mp["name"] else part
        return (f"Not yet mapped: this applies only in one part of this water — {shown} — which is "
                f"not drawn on the map yet; see {mp['cite']} of the regulations. It does not "
                f"apply to the rest of the water.")
    return (f"Not yet mapped: this applies only in one part of this water — {part} — which is "
            f"not drawn on the map yet. It does not apply to the rest of the water.")


def _unidentified_says(part: str) -> str:
    """A PART THE BOOK NEVER IDENTIFIES ("on parts", "various locations"): quoted as a place it read
    "applies only in one part of this water — on parts —". Said instead as what is true: the
    regulations do not say which parts, so no map can draw them (`catalogue.UNIDENTIFIED_PART`)."""
    m = C.UNIDENTIFIED_PART.match(C.strip_list_marker(part))
    if m.group("signed"):
        where = ("only on parts of this water marked on site by buoys and signs; the regulations "
                 "do not identify them, so they are not drawn on the map")
    elif (m.group("n") or "").lower() == "part":
        where = ("only on one part of this water, which the regulations do not identify, so it "
                 "is not drawn on the map")
    else:
        where = ("only on parts of this water that the regulations do not identify, so they are "
                 "not drawn on the map")
    return f"Not yet mapped: this applies {where}. It does not apply to the rest of the water."


#: The licensing tables, their id column, and whether the kind is placed.
_LIC_TABLES = (("designation", "designation_id", True), ("not_classified", "not_classified_id", True),
               ("requirement", "req_id", True), ("licence_terms", "terms_id", False),
               ("exemption", "exemption_id", False), ("alternative", "alternative_id", True))


#: THE CLASSIFIED PERIOD, said on every designation. 35 of 74 print no dates: "Class II water when
#: open" (Elk, Bull, Michel, Wigwam, St. Mary, White, Skookumchuck, Stellako …) or "Class I water
#: all year" (Lakelse, Gitnadoix, Kitsumkalum, Suskwa …), and a page reading only `fields.when` said
#: "during its classified period" with no dates. "When open" has no `when` BY DESIGN (licensing
#: decision 6: licensing is consulted only while the water is open, so the designation's period is
#: the water's open time); this says so explicitly, derived from the record's own verbatim and
#: `when`, and refuses a designation whose period cannot be read off it.
_WHEN_OPEN = re.compile(r"\bwhen(?:/where)?\s+open\b", re.I)
_ALL_YEAR = re.compile(r"\ball\s+year\b", re.I)
_PRINTS_DATES = re.compile(r"\b(?:jan|feb|mar|apr|may|june?|july?|aug|sept?|oct|nov|dec)[a-z]*\.?\s*\d",
                           re.I)


def designation_period(rid: str, fields: dict, verbatim: str, parts: dict) -> dict:
    """`{kind: when_open | all_year | dates, dates?, says}` for one designation, from its own
    verbatim and `when`. Exactly one reading must hold: dates in `when` and printed in the verbatim
    (and no 'when open' / 'all year'), or no `when` and the verbatim printing exactly one of 'when
    open' ('when/where open') and 'all year'. Anything else is refused."""
    cls = fields.get("classified")
    when = fields.get("when")
    v = str(verbatim or "")
    is_open, all_year, dated = bool(_WHEN_OPEN.search(v)), bool(_ALL_YEAR.search(v)), \
        bool(_PRINTS_DATES.search(v))
    if when:
        if set(when) != {"dates"} or not when["dates"]:
            raise SystemExit(f"designation {rid}: its `when` is not plain dates ({when}) — a "
                             f"classified period is a set of dates, all year, or 'when open'")
        if is_open or all_year or not dated:
            raise SystemExit(f"designation {rid}: `when` has dates but the verbatim "
                             f"{'says when open' if is_open else 'says all year' if all_year else 'prints none'}"
                             f": {v!r}")
        return {"kind": "dates", "dates": when["dates"],
                "says": f"Classified (Class {cls}) {parts.get('when') or ''}".strip()}
    if dated or is_open == all_year:
        raise SystemExit(f"designation {rid}: no classified period can be read — no `when`, and "
                         f"the verbatim {'prints dates' if dated else 'says both' if is_open else 'says neither when open nor all year'}"
                         f": {v!r}")
    if is_open:
        return {"kind": "when_open", "says": f"Classified (Class {cls}) whenever this water is open"}
    return {"kind": "all_year", "says": f"Class {cls} all year"}


#: "(Steelhead Stamp not required)" with no "unless" — `catalogue.Designation.waives_every_stamp`.
_UNLESS = re.compile(r"\bunless\b", re.I)
_MARKUP = re.compile(r"\*\*|\[[^\]]*\]")


def add_stamp_waivers(licensing: dict, names: dict) -> None:
    """THE STAMP WAIVER, said on the designation that prints it (user ruling 2026-10-02):
    `stamp_waiver: {outright, lifts, says}`. OUTRIGHT ("(Steelhead Stamp not required)") lifts every
    steelhead stamp on the designation's sections while it is in force — the classified-water stamp
    and each requirement that consents (`fields.waived_where`: the provincial stamp to fish for
    steelhead); a waiver "unless fishing for steelhead" lifts the classified-water stamp only.
    Neither lifts the Classified Waters Licence or a steelhead rule. Derived, like `period`, from
    the record's own fields; never authored."""
    consents = sorted(k for k, x in licensing.items() if x["kind"] == "requirement"
                      and x["fields"].get("waived_where") == "steelhead_stamp_waived")
    for k, x in licensing.items():
        w = x["fields"].get("steelhead_stamp_waived") if x["kind"] == "designation" else None
        if not w:
            continue
        outright = not _UNLESS.search(w["verbatim"])
        per = x["period"]
        when = ("whenever this water is open" if per["kind"] == "when_open"
                else "all year" if per["kind"] == "all_year" else x["parts"].get("when", ""))
        row = re.sub(r"\s+", " ", _MARKUP.sub("", x["verbatim"])).strip()
        name = names.get(x["entry_id"], "") or x["provenance"]["entry_name"]
        unit = x["fields"].get("unit_name") or ""
        if unit and unit.lower() != name.lower():
            name = f"{name} ({unit})"            # the Dean's "Anahim Lake to Iltasyuko River"
        says = (f"{name}: no Steelhead Stamp is required where this designation holds "
                f"(“{row}”), {when} — neither the classified-water stamp nor the Conservation "
                f"Surcharge Stamp to fish for steelhead. The Classified Waters Licence and the "
                f"steelhead rules still apply." if outright else
                f"{name}: the classified-water Steelhead Stamp is not required where this "
                f"designation holds (“{row}”), {when}, unless you fish for steelhead — then the "
                f"Conservation Surcharge Stamp is required as everywhere.")
        x["stamp_waiver"] = {"outright": outright,
                             "lifts": consents if outright else [],
                             "verbatim": w["verbatim"], "says": says}


def recorded_rules(rules: dict) -> list[str]:
    """Every rule carrying `record_retention: true` (`entry_id::rule_id`), sorted — the fish you
    must record."""
    return sorted(k for k, x in rules.items() if x["fields"].get("record_retention"))


def add_recorded(licensing: dict, rules: dict) -> None:
    """`records` on the "carry your paper licence" duty (`doing.act: retaining_recorded`): WHICH
    fish it is about — every record-duty rule, DERIVED, never typed (UI consumer's report,
    2026-10-03). The bundle's `parts.records` says the same fish in words."""
    got = recorded_rules(rules)
    for x in licensing.values():
        if (x["fields"].get("doing") or {}).get("act") == "retaining_recorded":
            x["records"] = list(got)


def protected_problems(doc: dict) -> list[str]:
    """Every protected fish a rule names is in `species.protected`, and every protected-species
    rule names at least one fish (the group alone, with no members, is what hid them). Run on the
    OUTPUT."""
    out, known = [], set(doc["species"].get("protected") or {})
    for k, x in doc["rules"].items():
        sp = x["fields"].get("species") or []
        if "PROTECTED_SPECIES" in sp:
            out.append(f"{k}: names PROTECTED_SPECIES, not the protected fish its row prints")
        bad = [c for c in sp if c in C.PROTECTED_FISH and c not in known]
        if bad:
            out.append(f"{k}: protected fish {bad} missing from species.protected")
    if not any(c in C.PROTECTED_FISH for x in doc["rules"].values()
               for c in x["fields"].get("species") or []):
        out.append("no rule names a protected fish")
    return out


def paper_licence_problems(doc: dict) -> list[str]:
    """The paper-licence duty names every record-duty rule and nothing else, and says them in its
    line. Run on the OUTPUT."""
    out, want = [], recorded_rules(doc["rules"])
    duties = [x for x in doc["licensing"].values()
              if (x["fields"].get("doing") or {}).get("act") == "retaining_recorded"]
    if not duties:
        out.append("no 'carry your paper licence' duty (doing.act retaining_recorded)")
    for x in duties:
        if x.get("records") != want:
            out.append(f"{x['id']}: records {x.get('records')} != the record-duty rules {want}")
        if not (x.get("parts") or {}).get("records"):
            out.append(f"{x['id']}: its line does not say which fish (parts.records)")
    for x in doc["licensing"].values():
        if "records" in x and x not in duties:
            out.append(f"{x['id']}: records on a record that is not the paper-licence duty")
    return out


def licence_classes(lic: dict) -> dict:
    """THE LICENCE CLASSES THE BOOK SELLS (p.5, "Licence Fees"; UI consumer's report 2026-10-03):
    each `licence_terms` record carrying a `name` — the document, who may buy it (a residency the
    table marks ★ is left out of `who`), how long it runs, and the printed fee per residency. The
    angler stays unknown: every class is said for the anglers it is sold to."""
    classes = []
    for k, x in sorted(lic.items()):
        f = x["fields"]
        if x["kind"] != "licence_terms" or not f.get("name"):
            continue
        classes.append({"id": k, "document": f["document"], "name": f["name"],
                        "who": f.get("who"), "classified": f.get("classified"),
                        "runs": ("licence_year" if f.get("sold") == "per_licence_year" else
                                 "per_day" if f.get("sold") == "per_day" else
                                 f"{f['valid_days']}_days" if f.get("valid_days") else None),
                        "fees_cad": f.get("fees_cad"), "label": x["label"]})
    return {
        "reading": "One record per class the fee table prints (p.5). `who` is who may buy it — "
                   "the table's ★ ('Not available') is a residency left out; `runs` is "
                   "licence_year (April 1 to March 31, from the date bought), per_day (a "
                   "Classified Waters day licence, at most 8 consecutive days each), or a fixed "
                   "run of days (1_days: One Day; 8_days: Eight Day, 'covering 8 consecutive "
                   "days'); `fees_cad` the printed fee for each residency. Say every class "
                   "conditionally ('if you are a non-resident …') — the angler is unknown.",
        "fees": "As printed in the 2025-2027 synopsis, taxes not included. FEES CHANGE from year "
                "to year and may change during the synopsis's term: show them as the book's, "
                "with its date, and send the angler to www.gov.bc.ca/fish-licence for today's.",
        "notes": [
            "You may buy as many One Day and Eight Day Licences as you need, but only ONE Annual "
            "Licence (p.5).",
            "Members of the Canadian military, students returning to B.C., and youth under 18 "
            "returning to B.C. to reside with a parent or guardian who is a resident, may be "
            "eligible to purchase licences at the resident rate (p.5).",
            "B.C. residents aged 65 and over may buy the annual basic licence at $5.71 or at the "
            "full resident rate; the angling is the same (p.5).",
            "Under 16: a B.C. resident needs no licence or stamp; a non-resident needs none but "
            "must be accompanied by a licence holder 16 or older, whose quota their catch counts "
            "toward, unless they buy their own (p.6, `zp:basic_licence`).",
            "Stamps (Conservation Surcharge Stamps) and the White Sturgeon Conservation Licence "
            "validate a basic licence; up to five annual stamps per licence (p.6).",
        ],
        "residency": {
            "page": 80,
            "resident": "your primary residence is in British Columbia, AND (a) you are a "
                        "Canadian citizen or landed immigrant and have been physically present in "
                        "B.C. for the greater portion of each of 6 calendar months out of the 12 "
                        "immediately preceding, OR (b) you are not a Canadian citizen or landed "
                        "immigrant but have been physically present in B.C. for the greater "
                        "portion of each of the immediately preceding 12 calendar months",
            "non_resident": "you are not a 'resident', but (a) you are a Canadian citizen or "
                            "landed immigrant, OR (b) your primary residence is in Canada and "
                            "you have resided in Canada for the preceding 12 months",
            "non_resident_alien": "you are neither a 'resident' nor a 'non-resident'",
        },
        "classes": classes,
    }


def _licensing_record(kind: str, idcol: str, placed: bool, r: dict, entry_name: str,
                      entry: dict | None = None) -> dict:
    rec = _j(r["record"])
    if rec.get("kind") != kind or rec.get("id") != r[idcol]:
        raise SystemExit(f"{r['entry_id']}#{r[idcol]}: the `record` JSON is a "
                         f"{rec.get('kind')} {rec.get('id')!r}, the row a {kind} {r[idcol]!r}")
    fields = {k: v for k, v in rec.items() if k not in ("kind", "id", "verbatim")}
    parts = _j(r["parts"], {})
    return {
        "id": f"{r['entry_id']}#{r[idcol]}",
        "entry_id": r["entry_id"], "record_id": r[idcol], "kind": kind,
        "label": r["label"], "parts": parts, "verbatim": r["verbatim"],
        **({"period": designation_period(f"{r['entry_id']}#{r[idcol]}", fields, r["verbatim"],
                                         parts)} if kind == "designation" else {}),
        **({"not_yet_mapped": not_yet_mapped(fields, r["verbatim"] or "", entry)}
           if str(fields.get("undrawn_part") or "").strip() else {}),
        "fields": dict(sorted(fields.items())),
        "placement": r["placement"] if placed else NOT_PLACED,
        "provenance": {
            "entry_name": entry_name,
            "uncertain": bool(r.get("uncertain") or 0),
            "why": r.get("unresolved"),
        },
    }


def section_counts(db) -> dict:
    """The bundle's section tallies (`about.sections`): counts, never a grouping into parts."""
    return {
        "total": db.execute("SELECT COUNT(*) FROM (SELECT sid FROM section_ruleset UNION "
                            "SELECT sid FROM section_licensing UNION "
                            "SELECT sid FROM item_section)").fetchone()[0],
        "with_a_ruleset": db.execute("SELECT COUNT(*) FROM section_ruleset").fetchone()[0],
        "with_a_licensing_set": db.execute("SELECT COUNT(*) FROM section_licensing").fetchone()[0],
        "on_a_named_water": db.execute("SELECT COUNT(DISTINCT sid) FROM item_section").fetchone()[0],
        "outside_bc": db.execute("SELECT COUNT(*) FROM outside_bc").fetchone()[0],
        "tidal": db.execute("SELECT COUNT(*) FROM tidal").fetchone()[0],
        "province_except": dict(db.execute("SELECT area_kind, COUNT(*) FROM province_except "
                                           "GROUP BY 1 ORDER BY 1").fetchall()),
        "anadromous_rainbow": db.execute("SELECT COUNT(DISTINCT sid) FROM steelhead_water")
                                .fetchone()[0],
        "steelhead": {("known", "possible")[c - 1]: n for c, n in db.execute(
            "SELECT code, COUNT(*) FROM section_steelhead GROUP BY 1 ORDER BY 1")},
    }


def read(bundle: Path) -> dict:
    """Everything the export ships, straight from the bundle."""
    if not Path(bundle).exists():
        raise SystemExit(f"pipeline.deliver.export: no bundle at {bundle} — build one with "
                         f"`python -m pipeline.deliver.bundle`")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    _need(db)
    meta = dict(db.execute("SELECT k, v FROM meta WHERE k != 'schema'"))

    entries, names = {}, {}
    for e in _rows(db, "SELECT * FROM entry ORDER BY entry_id"):
        extents = _j(e["extents"], [])
        # A DISPLAYED NAME NEVER SHOUTS: a row the book prints in capitals ("ENDAKO RIVER") is
        # shown "Endako River" (`display_case`); `official_name` keeps the spelling it came with.
        name = display_case(e["name"]) if e["name"] else e["name"]
        names[e["entry_id"]] = name or ""
        entries[e["entry_id"]] = {
            "kind": _entry_kind(e["entry_id"], extents),
            "chapter": EID.head_of(e["entry_id"]),
            "name": name, **({"official_name": e["name"]} if name != e["name"] else {}),
            "full_name": e["full_name"],
            "item_id": e["item_id"], "matched": _j(e["matched"], []),
            "mus": _j(e["mus"], []), "pages": _j(e["pages"], []),
            "symbols": _j(e["symbols"], []), "scope_note": e["scope_note"],
            "extents": extents, "printed": e["verbatim"],
            "rules": [], "licensing": [],
            # POINTERS ("See Lonzo Creek") — only on a row that prints one (see `guide.entries`).
            **({"see": _j(e["see"], [])} if e["see"] else {}),
        }

    rules = {}
    for r in _rows(db, "SELECT * FROM rule ORDER BY entry_id, rule_id"):
        x = _rule_record(r, names.get(r["entry_id"], ""), entries.get(r["entry_id"]))
        rules[x["id"]] = x
        entries[r["entry_id"]]["rules"].append(x["id"])

    licensing = {}
    for kind, idcol, placed in _LIC_TABLES:
        for r in _rows(db, f"SELECT * FROM {kind} ORDER BY entry_id, {idcol}"):
            x = _licensing_record(kind, idcol, placed, r, names.get(r["entry_id"], ""),
                                  entries.get(r["entry_id"]))
            licensing[x["id"]] = x
            entries[r["entry_id"]]["licensing"].append(x["id"])
    licensing = dict(sorted(licensing.items()))
    for e in entries.values():
        e["licensing"].sort()
    add_stamp_waivers(licensing, names)
    add_recorded(licensing, rules)

    licences = {d: {"name": n, "provincial": bool(p)}
                for d, n, p in db.execute("SELECT doc_id, name, provincial FROM licence "
                                          "ORDER BY doc_id")}

    # ---- where: the interned sets, and the named waters that carry them -------------------
    def sets(members_sql, count_sql, key):
        out: dict = {}
        for sid, n in db.execute(count_sql):
            out[str(sid)] = {"sections": n}
        for sid, eid, rid, via in db.execute(members_sql):
            out.setdefault(str(sid), {"sections": 0}).setdefault(via, []).append(key(eid, rid))
        return dict(sorted(out.items(), key=lambda kv: int(kv[0])))

    rulesets = sets("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                    "ORDER BY set_id, via, entry_id, rule_id",
                    "SELECT set_id, COUNT(*) FROM section_ruleset GROUP BY set_id",
                    lambda e, r: f"{e}::{r}")
    licensing_sets = sets("SELECT set_id, entry_id, record_id, via FROM licensing_set "
                          "ORDER BY set_id, via, entry_id, record_id",
                          "SELECT set_id, COUNT(*) FROM section_licensing GROUP BY set_id",
                          lambda e, r: f"{e}#{r}")

    by_item = defaultdict(list)
    for eid, e in entries.items():
        for it in e["matched"]:
            by_item[it].append(eid)
    waters = {}
    # THE WATER KIND is the bundle's `item.kind` — the registry's, decided once (a slough, a canal,
    # a river's wide reach drawn as a polygon is `stream`; user ruling 2026-10-03). Nothing here
    # recomputes it from a name. A lake cut into PARTS owns no section (`item.part_of` names it):
    # it is listed with none, `divided_into` its parts, and shown through them.
    for item_id, name, kind, part_of, n in db.execute(
            "SELECT i.item_id, i.name, i.kind, i.part_of, COUNT(s.sid) FROM item i "
            "LEFT JOIN item_section s ON s.ord = i.ord GROUP BY i.item_id ORDER BY i.item_id"):
        shown = display_case(name) if name else name
        waters[item_id] = {"name": shown, **({"official_name": name} if shown != name else {}),
                           "kind": kind, "sections": n,
                           "entries": sorted(by_item.get(item_id, [])),
                           "parts": [], "outside_bc": 0}
        if part_of:
            waters[item_id]["part_of"] = part_of
    for item_id, w in waters.items():
        if w.get("part_of"):
            waters[w["part_of"]].setdefault("divided_into", []).append(item_id)
    for w in waters.values():
        if w.get("divided_into"):
            if w["sections"]:
                raise SystemExit(f"pipeline.deliver.export: a lake cut into parts ({w['name']}) still "
                                 f"owns {w['sections']} section(s) — the bundle predates the "
                                 f"ruling (user ruling 2026-10-03)")
            w["divided_into"].sort()
            # its parts' rows are its rows: the whole is shown through them
            w["entries"] = sorted(set(w["entries"]).union(
                *(waters[c]["entries"] for c in w["divided_into"])))
    # ABSORBED IDS (`item_alias`): what an old id names now, for a saved link
    aliases = defaultdict(list)
    for alias, item_id in db.execute("SELECT alias, item_id FROM item_alias ORDER BY alias"):
        aliases[item_id].append(alias)
    for item_id, got in aliases.items():
        waters[item_id]["absorbed"] = got
    # ONE LIST PER WATER: which rule set and which licensing set its sections carry TOGETHER.
    # This was two per-water histograms — rule sets, licensing sets — and the pairing on each
    # section, which the bundle holds, was lost: the Dean's eight (ruleset, licensing set)
    # combinations read as five rule sets beside four licensing sets, and nothing said which Class
    # I unit went with which closure. A section with no set on one side is `null` there.
    #
    # A PART ALSO SAYS WHERE A PROVINCE-WIDE REQUIREMENT STOPS (`province_except`: the families of
    # areas its sections lie in — a national park) and WHERE A RAINBOW OVER 50 CM IS A STEELHEAD
    # (`anadromous_rainbow`). Both were per water, or absent: a count of park sections on the
    # water left the page to infer WHICH stretch was the park's from the permit.
    #
    # WHICH PARTS BORDER EACH OTHER (`touches`). A part is every section of the water carrying the
    # same sets, so it is a CLASS, not a stretch: two closed stretches with an open one between
    # them are one part if their sets agree, and two parts can lie apart. A reader merging "the
    # closed stretches" merged every closed part of a water, touching or not. `touches` lists the
    # indexes (into this water's `parts`) of the other parts some section of this one borders in
    # the stream graph (`section_touch`: end to end, or a branch of the water flowing into it).
    # Section handles stay in here (AGENTS 5); only the part-to-part relation leaves.
    # THE PARTS ARE THE BUNDLE'S (`part`, DATAFLOW P2): decided once by the bundle builder
    # (`bundle.derived`), in the order this file always listed them, so `part_ix` is the index
    # here. Nothing in the export groups sections into parts any more.
    from pipeline.deliver.bundle.derived import parts as _bundle_parts
    bundle_parts = _bundle_parts(db)
    for item_id, ps in bundle_parts.items():
        for p in ps:
            waters[item_id]["parts"].append({
                "ruleset": None if p.set_id is None else str(p.set_id),
                "licensing_set": None if p.licensing_set is None else str(p.licensing_set),
                "sections": p.sections,
                **({"province_except": list(p.province_except)} if p.province_except else {}),
                **({"anadromous_rainbow": True} if p.steelhead_water else {}),
                **({"steelhead": p.steelhead} if p.steelhead else {}),
                "touches": []})
    # HOW SURE WE ARE THAT STEELHEAD ARE HERE, per water (`section_steelhead`, user ruling
    # 2026-10-01): "known" if any part is known, else "possible" if any part is; absent otherwise.
    for w in waters.values():
        got = {p.get("steelhead") for p in w["parts"]}
        top = "known" if "known" in got else "possible" if "possible" in got else None
        if top:
            w["steelhead"] = top
    # WHY IT IS KNOWN, per water (`steelhead_source`): "regulations" where a steelhead row's rules
    # bind its known sections (those rows: `steelhead_rows`), "curated list" where the curated
    # known-steelhead list names it — a presence indicator that binds no rule.
    from pipeline.atlas.reach.steelhead import CURATED_LIST as _LISTED
    for item_id, eid in db.execute(
            "SELECT i.item_id, s.entry_id FROM steelhead_source s JOIN item i ON i.ord = s.ord "
            "ORDER BY i.item_id, s.entry_id"):
        w = waters[item_id]
        if eid == _LISTED:
            w["_listed"] = True
        else:
            w.setdefault("steelhead_rows", []).append(eid)
    for w in waters.values():
        listed = w.pop("_listed", False)
        src = (["regulations"] if w.get("steelhead_rows") else []) + (
            [_LISTED] if listed else [])
        if src:
            w["steelhead_source"] = src
    section_part: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for item_id, sid_, pi in db.execute(
            "SELECT i.item_id, ps.sid, ps.part_ix FROM part_section ps JOIN item i "
            "ON i.ord = ps.ord ORDER BY i.item_id, ps.sid"):
        section_part[sid_].append((item_id, pi))
    # WHERE STEELHEAD RULES APPLY, on EVERY part with a rule set (decision DF4: `true` or `false`,
    # never left for the reader to infer from absence), and on a known water none of whose parts
    # carries them. READ from the bundle's `part.steelhead_rules`, never decided here.
    add_steelhead_rules(waters, {(item_id, p.part_ix): p.steelhead_rules
                                 for item_id, ps in bundle_parts.items() for p in ps})
    # THE HOME REGION of a part with straddling sections (`section_home`): the region(s) those
    # pieces take their zone rules from — "this piece takes Region 3's rules".
    for item_id, ps in bundle_parts.items():
        for p in ps:
            if p.home_regions:
                waters[item_id]["parts"][p.part_ix]["home_region"] = list(p.home_regions)
    touching: set[tuple[str, int, int]] = set()
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        on_b = dict(section_part.get(b, ()))
        for item_id, pa in section_part.get(a, ()):
            pb = on_b.get(item_id)
            if pb is not None and pb != pa:
                touching |= {(item_id, pa, pb), (item_id, pb, pa)}
    for item_id, pa, pb in sorted(touching):
        waters[item_id]["parts"][pa]["touches"].append(pb)
    # WHERE EACH PART RUNS (`runs`): its stretches, upstream to downstream, each between two
    # cut points or natural ends, with km from the mouth — composed from `section_span` by the
    # bundle's own `spans.compose_runs`. A lake (or wetland) part is its polygon: one run with no
    # ends. The sections stay here; only the runs leave.
    splits = read_splits(db)
    # only the sections of a part are ever composed into runs (M10.3: not every section's span)
    span = {s: (a, b, lo, hi, off, shape) for s, a, b, lo, hi, off, shape in db.execute(
        "SELECT s.sid, s.lo_m, s.hi_m, a.token, b.token, s.off_stem, s.shape FROM section_span s "
        "JOIN part_section ps ON ps.sid = s.sid "
        "JOIN span_end a ON a.eid = s.lo JOIN span_end b ON b.eid = s.hi")}
    touch_of: dict[int, set[int]] = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        touch_of[a].add(b)
        touch_of[b].add(a)
    members: dict[tuple[str, int], list[int]] = defaultdict(list)
    for s, ps in section_part.items():
        for item_id, pi in ps:
            members[(item_id, pi)].append(s)
    for (item_id, pi), sids in sorted(members.items()):
        w = waters[item_id]
        w["parts"][pi]["runs"] = part_runs(item_id, w, sorted(sids), span, touch_of)
    # WATER B.C. DOES NOT GOVERN — the sections of each water that lie outside the province. They
    # carry no set (the build refuses one that does), so they are among the parts with
    # `ruleset: null`; this count says why they have none.
    for item_id, n in db.execute(
            "SELECT i.item_id, COUNT(*) FROM item i JOIN item_section s ON s.ord = i.ord "
            "JOIN outside_bc o ON o.sid = s.sid GROUP BY i.item_id"):
        waters[item_id]["outside_bc"] = n
    # WATER THE BOOK CALLS TIDAL (Nitinat Lake) — present only on a water with tidal sections. They
    # carry only the tidal row's own note (the build refuses any other rule or a licensing set),
    # and every province-wide requirement stops there (`province_except`: `tidal`).
    for item_id, eid, n in db.execute(
            "SELECT i.item_id, MIN(t.entry_id), COUNT(*) FROM item i JOIN item_section s "
            "ON s.ord = i.ord JOIN tidal t ON t.sid = s.sid GROUP BY i.item_id"):
        waters[item_id]["tidal"] = {"sections": n, "entry": eid, "guide": TIDAL_GUIDE}

    sections = section_counts(db)
    db.close()
    splits = referenced_edges(splits, entries, rules, licensing, waters)
    # ---- the record duty an annual quota carries (`recorded_by` / `records_for`) ------------
    for q, cands in record_candidates(rules, rulesets).items():
        if len(cands) == 1:          # two would be ambiguous: left unlinked, `problems` says so
            rules[q]["recorded_by"] = cands[0]
            rules[cands[0]].setdefault("records_for", []).append(q)
    return {"meta": meta, "entries": entries, "rules": rules, "licensing": licensing,
            "licences": licences, "rulesets": rulesets, "licensing_sets": licensing_sets,
            "waters": waters, "sections": sections, "splits": splits}


#: A region line is not a `split` row (a region boundary crosses hundreds of waters at hundreds of
#: places, all one line); its runs name it `region_line:<region>`.
REGIONS = ("1", "2", "3", "4", "5", "6", "7A", "7B", "8")


def read_splits(db) -> dict:
    """THE CUTS BY NAME — every split the atlas resolved, `id -> {name, kind, water_id, km}`, and
    the nine region lines. `water_id` / `km` (from the mouth of that water's main stem) are set
    when the cut stands at ONE place on a named water's main stem; a cut at several places lists
    them all in `at` and leaves both null; an area boundary (which crosses hundreds of waters) has
    neither — the run ending there carries the km."""
    out = {}
    for sid, name, kind, at, official, same, lake_id in db.execute(
            "SELECT split_id, name, kind, at, official_name, same_place_as, lake_id FROM split "
            "ORDER BY split_id"):
        at = _j(at, [])
        one = at[0] if len(at) == 1 else None
        out[sid] = {"name": name, "kind": kind,
                    "water_id": one[0] if one else None, "km": one[1] if one else None,
                    **({"at": [{"water_id": w, "km": k} for w, k in at]} if len(at) > 1 else {}),
                    **({"official_name": official} if official else {}),
                    **({"same_place_as": same} if same else {}),
                    **({"lake_id": lake_id} if lake_id else {})}
    for r in REGIONS:
        out[f"region_line:{r}"] = {"name": f"Region {r} boundary", "kind": "region_line",
                                   "water_id": None, "km": None}
    return dict(sorted(out.items()))


def _split_refs(o):
    """Every split id named under a `splits` key anywhere in `o` (extents, nested carve-outs)."""
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "splits" and isinstance(v, list):
                yield from (x for x in v if isinstance(x, str))
            else:
                yield from _split_refs(v)
    elif isinstance(o, list):
        for v in o:
            yield from _split_refs(v)


def referenced_edges(splits: dict, entries: dict, rules: dict, licensing: dict,
                     waters: dict) -> dict:
    """ONLY THE LAKE EDGES SOMETHING NAMES. The bundle's `split` table holds every lake's edge on
    every river (`kind: lake_edge`, 21,636 rows) because a curated cut landing in a lake binds
    by one (E2); 68 are named by an extent, and the runs end at `lake_inlet:` / `lake_outlet:`
    tokens, never at an edge id. A row nothing in the file names is a name no page can ask for:
    it is left in the bundle. Every other kind of cut ships whole. What counts as naming: an
    extent's `splits` (rules, licensing, entries — nested carve-outs included), a run's ends, a
    kept cut's `same_place_as`."""
    refs = set(_split_refs([e.get("extents") for e in entries.values()]))
    refs |= set(_split_refs([x["fields"] for x in rules.values()]))
    refs |= set(_split_refs([x["fields"] for x in licensing.values()]))
    refs |= {r[k] for w in waters.values() for p in w["parts"] for r in p.get("runs") or ()
             for k in ("from", "to") if r.get(k)}
    keep = {k for k, x in splits.items() if x["kind"] != "lake_edge" or k in refs}
    keep |= {splits[k]["same_place_as"] for k in list(keep) if splits[k].get("same_place_as")}
    return {k: x for k, x in splits.items() if k in keep}


def is_polygon_part(water: dict, sids: list[int], span: dict) -> bool:
    """Is this part its water's polygon — one run with no ends? A lake or wetland always; a STREAM
    part whose every section is a polygon the main stem does not pass through (`section_span.
    shape` with no window: a slough drawn only as polygons, a river's far polygon). A stream's
    polygon ON its stem has a window and runs with the line through it."""
    if water["kind"] != "stream":
        return True
    rows = [span.get(s) for s in sids]
    return bool(rows) and all(r is not None and r[5] == 1 and r[0] is None for r in rows)


def part_runs(item_id: str, water: dict, sids: list[int], span: dict, touch: dict) -> list[dict]:
    """One part's `runs`, upstream to downstream. A stream section with no span row is refused:
    a part whose stretches cannot be said would ship a run that covers less than the part. Runs
    follow the SHAPE of the sections (`section_span.shape`), never the water's name: a polygon
    part is one polygon run (`is_polygon_part`)."""
    if is_polygon_part(water, sids, span):
        return [{"from": None, "to": None, "km_from": None, "km_to": None,
                 "polygon": water["name"] if water.get("part_of") else "whole"}]
    missing = [s for s in sids if s not in span]
    if missing:
        raise SystemExit(f"pipeline.deliver.export: {len(missing)} section(s) of stream {item_id} have no "
                         f"`section_span` row — the bundle cannot say where its parts run. "
                         f"Rebuild it with `pipeline/deliver/bundle/spans.py`.")
    return [{k: v for k, v in r.items() if k != "sids"}
            for r in SP.compose_runs(span, touch, sids)]
