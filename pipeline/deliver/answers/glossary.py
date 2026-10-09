"""THE GLOSSARY (answers 2.2, user decision J 2026-10-08): "what does this mean" for every piece of
jargon the consumer page shows — classified waters, the regions, the daily and possession quotas,
catch and release, single barbless hook, bait ban, tributaries, hatchery and wild, steelhead, the
stamps, set lining, guided anglers, under-16 anglers.

GENERATED, NEVER WRITTEN PER WATER. Each term is one generator below: its words are a template
filled from the data — the export's rules (the possession multiplier, the quotas used in the
examples, the annual steelhead quota), its licensing records (fees, the classes, the stamps, the
under-16 paths) and entries (the region-wide tables and their pages) — and from the book's own
text (`CURATED.regulations.reference`: the p.80 definitions, the p.4 legend), quoted VERBATIM in
`quote`. Every term names its book pages (PRINTED page numbers: the export's entry `pages` are
printed; the definitions are p.80, the legend p.4, the adipose diagram p.13). A generator whose
data is missing raises: no term is ever written from a guess.

Model: `model.GlossaryTerm` / `schemas["top.glossary"]`. Version `VERSION`.
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, List, Optional

from pipeline.deliver.answers.common import AnswersError

VERSION = 1


# --------------------------------------------------------------------------------------------
# The book's text (verbatim, curated)
# --------------------------------------------------------------------------------------------

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("**", "")).strip()


def _paragraph_terms(text: str) -> Dict[str, str]:
    """`**term**: definition` (p.80) and `**Term:** definition` (p.4) paragraphs -> {term: text}."""
    out: Dict[str, str] = {}
    for para in re.split(r"\n\s*\n", text):
        m = re.match(r"\s*\*\*(.+?)(?::\*\*|\*\*:)\s*(.+)", para, re.S)
        if m:
            out[_clean(m.group(1)).lower()] = _clean(m.group(2))
    return out


class Book:
    """The curated transcription of the synopsis (source text, never edited to fit)."""

    def __init__(self, ref_dir: Optional[Path] = None):
        if ref_dir is None:
            from pipeline.common.curated import CURATED
            ref_dir = CURATED.regulations.reference
        self.dir = Path(ref_dir)
        self.defs = _paragraph_terms(self._read("definitions.md"))          # p.80
        self.legend = _paragraph_terms(self._read("water-specific-tables.md"))  # p.4
        self.regions: Dict[str, str] = {}
        for f in sorted(self.dir.glob("region-*.md")):
            first = self._read(f.name).splitlines()[0]
            m = re.match(r"#\s*Region\s+(\w+)\s+—\s+(.+)", first)
            if not m:
                raise AnswersError(f"glossary: {f.name} has no 'Region N — Name' title")
            self.regions[m.group(1).lower()] = _clean(m.group(2))

    def _read(self, name: str) -> str:
        p = self.dir / name
        if not p.is_file():
            raise AnswersError(f"glossary: the book's text {p} is missing")
        return p.read_text(encoding="utf-8")

    def define(self, term: str) -> str:
        t = self.defs.get(term.lower())
        if not t:
            raise AnswersError(f"glossary: the p.80 definitions have no {term!r}")
        return t

    def legend_of(self, term: str) -> str:
        t = self.legend.get(term.lower())
        if not t:
            raise AnswersError(f"glossary: the p.4 legend has no {term!r}")
        return t


# --------------------------------------------------------------------------------------------
# The data (the decoded export)
# --------------------------------------------------------------------------------------------

class Data:
    def __init__(self, doc: dict):
        self.doc = doc
        self.rules = doc["rules"]
        self.entries = doc["entries"]
        self.lic = doc["licensing"]
        self.licences = doc["licences"]

    def entry(self, eid: str) -> dict:
        e = self.entries.get(eid)
        if e is None:
            raise AnswersError(f"glossary: the export has no entry {eid}")
        return e

    def pages(self, eid: str) -> List[int]:
        return list(self.entry(eid)["pages"])

    def fee(self, key: str) -> dict:
        r = self.lic.get(key)
        if r is None or not (r["fields"].get("fees_cad")):
            raise AnswersError(f"glossary: the export has no fee record {key}")
        return r["fields"]["fees_cad"]

    def zone_rules(self, pred: Callable[[dict], bool]) -> List[dict]:
        return [r for rid, r in sorted(self.rules.items())
                if r["entry_id"].startswith("z") and not r["entry_id"].startswith("zp:")
                and pred(r)]

    def zone_chapters(self) -> Dict[str, List[int]]:
        """{region code ("1", "7a", …): the printed pages of its region-wide tables}."""
        out: Dict[str, List[int]] = {}
        for eid, e in self.entries.items():
            if e.get("kind") == "zone" and re.match(r"z\d", eid):
                ch = e["chapter"][1:]
                out.setdefault(ch, []).extend(e["pages"])
        return {k: sorted(set(v)) for k, v in sorted(out.items())}


def _money(x) -> str:
    return f"${x:,.2f}"


def _one(values, what: str):
    c = Counter(values)
    if not c:
        raise AnswersError(f"glossary: no {what} in the data")
    return c.most_common(1)[0][0]


# --------------------------------------------------------------------------------------------
# The terms
# --------------------------------------------------------------------------------------------

def _term(id_, term, says, pages, quote, source, example=None) -> dict:
    t = {"id": id_, "term": term, "says": says, "pages": sorted(set(pages)), "quote": quote,
         "source": source}
    if example:
        t["example"] = example
    return t


def _trout_totals(D: Data) -> Dict[str, int]:
    """{region: its trout/char daily total} from the region-wide tables (`…:trout_char_quota.r1`)."""
    out = {}
    for r in D.zone_rules(lambda r: r["rule_id"].endswith("quota.r1")
                          and "TROUT_CHAR" in (r["fields"].get("species") or [])
                          and isinstance(r["fields"].get("take"), int)):
        out[r["entry_id"].split(":")[0][1:]] = r["fields"]["take"]
    if not out:
        raise AnswersError("glossary: no region-wide trout/char total in the data")
    return out


def t_daily(B: Book, D: Data) -> dict:
    totals = _trout_totals(D)
    n = _one(totals.values(), "trout/char total")
    regs = [k.upper() for k, v in totals.items() if v == n]
    return _term(
        "daily_quota", "Daily quota (a day's limit)",
        "The most fish you may keep in one day, midnight to midnight — of one kind, a group of kinds "
        "or one size class. It counts every fish you keep that day, wherever you keep it: a "
        "region's quota is shared by all its waters.",
        [80], f"daily quota: {B.define('daily quota')} day: {B.define('day')}",
        "definitions p.80; region-wide trout/char totals",
        example=f"Region {regs[0]}'s trout/char quota is {n}: keep 2 at one lake in the morning and "
                f"you may keep {n - 2} more anywhere that day.")


def t_possession(B: Book, D: Data) -> dict:
    ks = [r["fields"]["per_daily"] for r in D.zone_rules(lambda r: r["fields"].get("per_daily"))]
    k = _one(ks, "possession multiplier")
    totals = _trout_totals(D)
    n = _one(totals.values(), "trout/char total")
    words = {2: "two", 3: "three"}.get(k, str(k))
    return _term(
        "possession_quota", "Possession quota",
        f"How many fish you may have at any one time — in your camp, cooler, vehicle or on the trip "
        f"home: {words} days' quota, unless a table says otherwise. Fish at the place you normally "
        f"live (your ordinary residence) don't count toward it.",
        sorted({80} | {p for e in ("z1:possession_quota",) for p in D.pages(e)}),
        f"possession quota: {B.define('possession quota')}",
        "definitions p.80; every region-wide 'Possession quotas = N daily quotas' rule",
        example=f"With a daily quota of {n} trout you may hold {n * k}: two days' fish at camp is "
                f"fine, a third day's is not.")


def t_release(B: Book, D: Data) -> dict:
    return _term("catch_and_release", "Catch and release (release)",
                 "You may fish for it, but every one you catch must go back into the water, "
                 "quickly and carefully. You can't keep any.",
                 [4], f"Catch and Release: {B.legend_of('catch and release')}", "legend p.4")


def t_no_fishing_for(B: Book, D: Data) -> dict:
    return _term("no_fishing_for", "No fishing for (closed)",
                 "You may not fish for it at all, not even to release it. If you catch one by "
                 "accident, release it at once.",
                 [4], f"No fishing for: {B.legend_of('no fishing for')}", "legend p.4")


def t_hook(B: Book, D: Data) -> dict:
    e = D.entry("zp:barbless_single_hook_streams")
    return _term("single_barbless_hook", "Single barbless hook",
                 "A hook with one point (no treble) and no barb — you may pinch the barb flat. "
                 "Every river, stream, creek and slough in B.C. requires it; on a lake only where "
                 "its row says so.",
                 [80] + list(e["pages"]),
                 f"barbless hook: {B.define('barbless hook')} single hook: "
                 f"{B.define('single hook')} {_clean(e['printed'])}",
                 "definitions p.80; zp:barbless_single_hook_streams")


def t_bait(B: Book, D: Data) -> dict:
    return _term("bait_ban", "Bait ban",
                 "No natural bait of any kind — worms, roe, insects, fish parts — for any fish, "
                 "while the ban runs. Lures and flies are fine.",
                 [4] + D.pages("zp:bait"), f"Bait Ban: {B.legend_of('bait ban')}",
                 "legend p.4; zp:bait")


def t_tributaries(B: Book, D: Data) -> dict:
    n = sum(1 for e in D.entries.values() if "Incl. Tribs" in (e.get("symbols") or []))
    return _term("tributaries", "Tributaries (Incl. Tribs, ✱)",
                 "The streams that flow into a water. A rule marked for tributaries also applies "
                 "on every stream flowing into the named water, and the streams flowing into "
                 "those — streams only, not the lakes on them. On this page, rules reached that "
                 "way say 'From downstream'.",
                 [4, 80], f"tributaries: {B.define('tributaries')} Tributaries: "
                          f"{B.legend_of('tributaries')}",
                 f"definitions p.80; legend p.4; {n} rows carry the tributary symbol")


def t_hatchery(B: Book, D: Data) -> dict:
    return _term("hatchery_wild", "Hatchery or wild (adipose fin)",
                 "Hatchery trout and steelhead are marked before release by removing the adipose "
                 "fin — the small fleshy fin on the back between the dorsal fin and the tail — so "
                 "a hatchery fish has a healed scar there. A fish with its adipose fin (or a fresh, "
                 "unhealed wound) is wild. Where only hatchery fish may be kept, every wild one goes "
                 "back.",
                 [13, 80], f"hatchery trout: {B.define('hatchery trout')} wild trout: "
                           f"{B.define('wild trout')}", "definitions p.80; adipose diagram p.13")


def t_steelhead(B: Book, D: Data) -> dict:
    annual = [r["fields"]["take"] for r in D.rules.values()
              if r["entry_id"] == "zp:steelhead" and r["fields"].get("period") == "annual"
              and r["fields"].get("take")]
    n = _one(annual, "annual hatchery steelhead quota")
    return _term("steelhead", "Steelhead",
                 f"A rainbow trout that went to sea and came back: on waters where they run, any "
                 f"rainbow over 50 cm is a steelhead and follows the steelhead rules. Every wild "
                 f"steelhead must be released; you may keep at most {n} hatchery steelhead a "
                 f"licence year in all of B.C.",
                 [80] + D.pages("zp:steelhead"),
                 f"steelhead: {B.define('steelhead')} {_clean(D.entry('zp:steelhead')['printed'])}",
                 "definitions p.80; zp:steelhead")


def t_stamp(B: Book, D: Data) -> dict:
    f = D.fee("zp:licence_fees#steelhead_stamp")
    return _term("steelhead_stamp", "Steelhead Stamp",
                 f"A Steelhead Conservation Surcharge Stamp, added to your basic licence. You need "
                 f"it to fish for steelhead anywhere in B.C., even if you release them — and on "
                 f"many Classified Waters, on set dates, whatever you fish for. "
                 f"{_money(f['resident'])} a year for B.C. residents, "
                 f"{_money(f['non_resident'])} for everyone else.",
                 D.pages("zp:steelhead") + D.pages("zp:classified_waters_licence") + [5],
                 _clean(D.entry("zp:steelhead")["printed"]),
                 "zp:steelhead; zp:classified_waters_licence; fee zp:licence_fees#steelhead_stamp")


def t_surcharge(B: Book, D: Data) -> dict:
    stamps = sorted(v["name"] for k, v in D.licences.items()
                    if "conservation surcharge" in v["name"].lower() and v.get("provincial"))
    if not stamps:
        raise AnswersError("glossary: no Conservation Surcharge Stamp in the export's licences")
    what = [re.sub(r"(?i)^conservation surcharge stamp for | conservation surcharge stamp$", "", s)
            for s in stamps]
    return _term("conservation_surcharge", "Conservation Surcharge Stamp",
                 f"An add-on to your basic licence that pays for conservation; you need one only for "
                 f"what it names: {', '.join(what)}. Keeping some of these fish also means "
                 f"recording them on your licence right away.",
                 [5, 7], "Your basic angling licence can be validated with up to five annual "
                         "Conservation Surcharge Stamps, plus a White Sturgeon Conservation Licence.",
                 "the export's licences (provincial stamps); fees p.5")


def t_classified(B: Book, D: Data) -> dict:
    classes = Counter(r["fields"].get("classified") for r in D.lic.values()
                      if r["kind"] == "designation" and r["fields"].get("classified"))
    res = D.fee("zp:licence_fees#classified_annual")["resident"]
    c1 = D.fee("zp:licence_fees#classified_class_i_day")["non_resident"]
    c2 = D.fee("zp:licence_fees#classified_class_ii_day")["non_resident"]
    return _term("classified_waters", "Classified Waters (Class I, Class II)",
                 f"Highly productive trout streams listed in the tables as Class I or Class II "
                 f"({classes.get('I', 0)} and {classes.get('II', 0)} designations). While a water "
                 f"is classified you need a Classified Waters Licence on top of your basic "
                 f"licence: B.C. residents buy one a year ({_money(res)}) for every classified "
                 f"water; anyone else buys it per day and per water ({_money(c1)} Class I, "
                 f"{_money(c2)} Class II).",
                 D.pages("zp:classified_waters_licence") + [5],
                 _clean(D.entry("zp:classified_waters_licence")["printed"])[:600],
                 "zp:classified_waters_licence; every designation record; fees p.5")


def t_set_line(B: Book, D: Data) -> dict:
    return _term("set_line", "Set line (set lining)",
                 "A line left in the water unattended. Allowed only in some lakes, with one line "
                 "and one hook, and any game fish but burbot must be released.",
                 [80] + D.pages("zp:set_lining"),
                 f"set line: {B.define('set line')} {_clean(D.entry('zp:set_lining')['printed'])}",
                 "definitions p.80; zp:set_lining")


def t_guided(B: Book, D: Data) -> dict:
    keys = [k for k in D.lic if k.startswith("zp:classified_waters_licence#cwl_non_resident")]
    if not keys:
        raise AnswersError("glossary: no non-resident Classified Waters records")
    return _term("guided", "Guided / non-guided angler",
                 "Guided means you fish with a licensed angling guide. Some rules differ for "
                 "non-residents depending on it: Classified Waters Licences are sold per day "
                 "(guided anglers give the guide's number), and some rivers close on set days to "
                 "non-guided non-resident aliens.",
                 D.pages("zp:classified_waters_licence"),
                 "GUIDED Non-Resident or Non-Resident Alien: per day, date- and water-specific; a "
                 "valid angling guide number must be entered; max 8 consecutive days",
                 "; ".join(keys))


def t_under_16(B: Book, D: Data) -> dict:
    r = D.lic.get("zp:basic_licence#under_16_non_resident")
    if r is None:
        raise AnswersError("glossary: no under-16 non-resident record")
    paths = r["fields"].get("satisfied_by") or []
    if not any(p.get("accompanied_by") for p in paths):
        raise AnswersError("glossary: the under-16 record has no accompanied path")
    return _term("under_16", "Anglers under 16",
                 "B.C. residents under 16 need no licence or stamp and have their own quota. "
                 "Under-16s from elsewhere need no licence either, but must fish with someone 16 or "
                 "older who holds the licences and stamps the fishing needs — and what they keep "
                 "counts toward that person's quota. To have their own quota, they buy the licences "
                 "a 16+ angler would.",
                 D.pages("zp:basic_licence"),
                 "If you are under 16 and NOT a resident of B.C.: You do not require any licence or "
                 "stamp, but you must be accompanied by a person 16 years or older who holds the "
                 "appropriate licences and stamps. Any fish you keep must be counted as part of the "
                 "catch and possession of your accompanying licence holder.",
                 "zp:basic_licence#under_16_non_resident")


def t_stream(B: Book, D: Data) -> dict:
    return _term("stream", "Stream (from streams)",
                 "Any flowing water — river, creek, slough, canal or side channel. 'From streams' "
                 "limits count only the fish you keep from flowing water; a lake's fish count "
                 "toward the region's total but not that share.",
                 [80], f"streams: {B.define('streams')} slough: {B.define('slough')}",
                 "definitions p.80")


def t_mu(B: Book, D: Data) -> dict:
    return _term("management_unit", "Management Unit (M.U.)",
                 "A numbered piece of a region (2-4, 6-25 …). The tables list it only to help you "
                 "find a water and tell apart waters with the same name.",
                 [4, 80], f"Management Unit: {B.define('management unit')}", "definitions p.80")


def t_year(B: Book, D: Data) -> dict:
    return _term("licence_year", "Licence year (a year)",
                 "April 1 to March 31. Annual licences, stamps and yearly quotas run on it, not on "
                 "the calendar year.",
                 [80], f"licence year: {B.define('licence year')}", "definitions p.80")


def t_regions(B: Book, D: Data) -> List[dict]:
    out = []
    chapters = D.zone_chapters()
    for code, pages in chapters.items():
        name = B.regions.get(code)
        if not name:
            raise AnswersError(f"glossary: the book has no title for Region {code.upper()}")
        out.append(_term(
            f"region_{code}", f"Region {code.upper()} – {name}",
            f"One of B.C.'s fishing regions. Region {code.upper()}'s region-wide quotas and "
            f"closures (p.{pages[0]}) apply on every water in it; a water's own row in the "
            f"region's tables adds to them or changes them for that water.",
            pages[:2], f"Region {code.upper()} — {name}", f"zone tables z{code}:*"))
    return out


GENERATORS = (t_daily, t_possession, t_release, t_no_fishing_for, t_hook, t_bait, t_tributaries,
              t_hatchery, t_steelhead, t_stamp, t_surcharge, t_classified, t_set_line, t_guided,
              t_under_16, t_stream, t_mu, t_year)


def build(doc: dict, ref_dir: Optional[Path] = None) -> dict:
    """The glossary: {version, terms} — every term generated from the export (`doc`, decoded) and
    the book's text."""
    B, D = Book(ref_dir), Data(doc)
    terms = [g(B, D) for g in GENERATORS] + t_regions(B, D)
    ids = [t["id"] for t in terms]
    if len(set(ids)) != len(ids):
        raise AnswersError("glossary: two terms share an id")
    return {"version": VERSION, "terms": terms}
