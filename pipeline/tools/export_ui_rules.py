"""The UI data export: every regulation record in the bundle, and a guide to reading it.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.export_ui_rules [--bundle B] [--out OUT]

WHAT IS IN IT. Everything the bundle holds about regulations, and nothing sampled:

  · every entry (the province, the zone chapters, the areas inside them, and the waters);
  · every rule, keyed `entry_id::rule_id`, with its generated `label`, its `verbatim`, its fields
    exactly as the bundle ships them, and its provenance;
  · every licensing record, keyed `entry_id#record_id`, the same way, with its placement;
  · the document register;
  · where each of them applies, in the bundle's own interned form: rule sets and licensing sets
    (each shared by many sections), and for every named water the sets its sections carry;
  · a `guide` explaining how to read all of it, generated from the model's own registries
    wherever the model has one, so the explanation cannot drift from the code.

WHAT IS NOT. Nothing is settled here: no tables, no verdicts, no "reads as". A record is data;
the guide says how to read it, and the reader applies that. Section handles never leave the
bundle (AGENTS 5), so membership is exported per set and per named water, never per section.

ONE SOURCE. Everything is read from `bundle.sqlite`. A bundle that lacks a column this export
reads, or that ships a retired field, is refused rather than worked around.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import typing
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle.rules import LIFT_KEYS
from pipeline.regs.parsing import catalogue as C
from pipeline.deliver.bundle.read import Authority, Scope, Source, source_of

BUNDLE = GENERATED.bundle / "bundle.sqlite"
OUT = GENERATED.base / "regs" / "ui-rules-export.json"

# --------------------------------------------------------------------------------------------
# What the bundle must carry, and what it must never carry
# --------------------------------------------------------------------------------------------

#: Columns this export reads beyond the long-standing ones. A bundle without them is refused:
#: `entry.matched` is every water a synopsis row covers (`item_id` is only the first), and
#: `rule.unresolved` is why a rule could not be placed, and `rule.exempts` is what a rule lifts,
#: resolved to the entry each lift reaches — it left `conditions` for a column of its own, so a
#: bundle without the column is one whose lifts this export would silently drop.
REQUIRED_COLUMNS = {"entry": ("matched", "see"),
                    "rule": ("unresolved", "exempts", "undrawn_part", "parts"),
                    **{t: ("parts",) for t in ("designation", "not_classified", "requirement",
                                              "licence_terms", "exemption", "alternative")},
                    "item": ("part_of",), "outside_bc": ("sid",), "tidal": ("sid", "entry_id"),
                    "section_touch": ("a", "b"),
                    "province_except": ("area_kind", "sid"),
                    "steelhead_water": ("sid",),
                    "section_steelhead": ("sid", "code"),
                    "steelhead_known": ("sid",), "steelhead_set": ("set_id", "code"),
                    "steelhead_source": ("ord", "entry_id"),
                    "section_span": ("sid", "lo_m", "hi_m", "lo", "hi", "off_stem"),
                    "span_end": ("eid", "token"),
                    "split": ("split_id", "name", "kind", "at")}

#: FIELD NAMES THE MODEL NO LONGER HAS. None may appear as a key anywhere in the output.
RETIRED_ANYWHERE = frozenset({
    "windows", "windows_are", "from_time", "to_time", "when_open",
    "over_cm", "under_cm", "band", "combined", "aggregation_domain",
    "required", "permitted", "allowed", "on_retention", "water_class", "licence_name",
    "issuing_jurisdiction", "grantor", "angler_class",
    "barbless", "hook_count", "max_lines", "max_flies", "max_weight_kg", "min_gap_cm",
    "max_gap_mm", "electric_only",
    "details", "rule_text", "restriction_type", "exempts_from", "subject",
    "needs_review", "locked", "reviewed_by", "reviewed_at", "registry_status", "registry_note",
    "parse_review", "revisit", "reference_only", "display_location", "location_text",
    "document_required", "access_permission",
})
#: Names that are current somewhere else in the model (`method` is a gear slot and a clause
#: condition, `document` a licence-terms field, `kind` the licensing discriminator, `dates` a
#: field of `when`) and retired only as a key on a RULE.
RETIRED_ON_RULE = RETIRED_ANYWHERE | frozenset({
    "method", "document", "allocation", "reason", "kind", "dates", "weekdays", "lure", "bait",
    "scope_text", "tributaries",
})
#: Rule types that were retired into `CatalogueEntry.licensing`.
RETIRED_TYPES = frozenset({"document_required", "access_permission",
                           "angling_from_vessel_prohibited"})


def _need(db: sqlite3.Connection) -> None:
    missing = [f"{t}.{c}" for t, cols in REQUIRED_COLUMNS.items()
               for c in cols if c not in {r[1] for r in db.execute(f"PRAGMA table_info({t})")}]
    if missing:
        raise SystemExit(
            f"export_ui_rules: the bundle has no {', '.join(missing)}. Rebuild it with a "
            f"`pipeline/deliver/bundle/rules.py` that writes `entry.matched` (every matched "
            f"item, JSON), `rule.unresolved` (the reach run's 'reason: detail', NULL when "
            f"bound), `rule.exempts` (each lift resolved to its entry, JSON), `item.part_of`, "
            f"the `outside_bc` table (the sections B.C. does not govern), `section_touch` "
            f"(which sections of a water border each other), and `section_span` / `span_end` / "
            f"`split` (where each section lies along its water, what ends it, the cuts by name).")


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
    head = entry_id.split(":", 1)[0]
    if head == "zp":
        return "province"
    if head.startswith("r"):
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


def _rule_record(r: dict, entry_name: str) -> dict:
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
        **({"not_yet_mapped": not_yet_mapped(r)} if _binds(r) == "sections_in_part" else {}),
        "fields": dict(sorted(fields.items())),
        "provenance": {
            "entry_name": entry_name,
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
#: them invented. The link is DERIVED here, never authored: a `record_retention` rule is an annual
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


#: WHERE A RULE ACTUALLY HOLDS, said once, on the record — so no reader has to cross-read
#: `fields.extents` against `provenance.uncertain` to learn that a rule it sees as `whole` binds
#: nothing (92 rules shipped that way: their row's water is not in the atlas).
BINDS_TEXT = {
    "sections": "placed: it holds on every section that carries it (find them in `rulesets`)",
    "sections_in_part": "placed on the sections of the water it is in, but it holds only in the "
                        "part `fields.undrawn_part` names, which nothing draws. SHOW it at the top "
                        "of the water, prominently, as a place not yet mapped (the record's "
                        "`not_yet_mapped`); NEVER colour or decide the water by it — it never "
                        "competes",
    "nowhere": "not placed: the reach builder could not bind it (`provenance.why`). Its "
               "`fields.extents` are what the rule states, not where it holds — a `whole` on an "
               "entry with no `matched` water names a water the atlas does not have. It can only "
               "ever raise 'unknown', never 'no rules here'",
}


def _binds(r: dict) -> str:
    if r["uncertain"]:
        return "nowhere"
    return "sections_in_part" if r["undrawn_part"] else "sections"


def not_yet_mapped(r: dict) -> dict:
    """THE FLAG A RULE IN AN UNDRAWN PART CARRIES (user ruling 2026-09-26): it holds only in a part
    of its water that nothing draws, so it is placed on the whole water to be SEEN there — at the
    top, marked as a place not yet mapped — and it never decides the water (`read.not_yet_mapped`:
    it never competes, displaces, lifts or suspends)."""
    part = str(r["undrawn_part"]).strip()
    identified = C.part_identifies_place(part)
    return {"display": "prominent", "part": part, "identified": identified,
            "says": _not_yet_mapped_says(part) if identified else _unidentified_says(part)}


def _not_yet_mapped_says(part: str) -> str:
    # "applies only {part}" read "applies only waters lying west of …" / "only the area at …" for
    # about half the 131 parts; the part is quoted as a phrase instead.
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
NOT_PLACED = "not_placed"


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


def _licensing_record(kind: str, idcol: str, placed: bool, r: dict, entry_name: str) -> dict:
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
        "fields": dict(sorted(fields.items())),
        "placement": r["placement"] if placed else NOT_PLACED,
        "provenance": {
            "entry_name": entry_name,
            "uncertain": bool(r.get("uncertain") or 0),
            "why": r.get("unresolved"),
        },
    }


def read(bundle: Path) -> dict:
    """Everything the export ships, straight from the bundle."""
    if not Path(bundle).exists():
        raise SystemExit(f"export_ui_rules: no bundle at {bundle} — build one with "
                         f"`python -m pipeline.deliver.bundle`")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    _need(db)
    meta = dict(db.execute("SELECT k, v FROM meta WHERE k != 'schema'"))

    entries, names = {}, {}
    for e in _rows(db, "SELECT * FROM entry ORDER BY entry_id"):
        extents = _j(e["extents"], [])
        names[e["entry_id"]] = e["name"] or ""
        entries[e["entry_id"]] = {
            "kind": _entry_kind(e["entry_id"], extents),
            "chapter": e["entry_id"].split(":", 1)[0],
            "name": e["name"], "full_name": e["full_name"],
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
        x = _rule_record(r, names.get(r["entry_id"], ""))
        rules[x["id"]] = x
        entries[r["entry_id"]]["rules"].append(x["id"])

    licensing = {}
    for kind, idcol, placed in _LIC_TABLES:
        for r in _rows(db, f"SELECT * FROM {kind} ORDER BY entry_id, {idcol}"):
            x = _licensing_record(kind, idcol, placed, r, names.get(r["entry_id"], ""))
            licensing[x["id"]] = x
            entries[r["entry_id"]]["licensing"].append(x["id"])
    licensing = dict(sorted(licensing.items()))
    for e in entries.values():
        e["licensing"].sort()

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
    for item_id, name, kind, part_of, n in db.execute(
            "SELECT i.item_id, i.name, i.kind, i.part_of, COUNT(*) FROM item i "
            "JOIN item_section s ON s.ord = i.ord GROUP BY i.item_id ORDER BY i.item_id"):
        waters[item_id] = {"name": name, "kind": kind, "sections": n,
                           "entries": sorted(by_item.get(item_id, [])),
                           "parts": [], "outside_bc": 0}
        if part_of:
            waters[item_id]["part_of"] = part_of
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
    _PART_SQL = (
        "FROM item i JOIN item_section s ON s.ord = i.ord "
        "LEFT JOIN section_ruleset r ON r.sid = s.sid "
        "LEFT JOIN section_licensing l ON l.sid = s.sid ")
    _PART_COLS = (
        "i.item_id, r.set_id, l.set_id, "
        "(SELECT group_concat(k, ',') FROM (SELECT p.area_kind AS k FROM province_except p "
        " WHERE p.sid = s.sid ORDER BY p.area_kind)) AS pe, "
        "EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = s.sid) AS sw, "
        # `section_steelhead` spelled out (the view, correlated per section, is re-run per row):
        # a known stream by its sid, everything else by its rule set - the view's own definition
        "CASE WHEN EXISTS (SELECT 1 FROM steelhead_known k WHERE k.sid = s.sid) THEN 'known' "
        "ELSE (SELECT CASE h.code WHEN 1 THEN 'known' WHEN 2 THEN 'possible' END "
        " FROM steelhead_set h WHERE h.set_id = r.set_id) END AS st")
    part_ix: dict[tuple, int] = {}
    for item_id, rs, ls, pe, sw, st, n in db.execute(
            f"SELECT {_PART_COLS}, COUNT(*) {_PART_SQL}"
            "GROUP BY i.item_id, r.set_id, l.set_id, pe, sw, st "
            "ORDER BY i.item_id, r.set_id IS NULL, r.set_id, l.set_id IS NULL, l.set_id, "
            "pe IS NOT NULL, pe, sw, st IS NULL, st"):
        part_ix[(item_id, rs, ls, pe, sw, st)] = len(waters[item_id]["parts"])
        waters[item_id]["parts"].append({
            "ruleset": None if rs is None else str(rs),
            "licensing_set": None if ls is None else str(ls), "sections": n,
            **({"province_except": pe.split(",")} if pe else {}),
            **({"anadromous_rainbow": True} if sw else {}),
            **({"steelhead": st} if st else {}),
            "touches": []})
    # HOW SURE WE ARE THAT STEELHEAD ARE HERE, per water (`section_steelhead`, user ruling
    # 2026-10-01): "known" if any part is known, else "possible" if any part is; absent otherwise.
    for w in waters.values():
        got = {p.get("steelhead") for p in w["parts"]}
        top = "known" if "known" in got else "possible" if "possible" in got else None
        if top:
            w["steelhead"] = top
    # WHICH ROWS MAKE IT KNOWN, per water (`steelhead_source`): the rows naming steelhead whose own
    # stretch or tributary walk reaches the water's known streams.
    for item_id, eid in db.execute(
            "SELECT i.item_id, s.entry_id FROM steelhead_source s JOIN item i ON i.ord = s.ord "
            "ORDER BY i.item_id, s.entry_id"):
        waters[item_id].setdefault("steelhead_source", []).append(eid)
    section_part: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for item_id, rs, ls, pe, sw, st, s in db.execute(f"SELECT {_PART_COLS}, s.sid {_PART_SQL}"):
        section_part[s].append((item_id, part_ix[(item_id, rs, ls, pe, sw, st)]))
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
    span = {s: (a, b, lo, hi, off) for s, a, b, lo, hi, off in db.execute(
        "SELECT s.sid, s.lo_m, s.hi_m, a.token, b.token, s.off_stem FROM section_span s "
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

    sections = {
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
    db.close()
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
    for sid, name, kind, at in db.execute("SELECT split_id, name, kind, at FROM split "
                                          "ORDER BY split_id"):
        at = _j(at, [])
        one = at[0] if len(at) == 1 else None
        out[sid] = {"name": name, "kind": kind,
                    "water_id": one[0] if one else None, "km": one[1] if one else None,
                    **({"at": [{"water_id": w, "km": k} for w, k in at]} if len(at) > 1 else {})}
    for r in REGIONS:
        out[f"region_line:{r}"] = {"name": f"Region {r} boundary", "kind": "region_line",
                                   "water_id": None, "km": None}
    return dict(sorted(out.items()))


def part_runs(item_id: str, water: dict, sids: list[int], span: dict, touch: dict) -> list[dict]:
    """One part's `runs`, upstream to downstream. A stream section with no span row is refused:
    a part whose stretches cannot be said would ship a run that covers less than the part."""
    if water["kind"] != "stream":
        return [{"from": None, "to": None, "km_from": None, "km_to": None,
                 "polygon": water["name"] if water.get("part_of") else "whole"}]
    missing = [s for s in sids if s not in span]
    if missing:
        raise SystemExit(f"export_ui_rules: {len(missing)} section(s) of stream {item_id} have no "
                         f"`section_span` row — the bundle cannot say where its parts run. "
                         f"Rebuild it with `pipeline/deliver/bundle/spans.py`.")
    return [{k: v for k, v in r.items() if k != "sids"}
            for r in SP.compose_runs(span, touch, sids)]


# --------------------------------------------------------------------------------------------
# The guide's words. Every table below is checked against the model's own registry (see
# `problems`), so a type, slot, act, kind or field the model gains is refused until it is
# explained here, and one the model loses cannot linger.
# --------------------------------------------------------------------------------------------

#: WHAT `touches` MEANS — one text, used by the guide and the field dictionary.
TOUCHES_TEXT = (
    "The indexes (into this water's `parts`) of the OTHER parts of this water that this part "
    "borders. Two parts touch when a section of one and a section of the other are joined in "
    "the stream graph: river pieces joined end to end (the two sides of a split point, or of any "
    "cut between differently regulated stretches), or a branch of this same water — a side "
    "channel, a braid, a fork bearing the same name — flowing into it at a confluence. Nothing "
    "else touches: two stretches with a differently regulated stretch between them do not, and a "
    "river above a lake does not touch the river below it, because the lake between them is "
    "another water. Another water never appears here: a tributary joining the river is its own "
    "water with its own `parts`, and a lake part (`part_of`) is its own water too. Symmetric (if "
    "0 lists 1, 1 lists 0), never lists the part itself, sorted, and `[]` when the part borders "
    "no other part. A PART IS NOT ONE STRETCH: it is every section carrying the same sets, so "
    "it may be several stretches that do not meet one another; `touches` says that SOME section "
    "of it borders the other part. TO MERGE NEIGHBOURS (e.g. 'closed all year' shown once for a "
    "run of closed stretches), merge only parts connected through `touches` — the connected "
    "groups of the graph whose edges are `touches`, among the parts that qualify — never every "
    "qualifying part of the water.")

#: WHAT A TIDAL WATER MEANS TO AN ANGLER — the page's words for `waters[item].tidal`, from the
#: book's own note (p.19, Nitinat Lake).
TIDAL_GUIDE = (
    "Tidal water: the federal tidal waters sport fishing regulations apply here, not the B.C. "
    "freshwater regulations. You need a federal Tidal Waters Sport Fishing Licence; no provincial "
    "quota, closure, gear rule, licence or stamp applies. Show this note at the top of the water, "
    "and never read its sections as 'open under the general rules'.")

#: How sure we are that steelhead are present (user ruling 2026-10-01) — on a part, and rolled up on
#: the water.
STEELHEAD_TEXT = (
    "HOW SURE WE ARE THAT STEELHEAD ARE HERE. \"known\": the book names steelhead on this water "
    "(its own row prints a steelhead quota, release, closure or 'Steelhead Stamp mandatory') or it "
    "is a tributary stream of such a water — or it is a lake whose own row names steelhead "
    "(Khartoum, Lois: they carry the whole provincial steelhead set, like a steelhead "
    "stream). \"possible\": a stream of Regions 1, 2, 3, 5 or 6 the book does not name "
    "steelhead on; the steelhead rules apply, but steelhead may not be present. ABSENT: no "
    "steelhead rule applies (a lake, or a region whose tables name no steelhead). SHOW: on "
    "\"possible\", a quiet line with the steelhead rules — 'Steelhead rules apply here; steelhead "
    "may not be present in this water.' On \"known\" show the steelhead rules plainly, no "
    "caveat. Absent: show nothing about steelhead. A part says it for its sections; the water's "
    "own `steelhead` is \"known\" if any part is known, else \"possible\" if any part is "
    "(for a list or a search row). Only \"known\" STREAM parts carry `anadromous_rainbow` (a "
    "rainbow over 50 cm is a steelhead there); on a \"possible\" stream a big rainbow is a rainbow.")

#: THE FILE, key by key — every top-level key (`dictionary_gaps` refuses one that is not here).
FILE_TEXT = {
    "about": "what the file is, the bundle it was read from (version, build, reach run and "
             "digest, `section_handles`), the counts, and corpus references that do not resolve",
    "guide": "how to read everything below — see `guide.contents`",
    "field_dictionary": "this: every key of the file and every field of its records, in words",
    "species": "the book's species list (p.86) under its headings, the groups and open subjects "
               "a rule may name, and the refused codes",
    "licences": "the document register: doc_id -> {name, provincial}",
    "entries": "every synopsis row, keyed by entry_id — see `entry`",
    "rules": "every rule, keyed `entry_id::rule_id` — see `rule`",
    "licensing": "every licensing record, keyed `entry_id#record_id` — see `licensing`",
    "rulesets": "the interned rule sets sections carry, keyed by a set id local to this file — "
                "see `rulesets{} / licensing_sets{}`",
    "licensing_sets": "the interned licensing sets, the same way",
    "waters": "every named water, keyed by its durable item_id — see `water`",
    "splits": "every cut a part's run can end at, by id — see `splits`",
    "index": "rule ids by type and by family, licensing ids by kind",
}

#: A water, key by key (`waters[item_id]`).
WATER_TEXT = {
    "name": "the water's name",
    "kind": "stream | lake | wetland, as the atlas types the water",
    "sections": "how many sections the water has; its parts' `sections` sum to it",
    "entries": "the synopsis rows that MATCH this water (their `matched` lists it) — not every "
               "row whose rules reach it: a zone, area or tributary walk reaches it through its "
               "parts' sets",
    "parts": "every (ruleset, licensing_set) pair its sections carry together — see "
             "`water.parts[]`",
    "outside_bc": "how many of its sections lie outside British Columbia (0 when none): they "
                  "carry no set; show 'outside B.C.' (`placement.outside_bc`)",
    "part_of": "ONLY on a lake PART: the item_id of the whole lake it was cut from "
               "(`placement.part_of`)",
    "steelhead": "ONLY where steelhead rules apply on some part: \"known\" if any part is known, "
                 "else \"possible\" — the roll-up of `water.parts[].steelhead`, for a list or a "
                 "search row",
    "steelhead_source": "ONLY on a water with KNOWN steelhead streams: the entry ids of the rows "
                        "naming steelhead that make it known — its own row, or the row of the "
                        "steelhead water it is a tributary of (the Nicola's is the Thompson's). "
                        "Absent on an own-row steelhead lake (Khartoum, Lois: its own row is in "
                        "`entries`) and on a \"possible\" water",
    "tidal": "ONLY where the book calls the water tidal (Nitinat Lake): {sections, entry, guide} "
             "— see `water.tidal`",
}

#: `waters[item].tidal`, key by key.
TIDAL_TEXT = {
    "sections": "how many of the water's sections are tidal",
    "entry": "the row that says the water is tidal",
    "guide": "the words to show at the top of the water (federal tidal regulations apply)",
}

#: A water's part, field by field.
WATER_PART_TEXT = {
    "ruleset": "the rule set its sections carry (a key of `rulesets`), or null: none",
    "licensing_set": "the licensing set its sections carry (a key of `licensing_sets`), or "
                     "null: none",
    "sections": "how many of the water's sections are in this part",
    "province_except": "present where the part lies in areas a province-wide requirement stops "
                       "at: the families of those areas — `national_parks` (for a record whose "
                       "extent names that kind), `tidal` (tidal water: EVERY province-wide "
                       "requirement stops there, whatever its record names)",
    "anadromous_rainbow": "present (true) where a rainbow over 50 cm is a steelhead — only on a "
                          "part whose `steelhead` is \"known\" (never a lake, never \"possible\")",
    "steelhead": STEELHEAD_TEXT,
    "touches": TOUCHES_TEXT,
    "runs": "WHERE THE PART RUNS: its stretches, upstream to downstream — [{from, to, km_from, "
            "km_to}] (see `water.parts[].runs[]`, and `placement.runs`). A part of several "
            "stretches has several runs. A lake or wetland part is its polygon: one run with no "
            "ends",
}

#: One run of a part, field by field.
RUN_TEXT = {
    "from": "the run's UPSTREAM end: an end token (`placement.runs.ends`) — a cut-point id (a key "
            "of `splits`), or a named natural end. null on a lake or wetland (a polygon has no "
            "ends)",
    "to": "the run's DOWNSTREAM end, the same way. Water flows from `from` to `to`",
    "km_from": "km from the water's MOUTH, along its main stem, at `from` (the larger number). "
               "null on a lake; on a `branch`, the point where the branch rejoins the main stem, "
               "or null when it never does",
    "km_to": "km from the mouth at `to` (km_to <= km_from). A run covers km_to..km_from of the "
             "main stem",
    "branch": "present (true) on a run that lies OFF the water's main stem — a side channel or "
              "braid of the same water whose sets differ from the stem beside it. Its ends are its "
              "own (its `source` and `mouth` are where it leaves and rejoins), and both km are the "
              "point it rejoins the stem",
    "polygon": "ONLY on a lake or wetland: `whole` (the run is the whole polygon), or — on a lake "
               "PART (`part_of`) — the part's own name ('Williston Lake — Nation Arm')",
}

#: The end tokens a run's `from` / `to` take. One vocabulary with `spans.end_token`.
END_TEXT = {
    "<split id>": "a cut the pipeline made at a curated split or a gauge station, by the SAME id "
                  "the rule extents use (`thompson_river__cnr_bridge`); its name and place are in "
                  "`splits`",
    "area:<name>": "an area boundary that is not a region (a park, an ecological reserve, a group "
                   "of management units) — also a key of `splits`. A run with this at BOTH ends "
                   "lies inside the area",
    "region_line:<region>": "a region boundary (`region_line:6`); a key of `splits`",
    "bc_border": "the provincial (or national) border: the water runs on outside B.C.",
    "lake_inlet:<item_id>": "the run ends where it flows INTO that lake (a key of `waters`; "
                            "`lake_inlet` alone: a lake that is not a named water)",
    "lake_outlet:<item_id>": "the run begins where it flows OUT of that lake (`lake_outlet` "
                             "alone: unnamed)",
    "confluence:<item_id>": "a cut the atlas made where that tributary (a key of `waters`) flows "
                            "in. Say it '<tributary name> confluence'",
    "mouth": "the water's own downstream end — into the sea, another river or a lake it ends in "
             "— with no cut",
    "source": "the water's own upstream end with no cut — its source, or where it takes its "
              "name (the Thompson at Kamloops)",
}

#: One cut in `splits`, field by field.
SPLIT_TEXT = {
    "name": "a short human name for the place: 'boundary signs', 'CNR bridge', 'Thompson River "
            "confluence', 'Garibaldi Park boundary' — the curated split's own label",
    "kind": "how the cut was placed: point | line | confluence | lake (a lake's inlet or outlet, "
            "with an offset) | gauge (a hydrometric station) | area (an area's boundary) | "
            "region_line",
    "water_id": "the named water whose main stem it stands on, when it stands at ONE place on "
                "one; null otherwise (an area boundary, or several places — see `at`)",
    "km": "km from that water's MOUTH along its main stem; null with `water_id`",
    "at": "ONLY on a cut at several places on named waters' main stems: every [{water_id, km}]",
}

#: A designation's classified period (`licensing[*].period`, designations only).
PERIOD_TEXT = {
    "kind": "when_open: the water is Classified whenever it is open (the book prints 'Class II "
            "water when open' or 'when/where open' — no dates, by design: licensing is consulted "
            "only while the water is open) | all_year: Classified every day ('Class I water all "
            "year') | dates: only on `dates`",
    "dates": "ONLY with kind `dates`: the designation's own `fields.when.dates`, as printed",
    "says": "the sentence to show: 'Classified (Class II) whenever this water is open', 'Class I "
            "all year', or 'Classified (Class II) Sep 1-Apr 30'",
}

TYPE_TEXT = {
    "retention_limit": "How many of a fish you may keep, on which clock, of which sizes. A "
                       "quota, a catch-and-release, a size limit and a closure to a species are "
                       "all this one type; `take`, `may_target` and `lengths` tell them apart.",
    "stop_fishing_after_quota": "Once you have caught and kept your quota of this fish (on the "
                                "clock in `period` — the book: 'your DAILY quota of hatchery "
                                "steelhead') you must stop fishing THAT WATER for the rest of "
                                "that period ('for the remainder of that day'), whatever you "
                                "fish for. It counts nothing itself.",
    "bait_restriction": "What may be on the hook — bait bans and the bait that is allowed "
                        "anyway. Carries `gear` clauses on the `bait` slot.",
    "tackle_restriction": "The rig: hooks, points, barbs, lures, flies, lines, weights. Carries "
                          "`gear` clauses.",
    "method_rule": "Whether a way of fishing is allowed at all — angling, ice fishing, spear "
                   "fishing, set lining, crayfish trapping, netting, snagging. Carries `gear` "
                   "clauses on the `method` slot or a spec slot, or `conduct`.",
    "vessel_rule": "Boats: whether they are allowed, under what propulsion, at what speed, "
                   "towing. Read `aspect` first.",
    "navigation_duty": "What a boat must do for other traffic.",
    "angler_closure": "The water is closed to ONE KIND of angler (`closed_to`), on the days in "
                      "`when`. A closure, never a quota; everyone else is unaffected.",
    "handling_rule": "What you must do with a fish or your gear — release immediately, do not "
                     "waste it. Carries `conduct` and/or `gear`.",
    "hazard": "A warning about the place. Governs nothing.",
    "advisory": "Information the book prints. Governs nothing, and must never be read as a "
                "limit.",
    "program_membership": "The water belongs to a named programme. Governs nothing.",
    "facility": "What is there — a launch, an accessible pier. Governs nothing.",
}

TYPE_CAN_CLOSE = {
    "retention_limit": "Yes — `take: 0` with `may_target: false` and no `while`, and not "
                       "`standing`, means you may not fish for those species at all. On "
                       "ALL_GAME_FISH that shuts the water for the dates in `when`.",
    "angler_closure": "For the anglers in `closed_to` only.",
    "method_rule": "Closes a method (a `ban` on the `method` slot), never the water.",
}

FAMILY_TEXT = {
    "retention": "what you may keep",
    "gear_and_method": "how you may fish",
    "vessel": "what your boat may do",
    "access": "who may fish here at all",
    "conduct": "what you must do",
    "information": "what the book tells you; governs nothing",
}

SLOT_TEXT = {
    "bait": "what is on the hook: any_bait, fin_fish, roe, invertebrate, dead_fin_fish, …",
    "lure": "the terminal object: artificial_fly, artificial_lure. `only: [artificial_fly]` is "
            "'artificial fly only' — NOT the same law as fly fishing only (see `definitions`)",
    "method": "how you fish: angling, fly_fishing, ice_fishing, set_lining, spear_fishing, … "
              "`only: [fly_fishing]` is 'fly fishing only' — NOT the same law as artificial fly "
              "only (see `definitions`)",
    "barb": "barbed | barbless — a barbless-hook rule is `only: [barbless]`",
    "set_lining": "how a set line must be built or marked",
    "crayfish_trapping": "how a crayfish trap must be built",
    "downrigger": "how a downrigger must be rigged (e.g. quick-release)",
    "light": "how a light must be used (e.g. submerged, attached to the line)",
    "ice_hut": "what an ice hut must be or carry (binds `while: [ice_fishing]`)",
    "hooks_per_line": "number of hooks on one line",
    "points_per_hook": "number of points on one hook — a single hook is `max: 1`",
    "lines_per_angler": "number of lines one angler may fish",
    "flies_per_line": "number of flies on one line",
    "terminal_attachments_per_line": "hooks, lures and flies together, on one line",
    "hook_gap_mm": "hook gap in millimetres",
    "weight_per_line_kg": "weight on one line, in kilograms",
    "bait_possession_kg": "bait you may possess, in kilograms",
    "light_to_hook_mm": "distance from a light to the hook, in millimetres",
}

#: The book's two fly-only laws, verbatim from its Definitions page, and how each is encoded. They
#: are DIFFERENT laws — a float or a sinker is lawful under one and not the other — and the corpus
#: encodes each where the book prints it; a reader must never merge them.
FLY_DEFINITIONS = {
    "lure": {
        "encoded_as": "{slot: lure, only: [artificial_fly]}",
        "book": "artificial fly: … Where gear is restricted to artificial flies, floats and "
                "sinkers may be attached to the line.",
        "means": "only artificial flies as the terminal object; floats and sinkers are allowed",
        "differs_from": "method",
    },
    "method": {
        "encoded_as": "{slot: method, only: [fly_fishing]}",
        "book": "fly fishing: angling with a line to which only an artificial fly is attached "
                "(floats, sinkers, or attracting devices may not be attached to the line when "
                "fishing is restricted to \"fly fishing only\").",
        "means": "fly fishing only: nothing but the fly on the line — no float, sinker or "
                 "attracting device",
        "differs_from": "lure",
    },
}

CLAUSE_TEXT = {
    "slot": "what the clause constrains — see `slots`",
    "of": "which members of the slot it speaks about; absent = all of them",
    "allow": "these are permitted; says nothing about the rest of the slot",
    "only": "a whitelist that closes the slot: nothing else is permitted",
    "ban": "these are prohibited; a total ban names the whole-slot member (any_bait)",
    "except": "members carved out of a `ban`",
    "members": "a choice of ONE from several kinds, qualifying a count "
               "('one hook, one lure or one fly')",
    "max": "a ceiling, in the unit the slot's name carries",
    "min": "a floor, in the unit the slot's name carries",
    "unlimited": "no ceiling on the count, said outright",
    "when": "the condition under which this clause applies (a `GearWhen`); absent = always",
    "requires": "how the thing itself must be built or carried (a `GearSpec`)",
    "must_be": "on a spec slot: the states the thing must be in",
    "unless": "conditions (each a `GearWhen`) that lift this clause",
}

GEAR_WHEN_TEXT = {
    "water": "stream | lake",
    "method": "while fishing by this method",
    "targeting": "while fishing FOR these species",
    "angler": "alone_in_boat | in_boat | in_powered_boat | from_shore — 'No angling from "
              "boats' is a method ban on angling `when: {angler: in_boat}`",
    "gear_in_use": "while this gear is in use",
    "note": "a condition the closed vocabulary cannot say; the rule then carries a "
            "review_reason",
}

GEAR_SPEC_TEXT = {
    "attached_to": "what it must be attached to (fishing_line)",
    "attachment": "how it is attached (quick_release)",
    "within_m_of_hook": "how close to the hook, in metres",
    "opening_shape": "the shape of a trap opening (circular)",
    "note": "a property the closed vocabulary cannot say; costs a review_reason",
}

#: The model's rule fields that a reader of this file meets, in words.
RULE_FIELD_TEXT = {
    "obligation": "should: the book gives this as ADVICE, not law. Absent = must (law) — "
                  "the default, so it is not repeated on every rule.",
    "species": "species codes the rule is about, as the book wrote them (groups included — "
               "expand with `species.groups`)",
    "species_except": "species carved out of `species`",
    "closed_to": "angler_closure only: WHO the water is closed to (a `Who`)",
    "closed_to_except": "angler_closure only: the anglers INSIDE `closed_to` the water stays "
                        "open to, each a `Who` — a Youth/Disabled Accompanied Water is closed to "
                        "anglers 16 and over except disabled B.C. residents and the companions of "
                        "an authorized angler",
    "gear": "ordered list of gear clauses — see `gear`",
    "derived_from": "the rule id (same entry) whose sentence implies this one",
    "condition_of": "the rule id (same entry) this rule is the proviso of",
    "while": "the means of fishing during which the rule binds; absent = any",
    "conduct": "act tokens: what you must or must not do — see `gear.conduct`",
    "when": "when the rule binds — see `time`; absent = all year",
    "take": "how many you may keep",
    "unlimited": "there is no number",
    "may_target": "false = you may not fish for it; true = fish for it and release",
    "period": "daily | possession | annual | monthly — the clock a quota runs on. Carried by "
              "every retention_limit (the clock its own number runs on) and every "
              "stop_fishing_after_quota (the clock of the quota whose filling stops you: the book "
              "prints 'your daily quota … for the remainder of that day', pp.21/49/51), and by "
              "nothing else.",
    "per_daily": "a possession limit as a multiple of the daily one",
    "within": "the rule id (same entry) this is a clause of; it counts inside its parent",
    "lengths": "ordered length ranges, each with its own take — see `sizes`",
    "record_retention": "keeping this fish must be recorded on your licence",
    "water": "stream | lake — the rule binds only on that kind of water",
    "origin": "wild | hatchery; absent = both",
    "when_targeting": "the species you are fishing FOR (bait and tackle rules)",
    "aspect": "vessel_rule: propulsion | speed | towing",
    "level": "vessel_rule propulsion: none | unpowered | electric_only | power_capped",
    "max_power_kw": "vessel_rule: the motor cap, kW",
    "max_kmh": "vessel_rule: the speed cap, km/h",
    "includes_tributaries": "true/false: the rule reaches (or not) the water's tributaries; "
                            "absent = inherit the entry's",
    "tributaries_only": "the tributaries, without the named water itself",
    "extents": "where on the water (or in which areas) the rule applies, as the reach builder "
               "reads it. A rule with none of its own is not given its entry's. What the rule "
               "STATES, not where it holds: read `binds` for that.",
    "exempts": "what the rule lifts — see `exempts`",
    "standing": "true: holds everywhere at places no dataset can draw — see `standing`",
    "authority": "superior: a federal or park authority, above the provincial ladder",
    "notice": "the DFO fishery notice the rule was published in",
    "suspended_while": "a rule id (same entry): this rule is dormant while that one binds",
    "extent_text": "the book's words for a place nothing could draw",
    "undrawn_part": "the book's words for the PART of the bound water this rule holds in, which "
                    "nothing draws. The rule is placed on the whole water it is in; show it there "
                    "as a note ('in <part>') and never colour the water by it — see `binds`",
    "life_stage": "adult: the rule holds for fish of that LIFE STAGE only, as the sentence prints "
                  "it — 'record your retention of adult chinook salmon' (p.7). The book defines "
                  "'adult' for chinook only (p.77: over 50 cm nose to fork in most non-tidal "
                  "waters, over 62 cm in some rivers), so the stage is its word, never a length; "
                  "the label's `what` says it ('Adult chinook')",
    "side": "north | south | east | west: the rule holds on that HALF OF THE CHANNEL only, "
            "lengthwise ('No Fishing on the west half of river …', Kitimat River). It is placed "
            "on the stretch its extents draw, but an angler on the other half follows the "
            "water's other rules there: the reference answers it `beside` them (it displaces "
            "nothing), and its `parts.side` says which half — show it; never colour or close the "
            "whole width by it (`gotchas.one_side_of_channel`)",
}

#: THE PARTS OF A LINE, in words. Checked against `catalogue.LABEL_PARTS` and
#: `catalogue.LICENSING_PARTS` (see `_registries`), so a part the model gains is refused until it is
#: explained here.
PART_TEXT = {
    "what": "the rule itself — its verdict and subject: 'No fishing for bull trout in streams', "
            "'Rainbow trout — 2 per day', 'Bait ban', 'Speed restriction (10 km/h)'. ABSENT when "
            "the rule has no structured content (an advisory, a hazard, a bare exemption): show "
            "the verbatim instead, marked as the book's text",
    "size": "the length bound: 'none under 30 cm' (a bound — write it in parentheses) or 'over "
            "50 cm' (a size class — attach it: 'release all over 50 cm')",
    "conditions": "which fish or which fishing: 'wild only', 'from streams', 'when fishing for "
                  "salmon', 'while set lining', 'in possession'",
    "when": "dates, weekdays, hours, and any season nobody could read ('as printed: …' — "
            "uncertain, never all year)",
    "where": "the place in the book's words, or named from what the extents draw",
    "side": "the half of the channel the rule holds on (`fields.side`): 'west half of the "
            "channel only — on the east half, this water's other regulations apply'. Show it "
            "wherever the rule is shown",
    "in_part": "the undrawn part the rule holds in (`fields.undrawn_part`): a note, never a "
               "colour — see `binds`",
    "lifts": "what the rule exempts from",
    "duty": "what you must do: 'record your retention on your licence immediately'",
    "suspended": "'not while “<the rule it sleeps under>” is in force'",
    "notice": "the DFO fishery notice it was published in",
}
LICENSING_PART_TEXT = {
    "who": "the anglers it is about",
    "what": "the fact: a designation's class, 'Not a Classified Water', the document sold, "
            "what an alternative accepts",
    "need": "what must be held (any one of the ways joined by 'or'); on an exemption, "
            "'no <documents>'",
    "must": "a duty to do",
    "way": "a way to satisfy it that is not a document ('be accompanied by …')",
    "doing": "the activity that triggers it ('to fish', 'to keep steelhead')",
    "where": "on which water, or during which designation's period",
    "when": "the dates, days and hours it holds",
    "unit": "the licence unit(s), as the page names them",
    "stamp": "the classified-water Steelhead Stamp here: its period, or its waiver",
    "terms": "how a document is sold",
    "instead": "what an alternative stands in for",
    "except": "anglers taken out of `who`",
    "suspended": "not in force while the named closure applies",
    "note": "a superior authority's note ('provincial licences are not valid here')",
}

#: Keys on an exported rule record outside `fields`.
RECORD_TEXT = {
    "id": "`entry_id::rule_id` — a rule id is unique only within its entry",
    "entry_id": "the synopsis row the rule was read from",
    "rule_id": "the rule's id inside its entry",
    "type": "one of `rule_types`",
    "family": "the type's family — see `families`",
    "dimension": "the second half of the competition key, (type, dimension) — see `ladder`",
    "label": "a PREVIEW: `parts` composed by the model's one composer (catalogue.compose). "
             "Never authored. A reader composes its own line from `parts` — see `labels`",
    "parts": "the rule's line as parts, each generated from its fields — see `labels`",
    "verbatim": "the sentence from the printed book, quoted",
    "binds": "where the rule holds: " + " | ".join(BINDS_TEXT) + " — see `placement.binds`",
    "not_yet_mapped": "ONLY on a rule that holds in a part nothing draws (`binds: "
                      "sections_in_part`): {display: 'prominent', part: the book's words for the "
                      "part, identified: false when those words name no place ('on parts', "
                      "'various locations' — the regulations never say which parts), says: the "
                      "sentence to show}. Show it AT THE TOP of the water, marked "
                      "as a place not yet mapped — never as the water's rule. It never decides "
                      "open or closed, never displaces or lifts anything — see "
                      "`placement.not_yet_mapped`",
    "fields": "the rule's own fields, as the bundle ships them",
    "provenance": "who wrote it and what it binds to — see below",
    "recorded_by": "ONLY on an annual quota (`period: annual`) whose record duty the book "
                   "prints: the `entry_id::rule_id` of that `record_retention` rule. Show its "
                   "text once, under the quota; never generate a 'record' line of your own — "
                   "see `retention.record_duty`",
    "records_for": "ONLY on a `record_retention` rule that is an annual quota's record duty: "
                   "the quotas (`entry_id::rule_id`) it is shown under — see "
                   "`retention.record_duty`",
}

PROVENANCE_TEXT = {
    "entry_name": "the entry's display name",
    "authority": "who wrote it: superior | province | region",
    "binds_to": "what it binds to: region (the region's standing table), area, water",
    "rank": "the ladder position derived from (authority, binds_to); smaller speaks first",
    "who": "the two axes in words",
    "scope": "the bundle's own `scope` column: section | area — whether the rule was written "
             "for a water or for an area",
    "uncertain": "the reach builder could not place it; it binds no section",
    "why": "the reach builder's reason, when uncertain",
}

LICENSING_RECORD_TEXT = {
    "id": "`entry_id#record_id`",
    "entry_id": "the synopsis row the record was read from",
    "record_id": "its id inside the entry",
    "kind": "one of `licensing.kinds`",
    "label": "a PREVIEW: `parts` composed by catalogue.compose_licensing; never authored",
    "parts": "the record's line as parts, each generated from its fields — see `labels`",
    "verbatim": "the sentence from the printed book",
    "fields": "the record's own fields, as the bundle ships them",
    "placement": "sections | province | on_designation | unresolved | not_placed",
    "period": "ONLY on a designation: its classified period, said outright — {kind: when_open | "
              "all_year | dates, dates?, says} (see `licensing.period`). Derived from the record's "
              "own verbatim and `when`; never absent on a designation",
    "provenance": "entry_name, and for an unresolved record `uncertain` and `why`",
}

LICENSING_KIND_TEXT = {
    "designation": "A FACT about a water: while `when` holds, the bound sections are a "
                   "Classified Water of class `classified` (I or II), in licence unit `unit`. "
                   "It obliges nothing itself; requirements fire on it. It may carry the "
                   "classified-water steelhead stamp period (`steelhead_stamp_during`) or its "
                   "waiver (`steelhead_stamp_waived`), and sleep while a closure rule binds "
                   "(`suspended_while`).",
    "not_classified": "An asserted ABSENCE: this part is NOT a Classified Water. Where a "
                      "designation also reaches it, the designation's binding there is "
                      "`contested` and the reader must say 'check'.",
    "requirement": "An OBLIGATION: anglers in `who` (minus `who_except`), `doing` this, where "
                   "and when it binds, must satisfy ANY ONE of `satisfied_by`, or do the "
                   "`conduct`. `on` ties it to a designation's period.",
    "licence_terms": "How a document is SOLD — per day or per licence year, day limits, "
                     "draws, fees. Never placed: attach it to the obligation whose document, "
                     "who, unit and class it names.",
    "exemption": "Anglers in `who` are released from `documents` AND from every duty whose "
                 "`presumes` are all among them ('produce your angling licence' means nothing to "
                 "an angler who need not hold one). Never placed.",
    "alternative": "A place where another document ALSO satisfies a requirement "
                   "(`alternative_to`). It only ever adds a path.",
}

LICENSING_FIELD_TEXT = {
    "classified": "I | II",
    "unit": "the licence unit a non-resident's per-day licence names",
    "unit_name": "the unit in words",
    "when": "when the record holds (a `When`); absent = all year",
    "extents": "where it applies, as the reach builder reads it",
    "includes_tributaries": "true/false; absent = inherit the entry's",
    "tributaries_only": "the tributaries without the named water",
    "tributary_excludes": "waters the tributary walk must not enter",
    "steelhead_stamp_during": "{when, verbatim}: the classified-water steelhead stamp runs here "
                              "then, whatever you fish for",
    "steelhead_stamp_waived": "{verbatim}: that stamp is not required here",
    "suspended_while": "[{rule_id, verbatim}]: dormant while that closure rule (same entry) "
                       "binds",
    "review_reason": "what a curator still has to settle",
    "satisfied_by": "the ways to satisfy it — ANY ONE path (see `paths`)",
    "conduct": "act tokens the requirement obliges (see `gear.conduct`)",
    "who": "which anglers (a `Who`); absent = every angler",
    "who_except": "anglers taken out of `who`",
    "doing": "what the angler is doing that triggers it (a `Doing`)",
    "water": "stream | lake",
    "on": "classified_period | steelhead_period: holds wherever a designation's period (or its "
          "stamp period) is in force",
    "authority": "superior: a federal or park authority; displaces every provincial obligation",
    "restates": "{entry_id, id}: the provincial record this row's own words repeat",
    "document": "the document sold (see `licences`)",
    "units": "the licence units the terms are about; absent = every unit",
    "sold": "per_licence_year | per_day",
    "covers": "every_unit | one_unit",
    "max_consecutive_days": "at most this many days in a row",
    "max_days_per_licence_year": "at most this many days in a licence year",
    "max_per_licence_year": "at most this many of the document in a licence year",
    "max_units_per_licence_year": "at most this many licence units in a licence year",
    "unlimited_days": "no day limit, said outright",
    "allocation": "open | booking | draw",
    "needs": "what a buyer must supply (angling_guide_number)",
    "fee_cad": "the fee in dollars",
    "documents": "the documents released",
    "presumes": "a conduct duty ABOUT these documents ('produce your angling licence'): it binds "
                "only an angler who must hold them, so an exemption releasing all of them "
                "releases the duty too",
    "alternative_to": "{entry_id, id}: the requirement this adds a path to",
}

DOING_TEXT = {
    "fishing": "any sport fishing at all",
    "targeting": "fishing FOR `species`",
    "retaining": "KEEPING `species` (of `lengths`, when given — the lengths name WHICH fish)",
    "retaining_recorded": "keeping a fish whose retention must be recorded on the licence "
                          "(which fish: rules with `record_retention`)",
    "guiding": "acting as an angling guide",
}

WHEN_TEXT = {
    "dates": "[{from_month, from_day, to_month, to_day}] — calendar ranges, no year, both ends "
             "inclusive; a range whose end is before its start wraps the year end",
    "hours": "{start, end}, each {at: 'HH:MM'} or {solar: sunrise|sunset, offset_min} "
             "(negative = before); wraps midnight the same way",
    "weekdays": "the days of the week it holds on; empty = every day",
    "unparsed": "a printed season nobody could read, verbatim. The rule is UNCERTAIN in time — "
                "never read it as all year",
}

#: One resolved lift, as the bundle ships it (`pipeline.deliver.bundle.rules.LIFT_KEYS`).
LIFT_TEXT = {
    "entry_id": "the entry of the rule lifted — resolved when the bundle was built, always present",
    "rule_id": "the rule lifted, in `entry_id`. One item per lifted rule: a zone default named by "
               "slug is resolved to each of its rules the lift reaches",
    "note": "the book's words, often the only statement of WHERE the lift reaches",
    "species": "the lift holds only for these fish (leaf codes) — the lifter names fewer than the "
               "lifted rule does, so the rule still binds every other species. Absent = every "
               "fish the lifted rule names",
    "when_targeting": "the lift holds only when fishing FOR these. The angler is unknown, so the "
                      "lifted rule stays and the lift is a condition on it",
    "while": "the lift holds only while doing these; the lifted rule binds everyone else",
    "when": "the lift holds only on these days/hours (the lifter's own `when`, shape as in "
            "`time`); on every other day the lifted rule binds. Absent = the lifter is in force "
            "every day the lifted rule is. A lift is never in force while its lifter is not",
    "origin": "the lift holds only for fish of this origin (hatchery | wild): the lifter keeps "
              "only those. Which a fish is shows only once it is caught, so the lifted rule stays "
              "and the lift is a condition on it (Kitimat River's 'Hatchery steelhead … daily "
              "quota = 2' could never reopen a closure for WILD steelhead; Kitimat's 'Hatchery "
              "rainbow trout … daily quota = 5, all year' lifts Region 6's stream release for "
              "hatchery rainbow only — a wild rainbow is still released Nov 1-June 30). "
              "A printed lift carries it as much as a derived one",
    "lengths": "[{min_cm, max_cm}] — the lift holds only for fish of these sizes (the sizes the "
               "lifter keeps). The lifted rule stays and the lift is a condition on it. A band "
               "that only restates a fish's own definition (a steelhead is a rainbow over 50 cm) "
               "is not carried. Only a derived lift (`basis`) carries it",
    "basis": "names_the_fish: the book prints no exemption; the lifter is a water row that NAMES "
             "a fish its region closes ('bass daily quota = 8' under Region 8's 'Bass: 0 quota, "
             "CLOSED TO FISHING (see tables for exceptions)'), and so lifts that closure for the "
             "fish both name — never further than the lifter covers (its dates, `origin`, "
             "`lengths`, and only where the lifter itself is bound), and never a closure whose "
             "own entry prints its exemptions. Derived when the bundle is built — never infer it. "
             "Absent = the lifter's own printed exemption",
    "equivalent": "'<entry_id>::<rule_id>' — the lifter's printed exemption names that blanket "
                  "closure of its own region, and this item is the SAME KIND of closure in "
                  "another region the lifter's water lies in: the same kind of water, and the "
                  "same closure BY NAME — spring, summer or winter, as the lifter's own words "
                  "say it ('Exempt from spring closure') or, where they name none ('Mainstem "
                  "open all year'), as the closure it names is called. Dates never decide it: "
                  "the Nechako's 'Exempt from spring closure' lifts Region 6's Fraser-watershed "
                  "spring closure (Apr 1-June 30) and never its Skeena/Nass winter closure (Jan "
                  "1-June 15), though the two overlap. It names EVERY closure of that kind — "
                  "the Nechako's also names the Iskut part of Region 6's spring closure — but a "
                  "lift only ever acts on the lifter's own sections: the water can lift nothing "
                  "but itself, so a named closure that never touches the water changes nothing. "
                  "West Road River's Region 5 row, 'the "
                  "regional spring closure does not add to its own mainstem closure', lifts Zone "
                  "7A's spring closure on its Zone 7A pieces. It holds only where the lifter is "
                  "bound. A species closure is never lifted this way. Absent = the book names "
                  "this rule",
    "caution": "{kind, says} — an INTERPRETATION WARNING to show beside the lift. kind "
               "'size_clause_override': the lift removes a region's size clause ('only 1 over 50 "
               "cm') because the water row prints a larger number for the fish (user ruling "
               "2026-09-26) AND prints it '(any size)' (user ruling 2026-09-28) — whether that "
               "means no size limit at all or only no minimum, the book does not say. The lift "
               "applies; `says` is the plain sentence to show with it. A row printing its own "
               "sizes, or none, lifts the clause with no caution. See `gotchas`",
}

LENGTH_TEXT = {
    "min_cm": "inclusive lower bound; absent = open",
    "max_cm": "inclusive upper bound; absent = open",
    "take": "how many of THESE you may keep; absent = the rule's own `take`",
}

WHO_TEXT = {
    "residency": "resident (of B.C.) | non_resident (not a B.C. resident, but a Canadian citizen "
                 "or permanent resident, or living in Canada) | non_resident_alien (neither)",
    "age": "under_16 | 16_plus",
    "guidance": "guided | non_guided",
    "status": "indian_bc_resident | metis | disabled",
    "role": "companion — fishing as the companion of an authorized angler on a Youth/Disabled "
            "Accompanied Water (printed p.4); not a partition: most anglers are no one's companion",
}

PATH_TEXT = {
    "hold": "hold ALL of these documents",
    "accompanied_by": "be accompanied by an angler in `who` holding what this fishing requires "
                      "of them",
    "as": "satisfy the requirements AS this `Who` instead",
    "quota": "own | counts_to_companion — a NOTE on an accompaniment path: whose quota the fish "
             "count against. Render it (an asterisk, a line); it is not arithmetic.",
}

VIA_TEXT = {
    "reach": "the record names this water (or area) and binds it directly",
    "trib": "it reached this section by the tributary walk from a water whose entry includes "
            "tributaries",
    "trib_pending": "a tributary walk that was not done — never treat the set as complete",
    "contested": "a designation on a section a not_classified record also binds — say 'check'",
}

#: A rule set or licensing set (`rulesets[id]`, `licensing_sets[id]`), key by key.
SET_TEXT = {
    "sections": "how many sections carry this set",
    **{k: f"the member ids that reach these sections this way — {v}" for k, v in VIA_TEXT.items()},
}

PLACEMENT_TEXT = {
    "sections": "bound to sections; find them through `licensing_sets`",
    "province": "applies everywhere; no section rows — EXCEPT, when its extent names "
                "`outside_area_kind`, on the sections that kind covers (bundle table "
                "`province_except`; on a water, each of its `parts` lists the families its "
                "sections lie in as `province_except`). The basic licence and "
                "the stamps do not hold inside National Parks, whose own permit does. On "
                "`province_except` kind `tidal` NO province-wide requirement holds, whatever its "
                "record names: tidal water is federal (the Tidal Waters Sport Fishing Licence)",
    "on_designation": "applies wherever a designation is in force (`on`); no section rows",
    "unresolved": "could not be placed: `provenance.uncertain` is true and `why` says why. For "
                  "licensing the unsafe direction is under-requiring, so render 'check', never "
                  "'none needed'",
    NOT_PLACED: "licence_terms and exemptions are never bound to a place",
}

ENTRY_TEXT = {
    "kind": "province | zone | area | water — see `entries`",
    "chapter": "the id prefix: zp (province), z<region> (a region's chapter), r<region> "
               "(a water in that region)",
    "name": "the display name", "full_name": "the heading as printed",
    "item_id": "the first water it matched", "matched": "every water it matched",
    "mus": "the management units the row was printed under",
    "pages": "the synopsis pages it is printed on", "symbols": "the printed glyphs, 1:1",
    "scope_note": "the curated sentence on which part of the water the entry covers",
    "extents": "the entry's own reach", "printed": "the whole printed passage",
    "rules": "its rule ids", "licensing": "its licensing record ids",
    "see": "its pointers, when it prints any: [{verbatim, entry_ids, relation}] or "
           "[{verbatim, unresolved}] — see `entries.pointers`",
}


# --------------------------------------------------------------------------------------------
# The guide, built from the model's registries and the data
# --------------------------------------------------------------------------------------------

def _enum(e) -> list[str]:
    return [m.value for m in e]


def _fields(model) -> list[str]:
    return [f.alias or n for n, f in model.model_fields.items()]


def _licensing_models() -> dict:
    return {typing.get_args(m.model_fields["kind"].annotation)[0]: m
            for m in typing.get_args(typing.get_args(C.LicensingRecord)[0])}


def _example(x: dict, *keys: str) -> dict:
    """A record, cut to what the concept is about. Always a real record, found by id."""
    got = {"id": x["id"], "label": x["label"], "parts": x["parts"], "verbatim": x["verbatim"]}
    if keys:
        got["fields"] = {k: x["fields"][k] for k in keys if k in x["fields"]}
    return got


class _Pick:
    """Examples chosen from the data by a test, never by a remembered id — so every example is
    current, and one that stops existing is simply replaced by the next match."""

    def __init__(self, recs: dict):
        self.recs = recs

    def __call__(self, test, *keys, n: int = 2) -> list[dict]:
        return [_example(x, *keys) for x in self.recs.values() if test(x)][:n]


def _slot_kind(s) -> str:
    if s in C._SET_SLOTS:
        return "set"
    if s in C._SPEC_SLOTS:
        return "spec"
    if s in C._MEASURED:
        return "measured"
    return "counted"


def _f(x):
    return x["fields"]


def _gear(x):
    return _f(x).get("gear") or []


def _closed_range(b):
    return b.get("min_cm") is not None and b.get("max_cm") is not None


def guide(d: dict) -> dict:
    rules, lic = d["rules"], d["licensing"]
    pick, lpick = _Pick(rules), _Pick(lic)
    types = Counter(x["type"] for x in rules.values())
    dims = defaultdict(Counter)
    used = defaultdict(Counter)
    for x in rules.values():
        dims[x["type"]][x["dimension"]] += 1
        for k in _f(x):
            used[x["type"]][k] += 1

    # ---- rule types and families -------------------------------------------------------
    rule_types = {}
    for t in _enum(C.RuleType):
        rule_types[t] = {
            "family": C._FAMILY[C.RuleType(t)],
            "means": TYPE_TEXT.get(t),
            "can_close_a_water": TYPE_CAN_CLOSE.get(t, "No."),
            "rules": types.get(t, 0),
            "fields_used": dict(used[t].most_common()),
            "dimensions": dict(dims[t].most_common(8)),
            "examples": pick(lambda x, t=t: x["type"] == t,
                             *[k for k, _ in used[t].most_common()
                               if k not in ("obligation", "period", "extents")][:5]),
        }
    families = {f: {"means": FAMILY_TEXT.get(f),
                    "types": [t for t in _enum(C.RuleType) if C._FAMILY[C.RuleType(t)] == f],
                    "rules": sum(types.get(t, 0) for t in _enum(C.RuleType)
                                 if C._FAMILY[C.RuleType(t)] == f)}
                for f in sorted(set(C._FAMILY.values()))}

    ranks = [{"authority": Authority.superior.value, "binds_to": "any", "rank":
              Source(Authority.superior, Scope.water).rank}]
    for a in (Authority.province, Authority.region):
        for sc in Scope:
            src = Source(a, sc, "4")
            ranks.append({"authority": a.value, "binds_to": sc.value, "rank": src.rank,
                          "in_words": src.words()})
    ranks.sort(key=lambda r: (r["rank"], r["authority"], r["binds_to"]))
    ladder = {
        "reference": "`pipeline.deliver.bundle.read.effective_rules(section, date, fish)` is this "
                     "ladder as code — the reference semantics the app must match; "
                     "`pipeline/tests/test_competition.py` holds the real cases it reproduces.",
        "per_fish": "Competition is decided PER FISH. Ask about one fish (a leaf code); a rule "
                    "takes part only if it speaks for that fish (its `species` expanded, minus "
                    "`species_except`; a rule naming no species speaks for every fish; "
                    "`when_targeting` rules only for the fish targeted). Two rules compete only "
                    "for the fish BOTH speak for: Zone B's 'Bull trout … release' speaks only for "
                    "bull trout, so it never displaces 'Trout/char: 5' for a rainbow, a lake trout "
                    "or any other trout or char — for them the 5 still applies.",
        "competition": "Two rules COMPETE only when they share (type, dimension) and speak for the "
                       "fish asked about. A water's "
                       "daily trout quota competes with its region's daily trout quota; a "
                       "fly-only rule and a barbless rule have different dimensions and BOTH "
                       "apply. Rules that do not compete all apply. Competition is decided "
                       "among the rules IN FORCE at the moment asked about (`when`): a rule "
                       "whose dates, weekdays or hours exclude that moment displaces nothing, and "
                       "the rules it would have displaced speak — the book's own reading "
                       "('Lake trout catch and release EXCEPT during months of February and July "
                       "(when regional quotas apply)'). So a water's dated quota gives way to its "
                       "region's outside its dates, and a stream's 'No fishing, Jan 1-Jun 15' "
                       "does not silence its region's lake trout release on Oct 1. A record "
                       "dormant under `suspended_while` is not in force (today only licensing "
                       "records carry it — no regulation rule does). A rule uncertain in time "
                       "(`when.unparsed`), or asked about for a date when it holds only some "
                       "hours, is shown BESIDE what it would displace, each with its own `when`, "
                       "never in place of it; so is a rule holding on one half of the channel "
                       "only (`fields.side`), which the other half's anglers do not answer to. A lift is in force only while its lifter is "
                       "(`exempts[].when`).",
        "who_speaks": "Among competitors for one fish, in this order: (1) a superior authority "
                      "(rank -1) is outside the ladder: nothing below it opens what it closed. "
                      "(2) NAMING: a rule that NAMES the fish beats a rule that names a GROUP "
                      "holding it, even when the group rule is written for a more specific place. "
                      "Kakwa Lake prints 'Trout/char daily quota = 2 (none under 40 cm)' and lies "
                      "in the Peace River watershed of Zone B, whose table prints 'Bull trout … "
                      "release … from the Peace River watershed all year': for a bull trout of any "
                      "size — 40 cm and over included — the zone's release speaks and none may be "
                      "kept; for every other trout or char the lake's 2 speaks (it outranks the "
                      "zone's 'Trout/char: 5' by place). A `within` clause is named at its parent "
                      "quota's level: 'Trout/char: 5, but not more than 1 bull trout' is a "
                      "trout/char quota, not a bull trout rule. (3) PLACE, within the same naming "
                      "level: the smaller rank speaks — a rule bound to this water beats one "
                      "bound to an area, which beats the region's standing table, which beats "
                      "the province. So a water row that itself NAMES the fish is authoritative: "
                      "Williston Lake (Zone B) prints 'Bull trout daily quota = 1 (none under 30 "
                      "cm or over 50 cm), Oct 16-Aug 14' and the Liard River watershed row prints "
                      "the same limit; both name bull trout, so on their dates they outrank the "
                      "zone's bull trout release. `binds_to` decides before `authority`: a "
                      "provincial rule written for one lake speaks there before the region's "
                      "table. A WATER TABLE'S AREA ROW — every extent `within` a named area "
                      "('CRESTON VALLEY WILDLIFE MANAGEMENT AREA (CVWMA) WATERS … EXCEPT Duck "
                      "Lake', Bowron Lake Park waters, the Liard River watershed) — is written for "
                      "an area, not for the water it lands on: it ranks as an area (2), so a "
                      "named water's own row beats it (Duck Lake's 'Bass daily quota = 3' "
                      "replaces the CVWMA's 'unlimited' there; Denetiah Creek's own bull trout "
                      "rule beats the Liard watershed row). "
                      "`provenance.rank` is the rank where the rule is written; on a "
                      "section it reached by the tributary walk (`via: trib` in its ruleset) a "
                      "water rule speaks at the `inherited` rung instead. A rule is displaced "
                      "only by a better rule of ANOTHER quota: a quota and its `within` clauses "
                      "are one statement ('Trout/char: 5, but not more than 3 lake trout') and "
                      "never displace each other. Rules that tie all speak. A RULE THAT IS "
                      "ITSELF DISPLACED DISPLACES NOTHING (2026-09-29): only a rule that still "
                      "speaks takes another away. At Cheslatta Lake on Nov 15 Region 6's lake "
                      "trout release names the fish and would take the region's 'Trout/char: 5' "
                      "and '3 Dolly Varden/bull trout and/or lake trout combined' with it — but "
                      "the lake's own dated quota overrides that release, so the 5 and the 3 "
                      "speak beside the lake's 3, exactly as they do on Jul 1. A SIZE RULE "
                      "DISPLACES ONLY OVER THE SIZES IT SPEAKS ABOUT: 'Rainbow trout over 50 cm "
                      "catch and release' (Lakelse Lake) or 'no rainbow trout over 70 cm' "
                      "(Chilko Lake) says nothing about a smaller fish, so it never takes away a "
                      "quota that counts the smaller ones — Region 6's 'Trout/char: 5' and "
                      "Chilko Lake's own 'Trout/char daily quota = 2' still speak for rainbow. "
                      "Between two quotas "
                      "that let the fish be kept — one of this water, one of the zone — the "
                      "WATER's number replaces the zone's when both state the same thing, "
                      "larger or smaller and whatever naming says; otherwise both speak, and a "
                      "larger number for one fish is a lift (`quotas_sit_beside`). Where a water row's "
                      "group rule must still speak over a zone rule that names a fish (a "
                      "'Catch and release' row under the zone's 'Burbot: 5'), the row LIFTS the "
                      "zone rule (`exempts`) — it does not rely on the ladder.",
        "closures": "The domain rule, in its owner's words: 'Regional always overrides "
                    "provincial (except full closure), and this water overrides regional "
                    "always (except closures unless they are lifted in this water's regs).' A "
                    "closure (take 0, `may_target` false) is lifted by an `exempts`, never "
                    "displaced by a competing rule — whatever it names or wherever it is written. "
                    "In NAMING it counts as naming every fish it covers, so it displaces every "
                    "competing rule that does not beat it by authority or by naming the fish — "
                    "what ranks below it by place (Clearwater Lake's 'No Fishing Nov 1-Apr 30' "
                    "silences Zone B's 'Burbot: 5' on those dates) AND a water row's GROUP rule "
                    "that ranks ABOVE it by place: a zone's steelhead closure silences a water's "
                    "'Trout daily quota = 1' for a steelhead, because the closure names the fish "
                    "and the water's trout quota does not. A WATER ROW THAT NAMES THE "
                    "FISH a region-wide closure names lifts that closure for that fish (the "
                    "book's '(see tables for exceptions)'): Okanagan River's 'bass daily quota = "
                    "8' lifts Region 8's 'Bass: 0 quota, CLOSED TO FISHING' there. It must NAME "
                    "the same fish — 'Trout daily quota = 1' names no steelhead, so West Road's "
                    "row never reopens Region 6's steelhead stream closure — and a group closure "
                    "('No fishing in any stream') names no fish and is lifted only by a printed "
                    "exemption. These lifts are in the data (`exempts[].basis: names_the_fish`); "
                    "the ladder removes a closure only by a lift, printed or derived, and never "
                    "infers one. A DERIVED LIFT NEVER REOPENS MORE THAN ITS LIFTER COVERS: it "
                    "carries the lifter's dates (`when`), origin (`origin`) and kept sizes "
                    "(`lengths`), and holds only where the lifter is bound. And a closure that "
                    "PRINTS ITS OWN EXEMPTION LIST takes none: Region 6's 'No fishing: in all "
                    "rivers and streams for steelhead, May 15 – June 15. Exemptions include "
                    "mainstem portions of the Skeena, Nass, Iskut, Stikine and Taku Rivers' "
                    "(p.49) names its exceptions, so Kitimat River's 'Hatchery steelhead daily "
                    "quota = 2' does not reopen it; nor does a closure printed '(No "
                    "exceptions)'. Region 8's '(see tables for exceptions)' sends the reader to "
                    "the rows, which is what a derived lift reads — and ONLY such a closure "
                    "takes one (user ruling 2026-09-28): a closure whose table does not send the "
                    "reader to the tables is lifted only by a printed exemption.",
        "water_release": "A WATER'S RELEASE SILENCES THE ZONE FOR THAT FISH, whatever the zone "
                         "rule's conditions. Competition keys on (type, dimension), and a zone "
                         "quota's conditions (origin, water kind, while, size-only) are part of "
                         "its dimension, so on its own the ladder never set Coquihalla River's "
                         "'Trout/char (including steelhead) catch and release' against Region "
                         "2's '2 hatchery steelhead over 50 cm allowed'. So, after the ladder: "
                         "an OUTRIGHT RELEASE in force for the fish (a retention_limit with take "
                         "0 at every length — no `lengths`, no `while`, no `when_targeting`, not "
                         "a `within` clause; a closure is one) that is written for this water, "
                         "or reaches it by the tributary walk, displaces every zone, area or "
                         "provincial retention_limit that would let the angler keep that fish "
                         "(take above 0, unlimited, or sizes only), provided every origin that "
                         "quota could keep is released here — by the water row or by the zone's "
                         "own releases. Chilliwack River's 'hatchery cutthroat catch and "
                         "release' (May) beside Region 2's 'Wild trout/char from streams' "
                         "releases every cutthroat, so 'Trout/char: 4' does not speak for one; "
                         "Morris Lake's 'Wild trout/char catch and release' leaves Region 2's 4 "
                         "speaking for hatchery trout. A release of a size class ('No wild trout "
                         "over 50 cm') releases only that class and silences nothing. The "
                         "water's release counts even when the ladder put it behind a zone "
                         "release NAMING the fish (Pine River's 'Catch and release all fish' "
                         "under Zone B's bull trout release still silences Zone B's '2 from "
                         "streams' for a bull trout), and a zone rule that names the fish but "
                         "lets it be kept never beats the water's release (Adams River's "
                         "'Rainbow trout and char catch and release' speaks for a lake trout "
                         "over Region 3's 'none under 60 cm'): naming lets only a STRICTER zone "
                         "rule beat a water's group. A release put behind a superior authority "
                         "that lets the fish be kept releases nothing. A rule "
                         "about another fish never takes part (per fish). The zone's own "
                         "releases, record-keeping duties and the possession multiplier stand "
                         "beside the water's release.",
        "zone_release_by_water": "A ZONE RELEASE LIMITED TO A KIND OF WATER (2026-09-29). A "
                                 "zone release printed for streams (or lakes) only — Region 3's "
                                 "'Bull trout (Dolly Varden) from streams, Aug 1-Oct 31', Region "
                                 "4's 'Trout/char release: in streams from Nov 1-Mar 31', "
                                 "'Kokanee … (none from streams)' — in force on a section of that "
                                 "kind of water displaces its OWN REGION'S TABLE's rules that let "
                                 "the fish be kept (the quota and its clauses: '1 over 50 cm', '4 "
                                 "from streams', '1 bull trout or lake trout'), exactly as a "
                                 "release printed without a water kind does. A size clause of "
                                 "another dimension ('none under 60 cm') stays beside it; "
                                 "closures, water rows and another region's rules are untouched. "
                                 "On a lake a stream release does not bind and the region's "
                                 "quotas speak (read.released_on_water, effective_rules step 4b). "
                                 "A water row printing its OWN dates for the fish overrides such "
                                 "a release on the days both hold (`dated_zone_release` (A)), "
                                 "and the overridden release then displaces nothing.",
        "dated_zone_release": "A DATED ZONE RULE AND A WATER'S OWN DATES (user rulings "
                              "2026-09-28). A zone, area or provincial retention rule on "
                              "printed dates — a release (take 0) or a quota — meets a water's "
                              "rule for the same fish in one of two ways. "
                              "(A) THE WATER ROW PRINTS ITS OWN DATES FOR THE FISH: the water's "
                              "rule overrides the dated zone rule, ON THE DAYS BOTH HOLD ONLY. "
                              "Cheslatta and Murray lakes (Region 6) print 'Lake trout catch and "
                              "release, Sept 15-Oct 31' and 'Lake trout daily and possession "
                              "quotas = 3' (Nov 1-Sept 14); Region 6 prints 'you must release … "
                              "Lake trout from Fraser and Skeena Watersheds, Sept 15-Nov 30'. On "
                              "Nov 1-30 the lake's quota of 3 speaks and the region's release "
                              "does not; on Sept 15-Oct 31 the lake's own release speaks; outside "
                              "Sept 15-Nov 30 the region's release is not in force anyway. The "
                              "rules must be COMPATIBLE: the water's rule names the fish (or "
                              "states the same set of fish as the zone rule), states a number "
                              "for it (a quota or a release, not a size clause or a duty), holds "
                              "for every fish the zone rule does (no narrower origin, water kind, "
                              "means or target), and the zone rule is a release or quota — NEVER "
                              "A CLOSURE: a blanket spring closure still closes the water. A "
                              "water rule for one fish never overrides a zone rule for another "
                              "(competition is per fish). The same holds for a zone release "
                              "LIMITED TO A KIND OF WATER (user ruling 2026-09-29; "
                              "`zone_release_by_water`): Region 4's 'Trout/char release: in "
                              "streams from Nov 1-Mar 31' gives way, on the days both hold, to "
                              "Michel Creek's own 'Trout/char catch and release, June 15-Mar 31' "
                              "(and to the Kootenay above Koocanusa's and the Fording's), and "
                              "Region 5's 'Bull trout (Dolly Varden) from streams, Aug 1-Oct 31' "
                              "to the Atnarko/Bella Coola tributaries' 'char catch and release, "
                              "Sept 1-May 31'. An overridden release then silences nothing. "
                              "(B) THE WATER ROW PRINTS NO DATES OF ITS OWN: the dated zone "
                              "release or closure is stricter than the water's quota and no "
                              "direct override of it, so on its dates it keeps speaking — beside "
                              "the water's quota, and the release binds: Region 3's 'Lake trout "
                              "from Oct 15-Jan 31' (must release) still speaks on Shuswap Lake, "
                              "whose row prints 'Char daily quota = 1 (none under 60 cm)'. "
                              "Naming and place never let an undated water quota displace it. It "
                              "goes only by: (1) THE EXACT SAME STATEMENT — the water's rule is "
                              "about the same fish, sizes, origin, water kind, means and target, "
                              "ON THE SAME DATES; (2) a lift the water's row PRINTS ('exempt from "
                              "…'); (3) for a closure, a derived lift by naming the fish, only "
                              "where the closure sends the reader to the tables ('see tables for "
                              "exceptions' — Region 8's bass) (`closures`). A water's release "
                              "still silences a zone quota (`water_release`), and an undated "
                              "zone release a water row NAMES the fish over still gives way to "
                              "the row (`who_speaks`).",
        "quotas_sit_beside": "A WATER'S QUOTA AND THE ZONE'S (user rulings 2026-09-26). This is "
                             "between two quotas that let a fish be kept — one written for this "
                             "water (or reaching it by the tributary walk), one of the zone, an "
                             "area or the province; releases and closures follow `closures` and "
                             "`water_release`. Three cases, and nothing else: "
                             "(1) THE SAME STATEMENT — the same fish or group, size bounds, origin, "
                             "water kind, means (`while`), target (`when_targeting`) and clock, "
                             "differing only in the number: the WATER's number replaces the "
                             "zone's, whether it is larger or smaller. It is never the smaller of "
                             "the two. Tranquille Lake's 'kokanee daily quota = 10' replaces Region "
                             "3's 'Kokanee: 5' (10 may be kept); Teslin Lake's 'Arctic grayling "
                             "daily and possession quotas = 4' replaces Region 6's 'Arctic "
                             "grayling: 3'. The water always overrides the same statement. "
                             "(2) A LARGER NUMBER FOR A FISH than the zone gives it: the water's "
                             "number REPLACES the zone's for that fish, and the data says so with a "
                             "lift (`exempts`, `species`), never by comparing numbers. Kootenay "
                             "Lake (main body) prints 'rainbow trout daily quota = 10 (any size)': "
                             "it lifts Region 4's 'Trout/char: 5' and its '1 rainbow trout or "
                             "cutthroat trout over 50 cm' FOR RAINBOW, so a rainbow there answers "
                             "to the 10 alone — and is counted apart from the 5, which still holds "
                             "every other trout and char (`counted_apart`). Perry Creek's 'brook "
                             "trout daily quota = 20' lifts the 5 for brook trout; Lois Lake's "
                             "'Rainbow trout/hatchery steelhead quota = 6 in the aggregate' lifts "
                             "Region 2's 'Trout/char: 4' for rainbow and steelhead (the region's "
                             "'2 hatchery steelhead' binds streams only and never reaches the "
                             "lake; its 'All wild steelhead' release does, and so does the "
                             "province's whole steelhead set — `steelhead_definition`). A LARGER "
                             "NUMBER ALSO "
                             "OVERRIDES THE ZONE'S "
                             "SIZE CLAUSE ('only 1 over 50 cm'), whether or not the row prints "
                             "'(any size)' (user ruling 2026-09-26): Jewel Lake's 'Brook trout "
                             "daily quota = 20' lifts Region 8's 5 AND its '1 over 50 cm' for "
                             "brook trout. Only where the row prints '(any size)' is that hard "
                             "to read (user ruling 2026-09-28) — no size limit at all, or only "
                             "no minimum? — so only such a lift carries a `caution` "
                             "(`exempts[].caution`, kind size_clause_override) the page must show "
                             "beside it (`gotchas`). 'Trout' includes char unless char are "
                             "excluded (p.86): Polley Lake's 'Trout daily quota = 8' is a "
                             "trout/char quota, the same statement as Region 5's 'Trout/char: 5', "
                             "and replaces it by case (1) — 8 trout and char together; it lifts "
                             "the region's '1 over 50 cm' (no caution: it prints no '(any "
                             "size)') and the region's "
                             "other clauses ('1 Dolly Varden/bull trout', '2 lake trout') still "
                             "hold. Where the water's number is larger "
                             "only than one CLAUSE of the zone's quota, only that clause is "
                             "lifted and the fish still counts toward the aggregate: Williston "
                             "Lake's 'Lake trout daily quota = 3' lifts Zone B's '2 lake trout'; "
                             "Duncan River's 'Rainbow trout daily quota = 5 (any size)' lifts "
                             "Region 4's '2 from streams' and '1 … over 50 cm' for rainbow, and the "
                             "5 trout/char still binds; a lake printing its own lake trout sizes "
                             "that keep more than one over 50 cm (Atlin, Bennett, Gwillim) lifts "
                             "the zone's '1 over 50 cm' for lake trout. A lift for one ORIGIN "
                             "(Kitimat's hatchery rainbow) carries `origin`: the zone's rule "
                             "stays for wild fish, partly lifted. "
                             "(3) DIFFERENT STATEMENTS SIT BESIDE EACH OTHER and both speak: the "
                             "zone's quota is a day's total over every water of the region, and "
                             "the water's says how many of it may come from here. The Dean River's "
                             "'Trout/char daily quota = 1 (none under 35 cm)' beside Region 5's "
                             "'Trout/char: 5' (1 a day from the Dean, counting toward the 5); "
                             "Kitimat River's 'Hatchery steelhead … daily quota = 2' beside Region "
                             "6's 'Trout/char: 5'; Dodd Lake's 'Wild trout/char daily quota = 2' "
                             "beside Region 2's 'Trout/char: 4'. Show both; each binds. A size "
                             "clause the water does not restate is a different statement: a "
                             "lake's 'Rainbow trout daily quota = 2' leaves Region 3's '1 over 50 "
                             "cm' beside it. So the only way a zone quota stops speaking for a "
                             "fish at a water is the same statement, a lift, a release "
                             "(`water_release`) or a closure — never a comparison of numbers the "
                             "reader makes.",
        "counted_apart": "A QUOTA COUNTED APART FROM THE AGGREGATE. The zone's 'Trout/char: 5' "
                         "counts every trout and char together; a fish the book gives its own, "
                         "larger number leaves that count. The data says so with a lift: the rule "
                         "with the larger number lifts the aggregate AND its clauses FOR THAT "
                         "FISH (`exempts[].species`), where it binds. Region 8 prints it in its "
                         "own table — 'Trout/char: 5, but not more than … 4 from streams … And "
                         "you may retain: 20 brook trout from streams' (p.68): on a stream a "
                         "brook trout answers to its 20 only, and on a lake (where the 20 does "
                         "not bind) to the trout/char 5. A water row does it the same way "
                         "(`quotas_sit_beside`, case 2): Kootenay Lake's rainbow 10, the brook "
                         "trout lakes and creeks of Regions 2, 4, 7A and 8 (5 to 20), Tranquille "
                         "Lake's rainbow 8, and Lois and Khartoum lakes' rainbow/hatchery "
                         "steelhead 6 in the aggregate. The larger number lifts the zone's size "
                         "clause ('1 over 50 cm') for that fish too — printed '(any size)' or "
                         "not; where the row prints '(any size)' the lift carries `caution` "
                         "(size_clause_override): show it (`gotchas.size_clause_override`). "
                         "Every other fish still counts toward the aggregate.",
        "steelhead_definition": "WHERE A RAINBOW OVER 50 CM IS A STEELHEAD. The book defines "
                                "(p.86): 'steelhead: a rainbow trout longer than 50 cm in waters "
                                "where anadromous rainbow trout are found.' It lists no such "
                                "waters, so a row states it (`CatalogueEntry.anadromous_rainbow`: "
                                "the stream rows whose own line names steelhead, and "
                                "Chilliwack/Vedder by ruling), and it holds on those streams AND "
                                "THEIR TRIBUTARY STREAMS — the parts whose `steelhead` is "
                                "\"known\" and that carry `anadromous_rainbow: true` (user ruling "
                                "2026-10-01). There a rainbow over 50 cm "
                                "IS a steelhead: ask about it as ST, and it is governed by the "
                                "steelhead rules. A rainbow rule there speaks only for rainbow "
                                "of 50 cm or less: its length bands over 50 cm speak for no "
                                "rainbow, and one that speaks only of rainbow over 50 cm "
                                "(Region 2's '1 over 50 cm') speaks for no rainbow at all. So "
                                "Chilliwack's 'hatchery rainbow trout catch and release (50 cm "
                                "or less)' releases every hatchery rainbow in May, and Region "
                                "2's 'Trout/char: 4' keeps no rainbow over 50 cm there — that "
                                "fish is a steelhead ('2 hatchery steelhead over 50 cm allowed', "
                                "'All wild steelhead' released). Elsewhere — a \"possible\" "
                                "stream, any lake — a rainbow of any size is a rainbow. "
                                "STEELHEAD RULES BIND STREAMS (user rulings 2026-10-01): every "
                                "provincial and zone steelhead rule (the wild release, the annual "
                                "hatchery quota of 10, the record duty, the Conservation "
                                "Surcharge Stamp, each zone's hatchery quota, release line and "
                                "'stop fishing after the hatchery quota') binds only the STREAMS "
                                "of the regions whose own tables name steelhead (1, 2, 3, 5 and "
                                "6), each zone's of its own area. A LAKE carries a steelhead rule "
                                "only when its own row names steelhead — Khartoum and Lois lakes "
                                "('Rainbow trout/hatchery steelhead quota = 6 in the aggregate') "
                                "— and then it carries the WHOLE PROVINCIAL STEELHEAD SET, as a "
                                "steelhead stream does (user ask 2026-10-02): the annual hatchery "
                                "10, 'All wild steelhead must be released', the record duty and "
                                "the Conservation Surcharge Stamp (the lake copies "
                                "`zp:steelhead::steelhead.r1b`, `.r2b`, `.r4b` and "
                                "`zp:steelhead#steelhead_targeting_lakes`), and Region 2's 'All "
                                "wild steelhead' release (`trout_char_quota.r7b`); not the zone's "
                                "'2 hatchery steelhead', which binds streams. Every water whose "
                                "own row names steelhead carries that set wherever the row's "
                                "steelhead lines bind in Regions 1, 2, 3, 5 and 6. A big lake "
                                "rainbow elsewhere falls under the rainbow size quota ('1 over 50 "
                                "cm'), never a steelhead rule. The export refuses a steelhead "
                                "rule on any other lake, and a steelhead row's water — stream or "
                                "own-row lake — missing any of the set. HOW SURE WE ARE THAT "
                                "STEELHEAD ARE PRESENT on a water is its `steelhead` (\"known\" "
                                "| \"possible\" | absent): see `field_dictionary` "
                                "`water.parts[].steelhead`.",
        "region": "A WATER TAKES THE ZONE RULES OF THE REGION IT LIES IN (user rulings "
                  "2026-09-25). A STREAM PIECE that wanders across a region line binds only "
                  "its home region's zone rules: the region holding most of its length (a West "
                  "Road River mainstem piece 54% in Zone 7A takes Zone 7A's table). A LAKE drawn "
                  "across a region line (never cut) binds BOTH regions' zone rules, and the MOST "
                  "STRICT applies (`two_regions`): Ahbau Lake (51% Region 5, 49% Zone 7A), Mara "
                  "Lake (61% Region 3, 39% Region 8). A WATER'S OWN ROW applies along the water's "
                  "whole length, in every region it lies in — West Road River's Region 5 row on "
                  "its Zone 7A and Region 6 pieces, the Nechako's Region 7 row on its Region 6 "
                  "pieces, the Similkameen's Region 8 row on its Region 2 pieces — and so do the "
                  "exemptions it prints: a row 'exempt from spring closure' lifts the spring "
                  "closure of whichever region each piece lies in (`exempts[].equivalent`). The "
                  "exception is a PER-REGION row: a water the book prints a row for in more than "
                  "one region (the Fraser in Regions 2, 3, 5 and 7; the Stellako in 6 and 7; the "
                  "West Road's tributaries in 6 and 7) — each such row is about the stretch in its "
                  "own region. A pointer row ('See Shuswap Lake in Region 3') never moves a water "
                  "to another region.",
        "two_regions": "TWO REGIONS' BASES ON ONE LAKE — THE MOST STRICT APPLIES. Where a lake "
                       "carries two regions' zone rules (`region`), neither table outranks the "
                       "other by place. Per fish, a zone rule of one region gives way to a "
                       "STRICTER zone rule of the other: a closure beats any retention rule "
                       "that is not a closure (closed beats open); an outright release beats a "
                       "quota keeping only fish it releases; of two quotas stating exactly the "
                       "same thing the LOWER number beats the higher (between two REGIONS' "
                       "tables only — a water's own number always replaces its zone's same "
                       "statement, `quotas_sit_beside`). "
                       "Quotas stating different things sit beside each other (the stricter "
                       "binds by itself), equal rules both speak, and gear and method "
                       "restrictions are never displaced — both regions' apply. A rule written "
                       "for the lake itself still outranks both tables.",
        "never_compete": "`standing` rules, the information family (hazard, advisory, "
                         "program_membership, facility), LIFT-ONLY rules (dimension `lift`: "
                         "an `exempts` and no number, bound, gear, duty or angler of their own — "
                         "'Exempt from spring closure'), and rules in a part NOBODY HAS DRAWN "
                         "(`not_yet_mapped`). A lift-only rule only removes what it lifts; it "
                         "never displaces a rule that shares its type.",
        "not_yet_mapped": "AN UNDRAWN-PART RULE NEVER GOVERNS THE WHOLE WATER (user ruling "
                          "2026-09-26). A rule holding only in a part of its water that nothing "
                          "draws (`fields.undrawn_part`; the record's `not_yet_mapped`) is placed "
                          "on the whole water so it can be seen there, and the reference answers "
                          "it with state `not_yet_mapped`: it never competes, never displaces, "
                          "never lifts, never silences a zone rule and never holds a record "
                          "dormant. Kinbasket Lake's 'No Fishing within 200 m of Bush-Sullivan "
                          "Bridge' does not close Kinbasket Lake, and it does not silence Zone "
                          "B's or Region 4's quotas there: they speak, and the closure is shown "
                          "at the top of the lake as a place not yet mapped.",
        "ranks": ranks,
        "dimension_by_type": {
            "retention_limit": "the period, plus '/size' when the rule is sizes with no count of "
                               "its own, plus '@' and the conditions it holds under — origin, "
                               "water kind, while (the means) and record (a record-keeping duty): "
                               "'daily@origin=wild' (release all wild steelhead) never shares a "
                               "key with a region's 'Trout/char: 5' ('daily'), so the number "
                               "cannot silence the release",
            "vessel_rule": "the aspect",
            "angler_closure": "closed_to:<who>, plus -except:<who> for each `closed_to_except`",
            "method_rule": "the methods it names, each with its clause's condition: 'no angling "
                           "from boats' is method:angling@angler=in_boat, so it never displaces "
                           "the province's unconditional angling allow (method:angling)",
            "tackle_restriction": "the set of slots it constrains",
            "bait_restriction": "ONE DOMAIN, ranked by where it applies: a permission (allow / "
                                "only) and a TOTAL ban (any_bait) share the key 'bait', so a "
                                "region's or water's bait ban speaks over the province's "
                                "'invertebrates may be used in streams unless a bait ban "
                                "applies'; a PARTIAL ban keeps the bait it names "
                                "('bait:fin_fish', 'bait:live_fin_fish', 'bait:invertebrate') so "
                                "it never displaces a ban on other bait. Plus '/<targeted "
                                "species>' and '@while=<means>' when the rule has them. The roe "
                                "possession cap is its own key, 'bait_possession:roe'",
            "every other type": "the type itself",
            "any type, lift-only": "'lift' — never competes (see never_compete)",
        },
        "examples": (
            pick(lambda x: x["type"] == "retention_limit" and (_f(x).get("take") or 0) > 0
                 and not _f(x).get("lengths") and x["provenance"]["binds_to"] == "water",
                 "species", "take", "period", n=1)
            + pick(lambda x: x["type"] == "retention_limit" and (_f(x).get("take") or 0) > 0
                   and not _f(x).get("lengths") and x["provenance"]["binds_to"] == "region",
                   "species", "take", "period", n=1)),
    }

    # ---- gear ---------------------------------------------------------------------------
    slot_use = defaultdict(list)
    members = defaultdict(set)
    for x in rules.values():
        for c in _gear(x):
            slot_use[c["slot"]].append(x["id"])
            for k in ("allow", "only", "ban", "except", "of", "members", "must_be"):
                members[c["slot"]].update(c.get(k) or [])
    slots = {}
    for s in C.Slot:
        kind = _slot_kind(s)
        slots[s.value] = {
            "kind": kind,
            "bound": {"set": "exactly one of allow / only / ban (never empty); of, except",
                      "counted": "max / min (whole numbers) or unlimited; members",
                      "measured": "max / min in the unit in the slot's name",
                      "spec": "must_be and/or requires"}[kind],
            "means": SLOT_TEXT.get(s.value),
            **({"definitions": FLY_DEFINITIONS[s.value]} if s.value in FLY_DEFINITIONS else {}),
            "rules": len(set(slot_use[s.value])),
            "members_seen": sorted(members[s.value]),
            "examples": pick(lambda x, s=s.value: any(c["slot"] == s for c in _gear(x)),
                             "gear", "while", "when_targeting"),
        }
    acts = {}
    for a, words in C.CONDUCT_ACTS.items():
        acts[a] = {"means": words,
                   "rules": sum(a in (_f(x).get("conduct") or []) for x in rules.values()),
                   "licensing": sum(a in (x["fields"].get("conduct") or [])
                                    for x in lic.values()),
                   "examples": (pick(lambda x, a=a: a in (_f(x).get("conduct") or []),
                                     "conduct", n=1)
                                + lpick(lambda x, a=a: a in (x["fields"].get("conduct") or []),
                                        "conduct", n=1))}
    methods_page = sorted((d["entries"].get("zp:allowable_methods") or {}).get("pages") or [])
    allowed = sorted({m for x in rules.values() if x["entry_id"].startswith("zp:")
                      for c in _gear(x) if c.get("slot") == "method"
                      for m in (c.get("allow") or []) if not _f(x).get("while")})
    same_slot = lambda x: len([c["slot"] for c in _gear(x)]) > len({c["slot"] for c in _gear(x)})
    gear = {
        "reading": "`gear` is an ORDERED list of clauses. Clauses on DIFFERENT slots are "
                   "independent and all apply. Clauses on the SAME slot are ordered and the "
                   "FIRST whose `when` matches wins; a clause with no `when` is the last word "
                   "on its slot. Nothing in a clause is a polarity flag: the direction is in "
                   "the key (allow / only / ban).",
        "slot_kinds": {
            "set": "chosen from a set of members",
            "counted": "a whole number of things",
            "measured": "a quantity in the unit its name carries",
            "spec": "how the thing must be built or carried; presence asserts, there is no "
                    "negation",
        },
        "slots": slots,
        "clause_fields": {k: CLAUSE_TEXT.get(k) for k in _fields(C.GearClause)},
        "when_fields": {k: GEAR_WHEN_TEXT.get(k) for k in _fields(C.GearWhen)},
        "requires_fields": {k: GEAR_SPEC_TEXT.get(k) for k in _fields(C.GearSpec)},
        "while": {
            "means": "The rule binds only WHILE the angler is doing one of these. A `take: 0` "
                     "on every game fish `while: [spear_fishing]` says which fish you may "
                     "spear; it does not close the water. Absent = whatever you are doing.",
            "means_of_fishing": {
                "tokens": sorted(C.WHILE_MEANS),
                "reading": "A WAY OF FISHING. The rule binds while you fish this way.",
            },
            "devices": {
                "tokens": sorted(C.WHILE_DEVICES),
                "reading": "A DEVICE, used while angling. The rule binds while you use it. It is "
                           "NOT a way of fishing, and nothing grants or bans it as one: a "
                           "downrigger or a light never appears in a `method` slot.",
            },
            "examples": pick(lambda x: bool(_f(x).get("while")), "while", "species", "take",
                             "gear"),
        },
        "methods": {
            "reading": "The ways you may sport fish are those the PROVINCE allows — an `allow` "
                       "on the `method` slot in a `zp:` rule with no `while` — narrowed here by "
                       "`only` and `ban`. A way of fishing no rule allows is NOT a lawful way to "
                       "sport fish anywhere: render an unmentioned method as not permitted, "
                       "never as 'no rule bans it here'. 'Sport fishing' is defined as angling, "
                       "spear fishing, set lining and crayfish trapping.",
            "book": {"text": "Your basic fishing licence entitles you to: angle …; angle with a "
                             "downrigger …; ice fish …; fish with a set line …; fish with a "
                             "spear or an arrow …; trap crayfish …. All other methods of taking "
                             "fin fish and crayfish are illegal.",
                     "where": "the synopsis's 'Allowable Fishing Methods' list",
                     "pages": methods_page},
            "allowed_by_the_province": allowed,
            "examples": pick(lambda x: x["entry_id"].startswith("zp:") and any(
                c.get("slot") == "method" and c.get("allow") for c in _gear(x)), "gear", n=3),
        },
        "conduct": {
            "means": "Acts you must do or must not do, as tokens named in the lawful direction "
                     "(`do_not_waste_catch`). The wording below is the model's own.",
            "acts": acts,
        },
        "first_match_per_slot": {
            "means": "Two clauses on one slot: the narrow one first, the general one last.",
            "examples": pick(same_slot, "gear"),
        },
        "examples_by_bound": {
            b: pick(lambda x, b=b: any(b in c for c in _gear(x)), "gear", n=1)
            for b in ("allow", "only", "ban", "except", "of", "members", "max", "min",
                      "unlimited", "when", "unless", "requires", "must_be")},
    }

    # ---- sizes, time, species, retention ------------------------------------------------
    L = lambda x: _f(x).get("lengths") or []
    sizes = {
        "reading": "`lengths` is an ORDERED list of length ranges. For a fish of a given "
                   "length the FIRST range that contains it answers; its `take` (or, absent, "
                   "the rule's `take`) is how many of those you may keep. A length no range "
                   "covers is not spoken about by this rule: at the top level nothing grants "
                   "it; inside a `within` clause the parent quota governs it. A grant is "
                   "written before the denial beneath it, so a shared endpoint is granted.",
        "range_fields": LENGTH_TEXT,
        "examples": {
            "quota with a ceiling": pick(lambda x: _f(x).get("take") and any(
                b.get("take") == 0 and b.get("min_cm") is not None for b in L(x)),
                "species", "take", "lengths", n=1),
            "floor": pick(lambda x: any(b.get("take") == 0 and b.get("max_cm") is not None
                                        and b.get("min_cm") is None for b in L(x)),
                          "species", "take", "lengths", n=1),
            "window (keep only between)": pick(lambda x: any(
                _closed_range(b) and b.get("take") != 0 for b in L(x)),
                "species", "take", "lengths", n=1),
            "hole (keep none between)": pick(lambda x: any(
                _closed_range(b) and b.get("take") == 0 for b in L(x)),
                "species", "take", "lengths", n=1),
            "a clause counting one size class": pick(lambda x: _f(x).get("within") and L(x)
                                                     and (_f(x).get("take") or 0) > 0,
                                                     "species", "take", "lengths", "within",
                                                     n=1),
        },
    }
    W = lambda x: _f(x).get("when") or {}
    wraps = lambda x: any((r["to_month"], r["to_day"]) < (r["from_month"], r["from_day"])
                          for r in W(x).get("dates") or [])
    time = {
        "reading": "`when` says when a rule binds. Absent means ALL YEAR, as the synopsis "
                   "says: 'When no date is listed, the regulations apply all year. Start and end "
                   "dates are inclusive.' There is no 'except' form: a rule printed as 'open "
                   "except …' stores the days it DOES hold.",
        "fields": {k: WHEN_TEXT.get(k) for k in _fields(C.When)},
        "hours_and_weekdays": "Apply them on the angler's own clock and day; they never "
                              "resolve to a date.",
        "unparsed": "Uncertain, never all year.",
        "suspended_while": "On a rule or a designation: dormant while the named closure rule in "
                           "the same entry binds ('not required until reopened').",
        "examples": {
            "dates": pick(lambda x: bool(W(x).get("dates")) and not wraps(x), "when", n=1),
            "wrapping the year end": pick(wraps, "when", n=1),
            "hours": pick(lambda x: bool(W(x).get("hours")), "when", n=1),
            "weekdays": pick(lambda x: bool(W(x).get("weekdays")), "when", n=1),
            "unparsed": pick(lambda x: bool(W(x).get("unparsed")), "when", n=1),
            "suspended_while": (pick(lambda x: bool(_f(x).get("suspended_while")),
                                     "suspended_while", n=1)
                                + lpick(lambda x: bool(x["fields"].get("suspended_while")),
                                        "suspended_while", n=1)),
        },
        "counts": {"rules_with_when": sum(bool(W(x)) for x in rules.values()),
                   "rules_all_year": sum(not W(x) for x in rules.values()),
                   "rules_with_unparsed": sum(bool(W(x).get("unparsed")) for x in rules.values())},
    }
    species = {
        "reading": "The fish are THE BOOK'S LIST and nothing else (p.86, 'Freshwater game fish "
                   "are defined as follows'): `species.fish`, under the book's own headings "
                   "(`species.families`). `species` on a record holds the codes the book wrote, "
                   "groups included. Expand a group with `species.groups[code].members` (flat). "
                   "`species_except` carves codes out after expansion. The OPEN subjects "
                   "(`ALL_FIN_FISH`, `PROTECTED_SPECIES`, `SALMON`) are words the book prints "
                   "for fish that are not game fish, and have no member list on purpose — an "
                   "empty list is NOT 'no fish': `ALL_FIN_FISH` is every fish (never crayfish), "
                   "and the other two hold no fish you can ask about.",
        "trout_includes_char": "'Trout' includes char unless char are specifically excluded "
                               "(p.86: 'all regulations that apply to trout (as a group) also "
                               "apply to char unless char are specifically excluded'). A row "
                               "or zone table excludes them by MENTIONING CHAR APART (user "
                               "ruling 2026-09-28): where it names a char on its own ('char', "
                               "Dolly Varden/bull trout, lake trout, brook trout — 'trout/char' "
                               "names char in and does not count), its bare 'trout' lines are "
                               "`TROUT_CHAR` with species_except `CHAR` — trout only (Region 6's "
                               "'Trout under 30 cm from any stream', Region 1's 'Trout: 4'). "
                               "Otherwise 'Trout daily quota = 2' is `TROUT_CHAR` and counts "
                               "char too. There is no trout-only code. For the same-statement "
                               "test (`ladder.quotas_sit_beside`) the printed word decides: a "
                               "lake's 'Trout daily quota = 2' and Region 1's 'Trout: 4' are the "
                               "same statement, so the lake's 2 replaces the 4 for trout. "
                               "'Char' (`CHAR`) is how the book "
                               "names char apart from trout, so a rule about char NAMES each "
                               "char (`ladder.who_speaks`): Region 1's 'you must release: All "
                               "char (includes Dolly Varden)' still releases a char on a lake "
                               "printing 'Trout daily quota = 2'.",
        "bull_trout_is_dolly_varden": "A bull trout IS a Dolly Varden in the regulations: '*Any "
                                      "bull trout that you catch and keep must be counted as "
                                      "part of your Dolly Varden quota' (p.86). One code, `DV`, "
                                      "named 'Dolly Varden/bull trout'; a rule printed about "
                                      "bull trout is about it, and its verbatim keeps the "
                                      "book's word.",
        "several_fish_one_number": "A number on a rule that names more than one fish is SHARED "
                                   "between them: 'Trout/char: 5' is five in total.",
        "examples": {
            "a group": pick(lambda x: any(c in C.SPECIES_GROUPS for c in
                                          _f(x).get("species") or []) and
                            (_f(x).get("take") or 0) > 0 and not L(x),
                            "species", "take", n=1),
            "species_except": pick(lambda x: bool(_f(x).get("species_except")),
                                   "species", "species_except", "take", n=1),
            "an open subject": pick(lambda x: any(c in C.OPEN_SUBJECTS
                                                  for c in _f(x).get("species") or []),
                                  "species", "take", "may_target", n=1),
        },
    }
    rl = lambda x: x["type"] == "retention_limit"
    F = lambda x, k: _f(x).get(k)
    retention = {
        "fields": {k: RULE_FIELD_TEXT.get(k) for k in ("take", "may_target", "unlimited", "period",
                                                   "per_daily", "within", "lengths",
                                                   "record_retention", "origin")},
        "three_readings_of_take_0": {
            "closed": "take 0 AND may_target false — you may not fish for it at all",
            "release": "take 0, may_target true, no `lengths` — fish for it, release every one",
            "size gate": "a range in `lengths` with take 0 — those sizes go back; not a closure. "
                         "Where `lengths` is present the ranges are the answer; the top-level take "
                         "only fills a range with no take of its own. 'No trout over 50 cm' is "
                         "`take: 0` with one range "
                         "{min_cm: 50, take: 0}, and says nothing about a trout under 50 cm",
            "and": "A take 0 carrying `while` closes that way of fishing, not the water; a "
                   "`standing` one is shown and decides nothing.",
        },
        "examples": {
            "closed": pick(lambda x: rl(x) and F(x, "take") == 0 and F(x, "may_target") is False
                           and not F(x, "while") and not F(x, "standing"),
                           "species", "take", "may_target", "when", n=1),
            "release": pick(lambda x: rl(x) and F(x, "take") == 0 and F(x, "may_target")
                            and not L(x), "species", "take", "may_target", n=1),
            "size gate": pick(lambda x: rl(x) and F(x, "take") is None and
                              any(b.get("take") == 0 for b in L(x)),
                              "species", "lengths", n=1),
            "possession": pick(lambda x: rl(x) and F(x, "per_daily"),
                               "species", "period", "per_daily", n=1),
            "annual": pick(lambda x: rl(x) and F(x, "period") == "annual",
                           "species", "take", "period", n=1),
            "unlimited": pick(lambda x: rl(x) and F(x, "unlimited"), "species", "unlimited",
                              n=1),
            "within": pick(lambda x: rl(x) and F(x, "within"), "species", "take", "within",
                           n=1),
            "record_retention": pick(lambda x: F(x, "record_retention"),
                                     "species", "record_retention", n=1),
        },
        "record_duty": {
            "reading": "An annual quota's duty to record what you keep is PRINTED, as its own "
                       "`record_retention` rule. The quota carries `recorded_by` (that rule's "
                       "id) and the record rule carries `records_for` (the quotas). NEVER "
                       "generate a 'record each one you keep on your licence' line for an annual "
                       "quota: show the linked record rule's text ONCE, under the quota, and do "
                       "not show it a second time as a rule of its own there. An annual quota "
                       "with no `recorded_by` gets nothing extra.",
            "link": "Derived, never authored: the record rule names the same fish (groups "
                    "expanded), the same origin or none, the same dates or none, and is in "
                    "force in every rule set that holds the quota. Exactly one such rule links.",
            "pairs": [{"quota": q, "record": x["recorded_by"], "quota_says": x["verbatim"],
                       "record_says": rules[x["recorded_by"]]["verbatim"]}
                      for q, x in sorted(rules.items()) if x.get("recorded_by")],
            "without_record": sorted(q for q, x in rules.items()
                                     if F(x, "period") == "annual" and not x.get("recorded_by")),
        },
    }
    vessel = {
        "fields": {k: RULE_FIELD_TEXT.get(k) for k in ("aspect", "level", "max_power_kw",
                                                   "max_kmh")},
        "levels_strictest_first": _enum(C.PropulsionLevel),
        "aspects": _enum(C.VesselAspect),
        "reading": "Read `aspect` first. Propulsion `level` is one ordered scale, strictest "
                   "first; `power_capped` carries `max_power_kw`. A speed rule carries `max_kmh`.",
        "examples": {a: pick(lambda x, a=a: F(x, "aspect") == a, "aspect", "level",
                             "max_power_kw", "max_kmh", "when", n=1)
                     for a in _enum(C.VesselAspect)},
    }
    def part(e):
        return any(k in e for k in ("species", "when_targeting", "while", "when"))
    exempts = {
        "reading": "A rule with `exempts` LIFTS the rules it names, where and while it binds. "
                   "Every item is RESOLVED to one rule, `entry_id::rule_id`: match on those "
                   "exact ids, never on a bare name. An item with no `species`, "
                   "`when_targeting`, `while` or `when` lifts that rule outright. One with any "
                   "of them lifts it only for those anglers, or only on those days — the rule stays in force for everyone "
                   "else, and since the angler is unknown the lift is a condition beside it, "
                   "never a removal. A lift is never wider than its lifter. A lift whose place "
                   "cannot be drawn is not applied, and a rule never lifts itself. Some lifts "
                   "are not printed as exemptions but DERIVED at build (`basis`): a water row "
                   "naming a fish its region closes lifts that closure for that fish (see "
                   "`ladder.closures`). Read them exactly like printed ones.",
        "fields": dict(LIFT_TEXT),
        "examples": {
            "whole": pick(lambda x: any(not part(e) for e in F(x, "exempts") or []),
                          "exempts", n=1),
            "in part": pick(lambda x: any(part(e) for e in F(x, "exempts") or []),
                            "exempts", "species", "when_targeting", "while", n=1),
            "with the book's note": pick(
                lambda x: any(e.get("note") for e in F(x, "exempts") or []), "exempts", n=1),
            "derived — the water names the fish": pick(
                lambda x: any(e.get("basis") for e in F(x, "exempts") or []), "exempts", n=1),
        },
        "rules": sum(bool(F(x, "exempts")) for x in rules.values()),
        "derived_lifts": sum(1 for x in rules.values() for e in F(x, "exempts") or []
                             if e.get("basis")),
    }
    standing = {
        "reading": "`standing: true` — the rule holds everywhere at places no dataset can draw "
                   "('within 23 m downstream of any fishway'). It is bound to every section so "
                   "it is always shown; it never decides a water's outcome and never competes.",
        "rules": [x["id"] for x in rules.values() if F(x, "standing")],
        "examples": pick(lambda x: bool(F(x, "standing")), "species", "take", "may_target"),
    }
    angler_closure = {
        "reading": "The water is closed to the anglers in `closed_to` (a `Who`) on the days in "
                   "`when`, except the anglers in any `closed_to_except` (each a `Who`). It never "
                   "competes with a quota. Because the angler is unknown, the answer is "
                   "conditional: 'if you are a non-guided non-resident alien, you may not angle "
                   "here on Saturdays'; on a Youth/Disabled Accompanied Water, 'if you are 16 or "
                   "over you may angle here only as a disabled B.C. resident or as the companion "
                   "of an authorized angler'.",
        "rules": [x["id"] for x in rules.values() if x["type"] == "angler_closure"],
        "examples": pick(lambda x: x["type"] == "angler_closure", "closed_to", "when"),
    }

    # ---- licensing ----------------------------------------------------------------------
    kinds = Counter(x["kind"] for x in lic.values())
    lmodels = _licensing_models()
    lkinds = {}
    for k, m in lmodels.items():
        lkinds[k] = {"means": LICENSING_KIND_TEXT.get(k),
                     "placed": next(p for t, _, p in _LIC_TABLES if t == k),
                     "records": kinds.get(k, 0),
                     "fields": [f for f in _fields(m) if f not in ("kind", "id", "verbatim")],
                     "examples": lpick(lambda x, k=k: x["kind"] == k, n=2)}
    has_path = lambda x, key: any(key in p for p in x["fields"].get("satisfied_by") or [])
    licensing = {
        "rules_of_reading": [
            "Licensing NEVER affects whether a water is open, closed or restricted. It is "
            "consulted only once the water is open for what the angler is doing, so a closed "
            "water needs no licence by construction.",
            "The angler is ALWAYS unknown. Every answer is conditional on who they are ('if "
            "you are a non-resident …'); there is no default angler.",
            "A requirement is met by ANY ONE of its paths; a `hold` path needs ALL of its "
            "documents.",
            "An `exemption` removes documents for the anglers it names, AND every duty whose "
            "`presumes` are all among those documents. The angler is unknown, so render such a "
            "duty conditionally ('unless you are an Indian resident of B.C.'), never drop it. An "
            "`alternative` only ever adds a path.",
            "A record with `restates` is the row's own words for a record stated elsewhere (a "
            "provincial one); read its `who` and `satisfied_by` as that record's — the Dean's "
            "'All anglers are required to buy a Classified Waters Licence' is 16 and over because "
            "the provincial requirement it restates is.",
            "A designation obliges nothing on its own; requirements with `on` fire where it is "
            "in force, and the classified-water steelhead stamp runs during "
            "`steelhead_stamp_during`, unless `steelhead_stamp_waived`.",
            "A designation's classified period is its `period`, never inferred from a missing "
            "`when`: `when_open` (Classified whenever the water is open), `all_year`, or "
            "`dates`. Show `period.says`.",
            "An unresolved record renders as 'check', never as 'none needed'.",
        ],
        "kinds": lkinds,
        "who": {
            "reading": "A set on every axis: the members listed are IN. An axis left out means "
                       "ANY member. `who` absent means every angler.",
            "axes": {a: {"members": list(v), "means": WHO_TEXT.get(a)} for a, v in C.WHO_AXES.items()},
            "examples": lpick(lambda x: bool(x["fields"].get("who")), "who", "who_except"),
        },
        "doing": {
            "acts": {a: DOING_TEXT.get(a) for a in typing.get_args(
                C.Doing.model_fields["act"].annotation)},
            "fields": _fields(C.Doing),
            "examples": [lpick(lambda x, a=a: (x["fields"].get("doing") or {}).get("act") == a,
                               "doing", "who", n=1)
                         for a in typing.get_args(C.Doing.model_fields["act"].annotation)],
        },
        "paths": {
            "fields": {k: PATH_TEXT.get(k) for k in _fields(C.Path)},
            "quota_values": list(typing.get_args(typing.get_args(
                C.Path.model_fields["quota"].annotation)[0])),
            "examples": {
                "hold": lpick(lambda x: has_path(x, "hold"), "satisfied_by", "who", n=1),
                "accompanied_by": lpick(lambda x: has_path(x, "accompanied_by"),
                                        "satisfied_by", "who", n=1),
                "as": lpick(lambda x: has_path(x, "as"), "satisfied_by", "who", n=1),
            },
        },
        "designations": {
            "stamp_during": lpick(lambda x: bool(x["fields"].get("steelhead_stamp_during")),
                                  "classified", "when", "steelhead_stamp_during", n=1),
            "stamp_waived": lpick(lambda x: bool(x["fields"].get("steelhead_stamp_waived")),
                                  "classified", "steelhead_stamp_waived", n=1),
            "on_designation": lpick(lambda x: bool(x["fields"].get("on")), "on", "doing",
                                    "satisfied_by", n=1),
        },
        "period": {
            "reading": "Every designation says when it is in force in `period`. 'Class II water "
                       "when open' prints no dates on purpose: licensing is consulted only while "
                       "the water is open, so the period is the water's own open time — show "
                       "'Classified (Class II) whenever this water is open', never 'during its "
                       "classified period'. 'Class I water all year' is every day. Otherwise the "
                       "period is `dates` (the record's own `when`).",
            "fields": PERIOD_TEXT,
            # Lists, not dicts keyed by kind: `when_open` is a retired FIELD name (licensing
            # decision 6), so it may be a value here and never a key.
            "counts": [{"kind": k, "designations": n} for k, n in sorted(Counter(
                x["period"]["kind"] for x in lic.values() if x["kind"] == "designation").items())],
            "examples": [dict(e, period=lic[e["id"]]["period"])
                         for k in ("when_open", "all_year", "dates")
                         for e in lpick(lambda x, k=k: x["kind"] == "designation"
                                        and x["period"]["kind"] == k, "classified", "when", n=1)],
        },
        "field_text": {k: LICENSING_FIELD_TEXT.get(k) for k in sorted(
            {f for m in lmodels.values() for f in _fields(m)} - {"kind", "id", "verbatim"})},
        "documents": "See `licences`: the register, `provincial` = sold to an angler under the "
                     "Wildlife Act (what 'any type of fishing licence or stamp' means).",
    }

    # ---- placement ----------------------------------------------------------------------
    rvia = Counter(v for s in d["rulesets"].values() for v in s if v != "sections")
    lvia = Counter(v for s in d["licensing_sets"].values() for v in s if v != "sections")
    placement = {
        "unnamed_sets": "`waters` lists only named waters: a ruleset or licensing set that no "
                        "water lists sits only on unnamed sections — reach it through a section's "
                        "`ruleset` / `licensing_set`, never through `waters`.",
        "reading":"Where a record applies is exported the way the bundle interns it. Many "
                   "sections carry the same set of records, so each SET is listed once "
                   "(`rulesets`, `licensing_sets`: its members grouped by `via`, and how many "
                   "sections carry it). Each named water lists its `parts`: every (ruleset, "
                   "licensing_set) pair its sections carry TOGETHER, and on how many sections — "
                   "so a licence area joins the rules on the same stretch — and, on a part, "
                   "`province_except` (the families of areas its sections lie in where a "
                   "province-wide requirement stops: a national park), `anadromous_rainbow` "
                   "(a rainbow over 50 cm is a steelhead there; `ladder.steelhead_definition`) "
                   "and `steelhead` (\"known\" | \"possible\": how sure we are that steelhead "
                   "are present; the water carries the roll-up). "
                   "`null` on either side "
                   "is a stretch with no set of that kind. The parts' sections sum to the "
                   "water's `sections`. A part is a class of sections, not one stretch; which "
                   "parts border each other is `touches` (below) — merge only parts connected "
                   "through it. Set ids are local to this file and change with every "
                   "build; never store one. Section handles never leave the bundle.",
        "touches": TOUCHES_TEXT,
        "runs": {
            "reading": "WHERE EACH PART RUNS. A part's `runs` are its stretches, UPSTREAM TO "
                       "DOWNSTREAM: each from one end (`from`, upstream) to another (`to`, "
                       "downstream), with `km_from` / `km_to` measured from the water's MOUTH "
                       "along its main stem (the blue line carrying most of its length), so "
                       "km_from >= km_to and the runs' km fall as you read down the list. Say a run "
                       "'from <from> to <to>' with each end's name: a cut-point id or region line "
                       "is a key of `splits` (its `name`); `lake_inlet:` / `lake_outlet:` / "
                       "`confluence:` name a water (`waters[id].name`); `mouth`, `source` and "
                       "`bc_border` are the water's own ends. A run between the same cut on both "
                       "ends lies inside an area ('within Chilliwack River Ecological Reserve'). "
                       "The pipeline cut the water at exactly these points: this reports them and "
                       "infers nothing. Use km_from / km_to to draw or order a stretch. A river "
                       "runs through a lake as two runs (one ending at `lake_inlet`, the next "
                       "starting at `lake_outlet`) — the lake is its own water.",
            "lakes": "A lake or wetland part is its polygon: ONE run with from, to, km_from and "
                     "km_to all null, and `polygon`: `whole`, or a lake PART's own name. A lake "
                     "has no ends to name; the polygon is the place.",
            "branches": "A run with `branch: true` lies off the main stem (a side channel or braid "
                        "of the same water whose sets differ from the stem beside it); both its km "
                        "are where it rejoins the stem (null when it never does).",
            "ends": END_TEXT,
            "fields": RUN_TEXT,
            "counts": {
                "stream_parts": sum(1 for w in d["waters"].values() if w["kind"] == "stream"
                                    for p in w["parts"]),
                "runs": sum(len(p["runs"]) for w in d["waters"].values() for p in w["parts"]),
                "parts_with_several_runs": sum(1 for w in d["waters"].values()
                                               for p in w["parts"] if len(p["runs"]) > 1),
                "branch_runs": sum(1 for w in d["waters"].values() for p in w["parts"]
                                   for r in p["runs"] if r.get("branch")),
            },
            "splits": "`splits` (top level) names every cut: id -> {name, kind, water_id, km}, "
                      "km from that water's mouth. See `field_dictionary.splits`.",
        },
        "outside_bc": "A water's `outside_bc` counts its sections outside British Columbia — "
                      "past the border, or in no region. No B.C. regulation applies there and "
                      "the book does not govern them: they carry no set (the build refuses one "
                      "that does), and must read 'outside B.C.', never 'open under the general "
                      "rules'.",
        "waters_with_sections_outside_bc": sum(1 for w in d["waters"].values()
                                               if w.get("outside_bc")),
        "tidal": "A water's `tidal` (present only where the book calls the water tidal — Nitinat "
                 "Lake) is `{sections, entry, guide}`: how many of its sections are tidal, the row "
                 "that says so, and the words to show. No provincial regulation holds there: its "
                 "sections carry only that row's note (the build refuses any other rule or a "
                 "licensing set), and every province-wide requirement stops there "
                 "(`province_except`: `tidal`).",
        "waters_with_tidal_sections": sum(1 for w in d["waters"].values() if w.get("tidal")),
        "steelhead": "A water's `steelhead` (and each part's) says how sure we are that steelhead "
                     "are present: " + STEELHEAD_TEXT + " A water's `steelhead_source` lists the "
                     "rows naming steelhead that make it known (its own row, or the row of the "
                     "water it is a tributary of). IN THE BUNDLE the same fact is "
                     "`section_steelhead.code` (a view): 1 = \"known\", 2 = \"possible\", no row "
                     "= absent; `steelhead_water` (a view) is the known streams.",
        "waters_by_steelhead": dict(sorted(Counter(
            w["steelhead"] for w in d["waters"].values() if w.get("steelhead")).items())),
        "part_of": "A lake the atlas cuts into parts (Kootenay Lake's Main Body and West Arms, "
                   "Williston Lake's arms and zones, Shannon Lake's netted-off portion) lists each "
                   "part as its own water, with `part_of` naming the whole. No record may name "
                   "the whole (the reach refuses it), so the whole's own entry here carries only "
                   "what reaches it by area or province-wide: it is a leftover of the cut, not a "
                   "stretch an angler fishes apart from its parts. Show a whole through its "
                   "parts — the waters whose `part_of` is its id — never through its own `parts`.",
        "lakes_cut_into_parts": sorted({w["part_of"] for w in d["waters"].values()
                                        if w.get("part_of")}),
        "via": {k: VIA_TEXT[k] for k in VIA_TEXT},
        "via_counts": {"rulesets": dict(sorted(rvia.items())),
                       "licensing_sets": dict(sorted(lvia.items()))},
        "licensing_placement": PLACEMENT_TEXT,
        "licensing_placement_counts": dict(sorted(Counter(
            x["placement"] for x in lic.values()).items())),
        "binds": {
            "reading": "Every rule says where it holds in `binds`.",
            "values": BINDS_TEXT,
            "counts": dict(sorted(Counter(x["binds"] for x in rules.values()).items())),
            "in_part": [x["id"] for x in rules.values() if x["binds"] == "sections_in_part"],
            "examples": pick(lambda x: x["binds"] == "sections_in_part", "undrawn_part",
                             "extents", n=2),
        },
        "uncertain": {
            "rules": [x["id"] for x in rules.values() if x["provenance"]["uncertain"]],
            "licensing": [x["id"] for x in lic.values() if x["provenance"]["uncertain"]],
            "reading": "An uncertain record binds nowhere: it can only ever raise 'unknown', "
                       "never 'no rules here'. `provenance.why` says why.",
        },
        # A row that is only a pointer binds nothing, so it has nothing to place. A row with no
        # matched water whose rules DO bind (an area row: Bowron Lake Park waters, the Liard
        # River watershed — thousands of sections through its `within` extents) is placed, and
        # is not listed (SP-14): "unplaced" means NOTHING of the row reaches the map.
        "unplaced_entries": [e for e, v in d["entries"].items() if not v["matched"]
                             and v["kind"] == "water"
                             and (v["rules"] or v["licensing"] or not v.get("see"))
                             and not any(rules[r]["binds"] != "nowhere" for r in v["rules"])],
        "unplaced_entries_reading": "Water rows NOTHING of which reaches the map: no matched "
                                    "water, and no rule of the row bound anywhere. Their rules "
                                    "are `binds: nowhere` and can only raise 'unknown'. An area "
                                    "row with no matched water whose `within` extents bind is "
                                    "placed and is not listed.",
    }

    in_part = [x for x in rules.values() if x.get("not_yet_mapped")]
    placement["not_yet_mapped"] = {
        "reading": "A rule holding only in a part of its water that nothing draws. Show it AT "
                   "THE TOP of the water, prominently, as a place not yet mapped "
                   "(`not_yet_mapped.says`), and never let it decide the water: it never "
                   "competes, never displaces, never lifts (`ladder.not_yet_mapped`).",
        "rules": len(in_part),
        "closures": sorted(x["id"] for x in in_part if _closure(_f(x) | {"type": x["type"]})),
    }
    cautions = [(x["id"], e) for x in rules.values() for e in F(x, "exempts") or []
                if e.get("caution")]
    gotchas = {
        "reading": "Places where the data is right and a page is still easy to get wrong. Each "
                   "names the field to key on.",
        "size_clause_override": {
            "says": "A water row printing a LARGER number for a fish than its region allows "
                    "overrides the region's size clause ('only 1 over 50 cm') for that fish — "
                    "'(any size)' printed or not (user ruling 2026-09-26). ONLY WHERE THE ROW "
                    "PRINTS '(ANY SIZE)' is that confusing (user ruling 2026-09-28): Kootenay "
                    "Lake's 'rainbow trout daily quota = 10 (any size)' — does 'any size' mean "
                    "no size limit at all, overriding Region 4's 'only 1 rainbow trout or "
                    "cutthroat trout over 50 cm', or only no minimum size? (Duncan and Lardeau "
                    "rivers, Quesnel Lake likewise.) Only those lifts carry a warning: show "
                    "`exempts[].caution.says` beside the lifter wherever the lift is shown, and "
                    "never drop it because the lift itself applied. A row printing its own "
                    "sizes (Gwillim Lake's 'none under 40 cm or over 60 cm', Kitimat's hatchery "
                    "steelhead '>50 cm') or no size at all (Jewel Lake's 'Brook trout daily "
                    "quota = 20') overrides the clause with no warning.",
            "key_on": "rules[*].fields.exempts[*].caution.kind == 'size_clause_override'",
            "lifts": len(cautions),
            "lifters": sorted({i for i, _ in cautions}),
        },
        "not_yet_mapped": {
            "says": "A rule whose part of the water nothing draws ('No Fishing within 200 m of "
                    "Bush-Sullivan Bridge' on Kinbasket Lake) is placed on the WHOLE water only "
                    "so it can be seen. Show it at the top of the water, marked as a place not "
                    "yet mapped; it never closes, colours or decides the water.",
            "key_on": "rules[*].not_yet_mapped (display: 'prominent'); in an answer, state "
                      "'not_yet_mapped'",
            "rules": len(in_part),
        },
        "trout_includes_char": {
            "says": "'Trout' includes char UNLESS THE REGULATION MENTIONS CHAR (p.86; user ruling "
                    "2026-09-28). A lake row printing only 'Trout daily quota = 2' is TROUT_CHAR: "
                    "a char counts toward the 2 — never label it 'trout only'. But where the SAME "
                    "row, or the same zone table, names a char on its own ('char', Dolly "
                    "Varden/bull trout, lake trout, brook trout), its bare 'trout' lines are "
                    "about trout alone: TROUT_CHAR with species_except CHAR (label 'Trout'). "
                    "Region 6's box mentions Dolly Varden and lake trout, so its '1 trout from "
                    "streams', 'Trout under 30 cm from any stream' and 'Trout of any size from "
                    "streams, Nov 1-June 30' do not bind char; Region 1's box releases 'All "
                    "char', so its 'Trout: 4' counts trout only. 'Trout/char' always includes "
                    "char. Show `note` beside every trout rule, and `note_trout_only` beside one "
                    "whose char are excluded, so the reader sees which reading applies.",
            "note": "Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) "
                    "unless the regulation lists char separately.",
            "note_trout_only": "This regulation lists char separately, so 'trout' here means "
                               "trout only (rainbow, steelhead, cutthroat, brown); char follow "
                               "their own lines.",
            "key_on": "species contains TROUT_CHAR (note); and species_except contains CHAR "
                      "(note_trout_only)",
            "trout_only_rules": sorted(i for i, x in rules.items()
                                       if "TROUT_CHAR" in (_f(x).get("species") or [])
                                       and "CHAR" in (_f(x).get("species_except") or [])),
        },
        "dated_zone_release_stands": {
            "says": "A DATED zone release or closure is NOT silenced by a water's quota for the "
                    "fish (user ruling 2026-09-28). Shuswap Lake prints 'Char daily quota = 1 "
                    "(none under 60 cm)'; Region 3 prints 'you must release … Lake trout from "
                    "Oct 15-Jan 31'. On those dates both speak and the release binds: no lake "
                    "trout may be kept. Only the exact same statement (same fish, sizes, origin, "
                    "water kind and the same dates), a lift the row prints, or a derived lift of "
                    "a closure that sends the reader to the tables ('see tables for exceptions', "
                    "Region 8's bass) takes the zone rule away. Never hide a dated zone release "
                    "because the water has its own number. BUT a water row printing ITS OWN "
                    "DATES for the fish overrides the dated zone rule on the days both hold "
                    "(user ruling 2026-09-28): Cheslatta Lake's 'Lake trout … quotas = 3' "
                    "(Nov 1-Sept 14) replaces Region 6's 'Lake trout from Fraser and Skeena "
                    "Watersheds, Sept 15-Nov 30' release on Nov 1-30 (`ladder.dated_zone_release` "
                    "(A)); a zone closure is never overridden this way.",
            "key_on": "an answer holding a zone rule with take 0 and `when.dates` beside a water "
                      "quota for the same fish (`ladder.dated_zone_release`)",
        },
        "one_side_of_channel": {
            "says": "A rule for ONE HALF of a river's channel ('No Fishing on the west half of "
                    "river between fishing boundary signs near Kitimat Hatchery outfall') is "
                    "placed on the stretch, because the map draws a river as one line, but it "
                    "holds on that half only: an angler on the other half follows the river's "
                    "other regulations there. The reference answers it `beside` those (it "
                    "displaces nothing). Show `parts.side` with it, prominently; never colour "
                    "or close the whole width by it.",
            "key_on": "rules[*].fields.side",
            "rules": sorted(i for i, x in rules.items() if _f(x).get("side")),
        },
        "bull_trout_is_dolly_varden": {
            "says": "Bull trout and Dolly Varden are one fish in the regulations (p.86). Ask "
                    "about `DV`; there is no bull trout code.",
            "key_on": "species.fish.DV",
        },
        "open_subjects": {
            "says": "`ALL_FIN_FISH`, `PROTECTED_SPECIES` and `SALMON` have empty member lists "
                    "on purpose. An empty list is not 'no fish'.",
            "key_on": "species.groups[*].open",
        },
        "source_artefacts": {
            "says": "Text the book prints inside a row that is NOT a regulation, so no rule "
                    "carries it and the page must not look for one. The row's `regs_verbatim` "
                    "still shows it, as printed.",
            "key_on": "entries[*].id + regs_verbatim; no rule quotes these",
            "artefacts": [{"entry": a["entry_id"], "row": a["row"], "text": a["text"],
                           "says": a["says"]} for a in C.SOURCE_ARTEFACTS],
        },
    }

    labels = {
        "reading": "A record's line is shipped as PARTS, each generated from its fields and "
                   "never authored. Compose them in the UI; `verbatim` — the book's sentence, "
                   "exact — is the provenance to show underneath, always. A part with nothing to "
                   "say is ABSENT, never a placeholder, and no part is ever the verbatim. With no "
                   "`what`, the rule's sentence IS the rule: show the verbatim as the book's text "
                   "and the other parts as context. `label` is one preview composed the "
                   "suggested way (catalogue.compose); a UI may compose differently from `parts`.",
        "book_reasons": "A reason the book gives ('located in an Ecological Reserve', 'for the "
                        "conservation of chinook') is NOT a part: the model has no field for "
                        "a reason, and a part is generated from fields only. It reaches a reader "
                        "through the verbatim shown underneath, never paraphrased.",
        "rule_parts": {k: PART_TEXT[k] for k in C.LABEL_PARTS},
        "rule_order": {
            "order": list(C.LABEL_PARTS),
            "suggested": "what (size), conditions, when — where — side — in part: in_part — "
                         "lifts — "
                         "duty — suspended (notice)",
        },
        "licensing_parts": {k: LICENSING_PART_TEXT[k] for k in C.LICENSING_PARTS},
        "licensing_order": {
            "need": "{who or 'You'} need {need} {doing} {where}, {when} ({except}); {note}.",
            "must": "{who}: {must} {doing} {where}, {when}",
            "way": "{who}: {doing} {where}, {when}, {way}",
            "otherwise": "{what}, {when} (licence unit: {unit}) | ({unit}): {terms}, {instead}. "
                         "{stamp}. {suspended}.",
        },
        "counts": {
            "rules_by_part": dict(sorted(Counter(k for x in rules.values()
                                                 for k in x["parts"]).items())),
            "rules_without_what": sum("what" not in x["parts"] for x in rules.values()),
        },
        "examples": {
            "with a place": pick(lambda x: "where" in x["parts"] and "when" in x["parts"], n=1),
            "in part": pick(lambda x: "in_part" in x["parts"], n=1),
            "no what": pick(lambda x: "what" not in x["parts"], n=1),
        },
    }
    contents = {
        "how_to_read": "the file's shape, ids and what is not in it",
        "entries": "what an entry is, and its four kinds",
        "labels": "a record's line as parts; how to compose them; verbatim underneath",
        "rule_types": "every rule type: what it means, fields it uses, competition, closing",
        "families": "the six families the types group into",
        "ladder": "which rules compete and who speaks",
        "gear": "slots, bounds, conditions, `while`, `conduct`, first match per slot",
        "sizes": "`lengths`: ordered ranges, first match wins",
        "time": "`when`: dates, hours, weekdays, unparsed, suspended_while",
        "species": "codes, groups, expansion, species_except, open groups",
        "retention": "take, may_target, period, per_daily, within, and the readings of take 0",
        "vessel": "aspect, level, power, speed",
        "exempts": "how a rule lifts another",
        "standing": "rules everywhere at places nobody can draw",
        "angler_closure": "closures to one kind of angler",
        "licensing": "kinds, who, doing, paths, designations and their classified `period`, and "
                     "the rules of reading",
        "placement": "sets, waters and their parts (where each runs: `runs`; the cuts: "
                     "`splits`), outside B.C., tidal water, steelhead known | possible, lake "
                     "parts, via, placement, binds (and undrawn parts: not_yet_mapped), "
                     "uncertain",
        "status": "the map and search colour (closed / own / base) — a separate file, "
                  "`status_index.bin`, computed from the same reference reader",
        "gotchas": "where a page is easy to get wrong: size-clause overrides, places not yet "
                   "mapped, trout includes char, dated zone releases, one side of the channel, "
                   "bull trout is Dolly Varden, open subjects, source artefacts",
        "cases": "SAMPLE WATERS to build the page against while it is built out — one or more "
                 "per mechanism, each with what to show and the reference answer",
    }
    status = {
        "reading": "THE MAP AND SEARCH COLOUR is not in this file. It ships beside it as "
                   "`status_index.bin` (`python -m pipeline.deliver.status_index`), one of three "
                   "answers per section and per water for every day of the year, computed from the "
                   "bundle by the same reference reader this guide restates "
                   "(`read.effective_rules`), so the colour cannot disagree with the rules the "
                   "page shows. Use it to colour; use this file to explain.",
        "statuses": {
            "closed": "on that day every game fish (the book's list minus crayfish) is answered "
                      "by an unconditional closure that speaks — not a closure of some hours, "
                      "weekdays or an unreadable season, not one half of the channel, not a "
                      "`not_yet_mapped` note, not a closure of some species only",
            "own": "not closed, and a water table's row (`r<n>:` — the water's own row, a cut "
                   "piece's, an area row, or one reaching it by the tributary walk) binds it; all "
                   "year, as the synopsis lists the water",
            "base": "neither: only zone, area-of-zone, provincial and superior rules bind it. "
                    "Absent from the file means base",
            "tidal": "tidal water (Nitinat Lake): no freshwater status; show `water.tidal.guide`",
            "outside": "outside B.C.: no B.C. regulation — never 'base'",
        },
        "water": "A water's colour is its parts' roll-up: closed when every part is closed "
                 "(tidal / outside when every part is); own when any part carries a row's rule; "
                 "base otherwise. Licensing never changes the colour (it never affects open or "
                 "closed).",
        "vintage": "The file carries the bundle's `section_handles` digest "
                   "(`about.bundle.section_handles`); a reader refuses one whose digest differs, "
                   "as it refuses tiles and a bundle of different vintages.",
    }
    return {
        "contents": contents,
        "how_to_read": {
            "shape": {
                "rules": "every rule, keyed `entry_id::rule_id`",
                "licensing": "every licensing record, keyed `entry_id#record_id`",
                "entries": "every synopsis row; lists its rule and licensing ids",
                "licences": "the document register",
                "rulesets / licensing_sets": "the interned sets of records that sections carry",
                "waters": "every named water (by durable item_id): its name, kind, section "
                          "count and matching rows (`entries`); its `parts` (the (ruleset, "
                          "licensing_set) pairs its sections carry together, with "
                          "`province_except`, `anadromous_rainbow` and `steelhead` (known | "
                          "possible) where they hold, `touches`: the other parts each borders, "
                          "and `runs`: where each runs, between which cuts); its "
                          "`outside_bc` count; `part_of` for a lake part; `steelhead` (the "
                          "parts' roll-up) and `steelhead_source` (the rows that make it "
                          "known); and `tidal` on the one tidal water — see "
                          "`field_dictionary.water`",
                "splits": "every cut a run can end at, by id: its name, and where it stands "
                          "(water and km from the mouth)",
                "species": "the book's species list (p.86) under its headings, and the groups "
                           "and open subjects a rule may name",
                "field_dictionary": "every field in the file, and what it means",
                "index": "ids grouped by type and kind",
            },
            "every_record": "Each rule and licensing record reads itself: `label` (generated "
                            "from its fields), `verbatim` (the printed sentence), `fields` "
                            "(exactly as the bundle ships them), and `provenance`. A rule also "
                            "says where it holds (`binds`): on its sections, only in an undrawn "
                            "part of them (a note — never colour a water by it), or nowhere.",
            "ids": "A rule id or record id is unique only within its entry; always use the "
                   "full key. item_id is the durable id of a water.",
            "not_included": "Nothing is settled: no quota tables, no open/closed verdicts, no "
                            "colours. This guide says how the fields are read; applying it is "
                            "the reader's job, and the ladder below is the rule for it. The "
                            "map's colour per section and water, per day, is a SEPARATE file "
                            "(`status_index.bin`, see `status`), not this one.",
        },
        "entries": {
            "kinds": {
                "province": "`zp:` — the provincial regulations; bind everywhere their extents "
                            "reach",
                "zone": "`z<region>:` — a region's chapter: its standing tables and notices",
                "area": "`z<region>:` whose own extents name a place smaller than a region "
                        "(a management-unit group, a wildlife management area, a park)",
                "water": "`r<region>:` — one row of a region's water table",
            },
            "counts": dict(sorted(Counter(e["kind"] for e in d["entries"].values()).items())),
            "fields": ENTRY_TEXT,
            "pointers": {
                "reading": "`see` is a POINTER the row prints — 'See Lonzo Creek', 'A tributary "
                           "of Slocan River. See Slocan River' — and never a rule: it binds "
                           "nothing. Show it as a link to each of `entry_ids` ('see Lonzo "
                           "Creek'). A row whose only content is a pointer has no rules and no "
                           "licensing: it is SKIPPED as a regulation and read as the link. A "
                           "pointer row BINDS NOTHING and moves nothing: the TARGET row carries "
                           "the regulations, on the waters it matches, in the region those "
                           "waters lie in ('MARA LAKE — See Shuswap Lake in Region 3': Shuswap "
                           "Lake's row covers Mara Lake). Where the target does not cover the "
                           "pointer's water (Panther Lake, Bighorn Creek, Nation River, the Arrow "
                           "Lakes rows) that water carries only what else binds it — a gap in "
                           "the target's reach, never something the pointer supplies. "
                           "`unresolved` (no `entry_ids`) is a pointer that names no row — show "
                           "its words, it cannot be followed.",
                "relation": {
                    "see": "a different water, governed by the named rows' regulations",
                    "alias": "this row's water IS the named row's (one water printed under two "
                             "names — 'JONES LAKE: See Wahleach Lake'); the named row's rules "
                             "already cover it",
                    "twin": "the same row printed under two region tables (a MU 6-1 lake in "
                            "both Region 5 and Region 6); the named row is the one that binds",
                },
                "counts": dict(sorted(Counter(
                    s.get("relation", "unresolved") for e in d["entries"].values()
                    for s in e.get("see") or []).items())),
                "pointer_only": sorted(e for e, v in d["entries"].items() if v.get("see")
                                       and not v["rules"] and not v["licensing"]),
            },
        },
        "labels": labels,
        "rule_types": rule_types,
        "families": families,
        "ladder": ladder,
        "gear": gear,
        "sizes": sizes,
        "time": time,
        "species": species,
        "retention": retention,
        "vessel": vessel,
        "exempts": exempts,
        "standing": standing,
        "angler_closure": angler_closure,
        "licensing": licensing,
        "placement": placement,
        "status": status,
        "gotchas": gotchas,
    }


# --------------------------------------------------------------------------------------------
# Test waters: one REAL case of every mechanism, with the answer the reference gives
# --------------------------------------------------------------------------------------------
#
# The consumer builds its page against a handful of waters; each mechanism of the ladder needs at
# least one it can assert against ("add more test waters to cover weird ones … we need to cover all
# orthogonal items", 2026-09-26). Every case is FOUND in the data by a predicate, exactly as the
# guide's examples are (`_Pick`): never a remembered id, so a case that stops existing is replaced
# by the next match, and a mechanism with no match is reported (`problems`) and fails the tests.
#
# A case names its water (`item_id`), the rule set and licensing set its sections carry
# (`waters[item].parts`), a date, a fish, and the EXPECTED answer: `read.effective_rules` for that
# part on that date — every rule that speaks, stands beside or is shown, with `partly_lifted` where
# a lift holds only in part. Section handles never leave the bundle (AGENTS 5): the answer depends
# only on the part's rule set and whether a rainbow over 50 cm is a steelhead there
# (`anadromous_rainbow`), both of which the case carries.

CASE_MECHANISMS = {
    "zone_only": "a water with no row of its own: the region's table speaks alone",
    "quota_beside": "a water quota that says something DIFFERENT from the zone's sits beside it — "
                    "both speak (`ladder.quotas_sit_beside`, case 3)",
    "larger_replaces": "a water row printing a LARGER number for a fish lifts the zone's quota "
                       "for that fish, and speaks alone (case 2)",
    "same_statement": "a water quota stating EXACTLY the zone's statement replaces the zone's "
                      "number, here a larger one (case 1)",
    "water_release": "a water's outright release silences a zone quota of another dimension for "
                     "that fish (`ladder.water_release`)",
    "naming_beats_group": "a zone rule NAMING the fish beats a water row's GROUP rule for it",
    "water_names_fish": "a water row that itself names the fish beats the zone's rule naming it",
    "within_clause_parent_level": "a zone quota's `within` clause naming the fish is read at its "
                                  "parent's (group) level: the water's group release beats it",
    "water_closure_silences_zone": "a water's dated closure speaks for every fish it covers as if "
                                   "it named it: the zone's named quota is silent on its dates",
    "derived_lift": "a water row naming a fish its region closes lifts that closure "
                    "(`exempts[].basis: names_the_fish`)",
    "printed_lift_reopens_closure": "a printed lift of a region's species closure reopens it",
    "closure_prints_its_exemptions": "a region's closure that prints its own exemption list takes "
                                     "no derived lift: a water row naming the fish leaves it standing",
    "blanket_closure_exempt": "a blanket seasonal closure lifted on a water exempt from it",
    "equivalent_lift": "a row's lift of its region's blanket closure reaches the SAME KIND of "
                       "closure in another region its water lies in (`exempts[].equivalent`)",
    "lift_for_one_fish": "a lift for one fish leaves the lifted rule standing for every other "
                         "fish (asked about a fish the lift does not name)",
    "partly_lifted": "a lift that holds only for some anglers or fish leaves the rule standing, "
                     "marked `partly_lifted`",
    "counted_apart": "a fish counted apart from the zone's aggregate by the zone's own table "
                     "(`ladder.counted_apart`)",
    "two_regions_lake": "a lake drawn across a region line: both regions' tables bind, the MOST "
                        "STRICT displaces the other (`ladder.two_regions`)",
    "home_region_differs": "a river piece taking the zone rules of the region it lies in, which "
                           "is not the region of its own row",
    "pointer": "a pointer row ('See X') binds nothing; the target row's rules speak on the "
               "pointer's water",
    "steelhead_water": "where a rainbow over 50 cm is a steelhead, a rule about rainbow over 50 cm "
                       "speaks for no rainbow",
    "dated_in_force": "a dated water rule on a day it is in force",
    "dated_out_of_force": "the same water and fish on a day the dated rule is not in force",
    "semicolon_dated_clause": "a date printed after a semicolon clause scopes only that clause: the "
                              "clause before it holds all year",
    "part_day_beside": "a rule in force only some hours or weekdays (or of unreadable season) "
                       "stands BESIDE what it would displace",
    "suspended_while": "a record dormant while its named closure is in force (`suspended_while`: "
                       "today only licensing records carry it) — asked on a day the closure "
                       "speaks",
    "size_band": "a quota with a size window (`lengths`)",
    "origin": "a rule about hatchery or wild fish only (`origin`)",
    "while_or_targeting": "a rule that holds only while fishing a way (`while`) or for a target "
                          "(`when_targeting`)",
    "gear_only": "a water whose own row says only how to fish (gear, bait, method)",
    "annual_clock": "a quota on another clock than the day (`period`)",
    "superior_authority": "a federal or park rule, outside the ladder",
    "standing": "a standing rule: shown everywhere, never deciding",
    "angler_closure": "a water closed to one kind of angler (`closed_to`)",
    "tributary_walk": "a water rule reaching this water by the tributary walk (`via: trib`)",
    "watershed_part": "a rule bound to a PART of a watershed, cut by FWA code (`Extent.watershed`)",
    "undrawn_part": "a rule held on the water as a note, in a part nothing draws (`undrawn_part`)",
    "licensing_classified": "a classified water (licensing designation)",
    "licensing_stamp": "a water where a stamp is required",
    "province_except": "a part where a province-wide licensing requirement stops (a national park)",
    "outside_bc": "a part of a water outside British Columbia: no rules at all",
    # ---- sample waters added 2026-09-26 (kinds the UI had nothing to look at for) ----------
    "clause_only_lift": "a water number larger than one CLAUSE of the zone's quota lifts only "
                        "that clause: the fish still counts toward the zone's aggregate "
                        "(Duncan-style)",
    "dated_lift": "a lift in force only while its lifter is: on a day the lifter's dates "
                  "exclude, the lifted zone rule speaks again (Williston-style)",
    "same_statement_smaller": "a water quota stating EXACTLY the zone's statement with a SMALLER "
                              "number replaces the zone's number (case 1)",
    "streams_only_zone_clause": "a zone clause that binds only on streams ('2 from streams'), "
                                "speaking on a stream",
    "two_regions_closure": "a lake across a region line: one region's CLOSURE of a fish "
                           "displaces the other region's open rule for it",
    "two_regions_release": "a lake across a region line: one region's RELEASE of a fish "
                           "displaces the other region's quota for it",
    "keep_beside_release": "a clause that KEEPS some of a fish speaking beside a release of "
                           "others of the same fish (hatchery kept, wild released)",
    "undrawn_closure": "a closure in a part nobody has drawn, held on the whole water as a note "
                       "(`not_yet_mapped`): it silences nothing",
    "size_clause_caution": "a larger water number printed '(any size)' lifts the zone's size "
                           "clause ('1 over 50 cm') too, and only such a lift carries `caution` "
                           "(size_clause_override)",
    "kootenay_rainbow_10": "Kootenay Lake (main body): rainbow 10 (any size) replaces Region 4's "
                           "trout/char 5 and its size clause for rainbow",
    "dean_beside": "the Dean River: its 'Trout/char daily quota = 1 (none under 35 cm)' sits "
                   "beside Region 5's 'Trout/char: 5'",
    "straddling_named_lake": "Ahbau Lake (Region 5 / Zone 7A) or Mara Lake (Region 3 / Region "
                             "8): both regions' tables bind the lake",
    # ---- sample waters added 2026-09-28 (user rulings A, B, C) -------------------------------
    "trout_word_same_statement": "Amor Lake: its 'Trout daily quota = 2' (no char mentioned: "
                                 "trout and char) and Region 1's 'Trout: 4' (its box mentions "
                                 "char: trout only) are the same statement — the lake's 2 "
                                 "replaces the 4 for a trout (`gotchas.trout_includes_char`)",
    "dated_zone_release_stands": "Shuswap Lake on Nov 1: Region 3's dated lake trout release "
                                 "speaks beside the lake's 'Char daily quota = 1' "
                                 "(`ladder.dated_zone_release`)",
    "water_dates_override": "Cheslatta Lake on Nov 15: the lake's own-dated 'Lake trout … "
                            "quotas = 3' (Nov 1-Sept 14) replaces Region 6's 'Lake trout … "
                            "Sept 15-Nov 30' release on the days both hold "
                            "(`ladder.dated_zone_release` (A))",
    "one_side_beside": "Kitimat River at the hatchery outfall: the west-half closure is shown "
                       "BESIDE the river's other rules, which the east half answers to "
                       "(`fields.side`)",
    # ---- sample waters added 2026-10-02 (steelhead: known | possible, lakes) ----------------
    "steelhead_known": "a KNOWN steelhead stream (`steelhead: known`): a row names steelhead on "
                       "it, or it is a tributary stream of such a water — the provincial "
                       "steelhead set and the region's steelhead lines apply, and a rainbow over "
                       "50 cm is a steelhead (`anadromous_rainbow`)",
    "steelhead_possible": "a POSSIBLE steelhead stream (`steelhead: possible`): a stream of "
                          "Regions 1, 2, 3, 5 or 6 the book names no steelhead on — the "
                          "steelhead rules apply, steelhead may not be present, and a rainbow "
                          "over 50 cm is a rainbow",
    "lake_no_steelhead": "a lake of a steelhead region whose own row names no steelhead: no "
                         "steelhead rule or stamp applies there (no `steelhead` on the part)",
    "own_row_steelhead_lake": "Khartoum or Lois Lake: the lake's own row names steelhead "
                              "('Rainbow trout/hatchery steelhead quota = 6 in the aggregate'), "
                              "so it carries the whole provincial steelhead set and Region 2's "
                              "wild release, as a steelhead stream does",
}

#: WHAT THE PAGE SHOULD SHOW for a case — one plain line for the builder, per mechanism.
#: `{water}` and `{fish}` are filled from the case.
WHAT_TO_SHOW = {
    "zone_only": "{water} has no row of its own: show the region's rules for {fish} as the "
                 "whole answer, labelled as the region's.",
    "quota_beside": "Show BOTH quotas for {fish} on {water} — the water's and the region's — "
                    "each binds; never pick one.",
    "larger_replaces": "Show only {water}'s larger number for {fish}; the region's aggregate is "
                       "lifted for that fish and must not appear.",
    "same_statement": "Show {water}'s number for {fish} in place of the region's identical "
                      "statement; the region's line is gone.",
    "water_release": "Show {water}'s release for {fish}; the region's quota for it is silenced "
                     "and must not appear.",
    "naming_beats_group": "The region's rule NAMING {fish} is the answer on {water}; the "
                          "water's group rule does not speak for it.",
    "water_names_fish": "{water}'s own rule naming {fish} is the answer; the region's rule "
                        "naming it is displaced.",
    "within_clause_parent_level": "{water}'s release speaks for {fish}; the region's clause "
                                  "naming {fish} is part of a group quota and does not beat it.",
    "water_closure_silences_zone": "On this date {water} is closed: show the closure; the "
                                   "region's quota for {fish} is silent.",
    "derived_lift": "{water}'s row names {fish}, so the region's closure of it is lifted here: "
                    "show the row, not the closure.",
    "printed_lift_reopens_closure": "{water} prints an exemption: the region's closure of "
                                    "{fish} is lifted and must not appear.",
    "closure_prints_its_exemptions": "The region's closure lists its own exemptions and {water} "
                                     "is not one: show the closure for {fish}.",
    "blanket_closure_exempt": "{water} is exempt from the region's seasonal closure: show it "
                              "open on this date.",
    "equivalent_lift": "{water}'s exemption reaches the same kind of closure in the other "
                       "region it lies in: show that closure lifted too.",
    "lift_for_one_fish": "The lift on {water} is for another fish: the lifted rule still "
                         "speaks for {fish} — show it.",
    "partly_lifted": "The rule still binds {fish} on {water} but is lifted for some anglers or "
                     "fish: show it with a 'partly lifted' marker and the lift's condition.",
    "counted_apart": "{fish} is counted apart from the region's aggregate here: show its own "
                     "number, not the aggregate.",
    "two_regions_lake": "{water} lies in two regions: show the stricter region's rule for "
                        "{fish}; the other's is displaced.",
    "home_region_differs": "This piece of {water} takes the zone rules of the region it lies "
                           "in, not its row's region: show those.",
    "pointer": "{water} is covered by the row it points at: show that row's rules and a link "
               "('See …').",
    "steelhead_water": "On {water} a rainbow over 50 cm is a steelhead: rules about rainbow "
                       "over 50 cm do not speak for {fish}.",
    "dated_in_force": "The water's dated rule for {fish} is in force on this date: show it.",
    "dated_out_of_force": "Outside its dates the water's rule is gone: show what the region "
                          "says for {fish}.",
    "semicolon_dated_clause": "The date after the semicolon scopes only the clause before it; "
                              "the first clause holds all year — show it.",
    "part_day_beside": "A rule in force only some hours or days: show it BESIDE the rule it "
                       "would replace, with its hours, never instead of it.",
    "suspended_while": "This is a LICENSING record (a licence, stamp or designation), dormant "
                       "while its closure speaks: show the closure, and the licensing line as "
                       "not required today. No regulation rule carries `suspended_while` — only "
                       "licensing records do — so the rule list never changes because of it.",
    "size_band": "{water}'s quota for {fish} has a size window: show the sizes with the "
                 "number.",
    "origin": "{water}'s rule is about hatchery or wild {fish} only: say which.",
    "trout_word_same_statement": "Show {water}'s trout number for {fish} in place of the "
                                 "region's 'Trout: 4'; a char there still answers to the "
                                 "region's char release. Show the trout note beside it.",
    "dated_zone_release_stands": "On this date show the region's release of {fish} BESIDE "
                                 "{water}'s own quota: the release binds, none may be kept.",
    "water_dates_override": "On this date {water}'s own quota for {fish} replaces the region's "
                            "dated release (the water's dates overlap it, and the water's row "
                            "wins on the overlap): show the water's quota and NOT the region's "
                            "release — and, BESIDE the water's quota, the region's quotas that "
                            "count {fish} with other fish (its 'Trout/char: 5' and clauses): an "
                            "overridden release silences nothing. On the water's own release "
                            "dates show its release.",
    "one_side_beside": "Show the closure on {water} with its side ('west half of the channel "
                       "only'), prominently, BESIDE the rules the other half follows — never as "
                       "the whole river closed.",
    "while_or_targeting": "The rule holds only while fishing a certain way or for a certain "
                          "fish: show the condition with it.",
    "gear_only": "{water}'s own row says only how to fish: show the gear lines and the "
                 "region's quotas for {fish}.",
    "annual_clock": "A quota on another clock than the day: show 'per licence year' (or the "
                    "clock) with the number. Never add a 'record it on your licence' line of "
                    "your own: if the quota has `recorded_by`, show that rule's text once, under "
                    "it; if not, say nothing extra.",
    "superior_authority": "A federal or park rule: show it first; nothing below it opens what "
                          "it closed.",
    "standing": "A rule that holds everywhere at undrawable places: show it as a note; it "
                "never decides the water.",
    "angler_closure": "{water} is closed to one kind of angler: show it conditionally ('if you "
                      "are …').",
    "tributary_walk": "This water is a tributary: show the downstream row's rule as reaching "
                      "it ('inherited from …').",
    "watershed_part": "A rule bound to part of a watershed: show it on this piece of {water}.",
    "undrawn_part": "Show this rule AT THE TOP of {water}, marked 'not yet mapped' with its "
                    "part in words; it decides nothing.",
    "licensing_classified": "{water} is a classified water here: show the designation and the "
                            "licence it requires, conditionally.",
    "licensing_stamp": "A stamp is required here for some anglers or fish: show it "
                       "conditionally.",
    "province_except": "This part lies where a province-wide requirement stops (a national "
                       "park): do not show that requirement here.",
    "outside_bc": "This part of {water} is outside B.C.: show 'outside B.C.', never 'no rules'.",
    "clause_only_lift": "{water}'s number lifts only a clause of the region's quota: show "
                        "{water}'s number AND the region's aggregate for {fish} — it still "
                        "counts toward it.",
    "dated_lift": "Outside the lifter's dates the region's rule speaks again: show it for "
                  "{fish} on this date, and the water's own dated rule as not in force.",
    "same_statement_smaller": "{water} states the region's quota for {fish} with a smaller "
                              "number: show {water}'s number only.",
    "streams_only_zone_clause": "On a stream the region's 'from streams' clause binds: show it "
                                "with the aggregate for {fish}.",
    "two_regions_closure": "{water} lies in two regions and one closes {fish}: show the "
                           "closure; the other region's open rule is displaced.",
    "two_regions_release": "{water} lies in two regions and one releases {fish}: show the "
                           "release; the other region's quota is displaced.",
    "keep_beside_release": "Show the keeping clause and the release for {fish} side by side "
                           "(e.g. hatchery kept, wild released) — both bind.",
    "undrawn_closure": "Show the closure at the top of {water} as a place NOT YET MAPPED; the "
                       "rest of the water is open under the rules that speak.",
    "size_clause_caution": "Show {water}'s number for {fish} with the caution beside it: it "
                           "overrides the region's 'only 1 over 50 cm', and whether '(any size)' "
                           "means no size limit or only no minimum, the book does not say.",
    "kootenay_rainbow_10": "A rainbow on the main body answers to the lake's 10 (any size) "
                           "alone; show the region's 5 for every other trout and char.",
    "dean_beside": "Show the Dean's 1 a day (none under 35 cm) beside Region 5's trout/char "
                   "5: both bind.",
    "straddling_named_lake": "{water} lies in two regions: show both regions' rules for {fish}, "
                             "the stricter displacing the other where they say the same thing.",
    "steelhead_known": "{water} is KNOWN steelhead water: show the steelhead rules that speak "
                       "for {fish} (`expect`) plainly, with no caveat — the provincial set (the "
                       "annual hatchery 10 with its record duty under it once, the wild release) "
                       "and the region's steelhead lines, except where the water's own row "
                       "displaces them (a row's release silences the 10) — and the Conservation "
                       "Surcharge Stamp, conditionally (licensing). A rainbow over 50 cm here is "
                       "a steelhead.",
    "steelhead_possible": "Steelhead rules apply on {water}, but the book names no steelhead "
                          "on it: show the same steelhead rules for {fish} under the quiet line "
                          "'Steelhead rules apply here; steelhead may not be present in this "
                          "water.' A rainbow over 50 cm here is a rainbow.",
    "lake_no_steelhead": "{water} carries no steelhead rule: show nothing about steelhead — no "
                         "steelhead quota, release or stamp, no 'may not be present' line. A "
                         "big rainbow here falls under the rainbow quota.",
    "own_row_steelhead_lake": "{water}'s own row names steelhead: show its own line and the "
                              "provincial steelhead set for {fish} plainly — the annual hatchery "
                              "10 (its record duty under it, once), the wild release, the stamp "
                              "(conditionally) — as on a known steelhead stream. A rainbow over "
                              "50 cm on the lake is still a rainbow (no `anadromous_rainbow`).",
}


def _leaves(x: dict) -> list[str]:
    """The FISH of the book's list (p.86) a rule is about — what a case may ask about. An open
    subject is not a fish: `ALL_FIN_FISH` is every fish but crayfish (`read.speaks_for`), and
    `PROTECTED_SPECIES` / `SALMON` hold no game fish (a case once asked about the fish
    "PROTECTED_SPECIES")."""
    def fish(codes):
        out = set()
        for c in C.expand_species(list(codes or [])):
            if c == "ALL_FIN_FISH":
                out |= set(C.BOOK_SPECIES) - {"CRA"}
            elif c in C.BOOK_SPECIES:
                out.add(c)
        return out
    return sorted(fish(x.get("species")) - fish(x.get("species_except")))


def _keeps(x: dict) -> bool:
    from pipeline.deliver.bundle.rules import yields_to_release
    return bool(yields_to_release(x)) and x.get("take") is not None


def _closure(x: dict) -> bool:
    return x.get("type") == "retention_limit" and x.get("take") == 0 and not x.get("may_target")


_DAYS = [(m, d) for m in (7, 8, 6, 9, 5, 10, 4, 11, 3, 12, 2, 1) for d in (1, 15)]


class _Cases:
    """The data a case is found in: the bundle's rules (`read.rules` shape, with ladder rank), its
    interned sets, and the named water parts carrying each."""

    def __init__(self, d: dict, bundle: Path):
        from pipeline.deliver.bundle import read as RD
        self.RD, self.d, self.path = RD, d, str(bundle)
        self.R = RD._rules_of(self.path)
        db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
        self.members: dict = defaultdict(dict)
        for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset"):
            if (e, r) in self.R:
                self.members[str(s)][(e, r)] = via
        self.lic: dict = defaultdict(list)
        for s, e, r in db.execute("SELECT set_id, entry_id, record_id FROM licensing_set"):
            self.lic[str(s)].append(f"{e}#{r}")
        self.sid = {(str(s), bool(sw)): sid for s, sw, sid in db.execute(
            "SELECT r.set_id, EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = r.sid), "
            "MIN(r.sid) FROM section_ruleset r GROUP BY 1, 2")}
        self.outside = dict(db.execute(
            "SELECT i.item_id, MIN(o.sid) FROM outside_bc o JOIN item_section s ON s.sid = o.sid "
            "JOIN item i ON i.ord = s.ord GROUP BY i.item_id"))
        db.close()
        self.parts = [(it, p) for it, w in sorted(d["waters"].items()) for p in w["parts"]]
        self.first_part: dict = {}
        for it, p in self.parts:
            if p["ruleset"] is not None:
                self.first_part.setdefault(p["ruleset"], (it, p))
        self.sets = sorted(self.first_part, key=int)

    # ---- the answer ----------------------------------------------------------------------
    def section(self, item: str, part: dict):
        if part["ruleset"] is None:
            return self.outside.get(item)
        return self.sid.get((part["ruleset"], bool(part.get("anadromous_rainbow"))))

    def answer(self, item: str, part: dict, on, fish: str) -> list[dict]:
        sid = self.section(item, part)
        if sid is None:
            return []
        return [{"id": f"{x['entry']}::{x['rule']}", "state": x["state"],
                 **({"partly_lifted": True} if x.get("partly_lifted") else {})}
                for x in self.RD.effective_rules(sid, on, fish, self.path)]

    def record(self, mech, item, part, on, fish, because, ans, licensing=False) -> dict:
        w = self.d["waters"][item]
        return {
            "mechanism": mech, "shows": CASE_MECHANISMS[mech],
            "what_to_show": WHAT_TO_SHOW[mech].format(water=w["name"] or item,
                                                      fish=C._SPECIES_WORDS.get(fish, fish)
                                                      .lower()),
            "water": {"item_id": item, "name": w["name"], "kind": w["kind"]},
            "entries": w["entries"],
            "ruleset": part["ruleset"], "licensing_set": part["licensing_set"],
            **({"anadromous_rainbow": True} if part.get("anadromous_rainbow") else {}),
            **({"province_except": part["province_except"]} if part.get("province_except") else {}),
            **({"steelhead": part["steelhead"]} if part.get("steelhead") else {}),
            "date": f"{on[0]:02d}-{on[1]:02d}", "fish": fish,
            "because": sorted(set(because)),
            "expect": ans,
            **({"expect_licensing": sorted(self.lic.get(part["licensing_set"] or "", []))}
               if licensing else {}),
        }

    # ---- helpers the finders share -------------------------------------------------------
    def day(self, *xs, state="yes"):
        return next((on for on in _DAYS
                     if all(self.RD.in_force(x.get("when"), on) == state for x in xs)), None)

    def day_out(self, x):
        return next((on for on in _DAYS if self.RD.in_force(x.get("when"), on) == "no"), None)

    def first(self, mech, cands, check, licensing=False):
        seen = set()
        for item, part, on, fish, because in cands:
            if on is None or fish is None:
                continue
            key = (part["ruleset"], bool(part.get("anadromous_rainbow")), on, fish)
            if key in seen:
                continue
            seen.add(key)
            ans = self.answer(item, part, on, fish)
            if check(ans):
                return self.record(mech, item, part, on, fish, because, ans, licensing)
        return None

    def by_set(self, test):
        """(item, part, set members) for every set carried by a named water, `test`ed on its
        members; `test` returns an iterable of (on, fish, because)."""
        for s in self.sets:
            item, part = self.first_part[s]
            for on, fish, because in test(self.members[s]) or ():
                yield item, part, on, fish, because


def _speaking(ans) -> set:
    return {a["id"] for a in ans if a["state"] == "speaks"}


def _ids(ans) -> set:
    return {a["id"] for a in ans}


def cases(d: dict, bundle: Path) -> dict:
    from pipeline.deliver.bundle.rules import release_origins, same_statement
    K = _Cases(d, bundle)
    R, RD = K.R, K.RD
    rid = lambda k: f"{k[0]}::{k[1]}"                              # noqa: E731
    water = lambda k: k[0].startswith("r") and R[k]["_rank"] == 0  # noqa: E731
    zone = lambda k: k[0].startswith("z") and R[k]["_rank"] >= 2   # noqa: E731
    got: dict = {}

    def add(mech, case):
        if case is not None:
            got.setdefault(mech, []).append(case)

    # zone_only: no water row binds; the region's trout/char quota speaks for a rainbow
    def t(m):
        if any(k[0].startswith("r") for k in m):
            return
        z = [k for k in m if k[0].startswith("z") and k[1].endswith("trout_char_quota.r1")]
        if z:
            yield (7, 1), "RB", [rid(z[0])]
    add("zone_only", K.first("zone_only", K.by_set(t),
                             lambda a: any(i.startswith("z") and "trout_char_quota" in i
                                           for i in _speaking(a))))

    # quota_beside: same fish or group, a size bound or other difference, smaller at the water
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a) and not a.get("within") and m[w] == "reach"):
                continue
            for z in m:
                b = R[z]
                if zone(z) and RD.base_region(z[0]) and _keeps(b) and not b.get("within") \
                        and set(_leaves(a)) <= set(_leaves(b)) and not same_statement(a, b) \
                        and (a.get("period") or "daily") == (b.get("period") or "daily") \
                        and a["take"] < b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    beside = lambda w, z: lambda a: {w, z} <= _speaking(a)    # noqa: E731
    for c in K.by_set(t):
        case = K.first("quota_beside", [c], beside(*c[4]))
        if case:
            add("quota_beside", case)
            break

    # larger_replaces: a water row's lift of the zone's aggregate for the fish it names larger
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a) and m[w] == "reach"):
                continue
            for x in a.get("exempts") or []:
                z = (x["entry_id"], x["rule_id"])
                b = R.get(z)
                if z in m and b and zone(z) and _keeps(b) and not b.get("within") \
                        and a["take"] > b["take"] and not x.get("origin"):
                    fish = sorted(set(_leaves(a)) & set(x.get("species") or _leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("larger_replaces", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("larger_replaces", case)
            break

    # same_statement: the water's larger number replaces the zone's
    def t(m):
        for w in m:
            for z in m:
                a, b = R[w], R[z]
                if water(w) and zone(z) and _keeps(a) and _keeps(b) and same_statement(a, b) \
                        and not b.get("within") and a["take"] > b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("same_statement", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("same_statement", case)
            break

    # water_release: an outright release silencing a zone quota of another dimension
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and release_origins(a) == frozenset({"wild", "hatchery"})
                    and not _closure(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and b.get("dimension") != a.get("dimension"):
                    fish = sorted(set(_leaves(a)) & set(_leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_release", [c], lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("water_release", case)
            break

    # naming_beats_group / water_names_fish / within_clause_parent_level
    def t(m):
        for z in m:
            b = R[z]
            if not (zone(z) and b.get("type") == "retention_limit" and not b.get("within")):
                continue
            for w in m:
                a = R[w]
                if water(w) and _keeps(a) and (a["type"], a["dimension"]) == \
                        (b["type"], b["dimension"]):
                    for f in _leaves(a):
                        if RD.names_fish(b, f) and not RD.names_fish(a, f) \
                                and RD.speaks_for(b, f):
                            yield K.day(a, b), f, [rid(z), rid(w)]
                            break
    for c in K.by_set(t):
        z, w = c[4]
        case = K.first("naming_beats_group", [c],
                       lambda a: z in _speaking(a) and w not in _speaking(a))
        if case:
            add("naming_beats_group", case)
            break

    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _keeps(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _closure(b) is False and b.get("take") == 0 and \
                        (a["type"], a["dimension"]) == (b["type"], b["dimension"]):
                    for f in _leaves(a):
                        if RD.names_fish(a, f) and RD.names_fish(b, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_names_fish", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("water_names_fish", case)
            break

    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and release_origins(a) and not _closure(a)):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and b.get("within"):
                    for f in _leaves(b):
                        if RD.names_fish(b, f) and RD.speaks_for(a, f) \
                                and not RD.names_fish(a, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("within_clause_parent_level", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("within_clause_parent_level", case)
            break

    # water_closure_silences_zone: a water's dated closure over the zone's named quota
    def t(m):
        for w in m:
            a = R[w]
            if not (water(w) and _closure(a) and (a.get("when") or {}).get("dates")):
                continue
            for z in m:
                b = R[z]
                if zone(z) and _keeps(b) and not b.get("within"):
                    for f in _leaves(b):
                        if f != "CRA" and RD.names_fish(b, f) and RD.speaks_for(a, f):
                            yield K.day(a, b), f, [rid(w), rid(z)]
                            break
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("water_closure_silences_zone", [c],
                       lambda a: w in _speaking(a) and z not in _speaking(a))
        if case:
            add("water_closure_silences_zone", case)
            break

    # lifts: derived, printed over a species closure, blanket, equivalent, one fish, partly
    def lifts(test):
        def t(m):
            for w in m:
                if m[w] != "reach":
                    continue
                for x in R[w].get("exempts") or []:
                    z = (x["entry_id"], x["rule_id"])
                    if z in m and z in R and test(w, x, z):
                        yield w, x, z
        return t

    def full(x):
        return not any(x.get(k) for k in ("origin", "lengths", "when_targeting", "while"))

    def lift_cases(mech, test, fish_of, check):
        finder = lifts(test)
        for s in K.sets:
            item, part = K.first_part[s]
            for w, x, z in finder(K.members[s]):
                f = fish_of(w, x, z)
                if f is None:
                    continue
                on = K.day(R[w], R[z])
                case = K.first(mech, [(item, part, on, f, [rid(w), rid(z)])], check(w, x, z))
                if case:
                    return case
        return None

    def lifted_fish(w, x, z):
        f = sorted(set(x.get("species") or _leaves(R[z])) & set(_leaves(R[w]) or _leaves(R[z])))
        return f[0] if f else None

    gone = lambda w, x, z: lambda a: rid(z) not in _ids(a)     # noqa: E731
    add("derived_lift", lift_cases(
        "derived_lift", lambda w, x, z: x.get("basis") and full(x) and water(w),
        lifted_fish, gone))
    add("printed_lift_reopens_closure", lift_cases(
        "printed_lift_reopens_closure",
        lambda w, x, z: not x.get("basis") and full(x) and water(w) and _closure(R[z])
        and zone(z) and bool(R[z].get("species")) and not x.get("equivalent")
        and set(R[z]["species"]).isdisjoint({"ALL_GAME_FISH", "ALL_FIN_FISH"}),
        lifted_fish, gone))
    add("blanket_closure_exempt", lift_cases(
        "blanket_closure_exempt",
        lambda w, x, z: full(x) and not x.get("equivalent") and water(w)
        and R[w].get("dimension") == "lift" and _closure(R[z]) and bool(R[z].get("water"))
        and set(R[z].get("species") or []) <= {"ALL_GAME_FISH", "ALL_FIN_FISH"},
        lambda w, x, z: "RB", gone))
    add("equivalent_lift", lift_cases(
        "equivalent_lift", lambda w, x, z: bool(x.get("equivalent")) and full(x),
        lambda w, x, z: "RB", gone))
    add("counted_apart", lift_cases(
        "counted_apart",
        lambda w, x, z: zone(w) and w[0] == z[0] and bool(x.get("species")) and full(x)
        and _keeps(R[w]),
        lambda w, x, z: x["species"][0],
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)))

    def other_fish(w, x, z):
        f = sorted(set(_leaves(R[z])) - set(x["species"]))
        return f[0] if x.get("species") and f else None
    add("lift_for_one_fish", lift_cases(
        "lift_for_one_fish", lambda w, x, z: water(w) and bool(x.get("species")) and full(x),
        other_fish,
        lambda w, x, z: lambda a: rid(z) in _speaking(a)))
    add("partly_lifted", lift_cases(
        "partly_lifted", lambda w, x, z: not full(x), lifted_fish,
        lambda w, x, z: lambda a: any(i["id"] == rid(z) and i.get("partly_lifted") for i in a)))

    # closure_prints_its_exemptions: a region's species closure a water row names, not lifted
    def t(m):
        for z in m:
            b = R[z]
            if not (zone(z) and _closure(b) and b.get("species")
                    and set(b["species"]).isdisjoint(set(read_aggregates()))):
                continue
            for w in m:
                a = R[w]
                if water(w) and _keeps(a) and not any(
                        (x["entry_id"], x["rule_id"]) == z for x in a.get("exempts") or []):
                    for f in _leaves(a):
                        if RD.names_fish(a, f) and RD.names_fish(b, f):
                            yield K.day(a, b), f, [rid(z), rid(w)]
                            break
    for c in K.by_set(t):
        z, w = c[4]
        case = K.first("closure_prints_its_exemptions", [c], lambda a: z in _speaking(a))
        if case:
            add("closure_prints_its_exemptions", case)
            break

    # two_regions_lake: two regions' tables on one lake, the stricter displacing the other. Never a
    # lake the book divides into parts (Williston: its Zone A and Zone B halves each take one
    # table) — the parent's own section there is the sliver the parts leave, not the lake.
    divided = {w["part_of"] for w in K.d["waters"].values() if w.get("part_of")}
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
        if len(bases) < 2 or K.d["waters"][item]["kind"] != "lake" or item in divided:
            continue
        found = None
        for f in ("RB", "LT", "KO", "DV", "EB", "BB", "WP", "NP", "MW"):
            ans = K.answer(item, part, (7, 1), f)
            said = _ids(ans)
            spoken = [tuple(i.split("::")) for i in _speaking(ans)]
            # the other table's rule that displaced it: a retention rule about this fish — never
            # a water closure silencing both tables (Kinbasket's undrawn "No Fishing within 200 m
            # of Bush-Sullivan Road Bridge" is held on the whole lake)
            if any(k in R and water(k) and _closure(R[k]) for k in spoken):
                continue
            other = lambda k: [o for o in spoken if o in R and zone(o)  # noqa: E731
                               and RD.base_region(o[0]) not in (None, RD.base_region(k[0]))
                               and R[o].get("type") == "retention_limit"
                               and not R[o].get("standing") and RD.speaks_for(R[o], f)]
            dropped = [k for k in m if zone(k) and RD.base_region(k[0])
                       and R[k].get("type") == "retention_limit"
                       and RD.speaks_for(R[k], f) and RD.in_force(R[k].get("when"), (7, 1)) == "yes"
                       and not R[k].get("standing") and rid(k) not in said and other(k)]
            if dropped:
                found = K.record("two_regions_lake", item, part, (7, 1), f,
                                 [rid(k) for k in dropped]
                                 + [rid(o) for k in dropped for o in other(k)], ans)
                break
        if found:
            add("two_regions_lake", found)
            break

    # home_region_differs: a stream piece whose zone rules are another region's than its row's
    fam = lambda r: r[:1] if r and r[:1] == "7" else r            # noqa: E731
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        if K.d["waters"][item]["kind"] != "stream":
            continue
        bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
        rows = {k[0].split(":", 1)[0][1:] for k in m if water(k) and m[k] == "reach"}
        if len(bases) != 1 or not rows or any(fam(r) == fam(next(iter(bases))) for r in rows):
            continue
        w = sorted(k for k in m if water(k) and m[k] == "reach")[0]
        z = sorted(k for k in m if zone(k) and k[1].endswith("trout_char_quota.r1"))
        case = K.first("home_region_differs", [(item, part, (7, 1), "RB", [rid(w)] + [rid(k) for k in z])],
                       lambda a: any(i.startswith("z") for i in _speaking(a)))
        if case:
            add("home_region_differs", case)
            break

    # pointer: a pointer-only row's water carries its target's rules
    for eid, e in sorted(K.d["entries"].items()):
        if not e.get("see") or e["rules"] or e["licensing"]:
            continue
        targets = {t for s_ in e["see"] for t in s_.get("entry_ids") or []}
        hit = None
        for it in e["matched"]:
            for p in (K.d["waters"].get(it) or {}).get("parts", []):
                if p["ruleset"] and any(k[0] in targets for k in K.members[p["ruleset"]]):
                    hit = (it, p)
                    break
            if hit:
                break
        if hit:
            case = K.first("pointer", [(hit[0], hit[1], (7, 1), "RB", [eid] + sorted(targets))],
                           lambda a: any(i.split("::")[0] in targets for i in _ids(a)))
            if case:
                add("pointer", case)
                break

    # steelhead_water: a rainbow rule over 50 cm only speaks for no rainbow
    for it, p in K.parts:
        if not (p.get("anadromous_rainbow") and p["ruleset"]):
            continue
        over = [k for k in K.members[p["ruleset"]] if RD.speaks_for(R[k], "RB")
                and R[k].get("lengths") and all((b.get("min_cm") or 0) >= 50
                                                for b in R[k]["lengths"])]
        if over:
            case = K.first("steelhead_water", [(it, p, (7, 15), "RB", [rid(k) for k in over])],
                           lambda a: not ({rid(k) for k in over} & _ids(a)))
            if case:
                add("steelhead_water", case)
                break

    # steelhead: known | possible streams, a lake with none, an own-row steelhead lake
    def steelhead_case(mech, part_test, check):
        for it, p in K.parts:
            w = K.d["waters"][it]
            if p["ruleset"] and part_test(w, p, set(rid(k) for k in K.members[p["ruleset"]])):
                why = sorted(rid(k) for k in K.members[p["ruleset"]] if is_steelhead_rule(
                    K.d["rules"][rid(k)])) or sorted(w["entries"])
                case = K.first(mech, [(it, p, (9, 1), "ST", why)], check, licensing=True)
                if case:
                    add(mech, case)
                    return
    has_province = lambda a: any(i.startswith(PROVINCE_STEELHEAD + "::")    # noqa: E731
                                 for i in _speaking(a))
    steelhead_case("steelhead_known", lambda w, p, m: w["kind"] == "stream"
                   and p.get("steelhead") == "known" and p.get("anadromous_rainbow")
                   and bool(set(w["entries"]) & set(w.get("steelhead_source") or ())),
                   has_province)
    steelhead_case("steelhead_possible", lambda w, p, m: w["kind"] == "stream"
                   and p.get("steelhead") == "possible" and not p.get("anadromous_rainbow"),
                   has_province)
    steelhead_case("lake_no_steelhead", lambda w, p, m: w["kind"] == "lake" and w["entries"]
                   and not p.get("steelhead") and _in_steelhead_region(m)
                   and not any(is_steelhead_rule(K.d["rules"][i]) for i in m),
                   lambda a: not any(is_steelhead_rule(K.d["rules"][i]) for i in _ids(a)
                                     if i in K.d["rules"]))
    steelhead_case("own_row_steelhead_lake", lambda w, p, m: w["kind"] == "lake"
                   and p.get("steelhead") == "known", has_province)

    # dated rules, in and out of force (the same water, fish and part)
    def t(m):
        for w in m:
            a = R[w]
            wh = a.get("when") or {}
            if water(w) and _keeps(a) and wh.get("dates") and not (
                    wh.get("hours") or wh.get("weekdays") or wh.get("unparsed")) and _leaves(a):
                yield K.day(a), _leaves(a)[0], [rid(w)]
    for c in K.by_set(t):
        (w,) = c[4]
        inside = K.first("dated_in_force", [c], lambda a: w in _speaking(a))
        e_, _, r_ = w.partition("::")
        out = K.day_out(R[(e_, r_)])
        outside = inside and K.first("dated_out_of_force", [c[:2] + (out,) + c[3:]],
                                     lambda a: w not in _ids(a))
        if inside and outside:
            add("dated_in_force", inside)
            add("dated_out_of_force", outside)
            break

    # semicolon_dated_clause: "A; B, <dates>" — A all year
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        hit = None
        for a_ in m:
            for b_ in m:
                a, b = R[a_], R[b_]
                if a_[0] != b_[0] or a_ == b_ or a.get("when") or \
                        not (b.get("when") or {}).get("dates"):
                    continue
                text = K.d["entries"][a_[0]]["printed"] or ""
                if f"{a['verbatim']}; {b['verbatim'][:12]}" in text:
                    hit = (a_, b_)
                    break
            if hit:
                break
        if hit:
            a_, b_ = hit
            case = K.first("semicolon_dated_clause",
                           [(item, part, K.day_out(R[b_]), "RB", [rid(a_), rid(b_)])],
                           lambda a: rid(a_) in _ids(a) and rid(b_) not in _ids(a))
            if case:
                add("semicolon_dated_clause", case)
                break

    # one rule of a kind, in the answer on a day it is in force
    def single(mech, test, check=None, state="yes"):
        for s in K.sets:
            item, part = K.first_part[s]
            for k in sorted(K.members[s]):
                x = R[k]
                if not test(k, x) or (water(k) and K.members[s][k] != "reach"):
                    continue
                f = (_leaves(x) or ["RB"])[0]
                if x.get("when_targeting"):
                    f = C.expand_species(list(x["when_targeting"]))[0]
                on = K.day(x, state=state)
                ok = check(k) if check else (lambda a, k=k: rid(k) in _ids(a))
                case = K.first(mech, [(item, part, on, f, [rid(k)])], ok)
                if case:
                    add(mech, case)
                    return

    part_day = lambda x: bool((x.get("when") or {}).get("hours") or (x.get("when") or {})
                              .get("weekdays") or (x.get("when") or {}).get("unparsed"))
    single("part_day_beside", lambda k, x: part_day(x),
           lambda k: lambda a: any(i["id"] == rid(k) and i["state"] == "beside" for i in a),
           state="part")
    # suspended_while: no rule carries it today; a licensing record does ("Steelhead Stamp not
    # required until reopened to steelhead fishing") — the case asks on a day its closure speaks.
    for it, p in K.parts:
        if not (p["ruleset"] and p["licensing_set"]):
            continue
        hit = None
        for i in K.lic.get(p["licensing_set"], []):
            x = K.d["licensing"].get(i)
            for sw in (x or {}).get("fields", {}).get("suspended_while") or []:
                k = (x["entry_id"], sw["rule_id"])
                if k in K.members[p["ruleset"]]:
                    hit = (i, k)
        if hit:
            i, k = hit
            case = K.first("suspended_while", [(it, p, K.day(R[k]), "ST", [i, rid(k)])],
                           lambda a: rid(k) in _speaking(a), licensing=True)
            if case:
                add("suspended_while", case)
                break
    single("size_band", lambda k, x: water(k) and _keeps(x) and any(
        b.get("min_cm") is not None and b.get("max_cm") is not None and b.get("take") != 0
        for b in x.get("lengths") or []))
    single("origin", lambda k, x: water(k) and bool(x.get("origin")) and _keeps(x))
    single("while_or_targeting", lambda k, x: water(k) and bool(x.get("while")
                                                                 or x.get("when_targeting")))
    single("annual_clock", lambda k, x: x.get("period") == "annual")
    single("superior_authority", lambda k, x: R[k]["_rank"] < 0,
           lambda k: lambda a: rid(k) in _speaking(a))
    single("standing", lambda k, x: bool(x.get("standing")))
    single("angler_closure", lambda k, x: x.get("type") == "angler_closure")
    for s in K.sets:
        item, part = K.first_part[s]
        m = K.members[s]
        trib = [k for k in sorted(m) if m[k] == "trib" and water(k)
                and R[k].get("type") == "retention_limit"]
        if trib:
            k = trib[0]
            case = K.first("tributary_walk", [(item, part, K.day(R[k]),
                                               (_leaves(R[k]) or ["RB"])[0], [rid(k)])],
                           lambda a: rid(k) in _speaking(a))
            if case:
                add("tributary_walk", case)
                break
    single("watershed_part", lambda k, x: any(e.get("watershed") for e in x.get("extents") or []))
    single("undrawn_part", lambda k, x: bool(x.get("undrawn_part")),
           lambda k: lambda a: any(i["id"] == rid(k) and i["state"] == "not_yet_mapped"
                                   for i in a))

    def gear_only(m):
        own = [k for k in m if water(k) and m[k] == "reach"]
        return own and all(R[k]["type"] in ("tackle_restriction", "bait_restriction",
                                            "method_rule") for k in own)
    for s in K.sets:
        item, part = K.first_part[s]
        if gear_only(K.members[s]):
            own = sorted(k for k in K.members[s] if water(k))
            case = K.first("gear_only", [(item, part, (7, 1), "RB", [rid(k) for k in own])],
                           lambda a: any(rid(k) in _ids(a) for k in own))
            if case:
                add("gear_only", case)
                break

    # ---- sample waters added 2026-09-26 ----------------------------------------------------
    # clause_only_lift: a water number lifting only a clause; the fish still counts toward the
    # parent aggregate, which speaks
    def parent(z):
        return (z[0], R[z]["within"]) if R[z].get("within") else None
    add("clause_only_lift", lift_cases(
        "clause_only_lift",
        lambda w, x, z: water(w) and full(x) and not x.get("when") and zone(z)
        and parent(z) in R and not any((y["entry_id"], y["rule_id"]) == parent(z)
                                       for y in R[w].get("exempts") or []),
        lifted_fish,
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)
        and rid(parent(z)) in _speaking(a)))

    # dated_lift: on a day the lifter's dates exclude, the lifted rule speaks again
    def dated_lift_cases():
        for s in K.sets:
            item, part = K.first_part[s]
            m = K.members[s]
            for w in sorted(m):
                if m[w] != "reach" or not water(w):
                    continue
                for x in R[w].get("exempts") or []:
                    z = (x["entry_id"], x["rule_id"])
                    if z not in m or not x.get("when") or x.get("origin") or x.get("lengths"):
                        continue
                    f = lifted_fish(w, x, z)
                    for on in _DAYS:
                        if f and RD.in_force(x["when"], on) == "no" \
                                and RD.in_force(R[z].get("when"), on) == "yes":
                            yield item, part, on, f, [rid(w), rid(z)]
    for c in dated_lift_cases():
        w, z = c[4]
        case = K.first("dated_lift", [c], lambda a: z in _speaking(a) and w not in _ids(a))
        if case:
            add("dated_lift", case)
            break

    # same_statement_smaller: the water's smaller number replaces the zone's same statement
    def t(m):
        for w in m:
            for z in m:
                a, b = R[w], R[z]
                if water(w) and zone(z) and _keeps(a) and _keeps(b) and same_statement(a, b) \
                        and not b.get("within") and a["take"] < b["take"] and _leaves(a):
                    yield K.day(a, b), _leaves(a)[0], [rid(w), rid(z)]
    for c in K.by_set(t):
        w, z = c[4]
        case = K.first("same_statement_smaller", [c],
                       lambda a: w in _speaking(a) and z not in _ids(a))
        if case:
            add("same_statement_smaller", case)
            break

    # streams_only_zone_clause: "2 from streams" speaking on a stream
    for s in K.sets:
        item, part = K.first_part[s]
        if K.d["waters"][item]["kind"] != "stream":
            continue
        m = K.members[s]
        hit = [k for k in sorted(m) if zone(k) and R[k].get("water") == "stream"
               and R[k].get("within") and _keeps(R[k]) and not R[k].get("origin")
               and _leaves(R[k])]
        if not hit:
            continue
        k = hit[0]
        case = K.first("streams_only_zone_clause",
                       [(item, part, K.day(R[k]), _leaves(R[k])[0], [rid(k)])],
                       lambda a, k=k: rid(k) in _speaking(a))
        if case:
            add("streams_only_zone_clause", case)
            break

    # two_regions_closure / two_regions_release: what displaced the other region's rule
    shut = lambda x: x.get("take") == 0 and not x.get("may_target")  # noqa: E731
    released = lambda x: (x.get("take") == 0 and x.get("may_target")  # noqa: E731
                          and not x.get("lengths"))
    for mech, how in (("two_regions_closure", shut), ("two_regions_release", released)):
        found = None
        for s in K.sets:
            item, part = K.first_part[s]
            m = K.members[s]
            bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
            if len(bases) < 2 or K.d["waters"][item]["kind"] != "lake" or item in divided:
                continue
            for f in ("RB", "LT", "KO", "DV", "EB", "BB", "WP", "NP", "MW", "GR", "WSG"):
                for on in _DAYS:
                    ans = K.answer(item, part, on, f)
                    spoken = [tuple(i.split("::")) for i in _speaking(ans)]
                    said = _ids(ans)
                    for k in m:
                        if not (zone(k) and RD.base_region(k[0]) and rid(k) not in said
                                and R[k].get("type") == "retention_limit"
                                and RD.speaks_for(R[k], f) and not R[k].get("standing")
                                and RD.in_force(R[k].get("when"), on) == "yes"
                                and (mech == "two_regions_closure" or _keeps(R[k]))
                                and not shut(R[k])):
                            continue
                        by = [o for o in spoken if o in R and zone(o)
                              and RD.base_region(o[0]) not in (None, RD.base_region(k[0]))
                              and how(R[o]) and RD.speaks_for(R[o], f)
                              and RD.stricter(R[o], R[k])]
                        if by:
                            found = K.record(mech, item, part, on, f,
                                             [rid(k)] + [rid(o) for o in by], ans)
                            break
                    if found:
                        break
                if found:
                    break
            if found:
                break
        add(mech, found)

    # keep_beside_release: a clause keeping some of a fish beside a release of the others
    def t(m):
        for k1 in m:
            a = R[k1]
            if not (_keeps(a) and a.get("origin") and a.get("take")):
                continue
            for k2 in m:
                b = R[k2]
                if k2 != k1 and b.get("take") == 0 and b.get("may_target") \
                        and not b.get("lengths") and b.get("origin") \
                        and b["origin"] != a["origin"]:
                    fish = sorted(set(_leaves(a)) & set(_leaves(b)))
                    if fish:
                        yield K.day(a, b), fish[0], [rid(k1), rid(k2)]
    for c in K.by_set(t):
        k1, k2 = c[4]
        case = K.first("keep_beside_release", [c],
                       lambda a: k1 in _speaking(a) and k2 in _speaking(a))
        if case:
            add("keep_beside_release", case)
            break

    # undrawn_closure: an undrawn closure shown as a note while the zone speaks (Kinbasket first)
    order = sorted(K.sets, key=lambda s_: (not any("kinbasket" in k[0]
                                                    for k in K.members[s_]), int(s_)))
    for s in order:
        item, part = K.first_part[s]
        m = K.members[s]
        und = [k for k in sorted(m) if R[k].get("undrawn_part") and _closure(R[k])]
        zq = [k for k in sorted(m) if zone(k) and _keeps(R[k]) and not R[k].get("within")
              and R[k].get("species")]
        if not (und and zq):
            continue
        k, z = und[0], zq[0]
        f = next((x for x in _leaves(R[z]) if RD.speaks_for(R[k], x)), None)
        on = K.day(R[k], R[z])
        case = K.first("undrawn_closure", [(item, part, on, f, [rid(k), rid(z)])],
                       lambda a, k=k: any(i["id"] == rid(k) and i["state"] == "not_yet_mapped"
                                          for i in a)
                       and any(i.startswith("z") for i in _speaking(a)))
        if case:
            add("undrawn_closure", case)
            break

    # size_clause_caution: a lift of a region size clause, carrying its caution
    add("size_clause_caution", lift_cases(
        "size_clause_caution",
        # only a row printing "(any size)" carries the caution (user ruling 2026-09-28)
        lambda w, x, z: water(w) and bool(x.get("caution")) and not x.get("origin")
        and "any size" in R[w]["verbatim"],
        lifted_fish,
        lambda w, x, z: lambda a: rid(w) in _speaking(a) and rid(z) not in _ids(a)))

    # named sample waters: Kootenay Lake's rainbow 10, the Dean beside, a straddling lake
    def named(mech, entry_prefix, fish, check, rule_test=lambda x: True, on=None):
        eids = sorted(e for e in K.d["entries"] if e.startswith(entry_prefix))
        for eid in eids:
            for it in K.d["entries"][eid]["matched"]:
                for p_ in (K.d["waters"].get(it) or {}).get("parts", []):
                    m = K.members.get(p_["ruleset"] or "", {})
                    mine = [k for k in sorted(m) if k[0] == eid and m[k] == "reach"
                            and rule_test(R[k]) and RD.speaks_for(R[k], fish)]
                    if not mine:
                        continue
                    day = on or K.day(*(R[k] for k in mine[:1])) or (7, 1)
                    zs = [k for k in sorted(m) if zone(k) and k[1].endswith("trout_char_quota.r1")]
                    case = K.first(mech, [(it, p_, day, fish, [rid(k) for k in mine[:1] + zs])],
                                   check(mine[0], zs))
                    if case:
                        return case
        return None
    add("kootenay_rainbow_10", named(
        "kootenay_rainbow_10", "r4:kootenay_lake_main_body", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a) and not (set(map(rid, zs)) & _ids(a)),
        lambda x: x.get("take") == 10))
    add("dean_beside", named(
        "dean_beside", "r5:dean_river@", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a) and bool(zs)
        and set(map(rid, zs)) <= _speaking(a),
        lambda x: _keeps(x) and "TROUT_CHAR" in (x.get("species") or [])))
    add("trout_word_same_statement", named(
        "trout_word_same_statement", "r1:amor_lake@", "RB",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z1:trout_quota::trout_quota.r1" not in _ids(a),
        lambda x: _keeps(x) and "TROUT_CHAR" in (x.get("species") or [])))
    add("dated_zone_release_stands", named(
        "dated_zone_release_stands", "r3:shuswap_lake_see_maps", "LT",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z3:trout_char_quota::trout_char_quota.r7" in _speaking(a),
        lambda x: _keeps(x) and "CHAR" in (x.get("species") or []) and x.get("take") == 1,
        on=(11, 1)))
    add("water_dates_override", named(
        "water_dates_override", "r6:cheslatta_lake", "LT",
        lambda w, zs: lambda a: rid(w) in _speaking(a)
        and "z6:trout_char_quota::trout_char_quota.r8" not in _speaking(a),
        lambda x: _keeps(x) and x.get("species") == ["LT"] and x.get("take") == 3
        and not x.get("within"), on=(11, 15)))
    add("one_side_beside", named(
        "one_side_beside", "r6:kitimat_river_angling", "RB",
        lambda w, zs: lambda a: any(i["id"] == rid(w) and i["state"] == "beside" for i in a)
        and any(i.startswith("r6:kitimat") for i in _speaking(a)),
        lambda x: bool(x.get("side")), on=(7, 1)))
    straddle = None
    for name in ("Ahbau Lake", "Mara Lake"):
        for it, w in sorted(K.d["waters"].items()):
            if w["name"] != name or w["kind"] != "lake":
                continue
            for p_ in w["parts"]:
                m = K.members.get(p_["ruleset"] or "", {})
                bases = {RD.base_region(k[0]) for k in m if zone(k)} - {None}
                if len(bases) < 2:
                    continue
                zs = sorted(k for k in m if zone(k) and k[1].endswith("trout_char_quota.r1"))
                straddle = K.first("straddling_named_lake",
                                   [(it, p_, (7, 1), "RB", [rid(k) for k in zs])],
                                   lambda a: len({i.split(":")[0] for i in _ids(a)
                                                  if i.startswith("z")
                                                  and not i.startswith("zp:")}) == 2)
                if straddle:
                    break
            if straddle:
                break
        if straddle:
            break
    add("straddling_named_lake", straddle)

    # licensing, the province's exceptions, and water outside B.C.
    L = K.d["licensing"]

    def lic_case(mech, test, part_test=lambda p: True):
        for it, p in K.parts:
            if p["ruleset"] and p["licensing_set"] and part_test(p) and any(
                    test(L[i]) for i in K.lic.get(p["licensing_set"], []) if i in L):
                why = [i for i in K.lic[p["licensing_set"]] if i in L and test(L[i])]
                case = K.first(mech, [(it, p, (7, 1), "RB", why)], lambda a: True,
                               licensing=True)
                if case:
                    add(mech, case)
                    return
    lic_case("licensing_classified", lambda x: x["kind"] == "designation"
             and bool(x["fields"].get("classified")))
    lic_case("licensing_stamp", lambda x: "stamp" in json.dumps(x["fields"]))
    lic_case("province_except", lambda x: True, lambda p: bool(p.get("province_except")))
    for it, w in sorted(K.d["waters"].items()):
        if w["outside_bc"] and it in K.outside:
            p = next((p for p in w["parts"] if p["ruleset"] is None
                      and p["licensing_set"] is None), None)
            if p:
                case = K.first("outside_bc", [(it, p, (7, 1), "RB", [])], lambda a: a == [],
                               licensing=True)
                if case:
                    add("outside_bc", case)
                    break

    return {
        "reading": "SAMPLE WATERS for the UI to look at and build against while it is being "
                   "built out — not a test oracle. One or more real waters per mechanism, most "
                   "found in the data by a test (a few named: Kootenay Lake, the Dean, "
                   "Ahbau/Mara), each with a plain `what_to_show` line and the answer the "
                   "reference semantics give today "
                   "(`pipeline.deliver.bundle.read.effective_rules`). Read one as: on the "
                   "water `water.item_id`, for the part whose sections "
                   "carry `ruleset` (and `licensing_set`; `anadromous_rainbow` where a rainbow "
                   "over 50 cm is a steelhead; `steelhead`, known | possible, where steelhead "
                   "rules apply — the part's own), on `date` (MM-DD, any year), for `fish` (a leaf "
                   "code), the rules in play are `expect` — each `speaks`, stands "
                   "`beside` (in force only some hours or days, or of unreadable season), is "
                   "`shown` (standing, information), or is `not_yet_mapped` (a part nobody has "
                   "drawn: show it at the top, it decides nothing); `partly_lifted` where a lift "
                   "holds only for some anglers or fish. `what_to_show` is one plain line for "
                   "the page builder. `because` names the records that make it this "
                   "mechanism. A licensing case also carries `expect_licensing`: every record "
                   "its licensing set holds. The ids are the export's own; section handles "
                   "never leave the bundle.",
        "mechanisms": CASE_MECHANISMS,
        "missing": sorted(set(CASE_MECHANISMS) - set(got)),
        "cases": [c for mech in CASE_MECHANISMS for c in got.get(mech, [])],
    }


def read_aggregates():
    from pipeline.deliver.bundle.rules import AGGREGATE_GROUPS
    return AGGREGATE_GROUPS


# --------------------------------------------------------------------------------------------
# Species, the field dictionary, the index
# --------------------------------------------------------------------------------------------

OPEN_GROUPS = C.OPEN_SUBJECTS


def _name(code: str) -> str:
    return C._SPECIES_WORDS.get(code) or code


def species_table() -> dict:
    """THE BOOK'S LIST (p.86) — every fish a rule may name, under the book's headings — and the
    groups and open subjects a rule may name instead. Nothing else: no official-table sub-species,
    no fish the book does not list."""
    members = {g: list(C.expand_species([g])) if g not in OPEN_GROUPS else []
               for g in C.SPECIES_GROUPS}
    in_group = defaultdict(list)
    for g, ms in members.items():
        for c in ms:
            in_group[c].append(g)
    fish = {}
    for fam, codes in C.BOOK_FAMILIES.items():
        for code in codes:
            d = {"name": _name(code), "family": fam,
                 "scientific": C.SCIENTIFIC_NAMES.get(code),
                 "groups": sorted(in_group[code])}
            if code == "DV":
                d["includes"] = "bull trout — '*Any bull trout that you catch and keep must be " \
                                "counted as part of your Dolly Varden quota' (p.86)"
            if code in C.DEFINITIONAL_SIZE:
                d["definitional_size"] = dict(C.DEFINITIONAL_SIZE[code])
                if code == "ST":
                    # WHERE the definition holds in this file (user ruling 2026-10-01)
                    d["definitional_size"]["marked_by"] = (
                        "waters[*].parts[*].anadromous_rainbow — the KNOWN steelhead streams "
                        "(`steelhead: known`), never a lake or a \"possible\" stream "
                        "(`guide.ladder.steelhead_definition`)")
            fish[code] = d
    groups = {g: ({"name": _name(g), "members": members[g]} if g not in OPEN_GROUPS else
                  {"name": _name(g), "members": [], "open": True})
              for g in sorted(C.SPECIES_GROUPS)}
    return {"source": "fishing_synopsis.pdf 2025-2027, p.86: 'Freshwater game fish are defined "
                      "as follows'",
            "families": {f: {"name": C._FAMILY_WORDS[f], "members": list(cs)}
                         for f, cs in C.BOOK_FAMILIES.items()},
            "fish": fish, "groups": groups,
            "trout_includes_char": "p.86: 'all regulations that apply to trout (as a group) "
                                   "also apply to char unless char are specifically excluded' "
                                   "— the trout group is TROUT_CHAR; a bare 'trout' line of a "
                                   "row or zone table that mentions char apart is TROUT_CHAR "
                                   "with species_except CHAR (user ruling 2026-09-28)",
            "trout_note": "Trout includes char (Dolly Varden/bull trout, lake trout, brook "
                          "trout) unless the regulation lists char separately.",
            # A SALMON THE BOOK NAMES — not a game fish, not in `fish` (user ruling 2026-09-28)
            "salmon": {c: {"name": _name(c), "group": g, "game_fish": False}
                       for c, g in sorted(C.SALMON_FISH.items())},
            "salmon_note": "Chinook is a named fish in the SALMON group — not a game fish and "
                           "not on the book's game-fish list (p.86); SALMON stays an open group "
                           "(it speaks for every salmon, named or not). The synopsis says little "
                           "about salmon ('record your retention of adult chinook salmon', the "
                           "salmon stamp, 'no spear fishing of Pacific salmon'): salmon "
                           "regulations will come later, with the DFO salmon implementation.",
            "refused": dict(sorted(C.REFUSED_SPECIES.items()))}


def field_dictionary(d: dict) -> dict:
    """Exactly the keys the records carry, each explained. Unexplained or unknown keys are
    reported by `problems`, never silently listed."""
    rule_keys = sorted({k for x in d["rules"].values() for k in x["fields"]})
    lic_keys = {k: sorted({f for x in d["licensing"].values() if x["kind"] == k
                           for f in x["fields"]}) for k in _licensing_models()}
    shipped = set(rule_keys)
    model = set(_fields(C.CatalogueRule)) - {"rule_id", "type", "verbatim"}
    return {
        "file": FILE_TEXT,
        "rule": {k: RECORD_TEXT[k] for k in RECORD_TEXT},
        "rule.fields": {k: RULE_FIELD_TEXT.get(k) for k in rule_keys},
        "rule.provenance": PROVENANCE_TEXT,
        "rule.fields.gear[]": {k: CLAUSE_TEXT.get(k) for k in _fields(C.GearClause)},
        "rule.fields.when": {k: WHEN_TEXT.get(k) for k in _fields(C.When)},
        "rule.fields.lengths[]": LENGTH_TEXT,
        "rule.fields.exempts[]": dict(LIFT_TEXT),
        "rule.fields.closed_to": {k: WHO_TEXT.get(k) for k in _fields(C.Who)},
        "model_rule_fields_not_shipped": sorted(model - shipped),
        "licensing": LICENSING_RECORD_TEXT,
        "licensing.fields": {k: {f: LICENSING_FIELD_TEXT.get(f) for f in fs}
                             for k, fs in lic_keys.items()},
        "licensing.provenance": {k: PROVENANCE_TEXT[k] for k in ("entry_name", "uncertain",
                                                                   "why")},
        "licensing.period": PERIOD_TEXT,
        "entry": ENTRY_TEXT,
        "water": WATER_TEXT,
        "water.tidal": TIDAL_TEXT,
        "water.parts[]": WATER_PART_TEXT,
        "water.parts[].runs[]": RUN_TEXT,
        "run ends (from / to)": END_TEXT,
        "splits": SPLIT_TEXT,
        "rulesets{} / licensing_sets{}": SET_TEXT,
    }


#: Where `dictionary_gaps` looks: (what it is called in the report, the dictionary section that
#: must explain it, a function from the doc to the dicts whose KEYS are the fields).
_DICTIONARY_SCOPES = (
    ("top-level key", ("file",), lambda doc: [doc]),
    ("water field", ("water",), lambda doc: doc["waters"].values()),
    ("water.tidal field", ("water.tidal",),
     lambda doc: [w["tidal"] for w in doc["waters"].values() if "tidal" in w]),
    ("part field", ("water.parts[]",),
     lambda doc: [p for w in doc["waters"].values() for p in w["parts"]]),
    ("run field", ("water.parts[].runs[]",),
     lambda doc: [r for w in doc["waters"].values() for p in w["parts"] for r in p["runs"]]),
    ("rule key", ("rule",), lambda doc: doc["rules"].values()),
    ("rule field", ("rule.fields",), lambda doc: [x["fields"] for x in doc["rules"].values()]),
    ("rule provenance key", ("rule.provenance",),
     lambda doc: [x["provenance"] for x in doc["rules"].values()]),
    ("licensing key", ("licensing",), lambda doc: doc["licensing"].values()),
    ("licensing provenance key", ("licensing.provenance",),
     lambda doc: [x["provenance"] for x in doc["licensing"].values()]),
    ("licensing period key", ("licensing.period",),
     lambda doc: [x["period"] for x in doc["licensing"].values() if x.get("period")]),
    ("entry field", ("entry",), lambda doc: doc["entries"].values()),
    ("set key", ("rulesets{} / licensing_sets{}",),
     lambda doc: list(doc["rulesets"].values()) + list(doc["licensing_sets"].values())),
    ("split field", ("splits",), lambda doc: doc["splits"].values()),
)


def dictionary_gaps(doc: dict) -> list[str]:
    """EVERY FIELD IN THE DATA IS DESCRIBED. A top-level key, or a key of a water, a part, a run, a
    rule (its record, `fields`, `provenance`), a licensing record (its record, `fields` by kind,
    `provenance`, `period`), an entry, a set or a split that the file carries but `field_dictionary`
    does not explain in words (a missing or empty entry) is refused."""
    fd, out = doc["field_dictionary"], []
    for what, (section,), records in _DICTIONARY_SCOPES:
        words = fd.get(section) or {}
        seen = set()
        for r in records(doc):
            seen |= set(r)
        out += [f"field_dictionary.{section} does not explain the {what} {k!r}"
                for k in sorted(seen) if not words.get(k)]
    lic = fd.get("licensing.fields") or {}
    for i, x in doc["licensing"].items():
        for k in x["fields"]:
            if not (lic.get(x["kind"]) or {}).get(k):
                out.append(f"field_dictionary.licensing.fields.{x['kind']} does not explain "
                           f"the licensing field {k!r}")
    return sorted(set(out))


def index(d: dict) -> dict:
    by_type, by_family, by_kind = defaultdict(list), defaultdict(list), defaultdict(list)
    for x in d["rules"].values():
        by_type[x["type"]].append(x["id"])
        by_family[x["family"]].append(x["id"])
    for x in d["licensing"].values():
        by_kind[x["kind"]].append(x["id"])
    return {"rules_by_type": dict(sorted(by_type.items())),
            "rules_by_family": dict(sorted(by_family.items())),
            "licensing_by_kind": dict(sorted(by_kind.items()))}


def build(bundle: Path = BUNDLE) -> dict:
    d = read(bundle)
    counts = {
        "entries": len(d["entries"]),
        "rules": len(d["rules"]),
        "licensing": len(d["licensing"]),
        "licensing_by_kind": dict(sorted(Counter(x["kind"] for x in d["licensing"].values())
                                         .items())),
        "licences": len(d["licences"]),
        "rulesets": len(d["rulesets"]),
        "licensing_sets": len(d["licensing_sets"]),
        "waters": len(d["waters"]),
        "splits": len(d["splits"]),
        "sections": d["sections"],
    }
    doc = {
        "about": {
            "what": "Every regulation record in the bundle, with a guide to reading it. "
                    "Generated by pipeline/tools/export_ui_rules.py; nothing is settled.",
            "bundle": {k: d["meta"].get(k) for k in ("version", "build", "reach_run",
                                                      "reach_digest", "section_handles")},
            "counts": counts,
            "unresolved_references": [],
        },
        "guide": dict(guide(d), cases=cases(d, bundle)),
        "field_dictionary": field_dictionary(d),
        "species": species_table(),
        "licences": d["licences"],
        "entries": d["entries"],
        "rules": d["rules"],
        "licensing": d["licensing"],
        "rulesets": d["rulesets"],
        "licensing_sets": d["licensing_sets"],
        "waters": d["waters"],
        "splits": d["splits"],
        "index": index(d),
    }
    doc["about"]["unresolved_references"] = corpus_references(doc)
    return doc


# --------------------------------------------------------------------------------------------
# The checks: run on the OUTPUT, so they prove what ships
# --------------------------------------------------------------------------------------------

def _keys(o, path=""):
    if isinstance(o, dict):
        for k, v in o.items():
            yield k, f"{path}.{k}"
            yield from _keys(v, f"{path}.{k}")
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from _keys(v, f"{path}[{i}]")


def retired_keys(doc: dict) -> list[str]:
    """Every place a retired field name appears as a key, or a retired rule type as a type."""
    out = []
    data = {k: v for k, v in doc.items() if k not in ("rules",)}
    for k, where in _keys(data):
        if k in RETIRED_ANYWHERE:
            out.append(where)
    for rid, x in doc["rules"].items():
        if x["type"] in RETIRED_TYPES:
            out.append(f".rules.{rid}.type={x['type']}")
        for k in x["fields"]:
            if k in RETIRED_ON_RULE:
                out.append(f".rules.{rid}.fields.{k}")
        for k, where in _keys(x, f".rules.{rid}"):
            if k in RETIRED_ANYWHERE:
                out.append(where)
    return sorted(set(out))


def _registries() -> list[tuple[str, dict, set]]:
    """(name, the words, the model's own registry) — they must name exactly the same members."""
    lm = _licensing_models()
    return [
        ("TYPE_TEXT", TYPE_TEXT, set(_enum(C.RuleType))),
        ("FAMILY_TEXT", FAMILY_TEXT, set(C._FAMILY.values())),
        ("SLOT_TEXT", SLOT_TEXT, set(_enum(C.Slot))),
        ("CLAUSE_TEXT", CLAUSE_TEXT, set(_fields(C.GearClause))),
        ("GEAR_WHEN_TEXT", GEAR_WHEN_TEXT, set(_fields(C.GearWhen))),
        ("GEAR_SPEC_TEXT", GEAR_SPEC_TEXT, set(_fields(C.GearSpec))),
        ("WHEN_TEXT", WHEN_TEXT, set(_fields(C.When))),
        ("LIFT_TEXT", LIFT_TEXT, set(LIFT_KEYS)),
        ("LENGTH_TEXT", LENGTH_TEXT, set(_fields(C.LengthBand))),
        ("WHO_TEXT", WHO_TEXT, set(C.WHO_AXES)),
        ("PATH_TEXT", PATH_TEXT, set(_fields(C.Path))),
        ("DOING_TEXT", DOING_TEXT, set(typing.get_args(C.Doing.model_fields["act"].annotation))),
        ("LICENSING_KIND_TEXT", LICENSING_KIND_TEXT, set(lm)),
        ("PART_TEXT", PART_TEXT, set(C.LABEL_PARTS)),
        ("LICENSING_PART_TEXT", LICENSING_PART_TEXT, set(C.LICENSING_PARTS)),
    ]


def unexplained(doc: dict) -> list[str]:
    """Registry members the guide does not explain, words for members the model no longer has,
    and shipped keys the dictionary does not explain."""
    g, out = doc["guide"], []
    for name, words, registry in _registries():
        out += [f"{name} does not explain {k}" for k in sorted(registry - set(words))]
        out += [f"{name} explains {k}, which the model does not have"
                for k in sorted(set(words) - registry)]
        out += [f"{name}[{k}] is empty" for k in sorted(words) if not words[k]]
    model = set(_fields(C.CatalogueRule))
    out += [f"RULE_FIELD_TEXT explains {k}, which CatalogueRule does not have"
            for k in sorted(set(RULE_FIELD_TEXT) - model)]
    lfields = {f for m in _licensing_models().values() for f in _fields(m)}
    out += [f"LICENSING_FIELD_TEXT explains {k}, which no licensing record has"
            for k in sorted(set(LICENSING_FIELD_TEXT) - lfields)]
    for t in _enum(C.RuleType):
        if not (g["rule_types"].get(t) or {}).get("means"):
            out.append(f"rule type {t}")
    for s in C.Slot:
        if not (g["gear"]["slots"].get(s.value) or {}).get("means"):
            out.append(f"slot {s.value}")
    for a in C.CONDUCT_ACTS:
        if not (g["gear"]["conduct"]["acts"].get(a) or {}).get("means"):
            out.append(f"conduct {a}")
    for k in _licensing_models():
        if not (g["licensing"]["kinds"].get(k) or {}).get("means"):
            out.append(f"licensing kind {k}")
    model = set(_fields(C.CatalogueRule))
    for k, v in doc["field_dictionary"]["rule.fields"].items():
        if not v:
            out.append(f"rule field {k} has no text")
        if k not in model:
            out.append(f"rule field {k} is not a CatalogueRule field")
    lmodels = _licensing_models()
    for kind, fs in doc["field_dictionary"]["licensing.fields"].items():
        for f, v in fs.items():
            if not v:
                out.append(f"{kind} field {f} has no text")
            if f not in _fields(lmodels[kind]):
                out.append(f"{kind} field {f} is not a {lmodels[kind].__name__} field")
    for sub, model_ in (("clause_fields", C.GearClause), ("when_fields", C.GearWhen),
                        ("requires_fields", C.GearSpec)):
        if set(g["gear"][sub]) != set(_fields(model_)):
            out.append(f"gear.{sub} differs from {model_.__name__}")
    return out


def touch_problems(waters: dict) -> list[str]:
    """Every `touches` list that is not sorted, names its own part, names no part of the water,
    or is not returned by the part it names."""
    out = []
    for item, w in waters.items():
        for i, p in enumerate(w["parts"]):
            t = p.get("touches")
            if t is None or t != sorted(set(t)) or i in t or any(
                    not 0 <= j < len(w["parts"]) or i not in w["parts"][j].get("touches", ())
                    for j in t):
                out.append(f"water {item} part {i} -> touches {t}")
    return out


def dangling(doc: dict) -> list[str]:
    """Every reference in the file that does not resolve."""
    R, L, E = doc["rules"], doc["licensing"], doc["entries"]
    out = touch_problems(doc["waters"])
    for eid, e in E.items():
        out += [f"entry {eid} -> rule {r}" for r in e["rules"] if r not in R]
        out += [f"entry {eid} -> see {t}" for s in e.get("see") or []
                for t in s.get("entry_ids") or [] if t not in E]
        out += [f"entry {eid} -> licensing {r}" for r in e["licensing"] if r not in L]
    for name, table, known in (("ruleset", doc["rulesets"], R),
                               ("licensing_set", doc["licensing_sets"], L)):
        for sid, s in table.items():
            for via, ids in s.items():
                if via == "sections":
                    continue
                out += [f"{name} {sid} -> {i}" for i in ids if i not in known]
    for item, w in doc["waters"].items():
        for p in w["parts"]:
            if p["ruleset"] is not None and p["ruleset"] not in doc["rulesets"]:
                out.append(f"water {item} -> ruleset {p['ruleset']}")
            if p["licensing_set"] is not None and p["licensing_set"] not in doc["licensing_sets"]:
                out.append(f"water {item} -> licensing_set {p['licensing_set']}")
        out += [f"water {item} -> entry {e}" for e in w["entries"] if e not in E]
        if w.get("part_of") and w["part_of"] not in doc["waters"]:
            out.append(f"water {item} -> part_of {w['part_of']}")
    def examples(o):
        if isinstance(o, dict):
            if {"id", "label", "verbatim"} <= set(o):
                yield o["id"]
                return
            for v in o.values():
                yield from examples(v)
        elif isinstance(o, list):
            for v in o:
                yield from examples(v)
    out += [f"guide example {i}" for i in examples(doc["guide"]) if i not in R and i not in L]
    for i, x in R.items():
        out += [f"rule {i} -> recorded_by {x['recorded_by']}"] if x.get("recorded_by") and \
            x["recorded_by"] not in R else []
        out += [f"rule {i} -> records_for {q}" for q in x.get("records_for") or [] if q not in R]
    g = doc["guide"]
    listed = (g["standing"]["rules"] + g["angler_closure"]["rules"]
              + g["placement"]["uncertain"]["rules"] + g["placement"]["binds"]["in_part"])
    out += [f"guide lists rule {i}" for i in listed if i not in R]
    out += [f"guide lists licensing {i}" for i in g["placement"]["uncertain"]["licensing"]
            if i not in L]
    out += [f"guide lists entry {i}" for i in g["placement"]["unplaced_entries"] if i not in E]
    for name, ids, known in [(f"index {k}", v, R) for k, v in
                             {**doc["index"]["rules_by_type"],
                              **doc["index"]["rules_by_family"]}.items()] + \
            [(f"index {k}", v, L) for k, v in doc["index"]["licensing_by_kind"].items()]:
        out += [f"{name} -> {i}" for i in ids if i not in known]
    return out


def corpus_references(doc: dict) -> list[str]:
    """References the RECORDS make to each other (a clause to its parent, a lift to what it
    lifts, a record to the one it restates) that do not resolve. These are defects in the
    corpus, not in the export: the file ships them under `about.unresolved_references`, and a
    lift whose target does not resolve must not be applied."""
    R, L = doc["rules"], doc["licensing"]
    out = []
    for x in R.values():
        f, eid = x["fields"], x["entry_id"]
        for key in ("within", "condition_of", "derived_from", "suspended_while"):
            if f.get(key) and f"{eid}::{f[key]}" not in R:
                out.append(f"rule {x['id']} {key} -> {f[key]}")
        # Resolved lifts: each names one rule, which must exist and must not be the lifter.
        for ex in f.get("exempts") or []:
            to = f"{ex.get('entry_id')}::{ex.get('rule_id')}"
            if to not in R or to == x["id"]:
                out.append(f"rule {x['id']} exempts -> {to}")
    for x in L.values():
        f, eid = x["fields"], x["entry_id"]
        for key in ("alternative_to", "restates"):
            ref = f.get(key)
            if ref and f"{ref['entry_id']}#{ref['id']}" not in L:
                out.append(f"licensing {x['id']} {key} -> {ref}")
        for s in f.get("suspended_while") or []:
            if f"{eid}::{s['rule_id']}" not in R:
                out.append(f"licensing {x['id']} suspended_while -> {s['rule_id']}")
        for p in f.get("satisfied_by") or []:
            out += [f"licensing {x['id']} hold -> {doc_}" for doc_ in p.get("hold") or []
                    if doc_ not in doc["licences"]]
        for doc_ in ([f["document"]] if f.get("document") else []) + (f.get("documents") or []):
            if doc_ not in doc["licences"]:
                out.append(f"licensing {x['id']} document -> {doc_}")

    return out


def case_problems(doc: dict) -> list[str]:
    """A mechanism with no real case, and a case naming anything the file does not hold."""
    g = doc["guide"]["cases"]
    out = [f"guide.cases: no real case of {m}" for m in g["missing"]]
    out += [f"guide.cases: WHAT_TO_SHOW has no line for {m}"
            for m in sorted(set(CASE_MECHANISMS) - set(WHAT_TO_SHOW))]
    out += [f"guide.cases: WHAT_TO_SHOW names {m}, which is no mechanism"
            for m in sorted(set(WHAT_TO_SHOW) - set(CASE_MECHANISMS))]
    for c in g["cases"]:
        w = doc["waters"].get(c["water"]["item_id"])
        tag = f"guide.cases {c['mechanism']}"
        if w is None:
            out.append(f"{tag}: water {c['water']['item_id']}")
            continue
        if not any(p["ruleset"] == c["ruleset"] and p["licensing_set"] == c["licensing_set"]
                   for p in w["parts"]):
            out.append(f"{tag}: no part of {c['water']['item_id']} carries ruleset "
                       f"{c['ruleset']} / licensing set {c['licensing_set']}")
        out += [f"{tag}: expects {x['id']}" for x in c["expect"] if x["id"] not in doc["rules"]]
        out += [f"{tag}: because {i}" for i in c["because"]
                if i not in doc["rules"] and i not in doc["licensing"] and i not in doc["entries"]]
        out += [f"{tag}: entry {e}" for e in c["entries"] if e not in doc["entries"]]
    return out


def is_steelhead_rule(x: dict) -> bool:
    """A rule ABOUT STEELHEAD: its species names steelhead (`ST`), or it names no fish and its
    sentence is about steelhead (Region 5's "steelhead fisheries within the Chilcotin River
    Watershed may be closed"). A group rule whose fish include steelhead ("Trout/char: 5") is not
    one — it is the rainbow/trout quota a big lake rainbow falls under."""
    sp = x["fields"].get("species") or []
    return "ST" in sp or (not sp and "steelhead" in str(x.get("verbatim") or "").lower())


def is_wild_steelhead_release(x: dict) -> bool:
    """The RELEASE of wild steelhead, outright (the province's "All wild steelhead must be released"
    and each steelhead region's "All wild steelhead" line — streams, and the lakes whose own row
    names steelhead; user rulings 2026-10-01): a retention_limit of steelhead alone, take 0, the fish
    may be fished for (not a closure), every wild fish (origin wild, or no origin — Regions 3 and 5
    print "ALL STEELHEAD"), with no date, size, water kind, record duty or quota clause. The
    province's "All wild steelhead must be released" and the zones' "And you must release: All wild
    steelhead" lines are these; a hatchery quota, the annual 10, the record duty, "stop fishing
    after the hatchery quota" and the Region 6 stream closure are not."""
    f = x["fields"]
    return (x.get("type") == "retention_limit" and (f.get("species") or []) == ["ST"]
            and f.get("take") == 0 and f.get("may_target") is True
            and f.get("origin") in (None, "wild")
            and not any(f.get(k) for k in ("when", "lengths", "water", "record_retention",
                                           "within", "while", "when_targeting"))
            and f.get("period") in (None, "daily"))


def is_steelhead_record(x: dict) -> bool:
    """A licensing record about fishing for steelhead (`doing.species` names `ST`): the stamp."""
    return "ST" in ((x["fields"].get("doing") or {}).get("species") or [])


#: The provincial steelhead entry, and the regions whose own tables name steelhead (p.15, 23, 30,
#: 48, 55; 4, 7A, 7B and 8 print no steelhead rule — 7A's Stellako prints only a stamp waiver).
PROVINCE_STEELHEAD = "zp:steelhead"
STEELHEAD_REGIONS = ("1", "2", "3", "5", "6")
#: THE PROVINCIAL STEELHEAD SET (book p.8, `zp:steelhead`), each member by what it says — so a
#: lake copy (`steelhead.r1b`, the stamp's `steelhead_targeting_lakes`) is the same member.
PROVINCE_SET = ("annual hatchery quota", "wild release", "record duty", "stamp")


def province_steelhead_member(x: dict) -> str | None:
    """Which member of the provincial steelhead set a rule or licensing record is, or None."""
    if x.get("entry_id") != PROVINCE_STEELHEAD:
        return None
    f = x["fields"]
    if "kind" in x:                          # a licensing record (a rule record has `type`)
        return "stamp" if x["kind"] == "requirement" and is_steelhead_record(x) else None
    if (f.get("species") or []) != ["ST"]:
        return None
    if f.get("period") == "annual" and f.get("origin") == "hatchery" and (f.get("take") or 0) > 0:
        return "annual hatchery quota"
    if is_wild_steelhead_release(x):
        return "wild release"
    if f.get("record_retention"):
        return "record duty"
    return None


def _members(table: dict, key, known: dict) -> set[str]:
    return {i for via, ids in (table.get(key) or {}).items() if via != "sections"
            for i in ids if i in known}


def _in_steelhead_region(rule_ids) -> bool:
    """A part is in a steelhead region when it carries that region's own zone table (a water
    takes the zone rules of the region it lies in; `ladder.region`)."""
    return any(i.split(":", 1)[0] in {f"z{n}" for n in STEELHEAD_REGIONS} for i in rule_ids)


def province_set_missing(rule_ids, record_ids, R: dict, L: dict) -> list[str]:
    """The members of the provincial steelhead set a part lacks, in `PROVINCE_SET` order."""
    have = {province_steelhead_member(R[i]) for i in rule_ids if i in R}
    have |= {province_steelhead_member(L[i]) for i in record_ids if i in L}
    return [m for m in PROVINCE_SET if m not in have]


def steelhead_lake_problems(doc: dict) -> list[str]:
    """A STEELHEAD RULE SHOWS ON A LAKE ONLY WHEN THE LAKE'S OWN ROW NAMES STEELHEAD, AND THEN THE
    WHOLE PROVINCIAL SET DOES TOO (user rulings 2026-10-01; user ask 2026-10-02). Big lake rainbow
    fall under the rainbow size quota, not the steelhead quota, so every provincial and zone
    steelhead rule binds streams only. Refused:
      - a lake whose ruleset or licensing set carries a steelhead rule or stamp record
        (`is_steelhead_rule`, `is_steelhead_record`) — the wild release included — when no row of
        the lake prints "steelhead";
      - on a lake whose own row does print it (Khartoum and Lois lakes: "Rainbow trout/hatchery
        steelhead quota = 6 in the aggregate"), a steelhead rule of ANOTHER row other than a wild
        release or a member of the provincial set (a zone's "2 hatchery steelhead" stays on
        streams), and a part lacking any member of the provincial set (`province_set_missing`);
      - a steelhead record placed province-wide — it would hold on every lake."""
    R, L, E = doc["rules"], doc["licensing"], doc["entries"]
    out = [f"steelhead record {i} is placed province-wide — it holds on every lake"
           for i, x in L.items() if is_steelhead_record(x) and x.get("placement") == "province"]
    bad: dict = {}
    for item, w in sorted(doc["waters"].items()):
        if w.get("kind") != "lake":
            continue
        own = {e for e in w.get("entries") or []
               if "steelhead" in str((E.get(e) or {}).get("printed") or "").lower()}
        for n, p in enumerate(w["parts"]):
            for table, known, test, key in (
                    (doc["rulesets"], R, is_steelhead_rule, "ruleset"),
                    (doc["licensing_sets"], L, is_steelhead_record, "licensing_set")):
                for i in sorted(_members(table, p[key], known)):
                    x = known[i]
                    if not test(x) or x["entry_id"] in own:
                        continue
                    allowed = own and (province_steelhead_member(x) or (
                        key == "ruleset" and is_wild_steelhead_release(x)))
                    if not allowed:
                        bad.setdefault(i, []).append(item)
            if own and p["ruleset"]:
                gone = province_set_missing(_members(doc["rulesets"], p["ruleset"], R),
                                            _members(doc["licensing_sets"], p["licensing_set"], L),
                                            R, L)
                if gone:
                    out.append(f"lake {item} part {n}: its own row names steelhead, but it lacks "
                               f"the provincial steelhead {', '.join(gone)}")
    for i, items in sorted(bad.items()):
        items = sorted(set(items))
        out.append(f"steelhead rule {i} shows on {len(items)} lake(s) whose own row does not "
                   f"allow it, e.g. {', '.join(items[:3])}")
    return out


def steelhead_set_problems(doc: dict) -> list[str]:
    """EVERY WATER WHOSE OWN ROW NAMES STEELHEAD CARRIES THE WHOLE PROVINCIAL STEELHEAD SET (user
    ask 2026-10-02) — and nothing else does. On the output, per water part:
      - a part carrying a steelhead rule of a WATER row (`is_steelhead_rule`, entry `r…`) in a
        steelhead region (`_in_steelhead_region`) must carry every member of `PROVINCE_SET`: the
        annual hatchery 10, the wild release, the record duty and the stamp;
      - a part carrying any member of the set must be in a steelhead region, and a stream or a
        lake whose own row names steelhead."""
    R, L, E, out = doc["rules"], doc["licensing"], doc["entries"], []
    for item, w in sorted(doc["waters"].items()):
        own_lake = w.get("kind") == "lake" and any(
            "steelhead" in str((E.get(e) or {}).get("printed") or "").lower()
            for e in w.get("entries") or [])
        for n, p in enumerate(w["parts"]):
            if not p["ruleset"]:
                continue
            rules = _members(doc["rulesets"], p["ruleset"], R)
            recs = _members(doc["licensing_sets"], p["licensing_set"], L)
            region = _in_steelhead_region(rules)
            rows = sorted({R[i]["entry_id"] for i in rules
                           if R[i]["entry_id"].startswith("r") and is_steelhead_rule(R[i])})
            tag = f"water {item} part {n}"
            if rows and region:
                gone = province_set_missing(rules, recs, R, L)
                if gone:
                    out.append(f"{tag}: {rows[0]} names steelhead here, but the part lacks the "
                               f"provincial steelhead {', '.join(gone)}")
            have = sorted(i for i in rules | recs
                          if province_steelhead_member((R.get(i) or L.get(i))))
            if have and not region:
                out.append(f"{tag}: provincial steelhead {have[0]} outside the steelhead regions")
            if have and w.get("kind") != "stream" and not own_lake:
                out.append(f"{tag}: provincial steelhead {have[0]} on a {w.get('kind')} whose own "
                           f"row does not name steelhead")
    return out


def steelhead_presence_problems(doc: dict) -> list[str]:
    """`steelhead` (known | possible) and `anadromous_rainbow` agree with each other and with the
    rules (user ruling 2026-10-01): a big rainbow is a steelhead only on a KNOWN stream part; a part
    marked known or possible carries the steelhead rules, a lake is marked only when its own row
    names steelhead, and the water's roll-up is its parts' best."""
    R, E = doc["rules"], doc["entries"]
    out = []
    for item, w in sorted(doc["waters"].items()):
        own = any("steelhead" in str((E.get(e) or {}).get("printed") or "").lower()
                  for e in w.get("entries") or [])
        got = {p.get("steelhead") for p in w["parts"]}
        top = "known" if "known" in got else "possible" if "possible" in got else None
        if w.get("steelhead") != top:
            out.append(f"water {item}: steelhead {w.get('steelhead')!r}, parts say {top!r}")
        if any(p.get("anadromous_rainbow") for p in w["parts"]) != bool(w.get("steelhead_source")):
            out.append(f"water {item}: steelhead_source {w.get('steelhead_source')!r} disagrees with "
                       f"its known streams")
        for n, p in enumerate(w["parts"]):
            tag, st = f"water {item} part {n}", p.get("steelhead")
            if st not in (None, "known", "possible"):
                out.append(f"{tag}: steelhead {st!r}")
            if p.get("anadromous_rainbow") and not (st == "known" and w["kind"] == "stream"):
                out.append(f"{tag}: anadromous_rainbow on a {st or 'unmarked'} {w['kind']}")
            if st == "known" and w["kind"] == "stream" and not p.get("anadromous_rainbow"):
                out.append(f"{tag}: a known stream where a big rainbow is not a steelhead")
            if w["kind"] != "stream" and st and not (st == "known" and own):
                out.append(f"{tag}: a {w['kind']} marked {st!r}")
            if st == "possible" and p["ruleset"] and not any(
                    is_steelhead_rule(R[i]) for via, ids in doc["rulesets"][p["ruleset"]].items()
                    if via != "sections" for i in ids if i in R):
                out.append(f"{tag}: \"possible\" but no steelhead rule applies")
    return out


def valid_end(token, doc: dict) -> bool:
    """An end token a run may carry: a key of `splits`, a bare natural end, or a prefixed one
    naming a water the file holds (a lake's ends name a lake or wetland)."""
    if token in doc["splits"] or token in SP.NATURAL_ENDS:
        return True
    head, _, item = str(token).partition(":")
    w = doc["waters"].get(item)
    if head in ("lake_inlet", "lake_outlet"):
        return w is not None and w["kind"] in ("lake", "wetland")
    return head == "confluence" and w is not None


def run_problems(doc: dict) -> list[str]:
    """Every part says where it runs, in ends the file can name, upstream to downstream."""
    out = []
    for item, w in doc["waters"].items():
        for i, p in enumerate(w["parts"]):
            runs, tag = p.get("runs"), f"water {item} part {i}"
            if not runs:
                out.append(f"{tag}: no runs")
                continue
            if w["kind"] != "stream":
                if len(runs) != 1 or any(runs[0][k] is not None for k in
                                         ("from", "to", "km_from", "km_to")):
                    out.append(f"{tag}: a polygon is one run with no ends")
                continue
            for r in runs:
                for k in ("from", "to"):
                    if not valid_end(r[k], doc):
                        out.append(f"{tag}: {k} {r[k]!r} is no cut or end the file can name")
                if r["km_from"] is not None and r["km_to"] is not None \
                        and r["km_from"] < r["km_to"]:
                    out.append(f"{tag}: runs uphill ({r['km_to']} -> {r['km_from']})")
                if not r.get("branch") and (r["km_from"] is None or r["km_to"] is None):
                    out.append(f"{tag}: a main-stem run with no km")
            km = [r["km_from"] for r in runs if r["km_from"] is not None]
            if km != sorted(km, reverse=True):
                out.append(f"{tag}: runs not ordered upstream to downstream ({km})")
    return out


def period_problems(doc: dict) -> list[str]:
    """Every designation carries a period that agrees with its own `when`."""
    out = []
    for i, x in doc["licensing"].items():
        per = x.get("period")
        if x["kind"] != "designation":
            if per is not None:
                out.append(f"{i}: a period on a {x['kind']}")
            continue
        when = x["fields"].get("when")
        if not per or per.get("kind") not in ("when_open", "all_year", "dates") \
                or not per.get("says"):
            out.append(f"designation {i}: no period")
        elif (per["kind"] == "dates") != bool(when) or \
                (when and per.get("dates") != when.get("dates")):
            out.append(f"designation {i}: period {per['kind']} disagrees with its when {when}")
    return out


def problems(doc: dict) -> list[str]:
    return ([f"retired key {w}" for w in retired_keys(doc)] + unexplained(doc) + dangling(doc)
            + dictionary_gaps(doc)
            + case_problems(doc) + record_link_problems(doc) + steelhead_lake_problems(doc)
            + steelhead_presence_problems(doc) + steelhead_set_problems(doc)
            + run_problems(doc) + period_problems(doc))


def dumps(doc: dict) -> str:
    return json.dumps(doc, indent=1, ensure_ascii=False) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bundle", type=Path, default=BUNDLE)
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    doc = build(a.bundle)
    bad = problems(doc)
    if bad:
        print(f"export_ui_rules: REFUSED — {len(bad)} problem(s) in the output:", file=sys.stderr)
        for p in bad[:40]:
            print(f"  {p}", file=sys.stderr)
        return 1
    for r in doc["about"]["unresolved_references"]:
        print(f"  corpus reference does not resolve: {r}", file=sys.stderr)
    text = dumps(doc)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(text, encoding="utf-8")
    c = doc["about"]["counts"]
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 1e6:.2f} MB)")
    for k, v in c.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
