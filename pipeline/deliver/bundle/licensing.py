"""Licensing, from the curated corpus and the reach builder's placements into the bundle.

WHAT A LICENSING RECORD IS, AND WHY IT IS NOT A RULE: see `CatalogueEntry.licensing`. It never
competes and never votes on open/closed; it says what you must hold, as whom, doing what, and
which waters are Classified. The reader composes the per-angler sentence (in core —
app/packages/core/src/regulations.ts); the bundle ships the records, their generated labels,
their quotes, and where the placed ones are.

THIS MODULE CONSUMES, IT DOES NOT DERIVE — the same contract as `rules.py`. Which sections a
designation covers is decided in `pipeline.atlas.reach` (`licensing_section.jsonl`), by the SAME
`build_reach` the rules go through, tributary walk and carve-outs included.

THE TABLES
    licence             the document register (`Document`), so a reader can name a document
    designation         Class I/II waters: class, unit, period, stamp period or waiver, suspension
    not_classified      "Part described is NOT a Classified Water" — an asserted absence
    requirement         who must hold what, doing what, where
    licence_terms       how a document is sold — NEVER placed (the Dean draw defect)
    exemption           who is released from which documents — never placed
    alternative         a place where another document also satisfies a requirement
    section_licensing   section -> set, interned like `section_ruleset`
    licensing_set       set -> (kind, entry_id, record_id, via)
  and four VIEWS over the last two — designation_section, not_classified_section,
  requirement_section, alternative_section — so a reader asks for one kind by name.

    `via` is how the record reaches the section: `reach`, `trib` (the tributary walk),
    `trib_pending` (a walk that was not done — never complete), or `contested` (a designation on
    a section a not_classified record also binds, acknowledged below; the reader must say "check").

EVERY RECORD CARRIES `placement` (sections | province | on_designation | unresolved), and an
unresolved one is `uncertain`. For licensing the unsafe direction is UNDER-requiring: a
requirement that could not be placed must read "check", never "none needed".
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

#: The four kinds with section bindings. The other two are never placed.
PLACED = ("designation", "requirement", "not_classified", "alternative")

#: A DESIGNATION AND A NOT_CLASSIFIED ON THE SAME SECTION, known and acknowledged.
#:
#: Design validator 7: the build refuses any section both bind, because one of them is wrong and
#: the book says which — "ALL tributaries (EXCEPT Coal Creek downstream of old MF&M Railway
#: Bridge …) are Class II waters". Here it is the Elk River's tributary designation, whose walk
#: reaches lower Coal Creek because the printed carve-out is not drawn: a `tributary_excludes` on
#: that split would also cut Coal Creek ABOVE the bridge, which is classified. Drawing it is a
#: curator's job (the designation's `review_reason` says so), and inventing the cut here is not.
#:
#: So an acknowledged pair ships, with the designation's binding on those sections marked
#: `contested` — neither side silently wins. An UNLISTED conflict fails the build naming both, and
#: a listed pair that no longer conflicts fails too, so this list cannot outlive its reason.
ACKNOWLEDGED_CONFLICTS: dict[tuple[tuple[str, str], tuple[str, str]], str] = {
    (("r4:elk_river_s_tributaries_see_exceptions@4-2+4-23", "elk_river"),
     ("r4:coal_creek_downstream_of_old_mf_m_railway_bridge_7_km_upstre@4-23", "not_classified")):
        "the Elk's '(EXCEPT Coal Creek downstream of old MF&M Railway Bridge…)' is not drawn",
    # Found by this check on its first full build: the Elk above Elko Dam, "including
    # tributaries", walks into lower Coal Creek by the same undrawn carve-out.
    (("r4:elk_river_upstream_of_elko_dam@4-2+4-23", "elk_river"),
     ("r4:coal_creek_downstream_of_old_mf_m_railway_bridge_7_km_upstre@4-23", "not_classified")):
        "the Elk (upstream of Elko Dam, including tributaries) reaches lower Coal Creek by the "
        "same undrawn carve-out",
}


def _jsonl(path: Path):
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _dump(rec) -> str:
    """The record as the corpus holds it, terse and by alias — the structured half of every row."""
    return json.dumps(rec.model_dump(mode="json", by_alias=True, exclude_none=True),
                      separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _j(v) -> str | None:
    return None if v is None else json.dumps(v, separators=(",", ":"), sort_keys=True,
                                             ensure_ascii=False)


def _days(when) -> set[int]:
    from pipeline.regs.parsing.catalogue import _days as days

    if when is None or not when.dates:
        return set(range(1, 367))
    return days(when.dates)


def write(db: sqlite3.Connection, reaches: Path, entries: list, cov,
          sid: dict[str, int]) -> None:
    """Write every licensing table. `entries` is the validated `CatalogueEntry` list the rule
    writer read — one pass over the corpus, so the two halves can never read different files."""
    from pipeline.regs.parsing.catalogue import (
        PROVINCIAL_ANGLER_DOCUMENTS, _DOC_WORDS, Alternative, Designation, Document, Exemption,
        LicenceTerms, NotClassified, Requirement, compose_licensing, licensing_parts,
    )

    placements_file = reaches / "licensing_placement.jsonl"
    sections_file = reaches / "licensing_section.jsonl"
    if not placements_file.exists() or not sections_file.exists():
        # A reach run from before licensing was placed. Bundling from it would ship every
        # designation bound nowhere — every classified water reading as unclassified.
        raise SystemExit(
            f"{reaches} has no licensing placements — it predates them. Re-run the reach "
            f"builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")

    # ---- the corpus -----------------------------------------------------------------
    records: dict[tuple[str, str], tuple] = {}          # (entry_id, id) -> (entry, record)
    units: dict[str, str] = {}
    for ce in entries:
        for x in ce.licensing:
            records[(ce.entry_id, x.id)] = (ce, x)
            if isinstance(x, Designation):
                units.setdefault(x.unit, x.unit_name)
    refs = {k: x for k, (_, x) in records.items()}

    # ---- the placements, checked against the corpus BOTH WAYS ----------------------------
    placed = {(p["entry_id"], p["record_id"]): p for p in _jsonl(placements_file)}
    want = {k for k, (_, x) in records.items() if x.kind in PLACED}
    missing, extra = sorted(want - set(placed)), sorted(set(placed) - want)
    if missing or extra:
        raise SystemExit(
            f"licensing: the reach run and the corpus disagree — {len(missing)} record(s) the "
            f"run never placed (e.g. {missing[:3]}), {len(extra)} it placed that the corpus no "
            f"longer has (e.g. {extra[:3]}). The run is stale; re-run the reach builder.")
    for k, p in placed.items():
        if p["kind"] != records[k][1].kind:
            raise SystemExit(f"licensing: {k} is a {records[k][1].kind} in the corpus and a "
                             f"{p['kind']} in the reach run — the run is stale")
    # A REQUIREMENT WITH A PLACE AND AN `on` THAT NO DESIGNATION REACHES holds nowhere
    # (`reach.licensing.on_designations`). It is a curation defect — the place and the period it
    # names never meet — and shipping it as "check" would hide that; the build stops instead.
    from pipeline.atlas.reach.licensing import NO_DESIGNATION
    nowhere = sorted(k for k, p in placed.items() if p.get("reason") == NO_DESIGNATION)
    if nowhere:
        raise SystemExit(
            f"licensing: {len(nowhere)} requirement(s) with `extents` and `on` reach no "
            f"designation that satisfies `on` — they hold nowhere: {nowhere[:5]}. Fix the "
            f"extents or the `on`.")

    # ---- section bindings ------------------------------------------------------------
    by_section: dict[str, set[tuple[str, str, str, str]]] = {}
    for r in _jsonl(sections_file):
        by_section.setdefault(r["section_id"], set()).add(
            (r["kind"], r["entry_id"], r["record_id"], r["scope"]))
    _unknown = [s for s in by_section if s not in sid]
    if _unknown:
        raise SystemExit(f"licensing: {len(_unknown):,} bound sections are not in the handle "
                         f"table (e.g. {_unknown[:3]}) — the reach run and the atlas disagree")

    # DESIGN VALIDATOR 7 — no section is both Classified and NOT Classified.
    conflicts: dict[tuple, list[str]] = {}
    for s, rows in by_section.items():
        ds = {(e, i) for k, e, i, _ in rows if k == "designation"}
        ns = {(e, i) for k, e, i, _ in rows if k == "not_classified"}
        for d in ds:
            for n in ns:
                conflicts.setdefault((d, n), []).append(s)
    unlisted = {k: v for k, v in conflicts.items() if k not in ACKNOWLEDGED_CONFLICTS}
    if unlisted:
        lines = [f"    designation {d[0]}#{d[1]}  vs  not_classified {n[0]}#{n[1]}  "
                 f"({len(v)} sections, e.g. {sorted(v)[:2]})" for (d, n), v in unlisted.items()]
        raise SystemExit(
            "licensing: a section is bound by a designation AND by a not_classified record. One "
            "of them is wrong — write the carve-out the book implies (tributary_excludes / "
            "extents) on the designation:\n" + "\n".join(lines))
    stale = [k for k in ACKNOWLEDGED_CONFLICTS if k not in conflicts]
    if stale:
        raise SystemExit(f"licensing: ACKNOWLEDGED_CONFLICTS lists {stale} but they no longer "
                         f"conflict — remove the entry")
    for (d, n) in ACKNOWLEDGED_CONFLICTS:
        if not records[d][1].review_reason:
            raise SystemExit(f"licensing: {d} is acknowledged as conflicting with {n} but "
                             f"carries no review_reason — the curator has not been told")
    contested = {(s, d) for (d, n), v in conflicts.items() for s in v}
    for s, rows in by_section.items():
        by_section[s] = {(k, e, i, "contested" if k == "designation" and (s, (e, i)) in contested
                          else via) for k, e, i, via in rows}

    # DESIGN VALIDATOR 6 — two DIFFERENT units on one section on overlapping dates is ambiguous
    # (which day licence does a non-resident buy?). Reported, never settled.
    ambiguous: Counter = Counter()
    for s, rows in by_section.items():
        ds = sorted({(e, i) for k, e, i, _ in rows if k == "designation"})
        for a in range(len(ds)):
            for b in range(a + 1, len(ds)):
                x, y = records[ds[a]][1], records[ds[b]][1]
                if x.unit != y.unit and _days(x.when) & _days(y.when):
                    ambiguous[(ds[a], ds[b])] += 1

    # ---- rows --------------------------------------------------------------------------
    def place(k):
        p = placed.get(k)
        if p is None:
            return None, 0, None
        unres = (f"{p['reason']}: {p['detail']}" if p["placement"] == "unresolved" else None)
        return p["placement"], 1 if p["placement"] == "unresolved" else 0, unres

    rows: dict[str, list] = {t: [] for t in ("designation", "not_classified", "requirement",
                                             "licence_terms", "exemption", "alternative")}
    for (eid, rid), (ce, x) in sorted(records.items()):
        sib = {r.rule_id: r for r in ce.rules}
        # THE PARTS, and the one preview composed from them (`catalogue.compose_licensing`).
        parts = licensing_parts(x, sib, units=units, refs=refs)
        placement, uncertain, unres = place((eid, rid))
        # in the model's order (`LICENSING_PARTS`), which `_j` would sort away
        common = (compose_licensing(parts),
                  json.dumps(parts, separators=(",", ":"), ensure_ascii=False), x.verbatim, x.review_reason or None,
                  _dump(x))
        if isinstance(x, Designation):
            rows["designation"].append((
                eid, rid, x.classified, x.unit, x.unit_name,
                placement, uncertain, unres, *common))
        elif isinstance(x, NotClassified):
            rows["not_classified"].append((eid, rid, placement, uncertain, unres, *common))
        elif isinstance(x, Requirement):
            rows["requirement"].append((
                eid, rid, x.on, x.authority, x.water.value if x.water else None,
                placement, uncertain, unres, *common))
        elif isinstance(x, LicenceTerms):
            rows["licence_terms"].append((eid, rid, x.document.value, x.classified,
                                          _j(list(x.units)), *common))
        elif isinstance(x, Exemption):
            rows["exemption"].append((eid, rid, _j([d.value for d in x.documents]), *common))
        elif isinstance(x, Alternative):
            rows["alternative"].append((
                eid, rid, x.alternative_to.entry_id, x.alternative_to.id,
                placement, uncertain, unres, *common))
        else:                                                           # pragma: no cover
            raise SystemExit(f"licensing: {eid}#{rid} is an unknown kind {x.kind!r}")

    tail = "label, parts, verbatim, review_reason, record"
    db.executemany(
        "INSERT INTO designation (entry_id, designation_id, classified, unit, unit_name, "
        f"placement, uncertain, unresolved, {tail}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows["designation"])
    db.executemany(
        "INSERT INTO not_classified (entry_id, not_classified_id, placement, uncertain, "
        f"unresolved, {tail}) VALUES (?,?,?,?,?,?,?,?,?,?)", rows["not_classified"])
    db.executemany(
        "INSERT INTO requirement (entry_id, req_id, on_designation, authority, water, "
        f"placement, uncertain, unresolved, {tail}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows["requirement"])
    db.executemany(
        "INSERT INTO licence_terms (entry_id, terms_id, document, classified, units, "
        f"{tail}) VALUES (?,?,?,?,?,?,?,?,?,?)", rows["licence_terms"])
    db.executemany(
        f"INSERT INTO exemption (entry_id, exemption_id, documents, {tail}) "
        "VALUES (?,?,?,?,?,?,?,?)", rows["exemption"])
    db.executemany(
        "INSERT INTO alternative (entry_id, alternative_id, alternative_to_entry, "
        f"alternative_to_id, placement, uncertain, unresolved, {tail}) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows["alternative"])
    for t, rs in rows.items():
        cov.filled(t, len(rs))

    # The register — emitted from the enum, so a document the corpus can name is one the
    # bundle can name (AGENTS 40: one source, generated across the boundary).
    db.executemany("INSERT INTO licence (doc_id, name, provincial) VALUES (?,?,?)",
                   [(d.value, _DOC_WORDS.get(d.value, d.value.replace("_", " ")),
                     1 if d.value in PROVINCIAL_ANGLER_DOCUMENTS else 0) for d in Document])
    cov.filled("licence", len(Document))

    # ---- the interned sets, exactly as rules.intern_sets does it ----------------------
    intern: dict[frozenset, int] = {}
    sets: list[list[tuple[str, str, str, str]]] = []
    section_set: dict[str, int] = {}
    for s in sorted(by_section):
        key = frozenset(by_section[s])
        got = intern.get(key)
        if got is None:
            got = intern[key] = len(sets)
            sets.append(sorted(key))
        section_set[s] = got
    db.executemany("INSERT INTO section_licensing (sid, set_id) VALUES (?,?)",
                   [(sid[k], v) for k, v in section_set.items()])
    cov.filled("section_licensing", len(section_set))
    db.executemany("INSERT INTO licensing_set (set_id, kind, entry_id, record_id, via) "
                   "VALUES (?,?,?,?,?)",
                   ((i, k, e, r, v) for i, rs in enumerate(sets) for k, e, r, v in rs))
    cov.filled("licensing_set", sum(len(s) for s in sets))

    # EVERY SET ROW NAMES A RECORD THAT EXISTS, in the table of its kind — checked against the
    # rows just written, not against the input they were written from.
    orphans = db.execute(
        "SELECT ls.kind, ls.entry_id, ls.record_id FROM licensing_set ls WHERE NOT EXISTS ("
        " SELECT 1 FROM designation d WHERE ls.kind='designation' AND d.entry_id=ls.entry_id"
        "   AND d.designation_id=ls.record_id"
        " UNION ALL SELECT 1 FROM not_classified n WHERE ls.kind='not_classified'"
        "   AND n.entry_id=ls.entry_id AND n.not_classified_id=ls.record_id"
        " UNION ALL SELECT 1 FROM requirement q WHERE ls.kind='requirement'"
        "   AND q.entry_id=ls.entry_id AND q.req_id=ls.record_id"
        " UNION ALL SELECT 1 FROM alternative a WHERE ls.kind='alternative'"
        "   AND a.entry_id=ls.entry_id AND a.alternative_id=ls.record_id) LIMIT 5").fetchall()
    if orphans:
        raise SystemExit(f"licensing_set names records that are not in the bundle: {orphans}")

    # ---- the report a person reads ------------------------------------------------------
    by_place = Counter((p["kind"], p["placement"]) for p in placed.values())
    via = Counter(v for rs in by_section.values() for _, _, _, v in rs)
    print("     licensing: " + " · ".join(f"{t} {len(rs)}" for t, rs in rows.items())
          + f" · {len(section_set):,} sections carry one, sharing {len(sets):,} sets")
    print("       placement: " + ", ".join(f"{k}/{p} {n}" for (k, p), n in sorted(by_place.items())))
    print("       via (section bindings): " + ", ".join(f"{k} {n:,}" for k, n in sorted(via.items())))
    for p in sorted(placed.values(), key=lambda p: (p["entry_id"], p["record_id"])):
        if p["placement"] == "unresolved":
            print(f"       UNRESOLVED {p['kind']} {p['entry_id']}#{p['record_id']}: {p['reason']}")
    for (d, n), v in sorted(conflicts.items()):
        print(f"       contested: {d[0]}#{d[1]} vs {n[0]}#{n[1]} on {len(v)} section(s) — "
              f"{ACKNOWLEDGED_CONFLICTS[(d, n)]}")
    if ambiguous:
        print(f"       ambiguous units (validator 6, reported): {len(ambiguous)} designation "
              f"pair(s) share sections on overlapping dates")
        for (a, b), n in ambiguous.most_common(8):
            print(f"         {a[0]}#{a[1]} ({records[a][1].unit}) / {b[0]}#{b[1]} "
                  f"({records[b][1].unit}): {n:,} sections")

    # THE CLASSIFIED GLYPH IS A CROSS-CHECK, NEVER THE BINDING (user decision 8). Reported both
    # ways: a glyph with no designation, and a designation with no glyph.
    glyph_only = sorted(ce.entry_id for ce in entries if "Classified" in ce.symbols
                        and not any(isinstance(x, Designation) for x in ce.licensing))
    desig_only = sorted(ce.entry_id for ce in entries if "Classified" not in ce.symbols
                        and any(isinstance(x, Designation) for x in ce.licensing))
    print(f"       symbol cross-check: {len(glyph_only)} (CW) glyph(s) with no designation"
          + (f" {glyph_only}" if glyph_only else "")
          + f"; {len(desig_only)} designation entr(ies) with no glyph")
