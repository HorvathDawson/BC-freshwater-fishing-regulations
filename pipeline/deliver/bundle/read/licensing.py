"""Licensing: which requirements are in force on a section, on a day.

The reference reader for WHERE AND WHEN a requirement holds — the angler is unknown, so `who` and `doing` are
left for the sentence (app/packages/core/src/regulations.ts) and every record that holds is returned.
Licensing never votes on open/closed: this answers what holds while the water is open, and a closed water
needs no licence by construction.
"""
from __future__ import annotations

import json
from pipeline.common import entry_id as EID
from pipeline.deliver.bundle.read.facts import in_force, not_yet_mapped
from pipeline.deliver.bundle.read.ladder import base_region


def designations_in_force(db, section: int, on) -> list[dict]:
    """The designations bound to `section` that are in force on the day: their `when` holds
    (a `part` day counts — it holds at some hours), and no `suspended_while` closure of their entry
    binds the section that day. Each is its record, with `entry_id` and `id`. Public: the licence
    answer reads it for the designation boxes and for which `licence_terms` (classified class,
    units) price the water (consumer 7.7 step 3)."""
    out = []
    rules_here: set | None = None
    for eid, did, rec in db.execute(
            "SELECT d.entry_id, d.designation_id, d.record FROM designation_section ds "
            "JOIN designation d ON d.entry_id = ds.entry_id AND d.designation_id = "
            "ds.designation_id WHERE ds.sid = ?", (section,)):
        r = json.loads(rec)
        if in_force(r.get("when"), on) == "no":
            continue
        asleep = False
        for s in r.get("suspended_while") or []:
            if rules_here is None:
                rules_here = {(e, x) for e, x in db.execute(
                    "SELECT rs.entry_id, rs.rule_id FROM section_ruleset sr JOIN ruleset rs "
                    "ON rs.set_id = sr.set_id WHERE sr.sid = ?", (section,))}
            if (eid, s["rule_id"]) in rules_here:
                w = db.execute("SELECT when_ FROM rule WHERE entry_id = ? AND rule_id = ?",
                               (eid, s["rule_id"])).fetchone()
                if in_force(json.loads(w[0]) if w and w[0] else None, on) == "yes":
                    asleep = True
        if not asleep:
            out.append(dict(r, entry_id=eid, id=did))
    return out


def stamp_waived_here(db, section: int, on) -> list[str]:
    """The designations (`entry_id#id`) that waive the Steelhead Stamp OUTRIGHT on this section
    on this day — "(Steelhead Stamp not required)", in force (`Designation.waives_every_stamp`).
    Where one holds, no steelhead stamp is required (RULINGS S8)."""
    from pipeline.regs.parsing.catalogue import Designation
    return sorted(f"{d['entry_id']}#{d['id']}" for d in designations_in_force(db, section, on)
                  if Designation.model_validate({k: v for k, v in d.items()
                                                 if k != "entry_id"}).waives_every_stamp)


def steelhead_stamp_period(desig, on) -> bool:
    """IS IT THE CLASSIFIED-WATER STEELHEAD STAMP'S PERIOD here on `on`: a designation in force (`desig`,
    `designations_in_force`) carries a `steelhead_stamp_during` whose `when` is not out of force that day. ONE answer
    (T125 item 8, answers 10): the requirement reader's `steelhead_period` and the licence answer's `stamp_period`
    each wrote it."""
    return any(d.get("steelhead_stamp_during") is not None
               and in_force(d["steelhead_stamp_during"].get("when"), on) != "no" for d in desig)


def requirements_in_force(db, section: int, on) -> dict:
    """Every requirement in force on `section` on the day, `{entry_id#req_id: why}` under
    `"holds"`, and the ones an outright stamp waiver lifts there, under `"waived"`:
    `{entry_id#req_id: [designation]}`;
    under `"also_printed"`, `{entry_id#req_id: [entry_id#req_id, …]}`, the records that RESTATE a
    holding one (one obligation printed twice is one key; RULINGS Li12) — and a zone table's
    `on_designation` restatement holds on its own region's designations only.

    A requirement holds where it is placed (`requirement_section`; `province` everywhere but its
    `province_except` kind and tidal water; `on_designation` wherever a designation satisfying its
    `on` is in force), on the days of its `when`; one with `on` and sections needs that designation
    in force too. `waived_where: steelhead_stamp_waived` lifts it where `stamp_waived_here` holds.

    A REQUIREMENT IN A PART NOBODY HAS DRAWN (`undrawn_part`: the Creston Valley WMA permit on
    the south end of Kootenay Lake's Main Body) never holds of the section: it is listed under
    `"not_yet_mapped"`, `{entry_id#req_id: part}`, to be SHOWN on the water, as a rule in an undrawn
    part is (`not_yet_mapped`).

    A REQUIREMENT FOR THE OTHER KIND OF WATER DOES NOT HOLD (RULINGS Li8): one printed
    for streams (`water: stream` — the province's Classified Waters Licence,
    `zp:steelhead#steelhead_targeting`) does not hold on a lake or a wetland. A record that
    RESTATES another binds as that record (`as_bound`: its `who`, `satisfied_by` and `water` —
    one obligation). The section's kind is `section_kind`: its named water's (`item.kind`), else its
    unnamed part's (the atlas's node kind, S1); only a section with neither keeps it. Listed under
    `"wrong_water"`, `{entry_id#req_id: the water it is for}`, as `waived` lists the waived ones.

    A SUPERIOR AUTHORITY'S REQUIREMENT DISPLACES THE PROVINCIAL ONES (RULINGS Li8): where
    a requirement with `authority: superior` holds (a national park's fishing permit), every other
    holding requirement that has a way to satisfy it (`satisfied_by`, as bound) is NOT VALID there —
    "a federal or park authority's requirement replaces provincial licences". It is moved to
    `"displaced"`, `{entry_id#req_id: [superior entry_id#req_id, …]}`. (`province_except` already
    keeps the province-placed records out of national parks; this reaches the sections- and
    designation-placed ones.)"""
    desig = designations_in_force(db, section, on)
    have = {"classified_period": bool(desig), "steelhead_period": steelhead_stamp_period(desig, on)}
    # A ZONE TABLE'S RESTATEMENT HOLDS ON ITS OWN REGION'S DESIGNATIONS ONLY (RULINGS Li12):
    # Region 4's "Classified Waters Licence … many East Kootenay rivers (map p.33)" is Region 4's
    # wording and held on the Dean (Region 5) and the Seymour (Region 1) beside the province's.
    desig_regions = {EID.region_of(d["entry_id"]) for d in desig}

    def own_region(eid: str) -> bool:
        r = base_region(eid)
        return r is None or EID.book_region(r) in desig_regions
    placed = {(e, r) for e, r in db.execute(
        "SELECT entry_id, req_id FROM requirement_section WHERE sid = ?", (section,))}
    tidal = db.execute("SELECT 1 FROM tidal WHERE sid = ?", (section,)).fetchone() is not None
    excepted = {k for (k,) in db.execute(
        "SELECT area_kind FROM province_except WHERE sid = ?", (section,))}
    waivers = stamp_waived_here(db, section, on)
    holds: dict[str, str] = {}
    waived: dict[str, list[str]] = {}
    undrawn: dict[str, str] = {}
    restated: dict[str, str] = {}
    records = {f"{e}#{r}": (e, r, p, json.loads(rec)) for e, r, p, rec in db.execute(
        "SELECT entry_id, req_id, placement, record FROM requirement")}
    for eid, rid, placement, r in records.values():
        if placement == "sections":
            if (eid, rid) not in placed:
                continue
            why = "sections"
        elif placement == "province":
            kinds = {x.get("outside_area_kind") for x in r.get("extents") or []} - {None}
            if tidal or kinds & excepted:
                continue
            why = "province"
        elif placement == "on_designation":
            if not own_region(eid):
                continue
            why = "on_designation"
        else:
            continue                                     # unresolved: the reader says "check"
        if r.get("on") and not have[r["on"]]:
            continue
        if in_force(r.get("when"), on) == "no":
            continue
        key = f"{eid}#{rid}"
        if not_yet_mapped(r):
            undrawn[key] = str(r["undrawn_part"]).strip()
            continue
        if r.get("waived_where") == "steelhead_stamp_waived" and waivers:
            waived[key] = waivers
            continue
        holds[key] = why
        if r.get("restates"):
            restated[key] = f"{r['restates']['entry_id']}#{r['restates']['id']}"
    # ONE OBLIGATION, PRINTED TWICE, IS ONE KEY (RULINGS Li12): a record that `restates`
    # another (Region 4's Classified Waters Licence line, the Shuswap row's char stamp, the
    # Creston permit on the CVWMA row) is folded into the record it restates when that one holds
    # too — listed under `also_printed` on the restated key, not as an obligation of its own.
    also: dict[str, list[str]] = {}
    for key, of in sorted(restated.items()):
        if of in holds and of != key:
            also.setdefault(of, []).append(key)
            holds.pop(key)
    # THE OTHER KIND OF WATER (G5): read on the obligation as bound — after the fold, so a
    # restatement goes where the record it restates goes
    kind = section_kind(db, section)
    every_req = {x: v[3] for x, v in records.items()}
    bound = {k: as_bound(records[k][3], every_req) for k in holds}
    wrong: dict[str, str] = {}
    if kind is not None:
        for k in list(holds):
            w = bound[k].get("water")
            if w and w != kind:
                wrong[k] = w
                holds.pop(k)
    # A SUPERIOR AUTHORITY DISPLACES (G5)
    superior = sorted(k for k in holds if records[k][3].get("authority") == "superior")
    displaced: dict[str, list[str]] = {}
    if superior:
        for k in list(holds):
            if records[k][3].get("authority") != "superior" and bound[k].get("satisfied_by"):
                displaced[k] = superior
                holds.pop(k)
    also = {k: v for k, v in also.items() if k in holds or k in displaced}
    return {"holds": holds, "waived": waived, "not_yet_mapped": undrawn, "also_printed": also,
            "wrong_water": wrong, "displaced": displaced,
            "considered": considered_records(db, section, tidal, excepted)}


#: Every licensing table and its id column: the records a section's sources may list.
LICENSING_TABLES = (("requirement", "req_id"), ("designation", "designation_id"),
                    ("exemption", "exemption_id"), ("alternative", "alternative_id"),
                    ("licence_terms", "terms_id"), ("not_classified", "not_classified_id"))


def considered_records(db, section: int, tidal: bool, excepted: set) -> list[str]:
    """EVERY RECORD A SECTION'S "ALL LICENCE SOURCES" LISTS (consumer 7.7 step 2, M4: decided here,
    beside the requirements in force, not again by the answers): its licensing set's records, in
    (entry, record) order, then every record placed `province`, `on_designation` or `not_placed`
    — unless an extent stops it at one of the section's province-exception kinds, or the water is
    tidal, where no province-wide record holds."""
    row = db.execute("SELECT set_id FROM section_licensing WHERE sid = ?", (section,)).fetchone()
    own = [f"{e}#{r}" for e, r in db.execute(
        "SELECT entry_id, record_id FROM licensing_set WHERE set_id = ? ORDER BY entry_id, "
        "record_id", (row[0],))] if row else []
    glob: list[str] = []
    if not tidal:
        recs = []
        for tab, idcol in LICENSING_TABLES:
            cols = [c[1] for c in db.execute(f"PRAGMA table_info({tab})")]
            for x in db.execute(f"SELECT * FROM {tab}"):
                d = dict(zip(cols, x))
                recs.append((f"{d['entry_id']}#{d[idcol]}", d.get("placement"),
                             json.loads(d["record"])))
        glob = [k for k, placement, r in sorted(recs, key=lambda t: t[0])
                if placement in ("province", "on_designation", "not_placed")
                and not any(x.get("outside_area_kind") in excepted for x in r.get("extents") or [])]
    return list(dict.fromkeys(own + glob))


def section_kind(db, section: int) -> str | None:
    """The WATER KIND of a section (lake, stream, wetland): the named water's (`item.kind`), else
    its unnamed part's (`unnamed_part.kind`: `reach.water_kind.kind_of`, the graph node's —
    RULINGS K9: "we definitely know this from the build"). None only for a section
    with neither (outside B.C., or a hand-built bundle with no places)."""
    got = db.execute("SELECT i.kind FROM item_section s JOIN item i ON i.ord = s.ord "
                     "WHERE s.sid = ?", (section,)).fetchone()
    if got:
        return got[0]
    got = db.execute("SELECT u.kind FROM section_ruleset r JOIN unnamed_part u ON u.up = r.up "
                     "WHERE r.sid = ?", (section,)).fetchone()
    return got[0] if got else None


def as_bound(r: dict, requirements: dict) -> dict:
    """A requirement as it binds: one that RESTATES another (`restates`) reads the restated record's
    `who`, `satisfied_by` and `water` — one obligation printed twice (RULINGS Li12). `requirements` is
    every requirement record by `entry_id#req_id`."""
    rs = r.get("restates")
    if rs:
        o = requirements.get(f"{rs['entry_id']}#{rs['id']}")
        if o:
            return dict(r, who=o.get("who"), satisfied_by=o.get("satisfied_by"),
                        water=o.get("water"))
    return r
