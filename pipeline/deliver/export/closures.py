"""What only the bundle's sets can say about closures.

`closures_combine` (a water's own closure and its zone's both hold), the gotchas added to `guide.gotchas`
(`with_closure_gotchas`), and the release/quota-under-closure scans (`_closure_scan`) the export refuses unless
each finding is listed as known (`RELEASE_UNDER_CLOSURE_KNOWN`, `QUOTA_UNDER_CLOSURE_KNOWN`).
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from pipeline.deliver import calendar as CAL
from pipeline.regs.parsing import catalogue as C
from pipeline.common import entry_id as EID
from pipeline.deliver.export.dictionary import _name
from pipeline.deliver.export.verdicts import answer_on, rule_keys_of, verdicts_of


def _bait_where(rules: dict, bait: dict) -> str:
    """Where the dated-bait-ban shape is printed today, read off the export (never a fixed list):
    the zones whose all-year bait ban a row's dated ban replaces, and the zones banning bait all
    year where no row does."""
    lifted = {n["zone_rule"] for e in bait.values() for n in e["notes"]}
    every = {i for i, x in rules.items() if EID.is_zone(x["entry_id"]) and x["type"] == "bait_restriction"
             and x.get("dimension") == "bait" and not x["fields"].get("when")}
    words = lambda ids: sorted({_zone_words(rules[i]["entry_id"]) for i in ids})  # noqa: E731
    have, rest = words(lifted), [w for w in words(every - lifted) if w not in words(lifted)]
    if not have:
        return ""
    out = (f" Today {', '.join(have)} print{'s' if len(have) == 1 else ''} this shape "
           f"({len(bait)} waters)")
    if rest:
        out += (f"; {', '.join(rest)} also ban bait all year, with no row printing a dated ban "
                f"on the same water")
    return out + "."


def with_closure_gotchas(g: dict, d: dict, bundle: Path) -> dict:
    """The two gotchas only the bundle's sets can say (`closures_combine`,
    `dated_bait_ban_replaces_zone`), added to `guide.gotchas` — generated, never hand-listed."""
    combine = closures_combine(bundle)
    bait = dated_bait_ban_replaces_zone(d["rules"])
    g["gotchas"]["closures_combine"] = {
        "says": "A water's OWN closure and its zone's seasonal closure (a spring, winter or "
                "summer stream closure; Region 6's steelhead closure) BOTH hold: the water is "
                "closed on the UNION of their dates (user ruling 2026-10-05). A row's closed "
                "season never shortens the zone's — the Stein's 'No Fishing Jan 1-May 31' "
                "under Region 3's 'No Fishing in any stream in Region 3 from Jan 1-June 30' is "
                "closed to June 30; the Nahatlatch below its lake likewise. A row beats the "
                "zone closure only where the book prints an exemption, or prints a dated "
                "catch and release, opening or quota inside the closure (then on exactly those "
                "dates, for exactly those fish: the Nicola below its lake, trout catch and "
                "release Jan 1-Feb 28) — and the bundle carries every such lift in `exempts`. "
                "On every entry listed here, show `notes[].says` beside the row's closure, so "
                "a reader never takes the row's dates for the whole closed season. A row's "
                "quota or release under the zone closure does not open the water either "
                "(kind `row_rule`).",
        "key_on": "entries[entry_id].notes[*] (kind row_closure | row_rule | subject_to); a "
                  "row_closure note's `zone_holds` are the days and fish on which the reader has "
                  "the zone closure speaking outside the row's dates (the only days it claims "
                  "both hold), its `zone_lifted` the zone's own days the reader lifts, for which "
                  "fish, by which rules; the answer itself already holds both closures (`ladder`)",
        "entries": combine,
    }
    g["gotchas"]["star_carries_exemptions"] = star_carries_exemptions(d, bundle)
    g["gotchas"]["farther_exemption_nearer_silent"] = farther_exemption_nearer_silent(bundle)
    g["gotchas"]["excepted_rivers_creeks_stay_closed"] = {
        "says": "A row closing its tributaries 'EXCEPT' some named rivers excepts those rivers' "
                "OWN channels only: a creek flowing into an excepted river is still a tributary of "
                "the row's water and stays closed (user ruling 2026-10-07, the Squamish row's 'No "
                "Fishing tributaries EXCEPT Ashlu, Cheakamus, Elaho, Mamquam, Powerhouse Channel', "
                "p.26). The excepted river follows its own row; never show its creeks as open "
                "because the river is.",
        "key_on": "rules[id].fields.tributary_excludes[*].walk_past == true: the excluded water "
                  "is out, the walk goes on above it",
        "entries": [e for e in EXCEPTED_RIVERS_CREEKS_CLOSED if e in d["entries"]],
    }
    binds: dict = {}
    for x in d["rules"].values():
        binds.setdefault(x["entry_id"], []).append(x.get("binds"))
    lost = sorted(i for i, e in d["entries"].items() if EID.is_water_row(i) and e.get("kind") == "water"
                  and not e.get("matched") and not e.get("item_id") and binds.get(i)
                  and all(b == "nowhere" for b in binds[i]))
    g["gotchas"]["not_located"] = {
        "says": "Rows the book prints whose water the map does not hold under that name in that "
                "management unit (no FWA feature, registry item or place name found): NOT LOCATED, "
                "NOT SHOWN on any water. Their rules bind nothing; list them where a reader searches "
                "by name, so an angler is never told such a water has only the zone rules.",
        "key_on": "entries[entry_id] with kind 'water', no matched and no item_id",
        "entries": lost,
    }
    notes = sorted(i for i in UNDRAWN_PART_TRIBUTARIES if i in d["rules"])
    g["gotchas"]["undrawn_part_tributaries"] = {
        "says": "A rule printed with ✱ (includes tributaries) whose own reach nothing draws — held on "
                "its water as an undrawn-part note — reaches the TRIBUTARIES OF THAT PART only, and "
                "those are not drawn either (user ruling 2026-10-07): the note says the part's "
                "tributaries are included; no tributary is coloured or closed by it. Show the note "
                "with that sentence on the water.",
        "key_on": "rules[id] listed here (an undrawn part whose rule reaches tributaries; "
                  "`UNDRAWN_PART_TRIBUTARIES`, pinned against the corpus by test_rules_round.py)",
        "rules": notes,
    }
    g["gotchas"]["dated_bait_ban_replaces_zone"] = {
        "says": "A row printing a DATED bait ban ('Bait ban, May 1-Nov 30') where its zone "
                "bans bait all year ('Bait ban: applies to all streams of Region 1, all year, "
                "with some important exceptions. Check the tables.') REPLACES the zone's ban on "
                "that water: bait is banned on the row's dates only (user ruling 2026-10-05). "
                "The row's lift of the zone ban (`exempts`, dated) carries it; never show the "
                "zone's all-year ban on the row's other days." + _bait_where(d["rules"], bait),
        "key_on": "entries[entry_id].notes[*]: the row's dated ban, the zone ban, the lift",
        "entries": bait,
    }
    return g


#: A DATED WATER RELEASE THAT SPEAKS WHILE A BLANKET CLOSURE OF ITS REGION ALSO SPEAKS can never
#: apply — it is exactly how a missed exemption shows (the Nicola, RU-2). The
#: export refuses one unless it is listed here with why it is benign: `(entry_id, rule_id)`.
RELEASE_UNDER_CLOSURE_KNOWN: dict[tuple[str, str], str] = {
    ("r4:duck_lake_permit_required_see_note_on_page_34@4-6", "duck_lake.r3"):
        "the Duck Lake row's bass catch and release (May 15-Jun 15) binds the STREAM sections of "
        "the entry's water (its tributary creeks), under Region 4's Apr 1-Jun 14 stream "
        "closure: both hold, and the creeks stay closed to Jun 14 (user decision 2026-10-05: "
        "the row's release does not lift the stream closure); the lake itself is not a stream",
}

#: A WATER ROW'S KEEPING QUOTA, IN FORCE, SILENCED WHILE A BLANKET ZONE CLOSURE SPEAKS (review F5).
#: Since RU-7 a full closure silences the keepers it beats whatever their key, so a missed
#: exemption shaped like a QUOTA (a row's "2 per day" the book means to hold through the zone's
#: closure) no longer shows as a contradiction on the page — it simply vanishes. The export
#: refuses one unless it is listed here with why the silence is the book: `(entry_id, rule_id)`.
QUOTA_UNDER_CLOSURE_KNOWN: dict[tuple[str, str], str] = {
    # Every reason below is read under the STRICT LIFT RULING (2026-10-05): a zone closure and a
    # water's row both hold — closed on the union of their dates — and a row beats the closure
    # only where the book prints an exemption or a dated release/opening/quota INSIDE it.
    ("r3:thompson_river_downstream_of_signs_at_kamloops_lake_outlet_t@3-13+3-14+3-18",
     "thompson_river_downstream_of_kamloops_lake.r2"):
        "the row's own Oct 1-May 31 closure and Region 3's Jan 1-Jun 30 spring stream closure "
        "both hold (the user's own example of the union); only the printed 'Additional opening "
        "May 1-31' lifts, on exactly those dates — so June is closed and the 2 per day holds "
        "from Jul 1",
    ("r3:nahatlatch_river@3-15", "nahatlatch_river.r3"):
        "Region 3's Jan 1-Jun 30 spring stream closure holds on the whole river: below the lake "
        "beside the row's own 'No Fishing Jan 1-May 31' (closed to Jun 30 — the p.28 list is a "
        "steelhead closure list, no exemption; user decision 2026-10-05), above it alone; the "
        "quota holds Jul 1-Dec 31",
    # (Mahood River r2 left this list 2026-10-06: under the DENETIAH ruling the row's own 'No
    # Fishing Jan 1-June 30' silences its quota — the water closing itself, not a zone closure
    # hiding a missed exemption; `_closure_scan` counts such a quota as speaking.)
    ("r6:west_road_blackwater_river_s_tributaries@6-1", "west_road_river_tributaries.r1"):
        "tributaries only: the mainstem row prints 'tributaries subject to spring closure', so "
        "Region 6's Fraser-watershed stream closure (Apr 1-Jun 30) holds on them and no lift "
        "reaches a tributary (user decision 2026-10-05); the 1 per day holds the rest of the "
        "year",
    ("r7:west_road_blackwater_river_s_tributaries@7-10",
     "west_road_blackwater_rivers_tributaries.r1"):
        "tributaries only: as Region 6's — Zone 7A's spring stream closure holds on them ("
        "'tributaries subject to spring closure'); the 1 per day holds the rest of the year",
    # (Duncan River r4/r5 left this list 2026-10-13: under T13 (owner D4, 2026-10-12, commit
    # ce74eaef) the ✱ row's "Exempt from spring closure" walks to its tributaries with its quotas,
    # so Region 4's stream closure no longer silences the 5 rainbow / 2 bull trout there.)
    ("r5:bowron_lake_park_waters_other_than_bowron_lake@5-16", "bowron_lake_park_waters.r1"):
        "the park's streams lying in Zone 7A fall under Zone 7A's spring stream closure: Region "
        "5's page exception (p.42) lifts only Region 5's closure, and the row prints no "
        "exemption Zone 7A's tables list; its lakes keep the 1 per day",
    ("r4:creston_valley_wildlife_management_area_cvwma_waters@4-6",
     "creston_valley_wma_waters.r1"):
        "bass 'no limit' on all CVWMA waters, undated: it lifts Region 4's bass CLOSURE (named "
        "fish), never the Apr 1-Jun 14 stream closure on its stream/slough sections (sloughs "
        "are streams, 2026-10-02) — both hold",
    ("r8:okanagan_river@8-1", "okanagan_river.r2"):
        "bass 8 per day, undated, on a stream under Region 8's spring stream closure outside "
        "the reaches the row prints exempt: both hold, closed Apr 1-Jun 30",
    ("r8:christina_creek@8-15", "christina_creek.r1"):
        "bass 8 per day, undated, under Region 8's spring stream closure: no printed exemption, "
        "both hold",
    ("r4:duck_lake_permit_required_see_note_on_page_34@4-6", "duck_lake.r1"):
        "the Duck Lake row's bass quota on its tributary creeks (STREAM sections) under Region "
        "4's Apr 1-Jun 14 stream closure: both hold, the creeks are closed to Jun 14 (user "
        "decision 2026-10-05: leave as built); the lake is not a stream",
    ("r4:duck_lake_permit_required_see_note_on_page_34@4-6", "duck_lake.r2"):
        "the same Duck Lake creeks: the bass size clause under the stream closure",
    ("r4:french_slough@4-7", "french_slough.r1"):
        "a slough is a stream (2026-10-02): Region 4's Apr 1-Jun 14 stream closure holds; the "
        "row's undated quota lifts the bass CLOSURE (named fish), not the stream closure",
    ("r4:lewis_cameron_slough@4-21", "lewis_cameron_slough.r1"):
        "a slough is a stream (2026-10-02): as French Slough",
    ("r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3", "kitimat_river.r5"):
        "the hatchery steelhead 2 is silenced by the row's OWN Mar 16-May 31 closure (RU-3) on "
        "the days Region 6's steelhead stream closure (May 15-Jun 15, its own exemption list "
        "printed — the Kitimat is not on it) also holds",
    # found by the every-day / every-fish sampling (review F6), the same class: a group-named
    # quota, for one fish, under a full zone closure OF THAT FISH on its dates
    ("r5:west_road_blackwater_river@5-12+5-13", "west_road_blackwater_river.r2"):
        "the trout/char 1 per day, asked for a steelhead May 15-Jun 15: Region 6's steelhead "
        "stream closure holds on the river's Region 6 pieces (its exemptions are the Skeena, "
        "Nass, Iskut, Stikine and Taku mainstems only, p.49); the row lifts nothing for it",
    ("r6:station_creek@6-9", "station_creek.r2"):
        "the trout/char 1 per day, for a steelhead under Region 6's May 15-Jun 15 steelhead "
        "stream closure: 'open all year' lifts only the full winter closure, never a species "
        "closure (G4, user decision 2026-10-05)",
    ("r6:hevenor_mcqueen_creek@6-30", "hevenor_creek.r2"):
        "as Station Creek: 'open all year' never lifts Region 6's steelhead stream closure",
    ("r6:two_mile_creek@6-8", "two_mile_creek.r2"):
        "as Station Creek: 'open all year' never lifts Region 6's steelhead stream closure",
    ("r5:bowron_lake_park_waters_other_than_bowron_lake@5-16", "bowron_lake_park_waters.r2"):
        "the park's possession clause beside its 1 per day (r1, above): the same Zone 7A spring "
        "stream closure on the park's streams",
    ("r4:creston_valley_wildlife_management_area_cvwma_waters@4-6",
     "creston_valley_wma_waters.r2"):
        "yellow perch 'no limit' beside the bass (r1, above): the same Region 4 Apr 1-Jun 14 "
        "stream closure on the CVWMA's stream/slough sections",
}


_SCAN: dict = {}


def _closure_scan(bundle: Path) -> dict:
    """ONE SCAN FOR BOTH CHECKS: every water-row (`r…`) retention rule that is a dated outright
    release or a keeping quota, on EVERY RULE KEY of each rule set it binds (DATAFLOW M8, decision
    U4: it used to read one section per set, whatever its steelhead flags), on EVERY day that may
    matter — the first day of each of its date ranges and the first day of each zone full
    closure bound on the same set (a release Jan 1-Feb 28 against a closure from Feb 1 is seen
    on Feb 1), wherever both are in force — for EACH fish it speaks for that a closure there
    speaks for. The answers are the stored verdicts' (`verdicts_of`); the reader is not called.
    Returns {"release": [...], "quota": [...]}, one finding per rule (its first: sets in order,
    each set's keys by their lowest section); the callers mark the known ones. A quota the
    WATER'S OWN full closure silences (`read` reason `water_closure`, the DENETIAH ruling) is no
    finding: the row closes its own water, no zone closure hides an exemption there."""
    from pipeline.deliver.bundle import read as RD
    from pipeline.deliver.bundle.rules import closure_grade, release_origins, keeps
    from pipeline.regs.parsing.catalogue import expand_species
    key = str(Path(bundle).resolve())
    if key in _SCAN:
        return _SCAN[key]
    R = RD._rules_of(str(bundle))
    store = verdicts_of(bundle)
    keys_of = rule_keys_of(bundle)
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        members: dict = defaultdict(list)
        for s, e, r in db.execute("SELECT set_id, entry_id, rule_id FROM ruleset"):
            if (e, r) not in R:
                raise SystemExit(f"pipeline.deliver.export: rule set {s} names {e}::{r}, not a rule")
            members[s].append((e, r))
    finally:
        db.close()

    def starts(x: dict) -> set:
        return {(d["from_month"], d["from_day"]) for d in (x.get("when") or {}).get("dates") or []}

    def fish_of(x: dict, asked) -> list:
        """The fish a rule speaks for, among those asked on the key: its named fish in the
        order it names them, then every other game fish (a rule naming none, or an open subject
        such as ALL_FIN_FISH, speaks for every game fish — `read.speaks_for`)."""
        order = list(dict.fromkeys(expand_species(list(x.get("species") or [])) + list(C.GAME_FISH)))
        return [f for f in order if f in asked and RD.speaks_for(x, f)]

    out: dict = {"release": [], "quota": []}
    seen: set = set()
    for s in sorted(members):
        shut_here = [k for k in members[s] if EID.is_table(str(k[0]))
                     and closure_grade(R[k]) == "full" and RD.base_region(k[0]) is not None
                     and not RD.not_yet_mapped(R[k])]
        if not shut_here:
            continue
        for key_ix, *_ in keys_of.get(s, []):
            asked = set(store.fish(key_ix))
            cands = []
            for k in sorted(members[s]):
                x = R[k]
                if k in seen or not EID.is_water_row(str(k[0])) or RD.not_yet_mapped(x) \
                        or closure_grade(x) is not None:
                    continue
                if release_origins(x) and starts(x):
                    cands.append(("release", k))
                elif keeps(x):
                    cands.append(("quota", k))
            cache: dict = {}
            for what, k in cands:
                x = R[k]
                days = starts(x) | {d for c in shut_here for d in (starts(R[c]) or {(1, 1)})}
                if not starts(x):
                    days.add((1, 1))
                hit = None
                for on in sorted(days):
                    if RD.in_force(x.get("when"), on) != "yes":
                        continue
                    shut_on = [c for c in shut_here if RD.in_force(R[c].get("when"), on) == "yes"]
                    if not shut_on:
                        continue
                    for fish in fish_of(x, asked):
                        if not any(RD.speaks_for(R[c], fish) for c in shut_on):
                            continue
                        # EVERY MOMENT of the key (weekday classes, inside / outside an hours
                        # window — `calendar.moments`; one for almost every key)
                        for m in range(len(store.moments(key_ix))):
                            if (on, fish, m) not in cache:
                                # a quota the WATER'S OWN closure silences (DENETIAH ruling,
                                # 2026-10-06) is the row closing itself, not a zone closure
                                # hiding a missed exemption — it counts as speaking here
                                cache[(on, fish, m)] = {
                                    tuple(y["id"].split("::", 1)): y
                                    for y in answer_on(store, key_ix, on, fish, trace=True,
                                                       moment=m)
                                    if y["state"] == "speaks" or y.get("reason") == "water_closure"}
                            got = cache[(on, fish, m)]
                            shut = [f"{e}::{r}" for (e, r) in sorted(got)
                                    if EID.is_table(str(e)) and closure_grade(R[(e, r)]) == "full"
                                    and RD.base_region(e) is not None]
                            # a release that SPEAKS under the closure; a quota in force that does
                            # NOT
                            if shut and ((k in got) if what == "release" else (k not in got)):
                                hit = (on, fish, shut)
                                break
                        if hit:
                            break
                    if hit:
                        break
                if hit:
                    seen.add(k)
                    on, fish, shut = hit
                    # what SHIPS (`_known`) names the RULE SET, never a section (AGENTS 5)
                    out[what].append({"key": k, "rule": f"{k[0]}::{k[1]}", "ruleset": str(s),
                                      "rule_key": key_ix,
                                      "date": list(on), "fish": fish, "closures": shut})
    _SCAN[key] = out
    return out


_MONTHS = CAL.MONTHS


def _runs_words(runs: list) -> str:
    return ", ".join(f"{_MONTHS[a - 1]} {b}" + ("" if (a, b) == (c, d) else
                                                 f"-{_MONTHS[c - 1]} {d}")
                     for a, b, c, d in runs) or "no day"


def _dates_words(x: dict) -> str:
    ds = (x.get("when") or {}).get("dates") or []
    return ", ".join(f"{_MONTHS[d['from_month'] - 1]} {d['from_day']}-"
                     f"{_MONTHS[d['to_month'] - 1]} {d['to_day']}" for d in ds) or "all year"


def _plain(text: str) -> str:
    """A verbatim without the book's bold markers, for a sentence that quotes it."""
    return (text or "").replace("**", "")


def _zone_words(entry_id: str) -> str:
    """`z3:…` -> "Region 3", `z7a:…` -> "Zone A" — whose closure it is, in the book's words."""
    r = EID.zone_region(entry_id) or ""
    if r and EID.book_region(r) != r:
        return f"Zone {r[-1].upper()}"
    return EID.region_words([r]) if r else "Region "


def closures_combine(bundle: Path) -> dict:
    """WHERE A WATER'S OWN CLOSURE AND A ZONE CLOSURE BOTH HOLD (user ruling 2026-10-05, the
    strict lift ruling): `{entry_id: {"name", "notes": [...]}}`, generated from the bundle.

    A zone's seasonal closure (a spring, winter or summer stream closure, Region 6's steelhead
    closure) and a row's own closure on the same water BOTH apply: the water is closed on the
    UNION of their dates. Where the row's closed season lies inside the zone's, or crosses it,
    a reader is easily misled into thinking the row's dates replace the zone's (the Stein's "No
    Fishing Jan 1-May 31" under Region 3's Jan 1-June 30: closed to June 30). Three shapes:

      `row_closure`  a row's closure (full, or partial: Shuswap Lake's "No Fishing Mar 15-May
                     14", which holds on a part nothing draws) bound beside a zone's full
                     SEASONAL closure on the same sections, for a fish both close, on days that
                     overlap, where the zone closes days the row does not. WHETHER THE ZONE
                     CLOSURE HOLDS IS THE READER'S (`read.effective_rules_bound`, once per rule
                     set, steelhead flags and day segment): the note claims "both hold" only on
                     the days and for the fish the zone closure SPEAKS outside the row's dates
                     (`zone_holds`); a pair where it never does (the Fulton, whose "Open June
                     16-Apr 30" lifts the Skeena winter closure on every day the row leaves
                     open) is no note. Where the reader shows the zone closure silenced inside
                     its own dates — a lift in part, dated or for some fish — the note names
                     those dates, fish and lifting rules (`zone_lifted`: the Nicola's trout
                     catch and release Jan 1-Feb 28; the Thompson's May opening, on the sections
                     it binds);
      `row_rule`     a row's dated release or keeping quota the reader silences under a zone
                     full closure (`_closure_scan`, the rows of `QUOTA_UNDER_CLOSURE_KNOWN` /
                     `RELEASE_UNDER_CLOSURE_KNOWN`): the row's rule does not open the water;
      `subject_to`   a row printing that its water, or its tributaries, are "subject to" a zone
                     closure (West Road River's "tributaries subject to spring closure").
    `closures_combine_problems` checks every `zone_holds` claim against the reader."""
    from pipeline.deliver.bundle import read as RD
    from pipeline.deliver.bundle.rules import closure_grade
    R = RD._rules_of(str(bundle))
    store = verdicts_of(bundle)
    keys_of = rule_keys_of(bundle)
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        bound: dict = defaultdict(list)
        for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset"):
            bound[s].append((e, r, via))
        names = dict(db.execute("SELECT entry_id, name FROM entry"))
        verbatim = dict(db.execute("SELECT entry_id, verbatim FROM entry"))
    finally:
        db.close()
    game = list(C.GAME_FISH)

    def days(x):
        got = RD._days_of(json.dumps((x.get("when") or {}).get("dates") or [], sort_keys=True))
        return got if got is not None else frozenset(CAL.MD)

    def fish(x):
        return {f for f in game if RD.speaks_for(x, f)}

    def zone_seasonal(k):
        x = R[k]
        return (RD.base_region(k[0]) is not None and closure_grade(x) == "full"
                and not RD.not_yet_mapped(x) and bool((x.get("when") or {}).get("dates")))

    def row_closure(k):
        return EID.is_water_row(str(k[0])) and closure_grade(R[k]) is not None

    # the candidate pairs per set, by their printed dates and fish (what could mislead)
    cands: dict = {}
    for s, rows in bound.items():
        ks = [(e, r) for e, r, _ in rows if (e, r) in R]
        zs = [k for k in ks if zone_seasonal(k)]
        if not zs:
            continue
        got = []
        for k in ks:
            if not row_closure(k):
                continue
            for z in zs:
                common = fish(R[k]) & fish(R[z])
                dk, dz = days(R[k]), days(R[z])
                if common and (dk & dz) and (dz - dk):
                    got.append((k, z, common))
        if got:
            cands[s] = got

    pairs: dict = {}
    every_key = sorted((s, sw, sr, k, n, rep_) for ks in keys_of.values()
                       for k, s, sw, sr, n, rep_ in ks)
    for s, sh_water, sh_rules, key_ix, nsec, sid in every_key:
        if s not in cands:
            continue
        rows = bound[s]
        # THE READINGS ARE THE VERDICTS': a day's answer is its reading's (`reading_of`), stored
        answer: dict = {}

        def speaks(d, f, key_ix=key_ix):
            # every moment of the key must say the same of the zone closure: a claim "both hold"
            # or "lifted" is made per day, and a day read two ways is refused, never guessed
            got = None
            for m in range(len(store.moments(key_ix))):
                r = store.reading_of(key_ix, d, m)
                if (r, f) not in answer:
                    answer[(r, f)] = {tuple(y["id"].split("::", 1))
                                      for y in answer_on(store, key_ix, CAL.MD[d], f, moment=m)
                                      if y["state"] == "speaks"}
                if got is not None and (z in got) != (z in answer[(r, f)]):
                    raise SystemExit(f"pipeline.deliver.export: closures_combine — {z} speaks at one "
                                     f"moment of rule key {key_ix} on day {d} and not at another")
                got = answer[(r, f)]
            return got

        for k, z, common in cands[s]:
            dk, dz = days(R[k]), days(R[z])
            holds: dict = defaultdict(set)      # fish -> days the zone speaks, the row closed not
            lifted: dict = defaultdict(set)     # fish -> days of the zone's own the reader lifts
            for f in sorted(common):
                for d in sorted(dz - {CAL.LEAP}):
                    if z in speaks(d, f):
                        if d not in dk:
                            holds[f].add(d)
                    else:
                        lifted[f].add(d)
            p = pairs.setdefault((k, z), {"sections": 0, "bound": 0, "_sid": None,
                                          "example": None,
                                          "holds": defaultdict(set), "lifted": {}})
            p["bound"] += nsec
            if any(holds.values()):
                p["sections"] += nsec
                # THE EXAMPLE IS A KEY, NEVER A SECTION (AGENTS 5): the (rule set, steelhead
                # water, steelhead rules) whose lowest section is the lowest the claim holds on —
                # `example_sid` resolves it from the bundle to re-ask the reader
                if p["_sid"] is None or (sid < p["_sid"]):
                    p["_sid"] = sid
                    p["example"] = {"ruleset": str(s), "anadromous_rainbow": bool(sh_water),
                                    "steelhead_rules": bool(sh_rules)}
            for f, ds in holds.items():
                p["holds"][f] |= ds
            by_days: dict = defaultdict(set)
            for f, ds in lifted.items():
                if ds:
                    by_days[frozenset(ds)].add(f)
            lifters = sorted(f"{e}::{r}" for e, r, _ in rows if (e, r) in R
                             and any((y.get("entry_id"), y.get("rule_id")) == z
                                     for y in R[(e, r)].get("exempts") or []))
            for ds, fs in by_days.items():
                q = p["lifted"].setdefault((ds, frozenset(fs)), {"sections": 0, "by": set()})
                q["sections"] += nsec
                q["by"].update(lifters)

    def fish_list(fs):
        return "all" if set(fs) >= set(game) else sorted(fs)

    def fish_words(fs):
        if fs == "all" or set(fs) >= set(game):
            return ""
        rest = set(game) - set(fs)
        if len(rest) < len(set(fs)):
            return " (for every game fish but " + ", ".join(_name(f) for f in sorted(rest)) + ")"
        return " (for " + ", ".join(_name(f) for f in sorted(fs)) + ")"

    out: dict = {}

    def note(eid, item):
        out.setdefault(eid, {"name": names.get(eid) or eid, "notes": []})["notes"].append(item)

    for (k, z), p in sorted(pairs.items()):
        if not p["sections"]:
            continue                # the zone closure never speaks outside the row's dates
        x, y = R[k], R[z]
        zw = _zone_words(z[0])
        groups: dict = defaultdict(set)
        for f, ds in p["holds"].items():
            if ds:
                groups[frozenset(ds)].add(f)
        zone_holds = [{"dates": _runs_words(CAL.runs(ds)), "runs": CAL.runs(ds), "fish": fish_list(fs)}
                      for ds, fs in sorted(groups.items(),
                                           key=lambda g: (min(g[0]), sorted(g[1])))]
        zone_lifted = [{"dates": _runs_words(CAL.runs(ds)), "runs": CAL.runs(ds), "fish": fish_list(fs),
                        "by": sorted(q["by"]), "sections": q["sections"]}
                       for (ds, fs), q in sorted(p["lifted"].items(),
                                                 key=lambda g: (min(g[0][0]), sorted(g[0][1])))]
        held = "; ".join(h["dates"] + fish_words(h["fish"]) for h in zone_holds)
        part = str(x.get("undrawn_part") or "").strip()
        part = f", on part of the water only: {part}" if part else ""
        says = (f"Both hold: the row's '{_plain(x['verbatim'])}' ({_dates_words(x)}{part}) and "
                f"{zw}'s '{_plain(y['verbatim'])}' ({_dates_words(y)}). {zw}'s closure still "
                f"holds on days the row's does not ({held}), so the water is closed on the "
                f"UNION of the two — the row's dates do not replace the zone's.")
        for lf in zone_lifted:
            by = ", ".join(f"'{_plain(R[tuple(b.split('::'))]['verbatim'])}'" for b in lf["by"]) \
                or "a lift"
            where = "" if lf["sections"] >= p["bound"] else \
                f" (on {lf['sections']:,} of its {p['bound']:,} sections)"
            says += (f" {zw}'s closure is lifted {lf['dates']}{fish_words(lf['fish'])} "
                     f"by {by}{where}.")
        note(k[0], {
            "kind": "row_closure", "row_rule": f"{k[0]}::{k[1]}", "row_says": x["verbatim"],
            "row_dates": _dates_words(x), "row_grade": closure_grade(x),
            "zone_rule": f"{z[0]}::{z[1]}", "zone_says": y["verbatim"],
            "zone_dates": _dates_words(y),
            "fish": fish_list(set().union(*groups.values())),
            "zone_holds": zone_holds, "zone_lifted": zone_lifted,
            "sections": p["sections"], "example": p["example"],
            "says": says})
    scan = _closure_scan(bundle)
    for what in ("release", "quota"):
        for f in scan[what]:
            k = tuple(f["key"])
            x = R[k]
            for c in f["closures"]:
                ze, zr = c.split("::")
                y = R[(ze, zr)]
                note(k[0], {
                    "kind": "row_rule", "row_rule": f"{k[0]}::{k[1]}", "row_says": x["verbatim"],
                    "row_dates": _dates_words(x), "zone_rule": c, "zone_says": y["verbatim"],
                    "zone_dates": _dates_words(y), "ruleset": f["ruleset"],
                    "says": f"The row's '{_plain(x['verbatim'])}' does not open the water while "
                            f"{_zone_words(ze)}'s '{_plain(y['verbatim'])}' ({_dates_words(y)}) is in "
                            f"force: both hold, and on those dates the water is closed"
                            f"{fish_words(fish(y))}."})
    subject = re.compile(r"(?:\w+\s+)?subject to (?:the )?(spring|summer|winter)[\w\s-]*closure",
                         re.I)
    for eid, v in sorted(verbatim.items()):
        m = subject.search(v or "")
        if m and EID.is_water_row(str(eid)):
            note(eid, {"kind": "subject_to", "row_says": m.group(0),
                       "says": f"The row prints '{m.group(0)}': the region's {m.group(1).lower()} "
                               f"closure holds there beside the row's own rules — both apply, "
                               f"and nothing in the row lifts it."})
    return out


def example_sid(example: dict, bundle: Path) -> int | None:
    """The lowest section of the bundle carrying a note's `example` key — (rule set, on steelhead
    water, steelhead rules apply) — the section the note was built on. Read here, in the bundle;
    never shipped (AGENTS 5)."""
    if not example.get("ruleset"):
        return None
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        return db.execute(
            "SELECT MIN(r.sid) FROM section_ruleset r WHERE r.set_id = ? "
            "AND (r.sid IN (SELECT sid FROM steelhead_water)) = ? "
            "AND (r.sid IN (SELECT sid FROM section_steelhead_rules)) = ?",
            (int(example["ruleset"]), int(bool(example.get("anadromous_rainbow"))),
             int(bool(example.get("steelhead_rules"))))).fetchone()[0]
    finally:
        db.close()


def closures_combine_problems(entries: dict, bundle: Path) -> list[str]:
    """EVERY "BOTH HOLD" CLAIM, CHECKED AGAINST THE READER: for each `row_closure` note, each of
    its `zone_holds` claims must have the zone closure SPEAKING (`read.effective_rules`) on the
    note's example section on at least one of the days it names, for one of the fish it names.
    A note built from the printed dates alone (the Fulton's "Both hold … Jan 1-Jun 15", where
    the row's "Open June 16-Apr 30" lifts the winter closure) fails here. The note names its
    example by key (`example`); the section is resolved here, from the bundle (`example_sid`)."""
    from pipeline.deliver.bundle import read as RD
    game = list(C.GAME_FISH)
    out = []
    for eid, e in sorted(entries.items()):
        for n in e["notes"]:
            if n["kind"] != "row_closure":
                continue
            z = n["zone_rule"]
            for h in n.get("zone_holds") or [{"runs": None, "fish": n.get("fish")}]:
                if not h.get("runs"):
                    out.append(f"{eid}: {n['row_rule']} under {z} claims both hold with no days")
                    continue
                fs = game if h["fish"] == "all" else h["fish"]
                sid = example_sid(n.get("example") or {}, bundle)
                hit = sid is not None and any(
                    f"{y['entry']}::{y['rule']}" == z and y["state"] == "speaks"
                    for d in CAL.run_days(h["runs"]) for f in fs
                    for y in RD.effective_rules(sid, CAL.MD[d], f, str(bundle)))
                if not hit:
                    out.append(f"{eid}: {n['row_rule']} — {z} does not speak on the example "
                               f"{n.get('example')} on any of {h['dates']} for {h['fish']}, "
                               f"yet the note says both hold")
    return out


#: THE UNDRAWN-PART RULES WHOSE ROW OR RULE INCLUDES TRIBUTARIES (user ruling 2026-10-07): held on
#: their water as a note, they no longer walk (`reach.classify.UNDRAWN_PART_DOES_NOT_WALK`), so the
#: tributaries of their part are named here. The bundle does not carry a rule's tributary scope;
#: `test_rules_round.py` re-derives this list from the corpus and refuses a drift.
UNDRAWN_PART_TRIBUTARIES = (
    # (owner 2026-10-27) Deena r1 and Mamin r3 are drawn now (low-confidence bridge cuts): they walk their tributaries
    "r7:dinosaur_lake_reservoir_downstream_of_w_a_c_bennett_dam@7-31::dinosaur_lake.r1",
)

#: Rows closing their tributaries EXCEPT named rivers, whose excepted rivers' creeks stay closed
#: (user ruling Q6, 2026-10-07; `tributary_excludes` with `walk_past`). Pinned against the corpus by
#: test_rules_round.py (the bundle does not carry `tributary_excludes`).
EXCEPTED_RIVERS_CREEKS_CLOSED = ("r2:squamish_river_s_tributaries@2-6",)


def star_carries_exemptions(d: dict, bundle=None) -> dict:
    """A ✱ ROW CARRIES ITS EXEMPTIONS TO ITS TRIBUTARIES (owner ruling D4, 2026-10-12, revising Q3 of
    2026-10-07): the row's ✱ (p.4: "all regulations cited apply to both the named body of water and
    its tributaries") carries a closure exemption the row prints like everything else it prints — a
    small Quinsam tributary is open Jul 15-Aug 31 like the Quinsam, which is "Exempt from July 15-Aug
    31 summer closure". A row WITHOUT the ✱ never lifts a closure on a tributary (L7). GENERATED from
    the export and the bundle: every lift-only rule of a ✱ row, and whether the walk bound it (`walks`
    — a rule that does not walk is a defect the reach run would show)."""
    walkers = {i for i, e in d["entries"].items() if "Incl. Tribs" in (e.get("symbols") or [])}
    walked: set = set()
    if bundle is not None:
        db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
        try:
            walked = {f"{e}::{r}" for e, r in db.execute(
                "SELECT DISTINCT entry_id, rule_id FROM ruleset WHERE via = 'trib'")}
        finally:
            db.close()
    # a lift-only rule (no number, no sizes) of a ✱ row that binds sections
    lifts = sorted(i for i, x in d["rules"].items() if x["entry_id"] in walkers
                   and x["fields"].get("exempts") and x.get("binds") == "sections"
                   and x["type"] == "retention_limit" and x["fields"].get("take") is None
                   and not x["fields"].get("unlimited") and not x["fields"].get("lengths"))
    return {
        "says": "A row marked ✱ (includes tributaries) carries an exemption it prints to its "
                "tributary streams too (owner ruling 2026-10-12): 'Exempt from July 15-Aug 31 "
                "summer closure', 'exempt from spring closure', 'open all year', 'Open June 16-Apr "
                "30' open the row's tributaries on the same days as its own water (`rules` lists "
                "every one). An exemption scoped to a part of the water (a named channel) opens "
                "only that part's tributaries. A row without ✱ never opens a tributary.",
        "key_on": "rules[id].fields.exempts on an entry whose symbols hold 'Incl. Tribs'; the "
                  "reach binds them on the tributaries (`walks`), so an answer there carries them",
        "rules": lifts,
        "walks": {i: i in walked for i in lifts} if bundle is not None else {},
    }


def farther_exemption_nearer_silent(bundle) -> dict:
    """OWNER RULING Q1 (2026-10-16, RULINGS T15): a farther ✱ row's printed exemption reaches ALL its
    tributaries — kept — including streams a NEARER water row binds (a nearer ✱ row's walk, or the stream's own
    row) that prints no such exemption; the owner is unsure, so it is SAID, on those waters. GENERATED from the
    bundle's rule sets (`nearer.silent_nearer`, the one predicate the answers' `display.inherited_caution`
    asks): per walked lifting rule, the silent nearer rows and the waters where both bind."""
    from pipeline.deliver.answers import common as AC
    from pipeline.deliver.bundle import nearer as NR, read as RD
    B = AC.load(str(bundle))
    pairs = RD._nearer_of(B.path)
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        items: dict = {}
        for set_id, item in db.execute(
                "SELECT DISTINCT s.set_id, i.item_id FROM section_ruleset s JOIN item_section x ON x.sid = s.sid "
                "JOIN item i ON i.ord = x.ord"):
            items.setdefault(set_id, set()).add(item)
    finally:
        db.close()
    got: dict = {}
    for set_id, bound in sorted(B.sets.items()):
        for e, r, via in bound:
            if via != "trib" or not B.rules[(e, r)].get("exempts"):
                continue
            for row, how in NR.silent_nearer(bound, B.rules, pairs, f"{e}::{r}"):
                x = got.setdefault(f"{e}::{r}", {"silent": {}, "items": set()})
                x["silent"][row] = how
                x["items"] |= items.get(set_id, set())
    return {
        "says": "A row marked ✱ carries an exemption it prints (a closure lifted, a release lifted, a size "
                "limit lifted) to ALL its tributaries — also to streams a NEARER row covers that prints no such "
                "exemption: a river's spring-closure exemption reaches the tributaries of a lake on that river "
                "though the lake's own ✱ row prints none (owner ruling 2026-10-16: kept, and said on those "
                "waters; `rules` lists every case). Show each answer's "
                "`inherited[].caution` beside the inherited line; it names both rows.",
        "key_on": "rules listed here (a walked lifting rule) × `silent` (each nearer water row printing no "
                  "such exemption: nearer_row — its walk reaches the stream and its water drains into the "
                  "lifter's; own_row — the stream's own row) × `items` (the waters both bind)",
        "rules": {k: {"silent": dict(sorted(v["silent"].items())), "items": sorted(v["items"])}
                  for k, v in sorted(got.items())},
    }


def dated_bait_ban_replaces_zone(rules: dict) -> dict:
    """WHERE A ROW'S DATED BAIT BAN REPLACES A ZONE'S ALL-YEAR ONE (user ruling 2026-10-05, G5):
    `{entry_id: {"name", "notes": [...]}}`, generated from the export's rules. Region 1's
    "Bait ban: applies to all streams of Region 1, all year, with some important exceptions.
    Check the tables" (p.13) is excepted by a row printing its own dated ban — the Quatse's
    "Bait ban, May 1-Nov 30" bans bait on those dates only. The lift (a row rule lifting a zone
    bait ban that has no dates, on the days the row's ban is not in force) is what says so; this
    names, per entry, the row's ban and the zone ban it replaces, so a page never shows the
    zone's all-year ban as holding on the row's other days. Unlike a closure, the row's dates DO
    replace the zone's here (closures never: `closures_combine`)."""
    out: dict = {}
    for i, x in sorted(rules.items()):
        if not EID.is_water_row(x["entry_id"]) or x["type"] != "bait_restriction":
            continue
        for y in x["fields"].get("exempts") or []:
            z = rules.get(f"{y['entry_id']}::{y['rule_id']}")
            if not z or not EID.is_table(z["entry_id"]) or z["type"] != "bait_restriction" \
                    or z["fields"].get("when"):
                continue
            bans = [b for b in rules.values() if b["entry_id"] == x["entry_id"]
                    and b["type"] == "bait_restriction" and b["fields"].get("when")
                    and not b["fields"].get("exempts")]
            if not bans:        # an undated "EXEMPT from bait ban" (Zone 7A) replaces nothing
                continue
            e = out.setdefault(x["entry_id"], {"name": x["provenance"]["entry_name"],
                                               "notes": []})
            for b in bans:
                e["notes"].append({
                    "row_rule": b["id"], "row_says": b["verbatim"],
                    "row_dates": _dates_words(b["fields"]), "zone_rule": z["id"],
                    "zone_says": z["verbatim"], "lift": i,
                    "lift_dates": _dates_words(y),
                    "says": f"The row's '{_plain(b['verbatim'])}' bans bait on its own dates only "
                            f"({_dates_words(b['fields'])}): it REPLACES "
                            f"{_zone_words(z['entry_id'])}'s '{_plain(z['verbatim'])}' on this water, "
                            f"which does not hold here {_dates_words(y)} (lifted by `{i}`)."})
    return out


def release_under_closure(bundle: Path) -> list[dict]:
    """EVERY DATED WATER-ROW RELEASE (`rules.release_origins`: take 0, may be fished for) that
    SPEAKS on a section and day where a FULL BLANKET CLOSURE of a zone table also speaks (every
    rule set it binds, every start day of it and of the closures, every fish it names —
    `_closure_scan`). Such a release can never apply (the water is closed), so it is a missed
    exemption or a curation slip, never a reading. `problems` refuses any not in
    `RELEASE_UNDER_CLOSURE_KNOWN`."""
    return _known(_closure_scan(bundle)["release"], RELEASE_UNDER_CLOSURE_KNOWN)


def quota_under_closure(bundle: Path) -> list[dict]:
    """EVERY WATER-ROW KEEPING QUOTA (`rules.keeps`) IN FORCE on a section and day —
    bound there, its dates holding: the reader's step 4 INPUT — that the reader SILENCES while
    a full blanket closure of a zone table speaks (`_closure_scan`). The page shows the closure
    alone; were the row meant to keep its quota through the closure (a missed exemption shaped
    like a quota, as the Stein's was a release), nothing else would say so. `problems` refuses
    any not in `QUOTA_UNDER_CLOSURE_KNOWN`."""
    return _known(_closure_scan(bundle)["quota"], QUOTA_UNDER_CLOSURE_KNOWN)


def _known(found: list[dict], known: dict) -> list[dict]:
    return [{**{a: b for a, b in x.items() if a not in ("key", "rule_key")},
             **({"known": known[x["key"]]} if x["key"] in known else {})} for x in found]


def release_under_closure_problems(doc: dict) -> list[str]:
    return ([f"a dated water release speaks under a blanket closure and is not listed as known: "
             f"{x['rule']} on ruleset {x['ruleset']} {x['date']} {x['fish']} with {x['closures']}"
             for x in doc["about"].get("release_under_closure") or [] if not x.get("known")]
            + [f"a water row's quota in force is silenced under a blanket closure and is not "
               f"listed as known: {x['rule']} on ruleset {x['ruleset']} {x['date']} {x['fish']} "
               f"with {x['closures']}"
               for x in doc["about"].get("quota_under_closure") or [] if not x.get("known")])
