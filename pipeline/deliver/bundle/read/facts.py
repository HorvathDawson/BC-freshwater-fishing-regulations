"""What one rule says, and what one section is — the facts the ladder asks.

Whether a rule is in force on a day and at a moment (`in_force`), which fish it speaks for or names
(`speaks_for`, `names_fish`), the lengths it covers (`covers`), and a section's steelhead presence, home
region and straddled regions, all read from the bundle."""
from __future__ import annotations

import json
from functools import lru_cache


def _day(on) -> int:
    """`datetime.date` or `(month, day)` -> the catalogue's day index (1..366): the delivery's one
    calendar (`pipeline.deliver.calendar.day_of`)."""
    from pipeline.deliver.calendar import day_of
    return day_of(on)


#: What `in_force` answers, in wire order (`types.InForce`: 0 no, 1 yes, 2 part).
IN_FORCE = ("no", "yes", "part")


@lru_cache(maxsize=None)
def _days_of(dates_json: str):
    from pipeline.regs.parsing.catalogue import DateRange, _days
    dates = [DateRange.model_validate(d) for d in json.loads(dates_json)]
    return frozenset(_days(dates)) if dates else None


def in_force(when: dict | None, on, at=None) -> str:
    """Whether a `when` (the bundle's JSON) holds on a day, at a MOMENT: "yes", "no", or "part".

    `at` is a `calendar.Moment` — a class of weekdays and, where the key holds an hours rule,
    inside or outside its window (`calendar.moments`). Asked at a moment, a weekday or hours rule
    DECIDES (RULINGS D4): "yes" on the weekdays and hours it covers,
    "no" at the others — Kootenay Lake's Lower West Arm keeps 5 kokanee on a Saturday and releases
    them on a Monday; the Fraser above Mission is closed from one hour after sunset to one hour
    before sunrise and open by day. A moment that straddles a rule's weekdays, or names another
    hours window, is refused: `moments` cuts every key so that none does.

    Without a moment (`at` None: the day only), such a rule reads "part", as does a season that
    could not be read (`unparsed`, always): shown BESIDE what it would displace, displacing
    nothing — the export's own scans, which ask a day and not a moment, still read it so."""
    if not when:
        return "yes"
    days = _days_of(json.dumps(when.get("dates") or [], sort_keys=True))
    if days is not None and _day(on) not in days:
        return "no"
    if when.get("unparsed"):
        return "part"
    wd, hr = when.get("weekdays"), when.get("hours")
    if not (wd or hr):
        return "yes"
    if at is None:
        return "part"
    from pipeline.deliver.calendar import ALL_WEEK, hours_key, weekday_mask
    if wd:
        m = weekday_mask(wd)
        if not at.weekdays & m:
            return "no"
        if at.weekdays & ~m & ALL_WEEK:
            raise ValueError(f"in_force: moment {at} straddles the weekdays {wd}")
    if hr:
        if at.hours != hours_key(hr):
            raise ValueError(f"in_force: moment {at} does not cut the hours window {hr}")
        if not at.inside:
            return "no"
    return "yes"


def speaks_for(rule: dict, fish: str) -> bool:
    """Whether a rule says anything about ONE fish (a leaf code of the book's list, "DV"). A rule
    that names no species binds whatever you catch; `ALL_FIN_FISH` is every fish but crayfish;
    the other open subjects (`PROTECTED_SPECIES`, `SALMON`) hold no game fish and so speak for
    none — but `SALMON` speaks for a salmon the book names (`catalogue.SALMON_FISH`: chinook);
    otherwise the fish must be in the expansion of `species` and not of `species_except`. A
    bait or tackle rule for a TARGET speaks only when the fish asked about is that target."""
    from pipeline.regs.parsing.catalogue import SALMON_FISH, expand_species
    tgt = rule.get("when_targeting") or []
    if tgt and fish not in expand_species(list(tgt)):
        return False
    sp = list(rule.get("species") or [])
    if not sp:
        return True
    if fish in expand_species(list(rule.get("species_except") or [])):
        return False
    if "ALL_FIN_FISH" in sp:
        # "Fin fish" is not crayfish: "release all fin fish caught in your trap" keeps the
        # crayfish, and `rules._lift_terms` leaves a crayfish quota standing under "all fish".
        return fish != "CRA"
    if fish in SALMON_FISH and SALMON_FISH[fish] in sp:
        return True
    return fish in expand_species(sp)


def names_fish(rule: dict, fish: str) -> bool:
    """Whether a rule NAMES this fish rather than a group holding it. "Bull trout … release" names
    the Dolly Varden/bull trout; "Trout/char daily quota = 2" names a group. "Char" names each char
    (`catalogue.NAMING_GROUPS`): once trout include char (p.80) it is the book's only way to name
    char apart from trout — Region 1's "you must release: All char (includes Dolly Varden)" names
    the char a lake's "Trout daily quota = 2" (a trout/char quota) would otherwise let be kept."""
    from pipeline.regs.parsing.catalogue import NAMING_GROUPS, SPECIES_GROUPS
    return any(c == fish or (c in NAMING_GROUPS and fish in SPECIES_GROUPS[c])
               for c in rule.get("species") or [])


def not_yet_mapped(rule: dict) -> bool:
    """A rule held on its water as a note because the part it names is not drawn (`undrawn_part`:
    Kinbasket Lake's "No Fishing within 200 m of Bush-Sullivan Bridge"). It is bound to the whole
    water only so it can be SHOWN there; it NEVER governs the whole water — it never competes,
    never displaces, never lifts, and never holds a record dormant (`effective_rules`)."""
    return bool(str(rule.get("undrawn_part") or "").strip())


#: A rainbow longer than this is a steelhead where anadromous rainbow are found (p.80).
def _steelhead_min_cm() -> int:
    from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE
    return int(DEFINITIONAL_SIZE["ST"]["min_cm"])


def steelhead_rules(db, section: int) -> bool:
    """Whether the provincial steelhead rules apply on this section (`section_steelhead_rules`:
    the reach run's `rules`, stored with the presence code). Where they do not, a rainbow of any
    size is a rainbow (RULINGS S7). A bundle without the view is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'section_steelhead_rules'") \
            .fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `section_steelhead_rules` view — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return False
    return db.execute("SELECT 1 FROM section_steelhead_rules WHERE sid = ?",
                      (section,)).fetchone() is not None


def home_region(db, section: int) -> str | None:
    """The region a STRADDLING section takes its zone rules from (`section_home`, the atlas's
    `region_home.json`), or None for a section in one region."""
    row = db.execute("SELECT region FROM section_home WHERE sid = ?", (section,)).fetchone()
    return row[0] if row else None


def steelhead_water(db, section: int) -> bool:
    """Whether the book's steelhead definition holds on this section (`steelhead_water`): a
    rainbow over 50 cm here is a steelhead. A bundle without the table is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'steelhead_water'").fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `steelhead_water` table — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return False                                    # a hand-made test bundle of rule sets
    return db.execute("SELECT 1 FROM steelhead_water WHERE sid = ?", (section,)).fetchone() \
        is not None


#: `section_steelhead.code` -> its word (schema.sql).
STEELHEAD_CODES = {1: "known", 2: "possible"}


def steelhead_presence(db, section: int) -> str | None:
    """How sure we are that steelhead are on this section (`section_steelhead`, RULINGS
    S1, S2): "known" (a steelhead row's own water or a rule of one binds it, or the curated
    known-steelhead
    list names its water — a presence indicator that changes no answer by itself;
    `steelhead_water` follows where steelhead rules apply),
    "possible" (any other stream the steelhead rules bind), or None. A rule bundle without the
    table is refused."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'section_steelhead'").fetchone():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule'").fetchone():
            raise SystemExit("read: the bundle has no `section_steelhead` table — rebuild it "
                             "(`python -m pipeline.deliver.bundle`)")
        return None
    row = db.execute("SELECT code FROM section_steelhead WHERE sid = ?", (section,)).fetchone()
    return STEELHEAD_CODES[row[0]] if row else None


def as_rainbow(x: dict) -> dict | None:
    """A rule READ FOR A RAINBOW WHERE A RAINBOW OVER 50 CM IS A STEELHEAD: its length bands over
    that range (every rainbow here is 50 cm or less). `None` when it speaks only of rainbow over 50
    cm; the rule unchanged when it has no bands or none lies outside; with the bands outside dropped
    otherwise — and with no bands at all when the one left covers every rainbow ("hatchery rainbow
    trout catch and release (50 cm or less)" is then an outright release of hatchery rainbow)."""
    bands = x.get("lengths") or []
    if not bands:
        return x
    top = _steelhead_min_cm()
    inside = [b for b in bands if b.get("min_cm") is None or b["min_cm"] < top]
    if not inside:
        return None
    if len(inside) == 1 and inside[0].get("min_cm") is None \
            and (inside[0].get("max_cm") is None or inside[0]["max_cm"] >= top):
        y = {k: v for k, v in x.items() if k != "lengths"}
        if inside[0].get("take") is not None:
            y["take"] = inside[0]["take"]
        # A size rule ("none under 50 cm" has no count of its own: `daily/size`) that now holds
        # at every rainbow length is a count: it competes where counts do.
        if y.get("take") is not None and "/size" in str(y.get("dimension") or ""):
            y["dimension"] = str(y["dimension"]).replace("/size", "", 1)
        return y
    return x if len(inside) == len(bands) else dict(x, lengths=inside)


def _spoken_lengths(x: dict, top: float = float("inf")) -> list | None:
    """THE LENGTHS A RULE SPEAKS ABOUT, as merged inclusive ranges `[(lo, hi), …]` over
    `[0, top]` — `None` when it speaks about every length (no `lengths`). A length no band covers
    is not spoken about by the rule (`catalogue.LengthBand`, "first match wins")."""
    bands = x.get("lengths") or []
    if not bands:
        return None
    got = sorted((float(b.get("min_cm") or 0), min(float(b["max_cm"]) if b.get("max_cm")
                                                   is not None else float("inf"), top))
                 for b in bands)
    out: list = []
    for lo, hi in got:
        if lo > hi:
            continue
        if out and lo <= out[-1][1] + 1:                    # whole centimetres: 29 | 30
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def covers(o: dict, k: dict, top: float = float("inf")) -> bool:
    """DOES `o` SPEAK ABOUT EVERY LENGTH `k` DOES? A rule displaces another only over the fish it
    speaks about (RULINGS Q25): Lakelse Lake's "Rainbow trout over 50 cm catch and release"
    says nothing about a 30 cm rainbow, so it must not silence Region 6's "Trout/char: 5" — the
    5 still counts every rainbow under 50 cm — and Chilko Lake's "no rainbow trout over 70 cm"
    leaves the lake's own "Trout/char daily quota = 2" speaking for rainbow. `top` caps the
    lengths asked about (a rainbow where a larger one is a steelhead: 50 cm)."""
    a = _spoken_lengths(o, top)
    if a is None:
        return True
    b = _spoken_lengths(k, top)
    if b is None:
        b = [(0.0, top)]
    return all(any(lo >= x and hi <= y for x, y in a) for lo, hi in b)


def released_on_water(x: dict) -> str | None:
    """THE WATER KIND A ZONE RELEASE IS LIMITED TO — "stream" for Region 3's "you must release:
    Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", Region 4's "Trout/char release: in
    streams from Nov 1-Mar 31" — or `None`.

    Only an outright release that is no closure (`take: 0`, the fish may still be fished for:
    `rules.release_origins`), carrying a `water`, whose every extent draws only that kind of
    water (`feature_types: [water]`), so that being bound to a section IS being on that kind of
    water. `effective_rules` step 4b lets such a release displace its own table's keeping quotas
    for the fish, exactly as a release printed without `water` does."""
    from pipeline.deliver.bundle.rules import closure_grade, release_origins
    water = x.get("water")
    if not water or not release_origins(x) or closure_grade(x) is not None:
        return None
    exts = x.get("extents") or []
    if not exts or any(list(e.get("feature_types") or []) != [water] for e in exts):
        return None
    return str(water)


def _base_dimension(x: dict) -> str:
    """A rule's dimension without its conditions: "daily@water=stream" -> "daily"."""
    return str(x.get("dimension") or "").split("@", 1)[0]
