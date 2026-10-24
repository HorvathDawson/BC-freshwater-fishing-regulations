"""Steelhead presence: `steelhead_known`, `steelhead_set` and `steelhead_source` from the reach run, proved
against the corpus and against the views the bundle derives from them."""
from __future__ import annotations

from pathlib import Path


def write_steelhead_presence(db, reaches, ces, sid, cov, registry=None, *,
                             set_of: dict[int, int]) -> tuple[dict, set, set]:
    """`steelhead_known`, `steelhead_set` and `steelhead_source` from the reach run's
    `steelhead_presence` (`pipeline.atlas.reach.steelhead`), checked against the corpus (every row
    flagged `anadromous_rainbow` was placed, and only steelhead rows were) and against itself: the
    view `section_steelhead` must give every section exactly the code the run gave it,
    `steelhead_water` exactly the run's `anadromous` sections, and `section_steelhead_rules`
    exactly the run's `rules` sections (where the provincial steelhead set applies)."""
    import json as _json
    from pipeline.atlas.reach.io import STEELHEAD_TABLE
    from pipeline.atlas.reach.steelhead import (CURATED_LIST, KNOWN, POSSIBLE, fingerprint,
                                                load_list, steelhead_row)
    CODE = {KNOWN: 1, POSSIBLE: 2}
    path = Path(reaches) / f"{STEELHEAD_TABLE}.jsonl"
    report = Path(reaches) / "report.json"
    if not path.exists() or not report.exists():
        raise SystemExit(
            f"{reaches} has no `{STEELHEAD_TABLE}` — the reach run predates it. Re-run the reach "
            f"builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    sh_report = _json.loads(report.read_text(encoding="utf-8")).get("steelhead") or {}
    by_entry = sh_report.get("by_entry") or {}
    flagged = {ce.entry_id for ce in ces if ce.anadromous_rainbow}
    rows = {ce.entry_id for ce in ces if steelhead_row(ce)}
    placed = {e for e, n in by_entry.items() if (n.get("sections") or 0) > 0}
    # THE CURATED LIST the run was given must be the list on disk now (its fingerprint).
    want, ran = fingerprint(load_list()), sh_report.get("list_fingerprint")
    if want != ran:
        raise SystemExit(
            f"steelhead: the reach run was made with curated known-steelhead list {ran!r}, not the "
            f"list on disk ({want}) — re-run the reach builder:\n    python -m "
            f"pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    if flagged - placed or placed - rows:
        raise SystemExit(
            f"steelhead_water: the reach run and the corpus disagree — flagged rows placing no "
            f"section {sorted(flagged - placed)[:5]}, placed rows that are no steelhead row "
            f"{sorted(placed - rows)[:5]}. Re-run the reach builder.")
    code: dict[int, int] = {}
    anadromous: set[int] = set()
    #: WHERE STEELHEAD RULES APPLY (`Presence.rules_apply`: the provincial set binds it) — the
    #: run's answer, carried as `steelhead_known.rules` / `steelhead_set.rules` and proved below.
    applies: set[int] = set()
    kind: dict[int, str] = {}
    source: dict[int, set[str]] = {}
    unknown = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = _json.loads(line)
            s = sid.get(r["section_id"])
            if s is None:
                unknown.append(r["section_id"])
                continue
            if r["steelhead"] not in CODE:
                raise SystemExit(f"steelhead_presence: {r['steelhead']!r} on {r['section_id']}")
            code[s] = CODE[r["steelhead"]]
            kind[s] = r["kind"]
            if r.get("rules"):
                applies.add(s)
            if r.get("anadromous"):
                if s not in applies:
                    raise SystemExit(f"steelhead_presence: anadromous on {r['section_id']}, "
                                     f"where no steelhead rule applies")
                # known by the book or by the curated list (RULINGS S5, S6)
                if r["steelhead"] != KNOWN or not (r.get("regulations") or r.get("listed")):
                    raise SystemExit(f"steelhead_presence: anadromous on {r['section_id']}, "
                                     f"which is not known steelhead water")
                anadromous.add(s)
            if r["steelhead"] == KNOWN:
                got = source.setdefault(s, set())
                if r.get("regulations"):
                    got.add(r["entry_id"])
                if r.get("listed"):
                    got.add(CURATED_LIST)
    if unknown:
        raise SystemExit(f"steelhead_presence: {len(unknown):,} sections are not in the handle "
                         f"table (e.g. {unknown[:3]}) — the reach run and the atlas disagree")
    # PER SECTION: every known stream, every steelhead-water section, and any known section with
    # no rule set. Every other section's code is its RULE SET's: one code (or none) per set; a set
    # whose sections disagree only because some are known keeps those known ones per section.
    loose = sorted(s for s, c in code.items() if c != 1 and s not in set_of)
    if loose:
        raise SystemExit(f"steelhead_presence: {len(loose)} section(s) with a code and no rule set "
                         f"(e.g. {loose[:3]}) — the code cannot be carried by its set")
    own = {s for s, c in code.items() if c == 1 and (kind[s] == "stream" or s in anadromous
                                                     or s not in set_of)}
    # A SECTION'S VALUE is (code, steelhead rules apply); a set carries one value or none.
    value = lambda s: None if s not in code else (code[s], int(s in applies))     # noqa: E731
    per_set: dict[int, set] = {}
    for s, k in set_of.items():
        if s not in own:
            per_set.setdefault(k, set()).add(value(s))
    mixed = {k for k, v in per_set.items() if len(v) > 1}
    if mixed:
        own |= {s for s, k in set_of.items() if k in mixed and code.get(s) == 1}
        per_set = {}
        for s, k in set_of.items():
            if s not in own:
                per_set.setdefault(k, set()).add(value(s))
        still = sorted(k for k, v in per_set.items() if len(v) > 1)
        if still:
            raise SystemExit(
                f"steelhead_presence: {len(still)} rule set(s) whose sections carry different "
                f"codes (e.g. set {still[0]}: {sorted(map(str, per_set[still[0]]))}) — the code is "
                f"not a fact of the set; store it per section")
    set_rows = sorted((k, *v.pop()) for k, v in per_set.items() if None not in v)
    db.executemany("INSERT INTO steelhead_known (sid, anadromous, rules) VALUES (?,?,?)",
                   [(s, int(s in anadromous), int(s in applies)) for s in sorted(own)])
    cov.filled("steelhead_known", len(own))
    db.executemany("INSERT INTO steelhead_set (set_id, code, rules) VALUES (?,?,?)", set_rows)
    cov.filled("steelhead_set", len(set_rows))
    # why each NAMED water's steelhead is known (per water, not per section)
    ord_of: dict[int, list[int]] = {}
    for o, s in db.execute("SELECT ord, sid FROM item_section"):
        ord_of.setdefault(s, []).append(o)
    src = sorted({(o, e) for s, es in source.items() for e in es for o in ord_of.get(s, ())})
    db.executemany("INSERT INTO steelhead_source (ord, entry_id) VALUES (?,?)", src)
    cov.filled("steelhead_source", len(src))
    return code, anadromous, applies


def prove_steelhead_presence(db, code: dict, anadromous: set, applies: set) -> None:
    """The stored steelhead views (`section_steelhead`, `steelhead_water`,
    `section_steelhead_rules`) against the reach run's answer they were written from — once
    `section_ruleset` holds the sets the views join."""
    got = dict(db.execute("SELECT sid, code FROM section_steelhead"))
    if got != code:
        bad = sorted(set(got.items()) ^ set(code.items()))
        raise SystemExit(f"section_steelhead: the stored form does not reproduce the reach run on "
                         f"{len(bad)} section(s) (e.g. {bad[:3]})")
    sw = {s for (s,) in db.execute("SELECT sid FROM steelhead_water")}
    if sw != anadromous:
        raise SystemExit(f"steelhead_water: the stored form does not reproduce the reach run's "
                         f"anadromous sections ({len(sw ^ anadromous)} differ)")
    sr = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
    if sr != applies:
        raise SystemExit(f"section_steelhead_rules: the stored form does not reproduce the reach "
                         f"run's sections where steelhead rules apply ({len(sr ^ applies)} differ)")
