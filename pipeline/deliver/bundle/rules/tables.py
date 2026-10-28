"""`write`: the regulation tables of the bundle — `entry`, `rule`, `section_ruleset`, `ruleset` and every
licensing table — from the reach run and the catalogue, with the interned sets (`intern_sets`) checked over
the rows WRITTEN, not just the input."""
from __future__ import annotations

import json
from array import array
import sqlite3
from pathlib import Path
from pipeline.common import entry_id as EID
from pipeline.deliver.bundle.rules.row import _mus_of, _rule_row
from pipeline.deliver.bundle.rules.lifts import blanket_closures, zone_closures
from pipeline.deliver.bundle.rules.see import _see_column, entry_name, see_pointer_rows
from pipeline.deliver.bundle.rules.regions import (
    _own_rows, co_bound_regions, equivalent_regions, own_entry_regions, regions_of_waters
)
from pipeline.deliver.bundle.rules.presence import prove_steelhead_presence, write_steelhead_presence
from pipeline.deliver.bundle.rules.diagnostics import ambiguous_cuts, run_rows


def _jsonl(path: Path):
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def intern_sets(rows) -> tuple[dict[str, int], list[list[tuple[str, str, str]]]]:
    """`(section -> set_id, sets)` from the reach builder's (rule, section, scope) rows.

    The set is over (entry_id, rule_id, SCOPE), not just the rule, so a section reached as a
    tributary and a section the rule names directly land in different sets even when the
    rules are identical. That is the point: `scope` is how a reader is told the difference
    between "no fishing here" and "no fishing here, because this creek joins a closed
    stretch of the Skeena", and 98.6% of all bindings are tributary ones. It costs 249 sets.

    Sets are sorted before interning so the same corpus always produces the same ids — a
    bundle whose set numbering moved between builds would diff as though every section had
    changed.
    """
    # MEMORY (DATAFLOW P1b): 149 M rows. Held as a (entry, rule, scope) TUPLE per row in a set per
    # section, they were the bundle build's 28 GB peak; each binding is now a small int (`ids`,
    # one tuple per distinct binding) appended to a compact unsigned array per section. Same sets,
    # same numbering: a set is still the sorted tuples, interned in sorted-section order.
    ids: dict[tuple[str, str, str], int] = {}
    binding: list[tuple[str, str, str]] = []
    by_section: dict[str, array] = {}
    for r in rows:
        t = (r["entry_id"], r["rule_id"], r["scope"])
        i = ids.get(t)
        if i is None:
            i = ids[t] = len(binding)
            binding.append(t)
        a = by_section.get(r["section_id"])
        if a is None:
            a = by_section[r["section_id"]] = array("I")
        a.append(i)

    intern: dict[frozenset, int] = {}
    sets: list[list[tuple[str, str, str]]] = []
    section_set: dict[str, int] = {}
    for section in sorted(by_section):
        key = frozenset(by_section.pop(section))
        got = intern.get(key)
        if got is None:
            got = intern[key] = len(sets)
            sets.append(sorted(binding[i] for i in key))
        section_set[section] = got
    return section_set, sets


def check_corpus(reaches: Path, entries: dict[str, dict]) -> str:
    """The corpus digest of `entries` (`io.corpus_digest`), refusing a reach run that states
    another — or none (it predates the stamp). A run made from another corpus binds rules the
    bundle would label from different text: an edited extent, date or species with unchanged ids
    used to pass, because only the id sets were compared (P2)."""
    from pipeline.regs.parsing.io import corpus_digest
    ran = json.loads((Path(reaches) / "report.json").read_text(encoding="utf-8")) \
        .get("entries_digest")
    now = corpus_digest(entries)
    if ran != now:
        raise SystemExit(
            f"the reach run at {reaches} was made from corpus "
            f"{ran or '(none stated: the run predates the entries digest)'}, not the entries on "
            f"disk ({now}) — re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    return now


def read_entries(entries_dir: Path, registry=None):
    """Every entry dict of every entry SOURCE under `entries_dir`: a directory of region files carrying `entries` (see
    `io.entry_sources`), files in name order. The bundle's one corpus read — `write` below, and the continuation
    pairings the donor panels need before `entry` exists (`continuation.catalogue_heads`, T76)."""
    from pipeline.regs.parsing.io import _holds_entries, read_entryfile
    for d in sorted({p.parent for p in Path(entries_dir).rglob("region-*.json")}):
        if not _holds_entries(d):
            continue
        for path in sorted(d.glob("region-*.json")):
            yield from read_entryfile(path, registry, require_entries=True).values()


def write(db: sqlite3.Connection, reaches: Path, entries_dir: Path, cov,
          build_dir: Path | None = None, *, home=None) -> None:
    """Write `entry`, `rule`, `section_ruleset`, `ruleset`, and every licensing table.

    `home` is the atlas's region home map (`registry.regions.home_view`), which the bundle build
    takes off the graph it already holds; `None` reads it from `build_dir` (loading `graph.pkl`)."""
    from pipeline.atlas.reach.io import iter_rule_sections, rule_section_file
    sections_file = rule_section_file(reaches)  # RAISES when absent (P2: a skip rendered every water open)

    # THE 88, FIRST. A rule nobody could place must never render as "no rules here" — it can
    # only ever raise "unknown" — so the flag has to be on the rule row itself, and it is
    # read from the reach builder's own failure table rather than guessed at from the
    # curation. `rule_unresolved` is the table that must never be silently short.
    unresolved = {(r["entry_id"], r["rule_id"]): f"{r['reason']}: {r['detail']}"
                  for r in _jsonl(reaches / "rule_unresolved.jsonl")}

    from pipeline.regs.parsing.catalogue import CatalogueEntry

    if build_dir is None:
        raise SystemExit("rules.write needs build_dir to resolve section handles")
    # THE ATLAS, READ ONCE, for the two things the rows need from it besides section handles:
    # the names a label gives a rule's place, and the sections B.C. does not govern.
    from pipeline.atlas.registry import load_registry
    from pipeline.deliver.bundle.place_names import PlaceNamer, area_names, resolved_labels
    registry = load_registry(str(Path(build_dir) / "registry.json"))
    namer = PlaceNamer(registry, resolved_labels(build_dir), area_names(build_dir))

    entry_rows, rule_rows = [], []
    ces = []
    docs = []
    # EVERY ENTRY SOURCE, and nothing else. A source is a directory of region files carrying
    # `entries` (see `io.entry_sources`); the DFO directory beside the catalogue holds region
    # files of another shape and is not one. Inside a source, a region file WITHOUT `entries` is
    # a defect, and stops the build (`require_entries`). Read through the corpus' one read point,
    # so an item id this registry folded into another names its item (`io.read_entryfile`).
    for e in read_entries(entries_dir, registry):
        # Read through the model, so a key from a retired shape is refused, not read.
        docs.append((e, CatalogueEntry.model_validate(e)))
    # THE SAME CORPUS THE REACH RUN WAS MADE FROM (P2): its full content, not only its ids.
    db.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('entries_digest', ?)",
               (check_corpus(reaches, {e["entry_id"]: e for e, _ in docs}),))
    # PER ENTRY (T133): which entries this bundle was made from, so the review tool can call ONE
    # entry built (its digest here = the reach run's = the file's) while another awaits a build.
    # `check_corpus` just proved the corpus is the run's, so these are the run's `entry_digests`.
    from pipeline.regs.parsing.io import entry_digests
    db.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('entry_digests', ?)",
               (json.dumps(entry_digests({e["entry_id"]: e for e, _ in docs}), sort_keys=True,
                           separators=(",", ":")),))
    # What an exemption may name: zone entries by slug, and every entry's rule ids.
    zones: dict[str, list[str]] = {}
    rules_of: dict[str, dict] = {}
    for _, ce in docs:
        rules_of[ce.entry_id] = {r.rule_id: r for r in ce.rules}
        if EID.is_table(ce.entry_id):
            zones.setdefault(ce.entry_id.split(":", 1)[1], []).append(ce.entry_id)
    entries_by_id = {ce.entry_id: ce for _, ce in docs}
    closures = zone_closures([ce for _, ce in docs])
    blankets = blanket_closures([ce for _, ce in docs])
    # THE ONE REGION MEMBERSHIP (T140): the home map the zone rules and the reach run read
    if home is None:
        from pipeline.atlas.registry.regions import home_view_from
        home = home_view_from(build_dir, registry)
    water_regions = regions_of_waters(registry, [ce for _, ce in docs], home)
    # One pass over the bindings (149 M rows on the full corpus), noting every rule it names. FIRST,
    # before any rule row: a row's lift carries into the regions whose zone tables the run bound
    # beside it (`co_bound_regions`), which only the sets say.
    bound_rules: set[tuple[str, str]] = set()

    def _noting(rows):
        for e, rid, s, scope in rows:
            bound_rules.add((e, rid))
            yield {"entry_id": e, "rule_id": rid, "section_id": s, "scope": scope}

    section_set, sets = intern_sets(_noting(iter_rule_sections(sections_file)))
    own_rows = _own_rows([ce for _, ce in docs])
    co_bound = co_bound_regions(sets, own_rows)
    own = own_entry_regions([ce for _, ce in docs])
    see_of = _see_column(entries_by_id)
    for e, ce in docs:
        ces.append(ce)
        matched = list(ce.matched)
        entry_rows.append((
            ce.entry_id,
            # The first match, or nothing. An entry that never matched a water keeps its
            # rules and its text and carries a null item — the app shows "we have a rule
            # for a water we cannot place" rather than dropping it (77 of these).
            matched[0] if matched else None,
            entry_name(ce),
            # What the page printed, kept whole — see the note in schema.sql.
            ce.name,
            ce.regs_verbatim,
            json.dumps(list(ce.symbols), separators=(",", ":")),
            json.dumps(_mus_of(ce.entry_id), separators=(",", ":")),
            json.dumps(list(ce.source_pages), separators=(",", ":")),
            ce.scope_note or None,
            json.dumps(e.get("extents") or [], separators=(",", ":")),
            # EVERY water matched; `item_id` above is only the first.
            json.dumps(matched, separators=(",", ":")),
            see_of.get(ce.entry_id),
        ))
        # A rule may name another in its entry (`suspended_while`), and its label says what
        # that rule is in words — so each label is built with its siblings to hand.
        siblings = {r.rule_id: r for r in ce.rules}
        place_of = namer.for_entry(ce.matched)
        for r in e.get("rules") or []:
            k = (e["entry_id"], r.get("rule_id"))
            rule_rows.append(_rule_row(e["entry_id"], r, k in unresolved, siblings, zones,
                                       rules_of, unresolved=unresolved.get(k),
                                       place_of=place_of, entries=entries_by_id,
                                       closures=closures,
                                       regions=equivalent_regions(
                                           ce, r.get("rule_id"), water_regions, co_bound, own),
                                       blankets=blankets))

    # NAMED, not positional. A `pages` column was added to the schema while this line kept
    # seven placeholders, and nothing caught it until 90 seconds into a province-wide rebuild
    # — which then wrote a 42 MB bundle with zero entries in it. Naming the columns makes that
    # failure impossible rather than merely tested.
    db.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, symbols,"
                   "                   mus, pages, scope_note, extents, matched, see)"
                   " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                   entry_rows)
    cov.filled("entry", len(entry_rows))
    pointers = see_pointer_rows(entries_by_id)
    db.executemany("INSERT INTO see_pointer (entry_id, name, ix, target_entry, target_name, item_id)"
                   " VALUES (?,?,?,?,?,?)", pointers)
    cov.filled("see_pointer", len(pointers))
    # COLUMNS NAMED, for the third time and the same reason. This was eleven positional
    # placeholders, and adding `limits` to the schema made it eleven values for twelve
    # columns — the fault that once shipped a 42 MB bundle with no entries in it.
    db.executemany("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts,"
                   "                  scope, when_, while_, standing, species, species_except,"
                   "                  exempts, take, may_target, conditions, uncertain, unresolved,"
                   "                  verbatim, extent_text, undrawn_part) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rule_rows)
    cov.filled("rule", len(rule_rows))

    # THE REACH RUN MUST BE THIS CORPUS'S. A run built before a rule was removed still binds it,
    # and `ruleset` then names a rule the `rule` table does not have — measured on the bundle
    # built after the licensing rules left: 144 such rules, 31,354 ruleset rows, in 2,347 of the
    # 2,375 sets. The reader's JOIN drops such a row without a word, so the check is here: every
    # rule the run placed exists, and every rule that exists was placed or explained.
    placed_rules = bound_rules | set(unresolved)
    have_rules = {(r[0], r[1]) for r in rule_rows}
    gone, never = sorted(placed_rules - have_rules), sorted(have_rules - placed_rules)
    if gone or never:
        raise SystemExit(
            f"rules: the reach run at {reaches} is not this corpus's — it binds {len(gone):,} "
            f"rule(s) the corpus no longer has (e.g. {gone[:3]}) and never saw {len(never):,} it "
            f"does have (e.g. {never[:3]}). Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")

    # The section is named by its HANDLE here, as everywhere else in the bundle. A rule bound
    # to a section the handle table does not know means the reach run and the atlas are not
    # the same build — which would silently bind rules to the wrong water, so it stops here.
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    _unknown = [s for s in section_set if s not in sid]
    if _unknown:
        raise SystemExit(
            f"section_ruleset: {len(_unknown):,} bound sections are not in the handle table "
            f"(e.g. {_unknown[:3]}) — the reach run and section_handles.txt disagree")
    # `section_ruleset` is written below, once its RULE KEY is known (the steelhead facts).
    set_of = {sid[k]: v for k, v in section_set.items()}
    db.executemany("INSERT INTO ruleset VALUES (?,?,?,?)",
                   ((i, e, r, s) for i, rows in enumerate(sets) for e, r, s in rows))
    cov.filled("ruleset", sum(len(s) for s in sets))

    # AND AGAIN AGAINST THE ROWS WRITTEN, not the input they came from: a ruleset row naming a
    # rule the bundle does not hold is a regulation that exists on the map and nowhere else.
    orphan = db.execute(
        "SELECT rs.entry_id, rs.rule_id FROM ruleset rs LEFT JOIN rule r "
        "ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id "
        "WHERE r.rule_id IS NULL LIMIT 5").fetchall()
    if orphan:
        raise SystemExit(f"ruleset names rules that are not in `rule`: {orphan}")

    # WATER B.C. DOES NOT GOVERN and WATER THE BOOK CALLS TIDAL: the two sets the reach builder
    # took out of every binding, READ FROM THE RUN (`outside_bc.jsonl`, `tidal.jsonl`) — never
    # derived again here — and written so a reader can say "outside B.C." / "tidal water" instead
    # of "no rules". Written before the licensing, whose province-wide requirements stop at the
    # `tidal` table's sections (`licensing.write`).
    from pipeline.atlas.reach.io import OUTSIDE_TABLE, TIDAL_TABLE, read_table
    outside = run_rows(read_table(reaches, OUTSIDE_TABLE), sid, OUTSIDE_TABLE)
    db.executemany("INSERT INTO outside_bc (sid) VALUES (?)", [(x["sid"],) for x in outside])
    cov.filled("outside_bc", len(outside))
    tidal = [(x["sid"], x["entry_id"]) for x in run_rows(read_table(reaches, TIDAL_TABLE), sid,
                                                          TIDAL_TABLE)]
    db.executemany("INSERT INTO tidal (sid, entry_id) VALUES (?,?)", tidal)
    cov.filled("tidal", len(tidal))
    own = {s: e for s, e in tidal}
    # an owner is a row marked tidal, or (T166) a TIDAL BOUNDARY `split:<id>` — the piece below it
    from pipeline.atlas.reach.outside import is_cut_owner
    rows = {e for e in own.values() if not is_cut_owner(e)}
    if rows - {ce.entry_id for ce in ces if getattr(ce, "tidal", False)}:
        raise SystemExit(f"tidal: the reach run names tidal rows the corpus does not mark tidal "
                         f"({sorted(rows)}) — re-run the reach builder")
    # THE CUTS THAT MATCHED SEVERAL PLACES ON THEIR LINE (RULINGS E16, `rule_ambiguous_cut`),
    # READ FROM THE RUN'S DIAGNOSTICS: every candidate and the one the builder took.
    amb = ambiguous_cuts(read_table(reaches, "rule_diagnostic"))
    db.executemany("INSERT INTO rule_ambiguous_cut (entry_id, rule_id, extent, split_id, used, "
                   "candidates, toward) VALUES (?,?,?,?,?,?,?)", amb)
    cov.filled("rule_ambiguous_cut", len(amb))
    # The same resolver places LICENSING records (Thompson below Kamloops Lake, Dean between
    # Anahim and Iltasyuko), so their choices ship too, keyed by the record.
    lamb = ambiguous_cuts(read_table(reaches, "licensing_diagnostic"),
                          table="licensing_diagnostic", id_key="record_id")
    db.executemany("INSERT INTO licensing_ambiguous_cut (entry_id, record_id, extent, split_id, "
                   "used, candidates, toward) VALUES (?,?,?,?,?,?,?)", lamb)
    cov.filled("licensing_ambiguous_cut", len(lamb))

    # Licensing: the other half of each entry, placed by the same reach run.
    from pipeline.deliver.bundle import licensing as _licensing
    _licensing.write(db, reaches, ces, cov, sid, registry)

    # HOW SURE WE ARE THAT STEELHEAD ARE HERE (`section_steelhead`), and WHERE A RAINBOW OVER 50 CM
    # IS A STEELHEAD (p.80, `steelhead_water`). Both from the reach run's `steelhead_presence`
    # (`pipeline.atlas.reach.steelhead`, RULINGS S1, S2): `known` is every water a rule
    # of a steelhead row binds, and every water on the curated known-steelhead list (a presence
    # indicator: it binds no rule); `possible` every other stream the provincial steelhead rules
    # bind. The definition holds on the BOOK's flowing known sections only — never a lake, never
    # "possible", never from the list. A flagged row the run placed nowhere would state the
    # definition nowhere, so it stops the build; so does a run that predates the table, or
    # disagrees with the corpus or the curated list.
    code, anadromous, applies = write_steelhead_presence(db, reaches, ces, sid, cov, registry,
                                                         set_of=set_of)
    # THE RULE KEY OF EVERY SECTION (DATAFLOW P2, decision U2): (rule set, a rainbow over 50 cm
    # is a steelhead here, the steelhead rules apply here) — decided here, where those facts are
    # born, and stored on `section_ruleset`; the views are proved to reproduce it below.
    from pipeline.deliver.bundle.derived import rule_keys
    kind_of_section = dict(db.execute("SELECT s.sid, i.kind FROM item_section s JOIN item i "
                                      "ON i.ord = s.ord"))
    key_rows, key_of = rule_keys(set_of, anadromous, applies, kind_of_section)
    db.executemany("INSERT INTO rule_key (key_ix, set_id, steelhead_water, steelhead_rules, kind, "
                   "sections, rep_sid) VALUES (?,?,?,?,?,?,?)", key_rows)
    cov.filled("rule_key", len(key_rows))
    db.executemany("INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (?,?,?)",
                   [(s_, v, key_of[s_]) for s_, v in sorted(set_of.items())])
    cov.filled("section_ruleset", len(set_of))
    prove_steelhead_presence(db, code, anadromous, applies)
    # THE PROOF THAT NOTHING BINDS OUTSIDE B.C., against the ROWS JUST WRITTEN — a reach run from
    # before the subtraction binds 181 such sections, and must not ship.
    bound_outside = {t: db.execute(f"SELECT COUNT(*) FROM outside_bc o JOIN {t} t "
                                   f"ON t.sid = o.sid").fetchone()[0]
                     for t in ("section_ruleset", "section_licensing")}
    if any(bound_outside.values()):
        raise SystemExit(
            f"outside_bc: sections outside British Columbia carry regulation sets "
            f"({bound_outside}) — the reach run predates the border subtraction, or the atlas "
            f"changed under it. Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    # THE PROOF THAT NO OTHER ROW BINDS TIDAL WATER (Nitinat Lake): the reach builder takes it
    # out of every other row (`reach.outside.tidal_owner`); a reach run from before that — or a
    # tidal row with a rule that is not a note — must not ship.
    foreign = [(s, e, r) for s, e, r in db.execute(
        "SELECT t.sid, rs.entry_id, rs.rule_id FROM tidal t JOIN section_ruleset sr ON sr.sid = t.sid "
        "JOIN ruleset rs ON rs.set_id = sr.set_id") if e != own[s]]
    licensed = db.execute("SELECT COUNT(*) FROM tidal t JOIN section_licensing l "
                          "ON l.sid = t.sid").fetchone()[0]
    if foreign or licensed:
        raise SystemExit(
            f"tidal: tidal sections carry provincial regulation — rules of other rows "
            f"{foreign[:5]} ({len(foreign)}), licensing sets on {licensed} section(s). Tidal "
            f"water takes only its own row's note. Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    # THE HOME REGION OF A STRADDLING SECTION (`section_home`): the atlas's own `region_home.json`
    # (`registry.regions.write_homes`), by handle — the region holding most of it. Display only since
    # T183: a straddler takes both regions' tables (the reach run bound both).
    from pipeline.atlas.registry.regions import read_homes
    homes = sorted((sid[h], r) for h, r in read_homes(build_dir).items() if h in sid)
    db.executemany("INSERT INTO section_home (sid, region) VALUES (?,?)", homes)
    cov.filled("section_home", len(homes))
    if namer.unnamed:
        print(f"     labels: {len(namer.unnamed)} cut-point(s) have no book name, so the rules "
              f"on them name no place:")
        for s_, why in sorted(namer.unnamed)[:20]:
            print(f"       {s_}: {why}")

    # COUNTED FROM THE SET, NOT FROM A TUPLE INDEX. This read `r[8]` — which is
    # `json.dumps(species)`, a string that is never empty ("[]" at minimum) and therefore
    # always truthy. Every build reported EVERY rule as uncertain: 3,422 of 3,422, a number
    # so obviously wrong it read as normal. The column itself was always right (256), so
    # nothing downstream was affected and nothing failed — only the line a person reads to
    # decide whether a build is healthy. The INSERT below names its columns for exactly this
    # reason; the summary went positional and drifted the moment a field moved.
    n_uncertain = len(set(unresolved) & {(r[0], r[1]) for r in rule_rows})
    print(f"     rules: {len(entry_rows):,} entries · {len(rule_rows):,} rules "
          f"({n_uncertain} uncertain) · {len(section_set):,} sections carry one, "
          f"sharing {len(sets):,} distinct sets")
