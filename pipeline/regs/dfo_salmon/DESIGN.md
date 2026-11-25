# DFO salmon: design (why it is built this way)

What each module does and how to run it: [`README.md`](README.md). This page is the reasoning that is still true, merged
in T1081 from the older design, data-flow, mapping, curation, plan and cascade pages (git history holds them).

## 1. The one measurement the design follows

Archived versions of the DFO pages, re-parsed with today's parser so any difference is the source moving, not us:

| span | waters | still present | rules |
|---|---|---|---|
| Region 6, 2017-07 to now (9 yr) | 45 to 76 | 45 / 45 | rewritten many times |
| Region 6, 2024-04 to now (2.3 yr) | 77 to 77 | 77 / 77 | +110 / -97 |

**The geography never moves; the rules turn over about half a year**. So the
expensive half, which registry item a name is, where the cut-points sit, whether tributaries are in, is curated ONCE by a
human and kept forever. The cheap half is thrown away and re-scraped every run. Reproduce the numbers with
`python -m pipeline.regs.dfo_salmon.churn --regions 1 2 6 --detail` (it reads archived versions through the Wayback index).

```
  page -> fetch -> parse -> untangle -> locations --+--> typed -> feed        (scraped, disposable)
                                                    | fingerprint
                                                    v
                               entries seed / reconcile -> CURATED locators   (human, durable)
                                                    | join at read time
                                                    v
                                      reach run (atlas/reach/dfo_salmon.py) -> deliver/salmon.py -> release `salmon`
```

`locations.extract()` is the fork: a LOCATOR is a place a rule can attach to (a named water's reach, or a cascade default
like section A or "tidal water Area 5"), carrying the scope text, the decomposed operation, landmark triage, tributary scope
and its FINGERPRINT; a RULE RECORD is species x dates x limits with no identity of its own, only its locator's fingerprint,
replaced wholesale every run (no ids, no diffing, no merge).

## 2. Identity: what a curated record must survive

- `fingerprint(section, water, specific_area)` is the sha256[:16] of the normalised triple. It is the join key, the way a
  gauge reading carries its station id. The feed never reads `location_id` (it exists only on the curated side, and a
  curator merging two locators would silently re-key every rule beneath them); `feed.py` reads nothing curated.
- `location_id` is assigned once and derived from nothing: deriving it from text would orphan a binding the day "Highway
  37 Bridge" became "Highway 37 bridge".
- `section` is an attribute with history, never part of the id: Section B became B(i)/B(ii) between 2018 and 2020 and
  every Skeena binding had to survive.
- `fingerprints` is a list: DFO flipped the Kispiox sign count between "three white triangular" and "the 4 triangular" and
  back; the second flip must cost nothing. Wording drift is absorbed on the curated side.
- `status: dormant` instead of deletion: these pages list OPENINGS, so a locator leaves when its fishery closes and returns
  later (the Kispiox River Resort reach cycled out and back four times since 2024). Absence is not deletion.
- A curated binding is never keyed on the scope string: it drifts constantly while the geography does not
  (`churn.compare_reaches()` separates rewording from real change).

What a human supplies per locator (and nothing can re-derive): `WaterBinding.item_ids` (the only place a name becomes
geometry: an exact registry hit or a curator, never heuristic), `Binding.extents` (the cut-points, validated by the shared
provincial `Extent` model), `Binding.tributaries` (three-valued, AGENTS 9) and `notes` / `spatial_caveat` (radius closures
and lake lines that are text, not geometry, so the app must not draw them as a plain fill).

`entries seed` adds locators new to the page and NEVER edits an existing record. `entries reconcile` classifies every
locator and never mutates:

| outcome | severity | meaning |
|---|--:|---|
| `ok` | 0 | fingerprint resolves, nothing moved |
| `dormant` / `revived` | 0 | absent from the page / published again; binding kept either way |
| `drift` | 1 | wording changed, a near match exists: confirm the rebind |
| `new` | 2 | no known locator on this water: needs a curator |
| `section_moved` | 3 | re-scopes everything bound beneath it |
| `structural` | 4 | banner set changed, locator count moved >20%, or >25% unbound: hold the region |

A held locator keeps its last-known-good binding and only that water is withheld: a scraper that goes dark because one new
creek appeared is worse than one that carries 76 waters and flags the 77th.

## 3. Matching names to waters

Exactly like the provincial regs: an exact registry name or variant hit binds (`match.py`), anything else is a suggestion
the owner marks (`suggest.py`, `preverify.py` re-check each suggestion by a second method; AGENTS 65-68). DFO uses forms
the Synopsis does not ("Zymoetz (Copper) River", "Queen Charlotte Islands" for Haida Gwaii). One Waters cell can glue a
name, an alias, a note and a scope together; `untangle` separates them and groups on the CLEANED name, so "Morice River"
and "Morice River (including tributaries)" are one river. A parenthetical like "Tatshenshini River (upstream of ...)" is a
scope, not an alias. The Skeena appears twice (B(i), B(ii)) and stays twice: the CNR Railway Bridge at Terrace is a real
cut with different rules each side, so waters key on `(section, name)`.

Reach `kind` says what a scope sentence is doing, which decides how it becomes geometry: `whole_water`, `tributaries_only`,
`whole_water_excluding`, `upstream_of` / `downstream_of` (one cut at an anchor), `between` (two anchors), `sign_bounded_zone`
(a closure polygon inside boundary signs), `tributary_set`, `named_tributaries`, `described`. Anchors keep the boundary
phrase verbatim; turning one into geometry is `atlas/splits/anchors.py` and the curated cuts, never a half-normalised guess.

## 4. Region 6 is a cascade

Every other region is a flat list of waters. Region 6 is nested boxes where the smallest box containing your water wins,
and the page says so ("Section A applies if stream, specific area, time period, quotas or other species restrictions are
not listed in the following sections"). Read flat, it says the opposite: section A alone reads "4 chinook a day".

```
A  all of Region 6 (the fallback)
|- B   Skeena watershed (a header with no rules)
|   |- B(i)  above the CNR Railway Bridge at Terrace      |- B(ii) below it
|- C   Nass watershed   |- D  Haida Gwaii   |- F  Fraser part of Region 6: no salmon fishing (banner only, no rows)
`- E   everything else on the mainland = Region 6 minus B, C, D, F  (a set difference, evaluated after its siblings)
    `- E:areas-3-4-5-6, E:areas-5, E:areas-6   streams flowing into tidal Area N (about a stream's OUTLET)
```

`cascade.build_scopes()` derives this tree and `chain_for(scope)` gives the lookup order, narrowest first (`E:areas-5, E,
A`; `B(i), B, A`). The ladder, innermost first: this stretch of this river, this river anywhere, this tidal area, this
lettered box, the box around it. 210 of Region 6's 235 rules sit on a named water. Each row carries a `precedence` (3 named
water, 2 area catch-all, 1 section catch-all, 0 region default). Precedence is a property of the LOCATOR (where a statement
sits), so it lives with the curated locators; the feed carries none and the join may never drop it (pinned by
`test_the_skeena_cascade_resolves_through_the_join`).

- Section F publishes no rows: the banner closure is synthesised as a row with `source: "section_banner"`.
- Exclusion lists are `<ul>` bullets (Babine Lake excludes a 400 m radius around 12 creeks); kept as `specific_area_bullets`.
  A closed area inside a place is a location of its own (`closures.py`, AGENTS 68); its circle centre is the creek mouth
  where it meets the lake shore (`closure_mouths.py`).
- `colspan="6"` on a five-column table (the B(i) banner is authored wrong): the grid builder clamps spans.
- When two rows on one water both cover a section, the one covering fewer sections answers first (`deliver.salmon.chain_order`).
- The tidal-Area rows belong to E alone: each is bound to "drains to Area N" OUTSIDE E's siblings, else the "no chinook" row
  covered 97% of the Skeena (the Skeena's mouth is in Area 4).
- A row about another fish ("All | No fishing for eulachon") is not a salmon rule; it is its own line.

## 5. The typed decode (`typed.py`): three defects the repo paid for once

- `take=0` is not "closed". `may_target` separates "do not fish for this" (`no_fishing`) from "fish for it and release it"
  (`non_retention`); a bare "0 per day" says neither, so it publishes the conservative way and is flagged for review.
- A size sub-limit is its own rule pointing at its parent with `within` ("4 per day, only 2 over 50 cm" is two rules; one
  rule carrying both numbers loses the 4).
- A gear rule is scoped by what you fish FOR (`when_targeting`), not what you may keep.
- An unreadable season goes to review; an unambiguous source typo ("Aprl 1") is published repaired and recorded; an EMPTY
  dates cell is a fact: no window means all year. (`CatalogueRule` does not check that a window parses; this does.)
- A limits wording not in the closed registry (`data/curated/dfo_salmon/wording.json`, `wording.py`) is flagged, never read
  as free text. Pure function of the row: no model, no credits.

## 6. The live feed and its refresh

`live.py` fetches (validated, atomic), types, joins and writes ONE small `feed/index.json` the app reads, so a rule change
reaches the app without a rebuild; the feeds worker (`app/workers/feeds/src/dfo/`) runs the same logic on a schedule. A
fingerprint nobody curated is held in `unplaced.json` and the owner told once; `live queue` seeds it for DFO mode in the
review app. `live run --dry-run` prints what a fetch would change against the typed rows in use (`refresh.py`): new,
reworded, dropped and moved places (the only thing the owner re-reviews), row changes per place with their FN numbers, the
marks that would move, and the urgent rows. A real run archives every fetched page once and stamps typed rows with the
page's sha256 and `dateModified`. Early exit is on the hash, never `dateModified` (the 2017 and 2018 pages have none and
Fishery Notice edits do not reliably move it). Publishing is never blocked by a hold; there are no credits or secrets, so it
is safe to automate. Never a GitHub schedule (the worker's cron starts it).

## 7. Invariants and where they are enforced

| invariant | enforced by |
|---|---|
| every parsed row lands in exactly one rule | `untangle.verify()` |
| the transcription reads nothing out of the limits cell | `test_the_transcription_reads_nothing_out_of_the_limits_cell` |
| the feed takes no curated input | `test_the_feed_takes_no_curated_input` |
| every scraped locator joins to a curated record | `test_no_scraped_rule_lands_on_a_locator_nobody_curated` |
| every rule's `verbatim` is a span of its own row | `test_every_scraped_rule_types_and_keeps_its_chain_of_custody` |
| `take=0` always says whether you may fish | `test_take_zero_says_whether_you_may_fish_at_all` |
| an unreadable season never publishes as "no season" | `test_an_unreadable_season_goes_to_review_rather_than_publishing_as_none` |
| a region that restructures holds rather than rebinding | `entries.reconcile` severity 4 |
| a row that says "per day" always yields a quota | `test_a_row_that_says_per_day_always_yields_a_quota` |
| the same page always produces the same feed | `test_the_feed_is_deterministic` |
| the Skeena cascade resolves through the join | `test_the_skeena_cascade_resolves_through_the_join` |
