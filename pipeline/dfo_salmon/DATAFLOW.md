# Data flow — splitting location from regulation

**Status:** `locations.py`, `entries.py` and the seeded `entries/` files are built and
tested. The cron job, the reach binding itself, and the export are not. Numbers are measured over 12 archived Region 6
versions and 6/5 for Regions 1/2; reproduce with `pipeline.dfo_salmon.churn`.

---

## 1. The split, and why it is at the column boundary

The DFO table has five columns. They have **two different lifetimes**:

| | columns | lifetime | who resolves it |
|---|---|---|---|
| **LOCATION** | `Waters`, `Specific area`, + the section banner (`A`, `B(i)`, …) | ~static | a human, offline, with the FWA graph |
| **REGULATION** | `Species`, `Dates`, `Limits/Gear` | ~50% turnover/year | the cron, unattended |

So the cron's job is not "re-derive everything". It is:

> **Assert that the location half is unchanged, then replace the regulation half.**

Everything below follows from that one sentence.

---

## 2. Two keys. Never one.

This is the crux, and getting it wrong is the whole failure mode:

| key | derived from | changes when | used for |
|---|---|---|---|
| `location_id` | **nothing** — assigned once by a curator | never | rules link to it; bindings hang off it |
| `fingerprint` | normalised location text | the source is reworded | the cron's *integrity check* |

**Identity is not the integrity check.** A fingerprint that moves means "a human should
look", not "this is a different place". If `location_id` were derived from the text,
`"Highway 37 Bridge"` → `"Highway 37 bridge"` would silently orphan a curated binding —
and that exact drift is in the archives, twice, in both directions.

---

## 3. The fast path — what cron does

```
fetch 9 pages                     ~0.5 s each, sha256 per page
  └─ all hashes unchanged? ───────────────────────────────► STOP (the common case)
parse → untangle                  237 reaches, ~0.2 s
  └─ for each reach:
       fingerprint(section, water, specific_area)
         ├─ hit in entries file ──► attach location_id, REPLACE its rules   ◄── ~99%
         ├─ near-match (≥0.75, same water) ──► propose rebind, HOLD the reach
         └─ no match ─────────────► NEW location, HOLD the reach
  └─ curated location absent from the page ──► mark dormant, KEEP the binding
publish rules for every bound location; hold only the unbound ones
```

One dict lookup per reach. No geometry, no graph, no FWA file. The whole run is
dominated by nine HTTP requests.

### Measured: how much actually needs a human

Replaying every archived version against a **cumulative** entries file (dormant
locations retained, so a seasonal revival is an exact hit, not a new location):

| Region | cron runs | exact hits | auto-matched drift | needs a human | exact rate | **human/run** |
|---|--:|--:|--:|--:|--:|--:|
| 6 Skeena | 7 | 885 | 5 | 3 | **99.1%** | **0.4** |
| 1 Vancouver Island | 4 | 215 | 1 | 5 | 97.3% | 1.2 |
| 2 Lower Mainland | 3 | 76 | 2 | 6 | 90.5% | 2.0 |

*(steady state — post-restructure versions only; §6 covers the restructure case.)*

**Under two locations per region per run.** That is the number that makes this design
viable: the cron is unattended in the ordinary case, and the review queue is a handful
of items, not a re-curation.

---

## 4. The slow path — offline, human, and cached

Location → geometry never runs in cron, because it needs the 9.6 GB FWA graph and
because tributary expansion is the highest-stakes logic in the pipeline.

```
new or rebound location
  └─ curator authors  extents[] + tributaries   (the §MAPPING.md vocabulary)
  └─ build_reach(entry, rule, registry, graph)  ← resolves, clips, classifies, EXPANDS
  └─ cache the resulting section ids into the entries file
```

Run it when a location is added or rebound. Never on a schedule. The cached section
list is what the cron joins against — **but it is cached per bundle version, never
across one** (AGENTS rule 6: 15% of surviving items changed their section list in one
rebuild).

---

## 5. Standardising the scrape: emit two files, not one

The single biggest simplification. The scraper should write the halves **separately**,
so the cron's check is a file comparison rather than a walk:

```jsonc
// output/dfo_salmon/locations/region-6.json   — what cron CHECKS
{ "region": "6", "source_sha256": "…", "scraped_at": "…",
  "locations": [
    { "fingerprint": "a3f19c22b7e1",
      "section": "B(i)",
      "waters": "Babine Lake",
      "specific_area": "Babine Lake excluding tributaries and those waters within a 400 m radius of …",
      "excludes": ["Morrison Creek", "…"],
      "precedence": 3 } ] }

// output/dfo_salmon/rules/region-6.json       — what cron PUBLISHES
{ "region": "6", "source_sha256": "…",
  "rules": [
    { "fingerprint": "a3f19c22b7e1",
      "species": "Sockeye", "dates": "Aug 1 to Aug 27", "limits_gear": "2 per day",
      "daily_limit": 2, "no_fishing": false,
      "fishery_notices": [{"text": "FN0679", "href": "https://notices.dfo-mpo.gc.ca/…"}] } ] }
```

`locations/region-6.json` is the *only* file the cron diffs. If it is byte-identical to
the last run, nothing about geography changed and every rule can be published blind.

**Rules carry no identity of their own.** They are replaced wholesale, keyed only by the
fingerprint of the location they sit on. No rule ids, no rule diffing, no merge. This is
what makes the volatile half cost nothing — and it is safe precisely because the rules
are re-derived from source every time.

---

## 6. The entries file — the curated half

```jsonc
// pipeline/dfo_salmon/entries/region-6.json   COMMITTED
{ "region": "6", "region_number": 6,
  "locations": [
    { "location_id": "6:babine-lake:excl-tribs",     // stable forever, never derived
      "water": "Babine Lake",
      "aliases": [],
      "section": "B(i)",                              // OBSERVED attribute, not identity
      "section_history": ["B(i)"],
      "fingerprint": "a3f19c22b7e1",                  // last confirmed source wording
      "fingerprint_history": ["9c1e…", "a3f1…"],      // every wording ever confirmed
      "source_text": { "waters": "…", "specific_area": "…" },

      "binding": {                                    // the expensive half
        "item_ids": ["…"],
        "extents": [{ "op": "whole", "splits": [] }],
        "tributaries": { "included": false },
        "sections_cached": { "bundle": "v19", "ids": ["…"] },
        "notes": [ "Closed within 400 m of the mouths of 12 named tributaries.",
                   "Closed east of a line from Gullwing Creek to the south shore." ],
        "spatial_caveat": true
      },
      "status": "active",                             // active | dormant
      "locked": true, "reviewed_by": "…", "reviewed_at": "…" } ] }
```

Three things worth noting:

* **`section` is an attribute with history.** When B became B(i)/B(ii) the binding must
  survive; only the attribute moves, and the move is a review event.
* **`fingerprint_history`** makes a *revived* wording a free hit. DFO flipped the Kispiox
  sign count between "three white triangular" and "the 4 triangular" **and back** — the
  second flip should cost nothing.
* **`status: dormant`** rather than deletion. Region 1 retired 4 waters of 31 and
  Region 2 retired 2 of 24; the Kispiox Resort reach has cycled out and back four times.

---

## 7. Babine, buffers, and location notes

**Agreed — do not model buffers or split polygons.** The binding is the whole lake, and
the 400 m exclusions become `binding.notes[]`. Same for the two lake-line cases
(Osoyoos North Basin, Quesnel Lake's Horsefly Bay). That removes 4 of the 9 unmappable
reaches from §MAPPING.md §7 and leaves nothing that blocks.

**The honest cost**, which needs one guard: binding the whole lake makes the rule
*over-permissive*. The map would show sockeye open inside the 400 m closures. So:

> `spatial_caveat: true` — the app **must** render `notes[]` alongside the rule and must
> not draw that rule as a plain fill. A caveated rule is a "check the notes" rule.

That is a display contract, not geometry, and it costs one boolean. Without it, the
simplification silently tells an angler a closed area is open.

---

## 8. Normalisation — minimal, and measured

Only collapse drift actually observed in the archives:

```
lowercase · collapse whitespace · strip punctuation
Hwy → Highway     #16 → 16       metres/meters → m      approx. → approx
Ck./Cr. → Creek   R. → River     & → and                curly quotes → ascii
```

**Do not normalise numbers.** "three signs" vs "4 signs" is a real difference in what the
source claims, and should trigger review rather than be smoothed away.

Normalisation alone removes 46% of location-change events on Region 6 and **0% on
Region 1** — it is worth doing because it is nearly free, but it is *not* the mechanism.
The mechanism is the cumulative entries file plus the drift matcher (§3), which is what
gets to 99%.

---

## 9. Why this is fast

| | cost |
|---|---|
| unchanged pages | 9 HTTP requests, stop at the hash |
| changed page | parse + untangle + one dict lookup per reach (237 total) |
| rules update | list replacement — no diff, no merge, no ids |
| geometry | **not in the loop** — cached in the entries file, recomputed only on rebind |
| human | ~0.4–2.0 locations per region per run |

The expensive things — the FWA graph, tributary expansion, curation — all sit on the
slow path and are touched only when a location genuinely changes, which the measurements
say is about once per region per run.

---

## 10. What actually changed, location-wise, across the archives

Every location add/remove across 12 Region 6 versions and 6/5 for Regions 1/2. The
shapes matter more than the totals, because each one is a case the reconciler has to
classify correctly.

**One structural event, 2018 → 2020.** Section B split into B(i)/B(ii) and the table
roughly doubled: 59 locations appeared at once (all of Babine, Bulkley, Morice, Skeena,
Kispiox, the whole B(ii) coastal set). This is not drift and must not be auto-rebound.

**Seasonal cycling — the dominant pattern.** These reappear on a binding that was kept:

| location | history |
|---|---|
| Kispiox River, "downstream of signs near Kispiox River Resort" | gone 2024-09, back 2025-05, gone 2026-01, back 2026-04 |
| Babine Lake, "within a 400 m radius of the mouth of Pinkut Creek" | added 2020, gone 2022, back 2025-08, gone 2026-04 |
| Babine River, the two Nilkitkwa confluence reaches | added 2024-09, gone 2025-05 |
| Squamish River powerline reach (R2) | gone 2024-09, back 2025-08 |
| Nakina River, Swift River (R6), Harrison River (R2) | all cycled out and back |

**Wholesale rewrites of the same place** — the case text similarity misses:

```
Stamp River   was: "Between fishing boundary signs approximately 200m upstream of and
                    500m downstream of the Stamp Falls fishway"
              now: "From approximately 200 m upstream of the Stamp Falls fishway
                    (50 downstream of signs) …"
Skeena River  was: "mainstem waters within 3 white triangular fishing boundary signs …"
              now: "all waters within the 4 triangular fishing boundary signs …"   (and back, twice)
```

**Real extent changes wearing drift's clothes** — these must NOT be auto-matched:

```
Harrison River  was: "…downstream to the confluence with the Fraser River"
                now: "…downstream to the Highway 7 Bridge"      ← a different terminus
```

**Genuine retirements** (dormant, not deleted): San Juan River, Tsitika River, Somass
and Stamp River tributaries (R1); Birkenhead River, Booth Creek (R2). Region 6 has
retired nothing in nine years.

That last pair of categories is why **nothing auto-binds on similarity**. An exact
fingerprint is the only thing that binds without a human; everything else arrives as a
ranked *proposal*.

---

## 11. The severity ladder — replayed against history

`entries.reconcile()` classifies every location, and the worst outcome decides whether
the region publishes:

| status | severity | who acts |
|---|--:|---|
| `ok` exact fingerprint hit | 0 | nobody |
| `dormant` absent from the page, binding kept | 0 | nobody |
| `revived` a dormant location is published again | 0 | nobody |
| `drift` reworded, candidates proposed | 1 | confirm the rebind |
| `new` no known location on this water | 2 | bind it |
| `section_moved` the section attribute changed | 3 | re-scope everything under it |
| `structural` the table itself moved | 4 | **hold the whole region** |

Structural fires on any of: the section set changing, the location count moving more
than ±20%, or more than 25% of locations arriving unbound.

Seeding on the oldest archived version and replaying every later one:

```
region 6   run       ok  rev  dorm  drift  new   verdict
           201805    59    0     5      5    1   review 6
           202004    59    1     6      5   59   HOLD REGION
              !! section set changed: [A B C D E F] -> [A B(i) B(ii) C D E F]
              !! location count moved 54 -> 86 (+59%, threshold ±20%)
              !! 64/124 locations unbound (52% > 25%)
           202404   124    1     5     10    1   review 11
           202409   135    0     1      1    2   review 3
           202412   137    0     1      1    0   review 1
           202505   136    1     2      0    0   clean
           202508   136    0     1      2    0   review 2
           202601   136    0     2      1    0   review 1
           202604   134    3     3      0    0   clean
           LIVE     135    0     2      3    0   review 3
```

The one event that should stop the pipeline does, on all three signals; ordinary
in-season updates cost **1–3 confirmations, and two runs need nobody at all**.

---

## 12. What is built

```bash
.venv/bin/python -m pipeline.dfo_salmon.locations     # -> locations/ + rules/ (248 + 439)
.venv/bin/python -m pipeline.dfo_salmon.entries seed  # create/extend entries/region-*.json
.venv/bin/python -m pipeline.dfo_salmon.entries reconcile [--verbose]
```

`entries/region-*.json` is committed and holds all 248 locations across 9 regions,
**all currently unbound** — `binding.item_ids` and `binding.extents` are empty, so
nothing can resolve against them yet. That is the honest state: the identity, the
cascade, the inheritance chains and the caveats are recorded; the geometry is not.

Region 6's cascade is carried as locations, not beside them, because rules attach to
the defaults exactly as they attach to a named water:

```
6:a:region                    kind=region_default   precedence=0
6:b(i):section                kind=section_default  precedence=1  inherits=[B, A]
6:e:area:areas-5              kind=area_default     precedence=2  inherits=[E, A]
6:f:closure                   kind=closure
6:babine-lake:including-…     kind=water            precedence=3  inherits=[B(i), B, A]
```

### Feeding reach + tributaries

`entries.to_reach_input(location, rules)` returns the `(entry, rules)` dicts that
`pipeline.reach.build.build_reach` consumes, so the DFO side reuses the provincial
resolver, the tributary walk and carve-out blocking rather than reimplementing them:

```python
entry, rules = to_reach_input(loc, scraped_rules)
for rule in rules:
    binding, diags = build_reach(entry, rule, registry, graph)
```

`build_reach` resolves the extent first and hands the tributary walk the measure window
it resolved to — which is what makes `downstream_of(signs)` + `tributaries=True` the
*watershed below the signs* rather than "the mainstem below the signs plus every
tributary anywhere". Call `build_reach`, never the layers underneath.

---

## 13. Seed the superset, not the current page

These pages list *openings*, so a location leaves when its fishery closes and returns
later. Seeding only from today's page means paying to re-curate it every time it comes
back. Seeding the **union of every archived version** makes a revival an exact
fingerprint hit on an already-bound record.

Measured by replaying every archived version through the reconciler:

| Region | seeded from one version | seeded from the superset |
|---|--:|--:|
| 6 Skeena | **102** reviews | **3** |
| 1 Vancouver Island | 19 | 1 |
| 2 Lower Mainland | 20 | 1 |

The work does not vanish — it moves. Those cycling locations become `revived` outcomes
(6 → 105 on Region 6), which need nobody. The residue is genuinely new water.

Location counts grow accordingly, and this is the set to curate:

| | live page | + archived history |
|---|--:|--:|
| Region 6 | 138 | **166** |
| Region 1 | 55 | **81** |
| Region 2 | 26 | **66** |
| all 9 regions | 248 | **354** |

```bash
.venv/bin/python -m pipeline.dfo_salmon.entries seed --history
```

Oldest version first, so ids read in the order DFO introduced them. The superset pass
never marks anything dormant — absence from a 2017 page says nothing about today; only
the live pass sets active/dormant.

---

## 14. The cascade, as spatial scopes

The lettered sections are not labels. Each is a real extent, and the letters describe
containment. `cascade.build_scopes()` derives this tree and stores it in the entry file:

```
A   region            All Region 6 waters
├── B   watershed         Skeena River                     (container only — no rules)
│   ├── B(i)  watershed_above   Skeena @ CNR Railway Bridge at Terrace
│   └── B(ii) watershed_below   Skeena @ CNR Railway Bridge at Terrace
├── C   watershed         Nass River
├── D   island_group      Queen Charlotte Islands / Haida Gwaii
├── F   watershed         Fraser River  (within Region 6 — closed outright)
└── E   residual          minus [B, C, D, F]
    ├── E:areas-3-4-5-6   tidal_areas  [3, 4, 5, 6]
    ├── E:areas-5         tidal_areas  [5]
    └── E:areas-6         tidal_areas  [6]
```

Two things a flat inheritance list got wrong:

* **B(i) and B(ii) are the watershed above and below one point** — the same primitive
  as a named water's reach (`upstream_of(anchor)` over a tributary-included item), just
  applied to the whole Skeena. Nothing new is needed to bind them; they are the largest
  instance of the case the model already handles.
* **E is a set difference**, not a list. "Other Mainland Watersheds, except for the
  Fraser" = Region 6 minus Skeena, Nass, Haida Gwaii and Fraser. It cannot be bound by
  naming waters, and it must be evaluated *after* its siblings.

`chain_for(scope_id)` gives the lookup order, narrowest first:
`E:areas-5 → E → A`, `B(i) → B → A`, `F → A`. A named water's own reach is consulted
before any scope containing it.

### The tidal Areas — the one open spatial primitive

`E`'s sub-scopes mean *"streams and lakes flowing into tidal waters of Area N"*, which
is about a stream's **outlet**, not containment:

| Area | Coast |
|---|---|
| 3 | Portland Inlet, Alaska border |
| 4 | Prince Rupert, Porcher Island |
| 5 | Banks Island |
| 6 | Kitimat, Kemano Bay |

The polygons are not new work: `data/build_tidal_boundary.py` and
`process_tidal_boundary.py` already consume
`dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp` — the DFO Pacific Fishery Management Area
subareas — and the repo's `tidal_boundary` layer is built *from* it. Only the merged
polygon was retained in `bc_fisheries_data.gpkg`; the per-Area geometry needs
re-fetching from the same source (the build gpkg lives in the R2 build-assets bucket).

Given those, `drains_to_area(N)` is: take each coastal stream's outlet where it meets
the existing `tidal_boundary`, test which Area polygon contains it, and scope to the
watershed above that outlet — the watershed-above-a-point primitive again.

`cascade.unresolvable()` lists exactly the scopes that need more than a named water:
`E`, and the three `E:areas-*`. Four scopes, one mechanism.

---

## 15. Open, and what I would decide

1. **`location_id` format.** `6:babine-lake:excl-tribs` is readable and diffs well;
   a counter (`6:loc:0041`) never collides. I would take the readable one — the whole
   point is that a human reviews these.
2. **Does `dormant` publish?** I would say no: a dormant location has no rules on the
   page, so there is nothing to publish. It stays in the file to make revival free.
3. **Where does the review happen?** At ~1–2 items per region per run, a Markdown
   report in the PR is enough. `curation-review/` is more machinery than the volume
   justifies until it isn't.
4. **Initial curation is the real cost**: ~237 locations to bind once. That is the
   project, and everything above exists to make sure it is paid only once.
