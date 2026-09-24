# Mapping DFO salmon locations onto entries

**Design note. Nothing here is built.** Numbers are measured from the 9 live pages
(237 reaches, 2026-08-29) and from archived versions; reproduce with
`python -m pipeline.regs.dfo_salmon.untangle` and `python -m pipeline.regs.dfo_salmon.churn`.

---

## 0. The short version

**Do not invent a second geometry vocabulary.** The provincial entry model already
expresses everything DFO says, and the hardest case — *"the watershed of a stream above
a point"* — is already implemented, tested, and described in this repo as the
highest-stakes logic in the pipeline.

Measured: **228 of 237 DFO reach scopes (96.2%) map directly onto the existing
`Extent` ops.** The work is not new geometry. It is (a) authoring the bindings, (b)
keying them so they survive what the source actually does, and (c) a join at export.

---

## 1. Coverage, per region

Every reach on every page, decomposed into `op` (see §3):

| op | 1 | 2 | 3 | 4 | 5a | 5b | 6 | 7 | 8 | **total** |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `whole` | 21 | 10 | 3 | · | · | 8 | 67 | · | 1 | **110** |
| `between` | 21 | 5 | 4 | · | 1 | 2 | 12 | 1 | 4 | **50** |
| `downstream_of` | 10 | 8 | · | · | · | 1 | 17 | · | · | **36** |
| `upstream_of` | 3 | 3 | · | · | · | 1 | 29 | · | · | **36** |
| `within` (zone/area) | · | · | · | · | 1 | 1 | 3 | · | · | **5** |
| **total reaches** | **55** | **26** | **7** | **0** | **2** | **13** | **128** | **1** | **5** | **237** |

Region 4 has no reaches at all — one blanket non-retention row. Every other region is
served by the same five ops. **Region 6 is not a special case; it is just larger.**

The 9 that do not map cleanly are listed in §7. None needs a new op.

---

## 2. Identity — the decision the history forces

Twelve archived Region 6 versions (2017, 2018, 2020, 2022, 2024×3, 2025×3, 2026×2) say
a water name, once published, never disappears — **union of all versions = 76 waters,
all 76 present today, zero retired.** The **one** structural change in that decade was
section B splitting into B(i)/B(ii) at the CNR Railway Bridge, which happened between
2018-05 and 2020-04.

**That property is Region 6's, not a law.** Region 1 retired 4 of 31 waters over the
same span (San Juan River, Tsitika River, Somass and Stamp River tributaries) and
Region 2 retired 2 of 24 (Birkenhead River, Booth Creek). Those are *dormant*, not
deleted — these pages list openings, so a water leaves when its fishery closes. A
binding must be kept and marked dormant, never dropped.

> **Entry identity is `(region_number, normalized_water_name)`.**
> Section is a *versioned attribute*, never part of the id.
> Reach identity is a *curator-assigned key*, never the scope text.

Keying on section would have orphaned every Skeena binding the day B split. Keying a
reach on its scope string would orphan it on `"Highway 37 Bridge"` → `"Highway 37
bridge"`. Both drifts are real and both are pinned by tests.

A water appearing in two sections (the Skeena, in B(i) and B(ii)) is **one entry with
reaches on both sides of the cut** — not two entries. The section boundary is itself a
`between`/`upstream_of` extent, so it is expressible as geometry rather than as identity.

---

## 3. Scope is compositional, not a single kind

The first cut at this used one `kind` enum per reach and it was wrong: it forced a
choice between `downstream_of` and `excluding tributaries` for
*"downstream of the Morice River confluence excluding tributaries"*, which states both.

A DFO scope decomposes into **four independent axes**:

```
reach_scope  =  op(anchors…)            ← where along the water
              ∧ tributary_scope         ← none | included | only
              ∧ carve_outs[]            ← EXCEPT clauses, subtracted
              ∧ item_scope              ← which registry item(s)
```

That is exactly `Extent(op, splits, item_id/item_ids, area_id)` plus
the catalogue's `includes_tributaries` / `tributaries_only` / `tributary_excludes`. No extension
required.

### Tributary scope is per-reach in DFO

The provincial synopsis states tributary inclusion **per row**; DFO states it **per
reach**. Kitseguecla River carries a bait ban on the mainstem and a chinook closure on
the mainstem *and* its tributaries — one water, two different tributary scopes.

This is already handled: `includes_tributaries` is three-valued on a rule and inherits
from the entry when `None` (AGENTS rule 9 — reading only the rule's own field
undercounts 132 vs the real 554). So:

* `entry.includes_tributaries` ← the location's flag, else its water's (`to_reach_input`);
* **the reach's tributary scope becomes the rule's `includes_tributaries`.**

---

## 4. "The watershed of a stream above a point"

This is the case you flagged, and it is the most common non-trivial one:
**37 reaches across 4 regions are directional *and* tributary-included** (R6: 32,
R1: 2, R2: 2, R5b: 1).

Example — Kispiox River, section B(i):

```
water : Kispiox River (including tributaries)
reach : "downstream of fishing boundary signs near Kispiox River Resort"
rule  : Pink, Jun 16 to Aug 23, 2 per day
```

The bound water is **the mainstem below the signs, plus every tributary that joins
below the signs** — a subtree of the flow graph. A tributary joining *above* the signs
is out of scope even though the water "includes tributaries".

### It already works, and the order of operations is why

`pipeline/atlas/reach/build.py::build_reach` composes exactly this:

```
resolve_extent(op, splits)   →  sections + the measure `window` it resolved to
        ↓
classify(...)                →  with expand_tributaries(window=window)
        ↓
tributaries.expand           →  walk everything draining into THAT window
```

The tributary walk is handed the window the extent resolved to, *so it never re-derives
where the reach starts*. `upstream_of(X)` then `expand` therefore yields the watershed
above X — not "the mainstem above X, plus all tributaries anywhere", which is the wrong
answer and the one a naive union produces.

The docstring records that the review app got this wrong once: it resolved and
classified but never expanded, so a curator confirming "including tributaries" was shown
the mainstem alone. **DFO must call `build_reach`, not the layers underneath.**

### Carve-outs come free

*"all tributaries of the Bulkley River other than Morice River and tributaries, Suskwa
River and tributaries, and Two Mile Creek"* →
`tributaries.only = True` + three `excludes` extents.

`resolve_carve_outs` blocks each named stream **and everything above it**, and blocks
*during* the walk rather than subtracting afterwards — so the walk cannot descend
through excluded water into catchments that drain only through it. That is precisely
what the Bulkley clause means.

---

## 5. The two cascades

Resolution is a cascade at **two** scales, and both must be applied or a closure leaks.

**Between waters** (§ Region 6, from `untangle.py`):
```
3 named water  ›  2 area catch-all  ›  1 section catch-all  ›  0 region default
```

**Within one water** — 7 reaches say *"except in those areas and times listed below"* /
*"all waters unless indicated below"* (R1: 5, R5b: 2). A reach that says this is a
**default for that water**, overridden by the narrower reaches published after it.
Source order is the precedence order.

So a resolved answer for (section, species, date) is:
```
narrowest matching reach on the water
  → the water's own "unless below" default reach
    → area catch-all → section catch-all → region default
```

---

## 6. Getting the information out

The curated half and the scraped half stay in separate files and are joined at build
time. **Rules are never curated** — they turn over ~50%/year.

```
entries/region-6.json         CURATED   water → reach → (extents, tributaries), locked
untangled/region6.json        SCRAPED   water → reach → rules (species/dates/limits)
        └──────── join on (entry_id, reach_key) ────────┘
                              ↓
                      build_reach()  per rule
                              ↓
        dfo_salmon/region6.bound.json   reach_key → section ids + rules
                              ↓
        export: section_id → [dfo_rule]  (the shape the app consumes)
```

**Query shape.** Given `(section_id, date, species)`:
1. collect every DFO rule bound to that section, with its `precedence`;
2. drop rules whose date window excludes the date, and whose species does not match
   (`"All"` and `"Sockeye, pink and chum"` both match sockeye);
3. take the highest precedence remaining; ties resolve in source order;
4. attach any `fishery_notices` — those are **live variation orders** and are the part
   most likely to be stale.

**Two authorities, never merged.** A section can carry a provincial trout rule and a
federal salmon rule simultaneously and neither overrides the other. The export must
keep `authority: "dfo" | "province"` on every rule, and the app should surface salmon
rules as their own layer. Merging them into one list is the failure this whole module
is arranged to prevent.

**Bind to `item_id`, never `section_id`** (AGENTS rule 5) — and never cache a resolved
section list across bundle versions (rule 6).

---

## 7. What does not map — 9 reaches, 4 classes

| class | n | example | disposition |
|---|--:|---|---|
| **other named water in scope** | 3 | *"including Colonial River."*; *"North Alouette and tributaries"* | `Extent.item_ids` — the model already supports multi-item extents |
| **cross-reference in scope** | 2 | *"including tributaries ( See the Tahltan River )"* | pointer, not geometry — emit as `see_also` |
| **lake sub-basin by a line** | 3 | Osoyoos north of the Hwy 3 bridge; Quesnel Lake's Horsefly Bay; Babine "east of a line from Gullwing Creek" | **deferred class** — a lake divided into closure areas is already a deferred case in this repo |
| **radius buffer** | 1 | Babine Lake, *"within a 400 m radius"* of 12 named creek mouths | **genuine gap** — no buffer op exists. Needs either a new `Op.WITHIN_RADIUS` or a curated polygon |

Plus 3 reaches carry lat/lon pairs (R3, R5a, R8) — resolvable, but they need a
coordinate anchor type rather than a named-feature anchor.

**Nine items, of which one is a real modelling gap.** Everything else is either already
expressible or already a known deferred class.

---

## 8. Per-region verdict

| Region | Reaches | Maps cleanly | Notes |
|---|--:|---|---|
| 1 Vancouver Island | 55 | 52 | 5 within-water forward refs; 2 "all open portions"; 1 multi-item |
| 2 Lower Mainland | 26 | 25 | 1 nested water (North Alouette) |
| 3 Thompson-Nicola | 7 | 7 | 1 lat/lon anchor |
| 4 Kootenays | 0 | — | blanket closure, no geometry |
| 5a Cariboo (Fraser) | 2 | 1 | Quesnel Lake / Horsefly Bay polygon |
| 5b Cariboo (coastal) | 13 | 13 | 1 Management-Unit `within`; 2 forward refs |
| **6 Skeena** | **128** | **124** | 32 watershed-above-a-point; 1 radius buffer; 2 cross-refs; 1 lake line |
| 7 Omineca-Peace | 1 | 1 | — |
| 8 Okanagan | 5 | 4 | Osoyoos North Basin polygon |

**It works for every region.** The residue is 9 reaches, concentrated in lakes.

---

## 9. Compatibility with historical versions — verified

Every cached version of every region, re-parsed with the current parser and decomposed
with the §3 axes. **The op vocabulary is stable across nine years:**

| Region | Versions | Span | op coverage | Waters retired |
|---|--:|---|---|---|
| 6 Skeena | 12 | 2017-07 → 2026-08 | **93–97%** | **0 of 76** |
| 1 Vancouver Island | 6 | 2024-01 → 2026-08 | 94–97% | 4 of 31 |
| 2 Lower Mainland | 5 | 2024-01 → 2026-04 | 96–100% | 2 of 24 |

No version of any page ever needed an op that does not exist. The residue stays at
3–7% and is the same four classes listed in §7. **The mapping is compatible with what
the source actually does.**

### A parser defect this check found

The 2020-04-08 Region 6 archive holds 202 `<tr>` of which **only 23 are direct children
of `<tbody>`** — an unclosed cell tag makes html.parser nest the other 179 inside each
other. `find_all("tr", recursive=False)` dropped 88% of that page's rules with no error
(6 waters instead of 77), and it silently cost the 2018 page a third of its rows too.
Fixed in `_row_tags()`, pinned by two tests. Current-page output is unchanged (439 rows
before and after) — this only ever affected historical replay, which is the argument for
doing historical replay.

## 10. Still to verify

* **Regions 3, 4, 5a, 5b, 7 and 8 have no archived versions pulled** — Wayback 503s
  under load (3 of 21 targets in one pass). Their flux is inferred from 1/2/6, not
  measured. 2019, 2021 and 2023 are still missing for Region 6. Backfill opportunistically.
* **The resolver contract is recorded intent for `within`.** `Op` says directional ops
  follow the water across items; `within` with an `area_id` is the least-exercised path
  and DFO needs it for tidal Areas (R6) and Management Units (R5b).
* **DFO tidal Areas are not provincial regions.** R6 section E scopes by
  *"streams flowing into tidal water Area 5"*. That needs a DFO Area → watershed
  mapping that does not exist in this repo yet. 5 reaches depend on it.
