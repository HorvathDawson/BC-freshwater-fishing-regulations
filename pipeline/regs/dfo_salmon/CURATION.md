# What is manual, what is not, and how to do the manual part

> **Running a curation sitting?** This document is the *design*. The *operating manual* —
> exact commands, the per-water loop, current counts, and the traps — is
> [`pipeline/docs/HANDOFF-dfo-curation.md`](../docs/HANDOFF-dfo-curation.md).

**Every number here is measured** from the 346 seeded locations and 438 scraped rules.
Reproduce with `pipeline.regs.dfo_salmon.locations`, `… entries seed --history`, and the
registry at `output/v2/full/registry.json`.

---

## 1. The dividing line

| | automatic, every run | manual, once per location |
|---|---|---|
| which page changed | `sha256` per page | — |
| location identity | `fingerprint` exact match (93–99%) | confirm a rebind when it drifts |
| **which registry item** | *proposed* by name (77% unique) | **confirm or pick** |
| **where the cut points are** | anchor phrase extracted verbatim | **place the split** |
| tributary in/out | read off the source text | confirm |
| carve-outs | listed verbatim | author as extents |
| the cascade scopes | tree derived (`cascade.py`) | **bind 17 scopes** |
| species | 8 distinct values, all mapped | — |
| dates | 427/438 parse (97.5%) | 11 rows, 3 known classes |
| limits / gear / quota | parsed to flags + verbatim | — |
| reach → sections | `build_reach()` | — |

**The regulation half is essentially free. All of the manual cost is geometry.**

---

## 2. The work, bucketed

346 locations (superset of every archived version), by what a human must supply:

| bucket | n | what you do | per item |
|---|--:|---|---|
| **A. whole water** — unique name match | 89 | confirm the proposed item | seconds |
| A. whole water — ambiguous (2–23 candidates) | 15 | pick the right item | ~1 min |
| A. whole water — no name match | 13 | find the item by hand | ~2 min |
| **B. one cut point** — unique match | 75 | confirm item + place 1 split | ~2 min |
| B. one cut point — ambiguous / no match | 18 | + find the item | ~3 min |
| **C. two cut points** — unique match | 75 | confirm item + place 2 splits | ~3 min |
| C. two cut points — ambiguous / no match | 9 | + find the item | ~4 min |
| **D. carve-out / zone** | 15 | item + exclusion extents | ~5 min |
| **E. prose** | 20 | read it and decide | varies |
| **SCOPE** — region / section / residual / areas / closure | 17 | see §5 | varies |

Distribution by region: Region 6 carries 163 of the 346, Region 1 81, Region 2 66.
Regions 3, 4, 5a, 5b, 7 and 8 total 36 between them.

**77% of water locations (271 of 329) resolve to exactly one registry item by name**,
so the common case is confirming a proposal, not searching. 25 are ambiguous (Long Lake
has 23 candidates; most have 2–3) and 33 have no match — those are name variants
("Cayeghle River", "Zymoetz (Copper) River"), compound names
("Chilliwack/Vedder River (including Sumas River)"), and tributary-set pseudo-waters
("Somass River tributaries").

### Cut points are the real cost

177 locations need cut points; **261 anchor placements** in total (93 × 1, 84 × 2).

Two different questions, with two very different answers:

| question | answer |
|---|---|
| is the *water* already in `splits.json`? | 110 of 177 locations — **62%** |
| is the *anchor itself* already a curated split? | 52 of 261 — **20%** |

**209 anchors (80%) must be authored.** Being on a river that already has splits helps —
the block exists, the conventions are set, the geometry is loaded — but the specific
bridge or boundary sign usually is not there yet. By region: Region 6 needs 63,
Region 2 59, Region 1 58, and the rest 29 between them.

39 distinct waters have no split block at all. The heaviest are Chilliwack/Vedder
(6 locations), Alouette (5), Tatshenshini (5), Stave (4).

> **The extracted anchor phrase is a hint, not a contract.** It is often poor —
> `"the"`, `"the Canyon City Bridge located approximately 5"` (truncated at a decimal),
> phrases pulled from an `except` clause. It does not matter much: the fingerprint
> hashes the *whole* `specific_area` string, the binding is `(op, split_ids)`, and the
> curator reads the full verbatim sentence, which is always correct. Improving the
> phrase saves a little reading, nothing more.
>
> **`op` is the field that matters** — it decides how many splits a location needs and
> what shape the extent is. It is audited: see §8.

---

## 3. How to actually do it: curate by **water**, not by location

329 water locations sit on **162 distinct (region, water) pairs — 2.0 locations each**.
Identifying the registry item is per *water*; only the cut points are per *location*.
So the unit of work is a water, and picking the item once serves every reach on it.

The distribution is very top-heavy:

| locations on one water | waters |
|---|--:|
| 1 | 96 |
| 2 | 34 |
| 3–6 | 28 |
| 8–15 | 4 |

The four heaviest are Skeena River (15), Stamp River (14), Morice River (9) and Somass
River (8). **Doing those four covers 46 locations — 14% of the corpus in four sittings.**

### Suggested order

1. **The 17 scopes first** (§5). Section B(i) is the whole upper Skeena; binding it
   gives every water inside it a sane fallback, so a mistake below is visible.
2. **The 4 heaviest waters** — Skeena, Stamp, Morice, Somass. 46 locations.
3. **The 96 single-location waters with a unique name match** — a confirm queue. These
   should go quickly and they are ~30% of the corpus.
4. **Ambiguous and no-match** (58) — needs judgement, batch them separately.
5. **Carve-outs and prose** (35) — last, when the conventions are settled.

### What the tool should give you, per water

```
Bulkley River   [region 6, section B(i)]   4 locations
  registry candidates:  ✔ item:… "BULKLEY RIVER"        (unique name match)
  existing splits on this water:  3 curated
  ┌ reach 1  "downstream of the Morice River confluence excluding tributaries."
  │    op downstream_of · anchor "the Morice River confluence" · tributaries: FALSE
  │    → existing split `bulkley__morice_into_bulkley`  ✔ reuse?
  ├ reach 2  "all tributaries of the Bulkley other than Morice…, Suskwa…, Two Mile Creek"
  │    tributaries_only · 3 carve-outs to author
  └ …
  rules that will attach: 11
```

Everything on that card except the two ✔ decisions is already in the entry file. The
review app in `curation-review/` is the natural host; at 162 waters a generated
Markdown worklist would also do.

---

## 4. Reach building — how each bucket becomes geometry

All of it goes through `pipeline.atlas.reach.build.build_reach`, via
`entries.to_reach_input()`. **Never call the layers underneath**: the review app once
resolved and classified without expanding, and showed a curator the mainstem of a reach
that included its whole tributary system.

| bucket | binding you author | what `build_reach` does |
|---|---|---|
| A. whole water | `item_ids=[X]`, `extents=[{op: whole}]` | every section of X |
| A. + "including tributaries" | `… tributaries=True` | X, then the tributary walk over all of it |
| B. one cut | `extents=[{op: upstream_of, splits:[s]}]` | sections above `s`, following the water |
| B. + tributaries | `… tributaries=True` | **the watershed above `s`** — the walk starts from the resolved window, so tributaries joining below `s` are correctly out |
| C. two cuts | `extents=[{op: between, splits:[a,b]}]` | sections between them |
| D. tributary set | `tributaries_only=True`, `tributary_excludes=[…]` | tributaries minus each excluded stream **and everything above it**, blocked during the walk |
| D. spanning two waters | `item_ids=[X, Y]` | a reach whose ends sit on different items |
| E. prose | whatever it turns out to be | — |

Two invariants worth stating because they are easy to get wrong:

* **Order matters.** `build_reach` resolves the extent first and hands the tributary
  walk the measure window it resolved to. `upstream_of` *then* expand = watershed above
  the point. Expanding first and cutting after gives a different, wrong answer.
* **A cached section list is per bundle version.** `binding.sections_cached` records
  which bundle produced it; never reuse one across a rebuild (15% of surviving items
  changed their section list in one rebuild).

---

## 5. The 17 scopes

These are not waters and need their own treatment:

| scope | n | how to bind |
|---|--:|---|
| `A` region default | 1 | the Region 6 boundary polygon |
| `B`, `C`, `D`, `F` + section defaults | 9 | `B` = the Skeena watershed = item + `tributaries=True`; `B(i)`/`B(ii)` = the same watershed cut `upstream_of` / `downstream_of` the **CNR Railway Bridge at Terrace** — one split, two scopes. `C` = Nass, `F` = Fraser ∩ Region 6. `D` = Haida Gwaii watersheds |
| `F` closure | 1 | as above; the rule is a blanket closure |
| `E` residual | 1 | **nothing to bind** — see below |
| 3 × `E:areas-*` | 3 | **the only genuinely open ones** |

**`E` needs no binding.** "Other Mainland Watersheds, except for the Fraser" reads like
a set difference, but nothing has to compute one. A water *listed* in the table already
declares its section; an *unlisted* water is placed by testing its siblings in order —
in the Skeena? the Nass? Haida Gwaii? the Fraser? — and falling through to E. Those
siblings are bound anyway, so E is free. Rendering "everywhere E applies" is that same
fallthrough run over every section: derived, never authored.

The three `E:areas-*` mean *"streams flowing into tidal waters of Area N"*, which is
about a stream's **outlet**, not containment:

| Area | coast |
|---|---|
| 3 | Portland Inlet, Alaska border |
| 4 | Prince Rupert, Porcher Island |
| 5 | Banks Island |
| 6 | Kitimat, Kemano Bay |

The polygons are not new work: `data/build_tidal_boundary.py` already consumes
`dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp`, and the repo's `tidal_boundary` layer is
built from it — only the merged polygon was kept. Re-fetch the source, then
`drains_to_area(N)` = take each coastal stream's outlet at the existing tidal boundary,
test which Area polygon contains it, and scope to the watershed above that outlet. The
watershed-above-a-point primitive again.

---

## 6. Matching the rules back — measured on the real scrape

438 rules across 9 regions. This half needs no curation.

**Species — 8 distinct values in the entire corpus:**

| value | n |
|---|--:|
| `Coho` | 132 |
| `Chinook` | 114 |
| `All` | 103 |
| `Sockeye` | 44 |
| `Pink` | 28 |
| `Chum` | 15 |
| `Sockeye, pink and chum` | 1 |
| `Sockeye, Pink, Chum` | 1 |

Six singles, two compounds (the same rule, punctuated two ways — a drift instance), and
`All`. They map onto `pipeline/regs/parsing/species.py` codes with a fixed 8-row table.

**Dates — 427 of 438 (97.5%) parse** with the existing
`pipeline.regs.parsing.dates.parse_date_window`, unchanged. The 11 that do not are three
honest classes, not failures:

| value | n | meaning |
|---|--:|---|
| `To be determined` | 5 | not yet set — no window; render as "not yet announced" |
| `Apr 1 until further notice` | 5 | open-ended from Apr 1 |
| `` (empty) | 1 | banner-derived rule (section F) — no window |

**Limits / gear / quota** are parsed to flags alongside the verbatim string:
`daily_limit`, `no_fishing`, `non_retention`, `hatchery_marked_only`, `bait_ban`,
`single_barbless_hook`. The verbatim `limits_gear` is always kept and is what should be
displayed; the flags are for filtering and colouring only.

### The join

```
rules/region-6.json      rule.fingerprint  ─┐
                                            ├─► entries/region-6.json  (location_id, binding)
locations/region-6.json  location.fingerprint ┘
                                            └─► build_reach()  ─►  section_id → [rule]
```

Then, for `(section_id, date, species)`:

1. collect every DFO rule bound to that section, with its `precedence` and `section_key`;
2. drop rules whose parsed date window excludes the date, and whose species does not
   match (`All` matches everything; `Sockeye, pink and chum` matches each of the three);
3. take the **highest precedence** remaining, walking `chain_for(section_key)` —
   named water › area catch-all › section catch-all › region default;
4. attach `fishery_notices` — those are live variation orders and the most perishable
   part of the answer;
5. attach `binding.notes` whenever `spatial_caveat` is set, and do **not** render that
   rule as a plain fill.

Every rule keeps `authority: "dfo"`. A section can carry a provincial trout rule and a
federal salmon rule at once, and neither overrides the other.

---

## 7. Honest totals

| | count |
|---|--:|
| locations to bind | **346** |
| …on distinct waters | 162 |
| …with a unique item proposed for you | 271 (77% of water locations) |
| anchor placements needed | 261 |
| …already a curated split | 52 (20%) |
| …**to author** | **209** |
| …on a water whose split block already exists | 62% of locations |
| scopes to bind | 14 (`E` is the else-branch and needs none) |
| scopes needing new machinery | **3** (the `E:areas-*` outlet test) |
| rules needing curation | **0** |

The parser work is done and the identity/cascade/caveat scaffolding is recorded. What
remains is 162 waters' worth of item confirmation and roughly 261 cut points — and
about six in ten of those sit on rivers this repo has already curated once.


---

## 8. What I would do next, in order

1. **`op` is audited and clean — keep it that way.** A sweep for contradictions
   ("between X and Y" carrying a one-sided op; op-words taken from a trailing `Note:`
   clause) found 10 of 329 wrong. Both causes are fixed and the sweep now returns zero;
   it runs as a test over the whole corpus. `op` decides how many splits a location
   needs, so it is the derived field worth guarding. The *anchor phrase* is only a
   reading aid and does not need the same care.

2. **Bind the 13 scopes.** Small, high-leverage, and self-checking: `B(i)`/`B(ii)` are
   one split (the CNR Railway Bridge at Terrace) used twice, and getting them right
   gives every water inside the Skeena a sane fallback, so mistakes below are visible.

3. **Build the per-water curation card** (§3). 162 waters, ~2 locations each, with the
   registry candidate, the existing splits on that water, and the anchors pre-extracted.
   At this volume a generated worklist is enough; the review app is the natural home if
   it turns out not to be.

4. **Re-fetch the PFMA subareas layer** and implement `drains_to_area(N)` — outlet at
   the existing tidal boundary, point-in-polygon against the Area, watershed above it.
   Unblocks the last 3 scopes and 5 locations.

5. **Then curate**, heaviest waters first: Skeena (15 locations), Stamp (14),
   Morice (9), Somass (8) — 46 locations in four sittings.

6. **Wire the cron** only once something is bound. Until then it has nothing to publish,
   and `validate` + `reconcile` already tell you whether the source moved.
