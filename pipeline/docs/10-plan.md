# 10 — The plan: resolution → artifacts → clients

Single source for what happens after curation. Replaces and absorbs the old `06` (zone/MU regs),
`09` (storage & client), `15` (current state), `16` (delivery), `17` (regulation model) and `18`
(builder), which are in `archive/`.

---

## 1. Where we are

```
splits ─▶ graph+registry ─▶ parse (human) ─▶ curation review ─▶  ???  ─▶ clients
  ✅            ✅               ✅              🔄 108/1392       ❌      v1 only
```

| | |
|---|---|
| graph | 2,030,222 nodes · 49,639 registry sections (**2.4%** of the network) |
| registry | 19,722 items — 12,000 stream, 7,672 lake, 46 wetland, 4 area |
| entries | 1,392 (1,340 matched, 52 no_registry) · **108 confirmed** · 44 reference-only |
| rules | 3,038 — harvest 807, gear 787, closure 690, vessel 391, note 263, licensing 100 |
| tests | 275 pass / 8 skip |

**The gap:** v1 (live at canifishthis.ca) is archived under `archive/pipeline/` and no longer
buildable; v2 stops at the review app. Nothing downstream consumes the section spine.

Curation gates *launch*, not this work. Every entry confirmed later is a data push, not a release.

---

## 2. How a reach is resolved today

The input types, and what turns them into sections:

```
Entry ─ rules[] ─ Extent{op, splits[], item_id|item_ids[], area_id}
                        │
                        └─▶ reuse.resolve_extent(covered_ids, extent) ─▶ [section_id, …]
```

- `op` ∈ `whole | upstream_of | downstream_of | between | within` — 2,525 `whole`, 152
  `downstream_of`, 144 `between`, 119 `upstream_of`, 2 `within`.
- Resolution is **by route measure on the cut's own blue line**, never a flow walk. A `between`
  spanning a name change resolves as the intersection of two half-lines.
- Entry-level `scope` clips every rule in the row (the four Fraser regional rows; the two Adams rows).
- `includes_tributaries` — **554 rules across 264 entries**, not the 111 first counted. The field is
  three-valued: `None` inherits `entry.tributaries.included` (`entry_models.py:206`), and 210 entries
  set it. 44 rules are tributaries-only, not 4.

> **The reach-scoped tributary walk does not exist yet.** An earlier draft of this plan claimed
> `resolve_extent` "already gets this right". It does not — it never touches tributaries
> (`reuse.py:615-750`). The only v2 tributary code is `item_tributaries`, which is **one level, from
> the confluences layer, and item-scoped**. Lifting `resolve_extent` therefore gets you none of the
> bitmap: the recursive, reach-scoped, barrier-aware walk is **unwritten work** and has to be
> sequenced as such.

`resolve_extent` is still the most-tested logic in the project and must not be reimplemented for the
*direct* extents. Task 1 is lifting it into `pipeline/` so the builder and the review app share one
implementation — but it is a smaller task than it looked.

### Two kinds of regulation, and only one attaches to a water

1. **Water-specific** — "Adams River: quota 2". Attached to sections.
2. **Base / zone / provincial** — "Region 4 trout limit is 5", "no fishing in Ecological Reserves".
   Rules for fishing *in this area*. **Resolved by location, never copied onto sections.**

`archive/pipeline/enrichment/base_regulations.json` holds 235 of these — 228 zone (by `zone_ids` +
`feature_types`), 3 provincial (parks/reserves via `admin_targets`, `buffer_m: 500`), 2 land-access,
2 municipal. Hand-sourced with synopsis page citations: **port the data, rebuild the assigner.**

**MU is the lookup key, not region.** Region is a display grouping. The client resolves its MU by
point-in-polygon and pulls that MU's overlay — one "General rules for this area" panel beside the
"This water" panel. A river crossing an MU boundary with no *specific* difference stays **one
section**; only genuinely differing specific regs get a curated `mu_boundary` split (the Fraser).
Curated, not automatic — auto-splitting would couple section geometry to regulation data, so a new
reg would silently change `section_id`s.

**Defaults are lifted, not replaced.** `exempts_from` (81 rules, 7 codes) is how a water-specific rule
removes a default:

```
applicable = base regs for (MU, feature_type, admin areas)
           − those a water-specific rule here exempts_from
           + the water-specific rules
```

This subtraction is the only correct way to answer "is it open" — open is often the *absence* of a
default plus an exemption, not the absence of a closure.

---

## 3. The four measurements that constrain the artifacts

**① 97.6% of the network is not in the registry.** A tap on an unnamed creek is the normal case.

**② `includes_tributaries` expands enormously.**

```
Fraser    own 37 sections → 419,164 tributary sections (20.6% of BC)
Kootenay  own 47          →  63,978        Iskut 28,328        Chemainus 361
```

111 rules across 83 items. Materialising these as rows means one Fraser rule → 419,164 rows.

**③ The cheap fix is wrong, instructively.** FWA watershed-code prefix never *misses* reachable water
(BFS-only = 0 in all tests) but over-includes: the Kootenay gains **2,563 sections including Moyie
River, Yahk River, 3,205 km** — waters that join *below* the regulated reach.

> Tributary scope is relative to **the rule's extent**, not the named river.

**④ The regulation data is small.** Registry + entries + FTS5 as SQLite: **9.6 MB, 2.8 MB gzipped,
2,462 pages.** Geometry and tributary scope are the only things with scale problems.

---

## 4. What it turns into

> ⚠️ **Superseded by [`13-build-plan.md`](13-build-plan.md) §1–§2.** The *schema* below is current and
> its census corrections all stand. The *artifact set*, the "one artifact set, two access modes"
> commitment, and the sharding discussion are replaced: web and mobile get separate packagers over one
> content store, and gauges / stocking / bathymetry — absent below — get homes.

**Format: single-file, internally-indexed, range-readable archives.** SQLite via a page-aligned VFS
(`wa-sqlite`/`sql.js-httpvfs`) on web, `expo-sqlite` on device; PMTiles for geometry. Web transfers
~100–200 KB to answer a tap; mobile downloads the same bytes once. **One artifact set, two access
modes, no second code path** — which is what makes the React Native port a renderer swap.

*Rejected:* sharded JSON (needs a bespoke layout **and** a client router — that is how `tier0.json`
reached 24.9 MB); Parquet/DuckDB-wasm (strong on scans, weak on point lookups and FTS); a Worker query
API (puts resolution on a server the mobile app cannot reach offline → two implementations of the
most error-prone logic).

### Artifacts

```
core.sqlite       item · section · boundary · species · base_rule · base_rule_target ·
                  section_index (dense id map) · search (FTS5)
region-N.sqlite   entry · rule · rule_section · rule_date · rule_species · rule_exemption ·
                  rule_unresolved
scopes.bin        per-rule tributary bitmaps (roaring), indexed by dense id
areas.geojson     pre-buffered park / eco-reserve polygons + MU polygons
waters.pmtiles    geometry; feature props carry section_id + dense_id
manifest.json     the only mutable object — versions, per-shard sha256, sizes
```

### Schema

Revised after a census of the real data (2026-08-28). The first draft dropped a lot and mis-keyed
more; every change below is a defect that was found, not a preference.

```sql
-- core.sqlite
item(id TEXT PK, name TEXT, kind TEXT, region TEXT)     -- region: so a tap can pick a shard
item_variant(item_id TEXT, name TEXT)                   -- 25,800 variants; search MUST cover these
item_mu(item_id TEXT, mu TEXT)                          -- 1,657 items span >1 MU
item_ref(item_id TEXT, ref_id TEXT)                     -- 19,718 ref_ids: overrides, deep links
item_section(item_id TEXT, section_id TEXT)             -- MANY-TO-MANY: 97 sections sit in 2 items
section(id TEXT PK, kind TEXT, blk TEXT, lake_wbk TEXT, -- 7,409 of 49,639 are lake:{wbk}
        down_m REAL, up_m REAL, display_name TEXT,
        location_identifier TEXT, out_of_bc INT,        -- cross-border pieces are NOT BC-regulated
        lower_bound TEXT, upper_bound TEXT)
section_area(section_id TEXT, area_id TEXT)             -- area IDS, joinable to areas.geojson
boundary(item_id TEXT, id TEXT, label TEXT, kind TEXT,  -- PK (item_id,id): `id` alone has 58 dupes
         ref TEXT, wbk TEXT, PRIMARY KEY(item_id, id))
boundary_alias(item_id TEXT, boundary_id TEXT, alias TEXT)   -- how a split landing in a lake resolves
species(code TEXT PK, name TEXT)
species_group(group_code TEXT, member_code TEXT)        -- membership is many-to-many
search(name, item_id UNINDEXED)                         -- FTS5 over item + variant names

-- regs.sqlite  (ONE database with a region column — see "Sharding" below)
entry(entry_id TEXT PK, region TEXT, name TEXT, regs_verbatim TEXT,   -- the source text, verbatim
      registry_status TEXT, registry_note TEXT, confirmed INT,
      reviewed_by TEXT, reviewed_at TEXT, revisit INT, revisit_note TEXT,
      reference_only INT, source_image TEXT)
entry_item(entry_id TEXT, item_id TEXT)                 -- 26 entries cover >1 item; §5 needs this
entry_scope(entry_id TEXT, seq INT, op TEXT, splits TEXT, item_id TEXT, area_id TEXT)
                                                        -- 10 entries; clips EVERY rule in the row
rule(entry_id TEXT, rule_id TEXT, type TEXT, details TEXT, rule_text TEXT,
     exception TEXT, display_location TEXT,             -- 2,907/3,038 — the user-facing reach label
     location_text TEXT, needs_review INT, review_reason TEXT,
     includes_tributaries INT, tributaries_only INT, scope_id INT,
     PRIMARY KEY(entry_id, rule_id))                    -- rule_id is unique WITHIN an entry only
rule_extent(entry_id TEXT, rule_id TEXT, seq INT, op TEXT, splits TEXT,
            item_id TEXT, item_ids TEXT, area_id TEXT, area_kind TEXT, feature_types TEXT)
rule_section(entry_id TEXT, rule_id TEXT, section_id TEXT)
rule_date(entry_id TEXT, rule_id TEXT, from_month INT, from_day INT,
          to_month INT, to_day INT, annual INT, year INT, raw TEXT)
rule_date_status(entry_id TEXT, rule_id TEXT, status TEXT)   -- year_round | dated | UNKNOWN
rule_species(entry_id TEXT, rule_id TEXT, code TEXT)
rule_exemption(entry_id TEXT, rule_id TEXT, exempts TEXT)
rule_unresolved(entry_id TEXT, rule_id TEXT, reason TEXT)
rule_locator(entry_id TEXT, rule_id TEXT, text TEXT)    -- 176 unresolved_locators
tributary_exclude(entry_id TEXT, rule_id TEXT, op TEXT, splits TEXT, item_id TEXT)
conflict(section_id TEXT, a_entry TEXT, a_rule TEXT, b_entry TEXT, b_rule TEXT, kind TEXT)

-- base regulations (235 rules, from v1)
base_rule(id TEXT PK, source TEXT, type TEXT, details TEXT, rule_text TEXT,
          notes TEXT, disabled INT, reach_level TEXT, scope_location TEXT)
base_rule_zone(rule_id TEXT, zone TEXT)          -- REGIONS ("1".."8","7A","7B") — not MUs
base_rule_mu(rule_id TEXT, mu TEXT, mode TEXT)   -- mode: include | exclude
base_rule_feature(rule_id TEXT, feature_type TEXT)          -- incl. `manmade` (126 of 235)
base_rule_area(rule_id TEXT, layer TEXT, type_filter TEXT, feature_id TEXT, buffer_m REAL)
base_rule_wbk(rule_id TEXT, wbk TEXT)
base_rule_date(rule_id TEXT, from_month INT, from_day INT, to_month INT, to_day INT)
base_rule_species(rule_id TEXT, code TEXT)
```

**What the census corrected, and why each matters**

| found | why it is a defect |
|---|---|
| `rule_id` is unique **within an entry only** — 49 collide corpus-wide, 8 inside one region (both Nation Lakes entries emit `nation_lakes.r1`) | every child table keyed on `rule_id` alone silently merges two different rules' sections, dates and species |
| no `entry → item` mapping, though 1,339 entries have `matched[]` and 26 cover several items | §5's "item_id → rules on this water" could not be executed at all |
| 97 sections belong to **two** items (`356567939:0` is both Alnus Creek and a park boundary) | `section.item_id` as a scalar loses one of them |
| 7,409 sections are lakes (`lake:{wbk}`, no blk, no measures); 309 items have **zero** sections | a stream-shaped `section` table cannot hold them, and those items can still carry a rule |
| extents were erased into `rule_section` | no audit or rebuild path — which contradicts this plan's own parity guard ⑫ — and the 2 `within(area)` extents had nowhere to live |
| `regs_verbatim` dropped (1,392/1,392) | breaks the chain of custody from printed synopsis → parsed rule. See red-team §"legal" |
| `entry.scope` dropped (10 entries) | §2 says scope clips every rule in the row; without it those rules **over-apply** |
| `display_location` dropped — **2,907 of 3,038 populated** | it is the reach label the UI is supposed to show |
| `needs_review` (305), `unresolved_locators` (176) dropped | they would ship as confident bindings |
| `variants` dropped (25,800) and FTS5 indexed `name` only | "Lake Weston" would not find "Weston Lake" |
| boundary `aliases` (5) and `wbk` (21,969) dropped; `boundary.id` has 58 dupes | aliases are exactly how a split landing in a lake resolves — dropping them strands those bindings |
| `base_rule` `zone_ids` are **regions**, not MUs (230/235) | the first draft's `base_rule_target.mu` required a zone→MU expansion that exists nowhere, and `7A`/`7B` are neither |
| `disabled` flag on 7 base rules dropped | they would have shipped live |
| 23 base rules have dates; `manmade` feature type (126) is outside the registry vocabulary | zone seasonal closures would render `unknown`; manmade targets had no home |

### Sharding — revised

**One `regs.sqlite` with a `region` column, not eight shards.** The distribution is flat (R1 487
rules … R8 234) and the whole database is 9.6 MB, so eight shards are a few hundred KB each. Sharding
buys no meaningful delta saving at that size while costing a router, and it forces open-decision #5
(the Fraser is regulated from four regional rows; 1,657 items span more than one MU). Revisit only if
the database grows an order of magnitude.

`rule_section` holds **direct** extents only (2,525 `whole` + 415 bounded — small); `rule_extent`
keeps the authored form beside it so the resolution can be audited and rebuilt. Tributary membership
is a bitmap, never rows.

### The tile is the index for unnamed water

Feature properties carry `section_id` and `dense_id`. That deletes the fid→reach chain that made v1's
`tier0.json` 24.9 MB — **16 MB of it was `fids[]` highlight lists** (713,721 fid strings) — plus 923 MB
of shards and most of the 1.2 GB mobile SQLite. Highlight by `section_id` via MapLibre `feature-state`.
The alternative is a 2-million-row lookup table, which would dominate any offline budget.

---

## 5. Data flow

> ⚠️ **Superseded by [`13-build-plan.md`](13-build-plan.md) §1.1 and §3.** "Tiles and data version
> together" holds for the spine and regulations, but not for live feeds: a 30-minute gauge tick must
> not re-version the bundle. Two version domains, joined on `item_id`.

```
BUILD                                    CLIENTS
registry.json ─┐                         tap → PMTiles feature
entries/*.json ├─▶ resolve_extent ──┐         { section_id, dense_id }
base_regs.json ┘   (shared, §2)     │             │
graph ─────────────────────────────┤             ├─ item_id?  → region shard: rules on this water
                                    ▼             ├─ dense_id  → scopes.bin: tributary rules
                        core + region shards      ├─ lat/lon   → areas: park/reserve closures
                        scopes.bin (bitmaps)      ├─ lat/lon   → MU polygon → base_rule overlay
                        areas.geojson             └─ minus exemptions (§2)
                        waters.pmtiles                        │
                        manifest.json  ──────────────▶  two panels: "This water" + "General rules"
```

**Mobile updates:** content-addressed shards; region sharding means an in-season change in Region 3
moves one shard. Manifest diff → download → verify sha256 → staging → atomic swap → previous
generation retained for rollback. **Push carries a version, never data**, so a dropped or duplicated
push cannot corrupt anything and a month-offline client converges identically.

**Web updates, on the fly:** same manifest, polled and on `visibilitychange`. Because reads are ranges
against immutable content-addressed URLs, an update is: fetch manifest → swap the URL the VFS reads →
drop the page cache. No reload, no re-download of anything unvisited.

**Hard rule: tiles and data version together.** A `section_id` in the tiles but not in `core` is a tap
that resolves to nothing. The manifest pins both; the client refuses a mixed pair.

---

## 6. Issues, and what to do about each

| # | Issue | Solution |
|---|---|---|
| ① | **97.6% of water has no registry item** — a tap on an unnamed creek is normal | Tile carries identity; MU/area resolved by point-in-polygon; falls through to the MU overlay. No lookup table |
| ② | **Tributary expansion is 419k sections for one Fraser rule** | Per-rule **roaring bitmaps** over dense ids, computed by the real resolver. `tributaries_only` (4 rules) is just a bitmap omitting the mainstem |
| ③ | **WSC-prefix shortcut over-applies by 3,205 km on the Kootenay** | Don't use it. Scope from the rule's resolved sections. Prefix is at most a pre-filter |
| ④ | **Area membership (`in_areas`) lives on graph nodes, not the registry** — and is useless for the unnamed creek inside a reserve, which is the case that matters | Ship pre-buffered polygons; test client-side. Covers named and unnamed alike |
| ⑤ | **A water spans several regions** (Fraser: 4) with different defaults | MU by point-in-polygon, never a per-item region field. Genuinely differing *specific* regs get a curated `mu_boundary` split |
| ⑥ | **Section ids are `{blk}:{measure}`** and move when splits change — breaking deltas, deep links and the dense-id map | `graph/cutting.py:section_id()` already has a measure-independent form: `sha1(blk|lower_boundary|upper_boundary|lake_wbk)[:16]`. Boundary ids survive a split moving. **Currently only called from tests** — adopting it as the exported id is the likely answer |
| ⑦ | **Two type vocabularies** — v2's 6 enums vs base regs' 14 free-text strings | Map to one: `Closed`/`Time Restriction`→closure; `Quota`/`Possession`/`Annual`/`Catch and Release`→harvest; `Gear`/`Bait`→gear_restriction; `Licence`→licensing; `Notice`/`Advisory`→note |
| ⑧ | **Dates: 19% coverage, free text** (`"July 15-Aug 31"`) — the closure layer cannot be built | Structured model + normaliser over the 584 strings; residue surfaced for curation; an explicit **year-round** marker so "no dates" ≠ "unknown" |
| ⑨ | **Unknown dates rendered as open** — the one failure with real consequences | Four statuses: `closed` / `restricted` / `open` / **`unknown`**, and `unknown` must render distinctly |
| ⑩ | **73% of rules name no species** — a naive species filter hides the closure that ends the trip | Filter applies to `harvest` only. Closures, gear, vessel, licensing always shown. "What applies to you, with your Rainbow Trout limits highlighted" |
| ⑪ | **A rule could vanish** between review app and bundle | Every rule lands in `rule_section`/`scopes.bin` or in `rule_unresolved` with a reason; counts asserted |
| ⑫ | **Silent under-application** — resolves to fewer sections than it should | Resolver parity test: bundle output == review app output, per confirmed entry |
| ⑬ | **Non-deterministic builds** churn every hash, turning deltas into full downloads | Byte-identical rebuild test: fixed page size, no timestamps, deterministic ordering, `VACUUM` |
| ⑭ | **Interrupted update leaves a half-written DB** | Staging + atomic swap + retained previous generation |
| ⑮ | **Device eviction** — iOS/Android evict app data; web OPFS is evictable and absent in some Safari configs | Client must survive a cold cache and re-fetch. Never assume the bundle is present |
| ⑯ | **Only 108/1,392 entries confirmed** — ship only confirmed and you ship almost nothing; ship everything and you ship unreviewed LLM output as regulation | `entry.confirmed` flag + a UI treatment. **Product decision, §8** |
| ⑰ | **Two rules disagreeing on one section** — no precedence model exists | Detect and surface conflicts at build time rather than letting the client pick |
| ⑱ | **FTS5 ranking over HTTP range** reads more pages than a point lookup | Measure. Fallback: a prefix index over ~19.7k display names, well under a megabyte |
| ⑲ | **Offline geometry.** `bc.pmtiles` is *not* BC's water — it is the **Protomaps basemap** (z0–15, layers `buildings/pois/landuse/…`). The water artifact is `freshwater_atlas.pmtiles`. The earlier framing of this issue was simply wrong | Measured from `graph.gpkg`: regulable water is **180.5 MB of 2,180.7 MB** of WKB — **8.3%**. Through tippecanoe at v1's settings that is **~15–30 MB**, i.e. shippable offline. **The real blocker is the basemap**, and the fix is a stripped extract (earth/water/roads/places, z4–12) — a separate problem from the waters layer |
| ⑳ | **A killed build worker exits 0** — the full build died three times reporting success | Fix the exit path before this pipeline depends on it in CI |

### Found by review, 2026-08-28

| # | Issue | Solution |
|---|---|---|
| ㉑ | **`exempts_from` has no join key.** The 7 codes are free strings; `base_regulations.json` has no exemption-code column, so §2's `defaults − exemptions` subtraction — called "the only correct way to answer is it open" — is **unimplementable today** | Hand-add `exempt_code` to the ~9 default families. 235 rows, an afternoon |
| ㉒ | **Base regs are keyed by REGION, not MU** — 225 of 235 use `zone_ids` (`1`…`8`, `7A`, `7B`); only 10 use `mu_ids`. §2's "MU is the lookup key" was wrong, and `7A`/`7B` are neither region nor MU | Resolve MU by polygon, then MU→zone. Schema: `base_rule_zone` + `base_rule_mu(mode)` |
| ㉓ | **In-season notices are absent from the artifact set entirely** — the scraped mid-cycle closures the printed synopsis lacks. The highest-value volatile data, and nothing carries it | Design it in now, not later. It is the thing an update mechanism exists for |
| ㉔ | **`within(area)` is broken end to end.** Both `area_id`s dangle — one points at a *split id*, the other matches nothing. Neither names any of the 4 registry `area:` items | Fix the two references; make an unresolvable `area_id` a `rule_unresolved` row, never a silent empty |
| ㉕ | **Zero-section items.** 263 lakes and all 46 wetlands have no sections; **16 zero-section lakes are matched by 12 entries / 14 rules** and cannot resolve at all | They need a `rule_unresolved` reason. Wetlands carry no entries — stop listing `wetland` as a live kind |
| ㉖ | **The section count double-counts.** 49,639 refs, **49,542 distinct** — the 4 `area:` items own 4,731 sections, 97 shared with a water item | `item_section` many-to-many (done in §4); quote 49,542 as the section count |
| ㉗ | **Fail-open entry scope.** `_scope_sections` returned `None` for both "no scope" and "scope is broken", and the caller read `None` as "do not clip" — a regional row would silently widen to the whole river | **Fixed 2026-08-28**: returns `(sections, failed)`, and `entry_reaches` surfaces `scope_unresolved`. Test added |
| ㉘ | **⑫ parity is unsatisfiable as written** — `entry_reaches` returns direct extents only, so a bundle with tributary bitmaps fails parity by construction for all 554 rules | Scope the parity assertion to direct extents; test tributary expansion separately against a flow walk |
| ㉙ | **`sections_override` has no home.** Zero uses today, but it is a legal substitute for `extents` (the validator accepts a rule with neither only if it is set) and is ignored by `entry_reaches` | Add the column and honour it, or first use silently drops the rule |
| ㉚ | **Two bugs in `r2-worker/src/index.ts`**: suffix ranges (`bytes=-N`) fall through to a **full-object GET returned as 206** with a fabricated `Content-Range`; and Cache API `put()` throws on a 206, swallowed by `.catch(()=>{})`, so no range read is ever edge-cached | Both must be fixed before a range-reading client exists |
| ㉛ | **The mobile DB ships as an app-store asset** (`mobile/app.json` bundles `assets/db/**`) — a data fix currently requires an app release | Download-on-first-run, before anything else in the mobile track |
| ㉜ | **354 of 690 closures are `No Fishing` with no dates — year-round, not unknown.** ⑨ miscounted. And a **fifth** status is missing: `default-only`, the 97.6% with no assessed rule | Reserve `unknown` for normaliser failures and unconfirmed entries. Never paint `default-only` the same green as an assessed open reach |
| ㉝ | **Base regs carry zero species tagging** (0 of 235); the 86 zone quota rules hold species in free text | Tag the 235 rows, or the species feature cannot work in the panel that serves the majority of taps |
| ㉞ | **`scopes.bin` is one global monolith indexed by dense id** — any registry change renumbers ids, so the whole file changes every build and deltas are worthless | Dense ids **append-only from a stable sorted key, never reused**; shard by region |
| ㉟ | **Source-page images and verbatim text are missing from the artifact set** — v1 showed the synopsis page beside the parsed rule | A trust regression. `regs_verbatim` is now in the schema; the page image needs a home too |
| ㊱ | **"Confirmed" does not mean clean.** Of the 267 rules inside the 108 locked entries, **76 still carry `needs_review`, 63 carry `unresolved_locators`, 5 have no extents**. Confirming locks an entry; it does not clear flags — so "ship only confirmed" still ships 63 rules whose location never resolved | A separate `publishable` predicate: locked **and** no unresolved locators **and** every rule resolved to ≥1 section. `/confirm` should surface residual flags |
| ㊲ | **The ⑫ parity test becomes tautological.** Once the builder and the review app share one resolver, "bundle == review app" is the same code answering twice — it tests serialization | The real oracle is the invariant `API.md` already states: `upstream_of ∪ downstream_of ∪ unclassified == the item, disjointly`. Run over all 415 bounded extents, that is what would have caught the braid edge-direction and split-alias defects this project already hit |
| ㊳ | **⑪'s guard is a count, and under-application produces zero rows rather than missing rows.** A `between` whose half-lines do not intersect returns `{"sections": []}` — which satisfies "every rule landed somewhere" while selecting nothing | Strengthen to: **no rule may end with zero sections without an explicit `rule_unresolved` row** |
| ㊴ | **`unclassified` and `ambiguous_cut` have no home in the schema.** `reuse.py` surfaces straddling pieces, and for a split landing at two measures it uses the **lower** and reports the alternative | A straddling piece dropped at a closure boundary, or a closure shortened to the lower cut, is exactly a wrong "open". Both need columns |
| ㊵ | **Data lifecycle is missing entirely — the largest structural hole.** All 231 sourced base regs cite the **2025–2027 synopsis, which expires 31 Mar 2027**. Nothing carries a synopsis edition or `valid_until`; nothing makes a client refuse stale data rather than keep rendering last cycle's openings | Edition + `valid_until` in the manifest; the client degrades loudly. Model in-season amendments (㉓) as first-class, not as a reissue |
| ㊶ | **52 `no_registry` entries carry 95 rules, 13 of them closures** (Arrow Lakes, Strathcona Park Waters, CVWMA, Kikomun Creek Park). No item → no section → no row → they vanish | A text-only, name-searchable path so an unmatched closure is still findable |
| ㊷ | **`pytest.ini` sets `addopts = -m "not slow"` — 153 tests are deselected by default.** Determinism (⑬) and any full-build guard would be written as `slow` and silently never run | Make the CI job run the slow set explicitly |
| ㊸ | **`details` is paraphrase and `rule_text` is verbatim, but nothing marks which is which.** (`rule_text` checks out: 0 of 3,038 are not a substring of `regs_verbatim` — pin that as a test.) `exception` is populated on only **17 of 3,038** rules, implausibly low for this synopsis | Mark provenance per field. Investigate whether exceptions are being folded into `details` or dropped in parsing |

---

## 7. Sequencing

> ⚠️ **Superseded by [`13-build-plan.md`](13-build-plan.md) §4–§5**, which carries acceptance criteria
> per step. Open decision #1 (section id) is **closed there by measurement**.

Revised after review — dates moved early, because building the artifacts first means validating them
against a bundle that cannot express the app's central question.

1. **Lift `resolve_extent` into `pipeline/`** — refactor for the *direct* extents. Smaller than it
   looked: the tributary walk does not exist yet and is separate, unwritten work.
2. **Decide the exported section id** (⑥) — a migration of 49,542 ids, so it goes before the schema.
3. **Structured dates** (⑧, ㉜) — 584 strings over only **167 unique values**, roughly a day, and
   354 closures are undated "No Fishing" = year-round. Without this the closure layer cannot exist and
   nothing downstream can be validated against the real question.
4. `regs.sqlite` + `core.sqlite`, with determinism (⑬) and the **invariant** oracle (㊲), not the
   tautological parity one.
5. **Base regulations** — port the 235, add `exempt_code` (㉑) and species tags (㉝) so the General
   panel works at all.
6. The tributary walk, then bitmaps + a stable dense-id map (㉞), validated against a flow walk.
7. Areas + MU polygons, pre-buffered; the client-side containment path.
8. Manifest, verify/swap/rollback, against a fabricated second version. Fix the Worker bugs (㉚).
9. Tiles: regulable waters (~15–30 MB) and a stripped basemap (⑲).

1–3 can start now; none waits on curation.

## 8. Open decisions

1. **Exported section id** — adopt the hash form, or accept churn?
2. **Does the bundle ship unconfirmed entries**, and how does the UI mark them?
3. **Precedence** when two rules cover one section and disagree.
4. **Offline geometry scope** — needs ⑲ measured first.
5. **Region shard by entry region or by geography?** The Fraser is regulated from four regional rows.
6. **`Catch and Release` → harvest?** It is a zero limit, but may read better beside gear.
