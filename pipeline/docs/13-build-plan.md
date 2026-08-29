# 13 — Delivery plan: from artifacts to two apps

Supersedes [`10-plan.md`](10-plan.md) §4 (artifacts), §5 (data flow) and §7 (sequencing). Doc 10 stays
the source for **resolution semantics**, the **43 issues**, and the **content census** — all of which
hold. What changes here is the delivery architecture, because doc 10 makes three commitments this doc
argues against:

1. *"One artifact set, two access modes, no second code path."* — Web and mobile optimise opposite
   things. What must not fork is **resolution semantics**, not the wire format.
2. *"Hard rule: tiles and data version together."* — Correct for the spine, wrong for a gauge reading
   that changes every 30 minutes.
3. Its artifact list has **no home for gauges, stocking, or bathymetry** — three datasets v1 shipped
   and the next app will show more of, not less.

Circled numbers (⑥, ㊲, …) are doc 10 §6 issues.

**Design stance: no v1 inheritance.** v1 is read here as *evidence about constraints*, never as a
template. Where it is cited it is because it measured something or failed instructively.

---

## 0. Measurements taken before planning

Re-runnable via `pipeline/tools/id_churn.py` (step 2). Measured across the only two real builds that
exist: `output/v2/full` → `output/v2/full_new` (the added-streams work, 49,542 → 64,128 sections).

### 0.1 — Identifier durability: a 3-tier hierarchy

| key | before | after | survived | lost | new |
|---|---|---|---|---|---|
| **`item_id`** | 19,722 | 19,698 | 19,698 (**99.88%**) | 24 | 0 |
| **`(item_id, boundary_id)`** | 22,760 | 24,237 | 22,687 (**99.68%**) | 73 | 1,550 |
| ↳ *curated split cuts only* | 271 | 270 | 270 (**99.63%**) | 1 | 0 |
| **`section_id`** `{blk}:{measure}` | 49,542 | 64,128 | 46,565 (**94.0%**) | 2,977 | 17,563 |

`item_id` and `boundary_id` are **externally derived or hand-authored** — `gnis:8634`,
`chilliwack_vedder_rivers__tamihi_rapids_bridge`. `section_id` is **build derived**, and churns 6% on a
routine build. §2 turns this into an addressing rule.

### 0.2 — But a stable id is not a stable *snapshot*

Of the 19,698 items that survived, only **16,748 (85.0%) have an identical section list**. **2,950
items changed content** while keeping their id.

This is the correct behaviour — the water did not change, our model of it improved — but it has a hard
consequence: **`item_id` is a durable *address*, not a durable *answer*.** No client may cache a
resolved section list across bundle versions, and every durable binding must be **re-resolved** against
the bundle currently held. That re-resolution is what the reach builder (§6) exists to do, and it is
why the reach builder has to run on every build rather than only when curation changes.

**24 items disappeared entirely**, so a durable binding can also dangle. Two consequences:
- The content store needs **`item_tombstone(old_item_id, reason, replaced_by)`**, or a saved place
  silently resolves to nothing.
- Those 24 are suspicious *as data*: every one is `kind=stream` but named "…Lake" — Pitt Lake, Sooke
  Lake, Green Lake, Tahltan Lake — with 1–4 sections each. Worth a curation look; likely
  lake-named stream fragments absorbed by the added-streams work.

### 0.3 — ⑥ is not worth doing. The hash section id buys **one id out of 49,542**

| id form | survived | lost | new |
|---|---|---|---|
| `{blk}:{measure}` | 46,565 (**94.0%**) | 2,977 | 17,563 |
| `sha1(blk\|lower\|upper\|lake_wbk)[:16]` | 46,566 (**94.0%**) | 2,976 | 17,562 |

Identical, and structurally so: the hash is over the *bound pair*, and adding or moving a split changes
the bound pair for exactly the sections whose lower route measure changes. Same churn set by
construction. The hash only wins on a **re-anchored** split — one section here.

**Close open decision #1: keep `{blk}:{measure}`.** The stability problem ⑥ was trying to solve is
solved by §2 — *not binding external things to section ids at all* — not by changing the id form.

*Caveat:* this build pair is **additive**. A re-anchoring-only build is the case that favours the hash
and is unmeasured. Reopen gate in step 2.

*Also found:* `cutting.section_id()`'s hash branch takes `lake_wbk`, but `StreamNode` carries `wbk`, and
lake nodes have `blk == ""` with no bounds. Fed what a node actually carries, **all 7,409 lake sections
hash to one value** (`sha1("|||")`). It survives because the function is **called only from tests** —
production ids are minted as `f"{blk}:{int(down_m)}"` in the node constructor. Step 2 disposes of it.

### 0.4 — Bathymetry is two datasets with opposite delivery needs

| | count | size | offline? |
|---|---|---|---|
| Depth **contour polygons** (`bathymetry_polygons.gpkg`) | 2,744 | 17.3 MB | **Yes** — a few MB tiled |
| Scanned **map sheets** (`data/bathymetry_pdfs`) | 2,685 | **738 MB** | **No** — on demand only |

Treating "bathymetry" as one feature is the mistake. Contours are a **map layer**; sheets are **per-lake
documents**. Likewise 450 gauge stations of live readings is a **feed**, not a layer.

### 0.5 — Prior art: what happens with no shared core

The new app is built from the ground up and inherits no code from `webapp/` or `mobile/`. Those trees
are read here only as **evidence about a failure mode**, and the evidence is stark: seven filenames
exist in both, and the four that are pure logic — the four that should have been one implementation —
have **completely diverged**:

| file | web | mobile | lines differing |
|---|---|---|---|
| `useDawnDusk.ts` | 77 | 83 | **every line** |
| `featureUtils.ts` | 407 | 171 | 494 |
| `regulationsService.ts` | 144 | 89 | 134 |
| `waterbodyDataService.ts` | 1,157 | 116 | 1,203 |

Doc 10 worried about "two implementations of the most error-prone logic" in the *pipeline*. It happened
in the *clients* instead — not through carelessness, but because two apps were created independently
and neither was ever the shared source. §4.4 is the structural fix: **the shared core exists before
either app does.**

---

## 1. The architectural correction

### 1.1 — Three data classes, not one

| class | contents | changes | lifetime |
|---|---|---|---|
| **Spine** | sections, items, boundaries, geometry tiles, bathy contours | per full build (manual, ~18 min) | months |
| **Regulatory** | entries, rules, base regs, dates, species | per synopsis edition (**expires 31 Mar 2027**) + in-season amendments | one edition |
| **Live** | gauge readings (30 min), stocking (weekly), in-season notices (6 h) | continuously | hours–days |

Doc 10's *"tiles and data version together; the client refuses a mixed pair"* is **right for
spine + regulatory** and **wrong for live**: under one manifest a 30-minute gauge tick re-versions the
bundle and invalidates every client's cache.

**Two version domains.**
- **Bundle version** — spine + regulatory, pinned together, content-addressed, mixed pair refused.
- **Feed version** — one per live channel, versioned independently, joined by **`item_id`** (§2), never
  by `section_id` and never by a build-time match table.

### 1.2 — One content model, two packagers, one conformance suite

The fear behind "no second code path" is legitimate — two implementations of resolution is how answers
diverge — but it applies to *logic*, not *packaging*.

```
                    ┌───────────────────────────────┐
  registry ────────▶│  pipeline/resolve   (step 1)  │  ONE resolver — never forks
  entries  ────────▶│  pipeline/reach     (§6)      │
  base regs ───────▶└──────────────┬────────────────┘
                                   ▼
                    ┌──────────────────────────────┐
                    │   CONTENT STORE              │  app-agnostic, layer-agnostic
                    └──────┬───────────────┬───────┘
              ┌────────────▼───┐   ┌───────▼────────────┐
              │ WEB PACKAGER   │   │ MOBILE PACKAGER    │  pure functions of the store
              │ nothing        │   │ everything         │
              │ resident       │   │ resident           │
              └────────┬───────┘   └───────┬────────────┘
                       └───────┬───────────┘
                         CONFORMANCE SUITE
                 both must answer a fixed query set identically
```

| | Mobile *(design centre)* | Web *(relaxation)* |
|---|---|---|
| Optimise | **total bytes + zero connectivity** | **bytes to first answer** |
| Resident corpus | all of it | none — random access into an immutable remote file |
| Live feeds | cache, render with an **age stamp** | fetch on tap |
| Update unit | delta a content-addressed shard | swap an immutable URL |

**Mobile is the design centre, and that ordering is load-bearing.** The offline case carries the only
*hard* constraints — a fixed storage budget, no round trips, no server to fall back on. A design that
satisfies it relaxes cleanly for web (fetch a subset on demand). The reverse does not hold: a web-first
design assumes a network at exactly the moments that are most convenient, and those assumptions are the
ones that cannot be removed later. v1 is the evidence — it was web-first, and its mobile port ended up
shipping a **1.2 GB SQLite as an app-store asset** (㉛), so a data fix required an app release.

**This partially rehabilitates doc 10's instinct, for a better reason.** A mobile-first bundle must be
complete, local and self-sufficient — and a complete local SQLite **can** be range-read over HTTP by
web. The reverse (assembling an offline bundle out of a web-optimised sharded layout) is strictly
worse. So the likely answer to open decision #5 is **one base format, two packagings** — differing in
index layout, shard boundaries, and what ships versus streams — not two formats. The difference from
doc 10 is that this is *derived from a constraint and still gated on step 6's measurement*, rather than
asserted. They must emit the same *answers* regardless; that is what the conformance suite enforces.

### 1.3 — Model entities, never screens

The app will show more info and different layers, so the store holds **entities and relations** and the
client composes screens. It must not hold precomputed per-screen payloads.

v1 is decisive evidence here: `tier0.json` reached **24.9 MB, of which 16 MB was `fids[]` highlight
lists** — a precomputed answer to one screen's question, which had to grow every time a layer was added.

**Test of the principle:** adding a layer the app does not have yet — water temperature, say — must
require a new entity table and a new feed, and **no edit to any existing table**.

---

## 2. Durable addressing — what binds to what

The rule that falls out of §0.1 and §0.2.

```
DURABLE — survives a rebuild                     PER-BUILD — recomputed every time
  item_id                        99.88%  ┐
  (item_id, boundary_id)         99.68%  ├──▶  reach builder (§6)  ──▶  section_id      94.0%
    curated cuts only            99.63%  │                             dense_id      packager-local
  Extent{op, splits[], item_id}          ┘
```

**The durable reach address is the `Extent` the curator already authors.** `{op: "between", splits:
[tamihi…, vedder_crossing…], item_id: "gnis:8634"}` is a name for a stretch of river that survives a
rebuild; the section set it resolves to does not. Nothing new needs inventing — the curated form *is*
the stable form, and the reach builder is the function from one to the other.

| thing | binds to | why |
|---|---|---|
| gauge station | `item_id` | a gauge is *on a river* |
| stocking release | `item_id` (via FIDQ `waterbody_id`) | stocking is *into a lake* |
| bathy sheet + contour | `item_id` | a sheet is *of a lake* |
| in-season notice | `item_id` + optional `Extent` | authored like a rule |
| user's saved place | durable reach address | must survive rebuilds |
| deep link / share URL | `item_id` (+ optional boundary pair) | must survive rebuilds |
| rule → water | `Extent` | already the durable form |
| tile feature → data row | `section_id` | same bundle, pinned by one manifest |
| scope bitmap index | `dense_id` | packager-internal |

> **Hard rule: `section_id` and `dense_id` never leave the bundle.** They may appear in tiles (pinned to
> the same manifest) and in packager-internal tables. They must **never** appear in a URL, a saved
> preference, a live feed, or a public API contract.

**Why this matters more than it looks.** v1 bound its `gauge_matches.json` and `stocking_matches.json`
to `reach_id`, a build-derived key. Its own docs list the consequence as a limitation: *"a new
gauge/waterbody only links to a reach after the next full build"* — and that full build needs the ~10 GB
FWA GeoPackage, so it cannot run in CI, so it is manual, so it is a bus-factor risk. That is not a bug
someone introduced. It is the **unavoidable** consequence of joining a 30-minute feed to a key that only
a full build can mint. Bind at `item_id` and the entire class of problem disappears: a new gauge appears
the moment the feed refreshes, with no build, no GeoPackage, and no human.

**And the converse discipline (from §0.2):** because `item_id` survives while 15% of items change
content, a durable binding must be *re-resolved*, never *cached*. A saved place stores
`{item_id, op, splits[]}` and asks the current bundle what that means today.

---

### 2.1 — `default_only` is two orthogonal axes fused into one enum

Raised by a UI review and **verified against the data**, 2026-08-29.

Doc 10 ㉜ added `default_only` as a fifth status for "the 97.6% with no assessed rule". Four
members of that enum describe an **outcome** (open / restricted / closed / unknown); the
fifth describes **provenance** (no water-specific record). They are independent, and fusing
them silently loses the outcome.

The proof is a real base rule:

```
zones=['3']  feature_types=['stream']  dates='Jan 1 – Jun 30'
"No fishing in any stream in Region 3 from Jan 1 to June 30 (see tables for exceptions)."
```

An unnamed creek in Region 3 on 20 May is **closed**. Under the current enum it renders
`default_only` — grey — because no rule names it. Region 1 has the same shape (`"No fishing
in any stream in Management Units 1-1 to 1-6 from July 15 to August 31"`). Grey drawn beside
red teaches a user that grey is the safe colour, which is precisely backwards here.

**The model needs two fields, not five values:**

| | |
|---|---|
| `outcome` | open · restricted · closed · unknown |
| `provenance` | water_specific · default_only |

Rendering: **hue carries outcome, texture carries provenance.** A default-only closure is
closed-red at reduced weight, not grey. `color.status.default_only` becomes a provenance
chip rather than a line colour.

This changes `rule_date_status`, the map style's `reg_status` enum (whose every-member rule
already forces the five to be coloured), and every "is it open" surface. Cheap now, and
expensive once clients render against it.

## 3. What the content store holds

Doc 10 §4's schema is sound and its census corrections are real defects — **keep it**, with these
changes.

**Additions**
```sql
-- live-feed anchors: item_id (§2), NOT section_id, NOT a build-time match table
gauge(station_id TEXT PK, name TEXT, item_id TEXT, lat REAL, lon REAL, kind TEXT)
stocking_water(waterbody_id TEXT PK, item_id TEXT, name TEXT)
bathy_sheet(sheet_id TEXT PK, item_id TEXT, title TEXT, sheet_no INT,
            url TEXT, bytes INT)                    -- the 738 MB stays REMOTE
-- bathy CONTOURS are a tile layer, not a table

-- durability (§0.2, §2)
item_tombstone(old_item_id TEXT PK, reason TEXT, replaced_by TEXT)

-- lifecycle / provenance (㊵, ㉟)
edition(id TEXT PK, label TEXT, valid_from TEXT, valid_until TEXT)   -- 2025-2027 -> 2027-03-31
source_page(entry_id TEXT, image_ref TEXT)                           -- the trust regression ㉟
```

**Removals from doc 10 §4** — `section_index` (dense id map) and `scopes.bin`'s global indexing move
**into the packagers**. A dense id is a *packing detail*, and ㉞ ("any registry change renumbers ids,
the whole file changes, deltas are worthless") is a symptom of having leaked it into the content model.
Each packager mints its own, append-only from a stable sorted key.

**Deferred to packagers, not content:** shard boundaries, page size, FTS ranking, bitmap encoding, tile
zoom ranges. All of doc 10 §4's "Sharding — revised" is a *web packager* decision.

---

## 4. The app: greenfield, mobile-first

**The new app inherits no code.** `webapp/` and `mobile/` are prior art (§0.5) — useful as evidence
about what goes wrong, not as a starting point. Everything below assumes a clean workspace, and the
design centre is **the device, offline, with no network** (§1.2).

### 4.1 — Put the platform boundary at I/O and rendering, never at logic

```
packages/
  core/      pure TS — domain types, precedence, date evaluation, species logic,
             unit + label formatting, "what applies here".
             ZERO platform imports. ZERO React.                         ← 100% shared
  data/      ONE interface (RegsSource), TWO implementations:
             data-mobile (local sqlite) · data-web (range reads over HTTP)
  ui/        headless React hooks — useReach, useRegsForPoint, useGauge.
             React, but NEVER react-dom and NEVER react-native.         ← 100% shared
  map-style/ generated MapLibre style JSON + layer/filter/highlight expressions  ← 100% shared
apps/
  mobile/    the primary target — RN + maplibre-react-native
  web/       the relaxation — maplibre-gl
```

**Why `ui/` is shareable and components are not:** React hooks are renderer-agnostic — the same
`useReach()` runs under `react-dom` and React Native, because it returns *data*, not elements.
Components are not, because `<div>` and `<View>` are different renderers. The split is therefore
**hooks shared, components decided by §4.2** — which is the opposite of where most teams draw the line.

| layer | shared? | why |
|---|---|---|
| domain logic, precedence, dates, formatting | **100%** | pure TS |
| data access **interface** | **100%** | one `RegsSource` contract |
| data access **implementation** | **0%** | local sqlite vs range-read HTTP |
| state + query hooks | **100%** | hooks are renderer-agnostic |
| MapLibre style / layers / filters | **100%** | both use MapLibre; the style spec is JSON |
| components | **§4.2** | the one genuinely open question |
| charts | **0%** | canvas/DOM vs RN svg |

### 4.2 — The cross-platform UI strategy is an open decision, and the map decides it

Two viable strategies for the **view** layer. Everything in §4.1 marked 100% is shared either way; this
is only about how much of the *UI* is written once.

| | **A — shared logic, forked views** | **B — shared views via react-native-web** |
|---|---|---|
| `core` / `data` / `ui` / `map-style` | shared | shared |
| components | written twice, each idiomatic | written once, RN primitives |
| the map | **forked** — different APIs | **still forked** — RNW does not unify them |
| web idioms (SEO/prerender, DOM a11y, CSS) | native | constrained by RN's model |

**The map forces a fork under either strategy.** `@maplibre/maplibre-react-native` and `maplibre-gl`
are different APIs with different lifecycles, and on this product the map *is* the app. So B's promise
— write the view once — does not extend to the part with the most view code.

**Recommendation: A**, on two grounds: the map forks regardless, and a public regulations site very
likely wants SEO and prerendering, which is where RN's component model on web costs the most. But this
is **decision #7**, it must be made deliberately at kickoff, and it is expensive to reverse. Decide it
on: how much of the UI is map-adjacent versus conventional lists and detail panels; whether web needs
SEO/prerender; and team size.

*(Under the previous plan I rejected B outright. That argument rested on protecting an existing
webapp's mature maplibre-gl integration. Greenfield removes that premise, so B deserves a real
evaluation rather than a reflex — the map argument is what survives.)*

### 4.3 — Four rules that keep it from forking

1. **Pin one React version across the workspace on day one.** The React 19-vs-18.3.1 skew between the
   existing apps was an artifact of two independently created projects. Greenfield removes it — but
   only if it is pinned deliberately, and only if `apps/mobile` and `apps/web` are never allowed to
   drift apart on it.
2. **`core/` has zero React and zero platform imports, enforced in CI** — a dependency-cruiser or lint
   boundary rule, not a convention. This is the single rule that would have prevented §0.5.
3. **Generate the TypeScript types from the Python content model.** Hand-writing row shapes twice is
   the exact mechanism by which `waterbodyDataService.ts` became 1,157 lines on web and 116 on mobile.
   Schema → emitted `.d.ts`, checked in, diffed in CI.
4. **The MapLibre style is a build artifact, not source.** Layer definitions, colours, filters and the
   `feature-state` highlight expressions are generated into one JSON both apps load. This is the
   largest lever on "we will drastically change how layers and info are shown": a layer change becomes
   one artifact regenerated, not two component trees edited.

### 4.4 — Build order: the shared core exists before either app

1. **`core` + the `RegsSource` interface + a fixture-backed implementation** — before either app is
   created. The conformance suite (§4.5) is green against fixtures at this point.
2. **`apps/mobile`** against a real bundle, offline on first run.
3. **`apps/web`** against the same content store via range reads.
4. Conformance suite runs **both** `RegsSource` implementations on every commit.

The ordering is the whole point. §0.5 happened because two apps were created independently and neither
was ever the shared source — so whichever moved first became a de facto reference the other chased.
Building `core` first means there is never such a moment.

### 4.5 — The conformance suite is what makes it real

`core/` plus both `data/` implementations are exercised by **one** test suite: a fixed set of queries —
tap at a point, search a name, ask "what applies here on this date", ask "is this open" with a live
override present — run against both `RegsSource` implementations, asserting identical answers. This is
§1.2's conformance suite seen from the client side, and it is the thing that catches drift on the day
it starts rather than 1,041 lines later.

## 5. Live channels (㉓ — the largest hole in doc 10)

| feed | cadence | size | offline behaviour |
|---|---|---|---|
| **gauges** | 30 min | 450 stations | last-known value **+ age stamp**; never rendered as live |
| **stocking** | weekly | ~thousands of waterbodies | bundle-able; staleness tolerable |
| **in-season** | 6 h | small | **must override a bundled rule** |

**In-season and rule precedence are the same problem.** An in-season notice is an authoritative override
of a rule the bundle already ships, so doc 10's open decision #3 (precedence) and ㉓ (in-season absent)
are one design: an **overlay with an explicit precedence order**, applied identically by both clients
from `core/`. Decide it once and in-season becomes a normal instance rather than a special case. It is
the only channel that can *correct a wrong answer between builds*, which makes it the highest-value
thing in this document.

**Offline degradation is a correctness requirement.** ⑨/㉜ established that an unknown date rendered as
open is the one failure with real consequences. A stale gauge and an unfetched in-season override are
the same failure class: both clients must render **age**, and must distinguish "no closure" from "could
not check for a closure".

---

## 6. The reach builder — the next thing to make

The stage that turns **curated intent into per-build bindings**, and the diff that tells a curator what
a rebuild did to their work. It is next because §0.2 makes it unavoidable (2,950 items changed content
in one routine build) and because everything downstream — the content store, the packagers, the bitmaps
— consumes its output.

### 6.0 — Where it stands today (measured 2026-08-28)

Today's resolver run over all 1,392 entries — the baseline the builder must reproduce:

| rule outcome | count |
|---|---|
| **bound** (resolved to ≥1 section) | **2,910** (95.8%) |
| `no_extents` (rule authored with no extent at all) | 107 |
| `all_extents_unresolved` | 18 |
| `resolved_but_EMPTY` | 3 |
| **total** | **3,038** |

**It runs in 42 seconds**, 5 s of which is the graph load. Entry scopes: 10/10 resolve, 0 unresolved.
0 errors.

Two consequences: the step-1 "≤ 5 min" budget is met with 7× headroom, and **parallelism is not
needed** — do not build it. The remaining decisions are all small buckets:

| bucket the builder must decide about | rules |
|---|---|
| straddling pieces (`unclassified`) | **40** (46 pieces total) |
| ambiguous cuts (one landmark, two spots) | **63** |
| more than one extent (union semantics) | **9** |
| partially resolved (some extents null) | **0** |
| **tributaries on a BOUNDED extent** — the hard walk case | **176** (79 `upstream_of`, 53 `downstream_of`, 44 `between`) |

**What the 107 `no_extents` rules actually are** — two different things, and only one is a defect:

| | rules | verdict |
|---|---|---|
| entry is `no_registry` (no item to author an extent against) | **95** | expected — this is ㊶, needs the text-only path |
| entry is **`matched`** but no extent was authored | **12** | **defect** |

The 12 include `Closed all year` (Moyie River), `No fishing` (Fraser region 2), `Speed restriction
(10 km/h)` (Columbia). Read plainly they mean *the whole matched item* — but with no extent they bind
to **zero sections**, so as things stand those closures would ship applying nowhere. Two candidate
fixes: default `no extents + matched item ⇒ {op: "whole"}`, or make it a hard validation error at
authoring time. **Defaulting silently is the more dangerous of the two** — it would also swallow a rule
whose extent was genuinely lost in parsing.

**What the 18 failures + 3 empties actually are** — the resolver itself is nearly clean:

| cause | rules | whose bug |
|---|---|---|
| item is in the registry but has **zero sections** (lake with no geometry) | **14** | registry/graph — ㉕ |
| `within(area)` — both `area_id`s dangle | **2** | data — ㉔ |
| **`between` resolves to nothing** (Fraser steelhead, Skeena, Mitchell, Peace) | **4** | **the resolver** |
| `upstream_of` on a 1-section item returns empty (South Thompson `note`) | **1** | **the resolver** |

**Diagnosed individually (2026-08-28) — the resolver's own defect surface is ONE bug, not five:**

| case | what actually happens | owner |
|---|---|---|
| **Peace** `between(peace_canyon_dam, hwy_29_bridge)` | Resolves correctly to node `359572348:1683398`, then the **entry scope** (*"from Hwy 29 Bridge to the Site C dam"*) clips it away — the rule describes a reach *above* the row it sits in. The resolver is right; the curation is inconsistent. | **curation** |
| **Fraser** steelhead | `fraser_river__spuzzum_creek_into_fraser_river` **is not a boundary on `gnis:39325`** — the cut does not exist. | **curation** |
| **Skeena** | `skeena_river__exchamsiks_river_into_skeena_river` likewise absent (the other cut, a `confluence`, resolves fine). | **curation** |
| **Mitchell** | **Real bug.** The lake boundary `mitchell_river__mitchell_lake` (`lake:329480767`) carries `split:mitchell_river__within_100_m_upstream` as an **alias**, so both split ids resolve to the same boundary. That lake bounds the blk at **two** measures (50 980.4 inlet, 67 438.3 outlet); `_cut_at` returns the lower for both, so `between(A,B)` collapses to `between(m,m)` = empty. | **code** |
| **South Thompson** `upstream_of(little_shuswap_lake)` | Correct: the item is one section ending *at* the lake, so nothing is upstream of the cut within it. The rule is a `note` — *"See Shuswap Lake regulations…"* — that legitimately selects nothing. Should be `reference_only`. | **curation** |

The Mitchell bug is the alias mechanism losing *which end* a split meant. The alias records that two
ids name the same boundary, but not that the boundary occurs at two measures. `_cut_at` already reports
the alternative in `ambiguous_cut` — the information exists, the resolver just cannot tell which alias
meant which end. **Fix: aliases must carry the measure (or the end) they resolve to.**

### The 12 matched-with-no-extent rules — do NOT default them to `whole`

An earlier draft of this doc suggested defaulting `no extents + matched item ⇒ {op: "whole"}`. **That
is wrong and would be dangerous.** All 12 carry `needs_review=True`, and **11 of 12 carry
`unresolved_locators`** with an explicit parser note:

> *"no boundary or area for the wetlands; reach left unbound rather than applied to the whole river"*
> *"neither end has a boundary in the menu; reach left unbound rather than guessed"*

They are **correctly parsed rules whose location has no boundary to bind to** — a 500 m bait ban at
Causeway Road, a 500 m radius at the Davis FSR Bridge, a sign line near the Nation River Bridge, the
CPR bridge on Mara Lake, the Columbia wetlands. Defaulting them to `whole` would apply a 500 m closure
to an entire arm of a lake. The parser is being conservative and correct.

**They need curated splits authored (human work).** Until then they ship as `rule_unresolved`. The one
exception is `qualicum_river.r7`, a `note` about a wheelchair-accessible fishing platform, which has no
locator at all.

### The 309 zero-section items

263 lakes + **all 46 wetlands**. Wetlands are overlays by design (`StreamNode.member_wbks`), never
nodes — doc 10 ㉕ is right that `wetland` should stop being listed as a live kind. The 263 lakes are
**isolated waters with no stream connection** (Hall Road Pond, Frazer Lake, Kinglet Lake): FWA has the
polygon, so the registry mints a named, matchable item, but the graph makes no node, so a rule on them
cannot resolve. **Fix is in the graph build — mint a node for an isolated lake** — not in the resolver.

### `within(area)`: both `area_id`s are simply wrong strings

| rule | authored `area_id` | registry has |
|---|---|---|
| `pitt_river.r1` | `pitt_river__garibaldi_pitt` | `area:within_garibaldi_park` |
| `wood_river_795.r1` | `within_hamber_provincial_park` | `area:hamber_prov_park_boundary` |

Neither is an `area:` id. Two edits fix the data — but note `resolve_extent` returns `None` for
`op == "within"` **unconditionally**, without ever reading `area_id`, so correcting the strings alone
changes nothing. Both halves are needed: fix the two ids (**curation**), and resolve `within` against
the area catalog `area_catalog.gpkg` (**code**).

### 6.1 — Contract

```
python -m pipeline.reach.build --build output/v2/full --out output/reaches/full
python -m pipeline.reach.build --build output/v2/full_new --against output/reaches/full
```

```
inputs    ResolveContext(graph, registry)    one build's output   (step 1)
          pipeline/parsing/entries/*.json    curated truth
outputs   rule_section(entry_id, rule_id, section_id)
          rule_extent(...)                   the AUTHORED form, preserved for audit + rebuild
          rule_unresolved(entry_id, rule_id, reason)
          rule_diagnostic(entry_id, rule_id, kind, payload)   -- straddlers, ambiguous cuts (㊴)
          report.json                        histograms + the diff
```

### 6.2 — Stages, per entry (entries are independent)

```
1  covered_ids     ← matched[] + also[]              the items this row regulates
2  entry scope     ← resolve_extent × scope[]        clip set, or scope_unresolved   (㉗)
3  per rule, per extent → Reach                      the shared resolver             (step 1)
4  union extents per rule
5  clip to the entry scope
6  classify        → bound | empty | unresolved(reason)
7  emit rows + diagnostics
```

### 6.3 — Five properties, each load-bearing

- **Pure and deterministic.** No clock, no network, sorted iteration throughout. Same
  `(build, entries)` → byte-identical output. This is the *precondition* for ⑬ determinism; a
  non-deterministic reach builder makes a byte-identical bundle impossible no matter what the packager
  does.
- **Total.** Every rule appears exactly once as `bound`, `empty`, or `unresolved` — never absent. This
  is ⑪ and ㊳ enforced at the point of production rather than audited afterwards.
- **Parallel.** 1,392 entries are independent. Load the ~0.7 GB graph once, fan out read-only. This is
  what keeps a full pass inside the step-1 budget.
- **Incremental.** Cache keyed on `sha256(entry_json ‖ build_id)`, so confirming one entry re-resolves
  in milliseconds. This is what lets the **review app call the reach builder** instead of keeping its
  own path — the structural fix for ㊲, since after it there is only one implementation to compare.
- **Auditable.** The authored extent is stored beside its resolution, so any binding can be explained
  and rebuilt without re-deriving it from geometry.

### 6.4 — Diff mode is the reason it is next

`--against <prev>` reports which rules changed section set between two builds: per rule
`added` / `removed` sections, grouped by entry, sorted by blast radius, with the entry's confirmed flag
shown.

Without it, a rebuild is unreviewable. §0.2 measured 2,950 items changing content in one routine
build — the curator's real question is not "which items moved" but **"which of my 108 confirmed entries
now mean something different"**, and nothing today can answer it. This also gives step 2's reopen gate
and the ⑬ determinism check a natural home.

### 6.5 — Acceptance criteria

- **6.1** Deterministic: two runs over the same `(build, entries)` are byte-identical.
- **6.2** Total: `bound + empty + unresolved == 3,038`, asserted; zero rules absent.
- **6.3** No rule is `empty` without a `rule_unresolved` reason, and no `unresolved` has
  `reason == None` (㊳).
- **6.4** Full pass within the step-1 budget: **≤ 5 min wall after graph load**, actuals recorded.
- **6.5** Incremental: re-resolving one changed entry touches only that entry; asserted by count.
- **6.6** Diff mode reproduces `output/v2/full` → `full_new` and reports the changed-rule set; the
  curated-cut survival number (**270/271**, §0.1) is reproduced from it.
- **6.7** The review app's `/reaches` endpoint is served **by this builder** — one implementation, and
  ㊲'s tautology stops being a question anyone can ask.
- **6.8** Diagnostics are preserved, not dropped: every `unclassified` and `ambiguous_cut` from the
  resolver lands in `rule_diagnostic` (㊴).

---

## 7. Ordered steps with acceptance criteria

### Step 1 — Lift the resolver into `pipeline/resolve/`

Not a file move: `resolve_extent` *fetches its own data* from `lru_cache`d loaders pinned to
`output/v2/full` (`reuse.py:35-39`), so a builder invoked with a different `--out` would silently
resolve against the wrong build. The lift inverts that — the resolver receives its world.

```
pipeline/resolve/
  context.py   ResolveContext(graph, registry) · from_build(out_dir)
  extent.py    cut_at · by_measure · between_across_lines · resolve_extent
  entry.py     entry_scope_sections · entry_reaches · clip
  waters.py    waters()     (curator labels; builder passes with_waters=False)
```

`Reach` replaces today's `dict | None`, whose `None` is overloaded across five distinct failures — which
is what ㉔, ㉕ and ㊳ each complain about separately:

```python
@dataclass(frozen=True)
class Reach:
    sections: list[str]; unclassified: list[str]; ambiguous_cut: list[dict]
    waters: list[str]; resolved: bool; reason: str | None      # None iff resolved
```

reasons: `area_scope` · `area_id_dangling` (㉔) · `no_sections_for_items` (㉕) · `cut_not_found` ·
`no_splits` · `bad_op` · `parallel_branches` (㊳). *Resolved but empty* stays a real answer.

`reuse.py` shrinks to path constants, loaders, `invalidate_caches()`, and a serializer emitting `null`
when `not resolved`. **`API.md` and the frontend are untouched.**

- **1.1** Snapshot **before** editing: `entry_reaches` for all **1,392** entries against
  `output/v2/full` → `pipeline/tests/fixtures/reaches-baseline.json.gz`. Post-lift, **byte-identical**.
  The only legitimate parity test for the lift, precisely because the baseline predates the move.
- **1.2** No resolution logic remains in `curation-review/backend/reuse.py`.
- **1.3** `rtk grep -n "output/v2" pipeline/resolve/` returns nothing.
- **1.4** `test_review_reaches.py` imports `pipeline.resolve`; the `sys.path` insert and
  `importorskip("reuse")` are deleted; all 19 tests pass with unchanged assertions.
- **1.5** `GET /api/entries/{id}/reaches` byte-identical for 20 fixed entries: Chilliwack (braids), the
  four Fraser rows and both Adams rows (entry `scope`), one lake-bounded reach (alias path).
- **1.6** Resolves all **3,038** rules against an arbitrary `--out`; **≤ 5 min wall after graph load,
  ≤ graph size + 2 GB peak RSS**, actuals in the commit message.
- **1.7 (㊳)** Every extent is `resolved` or carries a `reason`; histogram printed.
- **1.8** All **10** entry `scope` extents resolve; `scope_unresolved` still surfaced (keeps ㉗ fixed).

**Not in scope:** tributaries — `item_tributaries` stays in `reuse.py` (one level, item-scoped, for
drawing a curator a picture). The reach-scoped walk is step 8.

### Step 2 — Close ⑥, delete the trap

- **2.1** Doc 10 records `{blk}:{measure}`; decision #1 closed with §0.3.
- **2.2** `cutting.section_id()` **deleted**, or its hash branch fixed (`wbk`) *and* given a production
  caller. A function whose only caller is a test and which collapses 7,409 lakes into one id does not
  stay as-is.
- **2.3** Non-slow test: registry section ids unique within a build (0 dupes over 49,542 and 64,128).
- **2.4** `pipeline/tools/id_churn.py` prints §0.1 + §0.3 for any build pair in one command.
- **2.5 Reopen gate:** run 2.4 after the next **re-anchoring** build. Reopen ⑥ only if hash survival
  beats measure survival by **> 2 points**.

### Step 3 — Structured dates (⑧, ㉜)

584 strings over **167 unique values**.

- **3.1** All 167 normalise or land in an explicit residue file. Silent failure not permitted.
- **3.2** Golden table test pinning all 167 → parsed form.
- **3.3** `year_round` ≠ `unknown`; the **354 undated `No Fishing` closures are `year_round`**.
- **3.4** `unknown` is reserved for **normaliser failure**: assert `unknown` count == residue count.
- **3.5** ㉜'s `default_only` is a **client render state** for the 97.6% with no assessed rule — not a
  property of a rule, and must not appear in `rule_date_status`.
- **3.6** Round-trip: every parsed window re-serialises and re-parses identically.

### Step 4 — CI runs the slow tests (㊷)

`pytest.ini` sets `addopts = -m "not slow"`: **153 tests deselected by default.** Determinism, the
independent oracle and any full-build guard are all naturally `slow`.

- **4.1** A CI job runs `-m slow` explicitly and fails the build on it.
- **4.2** A test asserts the slow set is non-empty, so a marker rename cannot quietly empty that job.

### Step 5 — The reach builder — §6, criteria at §6.5

### Step 6 — Spike both packagings before committing to either

Build a throwaway packager for **one region** from the reach builder's output and measure.

- **6.1 Web:** bytes to answer a cold tap (target **≤ 200 KB**) and a text search.
- **6.2 Mobile:** full-corpus resident size; delta for a single-region regulatory change.
- **6.3** Both against the **same** content store, so the comparison is packaging only.
- **6.4** Decide shared format or two, and **record the numbers**. Assumption is not permitted —
  `tier0.json` is what a format chosen by argument costs.

### Step 7 — Build the content store, deterministic, with a real oracle

**The ㊲ oracle — three layers, three different questions.** After step 1, "bundle == review app" is one
resolver answering twice.

- **L1 — did the refactor change anything?** Step 1's pre-lift snapshot. Retired once step 1 merges.
- **L2 — is the resolver self-consistent?** The `API.md` invariants over the **425 bounded extents**
  (415 rule + 10 entry scope) and the **396 distinct (entry, cut) pairs** they bind. Not tautological:
  `by_measure` computes each direction independently and enforces none of these.
  - **L2a Partition** — `up(X).sections`, `down(X).sections`, `up(X).unclassified` pairwise disjoint,
    union exactly `universe(S)`.
  - **L2b Straddler symmetry** — `up(X).unclassified == down(X).unclassified`. Asymmetry = the
    **braid edge-direction** defect's signature.
  - **L2c Monotonicity** — same blk, `m(A) > m(B)` ⟹ `up(A) ⊆ up(B)`, `down(B) ⊆ down(A)`.
  - **L2d `between` = intersection** — `== down(A) ∩ up(B)`, order-insensitive.
  - **L2e Connectivity** — a resolved reach is connected after removing the cut. A disconnected
    component = the **split-alias** defect.
  - **L2f Alias equivalence** — binding by an alias resolves identically to the canonical id.
- **L3 — is the resolver right?** An **independent second implementation** for the on-blk case: pure
  route-measure selection from the GPKG geometry, no fixpoint, no adjacency. Equal on the mainstem
  subset (braids excluded — what L3 cannot judge). Marked `slow`.

**L2 is self-consistency, L1 a refactor net; only L3 is an oracle in the strict sense.**

- **7.1 (⑬)** Two builds from identical inputs are byte-identical: fixed page size, no timestamps,
  deterministic ordering, `VACUUM`. Marked `slow` (hence step 4 first).
- **7.2** L2 green on all 425 extents / 396 cut pairs, failures listed **per extent**.
- **7.3** L3 green on the mainstem subset (`slow`).
- **7.4** Census assertions as row counts: 49,542 distinct sections · 97 in two items · 7,409 lake
  sections · 25,800 variants in search · 1,392 `regs_verbatim` · 2,907 `display_location` · 10
  `entry_scope` · 305 `needs_review` · 176 locators. These are the fields doc 10's census caught the
  first draft **dropping**.
- **7.5 (㊸)** 0 of 3,038 `rule_text` are not a substring of their `regs_verbatim`.
- **7.6 (㉙)** `sections_override` has a column and is honoured (**0 uses today**).
- **7.7 (㊱)** `publishable` ≠ `confirmed`: locked **and** no unresolved locators **and** every rule ≥1
  section. Report the count over the 108 locked entries.
- **7.8 (⑳)** A killed build worker exits **non-zero**, tested by killing one.
- **7.9 (§1.3)** Layer-additivity: a new entity type requires no edit to an existing table.
- **7.10 (§0.2)** `item_tombstone` populated; a durable binding to a removed item resolves to a
  tombstone, never to silence.

### Step 8 — Tributary walk, bitmaps, stable dense ids

**554 rules / 264 entries**, three-valued; 132 set it explicitly.

- **8.1** Recursive, **reach-scoped** (relative to the rule's resolved sections, not the named river —
  ③), barrier-aware; validated against a flow walk, never a watershed-code prefix.
- **8.2 (③)** The Kootenay must **not** gain Moyie River, Yahk River, or the 3,205 km the WSC shortcut
  over-included. Asserted by name.
- **8.3** Roaring bitmaps — the Fraser rule is **one bitmap**, never 419,164 rows.
- **8.4 (㉞)** Dense ids append-only from a stable sorted key, minted **per packager**. Test: add an
  item, repack, assert every pre-existing dense id is unchanged.
- **8.5 (㉘)** Parity scoped to **direct** extents; tributary expansion tested separately.
- **8.6** `tributaries_only` (**44** rules, not 4) omits the mainstem; `tributary_exclude` subtracts.

### Step 9 — Base regulations (235 rows)

- **9.1** All 235 load; the **7 `disabled`** do not ship live.
- **9.2 (㉒)** Keyed by `base_rule_zone` (regions `1`–`8`, `7A`, `7B` — **225 of 235**) +
  `base_rule_mu` (10). MU by polygon, then MU→zone; no invented expansion.
- **9.3 (㉑)** `exempt_code` on the ~9 default families; assert every `exempts_from` value joins.
  Until this passes, doc 10 §2's `defaults − exemptions` is unimplementable and "is it open" cannot be
  answered.

  ⚠️ **Doc 10 ㉑'s "81 rules, 7 codes" is wrong.** Counted 2026-08-29: **59 rules**, and only
  **2 of the 7** vocabulary codes are used at all — `spring_closure` (55) and `bait_ban` (5).
  `single_barbless_hook`, `trout_char_release`, `bull_trout_release`, `summer_closure` and
  `kokanee_stream_quota` have **zero** uses.

  That is worse than a stale count. The subtraction doc 10 calls *"the only correct way to
  answer is it open"* has almost no data behind it, and a UI review found the textbook case
  proving it: the Lardeau River row states both its exemptions **in words** while carrying an
  empty `exempts_from`, typed as a `note`. So the mechanism cannot fire on the very example
  it exists for. Building the join (㉑) is necessary but **not sufficient** — the codes have
  to be stamped during curation too, and 5 of 7 have never been used once.
- **9.4 (㉝)** Species tags on the 235; assert the **86 zone quota rules** are tagged.
- **9.5 (⑦)** 14 free-text types → the 6 enums as a table test; an unmapped type is an error.

### Step 10 — Live channels + the overlay model (§5)

- **10.1** Feeds versioned **independently** and joined on **`item_id`**. Test: a feed update must not
  change the bundle version.
- **10.2** No build-time match table. Test: a station added to the feed binds to its item **without a
  rebuild** — the explicit repair of v1's stated limitation (§2).
- **10.3** In-season overrides apply through the **same precedence model** as rule-vs-rule (⑰).
- **10.4** Both clients render **age** on every live value and distinguish "no closure" from "could not
  check". Tested with the network disabled.
- **10.5 (㊵)** Edition + `valid_until` (**31 Mar 2027**) in the manifest; the client degrades loudly.

### Step 11 — Geometry and documents

- **11.1 (⑲)** Regulable water is **180.5 MB of WKB (8.3%)** → ~15–30 MB tiled: shippable. The blocker
  is the **basemap** — `bc.pmtiles` is Protomaps, not water. Ship a stripped extract (earth/water/
  roads/places, z4–12).
- **11.2** Tile props carry `section_id` + the packager's dense id. No fid→reach table.
- **11.3 (§0.4)** Bathy **contours** (17.3 MB) become a bundled tile layer. Bathy **sheets** (738 MB,
  2,685 PDFs) are **never bundled**: web links out, mobile downloads per lake on demand with a managed
  cache and an explicit size prompt.
- **11.4 (④)** Pre-buffered park / eco-reserve + MU polygons; containment tested **client-side**, so the
  unnamed creek inside a reserve resolves.
- **11.5 (㉔)** The two dangling `within(area)` `area_id`s fixed, or made `rule_unresolved`.

### Step 12 — Update mechanics

- **12.1 (⑭/⑮)** Staging → verify sha256 → atomic swap → previous generation retained. Client survives
  a cold cache.
- **12.2** Manifest pins tiles **and** bundle data; mixed pair refused. Feeds sit outside this fence by
  design (§1.1).
- **12.3** Push carries a **version, never data**.
- **12.4 (㉛)** Mobile downloads data on first run; it must **not** ship as an app-store asset.
- **12.5 (㉚)** Both `r2-worker/src/index.ts` bugs fixed before a range-reading client exists: suffix
  ranges (`bytes=-N`) falling through to a full-object GET returned as 206 with a fabricated
  `Content-Range`; and Cache API `put()` throwing on a 206 into a swallowed `.catch(()=>{})`, which is
  why no range read is ever edge-cached.
- **12.6** Tested against a fabricated second version, including an interrupted update.

---

## 8. Order

```
PIPELINE
 1 lift resolver ──▶ 5 REACH BUILDER ──┬─▶ 6 spike packagings ─▶ 7 content store ─┬─▶ 9 base regs ─▶ 10 live
 2 close ⑥ ────────┐                   │                                          ├─▶ 8 tributaries
 3 dates ──────────┼───────────────────┘                                          └─▶ 11 geometry ─▶ 12 updates
 4 slow-test CI ───┘                                    │
                                                        │ store shape settles
APP (greenfield, parallel)                              ▼
 §4.2 decide UI strategy ─▶ core + RegsSource iface ─▶ data-mobile ─▶ apps/mobile ─▶ data-web ─▶ apps/web
      (at kickoff)            (fixtures, no bundle)                   OFFLINE FIRST
```

**1, 2, 3 and 4 can start now** — none waits on curation or on any open decision. **Step 5, the reach
builder, is the next substantial thing to build**, and step 1 is its only prerequisite.

**The app track (§4) is independent of the pipeline track and can start in parallel**, because it is
greenfield: `packages/core` and the `RegsSource` interface can be built against fixtures before any
bundle exists (§4.4 step 1). What it *cannot* do is get ahead of step 6 — `data-mobile` and `data-web`
cannot be written until the packaging spike settles the store's shape. Decision #7 (§4.2) should be
made at app-track kickoff, not deferred.

**Step 7 is the first thing that blocks on a human decision.**

---

## 9. Open decisions

| # | Decision | Gates | Status |
|---|---|---|---|
| 1 | Exported section id | step 2 | **Closed** — keep `{blk}:{measure}` (§0.3) |
| 2 | Does the bundle ship **unconfirmed** entries, and how is it marked? | step 7 | **Open — needs the human** |
| 3 | **Precedence** when two rules disagree — *and* how in-season overrides apply (§5) | steps 7, 10 | **Open — needs the human** |
| 4 | Offline geometry scope | step 11 | Open; ⑲ and §0.4 now measured, so decidable |
| 5 | Shared file format for both packagers, or two? | step 6 | Open — but mobile-first now points at **one base format, two packagings** (§1.2); confirm by measurement |
| 6 | `Catch and Release` → harvest or gear? | step 9 | Open, low stakes |
| 7 | **Cross-platform UI strategy** — shared logic + forked views (A), or shared views via RNW (B)? | app kickoff | **Open — needs the human.** Recommendation A (§4.2); expensive to reverse |
