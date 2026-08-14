# DESIGN — Regulations → sections (the new matching model)

> **Status:** consolidated design, replacing `DRAFT-split-model-unification.md`.
> Supersedes the old match-table / overrides / reach-builder approach in `pipeline/`.

---

## The shift

The old pipeline matched a reg entry to a *whole waterbody*, then merged segments that
shared a reg-set into "reaches". The new pipeline never merges. The **graph already cut the
network into stable sections**; a reg's job is only to **attach its id to the sections it
governs**, via a small, editable pointer. Splits stay splits; a reg can be added or changed by
re-running one dumb join, and geometry never moves.

Everything is **auto-seeded where it's easy, overridable everywhere.**

---

## Five layers

| layer | built from | keyed by | lifecycle |
|---|---|---|---|
| **Graph** (sections) | FWA + curation | `node_id` (`{blk}:{m}` / `lake:{wbk}`) | rebuilt; the universal geometry+topology |
| **Splits** (geometry) | hand-curated | split `id`; `applies_to` an FWA channel | **separate & stable (ABI)**; fed *into* the graph build |
| **Registry** (named items) | **the graph** | FWA id (`gnis`/`wsc`/`wbk`/`blk`) | derived index over the graph |
| **Entries** (identity + rules) | frozen parse | `(region, mu, name)` | edited; re-parse is a deliberate merge |
| **Match** | auto lookup + overrides | entry → registry id | derived, override on top |

The **join** is the whole build: `entry.rules → splits → sections → section_id → [reg_id]`.

### 1. Graph — already the source of truth for sections

`StreamGraph` nodes are the atomic reaches. Each `StreamNode` already carries `gnis_id`,
`display_name`, `name_tuples` (name + variants), tributary topology (`up_adj` = ancestors), and a
derived `location_identifier` ("upstream of X" / "between X and Y" / "within {area}"). **Sections are
not MU-tagged** in this pipeline (no zone/base-reg matching here); each named registry item
carries the **set** of MUs its line passes through (line × WMU intersection over its segments),
used only to break name collisions — see §Registry. Sections come from ANY boundary source — a curated split, a lake edge, an
`mu_boundary`, the BC border, an `area_boundary` (park), or a **name-variant / display-name
override**. An override is itself a split source: it both *names* a BLK and *cuts* it into its own
section, so a Fraser side channel like "Seabird Island Channel" becomes its own named section even
though it still shares the Fraser's `gnis`/`wsc`. The matcher is blind to which source made a
boundary.

### 2. Splits — separate, self-contained geometry

Same anchor types as today (point / line / lake / confluence / mu_boundary / area_boundary, with
along-channel offsets), but **reorganized by waterbody** (see Data types). A split is **cut geometry
+ a stable `id`**; its `applies_to` names the FWA channel(s) it cuts and its `anchor` says where.
Splits are an **input to the graph build** (which applies them to produce section nodes); their `id`s
are what a rule points at. They do no name-matching.

### 3. Registry — a name-resolution index over the graph

One entry per **named** freshwater item (stream or lake) — plus **area** items (parks and other
admin polygons) — generated from the graph:

```jsonc
// registry/<id>.json
{ "id": "gnis:19983",
  "name": "Coquihalla River",
  "variants": ["Coquihalla R", ...],     // from name_tuples
  "kind": "stream",                        // stream | lake | area
  "mus": ["2-14","2-15"],                  // SET of MUs the item's line passes through (line x WMU); only to pick among name collisions
  "regions": ["2"] }
```

An **area item** (`kind: "area"`, e.g. `park:garibaldi`) resolves not by channel geometry but by
containment: **its sections are every section with any part inside the polygon** (geometric
intersection, no buffer). An `area_boundary` split may still cut the network where a channel crosses
the boundary — tagging the inside pieces (`StreamNode.in_areas`) — for cleaner display, but is not
required for matching; a straddling section still counts.

**Name variants are applied during graph generation**, not in the registry — the graph build is
where `name_tuples`/`display_name` (including overrides) are resolved onto each node, so the
registry merely surfaces what the graph already decided. With that, the registry absorbs what the
old pipeline scattered: the match-table's `BaseEntry` natural search and "which MUs does this
touch". Because it indexes **every** named item in the graph — not only ones printed in the
synopsis — an item is always resolvable.

**"All sections for a registry item"** = the graph nodes matching the item's id (`gnis`/`wsc`/`wbk`/
`blk`), in `down_m` order; its **tributary** sections = the graph ancestor walk (`up_adj`) from those
nodes. No enumeration, no propagation — a graph query.

### 4. Entries — frozen parse: identity + rules

The parser output is **frozen and checked in** (like `splits.json`), each rule with a stable
`rule_id`. After the one-time parse it is editable data; re-parsing is a reviewed diff/merge, never
an automatic overwrite — so curation attached to `rule_id`s never gets blown away.

A rule points at its reach by **`extents` (a list of `op + split ids`; their union)** — sections are
derived — with entry-level *and* rule-level scope that compose by intersection:

```jsonc
{ "entry_id": "atnarko_main",
  "identity": { "region": "5", "mus": ["5-4"], "name": "Atnarko River" },
  "regs_verbatim": "…the raw reg block, embedded for reference…",
  "matched": ["gnis:atnarko", "gnis:bella_coola"],   // written by the matcher (layer 5)
  "includes_tributaries": true,
  "scope": [],                                         // entry-wide Extents (was entry_location_text)
  "rules": [
    { "rule_id": "atnarko_main.r5", "type": "vessel_restriction",
      "extents": [ { "op": "between", "splits": ["goat_creek_confl", "talchako_confl"] } ],
      "includes_tributaries": false,                   // rule overrides entry flag
      "details": "No powered boats", "dates": [], "rule_text": "No vessels between …" }
  ] }
```

Operators: `whole · upstream_of(s) · downstream_of(s) · between(a,b) · within(area)`.
`includes_tributaries` (entry or rule level) pulls in the ancestor sections **minus** any
sections claimed by another entry (see §"Tributaries" below). A `sections_override: [...]`
escape hatch lets a genuinely un-resolvable rule name section ids directly.

### 5. Match — a thin, three-tier pointer

Matching stops being a subsystem; it is one derived field (`matched`) on an entry:

1. **Registry lookup (auto)** — entry `(name + mu + region)` → registry id. All the easy matches.
2. **Overrides (manual)** — pin/redirect when auto is wrong, ambiguous, or ungazetted.
3. **Unmatched** → flagged for curation.

Parse and match **unite on the entry**: the parser writes `{identity, rules}`; the matcher writes
`matched`. Nothing else needs to know how matching happened. In-season additions cost nothing —
the item is already in the registry; you author a rule pointing at it.

---

## The build (one dumb join)

```
splits.json ─┐
FWA ─────────┼─► graph build ─► sections  ─► registry (index)
curation ────┘                     │              │
                                   ▼              ▼
raw regs ─► parse ─► entries ─► matcher ─► entry.matched ─► JOIN ─► section_id → [reg_id]
                     (rules: op + split refs)                (rules pick sections by op)
```

For each entry: take its matched registry items → their sections → for each rule, select
sections by `(op, split refs)` intersected with entry+rule scope and the tributary flag →
append `reg_id`. Never reads another entry. Output is a flat overlay; re-run it any time without
re-cutting geometry.

### Tributaries — a set, not a walk

`includes_tributaries` resolves to "the item's ancestor sections in the graph". Exceptions are
**self-declaring**: a tributary reach that has its own entry (e.g. "Burnt Bridge upstream of
Sitkatapa") is claimed by that entry, and a **direct match beats an inherited one** — so the
parent's tributary set simply excludes it. `{all tribs − exceptions}` falls out of the graph plus
that one priority rule; nothing is enumerated or propagated.

---

## End-to-end data flow (worked: Chemainus)

```
1. DOWNLOAD    fetch the BC synopsis PDF(s)
2. EXTRACTION  pipeline/extraction: PDF → rows. Row = {name:"Chemainus River", region:1,
               mus:["1-5"], raw_regs:"No fishing between Copper Canyon Falls and the signs…"}
               → output/ (ephemeral, regenerable)
3. FWA DATA    data/: Freshwater Atlas geometry (every stream/lake)
4. SPLITS      pipeline/splits.json (by waterbody, curated FIRST): Chemainus →
               bannon_confluence, copper_canyon_falls, signs_100m  (id+label+note+anchor)
5. GRAPH BUILD pipeline/build: FWA + splits + name_variants → StreamGraph. Chemainus cut into
               sections chem_1..chem_4 (each with location_identifier)
               → REGISTRY item gnis:chemainus {variants, MU-set, its sections}
6. PARSE       revived agent_parsing (given raw_regs + Chemainus's splits as context) →
               pipeline/parsing/entries/region-1.json:
                 Entry{ name:"Chemainus", rules:[{closure, extents:[{between,[falls,signs]}]}] }
7. MATCH       matcher: "Chemainus River"(reg1, mu1-5) → gnis:chemainus (unique) → entry.matched
8. RESOLVE     resolver: rule → matched item's sections → extents pick chem_3 → append reg_id
                 → SectionRegs { chem_3: [closure_reg] }
9. DELIVER     serialize SectionRegs + geometry → client tiles/json → MAP highlights chem_3
```

**Frozen / checked-in:** step 4 (splits) + step 6 (entries) + `match_overrides` + `name_variants`.
**Derived each build:** everything else (graph, sections, registry, SectionRegs, client artifacts).

## Data types through the pipeline

Each stage has one clear datatype. **Curated inputs are separate files; everything downstream is
derived.** Splits are geometry consumed *only* by the graph build — never baked into entries or the
registry; a rule references a split by `id`, and "which sections" is read off section bounds.

### Curated inputs (hand-authored, each its own file)
- **`splits.json` — organized by waterbody** (converted from `waterbody-splits.json`). The grouping
  is for curation clarity: all cuts on one river sit together. Consumed **only** by the graph build,
  which flattens it to per-channel cuts. Shape:
  ```jsonc
  { "waterbodies": [
    { "name": "Chemainus River",         // human label — grouping/readability only
      "applies_to": { "gnis_id": "…" },  // WHICH FWA channel(s) these cuts sit on (gnis|wsc|blk; default — a split may override)
      "splits": [
        { "id": "bannon_confluence",     // human-readable; rule extents reference this for set logic
          "label": "Bannon Creek",       // clean display name -> drives location_identifier
          "anchor": { "type": "confluence", "tributary_wsc": "…" },  // WHERE the cut lands (+ its own resolution info, e.g. the tributary)
          "proximity_m": 500,
          "note": "Seasonal-closure boundary at the Bannon Creek mouth.",  // refined, user-facing
          "_note": "WSC self-validates; …" }                              // internal curation only (ignored by loader)
      ] } ] }
  ```
  **Self-contained geometry — not a match.** `applies_to` (which channel to cut) + `anchor` (where, with
  type-specific resolution info like `confluence.tributary_wsc`, `lake.wbk`, `mu_boundary.mu_a/mu_b`)
  are consumed **only** by the graph build. The splits file does no name-matching; the reg entry
  matches its name to a registry item independently. They meet only at the FWA id, as two data flows.
  - **`id`** — the stable ABI a rule's `Extent.splits` points at; human-readable; **globally unique**
    (so a cross-waterbody system rule can reference a cut on either river).
  - **`label`** — the clear name shown to users; the sectionizer builds `location_identifier`
    ("upstream of {label}") from it.
  - **`note`** vs **`_note`** — `note` is the refined, possibly user-facing explanation; any `_`-prefixed
    field is internal curation reasoning, ignored by the loader (replaces today's `_comment`/`_concern`).
  - The waterbody `name` + `applies_to` are for grouping and cut-resolution; region/MU is **not** here.
- **`Entry`** — the parser's output (see below). Editable; the new parser emits this shape directly.
- **`match_overrides`** — `match_overrides.json`. Pins `entry → registry_id` for the ambiguous tail.
- **`name_variants.json`** — graph-build name/override input (already exists; absorbs old `feature_display_names`).

### Graph-build products (from FWA + `splits.json`)
- **`Section`** *(exists in `models.py`)* — the atomic reach: `{section_id, blk, wsc, gnis_id, display_name, name_tuples, location_identifier, lower_bound, upper_bound, mu_ids, tributary_section_ids, is_lake, in_areas}`.
- **`SectionBoundary`** *(exists)* — `{boundary_id: "split:{id}" | "lake:{wbk}" | "outlet" | "headwaters", kind, label}`. **This is the link** from a split id to the sections it bounds — the basis of op resolution (no route-measure query).

### Registry (derived from the graph)
- **`RegistryItem`** — `{id, name, variants, kind: stream|lake|area, mus, regions}`.
  - **`id`** = the broadest stable key the graph's grouping carries: streams **`gnis:` → `wsc:` → `blk:`** (prefer `wsc:` over `blk:` — it spans the whole coded stream + its braids; `blk:` only for a lone channel); lakes **`wbk:`**; areas **`area:`**. Nothing assumes a gnis exists.
  - **`name` / `variants`** come straight from the **graph node** (`display_name` / `name_tuples`) — which already merged blk-chains and applied `name_variants`. The registry never re-derives names from raw FWA; it reflects what the graph decided (identity, merges, names, variants).
- **`registry_index`** — `item_id → [section_id]` (its own sections; tributaries via graph ancestors). Area items → sections intersecting the polygon.

### Entries (parser output = the curation surface)
The entries file is both the parser's output **and** the file a curator hand-edits (fix an `extent`,
add a `sections_override`, correct a `matched`). It is **self-contained**: it embeds the verbatim
source (`regs_verbatim` + per-rule `rule_text`), so you never need the ephemeral extraction to read
or curate it. Store as per-region files (`pipeline/parsing/entries/region-2.json`) for small diffs.
- **`Entry`** — `{entry_id, identity{name, region, mus[]}, regs_verbatim, matched:[registry_id], includes_tributaries, scope:[Extent], rules:[Rule]}`.
- **`Rule`** — `{rule_id, type, details, dates[], extents:[Extent], includes_tributaries: bool|null, sections_override:[section_id]?, needs_review: bool, review_reason?, rule_text, location_text}` (last two = verbatim provenance). `needs_review` = the parser couldn't confidently bind it → hand-curation queue.
- **`Extent`** — `{op: whole|upstream_of|downstream_of|between|within, splits:[split_id], item?: registry_id, area?, kind?}`. The `op+split` binding. A rule's `extents` is a **list → union** (covers "A plus B"); `item` scopes an extent to a *different* registry item than the entry's `matched` (covers "plus Tenas Lake" / named side channels). Entry `scope` ∩ each rule extent composes.

### Resolve output
- **`SectionRegs`** *(exists)* — `{section_id, named_reg_ids[], tributary_reg_ids[]}`. The flat overlay; the deliverable.
- **`CoverageReport`** — `{unresolved_rules[], orphan_splits[], unmatched_entries[], low_confidence[]}`.

**Flow:** `SplitDef + FWA → graph → Section/SectionBoundary → RegistryItem`; `parser → Entry(Rule/Extent)`; `matcher → Entry.matched`; `resolver(Entry × registry_index × Section) → SectionRegs (+ CoverageReport)`.

## The parser — rebuild on Claude Code, freeze, consume

**Today:** `pipeline/parsing/parser.py` sends synopsis-row batches to **Gemini**, validates against the
`ParsedBatch` pydantic models, and writes `output/pipeline/parsing/synopsis_parsed.json` (ephemeral);
`region`/`mu` come out null. (An archived `agent_parsing/` shows a chat-driven flow already existed.)

**New — drive the parse through Claude Code (chat + subagents), emit the `Entry` shape, freeze it in
`pipeline/parsing/`:**

- **Why Claude Code, not an API batch:** the parse is a **one-time frozen artifact**, so agentic care +
  in-context access to the curated files + human review beats fire-and-forget. Subagents parse batches
  (per region/page) for throughput. A future *recurring/in-season* parser can stay an automated job —
  different lifecycle, rebuilt from the archive.
- **Reuse the archived skeleton** — `archive/pipeline/agent_parsing/` already implements this workflow:
  `batch_exporter` (pending rows → `batch_NNN.json` + rendered prompt + digest manifest) → subagent per
  batch → `review_exporter` (independent reviewer subagent) → `ingest` (validate through the **same**
  pydantic gate, apply successes, fail loud) → `compare` (engine A/B diff). Revive it and change three
  things: emit the new `Entry` shape, feed the extra inputs below, and write to `pipeline/parsing/`.
- **Unit = one waterbody, with its splits as context.** The parse unit is a single waterbody entry:
  `raw_regs` + row metadata (region/mu → `identity`) + **its curated splits** (id, label, note, anchor
  kind) + the op enum. Binding a rule to a reach is then a **constrained selection** among the provided
  split ids — not blind geometry extraction — which is both far more robust and cheaper. This is why the
  splits are curated *first*, and it **replaces the separate seeder** (the parser emits `extents`
  directly). Pack several *small* waterbodies per subagent call for token efficiency; give gnarly ones
  (Atnarko) their own. The static prompt is cached.
- **Two-pass with reviewers + `needs_review`.** Each batch → an independent **reviewer subagent**
  (re-runs the pydantic gate, checks every `extent` binding is sane, audits cross-rule consistency);
  add a 2nd reviewer/self-consistency only for the unsure tail. Anything the parser can't confidently
  bind (no split matches "the 2nd bridge"; nested harvest; freeform polygon) is emitted with
  `needs_review:true` + a reason → the coverage report's hand-curation queue. Expectation: nearly
  everything auto; Atnarko/Bella-Coola-class → flagged.
- **Output → `pipeline/parsing/` (checked-in), not `output/`.** Because the parse **freezes** and is then
  hand-curated (extents, overrides), it must be version-controlled. `output/` stays for regenerable,
  ephemeral artifacts (raw extraction). Suggest per-region files (`pipeline/parsing/entries/region-2.json`
  …) for small, reviewable diffs.
- **Keep the pydantic gate:** the chain-of-custody checks (`location_text ⊆ rule_text`, verbatim fields,
  dates) still validate every emitted `Entry`.

**How it's consumed (interaction with everything).** Two clean tiers:
- **Frozen, checked-in, hand-curated:** `splits.json` (by waterbody) · `pipeline/parsing/entries/…` (parse)
  · `match_overrides.json` · `name_variants.json`.
- **Derived each build (ephemeral):** graph → sections → registry → `SectionRegs` + coverage → client.

```
FWA + splits.json + name_variants ─► graph build ─► sections + registry
pipeline/parsing/entries ──► matcher (+ match_overrides) ──► entry.matched
   then  resolver( entries × registry_index × sections ) ──► SectionRegs (+ CoverageReport) ──► client
```

Entries reference everything by **stable id** — registry ids (`matched`), split ids (`extent.splits`),
section ids (`sections_override`). A curator edits an entry in place; the build re-runs the join;
geometry never moves.

**Re-parse = deliberate merge, never overwrite.** A fresh parse diffs the checked-in entries by
`(entry_id, rule_id)`, preserving curated `extent` / `matched` / `sections_override`. That merge tool is
the one piece of tooling the freeze still needs (see "Still open").

## What happens to the old `overrides.json` (480 entries)

It doesn't survive as one file; each lever moves to its natural new home:

| old override field | new home |
|---|---|
| `gnis_ids` / `waterbody_keys` / `fwa_watershed_codes` / `blue_line_keys` / `waterbody_poly_ids` / `linear_feature_ids` | **`match_overrides`** — pin `entry → registry_id` |
| `skip` | entry matches nothing (empty `matched`) |
| `variant_of` / `canonical_name` | name resolution → `name_variants.json` / registry variants |
| `admin_targets` / `admin_feature_types` | **area `RegistryItem`** + rule `within(area, kind)` |
| `only_within_zones` | region prune (`identity.mus`) or explicit `within(mu)` |
| `ungazetted_waterbody_id` / `ungazetted_location` | synthetic `RegistryItem` (blk/area id) + authored geometry |

The current `overrides.json` stays in `archive/pipeline/matching/` as the source to port from.

## Worked example — Atnarko / Bella Coola system

Real parsed entry (today `output/pipeline/parsing/synopsis_parsed.json`; the rebuilt parser writes the
frozen `Entry` form to `pipeline/parsing/`): `includes_tributaries: true`,
`entry_location_text: "EXCEPT: Burnt Bridge Creek upstream of Sitkatapa Creek, Hunlen Creek
upstream of Hunlen Falls, and Young Creek upstream of Hwy 20"`, plus 8 rules. Printed under both
"Atnarko" and "Bella Coola" (one reg, a two-river system).

**Registry items** (from the graph): `Atnarko River`, `Bella Coola River`, `Burnt Bridge Creek`,
`Hunlen Creek`, `Young Creek`, `Tenas Lake` — each with variants + MUs + its sections.

**Splits** (geometry, keyed to registry items): `burnt_bridge_at_sitkatapa`, `hunlen_falls`,
`young_hwy20` (confluence/point), plus mainstem cuts: Tweedsmuir Park boundary (`area_boundary`),
`goat_creek_confl`, `talchako_confl`, `young_creek_confl`, signs-near-campsite (point), and the
Tenas Lake edge.

**System entry** → matched to `[gnis:atnarko, gnis:bella_coola]`, `includes_tributaries: true`.
Sections in scope = every section of both rivers + their ancestor (tributary) sections, minus the
three reaches claimed by their own entries. Then per rule:

| rule | op + splits | sections |
|---|---|---|
| closure Apr–Jun | `upstream_of(tweedsmuir_park)` (+ Tenas Lake) | park-and-above + Tenas node |
| closure | `between(tenas_lake, signs_campsite)` | that span |
| bait ban | `downstream_of(tweedsmuir_park)` | below the boundary |
| vessel | `whole`, mainstem-only | mainstem sections only (rule flag overrides trib flag) |
| vessel | `between(goat_creek_confl, talchako_confl)`, Atnarko only | that span |
| harvest / steelhead | `whole` | full scope |
| licensing | `downstream_of(young_creek_confl)` | below that confluence |

**Tributary entries** (`Burnt Bridge upstream of Sitkatapa`, etc.) match their own registry item,
`scope: upstream_of(sitkatapa)`, and attach their own rules to that reach. Their "see
Atnarko/Bella Coola" note is **ignored** — the downstream reach already got the system regs via
the parent's tributary set, and the upstream reach is claimed here. The Sitkatapa cut is the
watertight seam, with **zero cross-entry lookups**.

The residue to flag for curation: the nested *harvest* exception ("quota 1 EXCEPT Bella Coola
mainstem quota 2 EXCEPT char C&R on tributaries") — spatial ops can't express a two-level harvest
scope; the coverage report surfaces it for a `sections_override` or a hand edit.

---

## Coverage / reconciliation report (the safety net)

Every build emits: rules that resolved to **no** section (missing cuts to curate, or a bad
match), splits **no** rule points at (orphan geometry), and entries that matched **no** registry
item. This is the curation to-do list and the parse↔geometry consistency check in one.

---

## Decisions forced by review (must resolve before migration)

Load-bearing gaps found by tracing real entries + auditing the old `OverrideEntry` (usage counts
from `overrides.json`, 480 entries):

- **Region/MU is for item disambiguation, not section pruning** *(resolved)*. We do **not** tag every
  section with MUs — there is no zone/base-reg matching in this pipeline (that stays a separate MU
  overlay). `RegistryItem.mus` is the **set** of MUs an item's line passes through (line × WMU
  intersection over its segments — named items only, a bounded pass; a waterbody spanning several MUs
  carries all of them). It only picks the right item among same-named collisions — the reg's *stated*
  MU must be **in** the item's set. **Section selection is always complete via `registry_index`
  (item → all its sections)** — MU never filters sections. Region-scoped regs (Fraser Region 2 — one of the very few
  waterbodies carrying different regs per zone) are handled by a **curated `mu_boundary` split + a
  `downstream_of`/`between` op**, not auto-pruning.
- **Area closures need a type filter** *(HIGH; `admin_feature_types`)*. `within(area)` paints every
  section in the polygon; park regs like "all lakes in Kikomun Creek Park" need
  `within(area, kind=lake)` filtering on `Section.is_lake`. **Admin match = paint all valid sections
  touching the polygon** (your call), where "valid" honours this kind filter.
- **Auto-match only on a UNIQUE hit** *(MED; name collisions)*. Real data has `Boulder Creek` = 14
  gnis, and same-name collisions survive *within one watershed group + MU*. Tier-1 auto-match fires
  only when exactly one registry item matches `(name + mu∩region)`; any multi-hit falls through to
  overrides. Do not silently pick one.
- **Admin containment = any overlap** *(RESOLVED)*. A section is in the area if **any part of it
  lies inside the polygon** (geometric intersection; no buffer). Cutting at the boundary via
  `area_boundary` is an optional display refinement, not a matching requirement.

## The awkward real cases — and how each is encoded

1. **Union across operators** — "upstream of Park **plus** Tenas Lake" (Atnarko r0). Handled by
   `extents` being a **list (union)**, with `item` to reach another waterbody:
   `extents:[{op:upstream_of, splits:[tweedsmuir_park]}, {op:whole, item:"wbk:tenas_lake"}]`.
2. **A set of specifically-named side channels** — Seabird / Jesperson / Herrling (Fraser r3).
   Handled two ways that compose: a name-variant override **causes a split** so each channel is its
   own **blk-keyed registry item** matched by name; a rule can also list them via `extents` with
   per-extent `item` refs. No `sections_override` needed.
3. **Arbitrary polygon closure** — "area bounded by the 4 boundary signs" (Fraser r1). Curate the
   polygon as an **area** and use `within`:
   `splits:{id:"fraser_mission_closure", anchor:{type:"line", coords:[…4 signs…]}}` →
   `extents:[{op:"within", area:"fraser_mission_closure"}]`. Only a truly freeform blob with no clean
   boundary falls back to `sections_override`. (The linear "between two signs" variant is just
   `between(a,b)`.)
4. **Nested multi-level harvest exception** (Atnarko r2) — different *values* on nested scopes, not a
   spatial op. The **parser emits it as separate rules**, each spatial + valued (base quota=1 whole;
   quota=2 on Bella Coola mainstem; char C&R on tributaries). The overlay lists all three reg_ids on
   the overlapping sections; a **display precedence rule (most-specific wins)** picks the shown value.
   The only hard part is the parser splitting it — a prompting task, not a model gap.

## Systemic parse gap (input to the parser rebuild)

- **`region`/`mu` are null on all 1395 parsed entries; `rule.includes_tributaries` is null even where
  the text says "watershed".** These are exactly the fields the zone-pruning default and the
  tributary set need. They come from the **synopsis row metadata** (present in `waterbody-splits.json`
  cards as `mu`/`region`), not the LLM — so the fix is to carry row identity into the frozen entry,
  and to have the new parser set the tributary flag from "watershed"/"tributaries" wording.

## Still open

- **Registry granularity** — one FWA id per item; a river printed as one name spanning several `gnis`
  is handled via `matched: [...]`. An **override-named side channel is its own item keyed by `blk`**
  (it shares the parent's `gnis`/`wsc`), so a rule matches it by name directly (the Seabird path).
- **Ungazetted / custom waterbodies** — synthetic registry id + hand-authored geometry (today's
  `ungazetted_waterbody_id` + `[lon,lat]`); geometry ingest path still unspecified (5 real cases).
- **Pure "see X" cross-listings** — an entry with no rules of its own dedups to the same registry id;
  state this disposition explicitly (38 `skip` / 17 `variant_of` / 45 `canonical_name` in overrides).
- **Frozen-parse merge tooling** — how a re-parse diffs against checked-in entries without clobbering
  `sections_override` / edited rules.

## Building it — required pieces (in dependency order)

1. **New splits file** — convert `waterbody-splits.json` → by-waterbody `pipeline/splits.json`
   (`applies_to`, `splits[{id, label, note, _note, anchor}]`); update `load_split_defs` to read +
   flatten it.
2. **New models** — `Entry` / `Rule` / `Extent` (with `extents[]`, `item`, `needs_review`,
   `regs_verbatim`/`rule_text`) in `pipeline/parsing/models.py`; validation = chain-of-custody +
   "every `extent.split` exists".
3. **Registry build** — `RegistryItem` + `registry_index` + per-item MU-set. Needed both as **parse
   context** and for match.
4. **New prompt + examples** — rewrite `prompt.txt` / `examples.json` for "split into rules + bind
   `extents` from the **provided** splits + flag `needs_review`."
5. **Revive `agent_parsing`** — adapt `batch_exporter` (unit = waterbody + its splits),
   `prompt_render`, `review_exporter`, `ingest` (new schema; writes to `pipeline/parsing/`), `compare`.
6. **Matcher** — `entry → registry_id` (unique-hit) + `match_overrides` (ported from `overrides.json`).
7. **Resolver/join** — `Entry × registry_index × Section → SectionRegs`, region-pruned; + coverage /
   `needs_review` report.
8. **Precedence rule** (overlapping valued rules) + **frozen-parse merge tool** (re-parse by
   `(entry_id, rule_id)`).

Splits (1) + registry (3) must exist before the parse (4–5); models (2) before both parse and resolve.

## Migration — status

**DONE (2026-08-13):**
- **Archived** `pipeline/{matching,atlas,enrichment,tiles,graph,deploy,agent_parsing,recurring}` +
  old `__main__.py` + their tests → `archive/pipeline/` (reference only; a better `recurring/` will
  be rebuilt later from the archive). Kept: `extraction/`, `parsing/`, `utils/`.
- **Merged `stream_sections/` into `pipeline/`** (flat): the stream/section modules, `oneoff/`,
  `docs/`, tests, and data files now live under `pipeline/`. Imports repointed
  (`stream_sections.*` → `pipeline.*`); `wsc` consolidated to the single `pipeline/utils/wsc.py`
  (the duplicate `stream_sections/wsc.py` deleted, `anchors.py` repointed). Full suite green
  (121 passed, 11 skipped). No webapp/mobile breakage (they consume built artifacts, not Python).
- **Docs:** deleted `DRAFT-split-model-unification.md`, `10-matching-and-invariants.md` (watch-list
  salvaged to `ambiguous-matches.md`), and `16-phase5-match-plan.md`.
- **Grouped** the root modules into subpackages: `graph/` (blk_chains, names, graph, tributaries,
  cutting), `splits/` (anchors, splits, sectionizer, border), `io/` (serialize, export_gpkg);
  `models.py` / `build.py` / `run.py` + curated data (`splits.json`, `name_variants.json`) stay at
  root. Imports use explicit `pipeline.graph.graph` / `pipeline.splits.splits` (module-in-package).
  Added `pipeline/__main__.py` → `build.main`. Suite green (121 passed).
- **Dropped `feature_display_names.json`** — its data is already compiled into `name_variants.json`
  (the single graph-gen name-variant input). `overrides.json` stays in the archive (old matching).

**REMAINING — build the model:** see **"Building it — required pieces (in dependency order)"** above
(splits file → models → registry → prompt → revive agent_parsing → matcher → resolver → precedence +
merge tool). The parser binds `extents` directly from the provided splits, so there is no separate
seeder stage.

---

## DECISION 2026-08-14 — the parser's truth is the REGISTRY, not splits.json

**Change:** the parser is fed the **registry**, not `splits.json`. Each `RegistryItem` carries the
full set of **boundaries** (cut points) on that waterbody, so a rule's `extents` are a constrained
selection over *that item's own boundaries*. `splits.json` becomes an input consumed **only** by the
graph build.

**Why it's better:** the registry's boundary list is produced *during* the graph build, so it
naturally contains **both** curated splits **and** the auto-boundaries the build already creates
(lake edges, outlet, headwaters) — no second "manifest" file, no chicken-and-egg. The parser sees one
thing per waterbody; lakes like Tenas are already in the list, so "between Tenas Lake and the signs"
binds with no `needs_review`.

**Boundary id scheme (human-readable + robust).** A boundary is presented to the parser as:
```jsonc
{ "id": "tenas_lake",          // readable, UNIQUE within the registry item (curators/parser reference this)
  "kind": "lake",              // lake | confluence | point | area | outlet | headwaters | mu
  "label": "Tenas Lake",       // display name from the GRAPH node (already merged name_variants)
  "wbk": "329021804",          // robust key for lake kind
  "blks": ["360836756", "…"],  // every channel this boundary cuts -> handles a stream that BRAIDS into the lake
  "coord": [-125.71, 52.16] }
```
- **id** = slug of the graph `display_name` (canonical; name_variants already folded in), disambiguated
  (`_2`) if two boundaries on one item would collide. Readable — the parser can pick it from context.
- **wbk/blks** carry the robust identity separately, so the readable id never has to encode `wbk`, and a
  **braided** inflow (one lake, several blks) is ONE logical boundary mapping to several section edges.
- Curated `lake` anchors in `splits.json` become unnecessary — the auto lake boundary already appears in
  the registry with its label; author a curated split only to *rename* it or hang a specific reg boundary.

**Splits.json schema (this commit):** a split's per-split target override now uses a nested
`applies_to: {gnis_id|blk|wsc}` (same shape as the waterbody level), not floating keys. The loader
(`pipeline/splits/splits.py`) **validates** the file: a split under a `applies_to: null` waterbody must
carry its own `applies_to`, else load fails loud.

**Entry shape refinements (this commit):**
- Tributary scope is grouped into one object: `"tributaries": { "included": bool, "only": bool,
  "excludes": [Extent] }` (replaces the flat `includes_tributaries` / `tributary_only` / `excludes`).
  `excludes` are hand-curated carve-outs; the parser leaves them empty.
- `Extent.item` is only for the *itemless* case (e.g. `whole` scoping one river of a two-river match,
  "+Tenas Lake"). When an extent references split ids, the item is inferred from the split's own
  `applies_to` — so `item` is omitted (a selected split already knows which waterbody it's on).
- All **confluence** split ids use one format: `{landmark}_confluence` (e.g. `goat_creek_confluence`,
  `sitkatapa_confluence`) — uniform, so the parser/curator recognises a confluence boundary on sight.
