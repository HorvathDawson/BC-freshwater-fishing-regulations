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

Exceptions that do **not** self-declare (no separate entry) are named explicitly as **hand-curated
carve-outs** subtracted from the tributary set: entry-wide via `Tributaries.excludes` (applies to
every rule) or **per-rule via `Rule.tributary_excludes`** (scoped to one rule — e.g. a seasonal "No
Fishing in any tributaries except Quinsam River" where other rules on the same entry still cover
Quinsam). Each carve-out is an `Extent`: whole-tributary uses `item=<registry id>, op=whole`; a
partial carve-out references the boundary split(s). Consumed at the resolve step (Phase 5).

---

## End-to-end data flow (worked: Chemainus)

```
1. DOWNLOAD    fetch the BC synopsis PDF(s)
2. EXTRACTION  pipeline/regs/extraction: PDF → rows. Row = {name:"Chemainus River", region:1,
               mus:["1-5"], raw_regs:"No fishing between Copper Canyon Falls and the signs…"}
               → output/ (ephemeral, regenerable)
3. FWA DATA    data/: Freshwater Atlas geometry (every stream/lake)
4. SPLITS      pipeline/atlas/splits.json (by waterbody, curated FIRST): Chemainus →
               bannon_confluence, copper_canyon_falls, signs_100m  (id+label+note+anchor)
5. GRAPH BUILD pipeline/build: FWA + splits + name_variants → StreamGraph. Chemainus cut into
               sections chem_1..chem_4 (each with location_identifier)
               → REGISTRY item gnis:chemainus {variants, MU-set, its sections}
6. PARSE       revived agent_parsing (given raw_regs + Chemainus's splits as context) →
               pipeline/regs/parsing/entries/region-1.json:
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
or curate it. Store as per-region files (`pipeline/regs/parsing/entries/region-2.json`) for small diffs.
- **`Entry`** — `{entry_id, identity{name, region, mus[]}, regs_verbatim, source_symbols[], matched:[registry_id], tributaries{included,only,excludes[Extent]}, scope:[Extent], rules:[Rule], parse_review{verdict,model,issues[]}, locked, reviewed_by, revisit, revisit_note}`. `source_symbols`/`matched`/`registry_*` are ingest/matcher provenance; `parse_review` is the durable agent-review pass; `locked`/`reviewed_by`/`revisit*` are human curation.
- **`Rule`** — `{rule_id, type, details, dates[], extents:[Extent], includes_tributaries: bool|null, tributary_excludes:[Extent], sections_override:[section_id]?, needs_review: bool, review_reason?, rule_text, location_text}` (last two = verbatim provenance). `needs_review` = the parser couldn't confidently bind it → hand-curation queue. `tributary_excludes` = per-rule hand-curated carve-outs from THIS rule's tributary set (parser leaves `[]`).
- **`Extent`** — `{op: whole|upstream_of|downstream_of|between|within, splits:[split_id], item_id?: registry_id, area_id?, area_kind?}`. The `op+split` binding. A rule's `extents` is a **list → union** (covers "A plus B"); `item_id` scopes an extent to a *different* registry item than the entry's `matched` (covers "plus Tenas Lake" / named side channels). Entry `scope` ∩ each rule extent composes.

### Resolve output
- **`SectionRegs`** *(exists)* — `{section_id, named_reg_ids[], tributary_reg_ids[]}`. The flat overlay; the deliverable.
- **`CoverageReport`** — `{unresolved_rules[], orphan_splits[], unmatched_entries[], low_confidence[]}`.

**Flow:** `SplitDef + FWA → graph → Section/SectionBoundary → RegistryItem`; `parser → Entry(Rule/Extent)`; `matcher → Entry.matched`; `resolver(Entry × registry_index × Section) → SectionRegs (+ CoverageReport)`.

## The parser — rebuild on Claude Code, freeze, consume

**Today:** `pipeline/regs/parsing/parser.py` sends synopsis-row batches to **Gemini**, validates against the
`ParsedBatch` pydantic models, and writes `output/pipeline/regs/parsing/synopsis_parsed.json` (ephemeral);
`region`/`mu` come out null. (An archived `agent_parsing/` shows a chat-driven flow already existed.)

**New — drive the parse through Claude Code (chat + subagents), emit the `Entry` shape, freeze it in
`pipeline/regs/parsing/`:**

- **Why Claude Code, not an API batch:** the parse is a **one-time frozen artifact**, so agentic care +
  in-context access to the curated files + human review beats fire-and-forget. Subagents parse batches
  (per region/page) for throughput. A future *recurring/in-season* parser can stay an automated job —
  different lifecycle, rebuilt from the archive.
- **Reuse the archived skeleton** — `archive/pipeline/agent_parsing/` already implements this workflow:
  `batch_exporter` (pending rows → `batch_NNN.json` + rendered prompt + digest manifest) → subagent per
  batch → `review_exporter` (independent reviewer subagent) → `ingest` (validate through the **same**
  pydantic gate, apply successes, fail loud) → `compare` (engine A/B diff). Revive it and change three
  things: emit the new `Entry` shape, feed the extra inputs below, and write to `pipeline/regs/parsing/`.
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
- **Output → `pipeline/regs/parsing/` (checked-in), not `output/`.** Because the parse **freezes** and is then
  hand-curated (extents, overrides), it must be version-controlled. `output/` stays for regenerable,
  ephemeral artifacts (raw extraction). Suggest per-region files (`pipeline/regs/parsing/entries/region-2.json`
  …) for small, reviewable diffs.
- **Keep the pydantic gate:** the chain-of-custody checks (`location_text ⊆ rule_text`, verbatim fields,
  dates) still validate every emitted `Entry`.

**How it's consumed (interaction with everything).** Two clean tiers:
- **Frozen, checked-in, hand-curated:** `splits.json` (by waterbody) · `pipeline/regs/parsing/entries/…` (parse)
  · `match_overrides.json` · `name_variants.json`.
- **Derived each build (ephemeral):** graph → sections → registry → `SectionRegs` + coverage → client.

```
FWA + splits.json + name_variants ─► graph build ─► sections + registry
pipeline/regs/parsing/entries ──► matcher (+ match_overrides) ──► entry.matched
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

The current `overrides.json` stays in `archive/pipeline/regs/matching/` as the source to port from.

## Worked example — Atnarko / Bella Coola system

Real parsed entry (today `output/pipeline/regs/parsing/synopsis_parsed.json`; the rebuilt parser writes the
frozen `Entry` form to `pipeline/regs/parsing/`): `includes_tributaries: true`,
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

1. **New splits file** — convert `waterbody-splits.json` → by-waterbody `pipeline/atlas/splits.json`
   (`applies_to`, `splits[{id, label, note, _note, anchor}]`); update `load_split_defs` to read +
   flatten it.
2. **New models** — `Entry` / `Rule` / `Extent` (with `extents[]`, `item`, `needs_review`,
   `regs_verbatim`/`rule_text`) in `pipeline/regs/parsing/models.py`; validation = chain-of-custody +
   "every `extent.split` exists".
3. **Registry build** — `RegistryItem` + `registry_index` + per-item MU-set. Needed both as **parse
   context** and for match.
4. **New prompt + examples** — rewrite `prompt.txt` / `examples.json` for "split into rules + bind
   `extents` from the **provided** splits + flag `needs_review`."
5. **Revive `agent_parsing`** — adapt `batch_exporter` (unit = waterbody + its splits),
   `prompt_render`, `review_exporter`, `ingest` (new schema; writes to `pipeline/regs/parsing/`), `compare`.
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
  (`stream_sections.*` → `pipeline.*`); `wsc` consolidated to the single `pipeline/common/utils/wsc.py`
  (the duplicate `stream_sections/wsc.py` deleted, `anchors.py` repointed). Full suite green
  (121 passed, 11 skipped). No webapp/mobile breakage (they consume built artifacts, not Python).
- **Docs:** deleted `DRAFT-split-model-unification.md`, `10-matching-and-invariants.md` (watch-list
  salvaged to `ambiguous-matches.md`), and `16-phase5-match-plan.md`.
- **Grouped** the root modules into subpackages: `graph/` (blk_chains, names, graph, tributaries,
  cutting), `splits/` (anchors, splits, sectionizer, border), `io/` (serialize, export_gpkg);
  `models.py` / `build.py` / `run.py` + curated data (`splits.json`, `name_variants.json`) stay at
  root. Imports use explicit `pipeline.atlas.graph.graph` / `pipeline.atlas.splits.splits` (module-in-package).
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
(`pipeline/atlas/splits/splits.py`) **validates** the file: a split under a `applies_to: null` waterbody must
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

## DECISION 2026-08-14b — confluences stay CURATED (no auto-catalog); confluence-split shape

Rejected the "catalog all ~11.6k named-stream confluences" idea: it inverts the data flow (regs would
*drive* geometry instead of being applied to geometry), and auto-*cutting* explodes big rivers
(Fraser = 3,437 named descendants). **Confluence boundaries remain curated splits**, exactly as today.
(The registry still carries the AUTO boundaries the graph already makes — lake edges, outlet,
headwaters — those are cheap and finite. Only *confluences* are curated, because a confluence *cut* is
only ever needed where a reg draws a reach boundary there.)

**Confluence split shape (planned graph-side simplification):** a curated confluence carries only its
`tributary_wsc` (+ `_coord`/offset/`_note`). It needs **no `applies_to`** (the parent = the stream the
tributary flows into = `trim(tributary_wsc, 1)`, derivable at graph build / from topology) and **no
`id`/`label`** (both generated on the graph side from the *tributary + parent* display-names, e.g.
`eve_river__adam_river`, "Eve River → Adam River" — which also answers "confluence of what into what"
using post-name_variants names). Only a *non-tributary* point that happens to sit at a confluence keeps
an explicit id. Self-mouth confluences (a river's own mouth into a larger one) keep an explicit parent.
*Status: designed here; to implement when building the registry/graph confluence handling.*

**Label rule (implemented in build_splits):** a split's `label` is the BOUNDARY the reg names (from
`locator_text`) — "log boom", "signs at the tail of the canyon pool" — NOT the reference landmark in
`offset.anchor_label` ("IPP dam"). The offset carries the distance, so the "~N m up/downstream of X"
clause is stripped from the label. (Fixes the Kokish IPP mislabels + the apparent offset/id swap.)

## DECISION 2026-08-15 — areas: membership DECOUPLED from cutting (lazy catalog)

**Supersedes** the doc's earlier "areas are eager registry items" model. Two roles, and the **`cut` flag
in `areas.json` (renamed from `area_splits.json`) is the SOLE cut trigger** — not any "is it a full
closure" semantics:

- **`cut: true`** — split streams at the boundary (inside-only) *and* stay eager (`in_areas` → registry
  area items). Today: national parks, ecological reserves, Chilkoot, `land_access` where `restriction_level='closed'`.
- **`cut: false` (membership-only)** — a reg targets the area via override → the rule attaches to **every
  feature intersecting** the polygon (any part inside counts; no cut). New layers: all `parks_bc`, `wma`,
  all `land_access` levels (Malcolm Knapp incl.), named `watersheds` (Liard incl.), `historic_sites`.

**Lazy catalog (not eager membership).** The build writes a lightweight **area catalog** —
`{area_id, name, kind, polygon}` only, NOT per-area section lists (`pipeline/atlas/splits/area_catalog.py` →
a gpkg layer). Membership (intersects + `feature_types` filter) is computed at **resolve time**, only
for the few areas a reg references — so the registry stays lean and the build fast, yet any area (Liard
included) is referenceable. `area_id = area:{kind}:{slug}` (collision-safe).

**Areas are override-only** — never auto-matched (the matcher skips `area:` items). An override points a
row at an `area_id`; a rule binds `within(area, feature_types=[stream|lake|…])`. Wetlands are deferred
(not graph nodes).

## Datatype drift since the "Data types" section (2026-08-14/15)

The Entry/Rule/Extent shapes now carry more than the doc's §"Data types" lists — reconcile there later:
- **`Rule`** adds `display_location` (user-facing, curator-editable, non-verbatim), `unresolved_locators`
  (unbound locator phrases → forces `needs_review`), `species` (codes; validated vs `pipeline/regs/parsing/species.py`),
  and `date_windows()` (structured, validated from verbatim `dates` — hallucination guard).
- **`Extent`** adds `feature_types` (op=within only).
- **`Entry`** adds `locked` (human-freeze; re-parse must not overwrite).
- **Matcher/overrides** live in **`pipeline/regs/matching/`** (`matcher.py` + `overrides.json`, migrated from
  the archive, `name_variants` dropped). Overrides are region/MU-scoped: `{norm_name: [{region, mus,
  item_ids|skip|alias_of}]}`.

## Session structural cleanup (2026-08-15, no logic changes)

- `pipeline/common/models.py` → **`pipeline/common/models/` package** (enums · names · chains · graph · splits ·
  sections · regs · registry), re-exported from `__init__` so all imports are unchanged.
- `pipeline/regs/parsing/` prompts/docs → `pipeline/regs/parsing/prompts/`.
- **Perf:** border uses a vectorized `covered_by` prefilter; area membership uses one STRtree batch
  (`mark_inside_areas`); build cuts-then-marks-once. (Border stage still gated on the `bc_outline`
  WMU-union cost — flagged for a follow-up: STRtree tiling or a simplified/cached province outline.)

---

## PLAN 2026-08-16 — the MATCHER + RESOLVER flow (nailed down)

This section is the authoritative flow for **Goal 1** (parser gets, per entry, its registry item(s) +
their splits menu) and **Goal 2** (every entry resolves to the exact sections/polygons it governs). It
supersedes the scattered "layer 5 / match" notes above where they conflict.

### The one principle (removes the "double matching" confusion)

There is **ONE match**: `reg identity (name, region, mus) → registry item(s)`. It runs once, its result
is frozen on the entry as `matched: [registry_id]`, and it is used at two TIMES:
- **pre-parse** — to hand the parser the matched item(s) + their **boundary menu** (splits) so it can
  bind rule `extents`;
- **resolve** — the frozen `matched` + the parsed rules → sections.

The matcher only ever picks the **item(s)**. *How much* of an item a rule covers (whole / a reach /
tributaries / inlet-outlet / within an area) is **SCOPE**, applied by the **resolver** via `Op`s —
never by the matcher. This is what keeps the matcher simple.

### Layer A — Registry gains `ref_ids` (the id bridge) — ✅ IMPLEMENTED 2026-08-16

Each `RegistryItem` records **every FWA id it answers to** (`registry/build.py:_ref_ids`). **Streams**
answer to `gnis` (own + name-tuple) + trimmed `wsc` + `blk`. **Lakes/wetlands answer to `gnis` + `wbk`
ONLY** — a lake node also carries the through-river's `wsc`/`blk`, and emitting those would let a
*stream* override's `wsc`/`blk` pin false-match the lake (the 2026-08-16 fix). E.g. the Ballon lake item
answers to `{wbk:329480864, gnis:18257}` (gnis present because lake nodes carry it — the 2026-08-15
lake-gnis fix), **not** its through-river wsc. The matcher builds `id_index: ref_id → item_id` once
(`build_id_index`). **This is the whole "override re-resolution" story** — a curated `gnis:18257` pin
resolves to the lake item by index lookup; no data rewrite, no fuzzy matching. Validation: 417/431
typed-id overrides fully resolve; the dead tail is nameless features + version-drift (below).

### Layer B — Override format = the archive schema — ✅ IMPLEMENTED 2026-08-16

`pipeline/regs/matching/overrides.json` is the **archive file verbatim** (a LIST of 480 override objects):
`{type, criteria: {name_verbatim, region, mus}, note, skip, skip_reason, variant_of, gnis_ids,
waterbody_keys, fwa_watershed_codes, blue_line_keys, linear_feature_ids, waterbody_poly_ids,
admin_targets, admin_feature_types, only_within_zones, ungazetted_waterbody_id/location}`. The archive is
the **origin** of the old flattened dict file — every one of that file's 65 "extra" names traced back to
an archive entry (base-name re-keys + gnis-shared duplicates the migration generated), so nothing was
hand-curated after migration and there is nothing to merge. The lossy flattened dict file is retired.
`name_variants` is NOT here (it names features in the graph — see below).

**Overrides vs name_variants (division of labour).** An override pins an *identity→ids* decision
(region/MU disambiguation, skip, a curated id). Getting a *nameless* FWA feature to carry a name is
`name_variants`' job (it attaches names to graph nodes at build). So a reg's water gets a registry item
three ways, in order: (1) FWA already names it → node/layer item; (2) it's a named FWA lake/wetland with
no through-stream → added from the layer (`add_waterbody_items`, e.g. Frazer Lake); (3) it's an in-graph
nameless feature (oxbow side-channels) → a `name_variants` entry names it → it becomes an item. Overrides
then only carry disambiguation/skip/curation, not basic resolution.

### Layer C — the matcher: override → name, never guesses — ✅ IMPLEMENTED 2026-08-16

`match_row(...) -> MatchResult{ item_id, status, via, also, unresolved_ids, admin_targets, … }`

1. **Override** (keyed by `norm(name_verbatim)`, scoped by region + `mus`):
   - `skip` → status `skip` (carry `skip_reason`; note `variant_of` if present).
   - `variant_of` without skip → resolve the curator's **corrected** name by name lookup (`override_alias`).
   - typed ids (`gnis_ids`/`waterbody_keys`/`fwa_watershed_codes`/`blue_line_keys`) → each through
     `id_index`. Any that hit a NAMED item → status `override` (multi = combined via `also`). Ids that
     hit nothing are carried in `unresolved_ids`.
   - If **none** of the typed ids / `admin_targets` land a named item → status **`feature_pin`**
     (`via=override_feature`): the curated ids describe **nameless features** (oxbow channels by wsc,
     unnamed lakes by wbk), admin/area zones, or poly/fid pins → **deferred to the resolver** (Goal 2),
     which has the full graph and binds them to sections directly. This is *not* a name fallback — the
     matcher never guesses by name when an override pinned specific ids. The resolver fails loud if a
     pinned id is truly absent (version-drift), distinguishing that from nameless-by-design.
2. **Name auto-match** (no override): `norm(name)` → `name_index` → disambiguate by `item.mus ∩ row.mus`
   (region fallback) → **unique → matched**. Multi-hit → `ambiguous` (curate). None → `unmatched`.

> **Why `feature_pin` replaced `override_dead`:** the old rule failed loud on *any* unresolved pin, but
> the biggest "dead" cluster (Okanagan-oxbows: 26 wsc for nameless side-channels) is **correct curation**
> of features that by design have no named registry item. Those belong to the resolver, not a matcher
> error. The safety property is unchanged: **an override never silently falls back to name matching.**

**Areas are override-only** (the matcher never name-auto-matches an `area:` item). **Named wetlands**
(marshes/ponds/sloughs) are now `kind="wetland"` registry items (wbk-keyed) so a reg can target them by
name or a curated `wbk`/`gnis` pin — they are NOT graph nodes (a stream overlays them), added at build
via `add_wetland_items`.

### Layer D — how AREA / LAKE / TRIBUTARY / INLET-OUTLET regs are treated

The matcher picks the item; the **resolver** applies scope. Table of the tricky kinds:

| reg says | matcher picks | scope (entry/rule) | resolver yields |
|---|---|---|---|
| "Ballon Lake" | `wbk:` lake item | `whole` | the lake section(s) |
| "X Lake's tributaries" | the lake item | `tributaries.only=true` | `lake_tributaries(lake)` (drops through-mainstem) |
| "X Lake inlet & outlet streams" | the lake item | rule `op=inlet_outlet` *(NEW op)* | `lake_inlets ∪ lake_outlets` |
| "River, incl. tributaries" | the stream item | `tributaries.included=true` | item sections + ancestor closure |
| "River between A and B" | the stream item | `between(a,b)` | that reach |
| "all waters in Creston Valley WMA" | `area:` item (override `admin_targets`) | `within(area, feature_types)` | every section INTERSECTING the polygon, filtered by `feature_types` (stream/lake). "inside at all" = intersects |
| "all lakes in Kikomun Park" | `area:` item | `within(area, feature_types=[lake])` | intersecting sections where `is_lake` |

The parser sets `tributaries.only` / `.included` from the "tributaries"/"watershed" wording; `within`
comes from an override's `admin_targets`. So **"X Lake's tributaries" is NOT an unmatched row** — it
matches the LAKE, with a trib scope. (`inlet_outlet` is the one new `Op` to add; the tributary and
lake-inlet/outlet mechanisms already exist and are tested in `pipeline/atlas/graph/tributaries.py`.)

### Layer E — the parser context (Goal 1): the splits menu per entry

Pre-parse, for each row: matcher → item(s); then emit the parse unit:
```jsonc
{ "identity": {name, region, mus}, "regs_verbatim": "…",
  "matched": [ { "id": "gnis:chemainus", "name": "Chemainus River", "kind": "stream",
                 "boundaries": [ {id, label, kind}, … ],   // curated splits + lake edges + outlet/headwaters
                 "is_lake": false } ],
  "ops": ["whole","upstream_of","downstream_of","between","within","inlet_outlet"] }
```
- **stream** → its boundary list is the menu the parser binds `extents` against.
- **lake** → the lake + its inlet/outlet boundary refs (for "inlet streams") + through-river.
- **area** → the area item; rules use `within(area, feature_types)` (no boundary menu).

Binding a rule is a **constrained selection** among provided boundary ids / ops — robust and cheap.

### Layer F — locking the frozen parse (re-parse must never clobber curation)

Two guards, belt-and-suspenders:
- **Per-entry** `Entry.locked: bool` (already in the model) — the merge tool applies a re-parse only to
  **unlocked** entries; a locked entry's changes are reported for manual review, never auto-written.
- **Per-file lock** — a sidecar `pipeline/regs/parsing/entries/region-N.json.lock` (presence = locked). The
  ingest/writer **refuses to overwrite** a locked region file at all; only the explicit merge tool may,
  and only into unlocked entries. This makes "the parse is a one-time frozen artifact" enforceable, not
  just conventional. (A `parsing/entries/LOCKED` manifest listing locked regions is the equivalent.)

### Updated data flow

```
FWA + splits.json + name_variants ─► GRAPH BUILD ─► sections + REGISTRY
                                                     items{ id, name, variants, kind,
                                                            ref_ids{gnis,wbk,wsc,blk}, boundaries, mus }
raw regs (rows) ──► MATCHER ────────────────────────► row.matched:[registry_id] (+area/scope)
   (id_index + name_index over registry; overrides = archive schema; areas override-only)
                          │
              pre-parse:  └─► per-entry SPLITS MENU (matched item(s)'s boundaries) ─► PARSER (Claude Code)
                                                                                         │
                                                          FROZEN entries (rules: op+split refs, scope,
                                                          species, dates)  ◄── LOCK (Entry.locked + region .lock)
                                                                                         │
entries + registry + sections ──► RESOLVER ──► SectionRegs (+ CoverageReport)
   per entry: matched items → their sections (+ derived tribs / within-area membership / inlet-outlet),
   each rule's extents select sections by Op. matched is FROZEN; matcher re-runs only for new/unlocked rows.
```

### Build order (dependency-sorted)

1. **Registry `ref_ids`** (Layer A) — small addition to `RegistryItem` + `build_registry`.
2. **Override schema swap** (Layer B) — adopt archive shape; matcher reads typed ids via `id_index`.
3. **Matcher rewrite** (Layer C/D) — three tiers, id+name index, scope-agnostic; coverage over it.
4. **Parser context** (Layer E) — batch_exporter emits the per-entry boundary menu (already close).
5. **Lock** (Layer F) — `.lock` file honoured by ingest + the merge tool.
6. **Resolver** (Goal 2) — `Entry × registry × sections → SectionRegs`; area membership at resolve time
   (folds in the **lazy area-catalog** — the area catalog is *part of the resolver*, not a separate step).
7. **`inlet_outlet` Op** + precedence rule + merge tool.

### Area kinds — one `within(area)` op, composed three ways (2026-08-16)

The key realization: there is **one** area op, `within(area, feature_types)`; the "different area types"
are just **what the entry matched**, and the resolver **composes** by intersection. No separate
mechanisms, no manual tributary enumeration.

| kind | example | entry.matched | scope | resolver yields |
|---|---|---|---|---|
| **blanket** | "no fishing, any stream in X Ecological Reserve" | `[area:reserve:x]` (override `admin_targets`) | `within(area)` | **all** sections intersecting the polygon (`feature_types` filter) |
| **system-scoped** | "no fishing in Garibaldi Park" *for this river + tribs* | `[gnis:river]` + `tributaries.included` | rule `within(area:park:garibaldi)` | (river sections + **tributary ancestor closure**) **∩** the park polygon |
| **watershed** | "Liard River watershed" | `[gnis:liard]` + `tributaries.included` | `whole` | the river + its full tributary closure — **NO area polygon needed** |

Why this works:
- **The resolver's base set for an entry = its matched items' sections (+ tributary ancestor closure if
  `tributaries.included`).** Each rule extent then *filters* that base. `within(area)` filters the base
  to sections intersecting the area polygon.
  - blanket: matched IS the area → base = the area's members → `within` is the identity.
  - **Garibaldi: matched is the WATER → base = river+tribs → `within(park)` = river+tribs ∩ park.** The
    tributary sections inside the park fall out of `ancestor-closure ∩ polygon` — **never enumerated by
    hand.** A curator only sets `tributaries.included` + the `within(area)` extent.
- **Cutting makes "inside the park" clean.** Garibaldi and reserves are `cut:true` areas: streams are cut
  at the boundary at BUILD (geometry), so an inside section is a first-class node and the intersection is
  exact (no half-in straddling section closed wholesale). Cutting stays at build; membership stays lazy.
- **Watersheds are NOT areas.** "Liard watershed" = the drainage = the river's tributary ancestor closure
  — expressed as `matched:[gnis:liard] + tributaries.included + whole`. No polygon, no lazy-membership
  over a giant polygon. (A topographic-watershed *polygon* is only needed if a reg ever scopes by the
  drainage boundary rather than the network — none seen; revisit if one appears.)

**So lazy membership is viable everywhere:** the only areas that need a polygon are blanket + system-scoped
closures (parks/reserves/WMAs), all `cut:true`, all in the catalog; membership is computed at resolve only
for the few an entry references. Watersheds sidestep areas entirely. This resolves D2 (see below).

### Matcher rewrite — detailed plan (2026-08-16)

The current matcher hits only ~81%. That is **not** a matching-logic failure — the archive matched ~all
rows because its curated overrides pinned ids that existed then. Our lossy migration broke those ids, so
the fix is to restore them, not to invent new matching.

**Four concrete flaws → fixes:**
1. **Reads the flat/lossy overrides.** → Adopt the **archive schema** natively (`criteria`, typed id
   fields, `skip_reason`, `variant_of`, notes). Retire the flattened file.
2. **No id resolution.** → `RegistryItem` exposes **`ref_ids`** = every `gnis`/`wbk`/`wsc`/`blk` id its
   member nodes carry (lakes now carry gnis). The matcher builds `id_index: ref_id → item_id` once.
3. **Format drift** kills valid pins. → Normalize at override-load: `wsc` trailing-zero trim
   (`930-…-000000…` → `wsc:930-…`), `waterbody_poly_id → wbk`, `linear_feature_id → blk`.
4. **Silent drops.** → Pre-resolve every override id at LOAD; anything still unresolved is surfaced as a
   loud curation signal (never a name fallback, never dropped).

**Algorithm (one function, three tiers):**
`match(identity{name_verbatim, region, mus}) → {items:[registry_id], status, via, note}`
1. **Override** (by `norm(name_verbatim)` + region∩mu): `skip`→skip(+reason); `variant_of`→canonical's
   ids; typed ids→`id_index` (pre-resolved); `admin_targets`→area id(s). Dead id → loud.
2. **Name auto** (no override): `norm(name)`→`name_index`→candidates→MU-disambiguate→**unique**→matched;
   multi→ambiguous (curate).
3. **Unmatched** → coverage.
Areas are override-only (never name-auto-matched). Scope (whole/tribs/within/…) is the resolver's, never
the matcher's.

**Validation FIRST (before the full rewrite).** Build `ref_ids` + `id_index` (Layer A), load the
**archive** overrides through it, run `coverage`. This shows the *real* number empirically. Expectation:
~archive coverage (~98–100%). If it lands there → proceed to the full rewrite; if not → the coverage
table names exactly which typed ids still don't resolve, and we plan from data. Order: **A (ref_ids) →
validate → B (archive overrides) → C (matcher) → coverage**.

### Reach-variant as a split-source — worked example (Rainbow Alley)

A reach-scoped name-variant needs its reach to be a real section. Data flow, using the real case:
`{target:{blks:["360886970"]}, reach:{from_m:108816, to_m:111116}, names:[{name:"Rainbow Alley"}]}`
(the Babine River reach between Babine Lake and Nilkitkwa Lake).

- **Aligned case (today):** Babine Lake (upstream) and Nilkitkwa Lake (downstream) are lake nodes, so
  their edges already cut BLK 360886970 at ≈108816 and ≈111116. The piece `[108816,111116]` already
  exists → the name attaches to exactly that reach. No cut, no warning.
- **Unaligned case (the concern):** suppose "Foo Reach" is `blk X reach [5000,8000]` but BLK X is one
  piece `[0,20000]` (no split there). Today the name would paint the whole `[0,20000]` piece → **warning**.
- **The clean fix (snap-or-cut in the SPLIT stage, not the name pass):**
  1. The reach bounds `5000, 8000` are emitted as **SplitPoints into the split stage** (alongside
     `splits.json`), *before* border/area/name.
  2. `split_graph_at` applies them with **proximity-pickup**: `5000` — if an existing boundary (lake
     edge / curated split) sits within tolerance, **snap** (relabel it, no duplicate cut); else **cut**
     a new section. Same for `8000`.
  3. BLK X becomes `[0,5000],[5000,8000],[8000,20000]`; the middle is a discrete piece.
  4. The **name pass** then attaches "Foo Reach" to that exact piece.

Why this is safe: **cutting stays in the split stage** (where border/area also run, so a newly-cut piece
is still flagged), and **names stay in the name pass** — no "names create geometry after the geometry
passes" coupling. Snap-or-cut ("try existing splits first, else add one within a tolerance") falls out of
the split stage's existing `_pickup` logic. *Status: designed; today we WARN (reach-variants are rare, 0
misaligned). Implement the split-source path when a real misalignment appears.*

### Decisions (2026-08-16)

- **D1 — reach-scoped names sharing a parent gnis (Sicamous Narrows) → RESOLVED: parent item + reach
  scope.** Keep gnis-first grouping; a reach-named variant stays a named boundary within the parent
  item; matching its name resolves to the parent, scoped to that reach. (McArthur Slough has no gnis →
  already its own `blk:` item.)
- **D2 — area membership: lazy vs eager → LAZY (pending user confirm).** The Garibaldi concern is handled
  by *composition* (`within(park)` ∩ matched water+tribs) + *cut-at-build*, and watersheds sidestep areas
  (matched river + tribs). So lazy membership (catalog of polygons; membership computed at resolve for the
  few referenced areas) works for every case without manual tributary enumeration. Retire the eager
  `in_areas` pass; fold membership into the resolver.
- **D3 — override format → RESOLVED: adopt the archive schema wholesale** (Layer B); retire the flat
  migrated file.
- **D4 — lock mechanism → RESOLVED: per-entry `Entry.locked` only** (no per-region `.lock` file). The
  merge tool honours `locked` and never overwrites a locked entry; discipline over a file-level guard.
