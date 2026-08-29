# 06 — Zone / MU regulations (revised model)

Supersedes the earlier "zone→reg_set map + render clip" idea. The model below is simpler,
de-bloats the UI, and keeps section geometry regulation-free and stable.

## The key split in the problem

There are **two kinds** of regulation, and they want **different delivery**:

1. **Named-waterbody-specific regs** — "Adams River, quota X", "Fraser River (Zone 2) …".
   These belong to a *specific* section. **→ per-section, one reg set per section.**
2. **Base / zone-wide / provincial regs** — "in Region 4 the general trout limit is …",
   "no bait in MU 4-5", provincial park closures. These are "rules for fishing *in this
   area*", not about one waterbody. **→ an MU overlay, resolved by location, not attached to
   every section.**

Today both are flattened onto every reach, which is the source of the UI bloat. Separating
them is the fix.

## MU is the granularity key (not region)

Regulations **include/exclude specific management units** (the current `base_reg_assigner`
already works at MU granularity: `_resolve_mu_set` = zone_ids→MUs + include_mu_ids −
exclude_mu_ids). So:
- **Key everything on MU.** Region/Zone is a *display grouping* of MUs, not the lookup key.
- The client resolves the user's current **MU** (point-in-polygon against the WMU tile
  layer) and pulls the MU overlay for it.

## Base regs → MU overlay (kills the bloat)

- Build one small table: `mu_id → base_reg_set` (zone-wide + provincial + MU
  include/exclude resolved). Provincial park closures resolve by park polygon → the MUs /
  sub-extents they cover.
- The client shows these as a **"General regulations for this area"** panel driven by the
  clicked/located MU — **once**, not repeated on every section.
- A section does **not** store base regs. It only references named-waterbody-specific regs.
- **Similkameen case solved:** a river crossing an MU boundary with no *specific* difference
  stays **one section**; its differing *base* regs simply come from the overlay for whichever
  MU you're in. No geometry fragmentation, no bloat.

## Fraser case → curated MU-boundary split (your idea)

When a **named waterbody genuinely has different specific regs per zone/MU** (Fraser
River — Zone 2 rules ≠ Zone 3 rules on the same water):
- Author a **curated split at the MU boundary** in `splits.json` (a `mu_boundary` anchor
  type, `04`). The anchor resolves to the route measure where the blue line crosses the MU
  polygon boundary.
- Result: two ordinary sections, each with **one** reg set, labelled by `location_identifier`
  (e.g. "in Zone 2" / "in Zone 3", or the neighbouring-bound description). Fraser becomes
  "a section like any other river" — exactly your framing.
- **No special zone machinery, no render-clip, no per-section zone map.** The split system
  already built for lakes/landmarks handles it.

## Why curated, not automatic (the one tension)

We could auto-split any waterbody wherever its specific regs differ by MU. **We deliberately
don't**, because that couples *section geometry* to *regulation data*: a new zone-specific
reg would silently change section boundaries and therefore `section_id`s, breaking stability
and the "geometry decoupled from regs" principle (`07`). Fraser-type cases are **rare and
known**, so curating a handful of `mu_boundary` splits is cheap and keeps geometry stable.
(If auto-detection is ever wanted, gate it behind a validation that flags the case for a
human to confirm into `splits.json` — never let it mutate geometry silently.)

## Edge cases

- A named-waterbody reg with `only_within_zones` / MU include-exclude where the waterbody is
  entirely inside those MUs → attaches to the whole section, no split needed.
- A lake spanning two MUs with different lake-specific regs → curated `mu_boundary` split on
  the lake (rare).
- Base reg that references a named waterbody exception ("except Foo Lake") → stays in the MU
  overlay; the section for Foo Lake carries its own specific reg which the client shows as
  the more-specific rule.

## Client behaviour summary

- Click a stream → `section_id` → its (single) specific reg set.
- Simultaneously resolve clicked **MU** → show the MU **base-reg overlay** panel.
- Two clean panels ("This water" + "General rules for this area") instead of one bloated
  merged list.

## Open questions for the group

- Q1: Exact label wording for `mu_boundary` splits — "in Zone 2" vs neighbour-bound phrasing
  ("downstream of the Zone 2/3 boundary"). Lean: use the zone name when the split IS a zone
  boundary.
- Q2: Should the MU overlay be shipped in the boot bootstrap (small) or fetched lazily on
  first click? Lean: lazy — it's only needed once a user interacts.
- Q3: Confirm from `base_reg_assigner.py` that all base regs reduce cleanly to `mu_id →
  reg_set` with no region-only regs that can't be expanded to MUs.
