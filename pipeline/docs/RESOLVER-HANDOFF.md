# Resolver handoff — everything found, 2026-08-28

For whoever is working on `resolve_extent`. Findings only; nothing here has been fixed. Plan context:
[`13-build-plan.md`](13-build-plan.md) §6–7. **This is not exhaustive** — §6 lists what was *not*
looked at.

---

## 1. Where the code is

All in `curation-review/backend/reuse.py` (it has not been lifted into `pipeline/` yet — that is
step 1 of the plan):

| what | line |
|---|---|
| `resolve_extent(covered_ids, ex)` — the entry point | `reuse.py:615` |
| `_cut_at(refs, universe)` — split id → `(blk, measure, alternatives)` | `reuse.py:457` |
| `_by_measure(universe, blk, lo, hi)` — measure select + braid fixpoint | `reuse.py:493` |
| `_between_across_lines(universe, a, b)` — `between` spanning two blue lines | `reuse.py:587` |
| `_scope_sections(e, covered)` — the ENTRY-level scope clip | `reuse.py:755` |
| `entry_reaches(entry_id)` — per-rule driver | `reuse.py:783` |
| `_clip(got, clip)` — applies the entry scope | `reuse.py:802` |
| `_refs(bid)` — **the alias expansion (see the Mitchell bug)** | nested inside `resolve_extent` |
| tests | `pipeline/tests/test_review_reaches.py` (19 tests) |

Contract is documented in `curation-review/API.md` under `GET /api/entries/{entry_id}/reaches`.

**Design invariant worth preserving:** the resolver **never creates a section**. `_by_measure` only
filters pre-existing graph nodes by route measure. Splitting happens upstream in the sectionizer from
`pipeline/atlas/splits.json`. If the resolver could split, a regulation edit would silently change section
geometry — the coupling doc 10 explicitly forbids.

---

## 2. Baseline: it is in better shape than the issue list suggests

Ran today's resolver over **all 1,392 entries** (`output/v2/full`):

| rule outcome | count |
|---|---|
| **bound** (≥1 section) | **2,910** (95.8%) |
| `no_extents` | 107 |
| `all_extents_unresolved` | 18 |
| `resolved_but_EMPTY` | 3 |
| **total** | **3,038** |

**42 seconds total, 5 s of which is the graph load.** Entry scopes: 10/10 resolve. 0 exceptions.

Two conclusions: there is no performance problem, and **do not build parallelism** — it is not needed.

Other buckets, for context:

| | rules |
|---|---|
| straddling pieces (`unclassified`) | 40 (46 pieces) |
| ambiguous cuts (`ambiguous_cut` non-empty) | 63 |
| >1 extent on one rule | 9 |
| partially resolved (some extents null) | **0** |
| **tributaries on a BOUNDED extent** | **176** (79 `upstream_of`, 53 `downstream_of`, 44 `between`) |

---

## 3. THE ONE CONFIRMED RESOLVER BUG — Mitchell River

`gnis:29965 :: mitchell_river.r1` — `[closure] No fishing` — resolves to **zero sections**.

```
extent: between('mitchell_river__mitchell_lake', 'mitchell_river__within_100_m_upstream')
```

Registry boundaries on `gnis:29965`:

```
id='mitchell_river__mitchell_lake'          kind=lake  ref='lake:329480767'
    aliases=['split:mitchell_river__within_100_m_upstream']     <<<< both ids -> ONE boundary
id='mitchell_river__within_100_m_downstream' kind=split ref='split:mitchell_river__within_100_m_downstream'
id='mitchell_river__cameron_creek_into_mitchell_river' kind=confluence
id='mitchell_river__lake_329482627'          kind=lake  ref='lake:329482627'
```

Nodes on blk `356355331` — note the lake bounds the line at **two** measures:

```
356355331:32647   down=32647.2  up=42090.5   hi=split:…cameron_creek…
356355331:42090   down=42090.5  up=50921.5   hi=split:…within_100_m_downstream
356355331:50921   down=50921.5  up=50980.4   hi=lake:329480767     <-- river ENTERS the lake
356355331:67438   down=67438.3  up=81469.3   lo=lake:329480767     <-- river LEAVES the lake
```

**What happens:** `_refs('mitchell_river__within_100_m_upstream')` matches the lake boundary *via its
alias* and returns `{'lake:329480767', 'split:mitchell_river__within_100_m_upstream'}`.
`_cut_at` then collects **every** measure carrying any of those refs — `{50980.4, 67438.3}` — and
returns the **lowest**, 50980.4. The other split id resolves to the same boundary and therefore the
same measure. So:

```
_cut_at(A) -> ('356355331', 50980.44, [67438.25])
_cut_at(B) -> ('356355331', 50980.44, [67438.25])       # identical
between -> _by_measure(blk, min=50980.44, max=50980.44) -> {}   # empty range
```

**Root cause:** the alias records that two ids name the same boundary, but **not which end of it**.
A lake is one boundary occupying two positions on a blue line, and the alias mechanism flattens that.

`_cut_at` already surfaces the alternative in `ambiguous_cut`, so the information is present — the
resolver simply cannot tell which alias meant the inlet and which meant the outlet.

**Suggested direction (not implemented):** give an alias its own measure/end, so
`split:…within_100_m_upstream` resolves to 67438.3 while `mitchell_river__mitchell_lake` resolves to
50980.4. A `between` whose two cuts collapse to one measure while alternatives exist should be treated
as unresolved with a reason rather than silently returning empty.

**Related prior defect (already fixed, do not re-break):** a split landing inside a lake run exists
only as an alias on that lake's boundary — matching on `b.id` alone found nothing and stranded 46
extents. See the `_refs` docstring.

---

## 4. NOT resolver bugs — verified, and owned by curation

Do not "fix" these in code.

| case | finding |
|---|---|
| **Peace** `peace_river_hwy_29_bridge_to_site_c_dam.r5` | Resolves **correctly** to `359572348:1683398` (down 1683398.10, up 1684323.88 — exactly between the two cuts). The **entry scope** *"from Hwy 29 Bridge to the Site C dam"* then clips it away, because the rule describes a reach *above* the row it lives in. `_clip` returning empty here is the documented, intended signal. |
| **Fraser** `fraser_river_region3.r10` | `fraser_river__spuzzum_creek_into_fraser_river` **is not a boundary on `gnis:39325`**. `_cut_at` → `None`. The cut does not exist; it must be authored. |
| **Skeena** `skeena_river_mainstem_only.r1` | Same shape: `skeena_river__exchamsiks_river_into_skeena_river` absent. The other cut (`…kitsumkalum…`, kind `confluence`) resolves fine at 123961.24. |
| **South Thompson** `south_thompson_river.r3` | Correct. `gnis:8675` is a **single** section `356361760:0` (0 → 59966.2) whose upper bound *is* Little Shuswap Lake. `upstream_of` that cut selects nothing because nothing in the item is above it. The rule is a `note` — *"See Shuswap Lake regulations…"* — and should be `reference_only`. |

---

## 5. Upstream data problems that reach the resolver

### 5a. The 107 `no_extents` rules — 95 are unfixable here

- **95** belong to `no_registry` entries: no item exists, so no extent *can* be authored (issue ㊶).
- **12** belong to `matched` entries. **All 12 have `needs_review=True`; 11 have
  `unresolved_locators`.** The parser deliberately refused to guess:

  > *"no boundary or area for the wetlands; reach left unbound rather than applied to the whole river"*
  > *"neither end has a boundary in the menu; reach left unbound rather than guessed"*

  ⚠️ **Do NOT default these to `{op: "whole"}`.** They are real, specific locations with no boundary
  to bind to — a 500 m bait ban at Causeway Road, a 500 m radius at the Davis FSR Bridge, a sign line
  near the Nation River Bridge, the CPR bridge on Mara Lake, the Columbia wetlands. Defaulting would
  apply a 500 m closure to an entire lake arm. They need **curated splits authored**. Only
  `qualicum_river.r7` (a `note` about a wheelchair-accessible platform) has no locator at all.

### 5b. 309 zero-section registry items

263 lakes + **all 46 wetlands**. Wetlands are overlays by design (`StreamNode.member_wbks`), never
nodes. The 263 lakes are **isolated waters with no stream connection** (Hall Road Pond, Frazer Lake,
Kinglet Lake, Cheam Lake, Minnekhada Marsh…): FWA has the polygon so the registry mints a named,
matchable item, but the graph makes no node — so `op=whole` yields an empty universe and
`resolve_extent` returns `None`. **14 rules** hit this, several of them closures.

Fix belongs in the **graph build** (mint a node for an isolated lake), not the resolver. The resolver's
job is to return a *typed reason* (`no_sections_for_items`) instead of a bare `None`.

### 5c. `within(area)` — broken on both sides

```
pitt_river.r1     area_id='pitt_river__garibaldi_pitt'      registry has 'area:within_garibaldi_park'
wood_river_795.r1 area_id='within_hamber_provincial_park'   registry has 'area:hamber_prov_park_boundary'
```

The 4 `area:` registry items are: `area:hamber_prov_park_boundary`,
`area:pinnacles_park_upstream_boundary`, `area:tweedsmuir_park_upstream_boundary`,
`area:within_garibaldi_park`.

Neither authored id is an `area:` id — **but fixing the strings alone changes nothing**, because
`resolve_extent` returns `None` for `op == "within"` unconditionally and never reads `area_id`
(`reuse.py:641`). Both halves are required: correct the two ids (curation) **and** resolve `within`
against the area catalog (`output/v2/full/area_catalog.gpkg`).

---

## 6. What was NOT investigated — open leads

Be aware these are unexamined, not cleared:

1. **The 63 rules with `ambiguous_cut`.** `_cut_at` picks the **lowest** measure and reports the rest.
   Nobody has checked whether the lowest is the *right* reading in those 63 cases. The Mitchell bug is
   proof that the lowest can be flat wrong.
2. **The 40 rules with straddling pieces (46 pieces).** Surfaced but never *decided* — a straddler is
   currently neither shipped nor dropped. Likely rule-type dependent (include for a closure, exclude
   for an opening). Issue ㊴.
3. **Determinism.** `_by_measure` iterates a `set` in its fixpoint loop and `_cut_at` tie-breaks on
   `(length, blk)`. The result *should* be order-independent, but this has never been proven. Byte-
   identical builds depend on it. Cheap test: resolve everything N times, compare.
4. **The 9 multi-extent rules.** Union semantics are assumed, never specified or tested.
5. **`sections_override`** — 0 uses, and `entry_reaches` ignores it entirely (issue ㉙).
6. **Tributaries on bounded extents — 176 of them.** The reach-scoped tributary walk **does not
   exist**; `item_tributaries` is one-level and item-scoped, and is not part of resolution. "Between A
   and B, including tributaries" is unimplemented. This is the largest genuinely-missing piece.
7. **`_between_across_lines`** was not exercised against real data here — only the 4 failures above
   were traced, and none took that path.

---

## 7. Reproducing any of this

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0,"."); sys.path.insert(0,"curation-review/backend")
import reuse
reg, g = reuse._registry(), reuse._graph()          # graph load ~5s, ~0.7 GB
print(reuse.entry_reaches("gnis:29965"))            # the Mitchell case
EOF
```

`reuse._all_entries()` yields `(region, entry_dict)`. `reuse._match_and_item(e)` and
`reuse._covered_ids(e, mr)` give the item scope a rule resolves against.
