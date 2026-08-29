# Next up

---

## 0. The active build — `output/v2/full` (adopted 2026-08-29)

**Section 0's old warning about `full_named` is resolved and gone.** That build was never
adopted; it has been deleted along with `full_v4`. The blocker it described — 163 new
fine-grained `area:` items *replacing* the 4 legacy ones, which would have dangled
`pitt_river.r1` — was fixed in `pipeline/registry/build.py` by preserving an already-`area:`
prefixed id instead of re-slugging it:

```python
aid = area if area.startswith("area:") else f"area:{_slug(area)}"
```

So the adopted build has **167** area items = 4 legacy scoped + 163 blanket. Garibaldi
survives. **Any build made before that change has 163 and will dangle** — that is the real
discriminator, not the build's name.

### What the active build measures

| | build 19 (retired) | active (`full`) |
|---|---|---|
| registry items | 19,722 | 19,861 |
| `area:` items | 4 | **167** |
| lake/wetland items with **zero sections** | **309** | **0** |
| wetland items | 46 | 58 |
| rules bound | — | **2,925 / 3,037 (96.31%)** |
| `no_sections_for_items` | 14 rules | **0** |
| `cut_not_on_this_water` | 371 | **0** |

Unresolved 112 = `no_registry` 93 · `no_extents` 16 · `empty_after_scope` 1 ·
`cuts_collapsed` 1 · `area_id_dangling` 1.

### Why 309 items gained sections

`pipeline/graph/names.py::mint_waterbody_nodes` now mints an **edgeless node** for any named
waterbody no stream runs through. Two distinct causes had one symptom (empty `section_ids`):

* **isolated lakes** — no stream connection at all, so the graph never noded them;
* **overlaid wetlands** — a stream passes *through* the polygon, recorded only in
  `StreamNode.member_wbks`, so the wetland itself was never a node.

Minted nodes carry no geometry; the client draws the FWA polygon by `wbk`.

### ⚠️ Reading a registry diff: 24 `gnis:` items "disappeared"

Streams went 12,000 → 11,976. **This is a fix, not a loss.** All 24 were *lakes* mis-kinded
as `stream` (Pitt Lake, Sooke Lake, Tahltan Lake, Green Lake, …) because no lake node
existed, so the registry fell back to a `gnis:` stream item. Each is now a proper `wbk:`
lake item under the same name. Verified: no name was lost, and **no entry file references
any of the 24 old ids**. A further 12 items flipped `lake` → `wetland` (curated-wetland
kinding). Do not "restore" these.

### Which build the review app serves

`config.yaml` → `output.review_build` (currently `"full"`). Both `reuse.py` and the app's
rebuild button read `project_config.review_build_dir`, so **repointing the app at a newer
build is a config edit, not a code change.** The rebuild button writes into the same
directory it serves — for a build you want to verify first, build to a staging dir and swap.

⚠️ **The rebuild button is destructive while it runs.** `export_graph_gpkg` starts with
`p.unlink()`, so for the ~6 minutes it spends writing, `graph.gpkg` does not exist and every
map request in the review app fails. `rebuild.py` does call `reuse.invalidate_caches()` on
completion, so stale artifacts are not a problem — but the in-flight window is. For anything
you want to verify before adopting:

```bash
.venv/bin/python -m pipeline.build --full --out output/v2/_full_staging \
    --splits pipeline/splits.json
# verify, then:  rm -rf full_prev_bak && mv full full_prev_bak && mv _full_staging full
```

Worth making the button do this staging-and-swap itself; it is the only reason not to press it
mid-session.

---

## 1. PMTiles — what does it actually need?

**Question asked: "all it needs is the graph + registry, done?"**

**Almost — geometry is the missing input, and it is not in either of them.**

`StreamNode` stores no geometry on purpose ("the graph is pure topology + attributes… a
geometry-free consumer never pays for shapely"). Geometry lives in a sidecar:

| input | where | size | carries |
|---|---|---|---|
| `graph.pkl` | build dir | 647 MB | topology, bounds, names, `out_of_bc` — **no geometry** |
| `registry.json` | build dir | 13.6 MB | item → section ids, boundaries, variants |
| **`geometries.pkl`** | build dir | **2.07 GB** | **the actual lines, by `node_id`** |
| `graph.gpkg` | build dir | 3.77 GB | the same, queryable — what `reuse` reads for the map |

So: **graph + registry + geometries**. All three exist in `output/v2/full`.

### It does NOT need the reach builder

Worth being explicit, because it changes the ordering. Tiles carry **identity, not
regulation** — `section_id` plus the packager's dense id — and highlighting is done with
MapLibre `feature-state` at runtime. That is what deletes v1's fid→reach chain (`tier0.json`
was 24.9 MB, **16 MB of it `fids[]` highlight lists**).

**Consequence: tiles can be built now, in parallel with everything else.** They depend only
on a completed build, not on curation, not on resolution, not on the bundle schema. The one
hard rule is the manifest pins tiles and data together and the client refuses a mixed pair.

### Two artifacts, one blocker

1. **Regulable water** — measured: **180.5 MB of WKB out of 2,180.7 MB (8.3%)**. Through
   tippecanoe at v1's settings that is **~15–30 MB**: shippable offline. Not the problem.
2. **The basemap** — this is the blocker. `data/bc.pmtiles` is the **Protomaps basemap**
   (z0–15, `buildings/pois/landuse/…`), not BC's water; doc 10 ⑲'s original framing of this
   was simply wrong. Needs a stripped extract: **earth / water / roads / places, z4–12**.
3. **Bathymetry contours** (`archive/…/bathymetry_polygons.gpkg`, 2,744 polygons, 17.3 MB)
   are a third tile layer and are bundleable. The 2,685 scanned **map sheets (738 MB)** are
   never bundled — web links out, mobile fetches per lake on demand.

### Open questions before starting

- Zoom range and minzoom-per-`stream_order` (v1 assigned minzooms; is that still wanted?).
- Do `out_of_bc` pieces ship? They are kept in the graph for dotted display but BC regs do
  not apply — they must not be tappable as regulated water.
- **Minted waterbody nodes have no geometry in `geometries.pkl`** — the packager must pull
  their polygon from FWA by `wbk` or they silently vanish from the tiles. 309 of them.
- Lake sections are polygons (`lake:{wbk}`), streams are lines. Same layer or two?
- Which properties ride along: `section_id` and dense id certainly; `display_name` and
  `stream_order` are useful for labels and line weight but cost bytes on 49.5k features.

---

## 2. Entries with empty `matched` — two different problems, don't conflate them

Two **disjoint** problems get confused with each other. 52 entries have `matched: []` (all of
them `noreg_*`); separately, 7 entries have a *non-empty but incomplete* `matched`. Only the
second group is a migration.

### 2a. 7 entries with INCOMPLETE `matched` — `backfill_matched` fixes these, human must run it

These are **combined** rows whose stored `matched` is missing its `also_item_ids`. Dry run
(2026-08-29, against the active registry) says it would stamp 7:

```
wbk:329101302                                     -> wbk:329101302, wbk:329101330
gnis:14097#kootenay_river_downstream_of_idaho_border -> gnis:14097, gnis:39068, gnis:2123
wbk:328974978                                     -> wbk:328974978, wbk:329262641
wbk:329262668                                     -> wbk:329262668, wbk:329262653
wbk:329523072                                     -> wbk:329523072, wbk:329523070
wbk:329524023#norbury_garbutt_lake                -> wbk:329524023, wbk:329524047
wbk:329524100                                     -> wbk:329524100, wbk:328989162
```

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.backfill_matched \
    --registry output/v2/full/registry.json
```

It replays `batch_exporter` locally — **no credits, no `claude` CLI** — and touches only
`matched`, so locked entries keep their curated content. It is nonetheless blocked by the
agent auto-mode classifier (the `pipeline.parsing.*` path matches the parser-run guard), so
**a human runs this one.**

### 2b. 52 `noreg_*` entries with EMPTY `matched` — `backfill_matched` cannot fix these

`backfill_matched` keys on `entry_id`, and **`entry_id` encodes `item_id`**. A `noreg_*`
entry that now matches an item would export under a *different* entry id, so the backfill
never finds it. This is structural, not a bug.

Two of them now resolve against the active build, both unambiguously **by MU overlap**:

| entry | → item | how |
|---|---|---|
| `noreg_green_lake_871` (MU 5-1, unlocked) | `wbk:329170743` Green Lake, MU 5-1 | MU overlap, `also: ()` |
| `noreg_pitt_lake_347` (MU 2-8, unlocked) | `wbk:329291806` Pitt Lake, MU 2-8 | MU overlap, `also: ()` |

Both exist *because* minting created their lake node — they are the payoff of §0. Nothing is
broken today: `pipeline.reach.covered` falls back to a live re-match and the builder and the
review app agree exactly. But **attaching an item to a `no_registry` entry is a curation
decision, not a migration.** Do it through the review app's attach-item flow, or write a
tool that stamps from the live match — deliberately, not as a chore. Green Lake in
particular is a name BC reuses heavily; MU overlap is what makes these two safe, and that
will not hold for every future candidate.

---

## 2.5 Twenty-four exemptions are stated in prose only — small, high-value curation

Surfaced by a UI review asking whether `note` is a catch-all bin, then measured:

```
rules whose TEXT says "exempt"              83
  carrying a machine-readable exempts_from  59
  PROSE ONLY, invisible to the resolver     24
```

78 of the 83 are typed `note`, which is right — they lift a restriction rather than impose
one. The problem is the 24 that say so only in words:

```
gnis:21875  oyster_river.r1     "Exempt from summer closure"
gnis:23318  nitinat_river.r1    "Exempt from summer closure"
gnis:27060  puntledge_river.r1  "Exempt from summer closure"
gnis:27804  qualicum_river.r6   "Exempt from summer closure"
gnis:27883  quinsam_river.r3    "Exempt from summer closure"
```

**These fail in the dangerous direction.** An exemption the resolver cannot see means the
general closure stands, so the app reports a river CLOSED when the synopsis says it is
open — under-application on a closure, the mirror of ⑨ and just as wrong.

Note the cluster: every example needs `summer_closure`, one of the **five vocabulary codes
with zero uses**. The code exists; nobody stamped it. So this is not new machinery — it is
~24 rules to stamp during the curation pass, and it takes the vocabulary from 2 codes in
use to at least 3.

```python
import json, glob, re
for p in glob.glob("pipeline/parsing/entries/region-*.json"):
    for e in json.load(open(p))["entries"]:
        for r in e.get("rules") or []:
            txt = f"{r.get('details') or ''} {r.get('rule_text') or ''}"
            if re.search(r"\bexempt", txt, re.I) and not r.get("exempts_from"):
                print(e["entry_id"], r["rule_id"], (r.get("details") or "")[:60])
```

## 3. Tributary walk

555 rules are `tributaries_pending`; **176 of them sit on a BOUNDED extent** ("between A and
B, including tributaries"), which is the hard case. Reach-scoped, recursive, barrier-aware,
validated against a flow walk — never a watershed-code prefix (that over-includes the
Kootenay by 3,205 km). Design: `13-build-plan.md` step 8.

---

## 4. The four remaining unresolved rules (not counting `no_registry`/`no_extents`)

Small, specific, and each independently fixable.

### 4a. Mitchell River — `cuts_collapsed` (was "resolver bug", now authorable)

A lake boundary carrying a split id as an **alias** resolves both ids to the same measure,
collapsing `between` to nothing. Full trace: `RESOLVER-HANDOFF.md` §3.

**This no longer needs a resolver change.** `lake` anchors now honour `offset_m` /
`offset_dir` (`pipeline/splits/anchors.py`), so the intended cut can simply be authored:

```json
{"type": "lake", "wbk": 329480767, "offset_m": 100, "offset_dir": "upstream"}
```

Note the model validation that made this impossible was itself a bug — `offset_m` was
restricted to `point`/`confluence` and silently dropped 6 splits with only a *warning*.
`load_split_defs` now reports the real reason. **Read the skipped-split warning in the build
log**; it sits around line 40 and is easy to scroll past.

### 4b. Wood River — `area_id_dangling`

Entry says `within_hamber_provincial_park`; the registry has `area:hamber_prov_park_boundary`.
A one-line curation fix. (The sibling Pitt/Garibaldi case is already fixed — see §0.)

### 4c. `empty_after_scope` × 1

The entry scope clips the resolved reach to nothing. Per `RESOLVER-HANDOFF.md` §4 this is
sometimes the *correct, documented* signal (a rule describing water above the row it lives
in), so confirm intent before "fixing" it.

---

## 5. Unreviewed buckets — surfaced but never decided

Carried forward from `RESOLVER-HANDOFF.md` §6. These are **unexamined, not cleared.**

* **72 rules with `ambiguous_cut`.** `_cut_at` picks the **lowest** measure and reports the
  rest. Nobody has checked whether lowest is the right reading. Mitchell proves it can be
  flat wrong.
* **113 unclassified (straddling) pieces.** A straddler is currently neither shipped nor
  dropped. Likely rule-type dependent — include for a closure, exclude for an opening.
* **Determinism.** `_by_measure` iterates a `set`; `_cut_at` tie-breaks on `(length, blk)`.
  Byte-identical builds depend on order-independence that has never been proven. Cheap test:
  resolve N times, compare the digest (`pipeline.reach.cli` prints one).
* **9 multi-extent rules.** Union semantics assumed, never specified or tested.
* **`sections_override`** — 0 uses, and `entry_reaches` ignores it entirely.

---

## 6. The 73 locked-entry changes

Investigated and **benign**: Coldwater River went 1 → 12 sections, all named "Coldwater
River" on separate blue lines — the river's **side channels**, which the added-streams build
now includes. The registry got more complete, not wrong. Still worth a curator's eye, since
those rules were confirmed against a 1-section river.

---

## 7. Housekeeping notes

* **`output/v2` holds two builds only**: `full` (active) and `full_prev_bak` (rollback — the
  build adopted earlier the same day). `full_named`, `full_v4`, and the build-19 backup were
  deleted 2026-08-29. Each build is ~8.8 GB — budget for that before starting one.
* **`docs/waterbody-splits.json` is kept deliberately.** Its generator (`hack/build_splits.py`)
  was deleted as a v1 leftover, but the rows are the **provenance** for every curated split —
  the `datum=wbk (lake edge); offset {m,dir} authoritative; anchor coord is a cache` notes are
  what made it possible to find two double-offset anchors (Ash, Heber). Don't delete it
  because its generator is gone.
* **Two guards died with `build_splits.py`** and would need re-deriving if that workflow ever
  returns: (1) defer to an explicitly-named anchor type rather than re-anchoring it, and
  (2) never re-anchor a row that carries its own surveyed coordinate. Both were written
  after converting the Lardeau and Nahatlatch anchors *wrongly*.
* **JSON formatting is per-file and diffs explode if you get it wrong.** `pipeline/splits.json`
  is `indent=1`; entry files are `indent=2`; both are `ensure_ascii=False` with **no trailing
  newline**. Writing with the wrong settings produces a 45,000-line diff.
