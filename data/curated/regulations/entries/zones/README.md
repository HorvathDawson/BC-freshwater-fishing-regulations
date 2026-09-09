# Zone and area regulations

Regulations written against a **zone** rather than a water: a region's quotas, a park
closure, a bait ban over two management units, an advisory on Indigenous territory.

They are ordinary `Entry` objects, which is the point — they bind through the same resolver,
intern into the same rulesets and reach the app by the same path as a river row. Nothing in
this directory is a special case downstream.

    228 entries · 234 rules · 0 unresolved

Sources: converted from `archive/pipeline/enrichment/base_regulations.json` (235 archived
rules, 7 of them `disabled` and not carried across).

---

## How a zone rule reaches water

Every extent is `op: "within"`, and there are two ways to name the area:

```json
{ "op": "within", "area_id": "area:region:5" }                     ← one named area
{ "op": "within", "area_kind": "national_parks" }                  ← every area of a kind
{ "op": "within", "area_id": "area:region:5",
  "feature_types": ["stream"] }                                    ← narrowed to one kind
```

`area_kind` takes the union of every `area:<kind>:*`, so "prohibited in National Parks" is one
extent rather than seven ids that go stale when the province gazettes another park.

**`feature_types` is load-bearing.** A rule that says "no fishing in any stream" and omits it
binds every lake and wetland in the region too. That is not hypothetical — the first zone
rule authored did exactly this, closing 4,151 lakes and 2,712 wetlands in southern Vancouver
Island before the filter was implemented. `pipeline/tests/test_zone_entries.py` now fails on
the obvious phrasings, but it cannot catch every one.

The two municipal closures (Lost Lagoon, Beaver Lake) bind by **item** instead: `matched`
names the registry id and the extent is `{"op": "whole"}`.

---

## What is NOT yet right

### 1. Nine rules bind wider than the regulation says — `needs_review: true`

Each is flagged in the file with a `review_reason`. All have the same root cause: a
`within(area)` extent can only ADD water, so a regulation whose scope is an intersection
("the Fraser watershed **part of** Region 5") or a subtraction ("all streams **except** the
Peace") cannot be expressed, and is currently bound to the whole region.

| rule | binds | should bind |
|---|---|---|
| `z5:spring_stream_closure` | all Region 5 streams | Fraser + Thompson watershed ∩ Region 5, minus the Fraser mainstem |
| `z6:r6_steelhead_stream_closure` | all Region 6 streams | minus mainstem Skeena, Nass, Iskut, Stikine, Taku |
| `z7b:kokanee_closed_streams` | all Region 7B streams | minus the Peace River |
| `z7a:sturgeon_report_notice` | all Region 7A | Nechako + Upper Fraser populations |
| `z7b:site_c_tagging_notice` | all Region 7B | Site C Reservoir, Peace River and tributaries |
| `z3:steelhead_surcharge.r2` | all Region 3 | Classified Waters only |
| `z4:classified_waters_licence` | all Region 4 | Classified Waters only |
| `z2:steelhead_quota_stop_fishing` | — | classification only: archive said `Quota Enforcement`, recorded as `harvest` |
| `z2:protected_species` | — | classification only: archive said `Protected Species`, recorded as `closure` |

**The over-reach is in the safe direction for a closure and the unsafe direction for a
quota.** A closure that covers too much tells someone not to fish where they may; a quota
that covers too much tells them they may keep fish where they may not. `z7b:kokanee_closed_streams`
and `z5:spring_stream_closure` are closures. The Classified Waters pair are licensing rules,
which is the worse direction — they will tell an angler they need a licence endorsement on
water where none is required.

**What would fix them:** new area definitions in `data/curated/waters/areas.json`.

* a **watershed** area for the Fraser and Thompson (one exists for the Liard already —
  `area:watershed:liard_river` — so the mechanism is proven)
* a **Classified Waters** area, if the province publishes the boundaries
* per-water carve-outs, which are NOT an area problem: `Rule.tributary_excludes` and a
  `sections_override` already exist for naming specific waters out of a rule

### 2. Eighteen rules carry an `exception` that nothing subtracts

`grep '"exception"' region-*.json`. The carve-out text is preserved verbatim and is shown to
the reader, but no extent removes the excepted water — so the app will say "closed" and then
print "except the Peace River" underneath. Honest, but it is prose doing a job the extents
should do.

Most are small ("2 hatchery steelhead over 50 cm allowed", "except Koocanusa Reservoir").
Several say only "see tables for exceptions", which cannot be resolved at all until the
water-specific tables are cross-referenced — those are a **known permanent limitation**, not
a to-do.

### 3. Nine rules say "all B.C." but are filed per region

| text | filed in |
|---|---|
| "Annual catch quota for all B.C.: 10 steelhead per licence year" | regions 1, 2, 3, 4 |
| "Annual catch quota for all B.C.: 10 hatchery steelhead" | region 6 |
| Steelhead Conservation Surcharge Stamp | regions 3, 5, 6 (+ a Lower Mainland variant in 2) |

These are province-wide rules that the synopsis restates in each region's preamble, and the
archive kept that shape. Binding them per region is not WRONG — the union covers the province
— but it means a correction has to be made in four files, and the two steelhead annual-quota
texts disagree about whether the 10 fish must be hatchery.

**Decide:** move them to `region-provincial.json` as one rule each, or leave them mirroring
the source. If they move, the two conflicting steelhead texts have to be reconciled against
the printed synopsis first.

### 4. 108 of 234 rules share their text with another region

Not an error — the synopsis genuinely restates "Whitefish: 15" in eight regions — but worth
knowing that a change to one of those facts is a change in eight files.

### 5. Species phrases that did not resolve

| phrase | stored as | why it needs an eye |
|---|---|---|
| `Crappie` | `BCB` (Black Crappie) | the synopsis line is genus-level; the table has no general crappie |
| `Crayfish` | `CRA` (Signal Crayfish) | same — no general crayfish code |
| `game fish` | *(nothing)* | in `z7a:set_lining_lakes.r2`. Empty species reads as ALL species, which also sweeps in the burbot the rule explicitly exempts |

Everything else resolved through `resolve_species_phrase`. `Trout` is now the synthetic group
code `TRT` and `Trout and Char` is `["TRT","SLV"]` — see `pipeline/regs/parsing/species.py`.

---

## How to verify this yourself

### The automated checks — run these first

```bash
cd <repo root>

# structural: schema, chain of custody, unique ids, area ids exist, feature_types sanity
.venv/bin/python -m pytest pipeline/tests/test_zone_entries.py -v

# the whole pipeline suite
.venv/bin/python -m pytest pipeline/tests -q
```

⚠️ `test_every_named_area_id_exists_in_the_registry` **skips** unless the atlas build named by
`config.yaml`'s `atlas.default_build` has the region areas. It currently names `full`, which
predates them. Either rebuild `full` or point `default_build` at `full_regions` — until then
that check is not running, and a typo'd area id would go unnoticed.

### Bind them and read the outcome

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.reach.cli \
  --build data/generated/atlas/full_regions --out data/generated/reaches/zones
```

Expect `bound 3,196 · unresolved 88`. **The 88 are pre-existing synopsis failures, not zone
rules** — if that number moves, a zone rule stopped resolving.

### Check what a rule actually covers

The single most useful check, because a rule that binds is not a rule that binds *correctly*:

```bash
.venv/bin/python - <<'EOF'
import json, collections, pickle
RULE = "z5:spring_stream_closure"          # <- change this
secs = {json.loads(l)["section_id"] for l in
        open("data/generated/reaches/zones/rule_section.jsonl")
        if json.loads(l)["entry_id"] == RULE}
g = pickle.load(open("data/generated/atlas/full_regions/graph.pkl", "rb"))
n = g.nodes
kind = lambda s: str(getattr(getattr(n[s], "kind", None), "value", "?")).lower()
print(f"{len(secs):,} sections")
print(collections.Counter(kind(s) for s in secs if s in n))
names = collections.Counter((getattr(n[s], "display_name", "") or "(unnamed)")
                            for s in secs if s in n)
print("biggest waters:", names.most_common(12))
EOF
```

Two things to look for:

1. **Kinds.** A rule that says "in any stream" showing lakes or wetlands means `feature_types`
   is missing.
2. **Named waters.** If a rule naming the Fraser watershed lists the Bella Coola or the Dean,
   it has escaped its intended scope.

### What only you can check

The automated checks confirm the entries are well-formed and reach real water. They cannot
confirm the regulation is right. These need the printed synopsis open beside them:

* **The two steelhead annual quotas disagree** — 10 steelhead in regions 1–4, 10 *hatchery*
  steelhead in region 6. One of them is wrong.
* **Quantities live only in `details`** — "Daily quota 4 (all species combined)". There is no
  structured quantity field, so nothing validates that 4 against the synopsis. Spot-check the
  numbers.
* **Restriction types for the two classification flags** (`z2:steelhead_quota_stop_fishing`,
  `z2:protected_species`) were a judgement call, not a mapping.
* **Dates** parse cleanly, but a parsed window is not a correct window. `Jul 15 – Aug 31`
  reads the same whether or not the synopsis said Aug 30.

---

## Adding to this directory

A new file must be `region-<something>.json` — the loader globs `region-*.json`, which is
why the province-wide file is `region-provincial.json`.

`entry_id` must be unique across the WHOLE corpus, synopsis included. `read_all_entries`
refuses a collision at build time rather than namespacing it, because an id that changes
meaning between builds is the one thing `entry_id` may never do.

Read `region-provincial.json` first — it is the worked example for `area_kind`, item-scoped
closures and advisories.
