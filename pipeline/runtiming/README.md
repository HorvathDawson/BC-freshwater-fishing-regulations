# Run timing

When each salmon and steelhead run enters fresh water, and on which reach.

Source: the Pacific Salmon Foundation's dataset #90, *Average Run Timing of Salmon and
Steelhead Conservation Units*, built from LGL Ltd., DFO and Pacific Salmon Commission data
and revised about once a year. Surfaced through the Pacific Salmon Explorer, which
server-renders every conservation unit into one page per indicator — so a fetch is three
GETs, not 463 × 3.

## The three-way split

Same shape as `pipeline/gauges`, and for the same reason: so "do not re-run the join on
every build" is a fact about the import graph rather than something to remember.

| | needs | runs | writes |
|---|---|---|---|
| `fetch/` | HTTP only | ~once a year | `data/source/runtiming/` |
| `generate/` | the atlas — 2.6 GB of stream geometry | by hand, after a fetch | `data/generated/runtiming/` |
| `consume/` | only the frozen index | every build | bundle tables |

`runs.py` sits at the top because it belongs to both halves: the record `generate` writes
and `consume` reads, plus the run-label derivation. `consume` **cannot** import `generate`.

```bash
# once a year
python -m pipeline.runtiming.fetch.explorer        # 3 pages -> data/source/runtiming/
python -m pipeline.runtiming.fetch.boundaries      # CU vector tiles -> cu_tiles/

# after a fetch, on a machine that can hold the atlas
python -m pipeline.runtiming.generate.extract      # -> runs.json          (463 runs)
python -m pipeline.runtiming.generate.index        # -> section_cu.csv     (~35 s)
python -m pipeline.runtiming.generate.passage      # -> section_cu_passage.csv (~23 s)
python -m pipeline.runtiming.generate.profiles     # -> profiles + both cubes
python -m pipeline.runtiming.generate.indicators   # -> indicators.json    (optional)

# every build
python -m pipeline.runtiming.consume.bundle        # -> bundle tables
```

## Data, by what it costs to lose

| tree | holds |
|---|---|
| `data/source/runtiming/` | the three pages verbatim + `cu_tiles/`. Re-downloadable. |
| `data/generated/runtiming/` | `runs.json`, `section_cu.csv`, profiles, `run_cube.bin`. Re-runnable. |
| `data/curated/runtiming/review.json` | **authored**: run labels and coverage gaps. Not recreatable. |

Paths are in `config.yaml` under `generated.runtiming` and `curated.runtiming.review`, and
resolved through `pipeline.common.curated` — never as a literal.

## A reach has runs, not a run

This is the thing the whole design turns on. **4.45 species on the average reach**, and a
species can have more than one run on the same water:

| | |
|---|---|
| Central Coast steelhead | winter (peak Mar 30) **and** summer (peak Oct 26) — 9,854 reaches |
| West Vancouver Island steelhead | winter (Jan 30) and summer (Jun 12) — 4,578 reaches |
| Lower Fraser steelhead | winter (Feb 1) and summer (Jul 24) — 1,770 reaches |
| Takla-Trembleur sockeye | Early Stuart (Jul 10) and Summer (Aug 13) — 867 reaches |

So there are two outputs, because the map and the sheet ask different questions:

- **`run_cube.bin`** — "how strong is steelhead here in pentad 45" → one byte, no join. The
  **maximum** over that species' runs, which is right for a *colour*: the question a colour
  answers is "is anything of this species running here now".
- **`profile_runs.json`** — "*which* steelhead" → the list, with labels and peaks. The cube
  cannot answer this and must not pretend to. A sheet that says "Steelhead: peak Aug 11"
  has silently deleted the winter fishery.

### Brood lines are not runs

Pink (even) and Pink (odd) never coexist in a year, so maxing them together lit **415**
profiles with a second pink run that cannot happen. The cube therefore has **seven slots**,
not six — `Pink (even)` and `Pink (odd)` are separate — and the map picks by the year the
date pill is showing (`slot_for_year`). Sockeye's river- and lake-type do *not* split: both
rear in the same season and one river really can carry both.

## Run labels

`derive_label` reads the CU's own name: 100 of 463 state their run
("Skeena Coastal **Winters**", "Adams-**Early Summer**", "Kalum-**Late**"). Every
multi-run **steelhead** case is separable this way, which is why
`review.json`'s `run_labels` is empty.

Where two runs of one species are *not* separable by label, they are almost always two
different **populations** sharing a reach — Wannock and Rivers Inlet chinook, Kwinageese
and Upper Nass sockeye — and the CU *name* is the distinguisher. Do not invent a label for
those.

## The index

| | |
|---|---|
| stream sections read | 1,232,825 |
| fall inside at least one CU | **759,474 (61.6%)** |
| flat section→CU rows | 3,911,446 |
| **distinct CU-sets (profiles)** | **581** |
| profile membership rows | 3,626 |
| `run_clim` rows | 24,017 |
| `run_cube.bin` (581 × 7 × 73) | 290 KB (11 KB gzipped) |

A section joins every CU whose polygon contains its **midpoint**. The flat join is 3.9M
rows, which is not a thing to put in a bundle — but reaches do not carry arbitrary CU sets,
a whole watershed shares one, so they dedupe to 581. Same observation `ruleset` /
`section_ruleset` is built on.

`section_cu.csv` carries a `relation` column, today always `midpoint`. That column is the
fix for `section_gauge`'s mistake: it froze a trust score with no statement of what produced
it, so representativeness became policy nobody could re-derive. An overlap-weighted pass can
add its own kind here without a schema change.

## Spawning and passage

The Explorer's method note settles what the curve is dated to:

> "We adjust run timing estimates depending on the sampling location so that the run timing
> curves shown in the Pacific Salmon Explorer represent **river entry timing** regardless of
> where the data were collected."

So the curve is dated at the river mouth, not the spawning grounds — which makes the
containment join in `index.py` *backwards about accuracy*. It paints a CU's spawning
watershed, often hundreds of river kilometres inland, with a date measured at the coast.

`generate/passage.py` walks the atlas flow graph downstream from every CU to tidewater and
records the reaches the run crosses:

| | |
|---|---|
| passage reaches | 94,098 |
| passage rows | 437,847 |
| passage profiles | 1,192 |
| `passage_cube.bin` | 595 KB (14 KB gzipped) |
| runtime | ~23 s |

They are **two independent profile spaces** and must render differently — solid for
spawning, dashed for passage. "Fish spawn here" and "fish swim past here" are different
answers, and only one is a place to look for a holding fish.

### What we have direct data for

| | |
|---|---|
| river entry timing | **yes** — that is what the curve is, and at tidewater it is exact |
| river kilometres | **yes** — `length_m` summed downstream, published as `km_to_sea` |
| travel time upriver | **no** — so **no date shift is applied** |

Applying a migration rate would turn a measured curve into a modelled one silently, and the
model would be ours. `km_to_sea` is published instead so a reader can judge how stale the
entry date is where they stand (0–288 km on the Skeena).

### Two guards, both found on the Skeena

**A downstream walk does not stop at the river mouth.** Below the Nass it reaches the tidal
reaches below the Skeena and carries on — crediting `Nass Summer` steelhead to 168 reaches
of a river they never enter. A passage reach must now be an **ancestor** in the FWA
watershed code (Skeena root `400`, Nass `500`): the same test the gauge shed makes, in the
mirror direction. 10,950 reaches refused.

**Passage amplifies polygon spill.** A CU simplified to ~200 m can clip a handful of reaches
over a divide; 13 stray Skeena sections on Nass Summer survived the code test (they really
are Skeena) and walked the run down the whole mainstem. Drainage roots holding under 1% of a
CU's sections are dropped before the walk — 2,531 sections across 186 units. The threshold
is safe because real multi-drainage CUs are coastal aggregates holding 1–50% in their
smallest root, two orders of magnitude above the spill.

## Coverage — not every water is on the Explorer

Sixteen well-known rivers against six species gave **13 species-gaps**, and they cluster:
Chilliwack/Vedder, Capilano, Squamish, Campbell and Quinsam **chinook and coho**. The
Explorer maps *wild* Conservation Units, so a heavily fished hatchery system can be blank
here while being one of the busiest bars in the province.

A blank must never render as "no fish". `run_gap` carries the stated reason.

**The documented case.** The Chilliwack/Vedder has chum, coho, pink, sockeye and
steelhead CUs but **no chinook CU** — the three Lower Fraser chinook CUs all stop at
49.22 N and the Chilliwack runs at 49.10 N. Recorded in `review.json` so the app can say
"no chinook timing here" rather than draw an empty layer.
