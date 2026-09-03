# 15 — Current state (2026-08-28)

Docs `00`–`12` describe the v2 design and were written before most of it existed. Two of them are now
actively misleading, so read this first:

- **Root `README.md`** describes `pipeline/atlas/`, `tiles/`, `enrichment/`, `deploy/`. None of those
  exist under `pipeline/` any more — they are v1 and live in `archive/pipeline/`. `python -m pipeline`
  no longer runs v1's `--step all`; it runs the v2 section build (`pipeline/atlas/build.py`).
- **`12-testing.md` → "What's next"** lists the match step as not built. Matching, LLM parsing, the
  registry and the whole curation-review app were built after it was written. Its "73 tests (62 pass,
  11 skip)" is now **275 passing, 8 skipped**.

## Where the work actually is

```
splits ─▶ graph+registry ─▶ parse (human) ─▶ curation review ─▶  ??? ─▶ client
  ✅            ✅               ✅               🔄 7.8%        ❌      v1 only
```

| Stage | State |
|---|---|
| `pipeline/atlas/splits/` + `splits.json` | 391 curated split definitions; anchors incl. `mu_boundary`, `confluence`, `area_boundary` |
| `pipeline/atlas/build.py` → graph + registry | Whole province, ~18 min. **19,722 registry items** (12,000 stream, 7,672 lake, 46 wetland, 4 area) over **49,639 sections**; `registry.json` is 12 MB |
| `pipeline/atlas/graph/nests.py` | Braid-nest reduction: one route per water per destination it would otherwise lose (see its docstring for the rules that were tried and failed) |
| `pipeline/regs/parsing/` | 1,392 entries parsed from the synopsis. HUMAN-ONLY to run — spends credits |
| `curation-review/` | The review app. Reaches, splits editing, tributary carve-outs, rebuild |
| **bundle / tiles / deploy for v2** | **Does not exist.** No `deploy` or `bundle` path in `project_config.py` |
| `webapp/`, `mobile/` | Both consume **v1** artifacts (`tier0.json`, `regulations.sqlite`) |

## Curation progress — the gate

```
entries 1392   matched 1340 · no_registry 52 · locked/confirmed 108 (7.8%) · revisit 22
rules   3038   needs_review 305 · unresolved_locators 176 · no extents 107
```

The 107 extent-less rules are **not** a bulk-fixable batch:

- **95** sit on `no_registry` entries — 52 entries with no registry item to bind an extent to. The work
  is attaching an item, not authoring extents.
- **22** are on `reference_only` rows ("See Marble River regulations") which correctly carry no extent.
- Only **12** are on matched entries with a `location_text` to work from — those are real authoring.

## The gap that matters

V1 is what is live at canifishthis.ca: `tier0.json` + PMTiles → R2 → Worker → webapp, with the mobile
app reading a bundled `regulations.sqlite`. That chain is **archived and no longer buildable from
`pipeline/`**, and there is no `tier0.json` anywhere under `output/`.

V2 stops at the curation app. Nothing downstream consumes the section spine. Every improvement to the
graph — braid simplification, region splits, entry scopes — currently reaches no user.

Closing that is `16-bundle-and-clients.md`.

## Sizes worth knowing before designing anything

| Artifact | Size | Note |
|---|---|---|
| `registry.json` | 12 MB | 19,722 items / 49,639 sections |
| `pipeline/regs/parsing/entries/` | 4.2 MB | all 1,392 entries, all regions |
| `data/bc.pmtiles` | **4.1 GB** | every FWA stream in BC. The offline blocker |
| `data/bc_fisheries_data.gpkg` | 9.6 GB | source, never shipped |

Regulation data is small. Geometry is the whole problem.
