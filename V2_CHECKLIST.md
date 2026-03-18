# V2 Verification Checklist

Quick-reference checklist for verifying V2 after a pipeline run + deploy. Each item has a test method.

---

## Prerequisites

    ```bash
    # 1. Run full pipeline (conda activate fish)
    python -m pipeline --step all

    # 2. Run unit tests (248 tests)
    pytest pipeline/tests/ -q

    # 3. Seed local R2 + start worker + Vite dev server
    node scripts/dev.mjs
    # (runs seed.mjs automatically, then wrangler dev :8787, then Vite :5173)

    # If you only need to re-seed after a pipeline re-run:
    node scripts/seed.mjs --force
    ```

---

## A. Pipeline Output Checks

Run these after `python -m pipeline --step all` completes.

- [ ] **tier0.json exists and is non-empty** — `ls -lh output/pipeline/deploy/tier0.json`
- [ ] **PMTiles generated** — `ls -lh output/pipeline/deploy/*.pmtiles`
- [ ] **Search index has entries** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('search_index',[])), 'search entries')"`
- [ ] **Regulations present** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('regulations',{})), 'regulations')"`
- [ ] **Reaches present** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('reaches',{})), 'reaches')"`

---

## B. Name & Display Checks

Open the site in browser. Use search bar + InfoPanel.

- [ ] **Named stream search** — Search "Adams River" → result appears → click → InfoPanel title = "Adams River"
- [ ] **Named lake search** — Search "Okanagan Lake" → result appears → click → correct title + lake icon
- [ ] **"Also known as" aliases** — Find a feature with name variants → InfoPanel shows "Also known as:" section
- [ ] **Tributary alias** — Find a tributary (e.g. a small creek) → "Also known as: Tributary of [Parent]" shows if inherited
- [ ] **Admin alias** — Feature in a park/eco reserve → "In [Park Name]" context line below aliases
- [ ] **Unnamed stream excluded from search** — Type random letters → no "Unnamed" entries in results
- [ ] **Display name consistency** — Same name in search result, disambiguation menu, InfoPanel title, and browser tab title

---

## C. Regulation Display

Click a feature and check the InfoPanel regulation cards.

- [ ] **Zone regulations show with zone number** — Stream with zone regs → header shows "Region X — Zone X Regulations" (not just "Region X Zone Regulations")
- [ ] **Direct regulations show** — Named stream with specific regs → cards list closure/gear/quota rules
- [ ] **Tributary badge** — Inherited reg group shows "Tributary of [Parent]" badge on header
- [ ] **Multiple regulation groups** — Feature with both zone + specific regs → separate collapsible groups
- [ ] **Section deep link** — Click a regulation section → URL updates with `?s=` param

---

## D. Map Interaction

Test click/hover/highlight behavior on the map.

- [ ] **Stream click → select** — Click a stream → it highlights purple, InfoPanel opens
- [ ] **Lake click → select** — Click a lake polygon → polygon highlights, InfoPanel opens
- [ ] **Disambiguation menu** — Click where streams overlap → "MULTIPLE FEATURES" menu appears instantly
- [ ] **Disambiguation hover** — Hover an item in menu → corresponding feature highlights on map
- [ ] **Deselect** — Click empty map area → selection clears, InfoPanel closes
- [ ] **Zoom to feature** — Select from search → map flies to feature bbox

---

## E. Deep Links & URL State

Test that URLs restore correctly on page load.

- [ ] **Named feature URL** — Navigate to `/waterbody/<wbg>/` → feature loads + selects + flies to
- [ ] **Unnamed feature URL** — Navigate to `?f=<reach_id>` → resolves via API, selects correctly
- [ ] **Section param** — Navigate to `/waterbody/<wbg>/?s=<reach_id>` → correct section tab active
- [ ] **Share button** — Click share → URL copies → paste in new tab → same feature loads
- [ ] **Browser back/forward** — Select feature A → feature B → back → feature A restores

---

## F. Prerender & SEO

- [ ] **Pages generated** — `ls webapp/dist/waterbody/ | wc -l` → should be 18,000+
- [ ] **Sitemap exists** — `cat webapp/dist/sitemap.xml | head -20` → valid XML with URLs
- [ ] **Prerender content** — `cat webapp/dist/waterbody/<some-wbg>/index.html | grep "<title>"` → correct feature name in title
- [ ] **Canonical URL** — Prerendered pages have `<link rel="canonical">` tag

---

## G. Known Issues to Spot-Check

These are V1 bugs that have fixes/mitigations. Verify they haven't regressed.

| Check | How to test |
|-------|-------------|
| [ ] Morris Creek NOT under Chehalis | Search "Morris Creek" → no Chehalis regs in InfoPanel |
| [ ] Similkameen River all segments | Search "Similkameen" → click → all segments present across zones |
| [ ] Parker Lake no unnamed lakes grouped | Search "Parker Lake" → only Parker Lake, no unnamed extras |
| [ ] Liumchen eco reserve renders | Zoom to Liumchen → unnamed waterbody in reserve visible |
| [ ] Unnamed streams don't get reg names | Zoom into admin zone → unnamed streams still show "Unnamed", not admin area name (e.g. "LIARD RIVER WATERSHED") |
| [ ] Kootenay River segments correct | Search "Kootenay River" → segments split at Columbia Lake boundary |
| [ ] Chilliwack Lake under-streams hidden | Zoom into Chilliwack Lake → streams under polygon not visible |
| [ ] Dinosaur Lake streams visible | Search "Dinosaur Lake" → streams visible (no polygon hides them) |
| [ ] Tributary BFS stops at regulated lakes | Check a tributary chain → BFS doesn't cross through a lake with regs |

---

## H. Recently Fixed — Verify After Pipeline Run

These were open issues that now have implementations. Confirm they work in deployed output.

| Check | How to test |
|-------|-------------|
| [ ] Nicomen Slough lakes linked | Search "Nicomen Slough" → slough lakes included via `waterbody_poly_ids` resolver |
| [ ] Kettle River no US segments | Search "Kettle River" → no segments south of BC border (WMU boundary clip in `_load_streams`) |
| [ ] Piggott/Oyster Creek provenance | Search "Piggott Creek" → "Tributary of Oyster Creek" shows via `source` field |
| [ ] Bear River only R2 | Search "Bear River" (Region 2) → should show only R2 regs, not R1 (two-pass hysteresis rejects buffer-only matches) |
| [ ] Campbell River not "Tributary of" | Search "Campbell River" (R2) → should show "Also known as: Little Campbell River" as direct alias, NOT "Tributary of Little Campbell River" |

## I. Known Open Issues (not yet fixed)

These are expected failures or unimplemented items. Document behavior, don't block on these.

| Issue | Status |
|-------|--------|
| Tsitika eco reserves merged (single reg_id) | Needs per-reserve reg_ids or grouping key change |
| Dean River parsing incomplete | Complex multi-segment text — needs re-parse |
| Tidal boundary rivers (14 rivers) | Tidal cutoffs may be inaccurate |
| Lake Revelstoke streams visible | FWA data issue — upper reservoir has different WBK |
| Admin polygon splitting | Whole polygon gets regs, not just intersected portion |
| Missing admin boundaries (Knap, Malcolm, Rubble) | Not in source data |

---

## J. R2 Worker & Deployment

- [ ] **Worker TypeScript clean** — `cd r2-worker && npx tsc --noEmit` → 0 errors
- [ ] **Worker tests pass** — `cd r2-worker && npm test` → 14/14 pass
- [ ] **Resolve endpoint** — `curl <worker-url>/api/resolve?rids=<reach_id>` → returns JSON with reach data
- [ ] **Edge caching** — Second request to same tile returns `cf-cache-status: HIT`
- [ ] **Frontend build clean** — `cd webapp && npx tsc --noEmit` → 0 errors

---

## K. Unit Test Coverage

248 tests covering critical logic. Run after any code change.

```bash
pytest pipeline/tests/ -q
```

Key test classes:
- `TestBuildSearchIndex` (12 tests) — grouping, dedup, bbox union, name variants
- `TestPrecomputeMuFeatures` — MU assignment hysteresis
- `TestVariantOfPropagation` — name variant flow through reaches
- `TestFeatureResolverDispatch` — all resolver pathways (GNIS, WBK, BLK, FWA codes, poly IDs)
- `TestDisplayNameResolver` — stream/polygon name resolution consistency
- `TestDryRunIntegration` — end-to-end enrichment smoke test
