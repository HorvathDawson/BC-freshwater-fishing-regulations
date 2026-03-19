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

- [x] **tier0.json exists and is non-empty** — `ls -lh output/pipeline/deploy/tier0.json`
- [x] **PMTiles generated** — `ls -lh output/pipeline/deploy/*.pmtiles`
- [x] **Search index has entries** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('search_index',[])), 'search entries')"`
- [x] **Regulations present** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('regulations',{})), 'regulations')"`
- [x] **Reaches present** — `python -c "import json; d=json.load(open('output/pipeline/deploy/tier0.json')); print(len(d.get('reaches',{})), 'reaches')"`

---

## B. Name & Display Checks

Open the site in browser. Use search bar + InfoPanel.

- [ ] **Named stream search** — Search "Adams River" → result appears → click → InfoPanel title = "Adams River"
- [ ] **Named lake search** — Search "Okanagan Lake" → result appears → click → correct title + lake icon
    * ~~need search to zoom into feature better~~ Fixed: unified `flyToFeature` helper — search, URL restore, and zoom-to-section all use the same bbox + padding logic now
    
- [x] **"Also known as" aliases** — Find a feature with name variants → InfoPanel shows "Also known as:" section
- [x] **Tributary alias** — Find a tributary (e.g. a small creek) → "Also known as: Tributary of [Parent]" shows if inherited
    * Fixed: `canonical_name` field on OverrideEntry powers tributary/admin aliases. All 41 trib + 6 admin entries have canonical_name.

- [x] **Admin alias** — Feature in a park/eco reserve → "In [Park Name]" context line below aliases
    * Already implemented: InfoPanel renders `In {name}` for admin-sourced name_variants. 871 entries have admin aliases (e.g. "LIARD RIVER WATERSHED"). Test with e.g. Adsett Creek.

- [x] **Unnamed stream excluded from search** — Type random letters → no "Unnamed" entries in results
- [x] **Display name consistency** — Same name in search result, disambiguation menu, InfoPanel title, and browser tab title
- [x] **loading on slow click is wonky**
    * Fixed: delayed spinner (150ms delay, 300ms min visible) avoids flash on fast resolves
- [x] **tidal appearance should be better** no border when zoomed out and add low opacity color to it.
    * Fixed: border hidden below z9, fill uses interpolated opacity (0.06→0.12), lighter slate-400 color

---

## C. Regulation Display

Click a feature and check the InfoPanel regulation cards.

- [x] **Zone regulations show with zone number** — Stream with zone regs → header shows "Region X — Zone X Regulations" (not just "Region X Zone Regulations")
    * should be in badge maybe? 
- [x] **Direct regulations show** — Named stream with specific regs → cards list closure/gear/quota rules
- [x] **Tributary badge** — Inherited reg group shows "Tributary of [Parent]" badge on header
    * should add badge for admin match like "In ..." or something
- [x] **Multiple regulation groups** — Feature with both zone + specific regs → separate collapsible groups
    * not collapsible rn 
- [x] **Section deep link** — Click a regulation section → URL updates with `?s=` param
    * Fixed: URL update effect always includes `sectionFgid` from `frontend_group_id` on every navigation, not just tab switches

---

## D. Map Interaction

Test click/hover/highlight behavior on the map.

- [x] **Stream click → select** — Click a stream → it highlights purple, InfoPanel opens
- [x] **Lake click → select** — Click a lake polygon → polygon highlights, InfoPanel opens
- [x] **Disambiguation menu** — Click where streams overlap → "MULTIPLE FEATURES" menu appears instantly
- [x] **Disambiguation hover** — Hover an item in menu → corresponding feature highlights on map
- [x] **Deselect** — Click empty map area → selection clears, InfoPanel closes
- [x] **Zoom to feature** — Select from search → map flies to feature bbox

---

## E. Deep Links & URL State

Test that URLs restore correctly on page load.

- [x] **Named feature URL** — Navigate to `/waterbody/<wbg>/` → feature loads + selects + flies to
    * Fixed: `navigateToWaterbody` always writes `?s=` param; URL update effect extracts `frontend_group_id` as sectionFgid
- [x] **Unnamed feature URL** — Navigate to `?f=<reach_id>` → resolves via API, selects correctly
- [x] **Section param** — Navigate to `/waterbody/<wbg>/?s=<reach_id>` → correct section tab active
- [x] **Share button** — Click share → URL copies → paste in new tab → same feature loads
- [x] **Browser back/forward** — Select feature A → feature B → back → feature A restores
    * Fixed: switched `replaceState` → `pushState` + `popstate` listener with `popstateInProgressRef` guard to prevent infinite loops

---

## F. Prerender & SEO

- [x] **Pages generated** — `ls webapp/dist/waterbody/ | wc -l` → should be 18,000+
- [x] **Sitemap exists** — `cat webapp/dist/sitemap.xml | head -20` → valid XML with URLs
- [x] **Prerender content** — `cat webapp/dist/waterbody/<some-wbg>/index.html | grep "<title>"` → correct feature name in title
    * Fixed: picks most common reach name (non-"unnamed" tiebreaker), includes alt segment names + direct name_variants in title
- [x] **Canonical URL** — Prerendered pages have `<link rel="canonical">` tag

---

## G. Known Issues to Spot-Check

These are V1 bugs that have fixes/mitigations. Verify they haven't regressed.

| Check | How to test |
|-------|-------------|
| [x] Morris Creek NOT under Chehalis | Search "Morris Creek" → no Chehalis regs in InfoPanel |
| [x] Similkameen River all segments | Search "Similkameen" → click → all segments present across zones |
| [x] Parker Lake no unnamed lakes grouped | Search "Parker Lake" → only Parker Lake, no unnamed extras |
| [x] Liumchen eco reserve renders | Zoom to Liumchen → unnamed waterbody in reserve visible |
| [x] Unnamed streams don't get reg names | Zoom into admin zone → unnamed streams still show "Unnamed", not admin area name (e.g. "LIARD RIVER WATERSHED") |
| [x] Kootenay River segments correct | Search "Kootenay River" → segments split at Columbia Lake boundary |
| [x] Chilliwack Lake under-streams hidden | Zoom into Chilliwack Lake → streams under polygon not visible |
| [x] Dinosaur Lake streams visible | Search "Dinosaur Lake" → streams visible (no polygon hides them) |
| [x] Tributary BFS stops at regulated lakes | Check a tributary chain → BFS doesn't cross through a lake with regs |

---

## H. Recently Fixed — Verify After Pipeline Run

These were open issues that now have implementations. Confirm they work in deployed output.

| Check | How to test |
|-------|-------------|
| [x] Nicomen Slough lakes linked | Search "Nicomen Slough" → slough lakes included via `waterbody_poly_ids` resolver |
    * all there but they all show up when searched which is a bit nasty... not sure how to handle this
| [x] Kettle River no US segments | Search "Kettle River" → no segments south of BC border (WMU boundary clip in `_load_streams`) |
| [x] Piggott/Oyster Creek provenance | Search "Piggott Creek" → "Tributary of Oyster Creek" shows via `source` field |
| [x] Bear River only R2 | Search "Bear River" (Region 2) → should show only R2 regs, not R1 (two-pass hysteresis rejects buffer-only matches) |
| [x] Campbell River not "Tributary of" | Search "Campbell River" (R2) → should show "Also known as: Little Campbell River" as direct alias, NOT "Tributary of Little Campbell River" |

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
