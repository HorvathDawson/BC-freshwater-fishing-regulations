# T304 — A water page landing reads its own region's files (fast hand-over on slow lines)

Status: DONE 2026-11-06 — bb32b33bb; measured locally 2026-11-06: Fast-3G hand-over on /water/vedder-river 47.6 / 44.6 s → 24.9 / 25.0 s (below). Staging shows it after the next deploy with a FULL `build-seo` (not the relink fast path).

Source: T282 measured /water/vedder-river on Fast 3G: the 647 kB bundle is in at ~0 s, but the hand-over waits for the
whole core (1.4 MB) + whole water (2.2 MB) + regs-2 + sql.js wasm because no map region is focused on a landing, so the
T256 per-region slices aren't used (~37 s).

## Work
The prerendered water page names its region (e.g. `<meta name="cift:region" content="2">` / its item id); the app focuses
that region before the first read so core-<r>/water-<r> serve the landing. Measure Fast-3G "app took over" before/after.

Depends: T256, T269, T282. Size: S. Owner: no.

## Outcome (2026-11-05, bb32b33bb)
- **SEO build:** every water page carries `<meta name="cift:region" content="<r>">` beside `cift:item`
  (`prerender.ts` `appDocument`). `r` = `WaterModel.focus` (`model.ts` `focusOf`): the first keyed part's akey's first
  region (`ORDER BY region`), i.e. the regs-<r> the app's `regionDb` opens first — a bundle region string ("7a", not the
  model's numeric 7). A page without a valid region is REFUSED at build (throws), never written.
- **App:** `startLink.web.ts` reads the tag on `/water/<slug>` (`@app/ui` `pageRegion`) → `setStartRegion` → `useReleaseBoot`
  passes `focus` → `bootRelease` (web) calls `loader.focus(r)` right after making the loader, before it returns, so
  nothing reads core/water unfocused. The map's `onRegion` refocuses later as before; native ignores it.
- **No / bad tag (decision):** the start reads the whole files exactly as before AND reports it (`reportFailure`, deferred
  a microtask so the crash reporter installed later in App.tsx's module body receives it; console.error at once). Not
  silent because the build always writes the tag: its absence is a stale/broken page. A wrong-but-valid region costs
  bytes only (a slice answers only keys it covers, else the whole file).
- **One load path / T269 pairs:** the focus is a routing hint under the T256 cover guarantee
  (`slices.release.test.ts`: focused slice reads deep-equal whole-file reads), so landed / tapped / searched / refreshed
  reads give the same answer; the prerendered markup changes only by the one meta tag (pane unchanged).
- **Tests:** `tools/release-boot.test.ts` "(T304) … focuses the boot BEFORE the first read" (fixture core re-stamped as
  `core-2`: focused boot's first keyed core read fetches `core-2`, unfocused `core`; mutation-checked by removing the
  focus call → fails); `ui/startLink.test.ts` `pageRegion` (all 9 regions; null/""/"7"/"7A"/" 2"/"9" refused, whole
  message); `seo/prerender.test.ts` (tag written, null/"7"/"7A"/"" refused); `seo/site.test.ts` (fixture build: every
  page's tag = one of its water's regions). typecheck, platform, boundaries green. Full suite / `pnpm check` NOT run
  (heavy-job lock).

## Left / owed
- **Stored pages carry no tag until a full `build-seo`:** the staging fast path (`deploy/staging/relink-seo.mjs`) only
  rewrites asset names, so relinked pages land as today and each landing reports "names no region" until the water pages
  are rebuilt. Run a full SEO build with the next staging deploy.
- **Measurement owed (T282's method):** staging, Fast 3G (DevTools preset), cache off,
  `node app/tools/bad-net.mjs --profiles fast-3g --steps takeover` on `/water/vedder-river`: "app took over" before
  (T282: 37.7 / 37.0 s; release fetches core 1,380 kB + water 2,223 kB + regs-2 + wasm) vs after a full `build-seo` +
  deploy (expect `core-2` + `water-2` + `water-shared` instead of core + water). Record both times and the fetched files.

## Measured 2026-11-06 (local, the T409 rig)
A full `build-seo` over r-5a5999c94588 (the release staging serves) with the HEAD web export (e8ebf21f4): 19,635 water
pages, **every one carries `cift:region`** (1: 2,372 · 2: 2,027 · 3: 2,286 · 4: 2,773 · 5: 2,340 · 6: 3,118 · 7a: 1,950 ·
7b: 1,179 · 8: 1,590; none missing). Served from this machine, data from data-staging; `bad-net.mjs --profiles fast-3g
--steps takeover`, 2 runs each. "Before" = the same pages with the tag stripped by the server (the app then reads the
whole files and reports it — the pre-T304 page).

| | /water/vedder-river → app took over | release files fetched | / → map chrome |
|---|---|---|---|
| before (no tag) | 47.6 s / 44.6 s | water-shared 639 kB, **core 1,388 kB, water 2,489 kB**, regs-2 262 kB | 12.5 s / 12.5 s |
| after (tag) | **24.9 s / 25.0 s** | water-2 159 kB, water-shared 639 kB, **core-2 198 kB**, regs-2 262 kB | 12.5 s / 12.5 s |

−3.5 MB of release on a water landing, hand-over in about half the time; home unchanged. (Before is slower than T282's
37 s on staging because this release's whole `water` file is larger, 2,489 vs 2,223 kB.) What is left on the line before
the hand-over is fixed cost: water-shared 639 kB, the wasm 332 kB, the bundle 812 kB.
**Found while measuring (→ T409):** every landing fetches each face twice — the page's WOFF2 copies (`/assets/fonts/`,
T409 item 1) and then the app bundle's own TTF copies (8 × 36-48 kB, ~315 kB on a Fast-3G line before the hand-over).
