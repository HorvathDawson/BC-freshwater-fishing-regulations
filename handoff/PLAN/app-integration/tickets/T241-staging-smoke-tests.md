# T241 — staging smoke tests after every deploy

Status: DONE — ec26ed584 (2026-10-29)

Source: owner 2026-10-28 — approved proposal P7 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
`app/tools/smoke-staging.mjs` (Playwright): against https://staging.canifishthis.ca — map paints (canvas non-blank,
tiles 200), tap a known river → regs page, Conditions view shows a gauge, Salmon mode colours, the synopsis PDF opens,
a `/water/<slug>` page 200 with canonical, a legacy `/waterbody/...` → one 301, the in-map attribution names
OpenStreetMap; desktop + phone viewport; exits non-zero with a short failure list and shots. `deploy/staging/site.sh`
and `upload.sh` run it at the end (opt-out flag).
## Acceptance
Green on the live staging; a deliberately broken check (wrong slug) fails loudly; < 2 min.
Depends: none. Size: S. Owner: no.

## Outcome (ec26ed584)
- `app/tools/smoke-staging.mjs` — the repo's CDP driver (`regs-web/cdp.mjs`; Playwright is not a dependency). HTTP: feeds
  `/status.json` every feed ok (degraded = WARN), the daily backup ran < 30 h (T246), gauges feed < 3 h + in-season < 30 h,
  manifest + synopsis starts `%PDF-`, runtiming `tile_version` = manifest tiles, `/water/<slug>` 200 + canonical + noindex,
  `/waterbody/328939712/` → one 301 → 200, private/ 404. Browser (fresh profile; dismisses first-run + in-season sheets):
  map paints phone 390×844 + desktop 1440×900 (screenshot decoded in-page: ≥150 colours, every .pmtiles 200/206, no 4xx
  but favicon), attribution names OpenStreetMap, Salmon + Run timing each repaint (>0.3 % pixels vs Standard, no failed
  request), tap 102,260 → Gravel Slough regs, tap 319,602 → "Unnamed stream · Region 2", Conditions tab (map-horizons),
  `/?regs=gnis:7551&view=conditions` shows "observed <date> at" + m³/s, Vedder "Rule book page N" → blob PDF `#page=N`.
- Every step's shot in `--shots` (default a temp dir); a FAIL line carries its shot path. Refuses any non-staging host.
- Green on live staging: 18 pass, 52 s. `--slug nope-not-a-water` → FAIL "HTTP 404", exit 1.
- Wired: end of `deploy/staging/site.sh` (not with `--dry-run`) and `upload.sh`; `SMOKE=0` skips; shots
  `app/dist/smoke-staging/`. Manual workflow `.github/workflows/staging-smoke.yml` (no secrets; shots as artifact) —
  NOT run on GitHub yet: the branch is 1642 commits ahead of origin (unpushed); headless WebGL on ubuntu unproven.
