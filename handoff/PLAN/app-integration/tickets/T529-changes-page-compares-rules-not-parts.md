# T529 — /changes compares a water's rules, not its parts (staging site is down until this lands)

Status: DONE — 6dd5ff5e8; site smoke 18/18 and release --staging rerun done (r-a178299f6005)

Was: IN REVIEW — 6dd5ff5e8; staging site serves 0.7.0 (site_gate ok, 2026-11-09); left (Claude): the site smoke + `release --staging --min-app-version 0.7.0` rerun.
Earlier: OPEN — ship-blocker for the staging site (do first)

## What broke (2026-11-09)
- Release r-d14bdb180f55 (LATEST, on staging, `minAppVersion 0.7.0`) carries T517's new parts and names (water/3,
  geo/2).
- The staging web site still serves web 0.6.9, which cannot read that data. It shows "The map data could not be
  loaded." The phone app (build 22, 0.7.0) is fine.
- The site redeploy stopped in `app/tools/build-seo.mjs` (the error is thrown in `app/tools/seo/build.ts:247`):
  `seo: 1 budget/consistency problem(s): /changes is 180379 B gzip, over 30720`.
- The previous /changes page (r-6dce77a24e6a vs r-25a89ce9f8e6) listed 1 rule change.
- **Likely cause:** T517 re-cut every river into one part per connected run and renamed parts from their end tokens.
  A comparison keyed on parts, or on part names, therefore lists most waters as "Rules changed" even where nothing
  an angler must obey changed.

## What
1. Find how /changes builds its "Rules changed / New waters / no longer listed / Wording updated" lists (T490: "/changes = rule
   changes"; app/tools/seo/; the kept snapshot `<release>.json` it diffs against). Confirm the cause with numbers:
   how many waters each bucket holds for r-6dce77a24e6a → r-d14bdb180f55, and a sample of "changed" waters with the
   rule text before and after.
2. Fix the comparison. A water's rules are compared as what applies where:
   - rule text plus the stretch of water (in metres along the stream) it covers;
   - independent of how the water is split into parts or what the parts are called.

   So a water whose rules are the same is not a rule change, however it is cut. A real rule change still shows: one rule,
   one window, one closure, or where it applies. A part renamed with the same rules is not a change.
3. Do **not** raise the 30 KB budget, and do not hide rows to make it fit. If, after the fix, the honest list is still
   too large, say so and ask the owner how /changes should summarise. Never silently truncate (no silent fallbacks).
4. Tests: a water re-cut into different parts with the same rules is not a change; a changed rule on one stretch is a
   change; a renamed part with the same rules is not a change. Each is pinned with a mutation-style case.
5. Then redeploy the staging site:
   1. `pnpm -C app/apps/mobile export:web:staging`
   2. `cd app && node tools/build-seo.mjs --release-dir ../data/release/r-d14bdb180f55 --app dist/web-staging --threads 4`
   3. `bash deploy/staging/site.sh` (it runs the smoke)

   The smoke must be green. Then run `python -m pipeline release --staging --min-app-version 0.7.0`. Its new site check
   (`pipeline/deliver/release/site_gate.py`, 6f5790a44) refuses until the site is current. It must end with every
   stage done.
6. Add the check to fast-fail (AGENTS 20a): the /changes budget is known only after a ~15 min SEO build, so a cheap
   pre-check of the change count belongs where the release is known (release preflight or build-seo's start).

## Done when
- The staging site serves the 0.7.0 web build on r-d14bdb180f55.
- The smoke is green.
- /changes lists only real rule changes, within budget.
- The tests above pass, and `pnpm check` exits 0.
