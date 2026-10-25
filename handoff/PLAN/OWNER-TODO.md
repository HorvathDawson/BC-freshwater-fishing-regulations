# Owner to-do

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **T162 run timing — PSF's terms forbid redistribution.** salmonwatersheds.ca/terms: personal non-commercial use only;
  no copying, publishing, mirroring or redistributing (modified or not) without PSF's prior written permission. T162 is
  stopped before any fetch. Ask PSF (info@salmonwatersheds.ca) for written permission to ship derived run timing in the
  app, or pick another source. Also yours: the three Explorer pages under `data/source/runtiming/*.html` are tracked
  in this PUBLIC repo — untrack/purge them or not. Details: the T162 ticket's Outcome.

- **T67 braids — check every cut (phone-friendly):** https://claude.ai/artifact/Et8B4Z6ZbKNNw9mMkJWzFH
  Round 2 (2026-10-24): 28 proposed cuts (Right / Wrong / Unsure + note), 2 re-homed, 19 braids left alone, 45
  dropped ditches, and three yes/no calls on the Questions tab (a gauge on a named side channel cutting its mainstem;
  a moved length cut changing a 1 km Fraser piece's region 5 → 7; the Exchamsiks curated-split fix). Nothing is promoted until you have marked them. (Push is off in /config, so no
  notification was sent.)

- **SEO / domain (T188 plan, T198):** confirm you own canifishthis.ca and canifishthis.com; at the registrar/DNS set
  .com and www to 301 to https://canifishthis.ca. Once hosting (T86) is live: add Domain properties in Google Search
  Console (DNS TXT), import to Bing Webmaster, submit /sitemap.xml, and give Claude the tokens. Later, send the
  outreach by hand (T197). Plan: handoff/PLAN/seo/PLAN.md sections 4 and 7.

## Reviews

## Human runs (heavy, one at a time through scripts/heavy.sh)

- **T73 — shrink the reach hand-off (19 GB → ≈ 0.6 GB):** convert `reaches/full` to a side dir, binding-diff it
  (expect "no binding changed"), then convert `full` + `full_prev` in place; commands in the T73 ticket's Outcome.
- **T182 — the split release files:** `test_release.py` on a build, `deliver all`, then re-cut the browser walk pin
  (`app/tools/request-walk.mjs --pin`); commands in the T182 ticket's Outcome.
