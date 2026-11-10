# T544 — Staging site deploy is one command, and the site says which release it was built on

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 12.

## Today
- The site deploy is three commands typed from memory, printed by `pipeline/deliver/release/site_gate.py:32-37`
  (`pnpm export:web:staging`; `node app/tools/build-seo.mjs …`; `deploy/staging/site.sh`), about 15 min, and no script
  runs the SEO build under heavy.sh.
- `deploy/staging/site.sh:9-17` checks the pages name the current web bundle; its header says "on the same release" but
  nothing compares the SEO build's release with LATEST or staging's manifest. `app/tools/seo/build.ts:277` records it.
- The two recorded late failures here (site on 0.6.9 under 0.7.0 data; /changes over budget, T529) already have guards
  (site gate in preflight, `build-seo --check-changes`). What is left is the hand-typed chain and the unchecked vintage.

## Work
1. `deploy/staging/site-all.sh`: cheap checks first, then export → SEO build through heavy.sh (or `relink-seo` when only
   the app changed) → `site.sh`. The site gate's message and the runbook name this one command.
2. The site carries the release id it was built on; the staging smoke compares it with the manifest's release (warn when
   the min app version did not move, fail when it did).

## Acceptance
- One command redeploys the site from a clean tree; the smoke prints both release ids.

Depends: —. Size: S. Owner: no.
