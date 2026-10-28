# T196 — SEO page performance budget and Lighthouse CI

Status: PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 6.
## Work
Gate in the release step beside T129's WEB_BUDGET: HTML <= 30 KB gzip, CSS <= 10 KB gzip, no blocking JS; Lighthouse CI
mobile run on sample pages (LCP <= 1.8 s, CLS <= 0.05, Performance >= 95, SEO 100).
## Acceptance
The release fails when a budget is exceeded; the numbers are recorded.

Depends: T193. Size: S. Owner: no. Default: targets as in the plan.

## Outcome (committed 1c4dd582, 435ce2d8)
- `budget.ts`: html <= 30 KB gzip, p95 <= 16 KB gzip, css <= 10 KB gzip, critical css <= 2 KB, no blocking script; `buildAll` returns the
  problems and `pnpm seo` fails on any. `budget.json` records the numbers; `lighthouserc.json` (mobile, simulated 4G, LCP <= 1.8 s, CLS <=
  0.05, TBT <= 200 ms, Performance >= 95, SEO 100, A11y >= 95) is generated for sample built pages.
- Fixture numbers (39 waters): largest page 5,072 B gzip, p95 3,909 B, css 791 B, critical css 281 B. Mutation tests: oversized page,
  fat css, blocking script each fail.
- NOT run: `lhci autorun` (Lighthouse needs Chrome and is the release job's step; the CLI is not a workspace dependency).

## Staging 2026-10-28
Live on https://staging.canifishthis.ca with release r-46be3c135e68 (19,774 pages, budget clean; v1 redirects 19,147 water /
162 region / 22 home, each one 301, spot-checked). Lighthouse mobile: A11y 100, CLS 0, Performance 69-76 from Cloudflare-injected
bot/analytics script (zone setting, owner). Still PARTIAL: production deploy and Search Console are the owner's. Record, findings
and redo steps: handoff/PLAN/seo/staging.md; deploy scripts: deploy/staging/.
