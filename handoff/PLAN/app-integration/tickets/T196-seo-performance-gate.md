# T196 — SEO page performance budget and Lighthouse CI

Status: PARTIAL - built and green, uncommitted (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 6.
## Work
Gate in the release step beside T129's WEB_BUDGET: HTML <= 30 KB gzip, CSS <= 10 KB gzip, no blocking JS; Lighthouse CI
mobile run on sample pages (LCP <= 1.8 s, CLS <= 0.05, Performance >= 95, SEO 100).
## Acceptance
The release fails when a budget is exceeded; the numbers are recorded.

Depends: T193. Size: S. Owner: no. Default: targets as in the plan.

## Outcome (built and green but UNCOMMITTED because the sandbox refused the commit step; commit the listed paths, then mark DONE with the hash)
- `budget.ts`: html <= 30 KB gzip, p95 <= 16 KB gzip, css <= 10 KB gzip, critical css <= 2 KB, no blocking script; `buildAll` returns the
  problems and `pnpm seo` fails on any. `budget.json` records the numbers; `lighthouserc.json` (mobile, simulated 4G, LCP <= 1.8 s, CLS <=
  0.05, TBT <= 200 ms, Performance >= 95, SEO 100, A11y >= 95) is generated for sample built pages.
- Fixture numbers (39 waters): largest page 5,072 B gzip, p95 3,909 B, css 791 B, critical css 281 B. Mutation tests: oversized page,
  fat css, blocking script each fail.
- NOT run: `lhci autorun` (Lighthouse needs Chrome and is the release job's step; the CLI is not a workspace dependency).
