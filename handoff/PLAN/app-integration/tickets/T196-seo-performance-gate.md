# T196 — SEO page performance budget and Lighthouse CI

Status: OPEN.

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 6.
## Work
Gate in the release step beside T129's WEB_BUDGET: HTML <= 30 KB gzip, CSS <= 10 KB gzip, no blocking JS; Lighthouse CI
mobile run on sample pages (LCP <= 1.8 s, CLS <= 0.05, Performance >= 95, SEO 100).
## Acceptance
The release fails when a budget is exceeded; the numbers are recorded.

Depends: T193. Size: S. Owner: no. Default: targets as in the plan.
