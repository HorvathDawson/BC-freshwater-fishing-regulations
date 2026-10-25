# T192 — SEO slug map and per-water page model from the bundle

Status: OPEN.

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 1 (owner 2026-10-24).
## Work
Committed slug map (stable slugs, region suffix on collisions, 301 for removed waters). A page model per named water
built ONLY from the release bundle, using the app's presenter/decided code in app/packages/core.
## Acceptance
Every water with rules has one stable slug; the model's verdict and rules equal the presenter output on the same bundle
(mutation-pinned); nothing is read outside the bundle.

Depends: none. Size: S. Owner: no. Default: slug = ASCII-folded water name.
