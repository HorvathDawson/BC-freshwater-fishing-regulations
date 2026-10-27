# T192 — SEO slug map and per-water page model from the bundle

Status: PARTIAL - built and green, uncommitted (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 1 (owner 2026-10-24).
## Work
Committed slug map (stable slugs, region suffix on collisions, 301 for removed waters). A page model per named water
built ONLY from the release bundle, using the app's presenter/decided code in app/packages/core.
## Acceptance
Every water with rules has one stable slug; the model's verdict and rules equal the presenter output on the same bundle
(mutation-pinned); nothing is read outside the bundle.

Depends: none. Size: S. Owner: no. Default: slug = ASCII-folded water name.

## Outcome (built and green but UNCOMMITTED because the sandbox refused the commit step; commit the listed paths, then mark DONE with the hash)
- `app/tools/seo/slug.ts`: `assignSlugs` over the committed `app/seo/slug-map.json` (keyed by item id: a rename keeps its slug, a
  newcomer never takes one, colliding names BOTH take `-r<region>`, a removed water is reported for its 301). The committed map is
  EMPTY: the first real build with `--update-slugs` writes it; commit it then.
- `model.ts`: `waterModel` reads only the regs store (the release's files) through `loadRegsInputs` + `buildRegsPage`; every sentence is
  a presenter string. Region(s) come from `akey_region`. `verify.ts` holds a model to the app's pages (verdict, completeness, no
  invented words); `model.test.ts` runs it over the 43-water fixture release and plants three mutations (wrong verdict, dropped rule,
  invented sentence). Nothing reads data/curated or pipeline (a test scans the sources).
- Typecheck for tools/seo: `pnpm typecheck:seo` (new, in `check`).
- Paths: app/tools/seo/{slug,model,verify,fixture}.ts, model.test.ts, tsconfig.json; app/seo/slug-map.json; app/package.json.
