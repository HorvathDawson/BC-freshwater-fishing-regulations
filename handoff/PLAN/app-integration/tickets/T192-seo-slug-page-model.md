# T192 — SEO slug map and per-water page model from the bundle

Status: PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 1 (owner 2026-10-24).
## Work
Committed slug map (stable slugs, region suffix on collisions, 301 for removed waters). A page model per named water
built ONLY from the release bundle, using the app's presenter/decided code in app/packages/core.
## Acceptance
Every water with rules has one stable slug; the model's verdict and rules equal the presenter output on the same bundle
(mutation-pinned); nothing is read outside the bundle.

Depends: none. Size: S. Owner: no. Default: slug = ASCII-folded water name.

## Outcome (committed 1c4dd582, 435ce2d8)
- `app/tools/seo/slug.ts`: `assignSlugs` over the committed `app/seo/slug-map.json` (keyed by item id: a rename keeps its slug, a
  newcomer never takes one, colliding names BOTH take `-r<region>`, a removed water is reported for its 301). The committed map is
  EMPTY: the first real build with `--update-slugs` writes it; commit it then.
- `model.ts`: `waterModel` reads only the regs store (the release's files) through `loadRegsInputs` + `buildRegsPage`; every sentence is
  a presenter string. Region(s) come from `akey_region`. `verify.ts` holds a model to the app's pages (verdict, completeness, no
  invented words); `model.test.ts` runs it over the 43-water fixture release and plants three mutations (wrong verdict, dropped rule,
  invented sentence). Nothing reads data/curated or pipeline (a test scans the sources).
- Typecheck for tools/seo: `pnpm typecheck:seo` (new, in `check`).
- Paths: app/tools/seo/{slug,model,verify,fixture}.ts, model.test.ts, tsconfig.json; app/seo/slug-map.json; app/package.json.

## First real build 2026-10-27 (release r-c706f5eaa2a3, LATEST)
`node app/tools/build-seo.mjs --release-dir data/release/r-c706f5eaa2a3 --update-slugs`: 19,507 pages (19,482 waters), p95 2,948 B
gzip, css 814 B, budget clean, ~4 min. `app/seo/slug-map.json` committed (19,482 slugs). The real data found what the 43-water
fixture could not: 6 pairs of waters whose titles collided once a long name was cut ("Horseshoe..."). Fixed: a title's name is
never cut (brand, then "fishing", are dropped first), and same-name waters take "(Region N)" or "(Region N, no. k)".
Region 6's hub is 27.5 KB gzip against the 30 KB cap: split it (by letter or species) before the next growth.

## v1 URLs (coordinator, 2026-10-27)
The v1 site's 19,331 indexed `/waterbody/<watershed code | lake key>/` pages (live sitemap, `app/seo/legacy-paths.txt`) map through
`app/seo/legacy-crosswalk.json` (19,274 exact, 30 via a parent code, 5 region, 22 none). Built: 19,147 -> a water page, 162 -> a
region page, 22 -> home; kept 0 (canonical stays `/water/<slug>`), dropped-with-reason: the 162 + 22 (no rules page today / unknown id).
Real build: every target exists, no chains. `legacy.test.ts`. See seo/PLAN.md 2b.

## 2026-10-28 owner: query wording + human slugs (nothing live yet, so the map was re-made from empty)
- Title `<Name> fishing regulations, B.C. — Can I Fish This?`; H1 `<Name> fishing regulations, B.C.`; description `<Name> fishing regulations, B.C. (near <town>, Region N): <today's answer> <first rule>`; first line `<Name> is in Region N, near <town>.`. Town = the bundle's `item_near` nearest settlement (city/town/village/hamlet/locality).
- Slugs: a repeated name takes `-near-<town>` (else `-r<region>`); what still collides adds the region, then a counter. Real build: 19,482 slugs, 0 duplicates; 5,557 `-near-`, 2,034 with `-r<region>`, 1,411 end in a counter (same name and same nearest town, e.g. the Mud Lakes cluster). Legacy: 19,331 paths, all one 301 to a built page, no chains.

## 2026-10-28 owner: slug chain + specific-regulation pages
- Slug chain name -> region -> MU(s) -> nearest town -> index, only as far as needed (`mill-creek-r3`, `mud-lake-r3-3-12`, `...-near-quesnel`, `-2` last). MU = the `@<unit>` of a water's book entries. Real build: 19,482 slugs, 0 duplicates; stopped at name 12,699, region 3,977, MU 169, town 1,340, index 1,297; longest 73 chars (`squamish-powerhouse-channel-r2-2-6-near-squamish-riverside-day-use-area-2`).
- Water pages: section and group headings carry unique anchors; a "rules at a glance" block answers steelhead / salmon / trout / bait ban / fly-only / closures / boats questions with the page's own sentences (`topics.ts`), the FAQPage JSON-LD carries only those same Q/A pairs (checked as visible text), and each water links to its region's topic hubs.
- Topic hubs: `/topic/<t>` and `/region/N/<t>` (63 pages; lists capped at 600), linked from the region hub. Real build 19,774 pages, largest 8.0 KB gzip, p95 3.3 KB, all titles and descriptions unique.
