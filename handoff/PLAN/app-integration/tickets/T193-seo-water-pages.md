# T193 — SEO static water pages and About page

Status: PARTIAL - built and green, uncommitted (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 1 and 5.
## Work
app/tools/build-seo.mjs renders /water/<slug> HTML (full rules in plain words, today's line, book page, gauges, Open in
the app, not-a-legal-document line) and /about; it runs in the release job after the bundle is cut.
## Acceptance
Pages read without JS; page count == waters with rules; pages refresh on every release.

Depends: T192. Size: M. Owner: no. Default: no maps or interactive parts on the page.

## Outcome (built and green but UNCOMMITTED because the sandbox refused the commit step; commit the listed paths, then mark DONE with the hash)
- `app/tools/build-seo.mjs` (`pnpm seo --release-dir <folder>` | `--fixture`) loads `tools/seo/*.ts` through vite's SSR loader (vitest's own
  engine; knip allows it) and writes `app/dist/seo` (gitignored). Page = h1, "Can I fish X today?" + the verdict, as-of date and release id,
  the full rules by part (presenter wording, synopsis page linked per rule), connected waters, more in the region, centre point, the
  not-a-legal-document line, prominent "Open X in the app" (`/?regs=<item>`, the web start link). No JS, no map.
- Fixture run 2026-10-27: 39 water pages for 39 waters with rules, plus About, 8 regions x 2 hubs. A test checks each page's verdict class
  against the app's status for its parts, with a planted wrong verdict caught.
- NOT done: "nearby gauges" (gauges are not in the regs bundle files; needs a gauge read from the release) and the optional client refresh
  script (the static text is already right on release day). Wiring `pnpm seo` into the publish job is the release owner's step (publish.py
  is human-run).
- Paths: app/tools/build-seo.mjs, app/tools/seo/{html,site,build}.ts, site.test.ts, app/seo/site.json, app/knip.jsonc, app/package.json.
