# T193 — SEO static water pages and About page

Status: OPEN.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 1 and 5.
## Work
app/tools/build-seo.mjs renders /water/<slug> HTML (full rules in plain words, today's line, book page, gauges, Open in
the app, not-a-legal-document line) and /about; it runs in the release job after the bundle is cut.
## Acceptance
Pages read without JS; page count == waters with rules; pages refresh on every release.

Depends: T192. Size: M. Owner: no. Default: no maps or interactive parts on the page.
