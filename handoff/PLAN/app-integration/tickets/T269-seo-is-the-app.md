# T269 — Water pages ARE the app (prerendered regs pane), the separate text site retires

Status: OPEN (owner approved 2026-10-29: "yes that sounds good lets change the seo stuff to this").

Source: owner 2026-10-29: "make it so that all the regulation/condition pages that open when we click on a item double as seo
pages … i just dont want users here they should be in the app instead"; "it shouldnt change look of pane".

## Work
1. The web app serves `/water/<slug>` (slugs = committed `app/seo/slug-map.json`, unchanged) and opens that water's regs pane,
   exactly as a tap would; the URL updates when a named water is opened in the app (unnamed waters keep today's URLs, unindexed).
2. Build step prerenders, per named water, the app's own HTML shell + that water's regs pane markup (the same components/words,
   no second template) + title/description/canonical/JSON-LD from tools/seo (model.ts, jsonld.ts); the app hydrates and the
   map loads behind it. No visual difference from a tapped pane (screenshot diff gate).
3. Region/topic hubs and the sitemap stay (reuse site.ts/topics.ts) but link to `/water/<slug>` app pages; legacy 301s
   (`legacy.ts`) unchanged. The separate text-only water pages (html.ts page body) are retired.
4. Budgets: keep T196's per-page budget on the prerendered HTML (pane markup + critical CSS; app JS deferred).
5. Staging: site.sh builds this; staging stays noindex.

## Acceptance
`/water/<slug>` on staging shows the pane identical to the in-app tap (screenshot pair, phone + desktop); view-source holds
the rules text; legacy redirect test passes; hubs + sitemap link only app pages; no text-only water page is emitted.

Depends: T192–T198 (built). Size: L. Owner: no.
