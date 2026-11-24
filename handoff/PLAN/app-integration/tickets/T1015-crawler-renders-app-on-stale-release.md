# T1015 — Google's renderer runs the web app on cached old releases: what does the indexed page show?

Status: OPEN — next release batch.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1015 | Google's renderer runs the web app on cached old releases: does the indexed page show the app's boot error? | OPEN — next release batch | | Search Console rendered-page check, then the fallback if needed |

Source: T1005 triage. One install (`a916f6f81cd61f75`, a search engine's page renderer: a pinned clock, a fixed install
id, cached resources) sent ~450 production crash reports from `/water/` pages, 2026-11-19 → 11-23: 0.8.5's bundle
booting manifests of r-fc137c4040fa / r-6258ffded676 / r-fec3fab4c5aa ("release … is not one this app reads": the boot
stops on its error screen), 404s on pruned release files, "Wrong magic number for PMTiles archive" (range requests
answered from its cache). The static HTML of a water page carries the regulations; what Google RENDERS over it may be
the app's error screen.

## Work
1. Owner or a session with Search Console: URL Inspection → "Test live URL" → the rendered HTML and screenshot of two
   water pages (e.g. `/water/<slug>` for gnis:9657 and gnis:7776). Does the rendered page still hold the page's text, or
   has the app's boot error replaced it?
2. If the error replaces it: a crawler's run (core `webPlatformLine` says `web (crawler)`) keeps the static page's
   content when the app cannot boot (the app mounts beside it, never over it, until booted), and the boot failure is
   shown in the app's own frame. Never a different page for crawlers than for readers (cloaking): the same fallback a
   reader whose boot fails gets.

Depends: T1005 (the crawler is named in reports from 127296a64). Size: S–M. Owner: item 1 (Search Console).
