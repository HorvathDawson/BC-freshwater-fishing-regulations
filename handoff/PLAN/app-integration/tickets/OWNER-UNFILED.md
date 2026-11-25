# Owner asks with no ticket yet (T1081)

Things only the owner can do that belong to no open ticket. One `## <title>` section each, owner lines as in README.md
("Owner blocks"). The planning chat numbers a ticket for a section, moves its lines into that ticket's `## Owner`
block and deletes the section here. `node app/tools/ledger.mjs` lists the open lines in `views/OWNER.md`.

## 0.10.0 production release: your commands and your words
- [ ] 2026-11-25 — Run the production commands for 0.10.0 in the order the 0.10.0 chat hands them over with their dry runs: data (`pipeline release --prod`), then the site (it puts the new privacy page live), then the feeds worker. Per the 15:14 decision this comes after the repo move and the 0.10.0 re-cut, probably 2026-11-26; the phone builds and the App Store submit (T1071) follow.
- [ ] 2026-11-25 — Clear the What's-new words the 0.10.0 chat drafted as a proposal (its morning summary).

## Remove the machine-wide Gradle Metaspace workaround
- [ ] 2026-11-24 — Open `~/.gradle/gradle.properties` and delete the Metaspace line you added for the hang (the repo sets the same value now, PR #155; `build-apk.sh` prints a note while it is there). Nothing breaks if you leave it.

## Search Console: indexing requests and the 404 watch
- [ ] 2026-11-22 — Request indexing for the pages in `handoff/PLAN/seo/index-requests-top-100.md` in Search Console, about 10 a day.
- [ ] 2026-10-27 — Add the new sitemap beside the old one once the site is live, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs should show as "Page with redirect").

## Turn off the Copilot coding agent
- [ ] 2026-11-21 — Repo Settings > Copilot: turn off the Copilot coding agent (its environment is already deleted).

## Cloudflare Bots / JavaScript Detections (optional)
- [ ] 2026-11-22 — The Chrome "StorageType.persistent" warning on the site comes from Cloudflare's injected bot script: turn off Cloudflare Bots / JavaScript Detections to silence it.
