# Proposed tickets — 2026-10-28 (say yay / nay / later per line)

Gaps I see between today and a public launch. None are started. Size: S (hours), M (a day or two), L (more).

| # | Proposal | Why | Size | Yay/nay |
|---|---|---|---|---|
| P1 | **Data attribution + licences page and in-map credits** | OSM (ODbL) needs visible attribution and has share-alike rules for derived data (Northrop channel, OSM name variants); OGL-BC, DFO, PSF and ECCC each have attribution terms. One About → Sources page + map credit line. | S | |
| P2 | **Legal disclaimer + "regs as of" on every page** | "Not legal advice; the synopsis/DFO notice is the authority", the release date and feed ages shown per water. Protects anglers and us. | S | |
| P3 | **Report-a-problem flow** | "This looks wrong" on any water/section → stores water, part, date, app version, note → lands in a review-app queue (v1 had a feedback endpoint on bc-fishing-r2). Best source of real errors after launch. | M | |
| P4 | **Offline regions manager (phone)** | Choose regions to keep offline, see storage used, delete; today it's all-or-nothing (~434 MB tiles + basemap). | M | |
| P5 | **Annual synopsis update runbook + dry run** | The book changes every April 1. A written, rehearsed path: fetch PDF → parse (human-run) → diff vs last year → review queue of changed entries only → release. Do a dry run on the current book. | L | |
| P6 | **Production launch checklist ticket** | One list: data-worker double-gzip fix, prod R2 upload, feeds worker prod + crons, retire bc-fishing-cron + old workflows, DNS/.com→.ca 301s, Search Console, staging noindex off for prod, Cloudflare bot-script/robots settings. | S | |
| P7 | **Staging smoke tests after every deploy** | Playwright run against staging.canifishthis.ca (map paints, tap → regs, conditions, salmon, PDF, a /water page, a legacy 301) — fails loudly. | S | |
| P8 | **Crash + error reporting (privacy-light)** | Know when the app breaks on real phones: Sentry free tier or a tiny Cloudflare endpoint; no personal data. | S | |
| P9 | **Accessibility pass (native)** | Screen reader labels, dynamic font size, contrast in all three themes, touch targets — before store review. | M | |
| P10 | **App store readiness** | Icon, splash, store screenshots, descriptions, privacy policy/“data collected: none” labels, age rating, TestFlight/internal track. | M | |
| P11 | **Low-end Android performance** | Cold start, memory and map FPS on an older phone/emulator profile; budget + fixes. | M | |
| P12 | **Backups / disaster recovery** | Versioned backup of curated data + marks (git covers most) and R2 (release + feeds history) with a restore test. | S | |
| P13 | **Search upgrades** | Fuzzy/typo search, search by MU / region / nearby town, recent searches. | M | |
| P14 | **First-run + help** | A short first-open walkthrough (modes, date, tap a water, offline), and a help page explaining colours/badges. | S | |
| P15 | **Feeds/worker security** | Rate limits on worker routes, run-token rotation, a check that no private/ object is ever public, secrets inventory. | S | |
| P16 | **Expo SDK / dependency upgrade window** | Scheduled upgrade + patch re-apply (MapLibre RN patch) before store submission, so we don't ship on a stale SDK. | M | |

Reply with e.g. "yay 1 2 3 6 7, later 4 5, nay 13" and I'll turn the yays into tickets (Tnn + ledger) and start the
ones I can do alone.
