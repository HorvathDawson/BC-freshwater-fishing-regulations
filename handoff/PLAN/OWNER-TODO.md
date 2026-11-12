# Owner to-do

**The owner ticks items off in the Owner To-Do page: https://claude.ai/artifact/Fm5KqHqutxRJsk8UymFo6F** (owner 2026-11-11). Every open item for the owner is a row
there (collection `todo`: title, detail, group, ticket, order, added, done, note). Claude adds rows with the
ArtifactData tool when work produces something for the owner, and reads `done` and `note` back at the start of a
session. This file keeps the long text and history.

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- **T671 salmon readings to rule on** (2026-11-11; evidence, DFO's rows in full and counts: `app-integration/tickets/T671-salmon-findings-d1-d5.md`). Nothing is built for these three; the app reads as before until you say. (1) D1: DFO prints `All | Jul 24 to Aug 14 | No fishing for salmon` beside `Coho | Apr 1 to Mar 31 | 2 per day` on the Tatshenshini below the border (6,254 sections): the app shows coho open through the closure. On the same river above the border DFO dates coho around it (`Aug 15 to Jul 23`). The Cowichan is the reverse shape (`All | Sep 1 to July 14 | Open for salmon catch and release` with coho and chum openings inside it). Pick: leave; the All closure wins unless the species row is dated inside it; or the All row always wins. (2) D4: Region 6 section A says "in the aggregate (combined total), in Region 6" over chinook 4 and coho 4 a day (393,903 sections read it per water). Region 5B's same words are a pool by your T349 ruling. Pick: a Region 6 pool too, or boilerplate. (3) D5: a DFO place not drawn yet that is open while the mapped page reads closed (101,896 section-days; the Harrison on Nov 11). Pick: keep the page's answer and re-word the caveat and headline ("Closed today · open in part"), or the whole water reads "Check DFO" on those days. Ticket T671.
- **T637: Whiteswan inlet named so its closure stays; say if the name or the piece is wrong** (2026-11-11). Blue line 356560775 above Whiteswan Lake (from 8,623 m up) is now "Whiteswan Lake inlet stream", so "No Fishing EXCEPT below the falls" keeps covering it after the oxbow fix. Ticket T637.
- **Review app: the checklist is ready (T532, 2026-11-09).** Restart it (`bash curation-review/run.sh`). DFO tab = DFO
  places; Entries → "Extents checklist" = regulation extents (both together per water: 3,686 places on 1,374 waters).
  One water at a time; each row ✓/?/✗ with its map pin; "All correct" or "Something's wrong" (→ change the extent).
  Two small calls, defaults in use: (1) ✗ also flags "tributaries walked but the words don't say" and "a carve-out the
  words never state" — default: keep ✗ (look at it), say "make those ?" otherwise; (2) a check saves on the place only, not
  on its older rewordings (they are dormant) — default: yes.
- **Production release: later, after your DFO extent labelling (owner 2026-11-08).** The command when ready:
  `CONFIRM_PROD=r-a178299f6005 python -m pipeline release --prod r-a178299f6005` (or the newer release by then).
- **Phone check (current build): last done 2026-11-09 on build 24** (answers on the owner checks page,
  https://claude.ai/artifact/5LRBFFgvYeTRnygSBWvZbM). 20 tickets closed on "fine". Follow-ups filed: T563 (percentile
  alert follows each day), T564 (gauge page: forecast + readable chart), T565 (in-season notice at the top), T566 (map
  switching), T567 (Fraser at Mission reading); T401 pop-in kept, minor. **One question back to you: T498/T508 "a
  water's pane opens whole" was marked Not right with no note: which tab, and what loads late?**
- **Review app look (T532):** "will check soon". Ticket work waits for your green light (one agent at a time).

- **ANSWERED 11-05:** T260 switches = ink fill; CF token kept until prod (T404); Apple install id declared as Device ID
  (not linked); Heber: carry ignores whitespace-only changes; audits T333–T335 scoped (all areas, report + fix tickets,
  medium, delete archive/). (T425/T427 looks, T319 titles, icon, support URL: decided 2026-11-06.)
- **PSF run-timing files: ANSWERED 11-05** — fine as is; only the parsed `timing.json` stays in git, the PSF pages and their
  fetch record are git-ignored from now on (772fbf4f0).




- **Water-page speed (T405/T409): ANSWERED 2026-11-05.** Build the map only when it is first seen, web-only (local 93·93·93;
  a phone's "back to map" waits ~1–2 s once). Next chat, launch and hygiene, before the prod SEO launch. Keep Cloudflare's
  JS detections and Web Analytics beacon (0 ms blocking).

- **Region hub cards (T406 part 2): answered 11-05 — keep both as they are** (the bar where it is; the map button opens
  the whole map, no region link).
- **Changelog drafts: approved (owner 11-05, "I'm sure they are good")** — `draft` cleared on 0.1.0–0.5.0; 0.6.0 stays a
  draft until its release. Still yours: production has no Play config file (`play.json`) yet (prod transition, T404).
- **Gauge alerts, stock-style (T527, owner 11-09; replaces the T234 v1.1 cadence readings):** crosses above / below a
  value or a percentile for the day, once per crossing, ready again once back across by 2 % of the line. The per-alert
  cadence and the ±% in 24 h kind are gone; a saved ±% alert is not kept and the page says so. To confirm, as built:
  a percentile line is taken from the gauge's record when the alert is saved (delete + add again after a new HYDAT
  release to refresh it); custom percentiles are whole numbers 10–90 (the record has nothing past p10 / p90).

- **Plan to 0.5.0 approved (2026-11-03):** the plan file (`RELEASE-PLAN-0.5.0.md`) is in git history since T437. Yours along the way: a look at the compressed rule-book photo pages (T354, plan set 11-03)
  (T360/T354, batch 2); the Apple Developer account ($99) before the iOS release (T88); the feeds-worker deploy: now Claude's
  (owner 2026-11-03), when it makes sense (with 0.5.0: dfo-feed/3 + the ten forecast days).
- **Build pass (T266/T295/T297, 2026-10-31):** review `handoff/PLAN/app-integration/build-pass-2026-10-31.md`. The proof side run matched r-51d80e68f056 byte for byte, except the tile_version (T305) and the publish stamps. Nothing was promoted. Nothing is yours here.
- **App test bed (T255, 2026-10-29):** Android emulator scenarios for downloads/sync/offline — `app/tools/testbed/README.md`; last report `handoff/PLAN/app-integration/T255-shots/run-final/index.html`. Found and ticketed: the first install crash-loops on Android (T262, high), Wi-Fi return does not start a held update (T265).
- **ODbL offer (T235, 2026-10-28):** done as you asked (`data/LICENSE-ODbL.md`, `_licence` in each file). Yours: (1) the repo has no LICENSE of its own — the notice says the rest is "under its own terms"; pick one if you want it stated; (2) the app links files on the `redesign/stream-sections` branch (`licences.ts` `REPO_BLOB`) — flip to `main` when it merges; (3) the no-access land (incl. Malcolm Knapp) and reserve outlines are OSM, not the Province — now credited as OSM.
- **Update testing on your phone (T251, 2026-10-28):** dev build → About → Updates log → channel Test → reopen; then `node deploy/staging/test-updates.mjs reset | feed-bump gauges | release 1 | corrupt | missing | feed-404 dfo | min-app 99.0.0` and Check now (steps in tickets/T251). Before prod (T86/T240): the production data worker must allow the `X-App-Version` header (copy staging's data-worker.js CORS line), or web update checks fail.
- **Staging is live (2026-11-05: release r-5a5999c94588, SEO pages from it; was 2026-10-28 r-46be3c135e68): https://staging.canifishthis.ca** (app + SEO pages; data from
  data-staging.canifishthis.ca). Check: the map paints and a tap opens regs; a few `/water/<slug>` pages (e.g.
  `/water/vedder-river`), `/region/2`, `/sitemap.xml`, and an old link such as `/waterbody/930-055749/` (one 301 to
  `/water/san-juan-river`). Staging sends `X-Robots-Tag: noindex`. Decide on the zone: (a) Cloudflare **managed robots.txt**
  is prepended to ours (it says `Allow: /` for everyone and blocks AI crawlers); (b) **Bot Fight Mode / JS detections + Web
  Analytics** inject ~2.6 s of script into every page (Lighthouse Performance 69-76 instead of >= 95; our pages have no
  JS). Prod's data worker `bc-fishing-r2` double-gzips release files: use `deploy/staging/data-worker.js` for prod (T86).
  Details: handoff/PLAN/seo/staging.md.
- **SEO / domain — AT PROD LAUNCH ONLY, not now (owner 2026-11-05: no prod deploy for a while; tuning on staging).**
  These wait for the prod transition (T404 runbook). Nothing below is due today. Staging: https://staging.canifishthis.ca
  **The runbook is written (T404, 2026-11-07): `handoff/PLAN/release/PRODUCTION.md`** — every prod step in order with its
  command (all yours, each needs CONFIRM_PROD), every later release, rollback. Read it and answer Q171–Q175 (tickets/README).
  1. **(in the prod release runbook, T404)** **www.canifishthis.ca drops the path today** (`curl -sI https://www.canifishthis.ca/about` → `location: https://canifishthis.ca/`).
     Cloudflare dashboard → canifishthis.ca → Rules → Redirect Rules → edit the www rule: "Dynamic", expression
     `concat("https://canifishthis.ca", http.request.uri.path)`, status 301, **Preserve query string** on. (canifishthis.com and
     www.canifishthis.com already keep path + query — leave them.) Optional instead: remove those rules and deploy the generated
     host worker: `npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml` (after deploy/prod/site.sh has copied it).
  2. **Prod site deploy** (with the 0.6.0 release; T240 §5): build `app/dist/web-prod` (T240 line "SEO build for prod") and
     `node app/tools/build-seo.mjs --release-dir data/release/<r-id> --app app/dist/web-prod --threads 4 --out app/dist/seo-prod`
     (via scripts/heavy.sh), then `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh`. Check: `curl -sI https://canifishthis.ca/waterbody/930-055749/`
     → one 301 to /water/san-juan-river; `curl -sI https://canifishthis.ca/region/2/` → 301 (not 307); `curl -s https://canifishthis.ca/robots.txt`
     ends `Sitemap: https://canifishthis.ca/sitemap.xml`; no `x-robots-tag` header; `strict-transport-security` present.
  3. **Zone settings** (Security → Bots; Settings → robots.txt): keep Cloudflare's managed robots.txt OFF on canifishthis.ca so ours is the only one.
  4. **Search Console** (search.google.com/search-console): keep the v1 property; Add property → Domain → `canifishthis.ca` → copy the TXT
     record → Cloudflare DNS → Add record TXT `@` with that value → Verify. Repeat for `canifishthis.com`. Sitemaps → submit
     `https://canifishthis.ca/sitemap.xml` (expect ~19,928 discovered). URL Inspection → `/`, `/water/vedder-river`, `/region/2` → Request indexing (those three only).
  5. **Bing Webmaster** (bing.com/webmasters): Import from Google Search Console → pick canifishthis.ca → submit the same sitemap.
  6. Monthly: handoff/PLAN/seo/MEASURE.md (no analytics script — Search Console, Bing, Cloudflare traffic).
  7. Outreach (T197): after prod is live, fill the brackets in handoff/PLAN/seo/outreach/*.md and send each by hand; log in tracker.csv.

### Batch for the end of the closed-testing run (owner 2026-11-11: decide together at the end)

Answered 2026-11-11 (recorded in T665, T667, T666): the six tidal decisions, the six tests decisions, the fresh-copy test run.

Still open:
- T668 salmon places from two regions on one stretch (audit `app-integration/T668-audit.md`, 493 sections): built on the defaults D1 an undrawn place stays in its own region, D2 on a section in one region that region wins a tie, D3 a region-wide place binds every section in its region, D4 the card names the region, D5 D1 first. Owner 2026-11-11: a stretch that straddles a boundary is genuinely in both regions, so a straddler keeps both regions' places and the stricter answer per fish speaks (as the freshwater rule, RULINGS Z4); and "straddles should always take the most strict rules from both regions" (limits, sizes, open days, gear, hours). Measured: 432 of 493 sections change, 197 genuine straddlers, 84 statuses flip, all toward closed; shows with the next release. D1 and D4 confirmed by the owner 2026-11-11 ("Yes and yes"), with: on the Skeena the card row names the DFO section as well as the region (the cascade level): the lettered section as DFO's Skeena page prints it (A, B(i), B(ii) …), owner's clarification the same day. ALL ANSWERED; nothing open here until the phone check of that release.
- T668 straddlers, one default to confirm (built, commits dfb4ea7c1 b53d5e193, RULINGS Z4a): salmon openings on a straddling stretch now use the same strictest-wins comparison as the freshwater rules, with ONE deliberate difference: when one of the two DFO regions says nothing about a fish on that stretch, it counts as closed there (freshwater treats silence as 'the base rule applies'). Measured on the 197 straddlers: 19 sections now show both regions' words, none looser, no map colour change, no gear change. Default taken: silent region = closed. ANSWERED 2026-11-11: yes, salmon differs from freshwater here; a silent region counts as closed. The rest of the unification is T669.
- T664 Okanagan River Oxbows as one water (code 25471b393; lands with the next atlas build; detail `app-integration/T664-pending/README.md`): the 45 stream pieces become one water. Default taken: the 8 oxbow polygons that the source data classes as lake or wetland stay separate, so search will show 1 + 8 results, not 1. Folding them in would turn them into streams, which takes lake rules off them and puts stream rules on (including the spring stream closure the oxbow row exempts). ANSWERED 2026-11-11: fold them in. "They should have been converted to stream polygons like Nicomen Slough so it all becomes one. They aren't lakes, they are considered streams." Going into the next build; search shows one result.
- Nine DFO cuts from your marks (proposal patches in `app-integration/T664-pending/`, applied in the next build): Nakina River, DFO's wording is "from the centre line to the northern bank" between the two signs; a cut cannot take half the width of a river, so the rule will bind the whole channel between the signs (stricter than DFO on the south half). Default taken: whole channel. Owner 2026-11-11: will look later (odd that DFO covers one half). STILL OPEN.
- T668 card wording to confirm (app half built, d199f0d4d; shows with the next release): each salmon row names where its answer comes from. Skeena rows read "Region 6 · A", "Region 6 · B(i)", "Region 6 · Skeena River · B(ii)"; a straddler reads "Region 6 and Region 7 together". The river name shows only where DFO's own heading for the place carries it (so A and B(i) have none). All 137 active Region 6 places carry a lettered section; elsewhere only 1 place in Region 4 and 1 in 5B. Say if the river name should always come before the letter. Phone checks for that release: Prince George no longer lists the Mission "not yet mapped" caveat; rows show "Region 6 · …"; the Nanaimo tidal places appear as "DFO also lists …" rows.
- T668 straddler under a LIVE feed (options on the T668 ticket): from the release a straddler shows the stricter opening; under a fresh live feed with two readable openings of different limits the app shows both regions' words (never looser, but the two disagree). Options: 1 the worker starts a GitHub job that runs the Python comparison after each DFO check (exact; needs the branch on GitHub and a new app reader); 2 Python ships a pair table with the synced inputs (as fresh as the last sync); 3 leave it. Default taken: 3. Pick one.
- Feeds worker defaults taken (T549 DONE, T554 item 5 DONE, T633 7-8, T597 prod twin; staging deployed, version 88587c7d, all three feeds ok): a reworded DFO place reads `check`, not its carried answer; only an `accepted` wording counts, `proposed` is unknown; the stale line for DFO and in-season is 30 h (was 24); the gauge daily refill waits until 03:00 UTC; a failed HYDAT job is started once more after 6 h, then re-told daily in the digest; a place gone from DFO's page raises one urgent owner notice. Say if any should change.
- T666 one release path (24c74a64e; unit-tested, the next real release is the proof) defaults taken: no push unless `--push` (it stops once with the command); a changed golden page stops until `--accept-fixtures <token>`; the escapes (`--no-fast-fail`, `--no-ci-check`, `--ship-stale-atlas`, `--no-follow-tests`, `--no-fresh-copy`) are never remembered, the ordinary flags are until the release is on staging; a fresh-clone no-data test run is part of preflight once per release; `release-perf` runs on the candidate and could stop a release on a loaded machine. Say if any should change.
- T462 screens audit (`app-integration/T462-audit.md`, 85ba2e821): 30 findings, none in open/closed status or a printed number; screens leave regulations out. Fixes in flight as T670 (Keep and Gear tabs), T671 (salmon), T672 (tidal page, search dot, web hubs). One needs your reading, G6: Pend d'Oreille River, printed p.39 "EXEMPT from single barbless hooks" lifts only the Region 4 single-barbless rule in our data, so the province-wide stream rule still shows "Single barbless hook, no trebles" and the exemption has no visible effect. (1) lifts both, barbed/treble allowed; (2) lifts only the regional rule, app right as is. Default: 2 (stricter, no change).
- T671 D2 fix to review (6b02feb06; moved 2 sections toward OPEN): a DFO row `Chinook | To be determined | To be determined` on a narrower Campbell River place was typed with no species and blanked every species to "Check DFO". It now applies to chinook only, so on the Campbell River below the Quinsam coho reads open Sep 1–Dec 31 (`Coho | Sept 1 to Dec 31 | 4 hatchery-marked per day`) and pink Jul 15–Oct 15 (`Pink | Jul 15 to Oct 15 | 4 per day`). Other changes go toward closed or "Check DFO" (Kamloops Lake sockeye closed → Check DFO). Live on staging; release side with the next release. Before the next release someone runs `python -m pipeline.regs.dfo_salmon.feed` (no network, no credits; re-types the scraped rows; the review app reads those files, so Claude runs it when you are not labelling). D3 (chinook annual 10 over 50 cm, Region 8 monthly 4, now on the card; 188392691) shows with the next app build.
- T670 Keep and Gear findings (738f6608f; ticket `tickets/T670-keep-gear-screens-leave-out-regulations.md` holds the printed lines and counts). FIXED with words the app already had, shows with the next app build, wording to look over: K1 a water's own possession limit (643 waters; Charlie Lake "Have no more than 3 walleye in possession. Fish at home don't count.", p.65); K2 release-only fish under a keep limit (85 waters; Peace River "Release every mountain whitefish." under "Whitefish · 15 a day", p.66); K3 Region 3 lake trout release (8 waters; Shuswap "Release every lake trout, Oct 15–Jan 31.", p.28); K4/K5 Region 8 "Brook trout · 20 a day" on streams (1,042 waters, p.68) and Duncan tributaries bull trout 2; G1 "No angling (from a boat)" (81 waters; book prints "No angling from boats", p.51); G3 rules on part of a water (26 waters; Mahood Lake, Cowichan Lake); G4 the 15 mm hook limit (3 waters). HELD for you, nothing changed: G2 the bait heading on a closed day reads "… · not today" above an all-year bait ban (Cowichan River Jul 15; 2,705 waters): (a) "in force today", (b) "No natural bait — all year" (next release), (c) leave. G5 no Licence tab when game fish are closed but salmon is open (Somass, Nov 11); you ruled 11-04 the header goes on a fully closed page: show it when salmon is open, or leave. G7 the 26 classified-water licence terms of p.7 (Dean draw, 8-day cap, Kootenay Class II booking) are on no screen: which should the Licence tab carry.
- T672 tidal page, search dot, web hubs (84d420a08, 464139a6d, 82fb5a758; ticket `tickets/T672-tidal-page-search-dot-hubs.md`). FIXED, to look over: W1 83 named tidal parts said "This part of the water is outside B.C.: no B.C. rules apply." and now show the existing tidal page ("Tidal water: a different regulation system. …"); a map tap never reached the wrong page (search and links did). W2 the search dot on 14 waters with a tidal piece read amber on days every freshwater part was closed, now closed like the map and page (Kanaka, Chapman, Rogers, Nunns, Nathan, McLennan, Cougar Canyon creeks; Nanaimo, Coquitlam, Salmon, Nicomekl, Sooke, Chemainus, Campbell rivers); next release; revert 82fb5a758 if you disagree. W3 fly-only hubs 112 → 175 waters (Region 2: 4 → 49), bait-ban 5,294 → 5,451; next site deploy. G11 a doubled "Classified (Class II)" box said once (55 waters). Default taken: the 121 unnamed tidal sections (link-only) get marked tidal in the store at the next release so they show the tidal page too. HELD for you: W4 the web description names one region for 58 multi-region waters ("Fraser River … (near Quesnel, Region 2)"): (A) name every region, (B) drop the region when more than one, (C) leave. K9 Region 8 streams "Up to 4 brown, cutthroat or lake trout of the 5 from streams" (1,051 parts, four variants of the fish list): reword (e.g. "Up to 4 of the 5 may come from streams") or leave.
- T627 digest email samples to look at (built efe94f572, not deployed): https://claude.ai/artifact/27vC1CRDTBGjNVznacVoQ1 (a quiet day and a bad day, made-up data). Say deploy as is, or what to change. Not checked in a mail app at phone width.
- T628 item 5, closed (d0c00bc9f), FYI: a plain re-parse can no longer replace an entry carrying your review mark; ingest refuses and names the entry and mark, no override. 37 of 1,515 entries carry a mark today.
- T585/T555 gauge card wording to confirm (04316bd91, c58640b86, e4add6e9a; next app build): "Ranked at <time>; this reading <time>." / "Ranked at <time>; this reading is a different one." / "This reading has not been ranked yet." / offline lead "Offline — reading saved on this phone" / on staging and dev builds only, the card foot "Data release r-… · Data source Staging · Channel test". Defaults: an update found at start is applied at the first quiet moment, not before the map opens; the release line is on the gauge card only. T555 item 2 (feeds keyed by format) is now T673 with three decisions listed in the ticket, not built, not urgent.
- App leftovers, three small picks, nothing changed for any: T630 the app's flow chart and the public gauge page's chart differ (window, y scale, tick words "1.5k" vs "1500", x ticks, bands), so one presenter changes one of them: which chart's rules win, one tick wording, does the page gain the median line and third band (table in the T630 ticket; default leave both). T605 what a gauge link opens, since the app has no gauge-only screen (default not built). The kept-reading line says "… saved on this phone" on the web too after a reload ("this device" would fit a laptop; default unchanged).
- T676 reading to rule on (held, nothing built): DFO rows dated "<day> until further notice" (six places on the saved pages). The live feed reads them as a closure from that day with no end; the release file does not read the date and carries the row as an undated advisory. Same days closed today, different words on the screen depending on which copy the phone holds. Options: (1) read it as open-ended in the release too, so both match (Claude's default pick, needs your nod because it is a reading of DFO's words); (2) leave both as they are; (3) make the live feed show it as an advisory too. Ticket: T676.
- Next rebuild side build, measured (results: `handoff/PLAN/app-integration/next-rebuild-results.md`; no unexplained difference). Items for you, all left at today's behaviour unless said:
  - Tidal held lines are now 93, not 61: 32 new. Cohilukthan Slough (880 m) and Deas Slough (225 m) are over 100 m up the channel; Kainet Creek has a braid across its exit; 29 more the precise rule would make tidal that the dry-run pages showed "not cut" (Kwatna 3,125 m, Quaal 2,357, Bella Coola 1,320, Kitimat 1,251, Khutze 1,068, Kemano 813, San Juan 695 and others). All 93 keep build 27's sections and cuts. Ticking the 29 brings 31 more creek cuts and 24 braid cases. Map cards for all 93, the 32 new first: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q
  - Four creeks are newly cut at the tide line (built): FWA lines 354135384, 356070273 (a 6 m piece), Crescent Slough 356326465, 356349946. Look when the staging map is up.
  - Brem River group (5 lines) showed as cut on the dry-run map and is NOT cut: the Brem ends at the sea with nothing mapped below it.
  - Ucona River: the Strathcona Park rule now covers its first 33 m (the old shore cut there is dropped under your D1, and park membership is by whole piece). Say if that is wrong.
  - `lake:329461947`, an unnamed polygon on an Okanagan oxbow channel, stayed a lake (83 rules, unchanged). Fold it into the Oxbows or leave?
  - Dewdney Slough CONFIRMED the same water as Nicomen Slough (FWA has no Dewdney Slough; DFO prints "Dewdney Slough - See Nicomen Slough"). A search-name proposal ("Dewdney Slough" finds Nicomen Slough) is ready, not applied: say yes to apply.
  - Human run after promote: `bash pipeline/regs/parsing/run_parse.sh parse-missing` (nothing is expected to parse).
- Nicomen marks and Dewdney Slough: owner thinks Dewdney Slough is Nicomen Slough (one water); Claude confirms from the source data in the build prep and reports.
- Tidal map pages (same links as the tidal line above): held rivers, one card each https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q ; every cut on the map https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc
- Phone check of the latest Play internal build (0.7.5), the same rows as the Owner To-Do page (written 2026-11-11):
  1. Cold start, then open a water: close the app fully, open it, search a water you opened before, open it; the regulations show (the step the test bed missed; it passed 7 of 7 on the emulator rerun since).
  2. Satellite: zoomed in, a stream opens into two thin edges (T594); too faint or too heavy?
  3. Trout / Salmon / Runs: the key under the map follows the map, no stuck switch (T566).
  4. Gauges: a gauge reporting only the other of flow and level is a ringed grey dot, no text tag (T636).
  5. Okanagan River Oxbows near Oliver: side channels carry the Oxbows entry; the two hillside lines above the cuts do not (T637).
  6. Skeena or Nass estuary: a creek is cut at the tidal boundary; below reads tidal, above freshwater (T660).
  7. Readings (T623): Nechako inside Region 6, Bowron and Willow inside Region 5 closed Apr 1-Jun 30; Peace kokanee 10.
  8. A card warns of a closure on a part the map does not draw; the rule title keeps the book's words for the part.
  9. A water's page opening in one piece (T498/T508, "Not right" on build 24, no note): which tab, and what appears late?
  10. Stellako River (F12): the "10 hatchery steelhead" line was not found on staging with the new release (Regulations tab and its sheets; Salmon view not checked), so no card image exists; tick if you do not see it.
- Gauge wordings to confirm (T554/T633, built on these, in the next app build): "Gauge ranks use another HYDAT" when the feed's HYDAT differs from the release's; an alert on a silent gauge says "gauge not reporting since …" on its row and notifies once after 24 h; an unreadable alerts file shows a note, is reported, and is copied aside before anything overwrites it.
- Next Play build (not 0.7.5): a shared `/water/...` link opens the installed app on that water; without the app, the website (T625; only a Play install can verify).
- Tidal (T665), answered 2026-11-11 on the dry-run map (`app-integration/T665-dryrun-map.html`, sent in chat): cuts that leave nothing tidal are dropped; the 33 creeks are cut where they leave the tidal polygon and the tidal part taken out ("line end wins" withdrawn); Claude applies the same to 59 more creeks of that kind, say if wrong. Default to confirm: a water with its own tidal limit is never cut by the polygon. Still yours: eight main rivers (Bella Coola, Mamin, Apple x3, Kshwan, Sarita, Bedwell) held out until you look at their map page.
- Tidal (T665) rerun with those answers (commit 227c53b08, `app-integration/T665-dryrun.md`): 201 shore cuts (127.4 km tidal below them), 215 lines wholly inside removed (67.7 km). 61 lines HELD until you look, untouched in the next build: the 8 main rivers plus 53 where the polygon runs over ~100 m up a narrow channel (the Pitt-style over-cut). Held rivers, one card each: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q — all cuts on the map: https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc. For each held one: cut at the polygon edge, cut at the mouth (DFO default, straight line between the two most seaward points of land), or leave. Of the 33 mouth creeks: 19 cut, 11 held, 3 not cut. Also proposed, not applied: 24 mouth rows for the tidal limits file (`T665-tidal-limits-mouth-rows.patch`).
- Store screenshots (sent 2026-11-11): pick the order and titles (`app/tools/store-shots.json`).

## Reviews
- **T627 digest email: pick the look (2026-11-11).** Two sample digests (a quiet day, a bad day) are rendered from the
  new layout; say keep / change, then the feeds worker is deployed to staging with it. Not deployed yet.
- **Tidal follow-ups (T359, 2026-11-03):** review app → Tidal → Follow-ups (8). Decide: does a DEM point you
  confirmed become a cut (Caycuse 1,925 m, Dempster 550 m)? Re-mark Annacis/Cannery/Morey/Parsons "whole water";
  look at Skagit (US border) and Bear River (bridge).
- **DFO tab:** Nanaimo "tidal portions": both are below the tidal limit (Cedar Rd bridge), so tidal — T449 makes that a state (owner 11-06).
- **T227 DFO places not on the map (2026-10-28): review app → DFO, open each, "accept suggestion", Save extent.**
  - accept in review app → DFO: Cowichan — Sandy Pool sign → tidal boundary (= FWA mouth; active, high); weir/Cowichan Lake → 66 Mile Trestle (high); CNR mile 70.2 → Skutz Falls (high, new Skutz cut); "incl. tributaries and Cowichan Lake" = whole river + lake + tributaries (medium: lake's inflows counted). Last three dormant.
  - accept in review app → DFO: Capilano — "including tributaries" = whole + tributaries (high).
  - accept in review app → DFO: Kitimat — downstream / upstream of Hwy 37 bridge (high, OSM-checked); hatchery outfall signs = your 08-10 pins (high; west-bank-only width not modelled).
- **Release r-a871447c48ba verification follow-ups (2026-10-28):** (Claude re-cut since; the golden re-extraction of the Q62/Q63 words is moot, golden.json is frozen since T87. Whonnock 08MH100 and Beattie "Trout" answered 10-28: T228.)
- **T228 (2026-10-28):** two DFO queue items from T228. (1) Klinaklini: place "Management Units 5-6 to 5-11 boundary"
  as a split (pre-filled point). (2) Bella Coola: exclude the Atnarko (gnis:17209) from its tributaries.
- **T224 names (2026-10-29, live in r-62e2f3a8c81e):** review app **Names** tab (:5183, `#names/`), reject any that
  are wrong among these 20 auto-accepted names (list + why in tickets/T224 "Owner spot-check"): Cat Stream (applied: a 3 km OSM way
  matches 95 %; within your rule), The Cut Coulee, Cohen Creek, Benes Creek, College Creek, North McNaughton Creek, Brigade Bay Creek,
  Problem Creek, Coho Creek (OSM); Corinne Brook, Jantz Stream, Marshall Creek, Gill Brook, Mckay Creek, Shamrock Creek,
  Smith Brook (Abbotsford); Inkaneep Creek, Meadow Creek Diversion, Quinsam River Diversion Headpond, Noble Creek (ECCC).
  No rejects = done. The rest of the review list (36 high, 552 medium, 8,061 low) is optional.
- **T202 basemap (2026-10-27):** the basemap is now z13 (918 → 274.5 MB). Look at the app at your spots, z10–z18,
  light and dark: trail and FSR names should show from z15. Shots
  and numbers: `handoff/PLAN/tiles/T202-basemap-audit.md`.
- **"Needs a split" follow-ups (2026-10-27, live in r-8b43fb267199):** (1) the three guessed `low` cuts are in the
  review app, **Low-confidence cuts** queue (top bar button; or http://127.0.0.1:5183/?lowcuts) — each entry opens on
  satellite, flown to the cut, with why it is a guess: Chapman lower falls + 100 m below (SCRD survey point, ~576 m below
  the power line), Deena "second bridge" (no bridge mapped: a point 5 km above the tidal boundary), Mamin "third bridge"
  (Mamin Main road, 9.1 km above tidal). Right = Place y; wrong = Place n, "split misplaced", pick the right point on
  the map in the extent fix. (2) Columbia
  r11 "from Mud Lake": FWA names no Mud Lake, so it runs Fairmont → Columbia Lake (the outlet wetland, wbk 329093907, is
  the likely Mud Lake) — say if Mud Lake is elsewhere; (3) Stave r1/r2 (Ruskin/Northrop spawning channels) stay notes:
  the channels have no sections in the atlas — adding them is Claude's work (T222), nothing for you here.
- **T204 phone check:** Kanaka Creek's map now has two marks (112 Avenue bridge, Cliff Park Falls) and its lower part is
  two separately selectable stretches ("… · stretch 1 of 2"); say if the stretch wording should differ.
- **Map look (T64, T85, T113, T152; all built and live):** look at the map (wetland-heavy Fort Nelson and the
  Chilcotin without unnamed wetlands; z13–z18 bends on the Fraser/Vedder now overzoomed from z13; Vancouver's dense z13
  tiles; no OSM creeks under ours at z14+) and say if the look suffers — the basemap's OSM wetland fill is one style layer
  away. Best done in the same sitting as the T202 basemap look above (basemap z13 shipped 10-27).
- **Q4 / T55 + T149 — slough and canal candidates: review app, Sloughs tab only** (:5183, `#sloughs/`). All 2,424
  candidates with their pre-marks; your 150 review-page verdicts show as "page: …" and your 8 free-text notes as
  "your note" (incl. 329097416 Cruickshank, the one whose verdict you cleared: "similar this can be on moat lake").
  811 are confirmed (folded by T322); still open: the 135 wide-marsh pre-marks and the rest of your list.
  The old slough page (artifact Es3Mokt7Fs2yNvm7JgQua8) is retired — do not mark there; its whole database (151 verdicts + 58 spot checks)
  was saved as marks-final-2026-10-28.json and has since moved out of the tree (T322; read it from git history); nothing else reads it.

Cleared 2026-10-28 (already settled, stale-item sweep): Q114 pilot (you dropped it 10-25; you do the real review),
T211 feed plan (built 10-27 with your corrections; only the cron + R2 sync below is left), Q145 (= that feed: 6-hourly
in season), Q32 (T95 dropped; salmon is T164/T211 now), Q142 (lake splits deferred, your 10-28 "lake split later"),
Q150 (answered 10-23: own keeps amber), Q119 (Claude's own audit — Claude re-derives it, T125), T185 decide (your
marks are the decisions; applied, Hatzic left alone), DFO-salmon build + "on parts" rebuild (both live in
r-46be3c135e68), run-timing go (you said go 10-25 → T199; Q144 stays).
Answered 2026-10-24: Q125 (muted fill), T179 (400 m; a curated cut never moves; 25 m lake-anchor proposals; merged
splits keep every name and id), PSF permission (T162; their reply is not stored), braids marked (T67 promoted), Q7 (delete old releases — pruned to live + previous; `python -m pipeline.deliver.release.prune`).
Answered 2026-10-23: Q5 + Q34–Q41 (curation plan approved, defaults; splits mid-review must work; salmon pass later = T132), Q13 (Claude runs the reader gate), Q44 (no Level from a side channel; Flow only if it carries a large share), Q53 (as built), Q45 (leave unbound).

## Your open questions (30 listed below — each built with a default; the table in tickets/README.md has the wording)

Triaged 2026-10-23: 64 smaller calls were decided by Claude and 12 were already answered or obsolete — all recorded in
decisions.md "CLAUDE 2026-10-23 — owner-question triage" (read and reverse any). What is left needs you:

- **T387 point-bound facts (2026-11-04, default: left per tap):** every other fact on a Regulations page is now its
  PART's. Two still follow the tap: the sun line (sunrise / sunset at the tapped point — or the part's midpoint?) and the
  scroll memory (reopens where you left it per tapped reach — or per part?). Ticket T387 has the why.
- **Readings of the book (a wrong call misleads anglers):** Q124 a river's rules on unnamed widenings (456 polygons) ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- **Which gauge speaks (your policy):** Q31 the gauge rule · Q103 diversion
  gauges stop at their canal (24 sections lose every gauge) · Q113 cuts for 38 silent stations (atlas rebuild).
- **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- **"Today" is the phone's day, not BC's (T368 Q6, 2026-11-04):** your answer assumed the app counts days in BC time; it
  counts them in the PHONE's zone (`localDate`): an Alberta phone at 00:30 MT is already on the next day while BC is at
  23:30. Default: unchanged (device day). Switch "today" to America/Vancouver? (a behaviour change)
- **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- **Salmon (T164), refreshed 11-05:** 179 DFO places still need an extent; each has a fresh proposal (52 on existing
  cuts, 47 whole water, 65 new splits, 15 with a reason). Review app → DFO salmon extents → accept suggestion → Save
  extent. Includes Skeena B(i) (T229 Q3) and the 3 items T326 waits on. Claude rebuilds and measures after.
- **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- **Gauge gaps on main rivers (T174, shipped r-bda3fea28838):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters:** Frog/Hidden/Secret/Square (5-6), Redfern (5-15), Squirrel (6-1) — nothing of those names near
  the printed MU; if you know where any is, the proposal file `data/generated/review/t89-awaiting-water.json`
  lists the leads and the fix path.
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled).
- **Things only you can do or know:** Q4 slough marks — review app **Sloughs** tab (`#sloughs/`): 811 confirmed so far; still to call: the 135 wide-marsh pre-marks and 314 on your list (113 intermittent) · Q126 were you
  on Level? · Q9 phone testing · Q26 hosting/app name ·
  Q27 sign-off walk.

## Things only you can run or provide
- **Store listing sign-off (T244)**: privacy drafts APPROVED and the name DECIDED (owner 2026-11-06: "Can I Fish?" on the
  icon only, "Can I Fish This?" everywhere else). Still open from the 10-29 list: `app-integration/store/listing-draft.md` and `store/privacy-labels-draft.md`
  (nothing is submitted). Decide: the **app icon / adaptive icon / feature graphic** (DONE from the favicon, per your answer: `T244-shots/icons-preview.png`; add words to the feature graphic if you want), the support URL, categories. Contact = support@canifishthis.ca and the Location row (declared: collected + shared with Open-Meteo) are decided.
  Look at the new splash: `app-integration/T244-shots/` (web boot screen light/dark, native art); it shows on the next native build.
- **Production run (T240 section 11, 2026-10-29)**: the prod commands for map-assets (T270), HYDAT day flow (T274), the report-purge deploy (T271),
  and the SEO build against the prod app export (`--app`, T269/T279) are written in `tickets/T240-production-launch-checklist.md`.
- **Parser runs** (`run_parse.sh`, dispatch): human-only; any re-parse is yours to start.
- **Yearly synopsis update (T239, not started):** when the new synopsis comes out, Claude writes the runbook + book
  diff and does the dry run; the parse itself is yours to run (Claude hands you the command).
- **R2 upload** of a new release and of the gauge feed, with your credentials:
  `python -m pipeline.deliver.release.publish --release data/release/$(cat data/release/LATEST) --yes` (records
  data/release/PUBLISHED so the prune keeps it). When you set up the feed sync, upload ONLY `data/generated/gauges/feeds/`,
  not `feeds.runs/` (T177).
  T215: a release now also carries OUR synopsis PDF (`synopsis-<sha12>.pdf`, ~24 MB, raw, immutable; named in the manifest's
  `synopsis`): the same publish command uploads it, and the bucket's CORS must allow GET from the web origin (the app fetches it
  once per release to draw the highlight). Until a release with it is uploaded, "open the PDF" stays the plain gov.bc.ca link.
- **Feeds (T225)**: the gauges, DFO salmon and in-season feeds now run on the Cloudflare Worker `bc-fishing-feeds-staging`
  on a schedule (STAGING, switched on 2026-10-28; status: https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status).
  Emails go to your verified address: DFO places / in-season changes / a whole feed failing at once, the rest in one daily
  digest. When a DFO email arrives: `python -m pipeline.regs.dfo_salmon.live queue --regions <n>`, commit the entry files,
  then sync them (below). Production switch-on (worker + prod bucket) is yours.
- **Feed inputs publish from the Mac only** (T520, owner 2026-11-08): `feeds-sync-inputs.yml` is deleted. DFO entries,
  wording, roster and in-season matches go up by `deploy/staging/sync-inputs.sh` — the review app runs it by itself after
  a save (staging only; never git); production stays your `CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh`.
- **OWNER STEP — the GitHub R2 secrets** (T520): `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` were for the deleted sync
  workflow, BUT `feeds-hydat.yml` and `feeds-runtiming.yml` still read them (and `CLOUDFLARE_ACCOUNT_ID`). Remove them in
  GitHub → Settings → Secrets only once those two no longer need them; until then, consider scoping the token to the
  staging bucket (owner 2026-10-29: it covers staging + prod today).
- **`scripts/update-hydat.sh`**: run it when the "New HYDAT" email arrives (after the HYDAT action).
- **Phone testing** (T77, Q9; the 0.5.0 list: handoff/PLAN/OWNER-PHONE.md), **production hosting** (T86, Q26: R2 bucket, contact address, app name), **iOS build**
  (T88).
  - Gesture/navigation parity on the phone (T50, T77; owner 2026-10-27; same RNGH code as web, not seen on a device):
    Android edge-swipe back and the back button through the back table (HANDOFF-ui "Back, everywhere");
    `android:enableOnBackInvokedCallback="false"` in the manifest (predictive back off) is deliberate until judged;
    sideways view swipe on a water (Regulations/Conditions/Map) incl. not starting in the 24 px edge zones and not
    fighting the map pan / chart / minimap shields; sheet drag-to-close and sheet scroll-vs-drag hand-off; iOS
    left-edge back (`EdgeBack`); Spots/Conditions/More tab back goes to the Map tab; part step and upstream/downstream
    (▲/▼) keep one back; section highlight while scrolling the regs tabs; keyboard (search) vs sheets.
  - Native colouring (T74, 2026-10-27; emulator: 0 grey frames in 49 sampled pan frames): on the phone, pan and fling
    at z9–z13 and say whether any water shows plain/grey before its colour (`EXPO_PUBLIC_COLOUR_PROBE=1` logs
    `T74PROBE` lines per pan for the numbers); the five hatches;
    "At my location" (asks on the tap, then drops the pin where you stand); a theme switch keeps the hatches.
  - iPhone (T217, 2026-10-27; simulator: 0 grey frames in 49 sampled pan frames, pill/hatches/At my location seen):
    the same colouring checks on a real iPhone, plus the location system prompt on the tap (pre-granted on the
    simulator) and whether the gauge pill's slightly wider padding on iOS is acceptable.
  - Android on a real phone (T50, 2026-10-27; emulator-verified): the rule-book link opens the highlighted PDF in your
    PDF app with the entry's page FIRST and the whole book after it — say if that reads right; nothing sits under the
    status bar or the gesture / 3-button bar; changing the system font size keeps the water you had open.
  - Accessibility (T243 → T258, 2026-10-29): a TalkBack / VoiceOver walk (map → water → regs → conditions → salmon →
    menu) and the largest system text; the bottom tab labels stop growing at 1.3× (Q160) — say if that is wrong.
  - Regs scroll (T50, 2026-10-29): scroll a water's Regulations, switch to Conditions / another tab / change the font
    size, come back — it should be where you left it.
  - Offline maps (T238/T259): More → Offline maps; which map regions a first install keeps (Q161, default all).
- **Sign-off walk** at the end (T87) before app/design is deleted.

- **iOS (T88, T364, T365; 2026-11-05)** — **BENCHED (owner 11-06) until the DFO extents are labelled and Play closed testing has started; then pay the $99 (pick individual vs organization first) and run TestFlight, not the free 7-day route.** the real iPhone run (T364) is AFTER the launch-and-hygiene chat (owner 11-05). Claude built and tested everything the simulator can show (T313).
  1. Join the Apple Developer Program ($99/yr). Then put two files in `~/.canifishthis/`: `ios-team-id` (the Team ID,
     Membership details) and `asc-api-key.json` `{"key_id","issuer_id","key_path"}` for an App Store Connect API key
     (Users and Access → Integrations → Team key, role App Manager; keep the .p8 outside the repo).
  2. App Store Connect → My Apps → + New App: iOS, bundle id `ca.canifishthis.app`, the name, English (Canada), any SKU.
  3. The name (DECIDED 2026-11-06): "Can I Fish This?" in App Store Connect; the home-screen label stays "Can I Fish?".
  4. Run `deploy/staging/build-ios-testflight.sh` (staging data, for you + testers; `--dry-run` first lists what is
     missing). It uploads to TestFlight; install TestFlight on the iPhone and accept the build.
  5. **Real iPhone run (T364)** on that build: the location prompt on the first "My location" tap; the first offline
     download on Wi-Fi and on cellular (the foreground download, b8e33ca80); the left-edge swipe back from a water's page
     (simulator: a slow drag goes back, a fast mid-height one sometimes does not — say which works for your thumb); the
     gauge pill's padding; the walkthrough to its end, incl. step 17 over the Layers sheet (T336) with VoiceOver on;
     More → Data "Send crash reports" stays on. Memory: Settings → General → iPhone Storage is not it — just say if the
     app is ever closed by itself while you pan or open Conditions (a crash report then reaches the staging store, T362).
  6. App Store Connect forms (T365): App Privacy from `app-integration/store/privacy-labels-draft.md` (confirm the install
     id as Device ID, not linked); age rating; category; support URL; screenshots (6.9" + 6.5"); review notes. Export
     compliance: answer "standard encryption only" (the build already says so). The review build is
     `--production` and needs production hosting (T86) live.

- **SEO, v1 pages (2026-10-27):** keep your existing Search Console property; when the new site is live add the new sitemap
  beside the old one, and for two months watch Pages > Not found (404) and Redirect error (the 19,331 old `/waterbody/` URLs
  should show as Page with redirect). Deploy the generated `_worker.js` with `run_worker_first = ["/waterbody/*"]` (T86 hosting).

- **T371 (batch 7): Play Closed testing (the build is T495's gated production build; 0.5.0 originally).** In the Play Console: create the Closed testing track and a tester
  list (at least 12 people who will opt in and stay for 14 days), fill the app-content forms (Claude drafts the data
  safety answers and listing text). Then roll that build out there; that starts Google's clock for production access.
  Drafts ready (2026-11-04): `play/data-safety.md` and `play/listing.md`. Confirm the items marked OWNER: install id
  means Device IDs = Yes; the report's map centre counts as approximate location; App interactions is new; Open-Meteo
  and Esri sharing (you decided these 10-29); the app name ("Can I Fish?" or "Can I Fish This?"); whether to round
  spot coordinates before they go to Open-Meteo.
