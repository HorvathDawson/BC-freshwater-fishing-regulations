# Owner to-do

**The owner ticks items off in the Owner To-Do page: https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f** (owner 2026-11-11). Every open item for the owner is a row
there (collection `todos`: title, detail, group, ticket, order, added, done, note). Claude adds rows with the
ArtifactData tool when work produces something for the owner, and reads `done` and `note` back at the start of a
session. This file keeps the long text and history.

Things only you can do. Claude keeps this list current while you are away; the work itself lives in
`handoff/PLAN/app-integration/tickets/` (README.md is the ledger, and its "Owner questions" table has every question
with the default Claude uses until you answer). Newest first inside each section.

## Decisions that block work (nothing ships until you answer)

- ANSWERED 2026-11-12 (T678 release, size gate): **data size budgets after the pond fold**: "Raise now, shrink later, but the ticket should cover ways to shrink data across the board, not just ponds." Raised: core 1,450,000 → 1,650,000 B gzip, conditions 950,000 → 1,060,000, web first search 1,500,000 → 1,650,000 (measured: core 1,508,127, conditions 963,726 after D1). Shrinking every shipped file is ticket T686 (survey + proposal for you to pick from). RULINGS O5.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `dewdney-four-lines`): **the four Dewdney lines** (FWA 355991890/895/899/902, 1.6 km): "Yes, cover them". The Nicomen Slough rule covers them; nothing to build, the promoted atlas already does. RULINGS K10.
- ANSWERED 2026-11-12 (T678, Owner To-Do row `gauges-11-other-line`): **eleven gauges on a different river line**: "Take the suggestion". Nine keep today's line; Elk River near Natal (08NK016) and Fraser River at Steveston (08MH028) move to the new line. Ticket T682. RULINGS P49.
- ANSWERED 2026-11-12 and BUILT (b7749b726; ticket T671; Owner To-Do rows `t671-5b-aggregate`, `t671-d5-cr-part`): **T671 salmon, the two follow-up questions.**
  (1) Region 5B "in the aggregate" — your words: "5b says this on the ALL row (All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 in Region 5B, unless otherwise stated below (i.e. Region 5 not including the Fraser River watershed).)... So any water body not in the table will be with the total in aggregate (shared total) .. so basically items not in the table in mu 5-6 to 5-11 are shared but other row listed items have their own quota. Also the 4 salmon a day limit in total of all species is still a thing here too". That is what T349 already built in the release: an unlisted 5B water reads "Up to 4 chinook a day in total, all waters of Management Units 5-6 to 5-11 combined" (92,835 sections); a water with its own row (Bella Coola, Atnarko, Dean, …) has its own count; the combined 4 a day is the bucket line. The release does not change. The live feed (feeds worker) had not pooled — it read the ALL row as "4 a day" per water — and now pools the same way (shows with the owner's worker deploy). RULINGS Q29. Not changed, for your DFO labelling: the Dean River (41 sections) and Long Lake (1) have their own rows but those places are not drawn yet, so their sections show the pooled ALL row with the "some spots" caveat.
  (2) The part-headline for catch and release / check — answered "yes": "Closed today · catch and release in part" and "Closed today · check DFO for part", each with a caveat pointing to DFO for where; open wins, then catch and release, then check (RULINGS P47). On today's release: check 11,888 section-days (33 sections of the Fraser and South Thompson in Region 3, whose undrawn places print sockeye "To be determined"); catch and release 0 section-days. No status, bar or map changes. Shows with the next app build.
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T671): **T671 salmon readings.** D1 "Yes to option 1 but add to gotcha list": an All closure beats a species row on the same place unless the species row is dated inside it (RULINGS L24; gotcha in `pipeline/docs/GOTCHAS.md`). Tatshenshini below the border: coho now closed Jul 24–Aug 14 (6,254 sections, 22 days, toward closed; Koksilah, not drawn, likewise); nothing moves toward open. D4 "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate.": the combined 4 is Region 6's bucket line ("Up to 4 salmon a day in total, fresh and tidal waters combined"), each species keeps its own line — that is already how the app reads, so nothing changes (RULINGS Q28). D5 "Closed today · open in part": built (RULINGS P47; 87,419 section-days on 717 sections, the page's own answer kept). D1 shows with the next release, D5 with the next app build.
- ANSWERED 2026-11-12 ("It is this (no Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow ) so not the inlet. The outlet is below the falls. Inlet is all no fishing (every inlet) trout daily quota = 5 (catch and release all other species) when open)"; built: every one of the 15 inlets closed, T690, RULINGS E26, rides the atlas build): **T637: Whiteswan inlet named so its closure stays; say if the name or the piece is wrong** (2026-11-11). Blue line 356560775 above Whiteswan Lake (from 8,623 m up) is now "Whiteswan Lake inlet stream", so "No Fishing EXCEPT below the falls" keeps covering it after the oxbow fix. Ticket T637.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Review app: the checklist is ready (T532, 2026-11-09)** (the two small calls stand as built). Restart it (`bash curation-review/run.sh`). DFO tab = DFO
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
  draft until its release. **0.7.8 changelog: ANSWERED 2026-11-13 ("the 0.78 draft is good")** — `draft` cleared on 0.7.8. Still yours: production has no Play config file (`play.json`) yet (prod transition, T404).
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

Answered 2026-11-11 (later, tidal rulings in T678, T679):
- A fresh stub under 25 m at the top of a line counts as tidal (Ewen Slough, 356242813 become wholly tidal).
- Lines wholly in tidal water are kept in the graph and marked tidal; none is deleted.
- An unnamed widening pond on a river is the river, for whole-river rules and for "in streams" rules (T679 D1, Q154).
- Order of tidal sources: (1) a DFO written boundary, including curated splits that encode one; (2) otherwise coast crossing, where the line enters the sea or a tidal river; (3) the DFO polygon for map shading only. Polygon hand edits and non-DFO splits carry no authority. Verbatim on the splits: "No to this unless the curated splits are the DFO ones. Like all the splits added in region 1 and 2 especially."
- The Fraser's own channels and arms below its tidal boundary are tidal ("We still want all channels and stuff of Fraser below it's tidal boundary as tidal"). Sloughs are NOT covered ("Sloughs aren't channels. Sloughs could be really non tidal?") and are decided one by one.
- San Juan River: the tidal boundary is the San Juan Bridge, by DFO's wording ("... shoreward in a easterly direction of this boundary and downstream of the San Juan Bridge", https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s20-eng.html). ACCEPTED by you on 2026-11-11 ("No just accept that bridge please"): the curated split goes in as accepted, DFO's wording as the source, no review step (T678).
- T627 digest email: approved, deploy as is.

Answered 2026-11-12 (recorded in the tickets; Owner To-Do rows ticked with the answer):
- Owner approved promoting the second tidal candidate (full_t678b). The 17 lake-outlet gauges keep today's placement. The 187 open coast lines stay held at today's result until a grouped proposal is made.
- Salmon (T671, T676): D1 All row vs species row: the All closure wins unless the species row is dated inside it ("Yes to option 1 but add to gotcha list"). D4 Region 6 aggregate: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4. Individual species limits also apply. Species limit is seperate." (combined 4 is a pool note; species limits stay separate lines). D5 undrawn open place: "Closed today · open in part". D2 (Campbell River) accepted. T676 "until further notice": "it is from that day to no end. If the thing is for a closure then it closes etc" (open-ended from that day, doing what the row says, in release and feed).
- Tidal (T678): San Juan: the main-stem bridge is at the river's mouth, so the boundary stays at the mouth; the side channel is tidal below the Deering Road bridge (681 m up it). The 10 curated splits not traced to DFO (Qualicum, Toquaht, Sooke, Chemainus, Campbell River Area 13, Nitinat, Yorkson, Chambers, Noons, Nass): "Keep all as my decisions" (recorded as owner boundaries; this overrides "non-DFO splits carry no authority" for these ten). Kitimat: automatic mouth line. Marble and Tlell: "Mouth for marble river (signs near varney bay) could add it has signs to cut.. for tell say 1.5km upstream bridge option" (Marble at the mouth with a note that signs stand near Varney Bay; Tlell at the bridge 1.5 km upstream, the DFO option naming it: Claude's reading). The four held creek pieces (360586479, 360665787, 360675720, 360812872): the mouth line decides, shown in the T678 diff. Ucona's first 33 m under the Strathcona Park rule: fine. lake:329461947 folds into the Okanagan River Oxbows: yes. "Dewdney Slough" search name finds Nicomen Slough: yes, apply.
- T679: D1 ponds: built then reverted (the bundle refused it: 161 rule keys mixed lake and stream); goes into the next atlas build instead (16,152 ponds change; 97 rules gain, 32 lose). D2 partial salmon coverage: map and bar both give the species verdict. D3 nearer row wins: ignore the loops (receivers with flow-back removed), next build, diff shown. D4 words read at build time: leave, add a guard that stops the build and names the row when a row that matched before stops matching. D5 DFO confluence cuts: same as provincial, the joining water stays out unless DFO's words add tributaries; diff before it ships. D6: Python reads DFO rows tolerantly too (an unreadable row reads "check" and is reported, no longer stops a release). Gauge placement (#4, held): "One rule, accept the moves": atlas placement wins; 29 of 458 shown gauges move at the next build; list reported.
- Screens: T462 G6 Pend d'Oreille: the printed exemption lifts both rules; barbed and treble allowed. T670 G2 bait heading: "Why does it matter the river is closed? It shouldn't show any method or gear or quotas in the regulation card. If salmon are open or something at same time so that river is half closed then it should say in force." (a fully closed water shows no gear/method/quota; where something is open the rule reads "in force today"). T670 G5: Licence tab shown when salmon is open. T668 live straddler: "If python pair table is safe that sounds good we want these to always resolve on their own" (option 2, provided it measures safe). T368 Q6 "today": switch to B.C. time (America/Vancouver). T672 W4 web description: name every region.
- "Accept all as built": feeds worker defaults (T549/T554/T633/T597), T666 release path defaults, T585/T555 and T554/T633 gauge wordings, T182 caps, T532 checklist's two small calls, T630/T605/kept-reading line (leave), T672 K9 (leave), T670 K1-K5/G1/G3/G4 wordings, T672 W1-W3/G11, T668 card wording, T671 D2 fix, the 8 area names cut at 50 characters, T387 point-bound facts (leave per tap).
- Still yours: nothing from this "accept all as built" list; open items are under their own headings below. (Nakina half-river, T670 G7 and T627 answered 2026-11-12.)

Staging: release r-483b295de57c is live on https://staging.canifishthis.ca (site, data, feeds worker). Yours: look at the four new creek cuts (FWA lines 354135384, 356070273, Crescent Slough 356326465, 356349946), and Brem and Ucona, on the staging map.

Open, your call (tidal, T678 / T679):
- ANSWERED 2026-11-12 (build: T685; RULINGS E21–E24): one rule — water reaching the sea is fresh to the shoreline (G1, G2, all of G6, X2, X3, X4); a creek entering a tidal river is cut at the river's edge (G3, X1, X5, X6, X7, sloughs); G4/G5 keep today's cuts; five sloughs whole tidal (Deas, Woodward, McDonald, Tamboline, London). Original ask: Tidal coast proposal: 183 lines in seven groups + 20 Fraser sloughs (T678, replaces the old held-rivers, 46-estuaries, 61+32 held, four-new-cuts/Brem/Ucona and Fraser-slough rows; Owner To-Do row `tidal-coast-proposal`): https://claude.ai/artifact/2VRJAsyeeyMNmCiZpJewVA . Proposals only, nothing applied: G1 59 fresh to the shoreline; G2 17 drop the cut; G3 18 cut where the creek meets the river; G4 10 and G5 3 keep today's cut; G6 63 (E1 to E10) and G7 7 (X1 to X7) one answer per card; S1 to S20 Fraser sloughs, nothing filled in. Answer in chat.
- ANSWERED 2026-11-12 (T678 C, Owner To-Do row `held-five-build27-cut`): five held lines with a build-27 cut and nothing tidal below (354087703, 354154029, 356341234, 360445710, 360861076): "Drop the cuts". Ticket T684. RULINGS E20.

Still open:
- ANSWERED 2026-11-12 and BUILT (4bc03ac14; ticket T676): "until further notice" — "it is from that day to no end. If the thing is for a closure then it closes etc". The release now reads every DFO date cell with the live feed's reader, so both say "from <day> until further notice" (RULINGS D5). No open/closed status moves on today's pages; 6 places not drawn yet change their words. Shows with the next release (the feed already reads it so).
- T668 salmon places from two regions on one stretch (audit `app-integration/T668-audit.md`, 493 sections): built on the defaults D1 an undrawn place stays in its own region, D2 on a section in one region that region wins a tie, D3 a region-wide place binds every section in its region, D4 the card names the region, D5 D1 first. Owner 2026-11-11: a stretch that straddles a boundary is genuinely in both regions, so a straddler keeps both regions' places and the stricter answer per fish speaks (as the freshwater rule, RULINGS Z4); and "straddles should always take the most strict rules from both regions" (limits, sizes, open days, gear, hours). Measured: 432 of 493 sections change, 197 genuine straddlers, 84 statuses flip, all toward closed; shows with the next release. D1 and D4 confirmed by the owner 2026-11-11 ("Yes and yes"), with: on the Skeena the card row names the DFO section as well as the region (the cascade level): the lettered section as DFO's Skeena page prints it (A, B(i), B(ii) …), owner's clarification the same day. ALL ANSWERED; nothing open here until the phone check of that release.
- T668 straddlers, one default to confirm (built, commits dfb4ea7c1 b53d5e193, RULINGS Z4a): salmon openings on a straddling stretch now use the same strictest-wins comparison as the freshwater rules, with ONE deliberate difference: when one of the two DFO regions says nothing about a fish on that stretch, it counts as closed there (freshwater treats silence as 'the base rule applies'). Measured on the 197 straddlers: 19 sections now show both regions' words, none looser, no map colour change, no gear change. Default taken: silent region = closed. ANSWERED 2026-11-11: yes, salmon differs from freshwater here; a silent region counts as closed. The rest of the unification is T669.
- T664 Okanagan River Oxbows as one water (code 25471b393; lands with the next atlas build; detail `app-integration/T664-pending/README.md`): the 45 stream pieces become one water. Default taken: the 8 oxbow polygons that the source data classes as lake or wetland stay separate, so search will show 1 + 8 results, not 1. Folding them in would turn them into streams, which takes lake rules off them and puts stream rules on (including the spring stream closure the oxbow row exempts). ANSWERED 2026-11-11: fold them in. "They should have been converted to stream polygons like Nicomen Slough so it all becomes one. They aren't lakes, they are considered streams." Going into the next build; search shows one result.
- Nine DFO cuts from your marks (proposal patches in `app-integration/T664-pending/`, applied in the next build): Nakina River, DFO's wording is "from the centre line to the northern bank" between the two signs; a cut cannot take half the width of a river, so the rule will bind the whole channel between the signs (stricter than DFO on the south half). Default taken: whole channel. Owner 2026-11-11: will look later. ANSWERED 2026-11-12 (Owner To-Do row `nakina-half-width`): "Whole channel + note": the rule binds the whole stretch between the signs and the card says in DFO's words that it applies from the centre line to the northern bank only. Ticket T683. RULINGS E19.
- ANSWERED 2026-11-12 (T670 G7, Owner To-Do row `t670-g7-classified-terms`): the 26 classified-water licence terms of p.7: "Show on each water": each classified water's Licence tab lists the p.7 terms that apply to it, in the book's words. Ticket T670. RULINGS Li13.
- CLOSED 2026-11-12 (owner: "Close it"; row `t628-marked-entry-guard`): T628 item 5, a plain re-parse can no longer replace an entry carrying your review mark.
- Next rebuild side build, measured (results: `handoff/PLAN/app-integration/next-rebuild-results.md`; no unexplained difference). Items for you, all left at today's behaviour unless said:
  - parse-missing: NOT needed, closed. Checked without parsing (5b5caecd3 lane): 1,393 entries, 0 missing, no reference to a lost water id; entry ids come from the synopsis row, not the water.
- Tidal map pages (same links as the tidal line above): held rivers, one card each https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q ; every cut on the map https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc
- Phone check of the next build: Claude brings one fresh list (folds in the 0.7.5 checks: cold start, gauge ring, key switch, oxbows, page opens whole, part warning, three rule readings, satellite edges, tidal shore, Stellako steelhead line, plus T681 legend card and the T625 shared link). Owner 2026-11-12: "One fresh list for the new build", gone through in chat with pictures.
  - Salmon closed shows the year bar like game fish (Capilano River). Built on your "maybe" (2026-11-13): a water DFO covers whose salmon are closed all year now draws the Salmon (DFO) year bar (closed, today marked, legend) instead of the bare "Closed" word; 751 waters change. Region-wide closures (R4, R5, R7A, R7B) and waters DFO lists nothing for keep the bare word. Say if you prefer the bare word back.
  - More screen: Settings menu, Freshwater Atlas no longer "out of date", no repeated update info. (T703, owner 2026-11-13; next build. Pictures: ~/bcfish-session-backup-2026-11-12/more-settings/)
  - Map zoom carries momentum after a pinch: a quick pinch out or in, fingers lifted while still moving, keeps zooming for about a third of a second (also with the thumb held still and one finger flicking); a pinch held still before lifting stops where it is. (T707, your build-29 report "No zoom flick"; next build, Android.)
- **Phone check after the final build — notification inbox: bell badge, New/Earlier list, open a row, mark all read, an Updated notice** (T706 + T694, owner 2026-11-13: "The inbox can auto be added just add it to the post release checks for me to look at it"). Tap the bell: unread in-season notices under "New", read ones under "Earlier"; tap one (it opens its water's page at the notice and stops counting); "Mark all as read" clears the badge; an amended notice reads "Updated". Pictures (fixture feed, staging waters): `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/notifications/` (light and dark). Nothing to decide unless it looks wrong.
- **Added 2026-11-13 (Owner To-Do rows; every one is a proposal, nothing decided):**
  - Phone checks after 0.7.8 lands on Play internal: `phone-check-078-salmon-bar` (salmon "closed" waters draw the year bar; T678), `phone-check-078-pinch-zoom` (T707), `phone-check-078-inbox` (T694, T706; see also `t706-inbox-phone-check`), `phone-check-078-settings` (More → Settings, T703), `phone-check-078-map-look` (general look).
  - T708 questions: ANSWERED 2026-11-13 (recorded in T708 Owner decisions 6): `t708-haida-gwaii-tidal-mouth` (stays at the mouth for Copper, Deena, Mamin unless a written boundary is listed elsewhere; check found none), `t708-stave-ruskin-channel` (the whole described Ruskin channel; no change), `t708-mcarthur-island-slough` (the whole slough; no change to the stretch; the card keeps the book's place words). Still open: `t708-frog-redfern-squirrel` (look harder; evidence in T708 Work 8).
  - Research questions, one row per note in `handoff/PLAN/research/`: `research-accounts` (8 questions), `research-corridor-imagery` (6), `research-bathymetry` (6), `research-stocking` (6).
  - `store-descriptions-write` (T371): write the short (80 characters) and full (4000) store description from `handoff/PLAN/play/listing.md`.
  - `boundary-name-test-changes` (T678): the boundary names and test pins accepted during the release: Fraser (34 sections), Babine part 5 "juvenile counting weir", Skeena Exchamsiks → Zymagotitz and the Reserve parts (junction fix), Kanaka Creek "→ Source", Mat Creek top end, Coquitlam part names, the Skeena map tests, and the four test updates in 4ecbe1738 (Harrison 3 → 4 rules, unresolved 25 → 29, export codec lake edge as a run's end; plus the regenerated unnamed-stream sweep, −3 attributed). ANSWERED 2026-11-13: (1) Babine part 5 keeps "Juvenile counting weir"; (2) Skeena Reserve split accepted, side channels to be listed where they sit (T710, count first; side-channel name reads backwards, undecided); (3) "Source" stands, header text must match (T711); (4) Fraser cuts + Skeena map tests accepted; (5) Harrison r4 shows the book words with location text (T712); (6) steelhead rules r5b/r6b/r9b trace (T713).
  - `t709-preview-video`: watch the preview clip before any store upload (not ready; waits for the build).
  - `gagnon-williston` ticked: owner answered 2026-11-13 (mouth stays Williston; sign cut about 200 m above the unnamed lake, T708 Work 3).
  - ANSWERED 2026-11-13 (by dialog) and BUILT, released r-f749475d0962 (T708): (a) McArthur Island Slough shows the "in part" form "No powered boats — in part: from westerly entrance to 12th Street entrance to Park"; (b) Lakelse River: rename the end only, the lower stretch's end reads "Mouth (at the signs)", no new cut, no change to which water the rules cover; (c) Tlell r5: yes, take the tidal note off the 13 freshwater tributaries. `t708-mcarthur-lakelse-tlell` (T708, proposals, 2026-11-13): (a) McArthur Island Slough: the catalogue refuses the book's place words beside a whole-water rule; the only allowed form shows "No powered boats — in part: from westerly entrance to 12th Street entrance to Park" (a note on the slough). Accept that, or keep today's "No powered boats" (the book sentence still shows underneath)? (b) Lakelse "signs" at the mouth: needs either the designated mouth located (signs 1.7 km below the logging-road bridge) for a cut, or a new per-water mouth name; which? (c) Tlell r5: take the tidal note off the 13 freshwater tributaries (Work 6 proposal)?
  - ANSWERED 2026-11-13 (by dialog): the Harrison's two vessel entries in build 30 are fine; nothing to change, T712 closes. `t712-harrison-vessel-words` (T712, 2026-11-13): build 30 already shows the Harrison's two vessel chips with the location: "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (your 2026-11-12 "Keep them as two entries"); the pack's "r4 No powered boats" was its own short summary. Enough, or show the one printed line "Speed restriction or no powered boats in small bays along the river as signed" instead?
- **Phone check of build 29 (app 0.7.7)** (2026-11-12; on Play internal; release r-0a9930fb8826). This is the fresh list promised above. Look at these on the phone:
  1. Cold start: close the app, open it; the map comes up with no error.
  2. Gauge ring: a gauge that reports only flow or only level shows as a grey dot with a ring (Kootenay Lake has some).
  3. Map key switch: flip Trout and Salmon (and Runs) quickly; the key under the map always matches the map's colours.
  4. Oxbows: search "Okanagan River Oxbows"; one water, not 45 pieces.
  5. A water page opens whole: search a river and tap Open; the page shows the whole water, nothing loads in late.
  6. Part warning: Shuswap Lake (or Quesnel Lake) shows "Part of this lake is closed today" at the top of the Keep tab (the closure is on a part the map does not draw).
  7. The three rule readings (strictest rule wins on a straddle, "until further notice" dates, a part closed in a lake): say if any read wrong.
  8. Satellite edges: switch the map to satellite and zoom into a stream; the stream opens into two thin edges so the water shows through.
  9. Tidal shore: Deas Slough (the page is the tidal card, "Tidal water: a different regulation system..."), Power House Creek, Suter Brook; each ends at the tidal limit.
  10. Stellako River: the steelhead line reads "Rainbow trout and steelhead: release every one".
  11. T681 gauge legend card: on the Conditions map, a small card over the map says "Gauge reports level only" (under Flow).
  12. A shared link (T625): open a link someone sent you; it lands on the right water.
  13. Nakina River: the card says in DFO's words that the rule applies from the centre line to the northern bank only.
  **Known, do not report: Deas Slough's Map view (the Map tab on that water) keeps the previous camera and says "The parts of this water are not available" on build 29. Ticket T688: fixed in code, ships with the next APK. Pending your word (default built in): the tidal bar reads "Tidal water", the tidal paragraph, and a button "Show regulations for this water".** Also known: searching "Vedder River" then Open lands on the Chilliwack River's part 1: ticketed as T687 (the book pointer above the result opened the wrong water; fixed in the data, 5c294120d, shows with the next release).
- Defaults taken 2026-11-12 (app wave; each is on the Owner To-Do page, group "Look and pick", rows 6 to 13; act only if you disagree):
  1. ANSWERED 2026-11-12 (keep): T677 Ucona River: the Strathcona Park rule now covers the first 33 m of the Ucona. Keep as built, no cut where the park ends.
  2. ANSWERED 2026-11-13 (built): T673: ONE in-season matches file for every release ("Why not just have a single file that does both?"): a notice is placed by the river's map line and metres along it, so a phone on any release places it on its own sections (a section only partly inside shows it too). No per-release copy. Goes live when the main session publishes it with the final build. The "new" mark on an amended notice (T694) is built too, with the inbox (T706).
  3. ANSWERED 2026-11-12 (leave unmarked): T629: "state your FWID" in the Proof of Identity paragraph may be from the later printing. Left unmarked.
  4. ANSWERED 2026-11-12 (all themes): T467: the colour-blind check stays on all themes. The lake-shoreline look stays open for pictures.
  5. T540: "Reading the regulations" stays in AGENTS.md.
  6. ANSWERED 2026-11-12 ("Show the fee only"), built: T670's two basic-licence terms: only the $1.14 fee line for disabled B.C. residents is on the Licence tab (under the basic licence, for a B.C. resident adult, in the book's words); "one annual angling licence per year" stays off. RULINGS Li13b.
  7. ANSWERED 2026-11-12 (stays): T688: wording on a wholly tidal water's map: bar "Tidal water", the tidal card's first paragraph, button "Show regulations for this water". Also answered by dialog: the tidal area stays drawn and labelled but tapping it opens nothing, and the 16 wholly tidal waters stay in search with the tidal card alone (no "SALMON (DFO) Closed" line): built as T692, RULINGS P52.
  8. ANSWERED 2026-11-13 (owner: "fix it"; Claude's choice): Store screenshots (T319); shot 8 now reads "Rules and map work offline" and shows the More data list, each source with its date (Settings has no offline control on the web); order and titles from `app/tools/store-shots.json`: "Can I fish here today?", "Find any water", "Today's rules, in plain words", "River conditions", "Salmon openings", "When the runs come", "Works offline". The "Keep your spots" scene is phone-only and not shot. New set (staging site on r-0a9930fb8826) in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/store-077/`.
  9. T689: an own-rules lake (for example Cultus Lake, which is open) draws orange at 60% over the blue basemap water, which mixes to khaki and sits close to the "Not fishable" swatch in the map key (sewage ponds, tailings ponds, dumps only). Nothing is wrong in the rules; it has looked this way since at least Nov 4. Should own-rules lakes get a colour further from "Not fishable"? Default: leave as is for closed testing. (Also on the To-Do page.)
  10. ANSWERED 2026-11-12 (A: stay outside): T631 item 5, the three Skeena side channels at the Kitsumkalum mouth (360220078, 360220098, 360220099). You said "this i meant the auto mix mouth splitting method we developed we want to use that here if it is splitting tidal from fresh": it is NOT a tidal/fresh split (the Skeena's tidal boundary is 117 km downstream), so nothing changes. Then you asked whether they should be split like normal braids. Facts (pictures A–D in `/Users/dawson.horvath/bcfish-session-backup-2026-11-12/skeena-channels/`): they are ordinary braid channels of the Skeena, but by the map they lie wholly ABOVE the DFO place's top boundary (beside river km 125.5–128.6; the boundary is at 124.0); they look like they straddle it only because one junction in the FWA data carries a distance 3.35 km off its own map point. Options: A keep as today (outside the place); B put them whole inside; C carry the cut across by river distance (lands on none of them, same as A); D fix the bad junction itself (same answer as A, but that rule moves 298 junctions province-wide, so its own ticket). Default: A. Still open: do you still want a cut there? (Also on the To-Do page.)
  11. ANSWERED 2026-11-12 (no list): T678 plan 6, lines the pruning removed from tidal water: you lean to "just add them to the manual list". Count today: 1,204 unnamed FWA lines with no piece in the map data lie in tidal water — 1,133 are FWA "construction lines" drawn inside the Fraser, Skeena, Nass etc. river polygons (the river's own centre lines, not channels), 71 lie in the sea (none named). They have no piece in the map, so a list changes no answer and no picture; it is a record only. Recommendation: no manual list; if you want one, only the 71 sea lines, in a new file beside `data/curated/waters/shore_held.json` (not in it, whose rows are lines the coast cut acts on). Default: no list. (Also on the To-Do page.)
  12. ANSWERED 2026-11-12 (promote with the final build): The candidate map data (`full_next`, built, not promoted): what changes for an angler — searching "West Road River" or "Blackwater River" finds only the river, no longer five lakes as well (Kluskoil, Tsacha, Tundra, both Euchiniko); two Teslin River "Hutsigola Lake" edge ids swap (nothing names them). No rule changes where it applies. Default: it is promoted with the final build unless you object. (Also on the To-Do page.)
  13. T678 plan 6 wording: what "a line the prune took from tidal water" means now the coast rule ignores the DFO polygon. Default: an FWA line drawn in the sea or in a tidal river's polygon that has no piece in the map, listed with its reason (the count in 11 uses this). (Also on the To-Do page.)
- Tidal (T665), answered 2026-11-11 on the dry-run map (`app-integration/T665-dryrun-map.html`, sent in chat): cuts that leave nothing tidal are dropped; the 33 creeks are cut where they leave the tidal polygon and the tidal part taken out ("line end wins" withdrawn); Claude applies the same to 59 more creeks of that kind, say if wrong. Default to confirm: a water with its own tidal limit is never cut by the polygon. Still yours: eight main rivers (Bella Coola, Mamin, Apple x3, Kshwan, Sarita, Bedwell) held out until you look at their map page.
- Tidal (T665) rerun with those answers (commit 227c53b08, `app-integration/T665-dryrun.md`): 201 shore cuts (127.4 km tidal below them), 215 lines wholly inside removed (67.7 km). 61 lines HELD until you look, untouched in the next build: the 8 main rivers plus 53 where the polygon runs over ~100 m up a narrow channel (the Pitt-style over-cut). Held rivers, one card each: https://claude.ai/artifact/3v6g4Ggx8WJZg4Jvxras9q — all cuts on the map: https://claude.ai/artifact/3Jbd8SjT64Tj9HXvPgPBDc. For each held one: cut at the polygon edge, cut at the mouth (DFO default, straight line between the two most seaward points of land), or leave. Of the 33 mouth creeks: 19 cut, 11 held, 3 not cut. Also proposed, not applied: 24 mouth rows for the tidal limits file (`T665-tidal-limits-mouth-rows.patch`).
- Store screenshots (sent 2026-11-11): pick the order and titles (`app/tools/store-shots.json`).

## Reviews
- CLOSED 2026-11-12 (row `t627-read-staging-digest`): T627 staging digest email read by the owner: "looks good".
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

- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T387 point-bound facts (2026-11-04, default: left per tap):** every other fact on a Regulations page is now its
  PART's. Two still follow the tap: the sun line (sunrise / sunset at the tapped point — or the part's midpoint?) and the
  scroll memory (reopens where you left it per tapped reach — or per part?). Ticket T387 has the why.
- **Readings of the book (a wrong call misleads anglers):** ANSWERED 2026-11-12: Q124 a river's rules on unnamed widenings (456 polygons): keep as built now (the polygons take the river's rules); later, fold more small widenings into the stream and draw the stream line instead of the polygon where it is really the stream, real lakes stay lakes (owner's words in RULINGS E27, ticket T695; DEFERRED 2026-11-13 with stocked lakes and bathy map, no longer "after closed testing") ·
  (Q87, Q140, Q62, Q63 answered 2026-10-28.)
- ANSWERED 2026-11-12 ("Leave gaugeless"; "Show 08GC005 above the canal"; built, RULINGS P54): **T696: diversion gauges — no gaugeless section is a diversion gauge's.** The 28 stay gaugeless; 08GC005 now leads the Theodosia above its canal, labelled "measures a diversion" (shows on the next deliver + app build).
- ANSWERED 2026-11-12 ("Leave it": no intake cut; the ~160 m below the intake reads 08GC005 with the river above, as built): **T696: a cut at the Theodosia intake?** (2026-11-12). The canal leaves the river at the intake dam about 160 m ABOVE where the river's section begins (the 08GC006/08GC007 bypass gauge cut). So that 160 m stretch — where the bypass gauges stand — reads 08GC005 with the river above. Add a cut at the intake (rides the next atlas build, which the T690/T691 cuts already need) so it stays 08GC006's? Pictures: ~/bcfish-session-backup-2026-11-12/theodosia/theodosia_after_closeup.png (intake and section end marked), theodosia_before_closeup.png, theodosia_after_overview.png. Ticket T696.
- DONE (answered 2026-11-13, released r-f749475d0962): **T691: five silent gauge stations need a match call before they can be cut** (2026-11-12). Q113 is built (32 cuts, ride the atlas build, RULINGS P53). Not cut: 08FE002 Kemano Powerhouse and 08ND025 Revelstoke Project Outflow (never matched); 08JA013 Skins Lake Spillway, 08NE126 Arrow Reservoir Outflow, 08NJ158 Kootenay Lake Outflow (matches marked wrong, no water). Say which river line each measures, or leave them uncut. Also worth a look: 08JB013 "North Beach Creek above Allin Creek" is matched to "Beach Creek". (Final atlas build 2026-11-12: 08JB013 and 08PA004 Skagit made no cut — 520 m and 1,015 m from their line, over the 400 m limit.) Ticket T691. Claude's proposed answer per station (2026-11-13, NOT applied: a row each, in plain words, in the ticket's "Phase 2 proposals" table; pictures ~/bcfish-session-backup-2026-11-12/gauges/*.png): accept or change each row.
- ANSWERED 2026-11-12: **Which gauge speaks:** Q113 "silent doesn't mean retired so we should add cuts Incase they start transmitting again?" (built, T691) · Q103 "Canal gauge also serves river", then "Leave gaugeless" and "Show 08GC005 above the canal" (built, T696). Still open: Q31 the gauge rule.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **How it looks (taste):** Q130 each model's line under Change · Q132 day chips may still use ELF · Q79 "river" vs "stream" on the page ·
  Q91 "no gauge close enough" wording · Q123 how the Map view marks the selected part (built: dim the others; thin halo in
  `app-integration/T151-shots/alt-halo--*`) ·
  Q2 / Q139 / Q47 no-access and tidal notes on Conditions.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **"Today" is the phone's day, not BC's (T368 Q6, 2026-11-04):** (B.C. time is the settled reading; see the permanent daylight time note.) your answer assumed the app counts days in BC time; it
  counts them in the PHONE's zone (`localDate`): an Alberta phone at 00:30 MT is already on the next day while BC is at
- ANSWERED 2026-11-12 ("It should be in the vessel restriction section but it should say the locator text there"; built c63427bbd, RULINGS P51, shows on the next app build): **Vessel rules (T209):** the 10 close-wording undrawn parts kept ("various locations", "swimming areas") in
  on-parts.md: whole water too, or leave as notes? (`max_power_kw` 7.5 is stored on the 162 electric-only rules, 10-28.)
- ANSWERED 2026-11-12 ("Yes, book's full words"; built 81598325e, RULINGS P51 follow-up, shows on the next deliver + app build): **Harrison River vessel line (T209):** reads "Speed restriction or no powered boats — in small bays along the river as signed"; the other 21 undrawn-part vessel rules checked against the printed book: nothing else dropped.
- ANSWERED 2026-11-12 ("Keep them as two entries"; built 2841fb605, RULINGS P51 second follow-up, shows on the next deliver + app build): **"or" vessel lines as one joined entry? (T209)** No: speed restriction and no vessels are listed separately, each with the book's locator — Harrison "No powered boats — in small bays along the river as signed" and "Speed restriction — in small bays along the river as signed" (Claude's reading: the Harrison too); Okanagan, Kalamalka, Skaha "No vessels — various locations (as buoyed and signed)" and "Speed restriction — various locations (as buoyed and signed)".
- ANSWERED 2026-11-12 ("Yes, read the real boundary"; supersedes the earlier "'Unnamed lake (km 2.3)'"; built 040b79d16, RULINGS P55 follow-up, shows on the next release): **T697: part ends at an unnamed lake** read the real boundary that stands there, never the lake — Slack Creek "Region 8 boundary", Jones Creek "B.C. border"; a cut aliased onto a lake edge keeps its own name (also Duncan "Duncan Dam", Mitchell, Babine "Juvenile counting weir"). One end still has no real boundary: Gagnon Creek at km 3.66 (next row).
- ANSWERED 2026-11-13 ("Mouth is still Williston"; RULINGS P57; built in the T678 junctions candidate atlas `full_next`, shows on the next release): the mouth is where the creek enters Williston, the signs ARE the mouth (cut `gagnon_creek__fishing_boundary_signs_at_the_mouth`, the 400 m cut removed), the whole creek keeps the spring closure, the unnamed lake stays a lake of its own (`waters/pond_calls.json`). Was: **Gagnon Creek: is the unnamed lake really part of Williston?** (2026-11-12). You said: "Maybe we could put signs near the mouth? Let's just assume the mouth is the signs and add it as a variant name. Would this fix it? Seems it is in the wrong spot??", then "Where it enters Williston so it captures the whole stream. Also unnamed lake is part of Williston I think". NOT BUILT: the map data and imagery point the other way — the 58 ha lake (FWA wbk 329465442, coded "lake", Williston is coded "reservoir") sits behind what looks like a rail/road embankment, joined to Williston only by the creek's 74 m channel under it. Picture: ~/bcfish-session-backup-2026-11-12/lake-ends/gagnon_creek.jpg. Build it as you said anyway (the lake is Williston water, the signs at the creek's mouth at km 3.66, the whole flowing creek keeps the spring closure), or keep the lake as its own water and put the signs at the creek's mouth into that lake (km 3.66)? Ticket T697.
- **Salmon (T164), refreshed 11-05:** 179 DFO places still need an extent; each has a fresh proposal (52 on existing
  cuts, 47 whole water, 65 new splits, 15 with a reason). Review app → DFO salmon extents → accept suggestion → Save
  extent. Includes Skeena B(i) (T229 Q3) and the 3 items T326 waits on. Claude rebuilds and measures after.
- ANSWERED 2026-11-12: POSTPONED (owner: close as answered). **User accounts (T223, new 2026-10-28):** one account for web + phone. Questions Q-T223a–f in the ticket: auth
  provider, what syncs (spots / photos / pins / settings), privacy of spot locations, offline merge rule, optional or
  required, before or after first release. Nothing is built until you answer.
- **Batch 7:** Q146 re-centre the map when the parts list opens · Q147 steelhead lake wording on newly taken ponds ·
  Q148 dams → "lake" for the slough candidates · Q149 Bonaparte by hand.
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **Gauge gaps on main rivers (T174, shipped r-bda3fea28838), including Q154 (small ponds):** Q153 (side piece adopts the largest ANSWERED river section) and
  Q155 (a water's own lines between alike pieces) shipped: +407 Flow / +327 Level sections, 0 answers changed — veto =
  one switch each in panels.py. **Q154 is yours:** the "double-line polygons" are small FWA lakes; walking them as the
  river moves 96,590 sections (+95k Flow) — kept off. Numbers in tickets/T174. Your "how small?": examples +
  a proposed pool-only cut (< 1 ha and ≤ 2× the river's width: 10,650 of 136,080, 11,423 sections) in
  curation-review/q154-examples.md. **See them in the review app:** Sloughs tab → "Small ponds (Q154)"
  (`#sloughs/q154/`): the 20 md examples + a 60-pond sample (caught / just missed / clearly not, all regions), each
  on the satellite map with its outline and river; mark each pool / real lake / unsure (keys p / l / ?) and the tally
  shows how your calls line up with the cut — then say yes / no / another cut.
- **T89 six waters (updated 2026-11-13, T708):** Hidden, Secret and Square (= Whitton) Lake are DECIDED (owner "add the 3 high confidence lakes"; name records, applied by the next lane). Still unmatched: Frog (5-6), Redfern (5-15), Squirrel (6-1). Owner: "will look harder for frog and redfern later" (Owner To-Do row `t708-frog-redfern-squirrel`; evidence in T708 Work 8).
- **Run timing (T199, built 10-28 on staging; Q144 answered with the defaults):** (1) run
  `gh workflow run feeds-runtiming.yml --ref redesign/stream-sections -f upload=true` once (uses the repo's R2 secrets;
  staging only) and check its report; (2) after every atlas promote, rebuild the binding base on the Mac:
  `python -m pipeline.runtiming.feed.base --upload --yes`, then run the workflow; (3) when happy, uncomment its
  monthly schedule; (4) look at Run timing on the phone (T199-shots/).
- ANSWERED 2026-11-12 (owner: close as answered / accept as built): **T182 caps (built with defaults, veto if wrong):** offline total 20 MB (was 15); cold-load gate 3.35 MB (salmon on
  the first Salmon press, as you decided 10-28).
- ANSWERED 2026-11-12: **Scope and big rebuilds:** Q10 imagery corridor (offline corridor + pools go/no-go; online Esri is settled): no, offline imagery is not for the first release.
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
     Membership details) and `asc-api-key.json` `{"key_id","issuer_id","key_path"}` for an App Store Connect API key
     (Users and Access → Integrations → Team key, role App Manager; keep the .p8 outside the repo).
     missing). It uploads to TestFlight; install TestFlight on the iPhone and accept the build.
     download on Wi-Fi and on cellular (the foreground download, b8e33ca80); the left-edge swipe back from a water's page
     (simulator: a slow drag goes back, a fast mid-height one sometimes does not — say which works for your thumb); the
     gauge pill's padding; the walkthrough to its end, incl. step 17 over the Layers sheet (T336) with VoiceOver on;
     More → Data "Send crash reports" stays on. Memory: Settings → General → iPhone Storage is not it — just say if the
     app is ever closed by itself while you pan or open Conditions (a crash report then reaches the staging store, T362).
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
- **T691 gauge proposals: ANSWERED 2026-11-13** (all seven; recorded in the T691 ticket, "Phase 2 — OWNER ANSWERED"): 08JB013 rematch to the fork, no cut, name variant; 08PA004 is a Ross Lake level gauge; the five dam/outflow proposals accepted (Kemano stays unmatched). Any earlier "five silent gauge stations need a match call" / "7 proposals" row is closed by this.

- ANSWERED 2026-11-13 (T716): tap speed, open page before the double-tap wait? Owner: "Keep as is (Recommended)". No change; 0.7.9 carries the 0.35 s open.

- [ ] Phone check after 0.7.9 (build 31, Play internal, 2026-11-14), T716: tap a small stream, the Fraser and a lake. The tapped stretch lights up at once; page in under half a second; double tap still zooms and opens nothing. Say if the highlight is too small zoomed out or it still feels slow. (To-Do page row: phone-check-079-tap-speed)
