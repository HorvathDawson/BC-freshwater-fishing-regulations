order: 150

## Things only you can run or provide
- **T595/T596 /crashes reads (run token)**: the feeds worker's `GET /crashes` needs your RUN_TOKEN (403 without it; Claude may not read the token). Run on staging (`https://bc-fishing-feeds-staging.horvath-dawson.workers.dev`) and production (`https://feeds.canifishthis.ca`), or give a session the token to do it: `curl -H "Authorization: Bearer $RUN_TOKEN" "<worker>/crashes?since=2026-11-09"`. Look for: only `defect` kinds listed, none from the 11-19/20 emulator installs (T595); kind 4219b587 (.sqlite.part) absent since 0.7.3 (T596). Then tell Claude, who records both tickets.
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
     (Users and Access → Integrations → Team key, role Admin (App Manager cannot sign for the store); keep the .p8 outside the repo).
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
