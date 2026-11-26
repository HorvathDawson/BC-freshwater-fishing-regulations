# T1132 — A performance monitor in the app, and the privacy words and store forms for it

Status: OPEN — 0.10.1 hardening; scoped 2026-11-25, the owner picked the fullest set, the crash switch, 30 days, memory band + cores

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1132 | A performance monitor in the app, and the privacy words and store forms for it | OPEN — 0.10.1 hardening; scoped 2026-11-25, the owner picked the fullest set, the crash switch, 30 days, memory band + cores | | two steps: the middle set first, then MetricKit and Android slow frames; privacy words to the owner |

Source: owner 2026-11-25, in the iOS chat's App Store walk-through at App Privacy (relayed). He kept "Performance
Data" ticked although the app sends none today (the iOS chat recommended unticking), in his words: "keep ticked but
next release will add to rpivacy page and add the performance monitor". So from the 0.10.0 App Store submission the
label is one data type ahead of the app, until this ships. Related: T1098 (privacy page), T1099 (iPhone privacy
manifest), T1122 (the store forms match the page), T914 and T1049 (web and cold-start speed, which this would
measure), T333 / `no-silent-fallbacks`, `self-host-everything`.

## Settled 2026-11-25 14:26 (owner, multiple choice in the iOS chat, relayed)
"Not linked (Recommended)" for Performance Data on the App Store label. The constraint this ticket builds to:
purpose App Functionality, NOT linked to the user, not used for tracking. The monitor sends no install id and no
push token with its samples; item 3 below is answered. The iPhone manifest test (`app/tools/privacy-manifest.test.ts`)
holds the manifest to the labels table in the iOS submission page: the 0.10.0 manifest has 8 types and the live
label 9 until this ticket adds the manifest entry (`NSPrivacyCollectedDataTypePerformanceData`) and the table row
together.

## Design first, one page, the owner picks (no build before it)
1. What is measured. Proposed: cold start to first usable screen, a water page's load, the map's first frame, a
   data-pack download's time and size, with the app version, platform, OS major version and a coarse device class.
   No location, no water ids, no search words, no device id.
2. Where it goes: the project's own feeds worker (the route reports and crashes already use), never a third-party
   SDK. Sampled and batched so it adds no start-up cost and no wake-ups; the monitor must not slow what it measures.
3. Linked or not: sent without the install id and without the push token, so it is "not linked to you" on both
   stores. If the owner wants per-install trends, that is a different label and a different sentence: his pick.
4. A switch in the app (on by default or off by default), and what the first-run words say, if anything.
5. How it is read: a line in `pipeline reports`, percentiles per version.

## Work, after the picks
1. The monitor in `app/` (web, Android, iPhone, one code path) and the worker route with its storage and retention.
2. The privacy page words (`app/tools/seo/html.ts`), true to what is sent.
3. Play's Data safety form (the owner's clicks) and the iPhone privacy manifest entry (T1099), all four matching:
   the page, the manifest, the App Privacy label, Data safety (T1122's table gains the row).
4. Tests: nothing is sent with the switch off; the payload holds only the listed fields (a test that fails when a
   field is added without the page); a failed send is dropped quietly but counted (it is not a defect report).

Acceptance: the four places agree; a release build on each platform sends a sample the worker stores; the cold
start measured with the monitor on is not slower than with it off (bench). Severity: medium (the App Store label
says something the app does not do until this ships). Release: 0.10.1, by the owner's word, though that release is
otherwise "no features": it is the one exception. Model: Opus (privacy-sensitive payload, worker and native start
path). Size: M. Owner: yes (the design picks; the Data safety clicks).

## Scoped 2026-11-25 (read-only pass, Opus; the owner's picks, verbatim)

**What exists:** the crash client (`app/packages/ui/src/crashReport.ts`, payload `crash/2`, carries the install
id) and the worker (`app/workers/feeds/src/crashes.ts`, `reports.ts`, routes `index.ts:826-870`; R2 only; no IP
kept); `pipeline/reports.py` reads it. All timing code is bench-only (`openProbe.ts`, `startProbe.ts`,
`Map.native.tsx:525` onRendered, `tools/testbed/scenarios/perf.mjs`). The privacy page says "three things"
(`app/tools/seo/html.ts:231`).

**Owner's picks (2026-11-25, planning chat dialog):** scope = **"Fullest"**; switch = "Join the crash switch
(Recommended)" ("Send crash and speed reports", on by default); raw retention = "30 days (Recommended)" (totals
per version kept 13 months); device detail = **"Memory band + CPU cores"**. Not asked, a proposal: no first-run
notice.

**Design:** format `perf/1`, one POST of about 400 bytes, at most once a day per device, never in the same
request as a crash. Fields: app version, test or live, platform and OS major, memory band (under 3, 3 to 6, over
6 GB; "web"), CPU core count, data release, the numbers. NOT sent: install id, push token, time of day, water
ids, location, model, user agent; no identifier of any kind, not even a daily one. Numbers: cold start to map
drawn; water open (first five a session); foreground data update (ms and KB); JS stalls of 300 ms or more in the
first 60 s; and, by the owner's "Fullest", Apple MetricKit launch and hang histograms and Android FrameMetrics
slow frames. Raw notes rolled up once a day by ONE cron (not folded per POST) and deleted after 30 days.
`pipeline reports` gains a performance section: p50, p90, p95 and n per number, app version, platform, device
class, beside the version before.

**Stores:** the App Store label already says Performance Data (App Functionality, not linked, no tracking): no
change; the privacy manifest gains `NSPrivacyCollectedDataTypePerformanceData` (after `app.json:76`);
`docs/ios/APP-STORE-SUBMISSION.md:50-59` gains the row. Play: Diagnostics stays Optional because the
switch exists (`docs/play/data-safety.md:68`). Confirm the 35F9.1 boot-time reason's wording.

**Two steps, one release:** A (the middle set): lane 1 worker (Opus, M: new `workers/feeds/src/perf.ts`, routes,
purge, limits, `tools/wire.test.ts`, `pipeline/reports.py`), lane 2 app (Opus, M: new
`packages/ui/src/perfReport.ts` with a `PERF_FIELDS` list, the hooks, the exit-info module for process start,
the switch words; tests: nothing sent with the switch off, payload keys equal `PERF_FIELDS`; the bench with the
monitor on and off), lane 3 words and forms (Sonnet, S; the page goes live only with or after the app and the
worker). B (the native numbers): MetricKit and FrameMetrics (Opus, M-L), after A is measured not to slow the app.

**Privacy page words (proposal, the owner clears them; they now name the core count and the native numbers):**
"At most once a day the app sends my own server on Cloudflare a short note of how long it took: to start and
draw the map, to open up to five water pages, and to download a data update, how often it froze for a moment,
and the phone's own count of slow starts and slow frames. It holds only the app version, whether it is a test or
live build, the phone's system name and major version (or "web"), a rough size of the phone's memory, the number
of processor cores and the data release. It holds no install id, push token, location, water, search or time of
day, so I cannot connect it to you, to an install or to your other reports, and I do not try. It follows the
"Send crash and speed reports" switch: when that is off nothing is sent. I keep each note for 30 days and, after
that, only totals per app version."

**Risks:** few users make noisy percentiles and narrow buckets; iOS process-start reading and the web's
`timeOrigin` are unverified; the monitor must not slow what it measures; the Play form click is the owner's.

## Owner

- [x] 2026-11-25 — Design picks → scope "Fullest"; switch "Join the crash switch (Recommended)" ("Send crash and speed reports", on by default); raw retention "30 days (Recommended)"; device detail "Memory band + CPU cores"; Performance Data on the App Store label "Not linked (Recommended)" (2026-11-25).
- [ ] 2026-11-25 — Clear the privacy page words (the proposal in the Scoped section) and click the Play Data safety form when the app and the worker ship. Until then the App Store label is one data type ahead of the app.
