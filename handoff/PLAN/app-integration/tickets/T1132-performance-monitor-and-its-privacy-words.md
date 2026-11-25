# T1132 — A performance monitor in the app, and the privacy words and store forms for it

Status: OPEN — 0.10.1 (owner: "next release"); a one-page design first

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1132 | A performance monitor in the app, and the privacy words and store forms for it | OPEN — 0.10.1 (owner: "next release"); a one-page design first | | owner 2026-11-25 (iOS chat); the App Store label already says Performance Data |

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
