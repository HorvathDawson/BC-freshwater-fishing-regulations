# Restart page: the 0.10.0 release chat (roadmap row 2)

Rewritten 2026-11-25 23:30 MST by the 0.10.0 chat (it was the "0.9.1 follow-ups" chat; the release was renumbered
0.10.0 by the versioning rule). The planning chat owns ticket numbers: this chat numbers nothing and sends it finds.

## Where it stands

**0.10.0 is HELD and will be re-cut from main.** Owner, 2026-11-25 late, relayed by the planning chat: "we can redo
release later and also add on other changes to the release. like hardening items and patches."; production: "Yes,
wait; speed first (Recommended)"; overnight: "Skip staging; finish clean-up + repo-move (Recommended)". The re-cut
waits for the release-speed work (T1165, T1144, T1161, T1135, T1133).

- No `v0.10.0` or `-rc` tag exists. `data/release/LATEST` is still r-0aec88ed3edb. Staging and production are
  untouched by 0.10.0. Nothing was shipped anywhere.
- `release/0.10.0` (head ac4f3bedb) is retired as a release. Do NOT delete it or merge it back. What is only there:
  the cut and What's-new draft (redo at the next cut), the fixtures re-cut and four pins on r-71d09ad82e81 (drop),
  three ticket-file copies and one moved pin (drop). Everything else on it is a pick of main.
- The candidate r-71d09ad82e81 on disk is stale (cut before T1146 and T1140); delete it when disk is wanted.
- The new atlas was promoted on 2026-11-25 (parity: one new item, wbk:329504289 "Anderson Lake").

## Merged to main for this release

#155, #164, #185, #187, #189, #112, #194, #179, #166, #114, #212, #218, #260 (T1145 open colour), #264 (T1146 side
channels go with their water), #270 (privacy page names 0.10.0: revisit the version at the next cut), and #265
(T1140 part names) once main's pipeline tier is green again.

## What the next cut must do

1. Cut from main (`python -m pipeline release --cut <version>`); write the What's-new bullets. The 0.10.0 draft:
   - Regulations: a river's rule now reaches the lakes at its head, and a creek with its own line in the book keeps
     it where the river below is closed
   - Gear: a water says when treble or barbed hooks are allowed, ice fishing lists only what differs, and a fully
     closed water says so once
   - Map: a closure on one bank of a river is drawn on that bank only
   - Trout and char: tap a row to see every rule behind it and its rule-book pages
   - Links that name a river gauge or a stream by its code now open the right water
2. A full data build. The binding diff to accept must equal: the 46 head-lake rules (T1053) plus T1146's list
   (`handoff/PLAN/app-integration/T1146-files/what-changes.md`: 12 rules, +56 sections; licensing babine_river +20;
   DFO 1:cowichan-river +9) plus T1140's names, and nothing else.
3. Re-cut the fixtures and move every release-backed pin in ONE go: the T666 fixtures, the T1053 pins (glossary
   count, Glenn Creek sheet, the unnamed sample's page, Williston's why-line), `tools/seo/t1140.release.test.ts`,
   and `app/packages/ui-native/src/water/fixtures/t1140-single-part.json` (it reads the regs fixture, so it moves at
   every re-cut: regenerate it by the test's own render and check only data lines changed).
4. A release branch needs the ticket files of every ticket its commits name, or fast-fail's ledger check stops the
   preflight.
5. After the build's in-season step, run the in-season comparer; on the candidate re-check: Forsyth Creek's 991 m
   side channel is no longer open Sep 1–Oct 31; the Shelagyote channel carries the Babine Class I licence.
6. `minAppVersion` must not rise to the new version (the rehearsal derived 0.8.4).

## Timings measured on 2026-11-25 (the "before" for the speed work)

No full run finished. Measured parts: build stages reach + tiles + deliver about 33 min; the follows about 45 min
(93 pytest chunks; one run was stopped by me at chunk 59 on a plan change); `scripts/fast-fail.sh` 400 s;
the pre-push hook about 400 s, and it failed once on a 40 s vitest timeout under load
(`WaterScreen.sources.test.tsx`; a retry passed). The Mac slept 03:50–12:06 with the lid closed and stopped an
unattended run (T1139). Pushes from the main checkout fail on a stray untracked file (T1137).

## Left for hardening or the first patch

- T1150: 11 city lines share an FWA line's code (notes: `handoff/PLAN/app-integration/T1150-files/`).
- T1151 braid simplification. Creston Valley WMA and Strathcona park over-reach.
- T1140 review leftovers: a several-channel name or label can say "joining" for a channel that does not join (no
  real name does today; the range labels do); a keyed part missing from the map's places is reported once but
  nothing shows on screen; no test runs the real v0.9.0 code (0.9.0 shows "Noons Creek : Turner Creek (Port
  Moody)" with a stray space); a city stream with an OSM or excluded-FWA name gets no own-name row.
- Owner's, unchanged: TestFlight 0.9.0 (45) external submit; every CONFIRM_PROD command.
