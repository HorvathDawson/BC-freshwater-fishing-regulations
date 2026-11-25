# App Store listing text and picture check for version 0.9.0 (T1071, evidence page)

The page the owner pastes from is `handoff/PLAN/ios/APP-STORE-SUBMISSION.md`. This page keeps the counts, the flagged phrases and the picture check behind it.

Written 2026-11-24 by the T1071 docs lane. Nothing is entered in App Store Connect. Counts were made with python `len()`.
Apple limits are from memory of App Store Connect (check the live screen). Source of words: `app/store-listing/listing.json` (owner decision 2026-11-14).

# Part A: text to paste

## Fields

| Field | Limit | Count | Status |
|---|---|---|---|
| Name | 30 | 16 | fits |
| Subtitle | 30 | 30 | fits, exactly at the limit |
| Promotional text | 170 | 74 | fits |
| Description | 4000 | 3924 | fits, 76 spare |
| Keywords | 100 | 100 | fits, exactly at the limit (commas counted) |
| What's New | 4000 | 385 | fits |

### Name
```
Can I Fish This?
```

### Subtitle
```
BC fishing rules, by the water
```

### Promotional text
```
Your pocket guide to BC fishing regulations. Less searching, more fishing!
```

### Keywords
```
regulations,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge,map,synopsis,dfo
```
Notes: "fishing", "BC" and "rules" also sit in the name and subtitle; Apple indexes those fields already, so those three words may waste about 17 characters. Optional, not changed here (the owner picked this list). "license" is the US spelling; a Canadian searcher may type "licence". Both fit only if a word is dropped. Not verified how Apple weighs this.

### What's New (proposal, version 0.9.0, the first App Store version)
```
First release on the App Store. Tap any lake, river or stream in British Columbia to see its fishing regulations for the day you pick. Also in this version: river conditions and forecasts, salmon limits and run timing, flow alerts, saved spots with photos and notes, and offline use after the first download. The official regulations always take precedence; check them before you fish.
```
Why not the 0.9.0 changelog bullets: they describe changes from 0.8.5 (Sources tab, Class II line, bell). No App Store reader has seen 0.8.5. The proposal is built from the description's feature names.

### Description
The paste block is in `handoff/PLAN/ios/APP-STORE-SUBMISSION.md` (section 4): the Play "Long description" of `app/store-listing/listing.json`, unchanged, 3924 characters.

### URLs and other fields

| Field | Value | Status |
|---|---|---|
| Support URL | https://canifishthis.ca/about | HTTP 200, 5935 bytes (checked 2026-11-24) |
| Marketing URL | https://canifishthis.ca | HTTP 200, 25706 bytes |
| Privacy Policy URL | https://canifishthis.ca/privacy | HTTP 200, 16827 bytes |
| Copyright | `2026 <OWNER'S LEGAL NAME>` | the name is the owner's: individual developer account, the App Store shows his legal name as seller (START-HERE-ios.md, owner answer). I do not know it |
| Primary category | Navigation | decided |
| Secondary category | Sports | decided |
| Primary language | English (Canada) | decided |
| Bundle ID | `ca.canifishthis.app` | `app/apps/mobile/app.json`, matches |
| Contact email for review | support@canifishthis.ca | from `listing-draft.md` |
| Price | Free | owner, App is free (see flag 3) |

Privacy page text (read 2026-11-24): it names notifications ("on an iPhone with notifications on, a push token so I can tell the app to check for new data"; a "Push token" section: kept only with whether it is for Apple's test or live push service, deleted when notifications are turned off) and crash reports (on by default, switch at More, Settings, Privacy, "Send crash reports"; error records deleted after 90 days). Both match the review notes.

## Flagged phrases and facts

1. Other platforms: none found. A search of the description for Android, Google, Play, iPhone, iOS, free found no match. Nothing to replace.
2. "official": appears only as "NOT an official source" and "official regulations ... take precedence" and "not affiliated with ... Province of British Columbia or DFO". These are disclaimers, not claims to be official. Keep. They also match Apple's wish that a non-government app not imply government backing.
3. Price words: the description does not say "free". Nothing to change. Pricing is set in App Store Connect (Free).
4. Download size: the description says "approximately 500 MB". The app's own hint says "about 500 MB" (`app/packages/core/src/install.ts:219`). Measured first downloads in `handoff/PLAN/ios/START-HERE-ios.md` were 479 to 482 MB (0.7.x data). No file states a 0.9.0 figure. Use "about 500 MB" in both places: change the review notes (`ios/store/review-notes.md`, two spots) and `testflight-external.md` from "about 480 MB" to "about 500 MB". Latest release manifest `bytes` field is 225 MB for the data part only; I did not total the tiles and basemap, so a true 0.9.0 total is not verified.
5. Claims the 0.9.0 app may no longer match: not verified line by line. Checked: share link (associated domains `canifishthis.ca` are in app.json), flow alerts (More, Notifications), photos stay on the phone. The 0.9.0 changelog changes the bell and the Sources tab, none of which the description names, so nothing it says became false. Not verified on a running iPhone build.
6. "Water bodies are colour-coded ... for the date you select" and "River forecasts": taken from the Play text, which T319 says was checked against 0.8.2 code. Not rechecked against 0.9.0.
7. "federal hydrometric monitoring data": the river forecasts are BC River Forecast Centre per review-notes. The sentence says "live federal hydrometric ... including River forecasts". Slightly loose, not false for flow and level. Optional rewording, not made.
8. App Review notes also say the first download is about 480 MB: see 4.
9. `app.json` says buildNumber "44" in this worktree while the docs say build 45 is uploaded. The build script probably bumps it; not verified.

# Part B: pictures and icon

## B1. Is T1072 done?
**Not ready.** `T1072-store-screenshots-and-marketing-content.md` is `Status: OPEN`. Its only commit on main is the intake commit e762a2b39 (PR #129, merged). No T1072 branch exists and `gh pr list --search "T1072" --state all` shows only that intake PR. T1076 (the reshoot tool cannot be followed) is also OPEN. The 0.9.0 reshoot run (2026-11-24 13:01) failed: 07-saved-spot timed out and 20-help showed the website's lines; no set was delivered.

## B2. Screenshot files on disk (main checkout and backups, read only)
Newest complete iPhone set: `~/bcfish-session-backup-2026-11-12/store-t319-final-082/`, written 2026-11-19 02:42, rendered when 0.8.2 was landing on staging. It is T319 set 4 final, 7 shots. It is NOT from 0.9.0 and is not in the repo. No version string is stored in the files.

| Folder | Files | Pixels | Alpha | Format | Date |
|---|---|---|---|---|---|
| `store-t319-final-082/appstore-6.9/` | 01-01-map, 02-18-regs-trout, 03-21-rivers, 04-16-split-and-rules, 05-22-salmon-runs, 06-23-spots-offline, 07-06-search (7 PNG) | 1320 x 2868 each | no | png | 2026-11-19 |
| `store-t319-final-082/appstore-6.5/` | the same 7 names | 1242 x 2688 each | no | png | 2026-11-19 |
| `store-t319-set4-web/appstore-6.9/` and `appstore-6.5/` | same 7 names, earlier render (web only, current UI not 0.8.2) | 1320 x 2868 and 1242 x 2688 | no | png | 2026-11-19 01:33 |
| `handoff/PLAN/app-integration/store/graphics/store/light/appstore-6.7/` | 7 old set-1 shots (map, search, regulations, conditions, salmon, runs, offline) | 1290 x 2796 | no | png | 2026-11-05, stale |
| `.../light/appstore-6.5/` | same 7, old | 1242 x 2688 | no | png | 2026-11-05, stale |
| `.../light/appstore-5.5/`, `appstore-ipad13/` | old, not needed (iPhone only) | not read | not read | png | 2026-11-05 |
| `app/dist/store-shots/play-phone/` | report.json only (failed 0.9.0 run) | none | none | none | 2026-11-24 |

Per-file pixel size and alpha were read with `sips` for every file in the first five rows. All were PNG, no alpha. The folder `store/light/` default of the push script is the OLD set (it still has `appstore-6.7`, which the script no longer looks for; it wants `appstore-6.9` and `appstore-6.5`). The push script run with its defaults would find no 6.9 folder.

If you want a fresh 0.9.0 set (do not run here): the T319 command is
`STORE_SHOTS_SITE=<store-shots web export of the commit> node app/tools/store-shots.mjs --theme light`
where the site is a web export built with `EXPO_PUBLIC_STORE_SHOTS=1` and served locally. That is exactly what T1076 says the runbook step does not tell you; it is not fixed yet. Then check with `.venv/bin/python deploy/staging/appstore-screenshots-push.py` (plan, sends nothing) and, for the owner or a Claude run with the owner's go, `--commit`.

## B3. Check against Apple's screenshot rules (from memory; I did not fetch Apple's page)
Rules used: 6.9" takes 1320x2868, 1290x2796 or 1260x2736; 6.5" takes 1242x2688 or 1284x2778; one of the two is required (6.9" alone is enough); 1 to 10 shots per size; PNG or JPEG; no alpha; no iPad shots when the app is iPhone only (`supportsTablet` false). The push script encodes the same rules.

| Set | Count | Size | Alpha | Format | Result |
|---|---|---|---|---|---|
| final-082 6.9" | 7 | 1320x2868 | no | PNG | PASS (all 7) |
| final-082 6.5" | 7 | 1242x2688 | no | PNG | PASS (all 7) |
| set4-web 6.9" / 6.5" | 7 / 7 | as above | no | PNG | PASS |
| old light/appstore-6.7 | 7 | 1290x2796 | no | PNG | PASS on rules, but it is the old set-1 look, do not use |
| old light/appstore-6.5 | 7 | 1242x2688 | no | PNG | PASS on rules, old, do not use |
| iPad | none | n/a | n/a | n/a | not needed |

Format passes are technical only. Content is stale for 0.9.0 (rendered before 0.8.5 and 0.9.0 UI changes) and the owner has not picked a direction under T1072. No fails on size, alpha or format.

## B4. Has anything been uploaded to App Store Connect?
The docs say no. `OWNER-TODO.md` Q6 [T319]: "nothing was uploaded (a permission block)"; `STATUS.md` says the same; `ios/store/checklist.md` row 18 says "plan run, nothing sent yet". No doc says `--commit` ran. App Store Connect itself was not read (`--check` needs the API key; not run). Not verified against the live account.

## B5. App icon
| Item | Fact |
|---|---|
| `app/apps/mobile/assets/icon/icon-1024.png` | 1024 x 1024, hasAlpha yes (sips) |
| `app.json` `icon` | `./assets/icon/icon-1024.png` (line 6). No separate `ios.icon` key found |
| What it implies | Apple rejects a store icon with alpha. Build 45 uploaded and processed, so the icon in the binary passed Apple's check. Likely Expo flattens the icon when it makes the asset catalog; I did not confirm that in the build script or on the built icon. Not verified |
| Upload needed | No separate icon upload for an Expo build: the App Store icon comes from the build's asset catalog |
| Old checklist row 21 | says a flattened copy is made "at upload time". For an uploaded and processed build, no action is needed unless App Store Connect shows a missing icon. Not verified on the live page |

## B6. App preview video
Optional. T709 is POSTPONED (owner 2026-11-20: after this release; 2026-11-19 "ignore the video for now"). The only clip is Android footage (`~/bcfish-session-backup-2026-11-12/store-t319-set4-layout/preview-appstore-886x1920-from-android.mp4`). Apple wants footage of the iPhone app, so it is not sent. No iOS driver is written. Submit without one.
