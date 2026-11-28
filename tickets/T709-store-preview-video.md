# T709 — Store preview video from one command

Status: POSTPONED (owner 2026-11-25, on T1072's leftovers: "Drop both for now (Recommended)"; 2026-11-20: after this release; 11-19 "ignore the video for now"). Step 1 Android built: `node app/tools/store-video/run.mjs`.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T709 | Store preview video from ONE command (like T704): Android emulator recording on the final build + staging data, 25-30 s, ffmpeg captions/poster, spec check; iOS version later at Apple's sizes (benched) | POSTPONED (owner 2026-11-25: "Drop both for now (Recommended)"; step 1 Android built) | — | owner 2026-11-13 "we want to generate a nice App preview"; T704, T319, T244 |

Source: owner 2026-11-13: "we want to generate a nice App preview". Neighbours: T704 (store pictures, one command), T319,
T244. Nothing is uploaded by this ticket; the owner uploads.

Owner 2026-11-20 01:56: picked "Postpone" for the store video (with T715) until after this release. Owner 2026-11-21
12:47, asked for the 0.8.5 batch: "No, stay postponed".

## What
One command that makes the preview video(s), like `node app/tools/store-shots/run.mjs`: scripted, repeatable, from the
real app, no hand editing. The owner sees the clip before any upload.

Done, step 1 Android (2026-11-13): `node app/tools/store-video/run.mjs [--out DIR] [--at ISO]` (`run.mjs` + `drive.mjs` +
`checks.mjs`, `node --test app/tools/store-video/checks.test.mjs`), through `scripts/heavy.sh` (`HEAVY_LOCK=~/.claude/heavy-job-2.lock`
when the first lane is busy) on the kept test-bed APK, `data/release/LATEST` = staging's release; five scenes on the
Vedder (map tap → rules; day picker to June 10, closed; Salmon mode; Conditions gauge chart; saved spot "Vedder Crossing
pool"), each checked on screen; 864x1920 / 30 fps / H.264 High 4.0 / silent stereo AAC, 25-30 s, captions, end card,
poster = the coloured map. Output `app/dist/store-video/` (`preview.mp4`, `poster.png`, `report.json`, `work/frames/`).
- This ffmpeg has no `drawtext` (no libfreetype/libass): captions are SVG drawn to PNG by `rsvg-convert` and overlaid.
- 864x1920, not 1080x1920/2400: the emulator is 1080x2400 (9:20) and H.264 Level 4.0 cannot hold 1080x2400; 864x1920 is
  the same frame scaled, no bars.
- Not pinned: live gauge readings and the DFO feed (checked instead).

Choices (owner 2026-11-13: "do the best standard for it what ever would bring most clicks" — picked by Claude on that
brief): captions yes (stores autoplay muted); a silent stereo track, no music; one video; portrait only; poster = the
coloured map, set explicitly; website / share page the same clip (self-hosted, muted, no autoplay on mobile data); the
first 3 seconds answer the app's question on screen, name and icon last; 6–7 scenes, real taps at real speed.

## Work (left, after the postponement)
Step 2, when iOS opens (benched until DFO labelling and Play closed testing have started, owner 2026-11-06): the same
script on the iOS simulator (`app/tools/testbed/run-ios.mjs`, `lib/device-ios.mjs`), scaled and padded to Apple's sizes
(886 x 1920 and 1200 x 1600 first). Play: the owner uploads to YouTube.
Specs (read 2026-11-13 via a fetch tool's summary, re-read before the final cut):
- Apple, https://developer.apple.com/help/app-store-connect/reference/app-preview-specifications: 15 to 30 s; up to 3
  per device size and language; max 500 MB; max 30 fps; H.264 (.mov, .m4v, .mp4), 10-12 Mbps, up to High Profile Level
  4.0, or ProRes 422 HQ; audio stereo AAC 256 kbps, 44.1 or 48 kHz (add a silent stereo track to be safe); poster default
  5 s in. Portrait: iPhone 6.9" / 6.7" / 6.3" / 6.1" 886 x 1920; 5.5" and 4" 1080 x 1920; 4.7" 750 x 1334; iPad 13", 11",
  10.5" 1200 x 1600 (only the largest required size if the UI is the same). The owner's reading ("footage from the iOS
  app") is the safe plan: record the iOS app.
- Google Play, https://support.google.com/googleplay/android-developer/answer/9866151: a YouTube URL of one video (no
  playlist or channel, no timecode parameters), public or unlisted, embeddable, not age-restricted, monetisation/ads
  off; only the first 30 s autoplays; the real app in the first 10 s, at least 80 % the app in use; no black bars on a
  portrait video.

## Acceptance
- One command writes the files at the stores' exact specs: step 1 a portrait H.264 MP4 at 30 fps, 25-30 s, silent
  stereo track, plus the poster PNG; step 2 adds 886 x 1920 and 1200 x 1600 files at 15-30 s.
- A check fails the run on a wrong duration, size, codec, frame rate, file over 500 MB or a missing poster.
- Data: the staging release in `data/release/LATEST` at the time (named in report.json); the waters and the day pinned
  (the `--at` clock, same as T704); content checked on screen before each scene (the water name and a rule line seen)
  so nothing is recorded blank or stale.
- Output outside git (`app/dist/store-video/`); nothing uploaded.

Depends: the final build on Play internal; the emulator free of the release lane; iOS step waits for the bench to lift.
