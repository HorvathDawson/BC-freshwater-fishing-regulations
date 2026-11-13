# T709 — Store preview video from one command

Status: OPEN (before closed testing; needs the final build on Play internal and a free emulator)

Source: owner 2026-11-13: "we want to generate a nice App preview". Neighbours: T704 (store pictures, one command), T319,
T244. Nothing is uploaded by this ticket; the owner uploads.

## Goal
One command that makes the preview video(s), like `node app/tools/store-shots/run.mjs`: scripted, repeatable, from the
real app, no hand editing. Proposed: `node app/tools/store-video/run.mjs [--out DIR] [--at ISO]`.

## Specs (read from the official pages 2026-11-13; the page text was summarised by a fetch tool, so re-read before the final cut)
Apple, https://developer.apple.com/help/app-store-connect/reference/app-preview-specifications
- 15 to 30 s; up to 3 previews per device size and language; max 500 MB; max 30 fps.
- H.264 (.mov, .m4v, .mp4), 10-12 Mbps target, progressive, up to High Profile Level 4.0; or ProRes 422 HQ (.mov).
- Audio: stereo, AAC 256 kbps, 44.1 or 48 kHz (H.264). A silent file still needs a stereo track (unverified: whether Apple
  accepts no audio track at all; add a silent stereo track to be safe).
- Poster frame: default 5 s in; the owner can pick the frame in App Store Connect.
- Sizes, portrait: iPhone 6.9" / 6.7" / 6.3" / 6.1" 886 x 1920; 5.5" and 4" 1080 x 1920; 4.7" 750 x 1334. iPad 13", 11",
  10.5" 1200 x 1600. If the UI is the same across sizes only the largest required size is needed (it scales down).
- The page does not demand on-device capture in so many words; it says previews are mostly screen recordings and must be
  at the accepted size. The owner's reading ("footage from the iOS app") is the safe plan: record the iOS app.
Google Play, https://support.google.com/googleplay/android-developer/answer/9866151
- The promo video is a YouTube URL of one video (no playlist or channel, no timecode parameters), public or unlisted,
  embeddable, not age-restricted, monetisation/ads off.
- Only the first 30 s autoplays; show the real app experience in the first 10 s, at least 80 % of the video the app in use.
- Landscape or portrait; no black bars on a portrait video.
- Unverified: a required resolution or file size (YouTube's own limits apply).

## Plan in two steps
1. Now, Android: a screen recording of the emulator driven by the test bed, on the final build with the STAGING release
   data. Output for Play (via YouTube, uploaded by the owner) and optionally the website / share page.
2. Later, when iOS opens (benched until DFO labelling and Play closed testing have started, owner 2026-11-06): the same
   script on the iOS simulator, scaled and padded to Apple's sizes (886 x 1920 and 1200 x 1600 first).
Reusable for step 2: the storyboard and its waters/dates, the scene list, the caption text and burn-in step, the
ffmpeg trim/scale/encode/silent-audio stage, the duration/size/codec check, and the poster-frame pick. The test bed has an
iOS twin (`app/tools/testbed/run-ios.mjs`, `lib/device-ios.mjs`), so scene driving can follow. Android-only: `adb
screenrecord` capture and Maestro Android flows.

## Proposed 30 s storyboard (a proposal; the owner edits)
Silent-friendly, short captions (about 4 words) burned in, no claims the fact sheet (`handoff/PLAN/play/listing.md`) cannot back.
1. 0-4 s: the map opens on a coloured river, centred on a pinned place (strongest picture first). Caption: "Every BC water, coloured by today's rules".
2. 4-10 s: tap a water; its Regulations page opens on the stretch tapped. Caption: "The rules for that stretch".
3. 10-15 s: the day picker moves a few days; colours and rules change.
4. 15-20 s: the Salmon colouring / run timing on a salmon river, DFO salmon block shown.
5. 20-25 s: Conditions: live flow chart with the comparison against other years.
6. 25-28 s: a saved spot with its note.
7. 28-30 s: name and icon end card. Caption: "Can I Fish This?".
Not shown: anything not on the fact sheet or only in a later build; no promise of fish. Poster frame: the opening map
(scene 1) unless the owner prefers the regulations page; chosen by timestamp, not by hand.

## How it would be built (found, nothing run)
- Test bed (`app/tools/testbed`): Maestro flows (tap, scroll, search, tab; `flows/`), `lib/device.mjs` emulator control,
  scenarios with pinned steps. `adb screenrecord` is already used (`scenarios/walkthrough.mjs`, `switch.mjs`).
- `adb screenrecord` limits: 3 minutes per file (enough for 30 s); resolution is the device's unless `--size` is given;
  it writes a frame only when the screen changes (see `pane-frames.py`), so the clip must be re-timed to a constant frame
  rate; bit rate settable (`--bit-rate`).
- `ffmpeg` 8.1.2 is installed (`/opt/homebrew/bin/ffmpeg`): trim, constant 30 fps, scale/pad to the size, caption burn-in
  (`drawtext`, check the build has the filter), silent stereo AAC, H.264 High 4.0 at about 10-12 Mbps. `adb` is not on
  PATH here; the test bed finds the SDK's own (`~/Library/Android/sdk/platform-tools/adb`).
- Store-shots (`app/tools/store-shots/run.mjs`, `checks.mjs`) give the pattern: pinned clock, content checks before each
  capture, a report.json, exit 1 on failure.
- The emulator is 1080 x 1920-class; Play needs no exact size, so step 1 can keep the native 9:16 frame.

## Choices (owner 2026-11-13: "do the best standard for it what ever would bring most clicks" — picked by Claude on that brief)
1. **Captions: yes.** Both stores autoplay muted, so the video must read with no sound. Short, large captions, one per scene, kept clear of the top and bottom edges the stores overlay.
2. **Sound: a silent stereo track, no music.** Nothing to license, and nearly every view is muted. Music can be added later without re-recording.
3. **One video.** Play takes one; the first preview is the only one most people see. Spend the effort on its first 3 seconds.
4. **Portrait only.** Matches how the phone app is used and fills the store page on a phone.
5. **Poster frame: the coloured map**, set explicitly (not Apple's default 5 s frame).
6. **Website / share page: yes, the same clip**, self-hosted, muted, no autoplay on mobile data.
7. **Opening:** the first 3 seconds are the coloured map with a tap landing on a river and the answer appearing — the app's question answered on screen before any caption about features. No logo or title card at the start; name and icon come last.
8. **Pace:** 6–7 scenes, each one action, real taps at real speed, no speed-ups that make the app look faster than it is.

## Acceptance
- One command writes the files at the stores' exact specs: step 1 a portrait H.264 MP4 at 30 fps, 25-30 s, silent
  stereo track, plus the poster PNG; step 2 adds 886 x 1920 and 1200 x 1600 files at 15-30 s.
- A check fails the run on a wrong duration, size, codec, frame rate, file over 500 MB or a missing poster.
- Data: the staging release in `data/release/LATEST` at the time (named in report.json); the waters and the day pinned
  (the `--at` clock, same as T704); content checked on screen before each scene (the water name and a rule line seen)
  so nothing is recorded blank or stale.
- Output outside git (`app/dist/store-video/`); nothing uploaded.

## Dependencies
The final build on Play internal; the emulator free of the release lane; T704 done (it is); iOS step waits for the bench to lift.
