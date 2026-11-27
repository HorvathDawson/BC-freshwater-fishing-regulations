# T715 — Store video: the command builds its own starting state

Status: POSTPONED (owner 2026-11-20 with T709: after this release; earlier "these are after closed")

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T715 | Store video: the command builds its own starting state (fresh install, release, clock, settings, its own saved spot), checked on screen | POSTPONED (owner 2026-11-20 with T709; earlier "after closed") | — | owner 2026-11-13 ("create a proper state"); before the next regeneration for a store upload; follows T709 |

Source: owner 2026-11-13, after approving the first cut ("the video looks good"): "should make a ticket create a proper
state". Follows T709 (`node app/tools/store-video/run.mjs`, f535ebdca).

## Problem
The first cut passed one full run on an emulator that already held state from earlier work: the saved spot used in the
spot scene already existed, so the path that creates it was never exercised; the installed build, the downloaded
release, the map position, settings (palette, satellite, map mode), the in-season inbox's read marks and notification
permission were whatever was left behind. A run on a clean emulator, or after other test-bed work, may differ or fail.

## Goal
One command gives the same video from any starting point: it puts the emulator into a known state first, and says what
that state is.

## Work
1. A setup step that runs before scene 1 and is checked, not assumed:
   - the named app build installed (version and versionCode recorded), app data cleared, first-run prompts answered the
     same way every time (location, notifications);
   - the staging release named in `data/release/LATEST` downloaded and confirmed on screen (More's data list);
   - the pinned clock (`--at`), light palette, map ground (not satellite), default map mode;
   - the saved spot created by the script itself from fixed data (name, place, note, parking point) in the script's
     config, never taken from what is on the device;
   - in-season inbox and alerts in a fixed state.
2. A teardown, or a note in report.json, so the emulator is not left holding the video's state as if it were a
   person's.
3. report.json records the state it built: build, release id, clock, spot, settings.
4. No silent fallback: if a setup step cannot be confirmed on screen the run fails with the step named.
5. The Maestro driver start retry stays at one, and a second failure is an error.

## Acceptance
- Two runs from a freshly wiped emulator, and one run straight after another test-bed scenario, all pass and give
  videos whose scene frames match (same water, day, colours, spot, captions).
- The spot scene passes with no spot on the device beforehand.
- report.json names the build, release, clock and the spot it created.
- Still one command, nothing uploaded, output outside git.

## Notes
The same setup step is what the iOS run (T709 step 2) will need; write it so the scene list and the fixed data are
shared and only the device driving differs.

Owner 2026-11-20 01:56: postponed with T709 (was OPEN: after closed testing, owner 2026-11-13: "these are after
closed"; before the next time the video is regenerated for a store upload). Owner 2026-11-21 12:47, asked for the 0.8.5
batch: "No, stay postponed".
