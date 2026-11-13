# T673 — One in-season matches file for every release (FWA line + metres, not section ids)

Status: DONE (785fc9914) — built 2026-11-13 for this release. Takes effect when the main session publishes the /4 file
(feeds worker deploy, then `deploy/staging/sync-inputs.sh`) with the final build. Run timing (the old item 5) is not part
of it: still bound to one atlas's `section_handles` (see "Not in this ticket").

Source: T555 item 2 (data pipeline review 2026-11-09). In-season notices are BC Fish and Wildlife in-season regulation
changes, not DFO.

## The owner's decision (2026-11-12/13)
The old proposal here was a second, per-release copy of `matches.json`. The owner rejected it:
- "I still don't know why we need 2 in season for that?"
- "Why not just have a single file that does both?"
Then, to the proposal of ONE file that places each notice by the river's permanent map line and distances along it (FWA
blue line key + from/to measure in metres), so any phone on any release can work out which of its own sections fall in
the range: "Okay yes that sounds good please add that to this release".

## What was wrong
`feeds/inseason/matches.json` (`inseason-links/3`) named ONE release. The app used its partial-water placement only when
that release was its own; a phone on any other release showed the notice on the whole water and reported "off-release".
Every release that went out stranded the phones not yet updated.

## As built (`inseason-links/4`)
```json
{"format": "inseason-links/4", "digest": "…",
 "notices": {"is-00172e79bde1": {"waters": {"gnis:9343": {"runs": [[356566667, 0, 4681]],
                                                          "ends": [["mouth", "alexander_creek__easternmost_hwy_3"]]}}}}}
```
- Keyed by the water's durable item id (AGENTS 5). A whole-water match is `{"all": true}`. A part is `runs`: FWA blue
  line key + from/to metres. No release, no section id, no part index. `ends` are tokens the reading release's
  `display.ends` says (unchanged since T517).
- **The rule on the phone** (`@app/core` `inSeasonTouches` / `inSeasonCover`; Python `resolve.section_touched` /
  `touched`; one fixture `inseason-cover.parity.json` holds both): a section is covered when its line `[from, to)` on the
  range's stream OVERLAPS the range — wholly or partly inside. The app reads each section's line from the release it
  already ships (`lines`, `LineReads.sectionLine`: blk, from, to); no new shipped table, no size gate moved.
- **A section only partly inside** (the stretch's end is not on a section boundary in an older or newer release): the
  notice is shown on every section the range touches, with its "Where: From A to B" words. Never dropped. A range that
  touches nothing the phone holds shows on the whole water and is reported. A notice whose every water this release does
  not hold goes to the bell (T693, unchanged).
- **Lakes on a river** (a lake piece has no line in any release): `lakes: true` = every lake piece of that river. The
  resolver refuses a match covering only some of them (none today: 5 waters, all full). On another release the flag
  covers whatever lake pieces that release has.
- **Unknown format**: the app refuses the whole file. It reports "in-season matches.json: <format seen>, but this app
  reads inseason-links/4 — refused …". The last links it read answer meanwhile; with none, every notice goes to the bell.
  The feeds worker refuses another format in its run (shown on /status) and lists /4 as its input format.
- **Publisher guards** (`pipeline/regs/inseason/publish.py`): an unresolved match and a stale stamp are still refused.
  The old "resolved on a release that is not LATEST" refusal is replaced by: every water is held on LATEST; every range
  touches its water there; on the release the file was resolved on, every range starts and ends on its sections'
  boundaries (a measure shifted by a metre is refused, the mutation test); every end token has words in LATEST's
  `display.ends`.
- `notice_waters.json` was re-resolved on r-0a9930fb8826 with the new resolver. Only `lakes` lists became `true` and one
  stamp moved; no match changed. The runs read back identically under the touch rule (the resolver proves it).
- The feeds worker's count of matched notices reads the waters' keys. It works unchanged on /4 (tested).

## Old installed apps (build 29, `inseason-links/3` only)
After the /4 file is published, build 29 refuses it and reports it once per poll. It keeps the last /3 links it captured:
notices it already had stay placed as before while its release is the one they named. New or re-matched notices, and a
fresh install of build 29 (no capture), go to the bell ("not on the map yet"). Nothing breaks and no notice is hidden.
**No old-format file is published alongside.** Build 29 is on Play internal only (the owner's phone), it updates with
this release, and a second file is the copy the owner rejected. If a /3 reader ever had to be kept alive, it would be a
compatibility shim needing the owner's approval and a removal date. None is built.

## Deploy order (main session, with the final build)
1. Deploy the feeds worker (it lists `inseason-links/4` on /status.json `inputs`; until step 2 its in-season run fails
   on the /3 file, shown on /status).
2. `deploy/staging/sync-inputs.sh` right after (it runs `pipeline.regs.inseason.publish`; the format gate refuses /4
   until step 1 is live).
3. The app build (reads /4). Before step 2, a new app sees /3, reports it, and sends every notice to the bell.

## Tests
`pipeline/tests/test_inseason_publish.py` (32: ranges resolve on the release under test; a shifted measure is refused; an
off-water range is refused; publish from an older release), core `inseason.test.ts` (one file on two releases' section
sets with moved boundaries; partial overlap on both touched sections; the /3 file refused with its sentence), data
`feed/inseason.test.ts` (a fresh device meeting /3), ui `inseason.render.test.tsx`, worker `events.test.ts` (count on /4).

## Not in this ticket
Run timing (`runtiming/index.json`, bound to one `section_handles`; "stale" on another atlas) is unchanged. It needs a
decision of its own.
