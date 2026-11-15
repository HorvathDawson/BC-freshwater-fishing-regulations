# When you are back — 2026-11-15

Asked by you: "record everything i need to do for when i am back what i need to review with questions what was done
etc etc. what needs testings etc etc". This page is kept current by the main chat until you return. The same items are
rows on the To-Do page; reports named here are in `handoff/PLAN/curation-review/`.

Status line (updated 12:50): fixes landed and pushed; cut clean-up and the tributaries-only saves are running; the map
rebuild has NOT started (it waits for the cut clean-up and for the other chat's phone checks); the Android build is on
hold.

## 1. First thing to do
1. Restart the review app once (Ctrl-C, then `bash curation-review/run.sh`). It has new code: tributaries only /
   except a tributary, the tidal boundary as an end choice, warnings, clearer region-boundary names.
2. Read section 2 (questions) and answer what you can; nothing in it blocks the others.

## 2. Questions for you (each also a To-Do row)
Cuts and places
1. Seton River mouth: its own cut 254 m below the Highway 99 bridge cut (recommended), or reuse the bridge cut?
2. Shuswap closed stretch: DFO's two points are 2,054 m apart and look swapped. Use them as printed (2 km closed) or
   as worded (the Water Survey station to 1 km downstream)? Tell DFO?
3. Puntledge tailrace "50 m" cut: same anchor fault as the hatchery fence cut, about 27 m off. Fix the same way?
4. Sheldens Creek: the Spur 19 cut leaves only the top 290 m of a 3.69 km line; the second road (SP 19H) was never
   found. Needs your look on the map.
5. Lillooet fishing-boundary signs: which edge of town does DFO mean?
6. Pallant Creek: you said make the bridge the tidal boundary ("i think"). Being applied; say if not.
Rules of the system
7. A place whose last save is "wrong" with no fix still ships its older extent. Hold such places out of a release
   until re-marked (recommended)?
8. Rest-of extents: a retired place that still has an extent keeps taking water out of the rest (two on Nanaimo), and
   one sibling that cannot be drawn makes the whole rest unmapped. Change either?
9. Should a DFO lake place ever take in the river flowing through it when DFO's words do not name it? Fixed to "never"
   today (Redsand Lake was taking the whole upper Kitsumkalum basin).
10. Sproat-type lake loops: should the small pond the pieces pass through join the lake too? Should a loop a creek
    flows into also move?
Fraser and other channels
11. Add Herrling Island, Seabird Island North and Jeperson side channels to the Fraser (recommended)? Include Maria
    Slough? Add the missing Squamish Powerhouse channel?
One-bank places (Kitimat west bank, Nakina)
12. Six questions on the To-Do row "One-bank places": what "west bank" covers, the other bank, left/right, the stripe
    on wide stretches, tributaries, the rule-book twin.
Readings the proposal maker assumed (yours to confirm)
13. Nass x3 "mainstem waters" = no tributaries. Babine Lake x3 and Meziadin Lake "excluding tributaries" under a water
    whose heading includes them. Somass and Stamp "X River tributaries" = tributaries only.
From the second look at the 31 re-saved places
14. Thompson, Kamloops Lake outlet: the live place has no cut-outs for the three year-round no-fishing areas (Deadman,
    Juniper, Ashcroft); accept them in its closed-areas panel.
Skeena (asked as you left; answers come from the cut clean-up, see section 6)
15. Was there a Hells Gate cut on the Skeena? Confluence ends for "from Lakelse River mouth upstream to …" and "from
    the confluence with the Babine River downstream to …".
App looks
16. Dark-mode highlight on regulated streams: A / B / C (recommended C). Older open rows on the To-Do page (bar height,
    Back on place rows, card opening measure, phone check of 0.7.10) are unchanged.

## 3. Places for you to mark or re-mark (after the restart)
- Qualicum "all open portions" (refused save 2651): re-save with no water ticked.
- Fraser Region 3 mainstem (2578): re-check row 2; it now reads "Region 3 boundary · km 453".
- Nanaimo "500 m below the power line" (2442): carries its neighbour's extent.
- Morice signs (2591, 2592): tied to cuts 100 m off (the cut clean-up may already have re-pointed them; see section 6).
- The two Nass "mainstem" places, Seymour, and the retired Puntledge "from the weir" place: no mark yet.
- Illiance and Wilauks: saved with a new cut on the tidal boundary; should be an open downstream end (the cut clean-up
  re-saves them only if that is provable).
- Tatshenshini, both places: tick ✗ on rows 2 and 4, then "Confirm: the extent matches DFO's words"; or tell the main
  chat to save them.
- Gitnadoix "upstream of powerline crossing": saved as the whole river with the powerline point unused. Intended?
- Kitimat west bank: saved as a whole-width stretch; leave until question 12 is answered.
- After the rebuild: re-tick the three Puntledge marks (1263, 2447, 2687).
- The two Bulkley "all tributaries" places: saved for you as "unsure" from the proposal (they had no mark); confirm.
- The audit's list (section 5) after you have answered the questions.

## 4. What was done today (so you know what changed)
Saved for you, each noted "saved by Claude at the owner's request", your marks never edited, backups taken:
- Stave "downstream of B.C. Hydro dam": tributaries set to off (line 2672).
- Exchamsiks "upstream of Highway 16 bridge": the Highway 16 split restored (line 2728).
- Bella Coola "all tributaries": tributaries only, Atnarko excepted (line 2730). It no longer holds any Bella Coola
  mainstem or Atnarko section (10,048 sections down to 4,994), so mainstem chinook stops reading "no fishing".
- Bella Coola "(not including Atnarko River)", whole: the Atnarko excepted (line 2731), as your note on it asked.
- The two Bulkley "all tributaries of the Bulkley River" places (lines 2732, 2733): tributaries only. NOTE: these two
  had NO mark of yours; they were built from the proposal and saved as "unsure" for you to confirm or change.
Data
- Puntledge: weir cut added; the "75 m downstream" cut moved to 75 m below the weir (was on the weir).
- Redsand Lake bug fixed (2,639 sections down to 4; Kitsumkalum Lake 2,626 to 983).
- Lake loops (Sproat and seven others): pieces leave the river and join the lake, at the next rebuild.
- A named water now takes its own side channels; Fraser "mainstem upstream of the CP…" goes 41 to 50 sections.
- Shuswap closed stretch added to your app as a proposal.
- Proposals redone for 36 rows (tributaries, tributaries only, mouth/tidal ends); the rest untouched.
Review app (in after your restart): see section 1.
App
- Desktop split map (text left, map right) built on its own branch, 7,685 tests pass; not in the app yet.
Tools
- One audit command for the marks; trial run found 303 candidates (section 5).

## 5. The audit (to review later, as you said)
Trial counts: tributaries 120, waiting on rebuild 57, unmarked 50, side channels 26, rest-of 12, offset wordings 11,
mouth/tidal 10, group-saved 8, wrong/unsure with older extent 4, re-saved leftovers 3, one-bank 2. It will be run again
after the clean-up and the rebuild; the final list replaces these numbers here.

## 6. Still running / not done (the main chat fills this in)
- Cut clean-up: duplicates matched, confluence ends, Hells Gate search, bad cuts. RUNNING.
- Tributaries-only saves. DONE (lines 2730-2733); they land in the place files with the clean-up's apply step.
- Side map rebuild with the cleaned cuts, and its checks. NOT STARTED.
- Build step that carries accepted closed areas (Thompson, Shuswap) into the app. NOT WRITTEN; needed before release.
- App fix: BC border as a pickable point; reason shown when a place cannot be edited. NOT DONE.
- Review app speed after a save (rebuild only the saved place). NOT DONE.
- Android build with the code shrinker: ON HOLD. The other chat's background-install check failed three times; it is
  finding out whether the shrinker or last night's loader change (mine) is the cause.

## 7. What needs testing by you
- Review app after the restart: tributaries only / except; "to the tidal boundary" end; the warnings; the Shuswap
  closed-areas panel; Tatshenshini confirm.
- Staging website, once deployed: desktop split map; first river click; search opening on the river's first part.
- Phone: build 32 (0.7.10) check is still open; the next build carries the Upstream/Downstream and search fixes.

## 8. After your answers
Re-run the audit, rebuild and release to staging, your `/code-review ultra`, final build and phone check, closed
testing. Production stays yours.
