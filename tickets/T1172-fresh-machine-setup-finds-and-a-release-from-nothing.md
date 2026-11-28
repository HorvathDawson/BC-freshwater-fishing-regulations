# T1172 — A fresh machine: the setup finds, and a release made from nothing

Status: OPEN — no longer blocks the 0.10.0 re-cut: the gnis:2123 fix merged in #312 (2026-11-27) and all six step-back picks are answered (## Owner); left: Work items 2-4

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1172 | A fresh machine: the setup finds, and a release made from nothing | OPEN — does not block the re-cut (#312 merged; six picks answered) | — | a release from nothing on current main in the clean clone, the bc.pmtiles pin (find 1) and the setup-page leftovers |

Source: the fresh-clone proof of T1120 (repo-move chat, night of 2026-11-25, clean clone
`~/dev/personal/canifishthis-fresh`) and its rerun on current main by the planning chat, 2026-11-26. The planning
chat's notes: `review-of-the-nights-merges` (MED-1), `T1176-braids-scoping`, `side-channels-step-back`. Related:
T1120, T1140, T1146, T1149, T1176.

## The three finds of the proof (night of 2026-11-25)
1. **The raw basemap pin is a 404.** `bc.pmtiles` (4.4 GB) at `data.canifishthis.ca/bc.pmtiles` is gone, so the
   bootstrap fetch exits 1; curl's "exit 56 (rerun to resume)" hid the 404. Likely deleted by the old-site drop
   (`prod_prune.py` `V1_FILES`, lines 71-72; not proven). Only a basemap rebuild, the tidal audit tools and
   `release/manifest.py:199` read it. Owner, 2026-11-26 00:05: "fetch from pmtile directly? ... with same bbox as
   curreent then it updates too". To build, in ONE change: a fresh machine does not fetch it; the manifest reads
   the OSM day from `data/source/bc.pmtiles.source.json` (today `_pmtiles_osm_day`, `manifest.py:146-154`, returns
   None with no error, so a fresh release would say `basemap.date: null`); a basemap rebuild fetches a current
   Protomaps build with the recorded bbox and records its day and sha; the fetch names the HTTP status; a guard
   that no `pins.json` url is a key `V1_FILES` / `V1_DIRS` deletes; the dead `basemap` entry of
   `data/fetch_data.py:1861-1866` goes.
2. **`fwa_coast.gpkg` was not pinned** (the atlas fails in 1 s without it). Uploaded on the owner's "yes you can"
   and pinned by #297. Done.
3. **A fresh machine could not reach deliver** (the in-season resolve needed a release, which deliver makes).
   Fixed by #293 (`resolve --first-build`); proved on the clean clone 2026-11-26 03:10.

## Setup-page leftovers (`docs/runbooks/fresh-machine.md` is at 150 lines, its cap)
- Line 73 says to copy `bc.pmtiles` from another machine: rewrite when find 1 lands.
- Line 119 and `docs/repo-move/OUTSIDE-GIT.md:71`: `build.sh <work dir>` stops with "not published" until T1170
  publishes the MapLibre library.
- The setup fetch needed four runs on 2026-11-25 (curl 56 twice, the 404, 92): the fetch should resume and retry
  by itself.
- `python -m pipeline build --status` says "running" after a crash; a stopped deliver leaves
  `data/generated/.deliver-staging-*` behind.
- Skipped by the proof (#304): the iPhone simulator build and the `open_pr` test.

## The open blocker: main cannot build a release on real data (2026-11-26)
Rerun on current main (0745ce824) in the clean clone, 26 min 56 s: no release. The night's gnis:10607 stop is gone
(#265 fixed it). The new stop is in deliver's answers step: "gnis:2123 part 1: a side channel with no named end and
no join to say (T1140)" (Old Kootenay River Channel; `part.py`, the side words, about line 624).
- **Cause:** #264 (reach policy 51) merges a water's side channels into one part; #265 needs each side-channel part
  to have a named end or a join distance. The 170 m channel (section 317225) has neither, because
  `side_channels.py` refuses joins when the main line is one section. MED-1 of the review of the night's merges
  ("#264 and #265 have never run together on real data") came true.
- **The first fix does not work.** Branch `fix/T1140-single-section-mouth` (53a96c0e4, pushed, no PR): a
  one-section main line's mouth from its flow pointer. On the real geometry the pointer of 317223 meets main line
  317222 mid-line, 588 m from its first vertex and 10.8 km from its last, so it matches neither end; deliver still
  exits 1. Only gnis:2123 and the Okanagan River Oxbows reach that join rule.
- **Measured (read-only, 2026-11-26):**
  - The split is real: the side part = the main line's rules + two Creston Valley WMA rules (bass, yellow perch:
    no daily limit) + the CVWMA permit.
  - The side part is 99.40% inside the WMA (the 170 m channel alone 93.93%); the main line 0%.
  - Naming it by the area is not offered: all 35 WMAs are `cut = 0` in the area catalog (`area_closures.json`
    `"cut": false`), so `inside_rows` never offers them. T1149's Creston cut (`cut: "clean"`) would make it a
    candidate, with a REVIEWED_INSIDE entry.
  - The mouth is the first vertex (FWA measure 0; 532.5 m against 535.7 m; both ends touch the Kootenay). The atlas
    keeps FWA's first-vertex-downstream order (`blk_chains` sorts by `down_m`, `merge_ordered` never reverses), so
    "mouth = first vertex" needs no flow pointer. The pointer's 588 m target is a tie-break in `shed._down_one`.
- **The owner, 2026-11-26, verbatim:** "this depends on a lot first why is it split and a different section? if it
  shouldnt be a different section then it should get merged by the same rule ideas. then if there are cuts and it
  has different rules there should always be enough info. basicallyu we dont want to creat more sections by having
  a different name. the sidechannels (unless haviung their own gnis name or variant) should have same name as
  mainstem just with side channel src. i think. we are confusing the issue here a lot and might need a step back
  to figure out what we are actually solving here. lkooking upstream with how it is consummed etc etc"

## The step-back design (planning chat notes `side-channels-step-back`, read-only, 2026-11-26)
- A name never makes a part: a part splits only on rules (`bundle/derived.py` `part_rows`). An unnamed channel
  already takes its river's name with the source "side-channel": the owner's rule is how the data works.
- The stop is a measuring problem, not a naming one: the namer asked a gauge-route pointer (`section_down`) which
  end is the mouth; FWA already says it: the first vertex.
- Of the 26 side-channel-only parts, 14 are binding artefacts, 3 are the owner's call, 9 are real parts that need a
  name. Only those 9 stop builds; geometry simplification (T1176) clears the map, not them.
- Smallest unblock: "a one-section main line's mouth is its first vertex" in place of 53a96c0e4's rule
  (`side_channels.py` `join_measure`), plus a build check of that order on every stem (3,916 of 3,916 hold today),
  with tests. Predicted, not run: Old Kootenay p1 reads "side channels: one into Leach Lake; one joining 0.4 km from
  the mouth". Then, in order: the WMA name in 0.10.1, exact leave points in `spans.compute`, the binding fixes,
  geometry, the map look. Six picks wait (below).

## 2026-11-26 evening: the label rulings on #312 (planning chat)
The owner's words (verbatim): Snehumption: "Plain \"2 side channels\" (Recommended)" (the rule: if not every channel in the
part has a named end, the plain count). Klinaklini and MacMillan "Mouth → Source": "Plain \"Side channel\" / \"3 side
channels\" (Recommended)" (built after the app change in T1196). Short form: "Shorten Ecological Reserve to \"Eco Reserve\"".
Then, on the Skeena: "it should be \"[name] eco reserve\" everywhere" (names, ends and subtitles alike, e.g. "Skeena River
inside Skeena River Eco Reserve"). #312 round 4 built Snehumption "2 side channels" and "Eco Reserve"; round 5 carries "Eco
Reserve everywhere" (the Skeena p4 "Ecological Reserve boundary" end was the one left unshortened). Rulings page:
`docs/OWNER-RULINGS.md` batch 3.

## Work
1. The owner's picks on the step-back design; then the gnis:2123 fix it names (default: the first-vertex mouth and
   the stem-order check), a clean side deliver and the part diff (only Old Kootenay p1 should change).
2. A release from nothing: a full run in the clean clone, bootstrap to a release directory, on current main. The
   clean clone stays until a release comes out of it.
3. Find 1 (bc.pmtiles), as above.
4. The setup-page leftovers.

2026-11-26 the owner's pick ("Yes, but name it by the area") built: fix in PR #312 (no auto-merge), awaiting the
independent review and the owner's look at the words. A one-section main line's mouth is its first vertex, with a
stem-order guard on every build (3,916 of 3,916 start at the section below; 29,564 lowest pieces agree with FWA
junction measures, 0 backwards, 2,509 uncheckable). Membership-only parks/WMAs/ERs are offered for a reviewed pair only.
Old Kootenay p1 reads "Old Kootenay River Channel inside Creston Valley Wildlife Management Area". The clean clone made
release r-c8dc7712a9c0, rc=0 (reach 11m22s, tiles 16m13s, deliver 14m52s; reach and tiles reran on the rename's
comment edits). Only gnis:2123 and the Okanagan River Oxbows changed (Oxbows' name kept; its subtitle label gained
"joining 0.2–0.3 km"). Waiting for the owner's review: Thelwood Creek p0 is 100 % inside STRATHCONA - WESTMIN PARK,
logged as an INSIDE candidate and not offered.
2026-11-26 the review's MERGE (after the owner's look) and the owner's picks ("Fix the measure in this PR"; Thelwood:
"yes name it but this is something we need to fix generally") are built on PR #312. A side channel whose downstream
end lies away from its main line is measured where it leaves it: Old Kootenay 0.4–0.6 km; Stirling 20.4 → 20.9 km and
Theodosia 13.1 km are now "joining" rather than nearest-point. Thelwood p0 now reads "Thelwood Creek inside
Strathcona - Westmin Park". Also: L1 docstring, L2 any-backwards rule, L3 one area per name. Clean clone release
r-216c6f000d0b, rc=0 (deliver only, 13m34s). Still awaiting the owner's look at the words table in the PR before merge.
Left (owner, "fix generally"): parks that bind channels but not their river (T1149).
2026-11-26 round 3, the owner's rulings on the words, built on PR #312: a side-channel-only part shows a distance only
when its water has more than one such part. No water has two today, so none shows a distance. There is no verb ("N km
from the mouth"). The name uses the short area form ("inside Creston Valley WMA"). The subtitle names the area for every
part the bundle measures inside or at one, reviewed or not: the bundle measures every cut and membership-only area
(`part_inside.cut`), and REVIEWED_INSIDE is now only the name's gate. Clean clone release r-6f59a36e1f1e, rc=0
(deliver only, 14m29s). The words table and the owner's questions are in the PR; it waits for his look before merge.
2026-11-26 round 4 (the owner's three rulings on the table): a part takes the plain count unless every channel has a
named end (Snehumption p1 "2 side channels"); "Eco Reserve" short form; an unmeasured unique part is logged by name.
NOT built: Klinaklini p1 / MacMillan p2 "Mouth → Source" (the channels' own ends, `branch_end_tokens` for a salmon-cut
piece). Dropping them makes the app's `mergeNav` join throw, so the app must first take a joined span's ends from the
pieces that have them. Release r-ab4722f01fc9, rc=0. The 474-floor test is left as is (T1146 moved it; not re-pinned).
2026-11-26 round 5: "Eco Reserve" everywhere (owner: "it should be \"[name] eco reserve\" everywhere"). One function,
applied at `end_name` and `part_label`, changes 206 parts on 74 waters and 198 end tokens. The review's fixes: a part
that needs its distance but has none now stops the build; fast wiring tests; the stale comments. Release
r-83588a004b03, rc=0.

## Acceptance
The clean clone makes a release directory from nothing, with no copied file; deliver passes on gnis:2123, with a
test of a one-section main line and its side channel that fails without the fix; a fresh release names its basemap
date. Severity: HIGH (blocks the re-cut). Model: Opus. Size: M.

## Owner

- [x] 2026-11-26 — The step-back design's six picks (recommended first): unblock the re-cut by the first-vertex mouth only / plus area names now / hold for the Creston cut; Old Kootenay's main line 3.7 m outside the WMA: outside (two parts) / in it; the naming ladder: yes / keep today's; membership-only places in names: parks, WMAs, ERs, national parks / cut areas only; channels of a unit-line river follow it / keep their side; geometry: a side-atlas trial after the binding fixes / the look only / none. Default: nothing is built before you pick.
  → (1) "Yes, but name it by the area" (2026-11-26); #312: "Merge; names only (Recommended)" (2026-11-26; merged 2026-11-27). (2) two parts, by "Shorter: '... inside Creston Valley WMA'" and "Never inside (Recommended)" (2026-11-26). (3) superseded by T1191: "option 3 shoulds good ..." and "options one except if it is the only section it doesnt need a locator at all" (2026-11-26/27). (4) "Wholly inside any area, plus 'partly in' when the area's rule binds (Recommended)"; Thelwood: "yes name it but this is something we need to fix generally." (2026-11-26). (5) "campbell river is correct it straddles so is included in mu 1-10 and 1-6 so is in closure" (2026-11-25); the 14 side parts: "Fold by the rule, in the re-cut (Recommended)" (2026-11-27, T1240). (6) superseded by T1242: "D: simplify the water itself", then "Run the loop-removal trial first, then I decide" (2026-11-26); loops build 1 merged in #383.
- [x] 2026-11-26 — The stale candidate r-71d09ad82e81 → "can go later" (2026-11-26; delete it at the re-cut)
