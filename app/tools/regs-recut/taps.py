"""The phase-0 parity oracle: for every golden state, what the Python reference decoder says the app's tap must resolve.

    .venv/bin/python app/tools/regs-recut/taps.py [--release data/release/r-…] [--states app/fixtures/golden/states.json]
    -> app/fixtures/golden/taps.json

Per state: the water's item id (page v36's export ids, by the snapshot's water name), the part index, the date (2026, the
prototype's year), the weekday, then the store's answer: item_ord, akey, the segment starts, the moments, the segment index
(pipeline.deliver.answers.encode.segment_index, inside=False), the cell's five frame ids, and the per-day display frame id
over the leap calendar (1..366) so the TS year strip can be checked frame by frame. Plus a sample of unnamed section handles
(handle -> akey, place, the unnamed name). Read-only; no LLM, no network. The store is the RELEASE's (P20: no regs.sqlite),
put back together by `pipeline.deliver.store.release.province_store` and held to its store_digest when the release
carries one (`store_meta`; older releases: `meta.store.store_digest` is null and `answers_sha256` pairs the oracles).

And `leap`: the Feb 29 cases (owner, P06/D31: Feb 29 READS Feb 28's day, 59, on its OWN real weekday), each tapped as
`encode.tap_key` does (`calendar.reading_day_of` + `calendar_spec.weekday_of`): the Zymoetz below Limonite Creek
(`zymoetz_river.r10`, "Fridays, Saturdays, and Sundays") on Fri 2036-02-29 beside its Thu 2036-02-28, and Tue
2028-02-29 beside Mon 2028-02-28.

PART IDENTITY. A golden state names a part by its index in the PROTOTYPE's water; a release that re-cuts a water's parts
(two parts merge when their rows become the same) moves or removes that index. Every state whose golden part label is
readable (its `sub` minus the kind and section count; not "closed all year", which the prototype merges) is held to the
store's part at that index: same label (a "…"-truncated golden label as a prefix) and same section count. A state that
fails is an ERROR (exit 1) unless it is declared below — never a silent `akey: null` nor a tap on whatever water now has
that index. Declared states are themselves checked, so an entry that no longer describes the store fails too:
- PART_MOVED: the golden part is the store's part `ix` now, unchanged (label and section count equal there).
- PART_GROWN: the golden part is at the same index with the same rows, but its extent took in merged sections (label
  and/or count differ). Tapped and compared (`grown` on the state); the parity excludes only the words of the extent
  (the header's part label and the subtitle). `(ix, why)` (T517, r-d14bdb180f55): the same, at the store's part `ix`
  — river order renumbered the water, and the grown label names no rule-set group `t517_group` could find.
- every water with a PART_MOVED / PART_GROWN / PART_GONE state is listed in taps.json `recut` (item -> reason): its
  list of parts is not the golden's, so the parity excludes the Parts sheet on its states.
- PART_RECOUNTED: the golden part is the store's part `ix` (the same index, or a renumbered one) with the same label
  and rows, but the atlas added or removed sections under it (T67: braid cuts, a ditch leaving its river, moved
  length cuts), so only its section COUNT differs. Tapped and compared whole (`recounted` on the state: the reason and
  the store's count); the parity holds the subtitle to the golden's with the count replaced.
- A "closed all year" golden part (no label to check) is held to the store's part at its index being closed all year
  (T67: a renumbering put an open part at a closed state's index); one that is not must be declared.
- PART_TIDAL (T278): a tidal cut (T166/T185/T212: the piece below a stream's tidal boundary leaves every binding, no
  freshwater rules, off the map) took the golden part. `(None, tidal_ix, why)`: the WHOLE golden part is tidal now — the
  store's part `tidal_ix` is unkeyed and unlabelled (the tidal remainder) and no store part has the golden label; written
  to taps.json `tidal` (not `states`), and the parity excludes the state in every block (exclusions.ts PART_TIDAL_IDS).
  `(ix, tidal_ix, why)`: the freshwater remainder of the golden part is the store's part `ix` (its extent words changed,
  its rows did not) — tapped and compared like PART_GROWN (`tidal` on the state). A part only RENUMBERED by a tidal cut
  is PART_MOVED, citing the cut. Each entry cites its cut (the bundle's `tidal` table: section -> split / entry).
- PART_GONE: the golden part's water merged into the store's part `now_in` (or, T67, was split or recounted so
  the page's merged closed list is not the golden's); no store part has the golden label and count. Written to taps.json `gone` (not `states`), and the parity excludes the state with this reason
  (conformance/src/regs/exclusions.ts PART_GONE_IDS).
"""
import argparse, datetime, json, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.release import province_store  # noqa: E402
from pipeline.deliver.answers import encode as E           # noqa: E402
from pipeline.deliver.calendar import day_of, reading_day_of  # noqa: E402
from pipeline.common.calendar_spec import weekday_of        # noqa: E402

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_D0 = datetime.date(2026, 1, 1)
WD_OF_DAY = {day_of(((_D0 + datetime.timedelta(i)).month, (_D0 + datetime.timedelta(i)).day)): WEEKDAYS[(_D0 + datetime.timedelta(i)).weekday()] for i in range(365)}


# (T115, r-941dcc149b17) owner decision AGENTS 54 / tickets/T115-one-region-membership.md: a stream piece straddling a
# region line is its HOME region's only. The four Fraser pieces that carried two regions' rows (and so were parts of their
# own: golden x15p14/15/17/18) now carry their home region's rows and merge with the neighbouring part; the Fraser has 15
# parts (0..14), not 19. Measured old r-b518a2b507fa -> new by part_section: old 0..13 -> same index, 14 -> 4,
# 15 -> 6, 16 -> 14, 17 -> 1, 18 -> 0.
T183 = "T183 (owner 2026-10-24, tickets/T183): a straddling stream piece carries both regions' rules again (reverses T115)"
T115 = "T115 (owner decision AGENTS 54, tickets/T115-one-region-membership.md): a straddling stream piece is its home region's only"
T67 = "T67 (owner 2026-10-24, decisions.md \"braids accepted\"; RULINGS E17, AGENTS 12)"
T220 = "T220 (owner 2026-10-28, FIX \"complete\"): no channel line ends where the water goes on"
# (T278, r-62e2f3a8c81e) the tidal cuts: each takes the piece below its point out of every binding (bundle `tidal`)
FRASER_TIDAL = ("T278 (r-62e2f3a8c81e, tidal cut split:fraser_river__the_cpr_bridge_at_mission, gnis:39325 at 76.51 km): "
                "the Fraser below the CPR Bridge at Mission is tidal (29 sections, store part 16, unkeyed), so the golden "
                "part 'From the mouth up to the CPR Bridge at Mission' (old store part 3) has no freshwater part and the "
                "parts after it are renumbered down by one (4..16 -> 3..15)")
CAMPBELL_TIDAL = ("T278 (r-62e2f3a8c81e, tidal cut split:campbell_river__cement_block_boundary_sign, gnis:39532 at 1.63 km): "
                  "the Campbell from the mouth up to the cement block boundary sign is tidal (5 sections, store part 10, "
                  "unkeyed); the golden two-stretch part (old store part 3, 8 sections) keeps only its Maple Street -> "
                  "power line stretch (store part 5, 3 sections), and John Hart Lake -> Ladore Dam and cement block -> "
                  "Maple Street are renumbered 4 -> 3 and 5 -> 4 (part order follows rule-set ids)")
BRUNETTE_TIDAL = ("T278 (r-62e2f3a8c81e, tidal cut split:brunette_river__burlington_northern_railway_brid, gnis:10070 at "
                  "2.86 km): the Brunette below the Burlington Northern Railway Bridge is tidal (1 section, store part 2, "
                  "unkeyed), so part 0 starts at the bridge, not the mouth (2 sections, same rows)")
SKEENA_TIDAL = ("T278 (r-62e2f3a8c81e, tidal cut split:skeena_river__tidal_boundary, gnis:2936 at 6.4 km): the Skeena below "
                "its tidal boundary is tidal (1 section, store part 9, unkeyed), so part 0 starts at the tidal boundary")
COQUITLAM_TIDAL = ("T278 (r-62e2f3a8c81e, tidal cut split:coquitlam_river__mary_hill_bypass_bridge, gnis:19986 at 0.31 km): "
                   "the Coquitlam from the mouth up to the Mary Hill Bypass Bridge is tidal (1 section, store part 3, "
                   "unkeyed), so the golden mouth part (old store part 2) has no freshwater part and the Mary Hill -> "
                   "Lougheed part is renumbered 3 -> 2")
#: T517 (owner 2026-11-08, "one part per connected adjacent stretch"): a part is a connected run, numbered mouth up; the
#: golden part is its RULE-SET GROUP — the store's parts carrying the golden label, whose sections sum to the golden count.
T517 = ("T517 (owner 2026-11-08): one part per connected run, numbered in river order — the golden part is the rule-set "
        "group of the store's parts with its label (their sections summed), tapped at the group's lowest part")
# (r-8e04e839ea48, d2858cc80) Nitinat Lake (x31p) is a tidal-keyed water again: tapped, its tidal banner as before
T228 = ("T228 (a757756b, owner 2026-10-28): no-access land breaks a water's pieces — the Coquitlam's Lougheed -> lake "
        "outlet stretch (golden part 0, 10 sections) is cut at the Coquitlam Watershed boundary into store parts 0 (1 "
        "section) and 1 (9), so the mouth and Mary Hill parts are renumbered 1 -> 2 and 2 -> 3 (4 parts, not 3)")
_LEN = "the automatic length cuts moved off braid-spanned junctions (owner Q2)"
_GAUGE = "the old perpendicular sweep's gauge cuts are gone"
COLUMBIA_R11 = ('2026-10-27 (owner, decisions.md "needs a split"): columbia_river.r11 \'Speed restriction (10 km/h) from Mud Lake to Columbia Lake\' now binds the Fairmont -> Columbia Lake stretch, so the golden\'s two-stretch part (Kinbasket Lake -> Donald + Fairmont -> Columbia Lake outlet, 3 sections) is two parts: 3 (Fairmont, 2 sections, with the speed limit) and 7 (Kinbasket -> Donald, 1 section)')
PART_MOVED = {"x15p16_1010": 14,   # (T183) 15; then T278 FRASER_TIDAL: 14
              # (T278) FRASER_TIDAL: the golden part at old store part k (k > 3) is store part k - 1 (x15p9: closed all
              # year, held to the store's part being closed)
              "x15p4_1010": 3, "x15p5_1010": 4, "x15p6_1010": 5, "x15p7_1010": 6, "x15p8_1010": 7, "x15p9_1010": 8,
              "x15p11_1010": 10, "x15p12_1010": 11, "x15p13_1010": 12,
              # (T278) CAMPBELL_TIDAL renumbers the two parts after the golden part 3
              "x16p4_1010": 3, "x16p5_1010": 4,
              # (T67, then T183) the Skeena's Ecological Reserve is two parts (PART_GONE x19p5: 5 below the Exchamsiks cut,
              # 6 the host channel above it), which renumbers the parts after them
              "x19p6_1010": 7, "x19p7_1010": 8,
              # (T497) three pieces left the Kootenay for the Old Kootenay River Channel, which renumbers its parts
              "x9p6_1010": 5, "x9p4_1010": 3,
              # (T517, r-d14bdb180f55) river order renumbered the Campbell: the golden merged closed part (2 sections:
              # Elk Falls -> John Hart Dam, part 5, and the Strathcona Dam 100 m piece, part 9) is held at part 9, the
              # part the previous re-cut carried it to by label
              "x16p7_1010": 9,
              # (T228) the Coquitlam's no-access split renumbers the parts after it
              # (T228) x37p1/x37p2 were 1 -> 2 and 2 -> 3; then T278 COQUITLAM_TIDAL: x37p1's part is tidal
              # (PART_TIDAL) and x37p2's is back at its golden index 2
              }
#: why each PART_MOVED state moved, when it is not the water's RECUT_WHY
MOVED_WHY = {**{k: FRASER_TIDAL for k in ("x15p4_1010", "x15p5_1010", "x15p6_1010", "x15p7_1010", "x15p8_1010", "x15p9_1010",
                                          "x15p11_1010", "x15p12_1010", "x15p13_1010")},
             "x16p4_1010": CAMPBELL_TIDAL, "x16p5_1010": CAMPBELL_TIDAL,
             "x16p7_1010": f"{T517}: the Campbell's closed parts are 5 and 9 in river order"}
              # (T187) x22p6 (the Peace's Dinosaur Lake part) is back to its golden place and size: part 6, 1 section
#: gone states whose water's list of parts is still the golden's (labels and order the Parts sheet draws): not `recut`
GONE_NOT_RECUT: set[str] = set()
#: the reason a water is `recut` (its list of parts is not the golden's), by the state that shows it
RECUT_WHY = {"x22p2_1010": "T67 (owner 2026-10-24): the Peace's braids are cut at Site C, the 1,200 m signs and the "
                           "Halfway, so its parts are renumbered (part order follows rule-set ids)",
             "x19p6_1010": "T67 (owner 2026-10-24) and T183 (owner 2026-10-24: Skeena section 2 starts at the Exchamsiks "
                           "cut): the Ecological Reserve's Exchamsiks host channel above the cut, 360216952:1017, keeps the "
                           "section-2 designation the rest of the Reserve lost, and is a part of its own (8 -> 9 parts)",
             # (T183) the straddling pieces are parts of their own again (T115's merge undone, two regions' rules)
             "x15p16_1010": f"{T183}: the Fraser's straddling pieces are parts of their own again (15 -> 17 parts; the "
                            f"golden has 19); then {FRASER_TIDAL}",
             "x27p0_1010": f"{T183}: the West Road's straddling pieces are parts of their own (3 -> 5 parts)",
             "x18p3_1010": COLUMBIA_R11,
             "x37p1_1010": f"{T228}; then {COQUITLAM_TIDAL}",
             "x16p3_1010": CAMPBELL_TIDAL, "x23p0_1010": BRUNETTE_TIDAL}
#: (T517, r-d14bdb180f55) a grown state whose part river order renumbered: the store index of its rule-set group's lowest
#: part, declared (no --previous-answers in sync.mjs to carry the old index by label)
def _t517_at(ix, group):
    return f"; then {T517} — store part {ix} ({group})"


PART_GROWN = {
    "x15p0_1010": (19, f"{T183}: a Fraser piece at the Region 5 / 7A line carries both regions' rules and joined part 16 (109 -> 108 sections); then T678 D1 (owner 2026-11-11, r-0a9930fb8826): two ponds on the upper Fraser are the river now (3 -> 1 lakes, 108 -> 112 sections; same rows), and river order numbers it part 19"),
    "x14p0_1010": (3, f"{T220}: braid pieces that close a kept side channel were restored (26 -> 27 sections); then T678 D1 (owner 2026-11-11, r-0a9930fb8826): the Kitimat's pond lake:329233923 is the river now (27 -> 28 sections, no lake; same rows), and river order numbers it part 3"),
    "x13p4_1010": (7, "T322 (owner 2026-10-29 slough calls, folded 2026-11-03): a lake on this water is a slough of its stream now, so the part runs through one lake fewer (35 -> 42 sections; same rows); then T678 D1 (owner 2026-11-11, r-0a9930fb8826): 21 ponds on the Dean are the river now (24 -> 3 lakes, 17 -> 27 sections; same rows)"
                   + _t517_at(7, "parts 7, 8 and 9")),
    "x5p0_1010": (1, "T322 (owner 2026-10-29 slough calls, folded 2026-11-03): a lake on this water is a slough of its stream now, so the part runs through one lake fewer (9 -> 10 sections; same rows); then T678 D1 (owner 2026-11-11, r-0a9930fb8826): its last pond lake:329123781 is the river now (10 -> 11 sections, no lake; same rows)"
                   + _t517_at(1, "part 1")),
    "x27p0_1010": (2, f"{T183}: the West Road's pieces on the Region 5 / 6 / 7A lines are parts of their own; part 0 is 79 sections"
                   + _t517_at(2, "parts 2 and 5")),
    "x27p2_1010": (0, f"{T183}: the West Road's side channel above Charleson Creek (Region 5 and 7A) is part 3 now; part 2 is the mouth stretch"
                   + _t517_at(0, "part 0: the mouth stretch")),
    "x9p0_1010": (6, "T179 (owner 2026-10-25, decisions.md Koocanusa): the Koocanusa cut moved from the reservoir's outlet "
                 "(356570348:431810, where the hand point stood) to its INLET (490242), so this 1-section piece below "
                 "the lake now ends at the lake's outlet, not the curated 'u/s end' name (label only; same piece, same rows)"
                   + _t517_at(6, "part 6, after the unkeyed piece 5")),
    "x9p1_1010": (7, f"{T67}: the Kootenay's ditch-only line 356365735:0 (1,982.6 m) left the river item (owner Q1, "
                 f"DITCHES_LEAVE_RIVER_ITEMS); 3 length cuts on 356570348 moved: 52 -> 51 sections; then T179 (owner "
                 f"2026-10-25, decisions.md Koocanusa): the authored cut now stands on the reservoir's inlet (490242) and "
                 f"names it, so the part starts at 'the Koocanusa Reservoir upstream end', not 'Lake Koocanusa' (same rows)"
                   + _t517_at(7, "part 7")),
    "x19p1_1010": (3, "T183 (owner 2026-10-24): Skeena section 2 starts at the Exchamsiks confluence, not the Ecological Reserve boundary (61 -> 59 sections; the extent changed, the rows did not)"
                   + _t517_at(3, "parts 3 and 4")),
}
#: (2026-11-09) the store part each PART_GROWN state must land on: (label, section count) as `parts` reads it. Without
#: it, any part whose label differed from the golden's passed.
PART_GROWN_IS: dict = {
    'x15p0_1010': ('In Region 7 · From Region 5 boundary up to the top of the river (through 1 lake)', 112),
    'x14p0_1010': ('From the Hwy 37 bridge up to the top of the river', 28),
    'x27p0_1010': ('Two stretches: from Region 7A boundary up to Region 6 boundary (through 4 lakes), and from Region 6 boundary up to the top of the river (through 1 lake)', 72),
    'x27p2_1010': ('From the mouth up to Region 5 boundary', 3),
    'x19p1_1010': ('From the Exchamsiks River confluence up to 1.5 km upstream of the Kitsumkalum River confluence', 59),
    'x13p4_1010': ('From Anahim Lake up to the top of the river (through 3 lakes)', 27),
    'x9p1_1010': ('Upstream of Koocanusa Reservoir · From the Koocanusa Reservoir upstream end up to the White River confluence', 56),
    'x9p0_1010': ('From the B.C. border up to the Lake Koocanusa outlet', 1),
    'x5p0_1010': ('Upstream of Josephine Falls / See exceptions · From the Josephine Falls up to the top of the river', 11),
}
PART_GONE = {
    "x37p0_1010": (0, T228),
    "x9p2_1010": (2, f"{T220}: restored braid pieces joined the Kootenay's closed parts: the page's merged 'closed all "
                     f"year' list is 19 sections (part 2: 18, part 7: 1), not the golden's 16"),
    "x13p2_1010": (3, f"{T220}: restored braid pieces renumbered the Dean; the golden closed part (the page's merged "
                      f"'closed all year', 4 sections) is store parts 3 and 6 now, part 2 is the open 0.5-3.5 km stretch"),
    "x18p3_1010": (3, COLUMBIA_R11),
    "x27p1_1010": (0, f"{T183}: the West Road's Region 6 stretch below Carnlick Creek shares its rules with part 0 now"),
    "x15p17_1010": (15, f"{T183}: the golden part (356364114:674465, Region 5 and Region 7A) shares its rules with 699253 and is in part 16 (T278: 15) now"),
    "x15p18_1010": (15, f"{T183}: the golden part (356364114:699253, Region 5 and Region 7A) shares its rules with 674465 and is in part 16 (T278: 15) now"),
    "x15p15_1010": (13, f"{T183}: the golden part (356364114:434930, Region 3 and Region 5) has the same rules as 418405 and is in part 14 (T278: 13) now"),
    # not PART_GROWN: the rows differ too. The prototype's two-region page on 418405 drew Region 3's zone and r5's Fraser
    # rows r2-r4; T183 binds Region 5's zone rules (p.42) and r5 r1 / r6 there as well (tickets/T183 "so 14, not 12")
    "x15p14_1010": (13, f"{T183}: the golden part (356364114:418405, Region 3 and Region 5) is in part 14 (T278: 13) with :434930, "
                        f"which also carries Region 5's zone rules and the Region 5 Fraser rows r1 and r6 the prototype's "
                        f"page did not draw, so its rows are not the golden's"),
    # (T67, r-0ed3c2b2a0a7) two closed states whose golden page counts sections the release re-cut: the app's merged
    # "Closed all year on N stretches (M sections)" list is the store's, the golden's is the prototype's
    "x19p5_1010": (5, "T67 (owner 2026-10-24): the Exchamsiks cut split the Skeena's Ecological Reserve part, and T183 "
                      "(owner 2026-10-24) started Skeena section 2 at the cut — below it, 6 sections (the side channel "
                      "piece 360216962:96 among them) in part 5; above it, the Exchamsiks host channel 360216952:1017, "
                      "still section 2, in part 6; both closed all year, so the page merges two closed parts where the "
                      "golden had one"),
    "x22p2_1010": (3, "T67 (owner 2026-10-24): the Peace's closed parts hold 7 sections, not 5 — the Site C closure took "
                      "the braid pieces 359013560:165 and (T187) 359005110:3454 — and the first is renumbered 2 -> 3 "
                      "(part order follows rule-set ids)"),
}
# (T67, r-0ed3c2b2a0a7: braids cut at build, ditch-only lines leave their river, length cuts skip braid-spanned
# junctions, the old perpendicular gauge sweep removed). Measured against r-bb4331929428 by section id (registry of
# atlas full.prev vs the bundle of atlas full): every part below keeps its label and rows; only its section count moved.
# {state: (store part_ix, why)} — the golden part is the store's part `ix`, same label, another section count
PART_RECOUNTED = {
    "x9p3_1010": (7, "T497 (owner 2026-11-08, follow connections): three pieces of this stretch are the Old Kootenay "
                     "River Channel's now, which also renumbers the Kootenay's parts: 4 -> 1 sections, same label and rows"),
    "x9p5_1010": (4, "T497 renumbered (index 5 -> 4); before that T322 (owner 2026-10-29 slough calls, folded 2026-11-03): sloughs on the upper Kootenay are part of "
                     "its stream now, so the part has more sections under the same label and rows: 16 -> 18 sections"),
    "cow0_1010": (0, "T185 (owner marks 2026-10-27): the new Sandy Pool white-triangle sign cut (DFO "
                     "'from 66 Mile Trestle downstream to the sign') lies inside this stretch: 10 -> 11 sections"),
    "x21p2_1010": (2, "T179 (owner 2026-10-24, one cut-placement pass): gauge 08NK030 stood 232 m from the curated Elko "
                      "Dam cut and now joins it (gauge an alias), so the piece between is gone: 3 -> 2 sections"),
    "x17p1_1010": (1, "T179 (owner 2026-10-24, one cut-placement pass): gauge 08EC001 stood 88 m from the curated Fort "
                      "Babine Bridge cut and now joins it (gauge an alias), so the sliver between is gone: 2 -> 1 sections"),
    "x13p3_1010": (2, f"{T67}: the Dean's side channel 360227736 is cut at the 0.5 km sign (421 m): 2 -> 3 sections; "
                      f"then {T220}: restored braid pieces, 4 sections, renumbered 3 -> 2 (part order follows rule-set ids)"),
    "bc1_501": (1, f"{T220}: braid pieces that close a kept side channel were restored: 6 -> 7 sections"),
    "bc0_501": (0, f"{T220}: braid pieces that close a kept side channel were restored: 44 -> 47 sections"),
    "bc1_1010": (1, f"{T220}: braid pieces that close a kept side channel were restored: 6 -> 7 sections"),
    "bc0_1010": (0, f"{T220}: braid pieces that close a kept side channel were restored: 44 -> 47 sections"),
    "x13p1_1010": (1, f"{T220}: braid pieces that close a kept side channel were restored: 14 -> 15 sections"),
    "x15p1_1010": (1, f"{T220}: braid pieces that close a kept side channel were restored: 35 -> 36 sections"),
    "x21p1_1010": (1, f"{T220}: braid pieces that close a kept side channel were restored: 36 -> 37 sections"),
    "chw1_1010": (1, f"{T220}: a braid piece that closes a kept side channel was restored to the Vedder: 9 -> 10 sections"),
    "x15p2_1010": (2, f"{T67}: on the Fraser mainstem {_GAUGE} (94,834 / 108,135 / 111,137 / 122,096 / 132,550 / "
                      f"148,097 m) and {_LEN} (93,822 / 116,370 / 135,999 m): 40 -> 36 sections"),
    "x18p0_1010": (0, f"{T67}: on the Columbia (356570372) {_LEN}: 4 cuts -> 5, 132 -> 133 sections"),
    "x19p3_1010": (3, f"{T67}: the Cedarvale cut takes the braid 360243437 (1,069 m): 51 -> 52 sections"),
    "x20p2_1010": (2, f"{T67}: the Thompson's side channel 355995300 is cut at the boundary signs (187 m): 7 -> 8 sections"),
    "x22p0_1010": (0, f"{T67}: the Peace's braids are cut at Site C, the 1,200 m signs and the Halfway (+5 km), "
                      f"07FD002's cut moved 959 -> 836 m, and {_LEN} on 359572348: 54 -> 56 sections"),
    "x22p1_1010": (1, f"{T67}: the Peace braids 359005110 / 359013560 cut at Site C dam, and {_LEN}: 29 -> 30 sections"),
    "x22p3_1010": (4, f"{T67}: the Halfway braid 359012853 is cut at 3,358 m: 6 -> 7 sections; renumbered 3 -> 4 "
                      f"(part order follows rule-set ids)"),
}
# (T278) {state: (store part_ix of the freshwater remainder, or None when the whole golden part is tidal; the store's
# tidal part; why — the cut)}
PART_TIDAL = {
    "x15p3_1010": (None, 16, f"{FRASER_TIDAL} (before: {T67}: {_GAUGE} on the Fraser's side channels 355992485 and "
                             f"355994872, and 08MH028's cut moved 578 -> 545 m: 30 -> 29 sections)"),
    "x37p1_1010": (None, 3, f"{COQUITLAM_TIDAL} (before: {T228})"),
    "x16p3_1010": (2, 0, f"{CAMPBELL_TIDAL} (before: T185, owner marks 2026-10-27: the Quinsam River confluence cut lies "
                          f"inside the Maple Street stretch, 7 -> 8 sections); then {T517} — the tidal piece is part 0 "
                          f"(mouth first), the Maple Street -> power line group is parts 2 and 3 (2 + 1 sections), tapped "
                          f"at part 2"),
    # (T517, r-d14bdb180f55) river order numbers the tidal piece first: the remainder's group is parts 1 and 3
    "x23p0_1010": (1, 0, f"{BRUNETTE_TIDAL}; then {T517} — the tidal piece is part 0 (mouth first), the remainder's "
                         f"group is parts 1 and 3 (1 + 1 sections), tapped at part 1"),
    # (T517, r-d14bdb180f55) was PART_GROWN at index 0: river order numbers the Skeena's tidal piece first (part 0), so
    # the golden part's freshwater remainder is its rule-set group's lowest part, 2 (parts 2, 5, 6: 46 + 7 + 3 sections)
    "x19p0_1010": (2, 0, f"{T67}: the Exchamsiks confluence cut runs straight across and {_LEN}; then T183 (owner "
                         f"2026-10-24): the mainstem from the Ecological Reserve up to the Exchamsiks left section 2 and "
                         f"joined this part (3 stretches, 44 -> 48 sections); then {SKEENA_TIDAL}; then {T517} — the "
                         f"tidal piece below the tidal boundary is part 0 (mouth first), the remainder's group is parts "
                         f"2, 5 and 6 (56 sections), tapped at part 2"),
}
_SUB = re.compile(r"^(.*?) · (\d+) sections?(?: · (.*))?$")


def grown_why(sid):
    """The reason of a PART_GROWN state (its value, or the reason of an `(ix, why)` one)."""
    v = PART_GROWN[sid]
    return v[1] if isinstance(v, tuple) else v


def golden_part(st):
    """(label or None, section count) of the golden state's part, from its `sub`."""
    m = _SUB.match(st["sub"])
    if not m:
        raise SystemExit(f"taps: golden sub {st['sub']!r} is not 'kind · N sections[ · label]'")
    return m.group(3), int(m.group(2))


def same_part(golden, store):
    """Is the store's part (label, sections) the golden part? None when the golden label cannot tell (no label, or the
    prototype's merged 'closed all year')."""
    lab, n = golden
    if lab is None or lab == "closed all year":
        return None
    slab, sn = store
    return sn == n and (slab is not None and (slab.startswith(lab[:-1]) if lab.endswith("…") else slab == lab))


def t517_group(golden, parts):
    """The lowest store part of the golden part's rule-set group (T517): the store parts carrying the golden label (its
    full words: what the group is) — or None when no store part has it. Their summed sections may differ from the
    golden's where an older declared recount (braid pieces, cut moves) already said so."""
    lab, n = golden
    if lab is None or lab == "closed all year":
        return None
    hit = sorted(ix for ix, (sl, _) in parts.items() if same_part((lab, parts[ix][1]), (sl, parts[ix][1])))
    return hit[0] if hit else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", default=None, help="the release folder whose regs store to read (default: data/release/LATEST)")
    ap.add_argument("--states", default=str(REPO / "app/fixtures/golden/states.json"))
    ap.add_argument("--page", default=str(REPO / "app/fixtures/reference/page_v36.html"))
    ap.add_argument("--out", default=str(REPO / "app/fixtures/golden/taps.json"))
    ap.add_argument("--unnamed", type=int, default=20)
    ap.add_argument("--previous-answers", default=None,
                    help="(T517) the answers file of the release the declarations were written against: a declared "
                         "part index is carried to the store part of the same rule-set group (its label)")
    a = ap.parse_args()

    html = Path(a.page).read_text()
    D = json.loads(re.search(r'<script[^>]*id="data"[^>]*>([\s\S]*?)</script>', html).group(1))
    by_name = {w["name"]: w["id"] for w in D["waters"]}
    G = json.loads(Path(a.states).read_text())
    s = province_store(Path(a.release) if a.release else None)   # the release IS the store (P20: no regs.sqlite)
    c = s.con
    meta = dict(c.execute("select k, v from meta").fetchall())
    M = s.static("answers.moments")
    ord_of = {}
    for o, sid in c.execute("select item_ord, item_id from item"):
        ord_of[s.s(sid)] = o

    prev = json.loads(Path(a.previous_answers).read_text())["sections"]["display"]["waters"] \
        if a.previous_answers else {}

    def remap(item, ix):
        """(T517) a declared part index of the previous release -> the store part of its rule-set group (same label)."""
        old = (prev.get(item) or {}).get("parts") or []
        if ix is None or ix >= len(old) or old[ix] is None:
            return ix
        hit = sorted(i for i, (lab, _) in store_parts(item).items() if lab == old[ix]["label"])
        return hit[0] if hit else ix

    def store_parts(item):
        return {ix: (s.s(l) if l is not None else None, n) for ix, l, n in
                c.execute("select part_ix, label_s, sections from part where item_ord=?", (ord_of[item],))}

    t517: dict = {}

    def resolve(sid, st, item, part_ix):
        """-> (part_ix to tap, None) or (None, gone record); raises SystemExit on an undeclared mismatch. (T517) A
        state the release's river-order runs moved is resolved by `t517_group` first, and listed `recut`."""
        if item not in ord_of:
            return part_ix, None
        parts, g = store_parts(item), golden_part(st)
        closed = dict(c.execute("select part_ix, closed_all_year from part where item_ord=?", (ord_of[item],)))
        # a state declared GONE stays gone: its label may still name another group (the Skeena Reserve's remainder)
        gix = None if sid in PART_GONE else t517_group(g, parts)
        if gix is None and sid not in PART_GONE and g[0] == "closed all year" and closed.get(part_ix) != 1:
            # (T517) a closed state has no label: the river-order renumbering moved its closed part — the previous
            # release's part at that index, carried by its label (`remap`); else the store's closed part of the golden
            # size, else its lowest closed part
            r = remap(item, PART_MOVED.get(sid, part_ix))         # a declared move names the previous release's index
            shut = sorted(ix for ix, v in closed.items() if v == 1)
            same = [ix for ix in shut if parts[ix][1] == g[1]]
            gix = r if closed.get(r) == 1 else (same or shut or [None])[0]
        if gix is not None:
            # (T517) the store's rule-set group holds the golden part: tapped there, whatever older declaration the
            # state carries (they described the release's parts before river order); listed `recut` where it moved
            if gix != part_ix or parts[gix][1] != g[1]:
                t517[sid] = gix
            return gix, None
        closed = dict(c.execute("select part_ix, closed_all_year from part where item_ord=?", (ord_of[item],)))
        if sid in PART_TIDAL:
            ix, tix, why = PART_TIDAL[sid]
            ix = remap(item, ix)
            akeys = dict(c.execute("select part_ix, akey from part where item_ord=?", (ord_of[item],)))
            # (T517) river order numbers the tidal piece (unkeyed, unlabelled) and its freshwater remainder anew
            bare = [i for i, p in parts.items() if akeys.get(i) is None and p[0] is None]
            if len(bare) == 1 and bare[0] != tix:
                tix, t517[sid] = bare[0], bare[0]
            hit = [i for i, p in parts.items() if same_part(g, p)]
            if tix not in parts or akeys.get(tix) is not None or parts[tix][0] is not None or hit:
                raise SystemExit(f"taps: PART_TIDAL {sid} does not describe the store: tidal part {tix} {parts.get(tix)} "
                                 f"akey {akeys.get(tix)}, golden {g} is store part {hit}")
            if ix is None:
                return None, {"item": item, "partIx": part_ix, "tidalPartIx": tix, "reason": why}
            if akeys.get(ix) is None:
                raise SystemExit(f"taps: PART_TIDAL {sid}: the freshwater remainder {ix} {parts.get(ix)} has no key")
            return ix, None
        if sid in PART_MOVED:
            ix = remap(item, PART_MOVED[sid])
            if g[0] in (None, "closed all year"):
                # (T278) a closed state has no label to check: held to the store's part `ix` being closed all year
                if closed.get(ix) != 1 or parts.get(ix, (None, -1))[1] < 1:
                    raise SystemExit(f"taps: PART_MOVED {sid} -> {ix}: golden {g} is closed, store part {ix} {parts.get(ix)} is not")
                return ix, None
            if same_part(g, parts.get(ix, (None, -1))) is not True or same_part(g, parts.get(part_ix, (None, -1))):
                raise SystemExit(f"taps: PART_MOVED {sid} -> {ix} does not describe the store: golden {g}, store {parts.get(part_ix)} at {part_ix}, {parts.get(ix)} at {ix}")
            return ix, None
        if sid in PART_GONE:
            now_in, why = PART_GONE[sid]
            hit = [ix for ix, p in parts.items() if same_part(g, p)]
            if hit or now_in not in parts:
                raise SystemExit(f"taps: PART_GONE {sid} does not describe the store: golden {g} is store part {hit}, now_in {now_in} in {sorted(parts)}")
            return None, {"item": item, "partIx": part_ix, "nowIn": now_in, "reason": why}
        if sid in PART_RECOUNTED:
            ix, _ = PART_RECOUNTED[sid]
            ix = remap(item, ix)
            lab, n = g
            got = parts.get(ix, (None, -1))
            if lab is None or got[1] == n or same_part((lab, got[1]), got) is not True \
                    or (ix != part_ix and same_part(g, parts.get(part_ix, (None, -1)))):
                raise SystemExit(f"taps: PART_RECOUNTED {sid} -> {ix} does not describe the store: golden {g}, store {got} at {ix}")
            return ix, None
        if part_ix not in parts:
            raise SystemExit(f"taps: {sid} names part {part_ix} of {item}, the store has parts {min(parts)}..{max(parts)}: "
                             "the release re-cut the water — declare the state in PART_MOVED / PART_GROWN / PART_GONE")
        if sid in PART_GROWN and isinstance(PART_GROWN[sid], tuple):
            part_ix = t517.setdefault(sid, PART_GROWN[sid][0])
            if part_ix not in parts:
                raise SystemExit(f"taps: PART_GROWN {sid} -> {part_ix}: the store has parts {sorted(parts)}")
        elif sid in PART_GROWN and remap(item, part_ix) != part_ix:
            part_ix = t517.setdefault(sid, remap(item, part_ix))
        ok = same_part(g, parts[part_ix])
        if sid in PART_GROWN:
            if ok is not False:
                raise SystemExit(f"taps: PART_GROWN {sid} does not describe the store: part {part_ix} is the golden part {g} unchanged")
            # (2026-11-09) "the label differs" alone let a WRONG part pass: the grown part is named (label, section count)
            want = PART_GROWN_IS.get(sid)
            if want is None or tuple(parts[part_ix]) != want:
                raise SystemExit(f"taps: PART_GROWN {sid} -> {part_ix}: the store's part is {tuple(parts[part_ix])}, "
                                 f"PART_GROWN_IS names {want} — check it is the golden part grown, then name it")
            return part_ix, None
        if ok is False:
            raise SystemExit(f"taps: {sid}'s golden part {g} is not the store's part {part_ix} {parts[part_ix]}: "
                             "the release re-cut the water — declare the state in PART_MOVED / PART_GROWN / PART_GONE")
        if g[0] == "closed all year" and closed.get(part_ix) != 1:
            # the label cannot tell, the closure can: a renumbering put an open part at a closed state's index (T67)
            raise SystemExit(f"taps: {sid}'s golden part is closed all year, the store's part {part_ix} {parts[part_ix]} is "
                             "not: the release renumbered the water — declare the state in PART_GONE")
        return part_ix, None

    def tap(item, part_ix, month, day, weekday):
        o = ord_of.get(item)
        if o is None:
            return {"error": "no item"}
        row = c.execute("select akey from part where item_ord=? and part_ix=?", (o, part_ix)).fetchone()
        akey = row[0] if row else None
        if akey is None:
            raise SystemExit(f"taps: part {part_ix} of {item} has no answer key")
        seg, mom, nseg = c.execute("select seg, mom, nseg from akey where akey=?", (akey,)).fetchone()
        starts = json.loads(c.execute("select starts from seglist where seg=?", (seg,)).fetchone()[0])
        moms = None
        if mom is not None:
            moms = [M[i] for i in json.loads(c.execute("select ix from segmom where mom=?", (mom,)).fetchone()[0])]
        si = E.segment_index(starts, day_of((month, day)), moms, weekday, False)
        cell = c.execute("select ladder, rows, gear, licence, display from cell where akey=? and s=?", (akey, si)).fetchone()
        # the display frame of every day of the leap calendar, each at ITS OWN weekday in 2026 (page v36 segOf(w, md) =
        # segAt(w, md, weekdayOf(md), false), the snapshot's runsOf); day 60 (Feb 29, absent in 2026) at its first moment
        days = []
        for d in range(1, 367):
            if moms is None:
                sd = E.segment_index(starts, d, None, None, False)
            elif d == 60:
                sd = _first_moment(starts, d)
            else:
                sd = E.segment_index(starts, d, moms, WD_OF_DAY[d], False)
            days.append(c.execute("select display from cell where akey=? and s=?", (akey, sd)).fetchone()[0])
        return {"item_ord": o, "akey": akey, "seg": seg, "mom": mom, "nseg": nseg, "starts": starts, "moments": moms,
                "s": si, "cell": dict(zip(("ladder", "rows", "gear", "licence", "display"), cell)), "displayByDay": days}

    def leap(cid, item, part_ix, year, month, day):
        wd = weekday_of(year, month, day)
        akey = c.execute("select akey from part where item_ord=? and part_ix=?", (ord_of[item], part_ix)).fetchone()[0]
        seg, mom = c.execute("select seg, mom from akey where akey=?", (akey,)).fetchone()
        starts = json.loads(c.execute("select starts from seglist where seg=?", (seg,)).fetchone()[0])
        moms = None if mom is None else [M[i] for i in json.loads(c.execute("select ix from segmom where mom=?", (mom,)).fetchone()[0])]
        rd = reading_day_of((month, day))
        si = E.segment_index(starts, rd, moms, wd, False)
        cell = c.execute("select ladder, rows, gear, licence, display from cell where akey=? and s=?", (akey, si)).fetchone()
        return {"id": cid, "item": item, "partIx": part_ix, "date": f"{year}-{month:02d}-{day:02d}", "weekday": wd,
                "readingDay": rd, "akey": akey, "starts": starts, "moments": moms, "s": si,
                "cell": dict(zip(("ladder", "rows", "gear", "licence", "display"), cell))}

    out = {"meta": {"generator": "app/tools/regs-recut/taps.py", "store": {k: meta.get(k) for k in ("format", "section_handles", "reach_digest", "rule_ids_sha256", "answers_sha256", "store_digest", "layout")},
                    "golden": G["meta"].get("sha"), "version": G["meta"].get("version"), "year": 2026}, "states": {}, "unnamed": [],
           "leap": [leap("zymoetz_20360229", "gnis:3137", 0, 2036, 2, 29), leap("zymoetz_20360228", "gnis:3137", 0, 2036, 2, 28),
                    leap("zymoetz_20280229", "gnis:3137", 0, 2028, 2, 29), leap("zymoetz_20280228", "gnis:3137", 0, 2028, 2, 28)],
           "gone": {}, "tidal": {}, "recut": {}}
    for sid, st in G["states"].items():
        item = by_name.get(st["water"])
        part_ix = int(st["part"]) if st.get("part") not in (None, "") else 0
        md = st["md"]; month, day = md // 100, md % 100
        wd = WEEKDAYS[datetime.date(2026, month, day).weekday()]
        ix, gone = resolve(sid, st, item, part_ix) if item else (part_ix, None)
        if gone is not None:
            out["tidal" if sid in PART_TIDAL else "gone"][sid] = {**gone, "md": md, "date": f"2026-{month:02d}-{day:02d}", "weekday": wd}
            continue
        out["states"][sid] = {"item": item, "partIx": ix, "md": md, "date": f"2026-{month:02d}-{day:02d}", "weekday": wd,
                              **({"goldenPartIx": part_ix} if ix != part_ix else {}),
                              **({"grown": grown_why(sid)} if sid in PART_GROWN else {}),
                              **({"tidal": {"reason": PART_TIDAL[sid][2], "tidalPartIx": PART_TIDAL[sid][1]}}
                                 if sid in PART_TIDAL else {}),
                              **({"recounted": {"reason": PART_RECOUNTED[sid][1], "sections": store_parts(item)[ix][1]}}
                                 if sid in PART_RECOUNTED else {}),
                              **(tap(item, ix, month, day, wd) if item else {"error": "no water"})}
    # the waters whose parts the release re-cut: every state of one sees a different list of parts than the golden's
    for sid, gix in t517.items():
        out["recut"][by_name[G["states"][sid]["water"]]] = T517
    for sid in (*PART_MOVED, *PART_GROWN, *PART_GONE, *PART_TIDAL):
        if sid in GONE_NOT_RECUT or sid in t517:
            continue
        w = by_name[G["states"][sid]["water"]]
        out["recut"][w] = RECUT_WHY.get(sid) or out["recut"].get(w) or MOVED_WHY.get(sid) or T115
    decl = [set(PART_MOVED), set(PART_GROWN), set(PART_GONE), set(PART_RECOUNTED), set(PART_TIDAL)]
    twice = sorted(x for i, a in enumerate(decl) for b in decl[i + 1:] for x in a & b)
    if twice:
        raise SystemExit(f"taps: states declared in two categories: {twice}")
    stale = sorted(set().union(*decl) - set(G["states"]))
    if stale:
        raise SystemExit(f"taps: declared states not in the golden: {stale}")
    # unnamed: an evenly spaced sample of handles whose akey is past the named range, half streams and half the rest
    # (wetlands outnumber streams at the top of the handle range; T183's sample held 3 streams, none on its region's base)
    akeys = s.section_akeys()
    first = s.static("answers.unnamed")["first"]
    hs = sorted(h for h, k in akeys.items() if k and k - 1 >= first)
    pool = hs[::max(1, len(hs) // max(1, a.unnamed * 20))]
    named = [(h, s.unnamed_name(h)) for h in pool]
    streams = [p for p in named if p[1].startswith("Unnamed stream")]
    rest = [p for p in named if not p[1].startswith("Unnamed stream")]
    half = a.unnamed // 2
    pick = [g[::max(1, len(g) // max(1, n))][:n] for g, n in ((streams, half), (rest, a.unnamed - half))]

    def base_only(h: int) -> bool:
        """The section's rule set has no tributary rows: its region's base alone (golden.ts `baseUnnamedStream`)."""
        (rs,) = s.con.execute("SELECT ruleset FROM akey WHERE akey = ?", (s.akey_of(h),)).fetchone()
        row = s.con.execute("SELECT j FROM ruleset WHERE set_id = ?", (rs,)).fetchone()
        return row is not None and not s.val("ruleset", row[0]).get("trib")
    # (2026-10-27, r-8b43fb267199) the even sample can hold no stream on its region's base alone, which the edge_unnamed
    # fixture needs: then the first such stream of the pool takes the LAST stream slot (the first, 634, is the Harris
    # Creek tributary WaterScreen.unnamed.test.tsx taps)
    if pick[0] and not any(base_only(h) for h, _ in pick[0]):
        alt = next(((h, nm) for h, nm in streams if base_only(h) and (h, nm) not in pick[0]), None)
        if alt is not None:
            pick[0] = pick[0][:-1] + [alt]
    for h, name in sorted(pick[0] + pick[1]):
        out["unnamed"].append({"handle": h, "akey": s.akey_of(h), "place": s.place_of(h), "name": name})
    Path(a.out).write_text(json.dumps(out, indent=1))
    bad = [k for k, v in out["states"].items() if "error" in v]
    print(f"taps: {len(out['states'])} states ({len(bad)} unresolved: {bad[:5]}), {len(out['unnamed'])} unnamed, {len(out['leap'])} leap -> {a.out}")
    for sid, v in out["states"].items():
        if "goldenPartIx" in v:
            how = "PART_MOVED" if sid in PART_MOVED else "PART_TIDAL" if sid in PART_TIDAL else "PART_RECOUNTED"
            print(f"taps: {sid} golden part {v['goldenPartIx']} -> store part {v['partIx']} ({how})")
    for sid in PART_GROWN:
        print(f"taps: {sid} part {out['states'][sid]['partIx']} grew (PART_GROWN): {grown_why(sid)}")
    for sid in PART_RECOUNTED:
        print(f"taps: {sid} part {out['states'][sid]['partIx']} recounted (PART_RECOUNTED): {PART_RECOUNTED[sid][1]}")
    for sid in PART_TIDAL:
        if sid in out["states"]:
            print(f"taps: {sid} part {out['states'][sid]['partIx']} is the freshwater remainder (PART_TIDAL): {PART_TIDAL[sid][2]}")
    for sid, g in out["tidal"].items():
        print(f"taps: {sid} TIDAL (golden part {g['partIx']}, tidal part {g['tidalPartIx']}): {g['reason']}")
    for sid, g in out["gone"].items():
        print(f"taps: {sid} GONE (golden part {g['partIx']}, now in part {g['nowIn']}): {g['reason']}")
    if bad:
        raise SystemExit(f"taps: {len(bad)} states unresolved: {bad}")


def _first_moment(starts, d):
    import bisect
    i = bisect.bisect_right(starts, d) - 1
    return starts.index(starts[i])


if __name__ == "__main__":
    main()
