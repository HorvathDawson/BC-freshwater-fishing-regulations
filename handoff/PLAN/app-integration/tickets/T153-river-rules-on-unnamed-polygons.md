# T153 — A river's rules stop at every unnamed lake polygon on its course

Status: DONE — 62e9cb7c (built on Q124's stated default behind one flag; the owner's answer can flip it). Rebuild:
reach rerun → binding diff → deliver (no atlas rebuild; POLICY_VERSION 27 invalidates the reach cache).

Source: T148 census structural finding (handoff/PLAN/sloughs/v2/census.md, signals 6–7), 2026-10-23.

## Problem
A river entry's reach rule almost never binds a lake polygon on the river's own course: province-wide only 7 lake
polygons carry a rule from a stream entry. 456 unnamed lake/manmade polygons sit on rivers regulated by a named entry
(439 Lake–Definite, 9 Indefinite, 8 reservoir); each is a hole where the river's rules (often stricter: closures, bait
bans) fall back to zone rules. 180 of them were not in the first slough sweep. T55/T149 decide which pieces ARE the
river (fold → stream item); this ticket is the rule for the ones that stay lakes on the river's line.

## Work
1. The decision, with the owner: does a river row's rule apply to an unnamed lake polygon its main flow passes through
   (the book's "river" includes its widenings unless it names the lake)? Default proposal: yes for UNNAMED polygons the
   river's main flow runs through, when the rule's extent covers both sides; a NAMED lake never takes the river's row.
2. One function in the reach walk (may_enter / extent) deciding it, with a pin and a measured binding diff (every newly
   bound polygon named by river).

Rebuild: reach rerun + binding diff + deliver. Owner question Q124.

## As built (62e9cb7c)
- ONE function: `pipeline/atlas/reach/extent.py::river_widenings`, behind ONE flag
  `UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER = True`. Read only there (and in the cache pin); `resolve_extent._out` calls it
  for `whole`, the cuts (`upstream_of`/`downstream_of`/`between`) and a `within` that qualifies a named water (only the
  ponds inside the area). An area-only `within` (zone rules) never widens.
- A polygon is taken when: a waterbody node with no GNIS id or name that no registry water (stream/lake/wetland) holds;
  not already selected; it drains (`lake_out`) into a selected MAIN-FLOW piece P, and a selected main-flow piece on P's
  own blue line, from further up that line, enters it (`lake_in`). Ponds back to back are taken together (≤ 8); a named
  polygon in the chain breaks it. A pond holding the cut, at the reach's top or mouth, on a side channel (1100/1350) or
  fed from another blue line is never taken.
- Reported, never silent: `river_widening` diagnostic per rule (`{"polygons": [...]}`), and `widened` on the extent; the
  extent's `waters` still names only the river.
- `cache.POLICY_VERSION` 26 → 27; pin in `test_reach_builder.py`. Tests: `pipeline/tests/test_t153_river_widenings.py`
  (18, no data), each mutation listed in its docstring fails a named test (run: 12 mutations, all caught).

## Measured (scratch reach run with this code vs data/generated/reaches/full; same atlas, entries digest 4e3f729204098d06)
- Binding diff (`binding_diff`, JSON kept at scratchpad `reach-T153.json`): **284 rules bind different sections, every
  one by GAINING: 1,219 rule_section rows, 0 lost, 0 rescoped; every gained row is a `lake:` polygon — no stream
  section moved, the tributary walk is unchanged.** 3 unresolved rules now bind (below); 20 licensing records gain
  polygons (classified-water designations, `zp:steelhead` targeting +68). 311 rules carry the `river_widening`
  diagnostic (574 polygons); 8 of those never bind (the `feature_types: [stream]` filter — z6 steelhead stream closure —
  Salmon R. 3-26, Horsefly).
- **569 polygons newly bound**, every one listed with its river, area, point and rules in
  `handoff/PLAN/audit/T153-newly-bound.json` (largest first). 566 by a water row, by row region: R1 41 · R2 45 · R3 149 ·
  R4 53 · R5 119 · R6 82 · R7 (7A+7B) 62 · R8 15; 3 only by the derived steelhead rows. 154 rivers; most: Fiftyseven Cr
  33, Dean R 28, Fiftynine Cr 26, Watch Cr 21, Beaver Cr 19, Big Bar Cr 18, Salmon R 14, Denetiah Cr 12, Angus Cr 11,
  Kootenay R 11, Nechako R 11, Endako R 9.
- Against the census (T148, `candidates_v2` river_through + river_regulated + unnamed = 457): **380 of 457 now bound**.
  The 77 left, each checked against the graph: 65 pass every structural test but no rule of the row selects both
  sides (e.g. Deadman R ×10 — its row is "no trout under 25 cm downstream of Mowich Lake" and the ponds are upstream;
  Big Bar Cr 13, McLeod R 10, Bonaparte R 8); 5 are entered by a side channel (Mohun, Elaho, Bridge, Watch, Fergus);
  2 drain into a side channel; 4 are not entered from further up the same blue line (Ashlu, Brothers, Noons,
  Homathko); 1 drains into another polygon only (Kitseguecla).
  189 newly bound are not in the census at all (polygons on rows' extents the sweep did not reach, e.g. ponds on a
  `whole` tributary row like Buttle Lake's tributaries). Sweep classes of the bound: LAKE 208, UNSURE 101,
  LIKELY_STREAM 75, STREAM_BY_BOOK 52, not a candidate 130.
- The 10 largest (all spot-checked against the printed synopsis, PDF page − 2 to p.40, − 6 after):

  | polygon | river (row) | ha | book (printed page) | verdict |
  |---|---|---|---|---|
  | lake:329021790 | South Atnarko R. (Atnarko/Bella Coola [Includes Tributaries]) | 162 | p.43: quotas, cutthroat C&R, bait ban d/s of the park's E boundary, no steelhead fishing; names Tenas Lake only (own item, not taken) | consistent |
  | lake:329026698 | Pinkut Cr | 98 | p.53: bait ban (Pinkut Lake is its own row, not taken) | consistent |
  | lake:329291824 | Stave R (upper, 49.754 N) | 94 | p.26: both rules are spawning-channel `undrawn_part` notes + z2 marking | a NOTE gains a pond far from the channels — harmless (notes never colour), but see Q3 |
  | lake:329502178 | Salmon R 7-15 | 86 | p.61: exempt from spring closure | consistent |
  | lake:329247793 | Kootenay R (upstream of Koocanusa) | 69 | bait ban, C&R, quotas upstream of White R. | consistent |
  | lake:328984081 | Kwinageese R | 49 | p.52: bait ban | consistent |
  | lake:329184498 | McKinley Cr | 45 | p.46: "C&R downstream of McKinley Lake; bait ban" | bait ban right; **r1x (`whole`) gives it C&R although it is UPSTREAM of McKinley Lake** (r1, `downstream_of`, correctly did not take it) — a pre-existing r1x extent defect, now one pond wider |
  | lake:329632842 | Denetiah Cr | 39 | p.65: No Fishing Jul 1–15 (Denetiah Lake its own row, not taken) | consistent |
  | lake:329571952 | Kildala R | 37 | p.51: bait ban Jun 1–Oct 15 | consistent |
  | lake:329632849 | Denetiah Cr | 37 | p.65 as above | consistent |

## Adversarial review (2026-10-23)
- Second copies: the policy constant is read only in `extent.py` (+ cache pin) and `river_widenings(` is called only
  there — pinned by `test_one_function_decides_it`. `graph.tributaries._lake_mid_reach` asks a near-identical geometric
  question (a lake mid-reach on the same blue line, entered from further up) for a DIFFERENT purpose (whose inflows are
  tributaries; it also passes through named lakes and accepts one piece entering and leaving). Kept separate on purpose:
  folding them would make a named lake mid-reach a widening, or narrow the trib walk. Stated here so nobody "dedupes" it.
- Tests passing for the wrong reason: mutated 12 conditions; 11 were caught, `f.kind == "lake_in"` was NOT — added
  `test_only_the_line_entering_the_pond_counts`, now caught. The pond-count test asserts whole section lists, not
  membership.
- Measurement: re-read the diff — the claim "only ponds moved" checked by streaming both 19 GB tables (0 non-lake rows
  gained/lost, 0 rescoped). Unresolved changes were all `no_sections → bound`, never the reverse.
- Found: (a) **steelhead knock-on** — a pond a steelhead row now binds is a "known steelhead water", so the zone
  LAKE-variant steelhead rules bind it: z5 trout_char_quota.r6b +44, z6 r9b +23, z1 trout_quota.r5b +1 (all three
  were unresolved `no_sections` before), zp:steelhead r1b/r2b/r4b +68 each. Correct under the policy (the pond is the
  river, the river is steelhead water) but it is the lake wording of the rule landing on "river" (Q2). (b) McKinley
  r1x (above). (c) `_HELD` caches the held-section set per registry object (identity-checked); a long-lived process
  that loads many registries keeps each alive — harmless for the CLI and review app.
- Shared with the review app (AGENTS 16): the review app calls the same `build_reach`/`resolve_extent`, so it shows the
  ponds too; the 3,038-rule parity check was not re-run (it predates the corpus).

## Owner questions (built with the default; say if not)
- **Q124** (open): does a river row's rule apply to an UNNAMED lake polygon its main flow runs through when the rule
  covers both sides? DEFAULT BUILT: yes; a named lake never. One flag flips it back
  (`extent.UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER = False`, bump POLICY_VERSION).
- Q2 (new): a widened pond on a steelhead row picks up the zone's LAKE steelhead rule (it is FWA kind lake).
  Default: accept — same take (wild steelhead 0) as the stream rule in those regions.
- Q3 (new): a TRIBUTARY walk still never takes ponds (p.80: "tributaries" are streams); only ponds on the row's own
  extents widen. Default: keep.
- Catalogue fix (not this ticket): `r5:mckinley_creek@5-2` r1x is `whole` but the book says downstream of McKinley Lake.

## Follow-up: the card gate on the Kitimat's pond (2026-10-23) — A STEELHEAD RIVER'S WIDENING IS ANADROMOUS
**Found by** the batch-7 rebuild: `pipeline.deliver all` stopped at the card gate, 12 problems, all on answer key 2336
(rule `z6:trout_char_quota::trout_char_quota.r1`, at 01-15, 06-15, 07-15, 10-10): the size group "Cutthroat trout, brown
trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm between them" binds RB over 50 cm but does not
name it; the tier "Of these, only 1 may be over 50 cm — 2 if one is a hatchery steelhead" caps RB over 50 cm but RB sits
outside it; kind RB sizes [[0, 50]] are not the lengths RB may be kept at.

**Key 2336** = rule set 1709, ONE section: `lake:329233923` (the Kitimat River's unnamed pond, 53.974 N −128.073, newly
bound by T153), kind lake, steelhead known (the Kitimat's own water, via its widened `whole`), steelhead rules apply,
`anadromous` FALSE. Its trout rules: the Kitimat's r4 "Hatchery rainbow trout (adipose clipped, <50 cm) daily quota = 5"
(RB, hatchery, `lengths` ≤50 / >50 take 0), r5 "Hatchery steelhead (adipose clipped, >50 cm) daily quota = 2", Region 6's
r1 trout/char 5, r2 "1 over 50 cm", r3 DV/LT 3 and the lake twin r9b "all wild steelhead" (+ zp:steelhead r1b/r2b/r4b).
So the reader kept hatchery rainbows to 50 cm and a WILD rainbow at ANY size (1 over 50 under r2); the builder worded RB
from the leading (hatchery) origin, "50 cm or less", and left RB out of the size group — the gate was right that the card
misstated the rules. The river's own stream sections pass: there `anadromous` is true, so a rainbow over 50 cm IS a
steelhead (wild: released; hatchery: the river's 2).

**Verdict: a presence defect T153 introduced, not a builder gap.** The book: p.80 "steelhead: a rainbow trout longer than
50 cm in WATERS where anadromous rainbow trout are found" (not "streams"); the Kitimat row, p.51, splits rainbow from
steelhead at 50 cm on origin-marked fish ("Hatchery rainbow trout (adipose clipped, <50 cm)" / "Hatchery steelhead
(adipose clipped, >50 cm)") — a split that reads only where a big rainbow is a steelhead. T153's premise is that the pond
IS the river (its main flow runs through it; steelhead pass through it to spawn above), so a wild 60 cm rainbow there is
a wild steelhead and goes back (Region 6 "all wild steelhead" must be released, p.49 zone box; the provincial release). Wording the card
"wild rainbow trout over 50 cm: 1 may be kept" would tell an angler to keep a wild steelhead 10 m from where the river's
card releases it. The AGENTS 54 rule `anadromous = KNOWN ∧ STREAM ∧ RULES APPLY` (user ruling 2026-10-03) was written for
real lakes (Khartoum, Lois: their own rows print "hatchery steelhead"; resident big rainbow are rainbow there); a T153
widening is not a lake in that sense.

**Fix (one function, one flag):** `pipeline/atlas/reach/steelhead.py::Presence.anadromous` — steelhead rules apply AND
(a stream OR a pond a steelhead row's OWN water took as its river's widening). `Presence.add_row` collects those ponds from
the own-water binding's `river_widening` diagnostic (`self.widened`, kept inside the row's own sections). Flag
`steelhead.A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS = True`, read only there; `cache.POLICY_VERSION` 27 → 28 (pin in
`test_reach_builder.py`). With T153's flag off there are no widenings, so this changes nothing. Tests:
`pipeline/tests/test_t153_steelhead_widening.py` (whole presence rows of a toy river + pond; mutations listed in its
docstring, run). The answers builder is unchanged.

**Measured** (scratch reach run with this code vs `data/generated/reaches/full`, same atlas): `binding_diff` — **no
binding changed**; `rule_extent`, `rule_diagnostic`, `licensing_section` byte-identical. `steelhead_presence`: exactly
**68 rows change, every one a `lake:` polygon on the T153 list, only `anadromous` false → true** (the 68 Q147 counted:
Dean R 28, Atnarko/Bella Coola 9, Zymoetz/Copper 6, Telkwa 5, Kwinageese 4, Burnt Bridge Cr 4, Damdochax 3, Bulkley 2,
Nekite 2, Kitimat 1, Kakweiken 1, Sustut 1, Chilcotin 1, Ksi X'anmas 1). Scratch deliver on that run (bundle → verdicts → export → answers → card_check): **card gate 0 problems** (2,433 keys, 7,673 cards, 17,132 groups); key 2336 is now `steelhead_water` true and its card reads: the Kitimat River's rule — steelhead 2 ("hatchery rainbow trout over 50 cm"); the rest of the 5 — rainbow trout 5 "50 cm or less", the size group naming cutthroat, brown, Dolly Varden/bull, lake and brook trout. Rebuild needed: reach rerun (POLICY_VERSION 28 invalidates the cache) + deliver; no atlas rebuild.

**Owner question Q152** (built with the default): a pond a steelhead river takes as its widening is anadromous — a rainbow
over 50 cm in it is a steelhead, as on the river. Flip: `A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS = False` (then the
answers builder must learn to word a card where hatchery and wild rainbow keep at different sizes — it cannot today).
