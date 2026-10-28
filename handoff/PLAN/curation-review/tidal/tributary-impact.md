# Tidal removal vs tributary rules — impact check (2026-10-28)

Question (owner 2026-10-28): when tidal splits land, the piece of a stream below its tidal split is REMOVED from the
freshwater map. The owner believes no tributary ("includes tributaries") rule is actually affected. This checks it.

Inputs: T212 proposals `tidal-candidates.json` (961 of 1,163 with a cut above the mouth, > 1 m; the rest cut at the
mouth and remove nothing) × the live reach run `data/generated/reaches/full/rule_section.jsonl.gz` (`trib` = the
sections a rule got from its tributary walk) × FWA watershed codes (`bc_fisheries_data.gpkg` `streams`). A tributary
joins a river's tidal piece when its code's position on that river is below the river's local code at the cut.
Read-only; scripts were scratch (method above is the whole of it). Proposals are unreviewed, so counts move with review.

## Answer
**The belief holds — on one condition the build must keep:** the tributary walk runs on the full network (or before the
tidal piece is cut away), and a stream that enters a river's tidal piece is still that river's tributary. Then the only
thing any tributary rule loses is the tidal piece itself, and only **2 tributary-walk sections lie wholly below a cut**
(both are slivers at a creek mouth). If instead the walk ran on the freshwater-only network, **29 rules** would lose
whole tributary trees (Skeena 179 sections, Nass 262, Bella Coola 400, …). So: not a rule problem, a build-order rule.

## Counts
| | rules | waters | sections |
|---|---|---|---|
| A. Walk enters a tributary that has its own tidal piece (section on that tributary starting below its cut) | 14 | 8 rule sets, 17 tributaries | 24 rows; **2 wholly below** (Gordon r1 Wiggs Creek 0–29 m; Nass r1 Ksi Gingolx 0–1562 m), the rest straddle the cut (cut splits them, rule keeps the upper half). 19 of 24 continue above the cut = the walk passes THROUGH the tidal piece |
| B. Tributaries joining the rule's own river inside its tidal piece (walk reached them through it) | 25 | 13 rivers | see table B |
| A ∪ B | **29** distinct rules | | |
| Fraser below Mission | 0 tributary rules | | the lower Fraser carries zone rules + `fraser_river_upstream_of_mission` (straddling section only) |

### A — tributary with its own tidal piece
| rule(s) | tributary (cut m) | below / above the cut |
|---|---|---|
| atnarko_bella_coola_rivers r3 r4 r9 r10 r13 | Necleetsconnay River (616.6) | 0–20518 straddles |
| campbell_river.r4 | Nunns Creek (1424.5) | single section, straddles |
| capilano_river r2 r3 | Vinson Creek (69.3), Brothers Creek (46.0) | straddle |
| gold_river.r2 | Ahaminqas (15.4), Deer (70.1), Cascade (39.1) creeks, Ucona River (80.4) | straddle |
| gordon_river.r1 | Wiggs Creek (56.5) — **0–29 m wholly below**; Browns Creek (1597.5) | straddle otherwise |
| nass_river.r1 | Ksi Gingolx (2216.3) — **0–1562 m wholly below**; Chambers Creek (8837.4) | |
| skeena_river.r1 | Carlisle (3214.5), Kildala (2838.5), MacMillan (2555.7), Boneyard (2506.1) creeks | straddle |
| squamish_river.r1, squamish_river_tributaries.r1 | Monmouth Creek (341.1) | straddles |

### B — tributaries entering the river's tidal piece
| rule(s) | river (cut m, confidence, crossing) | trib sections / blue lines |
|---|---|---|
| atnarko_bella_coola_rivers r3 r4 r9 r10 r13 | Bella Coola (2183.3, low, blobs) | 400 / 394 |
| brem_river.r4 | Brem (1539.6, low, blobs) | 32 / 32 |
| campbell_river.r4 | Campbell (1630.7, medium, clean) | 2 / 2 |
| chemainus_river.r1 | Chemainus (2090.6, low, blobs) | 1 / 1 |
| gold_river.r2 | Gold (4074.9, medium, clean) | 38 / 34 |
| gordon_river.r1 | Gordon (1106.3, medium, clean) | 13 / 11 |
| kildala_river.r1 | Kildala (847.1, medium, clean) | 4 / 4 |
| kitimat_river r2b r3 r4 r5 r7 r8 | Kitimat (7137.3, low, blobs) | 22 / 19 |
| klinaklini_river.r1 | Klinaklini (2791.3, low, blobs) | 22 / 22 |
| mamin_river.r4 | Mamin (2971.6, low, blobs) | 3 / 3 |
| nass_river.r1 | Nass (1821.8, medium, clean) | 262 / 252 |
| nitinat_river.r1 | Nitinat (28142.8, low, blobs — measured through Nitinat Lake) | 29 / 29 |
| skeena_river.r1 | Skeena (6404.9, high, clean) | 179 / 130 |
| tlell_river r1 r2 r5 | Tlell (5750.3, medium, misses) | 1 / 1 |

## Side findings (not tributary walks; for T166 review)
- 143 water-row rules bind a section starting below a proposed cut; 27 bind a section **wholly** below one (they lose
  it — the intended removal): campbell r6, gold r2 r4, gordon r1, nanaimo r1 r6, qualicum r1 r3 r5 r6 r7,
  somass r2 r2x r3, tlell r1 r2 r5, coquitlam r2, pitt r2, rainy r1, kitimat r1 r3 r4 r5 r7 r8, nass r1.
- **kitimat_river.r1 would bind nothing**: its whole extent (blk 360885824, 4295–5046 m) lies below the proposed
  Kitimat cut (7137.3 m, low, blobs). Either the cut is too high or the rule is a tidal-reach rule — review that
  candidate before accepting it.
- Side channels / braids on blue lines other than the candidate's own line are not covered here (T166 step 2 cuts them
  at the same measure).

## What T166/T212 must do because of this
1. Expand tributaries on the full network (walk first), THEN drop the tidal pieces — never walk the freshwater-only
   graph. Pin: Skeena r1 keeps its 130 lower-estuary tributaries; Nass r1 keeps 252.
2. A stream entering tidal water stays a tributary of its river (no rule text says otherwise; owner to confirm if a case
   turns up).
3. Binding diff at build: expect only (a) tidal-piece sections dropped, (b) straddling sections split; anything else in
   a tributary rule is a bug.
