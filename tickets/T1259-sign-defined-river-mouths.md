# T1259 — Sign-defined river mouths in DFO's freshwater salmon places (Kitsumkalum first)

Status: OPEN — part 1 is now option B (owner 2026-11-27: the braids become Kitsumkalum water), built by the data and release chat on `fix/T1259-kitsumkalum-braids-own-water` (local); owner: see ## Owner (the measured look).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1259 | Sign-defined river mouths in DFO's freshwater salmon places (Kitsumkalum first) | OPEN | — | part 1 (0.10.0), option B: the braids become Kitsumkalum water; his measured look (braids + the 2.57 km creek) before merge; part 2 (0.11.0) |

Source: the owner, 2026-11-27, about DFO's Region 6 note "Kitsumkalum River (including tributaries) Note: The mouth of
the Kitsumkalum River is designated by fishing boundary signs located approx. 1.25 km downstream of the CNR bridge and
approx. 200 m east of the CNR bridge.", verbatim, in order:
- "make sure this is the real tidal boundary we have too (Note: The mouth of the Kitsumkalum River is designated by fishing boundary signs located approx. 1.25 km downstream of the CNR bridge and approx. 200 m east of the CNR bridge.)"
- "not tidal but mouth i mean will be a cut then closed maybe below? look at region dfo regs to decide how to handle?"
- after seeing the options picture: "the signsd for th emouth are really just saying that the mouth makes kitsumkalum include the braiding next to it on the skeena if that makes sense. it doesnt mean the mainstem of the skeeen is included in kitsumkalum only the sidechannel braiding there."
- asked "should the two braids (2.26 km) answer as the Kitsumkalum's, and when?": "Yes, in 0.10.0 (Recommended)".

All figures below are the data chat's dry-run measurements of 2026-11-27, reported, not re-measured here.

## Part 1 — in 0.10.0 (the data and release chat builds it for the re-cut)
Between the two sign points there are exactly two braids, both FWA Skeena side channels: blue-line keys 360222216
(1.63 km; it leaves the Skeena's main line at the "1.25 km downstream" sign) and 360222215 (0.63 km; it ends at the
"200 m east" sign), 2.26 km together.
- **Superseded 2026-11-27 by option B (## Owner): the braids become the Kitsumkalum's own water**, book rules too, through a validated curated file `waters/line_owners.json`; the paragraph below is the earlier option A.
- Built as curated extents on the DFO places only, not as a registry change: the two braids are added to the
  Kitsumkalum's lowest DFO places and excluded from the three Skeena DFO places that bind them today. They stay the
  Skeena's water for the book's rules and licensing. A registry change was rejected: it would also move the book's
  rules on those 2.26 km (gain the Kitsumkalum's "Angling closed to non-guided non-resident aliens, on Sundays" and its
  bait ban, change classified water from "Skeena River mainstem only" to the Kitsumkalum River, lose the Skeena
  mainstem's lift of "No fishing for steelhead in streams, May 15-Jun 15"), which the owner did not approve.
- Expected answer changes on those 2.26 km and nothing else: pink opens Jul 1-Aug 31 (2 a day; closed today by the
  Skeena's all-salmon closure); coho closes Nov 1-30; sockeye closes Sep 1-15.
- The Skeena's main line stays the Skeena's: its own DFO place is "mainstem waters near the Kitsumkalum River mouth,
  from the confluence with the Zymagotitz River ... upstream to the Classified Waters boundary at the top of Hell's
  Gate", bound today to 11.84 km (the main-line section plus three side channels).
- Checked: OSM's CN line crosses the Kitsumkalum 14.6 m from the curated bridge point.
- Not verified: the signs' real positions on the ground; no imagery was used.

## Part 2 — after 0.10.0 (0.11.0), open lines, not built
Each needs a picture and the owner's decision.
- Places on DFO's Region 6 page whose mouth or boundary is given by signs or a bridge near the mouth: Tseax River (signs
  where the former Nass Back Channel enters the Nass); Illiance, Kitsault, Wilauks (signs near the mouth); Kincolith,
  Sheldens (Spur 19), Pallant (a bridge at or near the mouth).
- The "near the X mouth" places: the Skeena near the Kitwanga River mouth (Mill Creek to the Highway 37 Bridge), the Nass
  near the Meziadin River confluence.
- Whether the BOOK should also treat those two braids as the Kitsumkalum (the book prints no mouth note; assumed no;
  the owner's decision if raised).
- General question for a ruling: 16 of the 20 DFO places worded "mainstem" bind side channels today (e.g. the Skeena
  downstream of the CNR bridge: 117 sections, 223 km). Should "mainstem waters" exclude side channels everywhere?

## Acceptance
Part 1: the 2.26 km answer as above, the Skeena main line unchanged, no other section's answer moves (diff rule
bindings, not just the atlas). Part 2: a picture and an owner decision per place before any change.

Related: T1240 (side channels reached by a DFO stretch), T1177 (DFO one logic).
Target: part 1 in 0.10.0; part 2 in 0.11.0. Depends: T1177. Size: S (part 1), M (part 2). Owner: yes (part 2).

## Owner

- [x] 2026-11-27 — The two Kitsumkalum-mouth braids (2.26 km) answer as the Kitsumkalum's, in 0.10.0: "Yes, in 0.10.0 (Recommended)".
- [x] 2026-11-27 — Are the bait line on the braids and the 23rd Skeena part fine (a) / bait line off (b) / another label (c)? → "thos e braids should be added as name variants of the stream and be moved to the stream instead of being part of the skeena all the way dopwn to the build step?" (2026-11-27); with the A-vs-B picture (B puts the book's Kitsumkalum rules on the braids): "B: the braids become Kitsumkalum water" (2026-11-27). Replaces option A above.
- [ ] 2026-11-27 — The measured look at option B before it merges: the braids, and the unnamed creek on the larger braid (2.03 km) with its feeder (0.54 km), +2.57 km that become Kitsumkalum tributaries (Sunday closure, bait ban, Class II, three Kitsumkalum DFO salmon places). Any opening there needs your yes.
- [ ] 2026-11-27 — Part 2 places and the "mainstem waters" side-channel question: decide per place once its picture is shown.
