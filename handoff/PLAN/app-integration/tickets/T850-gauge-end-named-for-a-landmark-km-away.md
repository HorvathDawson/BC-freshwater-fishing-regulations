# T850 — A part that ends at a gauge cut is named for a landmark up to 18 km away ("Canyon City bridge → Canyon City bridge")

Status: DONE bf22da0c2 (in r-86d43303bee2 and r-fc137c4040fa, on staging 2026-11-18). Owner: big issue (2026-11-18 13:43). Main chat's area: pipeline/deliver/bundle; found 2026-11-18 by the app chat's Nass salmon check.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T850 | A part ending at a gauge cut is named for a landmark up to 18 km away (Nass: "Canyon City bridge → Canyon City bridge"; 69 of 236) | DONE | bf22da0c2 (r-86d43303bee2) | owner 2026-11-18 (Nass names) |

On the staging release r-f1f2e8da9666 the Nass's open coho stretch (Canyon City bridge → water survey station 08DB001,
~11.8 km) is titled "Canyon City (Gitwinksihlkw) bridge … → Canyon City (Gitwinksihlkw) bridge", and the next part
"Canyon City bridge → Meziadin". The bundle's `split_near` row says gauge 08DB001 is 0.0 m from the bridge.

Cause (read, not yet proved by a test): `gauge_near` in `pipeline/deliver/bundle/topology.py` (~733-737) measures the
distance to the whole section, not to the section's end at the cut; the part's main channel is one section from the
bridge to the gauge, so the bridge reads 0 m from the gauge. 69 of 236 gauge-end names point at a landmark 1.5-18 km
from the cut (Halfway, Zymoetz, Telkwa, Atnarko, Squamish among them). Names only: where rules bind is unchanged.

Fix: measure to the cut point (the section's end vertex at the gauge), name the end for the gauge (or a landmark within a
small radius of it), and pin a test on the Nass and two others.

## Fixed (main chat, 2026-11-18 13:48): rides the 0.8.0 data release (a new release id; the 0.8.0 app build is unchanged)

Cause, proved: `gauge_near` took a point on the gauge's section (with one section, `representative_point()`, not even
the cut) and measured to the WHOLE of each named cut's sections, so the far end of the one section between them read
0 m; and a tributary was measured straight to its nearest drawn line, not to where it joins.

Fix: `gauge_near` measures ALONG THE MAIN STEM, as `crossing_near` already did: the gauge cut's own measure against each
named cut's measure and each named tributary's confluence measure (`_stem_feats`, now shared by both; a tributary whose
drawn sections never come within `TRIBUTARY_REACH_M` of the stem is no feature on it). Nearest within `NEAR_M` (2 km),
else no row: the answers say the cut by its km (T517: never the gauge's code). Test
`test_a_gauge_cut_is_near_what_stands_near_it_along_the_water_never_the_far_end_of_its_section` (fails on the old code:
bridge, 0 m).

On the r-f1f2e8da9666 build (372 gauge cuts end a part; old 236 rows, new 185): 128 same, 57 renamed, 51 now said by km.
Every dropped one checked: the 0 m ones were the bug (Nass bridge, Harrison "Hwy 7 bridge" twice, Pinkut "fish fence");
the ones with a distance were creeks running near the gauge that join 2-7 km away along the river (Pemberton Creek
joins the Lillooet 5.2 km below 08MG005) or never reach it on the drawn map (Loon, Blanchard, Dice: 0.9-1.1 km off).
The Nass's open coho part now reads "Canyon City (Gitwinksihlkw) bridge → km <its km>".

Not this change (pre-existing, read the on-disk bundle only): test_part_geo Cowichan parts 6 vs pinned 5, unlabelled
marks 25 vs 110, test_part_names_unnamed_lake crossings 482 vs >500 — stale pins against today's bundle, to re-pin after
the new release's build.
