# Missing-cuts audit (synopsis rules, not the DFO feed)

2026-11-13 · read-only · checked against release `r-adc4e248a163` (reach run `data/generated/reaches/full`,
digest `ece9fed50dc55db2`, which is the same digest as the release's bundle) and atlas `data/generated/atlas/full`.

**Short answer: Haida Gwaii has no missing cut.** All 8 Haida Gwaii rows are in the data, and every boundary
the book prints for them has a cut that the built output uses. The province has one clear missing cut (Pitt River,
"at Grant Narrows"), one rule that covers too much (Ruby Creek), one rule that covers nothing (Gagnon Creek), 6
lakes that matched no water, and a set of wordings the owner needs to rule on. The large group is lake areas
that are noted on the lake but not drawn (80 rules). That group is deferred by design.

---

## 1. Haida Gwaii (Region 1 file, MUs 6-12 / 6-13, printed pp. 15–19)

The book has 8 rows in these MUs (raw extraction, every row checked). The data has all 8 entries, each
matched to its FWA water.

| Water | Extent wording (printed page) | What the data does now | Verdict |
|---|---|---|---|
| Copper Creek | "No Fishing from Skidegate Lake to signs at second bridge 6 km upstream of tidal boundary, Feb 1-Apr 30" (p.15) | There is a cut at the South Bay Main bridge (7.68 km on the line), and the lake edge is at 13.09 km. The rule binds exactly 7,679–13,091 m. The other rules cover the whole creek. | ok. Two cut candidates at the lake; the reach picks the lake end nearer the bridge by the owner rule. Owner question Q1 (tidal). |
| Datlamen Creek | none ("Bait ban Nov 1-Apr 30; Class II…") (p.15) | Whole creek plus tributaries | ok |
| Deena Creek | "No Fishing upstream of fishing boundary signs at second bridge approximately 5 km upstream of the tidal boundary [Incl. Tribs], Feb 1-May 31" (p.15) | There is a cut at 5.31 km. The rule binds 5,311 m upward, plus the lake and its tributaries. | ok, but the cut position is a guess. Its cut note says no second bridge is mapped, so the point is 5.0 km above the FWA tidal crossing. Q2 |
| Honna River | none (p.16) | Whole river, including Stanley Lake | ok |
| Mamin River | "No Fishing upstream of fishing boundary signs on third bridge approximately 10 km upstream of the tidal boundary, Feb 1-Apr 30" (p.17) | There is a cut at 11.16 km (Mamin Main crossing). The rule binds 11,162 m upward. | ok, but the cut position is a guess. Only 2 bridges are mapped, so which one is "third" is unconfirmed. Q2 |
| Pallant Creek | none (p.17) | Whole creek. The DFO cut at the bridge near the mouth exists but no synopsis rule uses it. | ok |
| Tlell River | "Tidal waters regulations apply to waters downstream of tidal boundary sign located 1.5 km upstream of Hwy 16 Bridge" (p.18) | The tidal limit is this cut (5.75 km), and the freshwater rules are trimmed below it (3 sections removed). The advisory itself (`tlell_river.r5`) now binds **no part of the river**, only 13 tributary sections that join below the sign. | ok for the cut. Rule placement is a question: Q3 |
| Yakoun River | "No Fishing from Yakoun Lake downstream approximately 13 km to fishing boundary signs, Oct 1-Apr 30" (p.19) | There is a cut at 46.04 km and the lake is at 61.0 km. The rule binds 46,044–61,002 m (15 km) plus tributaries. | ok. The book says ~13 km, but the cut sits at the surveyed sign position from the 2021-22 Yakoun Guardian report. |

Haida Gwaii zone rows (`z1:hg_quota`, `z1:hg_bait_ban_streams`) are MU-wide and name no boundary.

I checked the bundle as well as the reach run: in `bundle.sqlite` each of these rules sits on exactly the sections
listed above (Copper 7,679–13,091; Deena 5,311–17,881; Mamin 11,162–32,384; Yakoun 46,044–61,002).

If the owner remembers missing Haida Gwaii cuts, they were probably fixed before this release, or they are
DFO-feed cuts (Yakoun 6 Mile Bridge, Pallant bridge near the mouth), which this audit did not cover.

---

## 2. Province-wide

What I checked: 1,393 water entries and 3,043 rules (all `r*` entries in `region-*.json`). Zone and provincial
rows were not checked one by one.

### Counts by class

| Region | a: no cut | b: unplaced (noted, not drawn) | c: wrong place | d: binds nothing / unmatched |
|---|---|---|---|---|
| 1 (incl. Haida Gwaii) | 0 | 2 lake | 1 (Tlell advisory, Q3) | 0 |
| 2 | 1 | 10 lake + 2 stream | 1 | 0 |
| 3 | 0 (+1 owner question) | 23 (all Shuswap/lake areas) | 0 | 0 |
| 4 | 0 | 10 lake + 1 stream + 2 carve-out notes | 0 | 0 |
| 5 | 0 | 7 lake | 0 | 5 lakes (18 rules) |
| 6 | 0 (+1 owner question) | 8 lake | 0 | 1 lake (3 rules) |
| 7 | 0 | 9 lake | 0 | 1 rule |
| 8 | 0 | 11 lake | 0 | 0 |
| **Total** | **1** | **85** (80 lake areas, 3 stream, 2 carve-outs) | **2** | **22 rules** |

None of the split ids that rules name is missing from the atlas: every one appears in `splits.resolved.json` or
the bundle's `split` table. No rule with a cut covers the whole water by accident; the 5 cases that looked that
way all checked out (Davie, Pennask, Bull, Burton, Meziadin).

### Class a: rule names a place, no cut, rule covers the whole water

1. **Pitt River `pitt_river.r2`** (R2, p.25), "speed restriction at Grant Narrows (10 km/h)". It binds 52 of 53
   Pitt River sections, both above and below Pitt Lake. The bundle label has also lost the place: it reads only
   "Speed restriction (10 km/h)". Cause: the place was not parsed into an extent. Grant Narrows is at the lake's
   outlet, where a lake-edge cut already exists (20.78 km), so this is a place on the water, not a reach.
   Probably `undrawn_part`.
2. *(owner question)* **McArthur Island Slough `mcarthur_island_slough.r1`** (R3, p.31), "No powered boats from
   westerly entrance to 12th Street entrance to Park". It covers the whole slough (3 sections) and the label
   dropped the wording. Whether that span is the whole slough is the owner's call.

### Class c: a cut exists but the rule lands on the wrong stretch

1. **Ruby Creek `ruby_creek.r1`** (R2, p.25), "No Fishing from Ruby Lake to fishing boundary signs
   approximately 100 m downstream". It is authored as `upstream_of` the sign cut (10.49 km), so it binds the
   correct 100 m piece (10,491–10,591) **and also the creek above Ruby Lake** (14,037–15,090, up to Ambrose
   Lake) plus 6 tributary sections. The wording is a `between`: the Ruby Lake cut exists at 10.59 km.
2. **Tlell River `tlell_river.r5`** (Haida Gwaii, R1, p.18). The tidal advisory binds 13 freshwater tributary
   sections and none of the river. Low impact (advisory only). See Q3.

### Class d: matched nothing or binds nothing

- **Unmatched lakes, which bind nowhere:** Frog Lake, Hidden Lake, Secret Lake, Square Lake (MU 5-6), Redfern
  Lake (5-15), Squirrel Lake (`r6:squirrel_lake@6-1`). 21 rules in total, all `no_sections_for_items` in
  `rule_unresolved.jsonl`. These are not cut defects; the water was never found.
- **Gagnon Creek `gagnon_creek.r1`** (R7, p.59), "Exempt from spring closure downstream of fishing boundary signs
  near its mouth". The sign cut is at the Williston Lake edge (1.76 km), and the creek has no section below
  1,761 m, so `downstream_of` selects nothing (`no_sections`). The exemption applies nowhere. Cause: the cut
  coincides with the creek's lower end.

### Class b: noted on the water, never drawn (count + list)

There are 83 `undrawn_part` rules plus 2 `no_extents` carve-out notes.
- **80 are lake closure areas or lake parts.** This is the known deferred class, so I have not listed them one by
  one. Examples: Mahood Lake ×8, Shuswap Lake ×7, Pitt Lake, Sakinaw, Babine "within 400 m of Pinkut Creek",
  Lake Revelstoke "Mica Dam to … Bigmouth Creek", Upper Arrow drawdown, Dinosaur Lake, and the "as buoyed and
  signed" swim areas. The full list can be regenerated with the script in §4.
- **3 on streams:** Harrison River r3/r4 "in small bays along the river as signed" (R2, p.23) and Columbia River
  r12 "in wetlands" (R4, p.35). There are no boundaries to cut.
- **2 carve-out notes with no extents:** `kinbasket_lake_tributaries.r2` and `kootenay_lake_tributaries.r2`
  ("Does not include the Columbia/Kootenay River upstream of …"). They are held as notes.
- The 2 unplaced pieces in `report.json` (Noons Creek, Seymour River: `flows_to_the_sea_unjoined`) are pieces,
  not rules.

### Separate group (not counted above): "on part(s)" rules that colour the whole water

There are 30 rules whose book text says only "on part(s)" with no place, all vessel rules. Examples: Brannen,
Elk, Prospect, Shawnigan, Cowichan lakes; Courtenay, Cowichan, Nanaimo, Stamp, Coquitlam, Chilko rivers; Nicomen
and Strawberry sloughs. By region: R1 17, R2 9, R5 2, R8 2. The bundle labels each one "… on parts", but its
extent is `whole`, not `undrawn_part`, so it binds every section of the water. AGENTS rule 13 lists "on parts" as
`undrawn_part`. The book names no boundary, so no cut is possible. Whether these should be `undrawn_part` is the
owner's call (Q5).

---

## 3. Owner questions

- **Q1 Haida Gwaii tidal boundary.** The book measures Copper Creek, Deena Creek and Mamin River distances "from
  the tidal boundary", but `tidal_limits.json` puts all three at **the mouth** (the auto-generated Oct 28 mouth
  rows). On the Mamin, FWA's tidal crossing is ~2.1 km up, so the freshwater rules cover ~2 km that FWA treats
  as tidal. The Dean River ("to tidal boundary", R5) is the same kind of case. Keep the mouth?
- **Q2 Guessed cuts.** Deena (5.31 km) and Mamin (11.16 km) are marked `low` confidence in the bundle: no second
  or third bridge is mapped. Across the province, 34 cuts carry a confidence mark in the bundle (`split.confidence`: 12 low, 22 medium),
  and these two are among them. One more
  is a distance mismatch: McRae Creek falls at 2.33 km against "approximately 4 km upstream of Christina Lake"
  (R8, p.70). Check on the ground or from imagery?
- **Q3 Tlell advisory.** Should the "tidal waters regulations apply below the sign" note sit on the tidal piece
  (it carries no provincial rule now) rather than on the freshwater tributaries below the sign?
- **Q4 Lakelse River** (R6, p.52): "The mouth of the river is designated by signs 1.7 km downstream of Lakelse
  River logging road bridge". This is a book-defined end of the river, and there is no cut for it. The
  power-line cut (3.75 km, "3.5 km upstream of the mouth") suggests the designated mouth is within ~0.25 km of
  FWA's. Cut it, or leave it?
- **Q5 "On part(s)"** (30 rules above): `undrawn_part` or whole?
- **Q6 Stave River Ruskin channel `stave_river.r1`** (R2, p.26): "from the inlet near the dam downstream to the
  boat ramp crossing". The whole channel item (4 pieces) is bound. Is that the same span?
- **Q7 Kootenay River `kootenay_river_lower.r1`** (R4, p.37): "CPR Bridge near Creston downstream 2 km to
  navigation dolphin". The cut ends at the Kootenay Lake edge, 0.67 km down. The other ~1.3 km is in the lake,
  which falls in the deferred lake class.

## 4. How I checked, and what I could not

- **Book side:** every row of `data/generated/regs/extraction/synopsis_raw_data.json` in MUs 6-12/6-13, against the
  curated entries in `data/curated/regulations/synopsis/region-1.json` (Haida Gwaii is administered from Region 1).
- **Output side:** for every rule, the sections it binds (`rule_section.jsonl.gz`, own vs tributary), its
  unresolved reason (`rule_unresolved.jsonl`), its diagnostics (ambiguous cut, tidal trim), the registry item's
  full section list (`atlas/full/registry.json`), the resolved cuts (`splits.resolved.json`), and the bundle's
  `split`, `section_measure`, `ruleset` and `rule` tables. The Haida Gwaii bindings were re-checked directly in
  `bundle.sqlite`.
- **Sweep:** I flagged rules whose wording has a place word (upstream / downstream / between / from…to / falls /
  bridge / signs / outlet / mouth / within N m / at <Name>…) but whose extent is `whole`, and rules whose cut does
  not divide the water. Then I read every flagged rule by hand (about 110) and dropped false hits: size
  "between 60 and 90 cm", hook "from point to shank", hours "from 21:00 to", Region 5's whole-water spring-closure
  exemption twins (`r*x`, which are whole by design), and entries clipped by the entry's own name extent (which
  works; checked for all ~40 name-extent entries). I also listed every rule with "N km / N m" wording beside its
  cut position (about 90) and read them for evident mismatches.
- Scratch scripts and outputs: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/51a6025f-10ca-4232-b730-e92b1263ce2f/scratchpad/`
  (`sweep.py` → `sweep.json`, `show.py <entry>` prints one entry's sections and bindings).
- **Not checked:**
  - Whether each cut sits on the right ground feature, except where the distance or the cut note makes it
    evident. I did not open a map.
  - Zone and provincial rows (`z*`, `zp:*`).
  - The DFO feed.
  - Licensing records (Class II, stamps). Their extents are whole on every Haida Gwaii row, which matches the
    book.
  - Whether the tributary walk on each rule is right.
  - What the app shows when it combines stretches. The bundle groups sections into parts, and I did not test
    how they display.
  - A wording with no place word that still implies a boundary would not have been flagged by the sweep.
