# T497 — channels that change water (owner review list, a proposal)

Code 1ab627c95. Side build `data/generated/side/t497` (atlas `atlas/full_next`, release `deliver/release/r-668799e82b0e`),
diffed against the live atlas `data/generated/atlas/full` and the live reach run `data/generated/reaches/full`.

**Rule (owner 2026-11-08, "follow connections"):** an unnamed channel takes the named water its two ends reach, walking
through other unnamed pieces and stopping at named water. If an end reaches the main river, or two different named waters,
or nothing named, the old magnitude rule stays (the channel stays with the main river). Blks that `name_variants` names
(Jeperson Side Channel) are left alone.

**What else moved: nothing.** Section ids are identical (1,958,281 handles), the item set is identical (21,318), braid_sweep
is identical, and the length braid sweep made no cut either way. 26 rules changed bindings, every one only on the sections
below. Two cut points left their river with the channels: the Fraser's junction with a Nicomen Slough polygon
(`fraser_river__lake_328970811`) and the Columbia's lake edge on wbk 329220393. No curated file references either.

20 sections: 19 line sections (24.3 km) and one 11 ha polygon. "Rules −/+" are the water-specific rules lost or gained. Zone and
provincial rules come along only where the change of water kind does it (the polygon).

## Fraser River `gnis:39325` → its sloughs and channels

| blk / section | length | old item → new item | rules − / + |
|---|---|---|---|
| 355991891:0 | 3,132 m | Fraser River → **Nicomen Slough** `gnis:21105` | − Fraser r1 (dead-fin-fish bait for sturgeon only), r3 (**closed 1 h after sunset to 1 h before sunrise**), r5 (**steelhead closed Aug 15–Dec 31**), r6 (do not impede commercial vessels), bait.r3 (sturgeon bait, Fraser in R2) · + nicomen_slough.r1 (8 km/h speed) |
| 355992357:0 | 2,274 m | Fraser River → **Maria Slough** `gnis:13499` | − Fraser r3 (night closure), r5 (steelhead closure), bait.r3 · keeps r1/r6 (they name Maria Slough) |
| 355994867:0 | 333 m | Fraser River → Morey Channel `gnis:14905` | none |
| 355994868:0 | 401 m | Fraser River → Morey Channel | none |
| 355994873:0 | 633 m | Fraser River → Morey Channel | none |
| 355994874:0 | 1,123 m | Fraser River → Morey Channel | none |

Each now carries exactly the rule set of its new water's existing sections.

## Columbia River `gnis:37414` → its channels

| blk / section | length | old item → new item | rules − / + |
|---|---|---|---|
| 356364760:0 | 5,175 m | Columbia River → Baldy Channel `gnis:7958` | none |
| 356364761:0 | 1,229 m | Columbia River → Baldy Channel | none |
| 356364764:0 | 2,056 m | Columbia River → Baldy Channel | none |
| 356364765:0 | 322 m | Columbia River → Baldy Channel | none |
| 356364765:322 | 340 m | Columbia River → Baldy Channel | none |
| 356364768:0 | 1,402 m | Columbia River → Baldy Channel | none |
| **lake:329220393** (unnamed polygon, 11 ha) | — | no water → **Baldy Channel** (stream) | − bait.r5, barbless_single_hook_streams.r2 (lake rules) · + Columbia r3, r4, r5, r10, r12, r13, r14 (Baldy is in the Columbia entry), spring_stream_closure.r1, trout_char_winter_release.r1, single_barbless_hook.r1, barbless r1/r3, bait.r4, species_quotas.r5, trout_char_quota.r3 |
| 356365646:445 | 258 m | Columbia River → Back Channel `gnis:17696` | none |
| 356365649:167 | 116 m | Columbia River → Back Channel | none |
| 356365650:192 | 439 m | Columbia River → Back Channel | none |
| 356366358:0 | 1,010 m | Columbia River → Hotsprings Channel `gnis:8203` | none |

The polygon follows the existing slough-fold ruling (`registry/flowing.py`): an unnamed polygon that a channel item both
enters and leaves is that channel. Before, the channels through it were filed under the Columbia, a river, which never
absorbs a polygon, so it was an unnamed lake. **Please check: it now reads as stream water, under the spring stream closure.**

## Kootenay River `gnis:14097` → Old Kootenay River Channel `gnis:2123`

| blk / section | length | old item → new item | rules − / + |
|---|---|---|---|
| 356365660:39 | 2,290 m | Kootenay River → Old Kootenay River Channel | − kootenay_river_lower.r2/r3 (**exempt from the Apr 1–Jun 14 closure and the Nov 1–Mar 31 trout/char release**) |
| 356365662:1210 | 171 m | Kootenay River → Old Kootenay River Channel | − kootenay_river_lower.r2/r3, r4 (burbot catch and release) |
| 356365665:13 | 1,584 m | Kootenay River → Old Kootenay River Channel | − kootenay_river_lower.r2/r3 |

These now match the channel's own section, apart from two things that depend on location: the Creston Valley WMA rules
(these three pieces are inside the WMA and the channel's original section is not), and burbot r4, which reaches the
original section through the tributary walk but not the 171 m piece. **Please check: without the Kootenay's exemption,
the zone spring closure and winter release now apply to these 4 km.**

## Not moved (the fallback held)

From the scan's "one end on a named sibling" list: 355991968 and 355991970 (Williamson Slough), 355993059 (Widgeon
Slough, Pitt), 356364756 (Baldy), and 356366353 and 356366356 (Hotsprings). Following their other end reaches the main river
(or a second water), so they stay with the main river as before.
