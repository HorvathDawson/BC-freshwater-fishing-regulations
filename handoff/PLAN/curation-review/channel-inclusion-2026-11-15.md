# A named water takes its own channels (T755 item 2, 2026-11-15)

Owner, mark 2578: "make sure that when i checked fraser as water for the mainstem items it includes all channels too.
like sea bird island etc."

## Where the restriction was

The editor's "on" ticks send the item ids in the mark (`ExtentFix.items`); `proposals.extent_of` writes them as the
extent's `item_id` / `item_ids`; `reach.extent.resolve_extent` then scopes the extent to those items' sections only
(`scope = ex["item_ids"] or [ex["item_id"]] or covered_ids`). The Fraser's DFO water lists the river and its named
channels and sloughs (`WaterBinding.item_ids`, 13 items, by override: "plus all named channels and sloughs without their
own regulation entries"), so ticking "Fraser River" alone dropped every channel that is its own item. Its anonymous
side channels were never lost (they are the river item's own sections).

## What "this water" means now

`pipeline/atlas/reach/channels.py`: an extent of a DFO place that names items holds on those items AND every item of
the place's water that is a side channel of one of them — it takes water from the item and returns it, it is a stream
(a lake on the line is not), it is the shorter of the two; a channel off a channel too. A channel the curated water
leaves out (Nicomen Slough) stays out. Applied in `build_reach` for an entry carrying the key `to_reach_input` sets, so
the review app's preview, the reach run, a mark from any editor, a suggestion and a hand edit all read the same; no mark
and no place file is rewritten. `DFO_NAMED_WATER_TAKES_ITS_CHANNELS = False` is the old reading (mutation pin,
`test_review_notes_1115.py`).

## Which in-force marks bind differently (scan of every place on a water of 2+ items whose extent names items, 2026-11-15)

| place | mark in force | before | after | channels taken in |
|---|---|---|---|---|
| `2:fraser-river:mainstem-waters-upstream-of-the-cp` (dormant) | 2575 verified, between[gnis:39325] Mission → Spuzzum | 41 sections, 178.3 km | 50 sections, 197.9 km | Maria Slough (Seabird Island), Jeperson Side Channel inside the stretch (11 channels on the water; the others lie outside it) |

No other bound place changes. Saved but not landed yet (waiting on new cuts), naming the Fraser alone, so they bind
WITH the channels when their cuts are built: 2568 (Mission → Hope), 2574 (Mission → Hope), 2576 (Hope → Spuzzum), 2570
(Region 3, Seton → Lillooet signs).

They re-bind by themselves at the next reach run: the meaning is resolved at build time, nothing waits on an apply
(the files already hold `item_id: gnis:39325`, which now means the water with its channels). The binding diff of the
next release shows the 9 sections.
