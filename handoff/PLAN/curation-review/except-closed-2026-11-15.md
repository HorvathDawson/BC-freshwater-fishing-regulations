# "except closed from … to …" inside a DFO place (T743, 2026-11-15)

Owner: "need help representing this ((middle) between Shuswap Falls and Mabel Lake except closed from the water survey
station (located at 50°19.105' N and 118°48.857' W) approximately 200 m down stream of Shuswap Falls Hatchery to WTFB
signs (located at 50°18.181' N and 118°49.105' W) one km down stream.)". Everything below is a proposal.

## Why it was missed (read)

`source_text.excludes` is the specific-area cell's `<li>` bullets only (`parse._extract_cell` → `untangle` →
`locations.extract`; the feeds worker's `untangle.ts` is the twin). The Shuswap clause is inline text, so `excludes` is
`[]` and `closures.derive` skipped the row.

## Change

`pipeline/regs/dfo_salmon/closures.py`: `inline_items` reads "except closed / except no fishing from <A> to <B>" from
the stored words when the row has no bulleted list, as ONE stretch piece (`inline: true`) with both ends' words and
printed coordinates (`coords_in`, degrees + decimal minutes, exact). Keyed with DFO's typing slips aside, so the dormant
wording ("( located", "200m") and the live one are one piece. The scrape is unchanged (no worker parity work, no places
file rewritten).

Corpus scan (run: every places row + 63 cached history pages, 4,585 rows): only the two Shuswap wordings match (on
every copy since 2023-02, coordinates identical). Not read, on purpose: Nanaimo / Nitinat / Somass / Stamp / Dean /
Skeena ("except … below": pointers to other rows), Chilliwack ("with exception of … closure area identified above"),
Puntledge ("excluding the Morrison Creek closure below"), Stave ("except you shall not fish for salmon in … Ruskin /
Northrop Spawning Channel": side channels DFO lists as rows of their own).

Derived entries before/after (run): the 4 existing entries (Thompson ×2, Babine ×2) are byte-identical; 2 added
(`8:shuswap-river:middle-between-shuswap-falls-and-m#closed`, `…~2#closed`): rule "Closed" (salmon, take 0, no
season claimed, as Babine), one stretch.

## The coordinates (run, FWA `streams` in `data/source/bc_fisheries_data.gpkg`, blue line 356363194)

| | printed | decimal | river measure | off line |
|---|---|---|---|---|
| end 1, "the water survey station" | 50°19.105' N 118°48.857' W | 50.3184167, −118.8142833 | km 133.08 | 13 m |
| end 2, "WTFB signs … one km down stream" | 50°18.181' N 118°49.105' W | 50.3030167, −118.8184167 | km 135.13 | 8 m |

Along the river between them: **2,054 m** (straight line 1,738 m).

- Flow: measure rises upstream; elevation 412 m at km 133 → 421 m at km 136.3, a 28 m step at km 136.35–136.58 (Shuswap
  Falls; GNIS falls point km 136.50; Wilsey Dam, WRIS, km 136.58), reservoir above. **Downstream is north**, to Mabel Lake.
- WSC gauge **08LC003 "Shuswap River near Lumby"** (`gauges/matches.json`: 50.30278, −118.81861) is at km 135.15, **27 m
  from end 2**. So the point printed for the SIGNS is the water survey station; the point printed for the STATION is
  2.05 km further downstream.
- The hatchery (DFO "Shuswap Falls Hatchery", 115 Shuswap Falls Road) is in no repo layer; the station sits 1.35 km
  below the falls, so "200 m downstream of the hatchery" fits a hatchery ~km 135.35 (inferred, not confirmed).
- Reading: the two points look **swapped** (station first in the words, second in position), and the "one km" does not
  match the 2.05 km between them. Either the first coordinate is mistyped (1 km below the station is km 134.13,
  ≈ 50°18.601' N 118°49.036' W), or "one km" is loose.

Stored as printed. Proposed option: station (printed end 2, km 135.13) → 1 km downstream (km 134.13).

## Proposal files (generated, tracked)

- `data/generated/review/dfo_closure_entries.json`: the 2 Shuswap entries added.
- `data/generated/review/dfo_closed_area_geometry.json`: one Shuswap piece (key `200d03a74aaf`, on `gnis:25478`,
  confidence low, a note naming the mismatch), new cuts `shuswap_closed_station_as_printed`,
  `shuswap_closed_wtfb_as_printed` (coord anchors at the printed points), and the alternative end 0
  `shuswap_closed_1km_below_station` (printed end 2 −1,000 m).

## What to save today (entry A)

The place `…~2` (and the dormant twin): `between` the Shuswap Falls cut and the Mabel Lake end, tributaries off. Do not
cut the closed stretch out by hand: it arrives as entry B (the closed-areas panel of the place), confirmed with a
`closed_area` mark.

## Thompson (read)

Entry B for the live Thompson `…~2` IS derived and its 3 pieces are placed as proposals. What is missing is downstream
of the review: no `closed_area` mark is confirmed (`dfo_salmon/closed_areas.json` does not exist), and nothing in the
atlas, reach or feed reads entry B yet (the T573 build-side step, `closed_areas.ABOUT`: "Nothing in the atlas or the feed
reads this yet"). The same gap holds for the Shuswap. Not a defect of `derive`/`attach`; not fixed here.
