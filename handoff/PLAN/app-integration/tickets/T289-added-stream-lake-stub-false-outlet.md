# T289 — Added-stream lake stub makes a false lake outlet (Thomas Creek shows open)

Status: DONE 2026-10-29 (852ecc4f; atlas promoted; release r-1f5d93644c5b, local LATEST — verified: Thomas + 4 tribs bind burnaby_lake_s_tributaries, Noons piece tidal, Cranberry mark on the mainstem with `low`)

Source: owner 2026-10-29 — Thomas Creek (Burnaby, added stream blk -2000000382) should be closed under "BURNABY LAKE'S
TRIBUTARIES 2-8 — No Fishing" (printed p.22) but shows open.

## Cause
The city line's straight connector to its receiver (the Brunette, `356294414`) cut the corner of Burnaby Lake
(`lake:329291868`, 14 m) and ran a 34 m stub out to the Brunette's nearest point — its lake OUTLET. The atlas read
creek -> lake -> stub -> Brunette: a second lake outlet, which `lake_tributaries` / `_through_blks` skip (the Koocanusa
guard). Only the stub took brunette_river_s_tributaries.r1; the 1.29 km creek and its tribs -383/-384/-385/-387 got the
zone base only. Not a c1029 regression (same shape before).

## Fix (generic, `pipeline/atlas/waters/added_streams/build_dataset.py`)
`_lake_stub_confluence`: when an added stream's connector crosses a lake the receiver FWA line runs through and leaves it
by <= `_LAKE_STUB_M` (60 m) to reach the receiver outside the lake, the connector re-joins the receiver INSIDE that lake
(held `_LAKE_IN_M` = 25 m off the receiver's lake edge) — a lake inlet, as Lawson / Crayfish Creek already join.
Reported by the build ("lake stub: N connector(s) re-joined …"). Tests: `test_added_stream_lake_stub.py` (4).

## Outcome
- The generic rule changed exactly 2 streams: **Thomas Creek** (+ tribs, wsc 100-019698-999999 -> -997400) and **Angelo
  Creek** -2000000096 (into Deer Lake, wsc …-438800 -> …-432100; same shape: 27 m stub). Not changed: Kaymar Trib.2
  -2000000247 (1.96 km before its lake: a real stream, not a stub), Meighan Creek -2100000096 (183 m stub > 60 m — a
  long connector; left for the owner if it is wrong).
- Item ids follow the wsc (wsc:100-019698-997400 etc.); no curated file names the old ids (continuations.json's Beecher
  row was already stale).
- Binding diff (side atlas + reach, before promote): 95 rules change. 92 zone rules: the 2 lake pieces renumbered into the
  lake and Noons Creek's piece below its new tidal boundary leaves (`360880849:0`). The other 3 are the expected ones:
  `burnaby_lake_s_tributaries.r1` and `brunette_river.r1` gain Thomas + its 4 tribs (Angelo renumbered),
  `brunette_river_s_tributaries.r1` gains Thomas + tribs (the stub section goes). Nothing else.

## Same rebuild (2026-10-29)
- Noons Creek tidal boundary = the Ioco Spur rail bridge (owner), `splits.json` flag.
- T224 owner decisions (coverage names accepted; gauge names credited to the gauge; 2 diversion names held — their items
  leave the registry: blk:354088905, blk:356368998).
- Cranberry canyon bottom: the cut point was already on the mainstem (0.4 m); the bundle's split row and part mark took
  the braid sweep's row (no confidence, on the side channel). `spans.split_rows` now takes the host row, and
  `topology` places a part mark where the main stems meet: 25 splits had the braid row first (2 regain `low`:
  Cranberry canyon bottom, Chuckwalla Ten Mile Pool). No splits.json point lies on a side channel beside its own
  mainstem (the 4 host rows on side channels are area boundaries / a confluence that cross every channel).
