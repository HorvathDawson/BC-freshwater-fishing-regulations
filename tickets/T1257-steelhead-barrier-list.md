# T1257 — Steelhead barrier list: streams above a proven barrier go back to the rainbow rules

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1257 | Steelhead barrier list: streams above a proven barrier go back to the rainbow rules | OPEN | — | a curated, sourced list of dams and falls with no fish passage; the owner's yes per barrier; 0.11.0 |

Source: the owner, 2026-11-27, asked whether every stream should read a rainbow over 50 cm as a steelhead, verbatim:
"should we just say all streams have rainbows over 50 as steelhead? we dont want to give wrong info about being able to keep a "steelhead" they are tightly regulated."
His pick: "Pacific streams now, barriers in 0.11.0 (Recommended)".

## Why
In 0.10.0 (T1250, branch fix/T1250-pacific-streams-steelhead) every stream of Regions 1, 2, 3, 5, 6 that drains to the
Pacific counts as steelhead water. Cost of not having barriers, measured: about 270,000 km above dams, falls and lakes
where a 55 cm fish is a resident rainbow the book lets an angler keep: Thompson above Kamloops Lake 46,004 km, Nechako
above Kenney Dam 10,297, Bridge above Carpenter Lake 6,737, above Quesnel Lake 5,616, above Seton Dam 4,853, unnamed
lakes and ponds on the way 67,762.

## Known / not known
- The book's test (p.80): "a rainbow trout longer than 50 cm in waters where anadromous rainbow trout are found".
- Lakes do not stop steelhead everywhere (Babine, Morice, Lakelse are passed), so a lake is not a barrier by itself.
- Not known: the full barrier list beyond Kenney Dam (Nechako), Terzaghi and La Joie (Bridge); each needs a source.
- Built in T1250 (owner: "Hold both but make sure it will be compatible with a larger barrier list"): the curated
  file `data/curated/waters/steelhead_barriers.json` with its two kinds of row, `basin` and `upstream_of` (a blue
  line and a measure); this ticket adds rows. Its first rows: the Mackenzie (200), the Yukon (800), the Columbia (300)
  and the Skagit above Ross Dam (970-106243).
- Ross Dam (Washington) has no fish passage: every B.C. section of the Skagit lies above it. Basin 970's other branch
  (970-030628: Bertrand, Fishtrap, Pepin creeks, to the Nooksack) is not held. Source still to attach.
- `upstream_of` walks the graph up from the pieces wholly above its measure: a tributary joining exactly at the
  measure goes with whichever piece the graph attaches it to. Proven only on the mini world (review 3).
- The Columbia (300): its sea-run fish are stopped downstream in Washington (Chief Joseph and Grand Coulee on the
  Columbia; Enloe Dam on the Similkameen, below the B.C. reach). Sources still to attach.

## Work (proposal)
A curated barrier list (dam / falls, location, source, passage status); streams above a barrier return to the rainbow
rules; each barrier carries its source and the owner's yes.

Target: 0.11.0. Depends: T1250. Size: M. Owner: yes (each barrier).

## Owner

- [ ] 2026-11-27 — Steelhead barriers: say yes or no to each proposed barrier once its source is attached.
