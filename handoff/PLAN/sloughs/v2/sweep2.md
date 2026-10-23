# T55 second sweep: sloughs, canals and the lake feature codes

2424 candidates: the 865 from the first sweep, revised, plus 1559 new ones the census found. Owner marks read: 98 ({'stream': 89, 'note-only': 1, 'not_stream': 5, 'unsure': 3}). Files: candidates_v2.json.gz (same record shape as the first sweep plus the new fields), revise.py.

Classes: STREAM_BY_BOOK 96, LIKELY_STREAM 827, UNSURE 520, LAKE 706, NOT_FISHING_WATER 275.

## The rules, and what the owner marks say about each

| id | rule | fires on (all candidates) | marked: agrees | marked: disagrees | support |
|---|---|---|---|---|---|
| R0 | Sewage settling basin, tailing pond or dump (FWA EA26700110 / AP90300100 / AP09200000) is NOT_FISHING_WATER. | 275 | 0 | 0 | No mark touches one (none was in the first sweep). Census: all 275 are drawn as lakes. |
| R1 | A piece whose inside line is one of a named lake's blue lines (the lake within 150 m), with no named river main flow of its own through it, no main-flow (1200) line at all and perimeter elongation under 4, is that LAKE (a bay or arm). | 3 | 1 | 0 | Owner notes: Sproat River piece "could actually be labelled as sproat lake too" (not_stream): its 1300 line is blue line 354087683, a 1450 line inside Sproat Lake 59 m away; Cruickshank "this can be on moat lake" (note only): its 1300 line is blue line 354087804, inside Moat Lake 24 m away. Touching alone is not used: 8 stream marks lie within 150 m of a named lake (Duffey, Nicola, Robert, McCauley, Hatzic...), all on a named river or a named slough. |
| R2 | A widening behind a dam or a reservoir on a river is a LAKE, unless the river's book entry names the reservoir as part of the river. | 44 | 0 | 0 | Owner notes: Quinsam "it is a dammed area", South Nanaimo "dammed area probably a lake?", Bonaparte "probably a lake" (all three unsure). There is no dam dataset in the repo: evidence is a reservoir FWA code, a touching manmade reservoir polygon, a touching polygon named "Reservoir". The owner notes are not used as evidence (a rule fed by the mark it is scored against would launder it), so none of the three fires. |
| R3 | Lake Arm (1475) lines inside, with no secondary flow and no named river through it, is a LAKE. | 0 | 0 | 0 | No mark has a 1475 line; the rule rests on what the code means. |
| R4 | Lake-Indefinite, Lake-Intermittent or Flooded Land on a river's line leans LIKELY_STREAM (a slough or side channel) when channel-shaped (perimeter elongation 3 or more) or slough/channel-named; round and large (over 5 ha, elongation under 2.5) is LAKE; anything else keeps its shape class (a round pond stays LAKE). | 148 | 0 | 0 | Marks: Raft River and Similkameen pieces (Intermittent), two Crooked River pieces (Indefinite) and Hatzis Slough (Flooded Land) all marked stream; perimeter elongation 5.6 to 54 except Hatzis (1.1, a named slough). R4 fired on none of them (they were already stream classes), so the marks constrain its shape floor, not its firing. |
| R5 | An UNSURE with a Secondary Flow (1300) line inside and elongation 3 or more leans LIKELY_STREAM. | 17 | 0 | 0 | Marks: 41 of the 89 stream marks have a 1300 line; the one not_stream with 1300 (Sproat) is caught first by R1. |
| R6 | An UNSURE with a named river main flow (1200) through a polygon 40 m wide or less, elongation 4 or more, leans LIKELY_STREAM. | 24 | 0 | 0 | Marks: river-through stream marks have median width under 30 m; see the width note below. |
| R7 | A canal polygon or named ditch/canal line that is not a stream item is a new candidate: STREAM_BY_BOOK when a book entry names it, else LIKELY_STREAM. | 2 | 0 | 0 | Marks: Serpentine canal polygon and Seton Canal both marked stream. |

## Agreement with the owner marks

97 marks with a verdict (the note-only mark is left out). Rows: owner verdict; columns: the class the rules give before the mark is applied.

**v2 rules**

| owner \ class | STREAM_BY_BOOK | LIKELY_STREAM | UNSURE | LAKE | NOT_FISHING_WATER |
|---|---|---|---|---|---|
| stream | 62 | 27 | 0 | 0 | 0 |
| not_stream | 3 | 1 | 0 | 1 | 0 |
| unsure | 0 | 3 | 0 | 0 | 0 |

Agreement (v2 rules): 90 of 97 (stream = STREAM_BY_BOOK or LIKELY_STREAM; not_stream = LAKE or NOT_FISHING_WATER; unsure = UNSURE).

**first sweep**

| owner \ class | STREAM_BY_BOOK | LIKELY_STREAM | UNSURE | LAKE | NOT_FISHING_WATER |
|---|---|---|---|---|---|
| stream | 62 | 27 | 0 | 0 | 0 |
| not_stream | 3 | 2 | 0 | 0 | 0 |
| unsure | 0 | 3 | 0 | 0 | 0 |

Agreement (first sweep): 89 of 97 (stream = STREAM_BY_BOOK or LIKELY_STREAM; not_stream = LAKE or NOT_FISHING_WATER; unsure = UNSURE).

### Rule exceptions (a rule disagrees with a mark; the class follows the owner)

| wbk | stream | owner | note | v1 | rules say | rule | why |
|---|---|---|---|---|---|---|---|
| 329292598 | Coquitlam River | not_stream |  | STREAM_BY_BOOK | STREAM_BY_BOOK | (no v2 rule fired; first-sweep class) | river-shaped (elongation 5.3, 21.1 m wide) unnamed piece on Coquitlam River's line; the river's own book entry (r2:coquitlam_river@2-8) binds the line on both s |
| 329037255 | Big Bar Creek | not_stream |  | STREAM_BY_BOOK | STREAM_BY_BOOK | (no v2 rule fired; first-sweep class) | river-shaped (elongation 6.0, 50.7 m wide) unnamed piece on Big Bar Creek's line; the river's own book entry (r3:big_bar_creek@3-31) binds the line on both side |
| 329247848 | Kootenay River | not_stream |  | STREAM_BY_BOOK | STREAM_BY_BOOK | (no v2 rule fired; first-sweep class) | river-shaped (elongation 7.0, 69.8 m wide) unnamed piece on Kootenay River's line; the river's own book entry (r4:kootenay_river_downstream_of_idaho_border@4-7+ |
| 329070300 | Quinsam River | unsure | im like a tiny bit unsure but stream is stricter regs. iut is a dammed area | LIKELY_STREAM | LIKELY_STREAM | (no v2 rule fired; first-sweep class) | elongation 4.1, 27.2 m wide on Quinsam River; its own entry binds both sides and stops here |
| 328966554 | South Nanaimo River | unsure | dammed area probnably a lake? | LIKELY_STREAM | LIKELY_STREAM | (no v2 rule fired; first-sweep class) | river polygon / side-channel signal on South Nanaimo River, elongation 3.4 |
| 329070659 | Elk River | not_stream |  | LIKELY_STREAM | LIKELY_STREAM | (no v2 rule fired; first-sweep class) | elongation 3.1, 21.8 m wide on Elk River; its own entry binds both sides and stops here |
| 329055141 | Bonaparte River | unsure | probably a lake | LIKELY_STREAM | LIKELY_STREAM | (no v2 rule fired; first-sweep class) | elongation 3.8, 45.4 m wide on Bonaparte River; its own entry binds both sides and stops here |

### Shape does not separate river-through pieces

| wbk | stream | owner | width m | elong | through_ratio |
|---|---|---|---|---|---|
| 328966554 | South Nanaimo River | unsure | 112.9 | 3.38 | 0.81 |
| 328971135 | Heakamie River | stream | 81.2 | 3.92 | 0.67 |
| 328963527 | Big Bar Creek | stream | 79.0 | 4.8 | 0.8 |
| 329601781 | Dean River | stream | 75.5 | 5.27 | 0.61 |
| 329505324 | Cayoosh Creek | stream | 68.8 | 5.46 | 0.9 |
| 329679959 | Lachmach River | stream | 60.1 | 6.64 | 0.87 |
| 329102746 | Crooked River | stream | 54.1 | 10.02 | 0.97 |
| 329037255 | Big Bar Creek | not_stream | 50.7 | 5.96 | 0.94 |
| 329055148 | Fiftynine Creek | stream | 49.5 | 3.87 | 0.81 |
| 329055647 | Fiftyseven Creek | stream | 47.3 | 3.11 | 0.12 |
| 329055141 | Bonaparte River | unsure | 45.4 | 3.78 | 0.05 |
| 329602028 | Dean River | stream | 43.3 | 5.39 | 0.72 |
| 329319148 | Nechako River | stream | 41.1 | 6.52 | 0.97 |
| 329316355 | Nicola River | stream | 39.3 | 4.15 | 0.93 |

Seven stream marks are wider than 45 m (Heakamie 81, Big Bar 79, Dean 75, Cayoosh 69, Lachmach 60, Fiftynine 50, Fiftyseven 47) while the not_stream Big Bar piece is 51 m and unsure Quinsam 27 m. A width or through-ratio rule that made the dammed ponds LAKE would turn those stream marks too, so no such rule is used; R2 needs dam evidence instead.

## What changed from the first sweep

58 of 865 first-sweep classes changed.

| v1 | v2 | n |
|---|---|---|
| UNSURE | LIKELY_STREAM | 44 |
| LIKELY_STREAM | UNSURE | 3 |
| LIKELY_STREAM | LAKE | 3 |
| STREAM_BY_BOOK | LAKE | 3 |
| LAKE | LIKELY_STREAM | 2 |
| LAKE | NOT_FISHING_WATER | 2 |
| UNSURE | LAKE | 1 |

| rule that changed it | n |
|---|---|
| R6 | 24 |
| R5 | 17 |
| owner mark | 7 |
| R4 | 5 |
| R1 | 3 |
| R0 | 2 |

Examples:

| wbk | stream | code | change | why |
|---|---|---|---|---|
| 329575816 | Eve River | Lake-Definite | UNSURE > LIKELY_STREAM | FWA Secondary Flow (1300) line inside (73 m), elongation 5.9: a side channel |
| 329506048 | Bridge River | Lake-Definite | UNSURE > LIKELY_STREAM | Bridge River main flow runs through a narrow polygon (33 m, elongation 5.5): the river |
| 329056124 | Fiftynine Creek | Lake-Definite | UNSURE > LIKELY_STREAM | Fiftynine Creek main flow runs through a narrow polygon (35 m, elongation 5.3): the river |
| 329056586 | Fiftynine Creek | Lake-Definite | UNSURE > LIKELY_STREAM | Fiftynine Creek main flow runs through a narrow polygon (30 m, elongation 4.9): the river |
| 329158582 | Gataga River | Lake-Intermittent | LAKE > LIKELY_STREAM | Lake-Intermittent on Gataga River's line, channel-shaped (perimeter elongation 21.0): a slough or side channel the river fills |
| 329298057 | Cameron River | Lake-Indefinite | LAKE > LIKELY_STREAM | Lake-Indefinite on Cameron River's line, channel-shaped (perimeter elongation 19.0): a slough or side channel the river fills |
| 329070300 | Quinsam River | Lake-Definite | LIKELY_STREAM > UNSURE | owner marked unsure ("im like a tiny bit unsure but stream is stricter regs. iut is a dammed area"); rules alone say LIKELY_STREAM (elongation 4.1, 27.2 m wide on Quinsam |
| 328966554 | South Nanaimo River | Lake-Definite | LIKELY_STREAM > UNSURE | owner marked unsure ("dammed area probnably a lake?"); rules alone say LIKELY_STREAM (river polygon / side-channel signal on South Nanaimo River, elongation 3.4) |
| 329055141 | Bonaparte River | Lake-Definite | LIKELY_STREAM > UNSURE | owner marked unsure ("probably a lake"); rules alone say LIKELY_STREAM (elongation 3.8, 45.4 m wide on Bonaparte River; its own entry binds both sides and stops here) |
| 329070659 | Elk River | Lake-Definite | LIKELY_STREAM > LAKE | owner marked not_stream; rules alone say LIKELY_STREAM (elongation 3.1, 21.8 m wide on Elk River; its own entry binds both sides and stops here) |
| 329016376 | Sproat River | Lake-Definite | LIKELY_STREAM > LAKE | owner marked not_stream ("this could actually be labelled as sproat lake too"); its side line is a blue line of Sproat Lake (wbk 329016196, 59 m away) and no named river  |
| 329097416 | Cruickshank River | Lake-Definite | LIKELY_STREAM > LAKE | its side line is a blue line of Moat Lake (wbk 329095509, 24 m away) and no named river runs through it: a bay or arm of that lake, not the river |
| 329292598 | Coquitlam River | Lake-Definite | STREAM_BY_BOOK > LAKE | owner marked not_stream; rules alone say STREAM_BY_BOOK (river-shaped (elongation 5.3, 21.1 m wide) unnamed piece on Coquitlam River's line; the river's own book entry (r |
| 329037255 | Big Bar Creek | Lake-Definite | STREAM_BY_BOOK > LAKE | owner marked not_stream; rules alone say STREAM_BY_BOOK (river-shaped (elongation 6.0, 50.7 m wide) unnamed piece on Big Bar Creek's line; the river's own book entry (r3: |
| 329247848 | Kootenay River | Lake-Definite | STREAM_BY_BOOK > LAKE | owner marked not_stream; rules alone say STREAM_BY_BOOK (river-shaped (elongation 7.0, 69.8 m wide) unnamed piece on Kootenay River's line; the river's own book entry (r4 |
| 329664272 | Shuswap River | Lake-Definite | UNSURE > LAKE | its side line is a blue line of Mabel Lake (wbk 329663955, 36 m away) and no named river runs through it: a bay or arm of that lake, not the river |
| 329083365 |  | Dump | LAKE > NOT_FISHING_WATER | FWA code AP09200000 (Dump): never a fishing water |
| 329083374 |  | Dump | LAKE > NOT_FISHING_WATER | FWA code AP09200000 (Dump): never a fishing water |

## New candidates

| found by | STREAM_BY_BOOK | LIKELY_STREAM | UNSURE | LAKE | NOT_FISHING_WATER | n |
|---|---|---|---|---|---|---|
| v2:A-soft-lake-on-river | 1 | 506 | 185 | 379 | 0 | 1071 |
| v2:B-not-fishing | 0 | 0 | 0 | 0 | 273 | 273 |
| v2:C-named-ditch-line | 2 | 0 | 0 | 0 | 0 | 2 |
| v2:D-reservoir-on-river | 0 | 0 | 0 | 39 | 0 | 39 |
| v2:F-flooded-on-river | 0 | 6 | 0 | 0 | 0 | 6 |
| v2:gap-on-regulated-river | 31 | 39 | 46 | 52 | 0 | 168 |

| class \ region | 1 | 2 | 3 | 4 | 5 | 6 | 7a | 7b | 8 |
|---|---|---|---|---|---|---|---|---|---|
| STREAM_BY_BOOK | 2 | 3 | 11 | 2 | 4 | 9 | 0 | 2 | 1 |
| LIKELY_STREAM | 6 | 5 | 31 | 7 | 48 | 83 | 46 | 311 | 14 |
| UNSURE | 6 | 6 | 19 | 5 | 24 | 44 | 17 | 102 | 8 |
| LAKE | 9 | 15 | 49 | 17 | 58 | 61 | 37 | 196 | 28 |
| NOT_FISHING_WATER | 43 | 23 | 33 | 15 | 6 | 26 | 50 | 55 | 22 |

Strongest new ones (STREAM_BY_BOOK, then LIKELY_STREAM on a regulated river):

| id | found by | stream | code | reg | mu | elong | width | lat,lon | why |
|---|---|---|---|---|---|---|---|---|---|
| 328988135 | v2:gap-on-regulated-river | Mohun Creek | Lake-Definite | 1 | 1-10 | 27.15 | 33.8 | 50.11799,-125.4409 | river-shaped (elongation 27.1, 34 m wide) unnamed piece on Mohun Creek, whose book entry binds the river but not this po |
| 329501162 | v2:gap-on-regulated-river | Mohun Creek | Lake-Definite | 1 | 1-10 | 14.92 | 16.1 | 50.12061,-125.46546 | river-shaped (elongation 14.9, 16 m wide) unnamed piece on Mohun Creek, whose book entry binds the river but not this po |
| 329292514 | v2:gap-on-regulated-river | Campbell River | Lake-Definite | 2 | 2-4 | 6.18 | 24.6 | 49.02239,-122.57036 | river-shaped (elongation 6.2, 25 m wide) unnamed piece on Campbell River, whose book entry binds the river but not this  |
| blk:356013125 | v2:C-named-ditch-line | Nelson Creek | Ditch | 2 | 2-8 |  |  | 49.2289,-122.87044 | named Ditch "Nelson Creek" is no stream item and has no sections; the book names it (r2:nelson_creek@2-8) |
| blk:356328804 | v2:C-named-ditch-line | Still Creek | Canal / Ditch | 2 | 2-8 |  |  | 49.25966,-123.00693 | named Canal/Ditch "Still Creek" is no stream item and has no sections; the book names it (r2:still_creek@2-8) |
| 329037270 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 21.93 | 40.7 | 51.32888,-121.96918 | river-shaped (elongation 21.9, 41 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this  |
| 329037373 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 15.65 | 38.0 | 51.28192,-121.98368 | river-shaped (elongation 15.7, 38 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this  |
| 329651323 | v2:gap-on-regulated-river | Albreda River | Lake-Definite | 3 | 3-43 | 12.05 | 23.0 | 52.63957,-119.16666 | river-shaped (elongation 12.1, 23 m wide) unnamed piece on Albreda River, whose book entry binds the river but not this  |
| 328963539 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 11.56 | 64.0 | 51.34264,-121.86197 | river-shaped (elongation 11.6, 64 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this  |
| 328963529 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 11.39 | 72.9 | 51.32327,-121.82066 | river-shaped (elongation 11.4, 73 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this  |
| 329037228 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 10.43 | 66.8 | 51.32274,-121.97523 | river-shaped (elongation 10.4, 67 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this  |
| 329037273 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 9.35 | 58.8 | 51.33209,-121.83481 | river-shaped (elongation 9.3, 59 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this p |
| 329037333 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 9.22 | 51.3 | 51.33445,-121.96155 | river-shaped (elongation 9.2, 51 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this p |
| 329037368 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 8.5 | 49.3 | 51.32819,-121.82827 | river-shaped (elongation 8.5, 49 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this p |
| 329037551 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 7.09 | 39.8 | 51.32046,-121.81864 | river-shaped (elongation 7.1, 40 m wide) unnamed piece on Big Bar Creek, whose book entry binds the river but not this p |
| 329505890 | v2:gap-on-regulated-river | Cayoosh Creek | Lake-Definite | 3 | 3-16 | 5.4 | 38.3 | 50.65691,-121.9843 | river-shaped (elongation 5.4, 38 m wide) unnamed piece on Cayoosh Creek, whose book entry binds the river but not this p |
| 329124111 | v2:gap-on-regulated-river | Cadorna Creek | Lake-Definite | 4 | 4-23 | 11.44 | 16.5 | 50.46479,-115.041 | river-shaped (elongation 11.4, 16 m wide) unnamed piece on Cadorna Creek, whose book entry binds the river but not this  |
| 329248176 | v2:gap-on-regulated-river | White River | Lake-Definite | 4 | 4-24 | 7.58 | 25.3 | 50.45169,-115.1877 | river-shaped (elongation 7.6, 25 m wide) unnamed piece on White River, whose book entry binds the river but not this pol |
| 329059951 | v2:gap-on-regulated-river | Bowron River | Lake-Definite | 5 | 5-16 | 17.2 | 16.0 | 53.1942,-121.27189 | river-shaped (elongation 17.2, 16 m wide) unnamed piece on Bowron River, whose book entry binds the river but not this p |
| 329171688 | v2:gap-on-regulated-river | Watch Creek | Lake-Definite | 5 | 5-1 | 10.62 | 42.6 | 51.51177,-121.07238 | river-shaped (elongation 10.6, 43 m wide) unnamed piece on Watch Creek, whose book entry binds the river but not this po |
| 329371937 | v2:A-soft-lake-on-river | Chimney Creek | Lake-Indefinite | 5 | 5-2 | 5.9 | 31.9 | 52.04073,-122.05635 | river-shaped (elongation 5.9, 32 m wide) unnamed piece on Chimney Creek, whose book entry binds the river but not this p |
| 329173031 | v2:gap-on-regulated-river | Watch Creek | Lake-Definite | 5 | 5-1 | 5.04 | 32.0 | 51.51152,-121.06699 | river-shaped (elongation 5.0, 32 m wide) unnamed piece on Watch Creek, whose book entry binds the river but not this pol |
| 329313065 | v2:gap-on-regulated-river | Ksi Sii Aks | Lake-Definite | 6 | 6-15 | 33.48 | 76.1 | 55.17213,-129.02561 | river-shaped (elongation 33.5, 76 m wide) unnamed piece on Ksi Sii Aks, whose book entry binds the river but not this po |
| 329243155 | v2:gap-on-regulated-river | Deep Creek | Lake-Definite | 6 | 6-15 | 15.29 | 12.2 | 54.5741,-128.5469 | river-shaped (elongation 15.3, 12 m wide) unnamed piece on Deep Creek, whose book entry binds the river but not this pol |
| 329152571 | v2:gap-on-regulated-river | Endako River | Lake-Definite | 6 | 6-4 | 14.48 | 14.6 | 54.3369,-125.9088 | river-shaped (elongation 14.5, 15 m wide) unnamed piece on Endako River, whose book entry binds the river but not this p |
| 328974096 | v2:gap-on-regulated-river | Deep Creek | Lake-Definite | 6 | 6-15 | 13.74 | 37.2 | 54.58135,-128.5548 | river-shaped (elongation 13.7, 37 m wide) unnamed piece on Deep Creek, whose book entry binds the river but not this pol |
| 329680970 | v2:gap-on-regulated-river | Lachmach River | Lake-Definite | 6 | 6-14 | 9.34 | 16.7 | 54.27524,-129.90471 | river-shaped (elongation 9.3, 17 m wide) unnamed piece on Lachmach River, whose book entry binds the river but not this  |
| 329154141 | v2:gap-on-regulated-river | Endako River | Lake-Definite | 6 | 6-4 | 7.1 | 11.3 | 54.35531,-125.93532 | river-shaped (elongation 7.1, 11 m wide) unnamed piece on Endako River, whose book entry binds the river but not this po |
| 329644091 | v2:gap-on-regulated-river | Damdochax Creek | Lake-Definite | 6 | 6-17 | 7.06 | 19.1 | 56.46768,-128.00606 | river-shaped (elongation 7.1, 19 m wide) unnamed piece on Damdochax Creek, whose book entry binds the river but not this |
| 329066410 | v2:gap-on-regulated-river | Station Creek | Lake-Definite | 6 | 6-9 | 5.61 | 35.5 | 55.23468,-127.6212 | river-shaped (elongation 5.6, 36 m wide) unnamed piece on Station Creek, whose book entry binds the river but not this p |
| 329411114 | v2:gap-on-regulated-river | Nakina River | Lake-Definite | 6 | 6-26 | 5.24 | 32.1 | 59.10503,-132.4254 | river-shaped (elongation 5.2, 32 m wide) unnamed piece on Nakina River, whose book entry binds the river but not this po |
| 329526939 | v2:gap-on-regulated-river | Dimsdale Creek | Lake-Definite | 7b | 7-19 | 10.85 | 30.9 | 54.16424,-120.45031 | river-shaped (elongation 10.8, 31 m wide) unnamed piece on Dimsdale Creek, whose book entry binds the river but not this |
| 329526166 | v2:gap-on-regulated-river | Dimsdale Creek | Lake-Definite | 7b | 7-19 | 8.34 | 72.4 | 54.16156,-120.45665 | river-shaped (elongation 8.3, 72 m wide) unnamed piece on Dimsdale Creek, whose book entry binds the river but not this  |
| 329219298 | v2:gap-on-regulated-river | McRae Creek | Lake-Definite | 8 | 8-15 | 8.95 | 14.3 | 49.24349,-118.13355 | river-shaped (elongation 8.9, 14 m wide) unnamed piece on McRae Creek, whose book entry binds the river but not this pol |
| 329095695 | v2:gap-on-regulated-river | Puntledge River | Lake-Definite | 1 | 1-6 | 10.33 | 53.5 | 49.49722,-125.30461 | elongation 10.3, mean width 54 m on Puntledge River: river-shaped |
| 329504353 | v2:gap-on-regulated-river | Caycuse River | Lake-Definite | 1 | 1-3 | 4.62 | 65.8 | 48.75465,-124.4954 | elongation 4.6, mean width 66 m on Caycuse River: river-shaped |
| 329576219 | v2:gap-on-regulated-river | Adam River | Lake-Definite | 1 | 1-10 | 4.13 | 12.7 | 50.15028,-126.169 | elongation 4.1, mean width 13 m on Adam River: river-shaped |
| 329532539 | v2:A-soft-lake-on-river | Squamish River | Lake-Indefinite | 2 | 2-6 | 5.25 | 104.8 | 50.28418,-123.41176 | elongation 5.2, mean width 105 m on Squamish River: river-shaped |
| 329532489 | v2:gap-on-regulated-river | Ashlu Creek | Lake-Definite | 2 | 2-6 | 4.73 | 148.8 | 50.09221,-123.62363 | elongation 4.7, mean width 149 m on Ashlu Creek: river-shaped |
| 329037419 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 15.67 | 35.3 | 51.29342,-121.75771 | elongation 15.7, mean width 35 m on Big Bar Creek: river-shaped |
| 329037289 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 12.95 | 49.3 | 51.27588,-121.73561 | elongation 12.9, mean width 49 m on Big Bar Creek: river-shaped |
| 329107090 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 12.22 | 35.4 | 51.11027,-120.83056 | elongation 12.2, mean width 35 m on Deadman River: river-shaped |
| 329106554 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 7.62 | 152.8 | 51.13745,-120.5707 | elongation 7.6, mean width 153 m on Deadman River: river-shaped |
| 329107046 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 6.46 | 47.4 | 51.11166,-120.80447 | elongation 6.5, mean width 47 m on Deadman River: river-shaped |
| 329107718 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 5.79 | 30.9 | 51.1116,-120.83056 | elongation 5.8, mean width 31 m on Deadman River: river-shaped |
| 329037566 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 5.55 | 42.6 | 51.27244,-121.73536 | elongation 5.5, mean width 43 m on Big Bar Creek: river-shaped |
| 329107250 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 5.19 | 42.4 | 51.11087,-120.82298 | elongation 5.2, mean width 42 m on Deadman River: river-shaped |
| 329107678 | v2:gap-on-regulated-river | Deadman River | Lake-Definite | 3 | 3-29 | 4.85 | 33.7 | 51.11482,-120.8821 | elongation 4.8, mean width 34 m on Deadman River: river-shaped |
| 329054910 | v2:gap-on-regulated-river | Bonaparte River | Lake-Definite | 3 | 3-30 | 4.69 | 103.7 | 51.172,-120.40722 | elongation 4.7, mean width 104 m on Bonaparte River: river-shaped |
| 329038699 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 4.5 | 12.9 | 51.29474,-121.76176 | elongation 4.5, mean width 13 m on Big Bar Creek: river-shaped |
| 329038227 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 4.11 | 23.3 | 51.27398,-121.7366 | elongation 4.1, mean width 23 m on Big Bar Creek: river-shaped |
| 329037658 | v2:gap-on-regulated-river | Big Bar Creek | Lake-Definite | 3 | 3-31 | 4.01 | 42.4 | 51.34406,-121.86737 | elongation 4.0, mean width 42 m on Big Bar Creek: river-shaped |
| 329525527 | v2:gap-on-regulated-river | Sandown Creek | Lake-Definite | 4 | 4-20 | 4.52 | 15.8 | 50.04055,-115.9075 | elongation 4.5, mean width 16 m on Sandown Creek: river-shaped |
| 329524675 | v2:gap-on-regulated-river | Sandown Creek | Lake-Definite | 4 | 4-20 | 4.14 | 42.5 | 50.04208,-115.91466 | elongation 4.1, mean width 42 m on Sandown Creek: river-shaped |
| 329371130 | v2:gap-on-regulated-river | Chimney Creek | Lake-Definite | 5 | 5-2 | 6.21 | 75.8 | 51.87824,-121.84261 | elongation 6.2, mean width 76 m on Chimney Creek: river-shaped |
| 329417409 | v2:gap-on-regulated-river | Baker Creek | Lake-Definite | 5 | 5-13 | 4.55 | 12.9 | 52.87772,-123.03125 | elongation 4.5, mean width 13 m on Baker Creek: river-shaped |
| 329483672 | v2:gap-on-regulated-river | Beaver Creek | Lake-Definite | 5 | 5-2 | 4.03 | 19.9 | 52.65664,-122.08601 | elongation 4.0, mean width 20 m on Beaver Creek: river-shaped |
| 328984081 | v2:gap-on-regulated-river | Kwinageese River | Lake-Definite | 6 | 6-30 | 18.58 | 154.2 | 56.00252,-128.75697 | elongation 18.6, mean width 154 m on Kwinageese River: river-shaped |
| 329571952 | v2:gap-on-regulated-river | Kildala River | Lake-Definite | 6 | 6-3 | 8.37 | 188.4 | 53.87982,-128.14385 | elongation 8.4, mean width 188 m on Kildala River: river-shaped |
| 329644257 | v2:gap-on-regulated-river | Damdochax Creek | Lake-Definite | 6 | 6-17 | 4.97 | 19.0 | 56.47574,-128.00931 | elongation 5.0, mean width 19 m on Damdochax Creek: river-shaped |

Weight of the A pool: 1071 unnamed Lake-Indefinite/Intermittent polygons with a named stream's main flow through; 7 sit on a blue line a named book entry regulates somewhere, 7 where that entry binds the river at the polygon, 602 are in Region 7B. Folding the rest changes only which zone rules and colour the polygon gets, so review the regulated ones first.

R2 evidence other than the polygon's own reservoir code (a touching manmade reservoir or a polygon named Reservoir): 0 ().

Not listed as candidates: 1376 unnamed ditch/canal blue lines that are no stream item. They are already line sections drawn and ruled as streams; with no name there is no stream item to fold them into.

## Not fishing water (sewage basins, tailing ponds, dumps)

275 polygons. Drawn: 275 (all as lakes in the lake tile layer). Bound by any water or area entry: 12. With a named stream's main flow through: 5.

| type | drawn from zoom | n |
|---|---|---|
| Dump | 11 | 2 |
| Mine-Tailing Pond | 8 | 3 |
| Mine-Tailing Pond | 9 | 4 |
| Mine-Tailing Pond | 10 | 5 |
| Mine-Tailing Pond | 11 | 14 |
| Mine-Tailing Pond | 12 | 18 |
| Mine-Tailing Pond | 13 | 11 |
| Mine-Tailing Pond | 14 | 4 |
| Settling Basin-Sewage | 9 | 1 |
| Settling Basin-Sewage | 10 | 13 |
| Settling Basin-Sewage | 11 | 41 |
| Settling Basin-Sewage | 12 | 64 |
| Settling Basin-Sewage | 13 | 61 |
| Settling Basin-Sewage | 14 | 34 |

Those the app binds a rule to or that sit on a named creek:

| wbk | type | ha | creek through | binds | zoom | lat,lon |
|---|---|---|---|---|---|---|
| 329563841 | Mine-Tailing Pond | 92.73 | Alkali Creek |  | 9 | 50.64978,-120.53214 |
| 329563839 | Mine-Tailing Pond | 159.39 | Pukaist Creek |  | 8 | 50.5564,-121.17244 |
| 329262752 | Settling Basin-Sewage | 2.87 | Little Sheep Creek |  | 11 | 49.10009,-117.8306 |
| 329398199 | Mine-Tailing Pond | 3.0 | Wilfred Creek |  | 11 | 55.12804,-121.51233 |
| 328977504 | Settling Basin-Sewage | 1.19 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.8332,-122.59297 |
| 329311812 | Settling Basin-Sewage | 1.23 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.83386,-122.59295 |
| 328977517 | Settling Basin-Sewage | 0.93 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.8088,-122.65858 |
| 328977523 | Settling Basin-Sewage | 0.69 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.80923,-122.66004 |
| 328977529 | Settling Basin-Sewage | 0.47 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.80876,-122.65699 |
| 329312106 | Settling Basin-Sewage | 0.73 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.80953,-122.66164 |
| 329375568 | Settling Basin-Sewage | 0.55 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.65716,-122.68789 |
| 329311850 | Settling Basin-Sewage | 1.16 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 12 | 58.79427,-122.68236 |
| 329375939 | Settling Basin-Sewage | 0.29 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 13 | 58.74311,-122.66946 |
| 329312761 | Settling Basin-Sewage | 0.14 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 13 | 58.83332,-122.59108 |
| 329312806 | Settling Basin-Sewage | 0.12 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 13 | 58.83375,-122.59105 |
| 329376416 | Settling Basin-Sewage | 0.09 |  | r7:liard_river_watershed_see_map_on_page_63@7-53 | 14 | 58.74367,-122.6692 |
| 329459124 | Mine-Tailing Pond | 231.03 | MacDonald Creek |  | 8 | 49.86274,-119.94629 |

Full list: every record with class NOT_FISHING_WATER in candidates_v2.json.gz.

## Naming suggestions

22 candidates carry a suggested name: the owner's note first, else the nearest FWA slough / canal / channel name on a line or polygon within 250 m (the polygon has no FWA name of its own). Fuller Slough is in no FWA or registry name, so it rests on the owner's note alone.

| wbk | stream (v1) | class | suggested | source | m | item | also near |
|---|---|---|---|---|---|---|---|
| 329292341 | Fraser River | STREAM_BY_BOOK | Williamson Slough | owner note |  |  |  |
| 329291987 | Ewen Slough | STREAM_BY_BOOK | Fuller Slough | owner note |  |  |  |
| 329178108 | Cheam Slough | STREAM_BY_BOOK | Cheam Slough | FWA stream line | 0.0 | gnis:13001 |  |
| 329292651 | Sturgeon Slough | STREAM_BY_BOOK | Sturgeon Slough | FWA stream line | 0.0 | gnis:16259, wsc:100-013706 |  |
| 329461544 | Mission Slough | STREAM_BY_BOOK | Mission Slough | FWA stream line | 0.0 | gnis:29941 |  |
| 329291936 | Catbird Slough | LIKELY_STREAM | Catbird Slough | FWA stream line | 0.0 | gnis:37665 |  |
| 329291851 | Sturgeon Slough | LIKELY_STREAM | Sturgeon Slough | FWA stream line | 0.0 | gnis:16259, wsc:100-013706 |  |
| 329292009 | Isle Slough | LIKELY_STREAM | Isle Slough | FWA stream line | 0.0 | gnis:10780 |  |
| 329221384 | Hogranch Channel | LIKELY_STREAM | Hogranch Channel | FWA stream line | 0.0 | gnis:21489 |  |
| 329220258 | Back Channel | LIKELY_STREAM | Back Channel | FWA stream line | 0.0 | gnis:17696 |  |
| 329220281 | Back Channel | LIKELY_STREAM | Back Channel | FWA stream line | 0.0 | gnis:17696 |  |
| 329221135 | Hogranch Channel | LIKELY_STREAM | Hogranch Channel | FWA stream line | 0.0 | gnis:21489 |  |
| 329328427 | McQueen Slough | LIKELY_STREAM | McQueen Slough | FWA stream line | 0.0 | gnis:8303 |  |
| 329328792 | McQueen Slough | LIKELY_STREAM | McQueen Slough | FWA stream line | 0.0 | gnis:8303 |  |
| 328948394 | McQueen Slough | LIKELY_STREAM | McQueen Slough | FWA stream line | 0.0 | gnis:8303 |  |
| 329225706 | Sloane Slough | LIKELY_STREAM | Sloane Slough | FWA stream line | 0.0 | gnis:27952 |  |
| 329292125 | Hatzis Slough | LIKELY_STREAM | Hatzic Slough | FWA stream line | 4.7 |  |  |
| 329221617 | Columbia River | STREAM_BY_BOOK | Hotsprings Channel | FWA stream line | 28.2 | gnis:8203 |  |
| 329220591 | Columbia River | LIKELY_STREAM | Hogranch Channel | FWA stream line | 80.6 | gnis:21489 |  |
| 329220753 | Columbia River | STREAM_BY_BOOK | Hotsprings Channel | FWA stream line | 182.5 | gnis:8203 |  |
| 329220828 | Hot Creek | UNSURE | Hotsprings Channel | FWA stream line | 219.7 | gnis:8203 |  |
| 329291813 | Quarry Slough | LAKE | Quarry Slough | FWA stream line | 0.0 | gnis:37695 |  |

## Part of a named lake (R1) and dam / reservoir evidence (R2)

| wbk | stream | named lake | shares blue line | v1 | v2 | owner |
|---|---|---|---|---|---|---|
| 329016376 | Sproat River | Sproat Lake | True | LIKELY_STREAM | LAKE | not_stream |
| 329097416 | Cruickshank River | Moat Lake | True | LIKELY_STREAM | LAKE |  |
| 329664272 | Shuswap River | Mabel Lake | True | UNSURE | LAKE |  |

| wbk | stream | evidence | book names as river | v1 | v2 | owner |
|---|---|---|---|---|---|---|
| 329504249 | Jordan River | own FWA code GB24300000 (Reservoir-Definite) |  | LAKE | LAKE |  |
| 329055368 | Clinton Creek | own FWA code GB24300000 (Reservoir-Definite) |  | LAKE | LAKE |  |
| 329056577 | Clinton Creek | own FWA code GB24300000 (Reservoir-Definite) |  | LAKE | LAKE |  |
| 329262742 | Acme Creek | own FWA code GB24300000 (Reservoir-Definite) |  | LAKE | LAKE |  |
| 329069292 | Bull River | own FWA code GB24300000 (Reservoir-Definite) |  | LAKE | LAKE |  |
| 329504694 | Charters River | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329198022 | Rainy River | own FWA code GB90100000 (Reservoir-Indefinite) |  |  | LAKE |  |
| 329533758 | Charles Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329533855 | Harvey Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329316106 | Kanevale Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 328988943 | East Canoe Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329037654 | Gillon Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329564879 | Guerin Creek | own FWA code GB90100110 (Reservoir-Intermittent) |  |  | LAKE |  |
| 329123655 | Harmer Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329262674 | Cambridge Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329248136 | Nixon Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329069744 | Gold Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329263652 | Bell Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329123476 | Greenhills Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329525633 | Jim Smith Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329586405 | South Hawks Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329481655 | Wiggins Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329587345 | South Hawks Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329060283 | Summit Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329589516 | South Hawks Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329481406 | Wiggins Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329589794 | South Hawks Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329101083 | Wingdam Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329242660 | Deep Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329066339 | Bessemer Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329065075 | Lu Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329020154 | Ruby Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329228560 | Norrie Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329660614 | Starfish Creek | own FWA code GB90100000 (Reservoir-Indefinite) |  |  | LAKE |  |
| 329459802 | Greenhow Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329664094 | Shuswap River | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329460714 | Trépanier Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329664562 | Duteau Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329460451 | MacDonald Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329665464 | Brash Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329460916 | Hydraulic Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329522626 | Lee Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329462083 | MacDonald Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |
| 329219998 | Moody Creek | own FWA code GB24300000 (Reservoir-Definite) |  |  | LAKE |  |

## Verification

An adversarial pass re-checked this sweep: leave-one-out against the marks, 50 pieces plotted against the FWA geometry (25 changed classes, 25 new candidates; verify/c*.png, verify/n*.png), the claimed book entries against the synopsis PDF, feature-code reads, owner-verdict compliance, and ids. Five defects were fixed in revise.py and the sweep re-run; every number above is the fixed run.

**Fixes**

- **V1, R1 over-reach.** R1 fired on a shared MAIN FLOW blue line, which only says the same creek also runs through the lake. Miller Creek 329070551 is a pond 140 m up an unnamed creek from Gooseneck Lake, not a bay of it. It also fired on river-shaped pieces: Obo River 329568770 has perimeter elongation 14 and is 71 m wide, a river reach between lakes. R1 now needs no 1200 line inside and perimeter elongation under 4. Sproat (3.05), Cruickshank (1.7) and Shuswap/Mabel (2.3, 1300 only) still fire. Miller goes back to UNSURE and Obo to LIKELY_STREAM, their first-sweep classes. Whatshan and Lemon were LAKE already.
- **V2, R4 promoted round ponds.** R4 sent every soft-coded piece that was not both round AND over 5 ha to LIKELY_STREAM. That moved 375 round ponds (perimeter elongation under 2) to stream, for example a 3.4 ha pond on Tunkwa Creek (elongation 1.0, 92 m wide), and Stave 329292335 and July Creek 329316476 (2.2). The R4 marks are all channel-shaped (5.6 to 54) except the named Hatzis Slough. R4 now promotes only perimeter elongation 3 or more, or a slough/channel name. The rest keep their shape class: under 2 LAKE, 2 to 4 UNSURE. Oxbows such as Gataga (21) and Cameron (19) still promote. Their record `elongation` (the first sweep's rotated-rectangle ratio, 2.0 and 1.75) under-reads a curved channel. The rules have always used the census perimeter elongation, now exported as `elongation_perimeter`.
- **V3, multi-code wetlands misread.** A wetland's FEATURE_CODE is the code of its largest piece. Flooded Land inside a marsh/swamp wetland was invisible to pool F and R4. 71 wetlands carry Flooded Land only in their codes; 3 have a named stream through them: Eightyseven Creek 328949530, Fredy Creek 328941785 and Maurice Creek 328939798. Pool F and R4 now read all codes. New field `fwa_feature_codes_all`; `fwa_feature_label` now lists every code, where it used to show only the first.
- **V4, STREAM_BY_BOOK on a reach the entry does not cover.** New-candidate STREAM_BY_BOOK used the census river_regulated, which is true when a named entry binds ANY section of the blue line. Several entries are reach-limited:
  - Big Bar Creek: "downstream of Big Bar Lake" (p.29).
  - Deadman River: "downstream of Mowich Lake" (p.29).
  - Chimney Creek: "downstream of Brunson Lake" (p.44).
  - Murray River: "Kinuseo Falls to signs about 2 km downstream" (p.65).
  - Attichika Creek: "500 m ... of the Thorn Creek confluence" (p.59).
  - Puntledge River.
  Because of that, 11 new STREAM_BY_BOOK sat where the river's own sections carry no such entry. verify/local_entries.py now reads the entries on the river's sections at each polygon (its own main-flow sections plus one up- and downstream). STREAM_BY_BOOK needs one of them. New field `river_entry_at_piece`. The gap-pool headline "Big Bar Creek about 12 pieces, Deadman 4" overstated the book: 7 of its STREAM_BY_BOOK pieces (3 Big Bar, 4 Deadman) lie outside the entry's reach.
- **V5, the rule text.** The R1 and R4 rule text and the A-pool caveat now say what the code does. The caveat counts the blue line regulated somewhere apart from the entry binding at the polygon.

**Leave-one-out (honest agreement).**

- Rule class against the marks: 90 of 97.
- Leave-one-out: 89 of 97.
- First sweep: 89 of 97.

A v2 rule fired on a marked record only 1 times ({'R1': 1}). Every other mark is scored on its first-sweep class.
- The one gain, Sproat, comes from R1. R1 was written from Sproat's own note, with Cruickshank's note-only mark as the second case. Drop Sproat and only Cruickshank (24 m) is left to set the distance. The 150 m reach that catches Sproat at 59 m is a choice, not a measurement.
- The honest out-of-sample figure is therefore 89 of 97, the first sweep's level. The v2 rules have not yet been tested on any mark they did not come from.
- R4, R5 and R6 change 0 marked records. Their "support" counts only show that marks are consistent with them, because those records were stream classes already.
- The 89 of 89 on stream marks is the first sweep's, and the sample is biased: the owner marked candidates that the first sweep showed.

**Invariants.**

- Owner-marked records off their verdict: 0.
- Duplicate ids: 0.
- New ids colliding with first-sweep ids: 0.
- First-sweep records all present.
- The lakes-layer reservoir code GB24300000 is on 2 polygons, Williston and Dinosaur Lake. Both are lake items, so neither is a candidate, and R2 reads the reservoir code in any layer.

**Spot checks.** 50 pieces were plotted.
- Sound: the R5 and R6 promotions (side channels and narrow reaches), the R0 dumps and tailing ponds (Pukaist and Alkali Creek run through tailing ponds), the R2 reservoirs (tiny impoundments on creeks), Kitseguecla Flooded Land, and the Gataga and Cameron oxbows.
- Wrong, now fixed: Miller and Obo under R1, and Stave, July Creek, Okanagan 329460279 and A-pool round ponds such as Indian Meadows, Black Angus, Gulch, Stronach and Hoffard under R4.
- Book entries checked against the PDF, printed page = PDF page - 2 up to p.40 and - 6 after:
  - Nelson Creek 2-8 "No Fishing" (p.25).
  - Still Creek 2-8 "No Fishing" (p.26).
  - Watch Creek 5-1 (p.47).
  - Damdochax Creek 6-17 (p.50).
  - Lachmach River 6-14 (p.52).
  - Bowron River 7-7 (p.59). The Bowron oxbow 329059951 is in MU 5-16, but the app binds r7:bowron_river on the sections there, so it stays STREAM_BY_BOOK as the app has it. Whether a Region 7 entry should reach into 5-16 is a question for the owner.

**Class moves made by the fixes** (before the verification pass, then after):

| before | after | n |
|---|---|---|
| LIKELY_STREAM | LAKE | 375 |
| LIKELY_STREAM | UNSURE | 185 |
| STREAM_BY_BOOK | LIKELY_STREAM | 11 |
| (new) | LIKELY_STREAM | 3 |
| LAKE | LIKELY_STREAM | 1 |
| LAKE | UNSURE | 1 |
