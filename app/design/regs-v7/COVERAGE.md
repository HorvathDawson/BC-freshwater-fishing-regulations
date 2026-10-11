# regs-v7 coverage, checked against the fisheval answer key

**V** = visible on the tab without tapping. **1** = one tap away (sheet, fold or part). Screens are `screens/NN-…`.
Any line drawn from `regs-v7/fixes.js` (answer-key wording, not v36 data) renders with `data-src="design"`.

## Q1: Vedder mouth part, Oct 10: keep a hatchery steelhead? How many?
| Must show | v7 | Where |
|---|---|---|
| 2 hatchery a day | V | Steelhead card, numeral 2 "hatchery a day", directly under the verdict (02) |
| Wild steelhead: release | V | Steelhead card, first line (02) |
| Stop fishing this water after 2 | V | Steelhead card, in bold (02) |
| 10 a year | V | Steelhead card (02) |
| Record on your paper licence right away | V | Steelhead card, and the Licence note on Gear (13) |
| Counts toward the trout and char total | V | Steelhead card, last line, and the verdict sentence (02) |

## Q2: Keep a wild cutthroat?
| Wild trout and char from streams: release every one | V | Trout and char lead, and the "Hatchery only, release wild ones" check (03) |
| How to tell wild from hatchery | V | Inline adipose drawings (03); sheet 08 is linked from the steelhead card, "How to tell", and "When you catch a fish" |

## Q3: Size limits
| Up to 4 trout and char a day | V | Verdict and group numeral 4 (02, 03). The v36 "2 a day in total / Keep 0 here" lines are removed. |
| Hatchery rainbow: any length, 50 cm or less, 4 a day, Jul 1–Apr 30; no 30 cm minimum, outside the 2-from-streams cap | V | Rainbow row, listed first, with chips and a note (04) |
| Other hatchery trout: none under 30 cm, at most 2 from streams, only 1 over 50 cm | V | Brook/brown/cutthroat row and the group check lines (03, 04) |
| Char: 1 a day, none under 60 cm | V | Char row (04) |
| Over 50 cm is a steelhead | V | Rainbow row note and steelhead card |

## Q4: Barbless? Bait?
| Single barbless hook | V | Gear: Line & hooks answer and table (14) |
| Bait OK today | V | Gear: Bait answer, "Bait OK today" (34) |
| May 1–31: bait ban, fly fishing only | V | The Bait answer's "Not May 1–31" line (34). Also shown on the open-days strip as a striped window with a key entry (02), and as the verdict on May 15 (29). |
| No dead fish as bait | V | Bait list: "Fish or fish parts: banned" |
| Roe: no more than 1 kg; receipt if bought | V | Bait list, last row (34) |

## Q5: Licence and stamps for steelhead; son, 15
| Basic licence + steelhead stamp, even for catch-and-release | V | Licence answer and the bold stamp line (13); also linked from the steelhead card |
| Prices | V | Licence documents list (13) |
| Paper licence to keep a hatchery steelhead | V | Licence note (13) and steelhead card |
| Under 16, B.C. resident: no licence or stamp, own limit | V | Age switch, then residency switch (16) |
| Under 16, non-resident: no licence only with a licensed adult, whose limit his fish count in; otherwise his own licence | V | Residency switch set to "Not a B.C. resident" (30) |

## Q6: Vedder mouth part, Jun 15
| Closed Jun 1–30, opens Jul 1 | V | Verdict "✕ Closed · Opens Jul 1", hatched on the strip (10) |

## Q7: Kootenay LWA kokanee, Sat vs Mon
| Sat: 5 · Mon: release only | V | Verdict sentence and the "At certain times" line (11, 12) |
| Mon–Fri: keep 0 | V | Kokanee row, in bold (12) |
| Combined 5 with the Upper West Arm | V | Kokanee row (11, 12) |
| Trout and char 2, only 1 bull trout | V | Group and kind rows |
| Boundaries: McDonalds Landing (Six Mile) to Corra Linn Dam | V | Verdict card (11) |

## Q8: Cowichan, Oct 10
| Closed below the CNR Mile 66 Trestle; reopens Nov 16 | V | Part 1 verdict: "No fishing … below the Mile 66 Trestle", "Opens Nov 16" (09); map card (25) |
| Upstream: open, fly fishing only | V | Green line in the part 1 verdict and the map card, linking to part 2 (09, 25). Parts 2–4 have full answers (31). |
| Bait ban and single barbless everywhere | V | Upstream verdicts and the Gear tab for parts 2–4 (31, 32) |
| Weir–Greendale closure Nov 15–Apr 15 | V | Part 4 strip |
| Side channel | V | v36's answer: open, bait ban. Its placement is a known data flag. |

## Q9: Hatchery vs wild
| Healed scar where the adipose fin was, with the diagram | V/1 | Inline drawings with larger captions (03); full sheet 08, linked from every "hatchery" line, the steelhead card, and "When you catch a fish" (33) |

## Q10: Snagged fish
| Release right away, even by accident; what "snagged" means | V | "When you catch a fish" on Regulations (33); Ways to fish on Gear; the Always sheet (28) |

## Other fixes
- The strip is readable without colour: ✓ open is solid, ✕ closed is hatched, and fly-only windows are striped. Each has a key entry and glyph.
- The verdict word carries a ✓ or ✕ glyph.
- The date control is labelled "CHANGE DATE" and is 44 px, in the bar beside the water name (not under the parts strip).
- Header: 154 px at the top of the page (status bar, bar and parts strip). Once the page scrolls past 40 px it collapses to 96 px (11.4%), with the parts strip replaced by dots in the bar.

**Result:** all 10 questions' "must show" items are visible or one tap away. Two answers depend on pipeline data fixes, not design: the Cowichan side channel placement, and r9's `closed:true` (50.0 cm). Those are listed in r1_grade.
