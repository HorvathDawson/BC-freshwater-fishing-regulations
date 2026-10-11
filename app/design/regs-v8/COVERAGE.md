# regs-v8 coverage, checked against the fisheval answer key (Q1–Q13)

**V** = visible on the tab without tapping. **1** = one tap away. Screens are `screens/NN-…`, numbered as in v7, with 35–41 new.
Every line drawn from `regs-v8/fixes.js` renders with `data-src="design"` and `data-cite="p.NN"`, where NN is the printed Synopsis page. A check of six screens found every design line cited.

## Changes from the round 2 grade
| Fix | Done |
|---|---|
| 1. Cowichan: both halves at equal weight | Split box above the verdict, two equal tappable cells with live status for the date: "✕ Below Mile 66: closed, opens Nov 16" and "✓ Above Mile 66: open, fly fishing only" (09, 31, 41). The same box is on the map card (25). |
| 2. Size chips | "30 cm or longer" and "60 cm or longer". No "none under" anywhere. |
| 3. Size summary in the verdict | "Sizes: rainbow 50 cm or less, no minimum (4) · other trout 30 cm or longer · char 60 cm or longer · only 1 over 50 cm (steelhead: 2)" (02) |
| 4. Rainbow row | Chips "no minimum size", "50 cm or less", "hatchery". The note carries the dates: "4 a day Jul 1–Apr 30; May 1–31: release" (04) |
| 5. Cowichan CNR Bridge → Greendale | Fly fishing only all year, on both the strip and the Gear tile (38, 39) |
| 6. Side channel | Takes the river's closures (Jul 4–14 and Jul 15–Aug 31). Shows a ⚠ placement line about the Sep 1–Nov 15 below-Mile-66 closure (41) |
| 7. May frame | Steelhead card plus a trout group: hatchery rainbow ≤50 cm and cutthroat release, brook/brown 30 cm or longer, char 60 cm or longer (35, 36) |
| 8. Steelhead duties | One bold boxed row: "Stop fishing this water after 2 · 10 a year · Record each one right away on a paper licence" (02) |
| 9. Under 16 | Both outcomes stacked, with no residency toggle: B.C. resident and not a B.C. resident (16) |
| 10. Cowichan trout | "Only 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)" (31, 38) |
| New | A "⚠ Closes …" line in the verdict when a closure starts within 45 days (35: Jun 1; 38: Jul 4) |

## Q1–Q10
These are unchanged from v7's coverage (`regs-v7/COVERAGE.md`): every item is visible or one tap away. The items that were hard to find are now on the first screen:
- Steelhead duties: the bold duty row (02).
- Size rules: the verdict size line (02).
- Cowichan split: the equal-weight box (09).
- Non-resident under 16: stacked outcomes (16).

## Q11: Vedder mouth part, May 15 (35, 36, 37)
| Must show | v8 |
|---|---|
| Fly fishing only and bait ban, May 1–31 | V: verdict sentence, striped strip window and key, Gear Bait "Bait ban (May 1–31)", Ways "Fly fishing only" (37) |
| Hatchery rainbow ≤50 cm and hatchery cutthroat: release | V: verdict sentence; Rainbow and Cutthroat rows "Release" (36) |
| Hatchery steelhead 2 | V: verdict and steelhead card (35) |
| June closure coming | V: "⚠ Closes Jun 1: no fishing Jun 1–Jun 30" (35) |
| Single barbless hook | V: Gear Line & hooks (37) |

## Q12: Cowichan, CNR Bridge → Greendale, July 1 (38, 39)
| Must show | v8 |
|---|---|
| Open, fly fishing only (all year) | V: verdict "Open, fly fishing only…", strip key "Jan 1–Jul 3, Sep 1–Dec 31: fly fishing only (all year)", Gear Ways (39) |
| Bait ban, single barbless | V: verdict sentence; Gear Bait "Bait ban, all year, in all Region 1 streams"; Line & hooks |
| 2 hatchery, wild released (including wild steelhead) | V: Trout group lead (38) |
| Upcoming Jul 4 closure | V: "⚠ Closes Jul 4: no fishing Jul 4–Aug 31" (38) |

## Q13: Cowichan below Mile 66, August 1 (40)
| Must show | v8 |
|---|---|
| Closed | V: "✕ Closed on Aug 1"; split box shows both halves closed |
| Both reasons | V: "Why it's closed" lists Jul 4–14 (p.15), Jul 15–Aug 31 (river row and Region 1 summer closure, p.13, p.15), and Sep 1–Nov 15 below Mile 66 (p.15) |
| Continues into Sep 1–Nov 15; reopens Nov 16 | V: "Opens Nov 16" and the strip |

## Still open (pipeline, not design)
- The side channel's placement against Mile 66.
- r9 lacks `closed: true` (a 50.0 cm rainbow).
- v36's "N a day in total / Keep 0 here" template.
- The May display gap noted in Q11: v36 shows no rainbow or cutthroat row. The fixes.js lines cover all of these only until the bundle carries them.
