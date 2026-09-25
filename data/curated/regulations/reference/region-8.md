# Region 8 — Okanagan

Transcribed verbatim from the 2025-2027 synopsis Region 8 chapter. **Source text.**

---

## General Regulations

**No fishing:** (spring closure) in any stream in Region 8 from Apr 1-June 30 (see tables for
exceptions). See definition of “streams” on page 80.
**Single barbless hook:** must be used in all streams of Region 8, all year.

## Region 8 Daily Quotas

```
Trout/char: 5, but not more than
  • 1 over 50 cm
  • 4 from streams (only 2 over 30 cm)
And you may retain:
  20 brook trout from streams
And you must release:
  Bull trout (Dolly Varden) from streams

Bass:         0 quota, CLOSED TO FISHING (see tables for exceptions)
Burbot:       2        Crappie: 20        Crayfish: 25
Kokanee:      5 (none from streams)
Walleye:      8        Whitefish: 15 (all species combined)
Yellow perch: 0 quota, CLOSED TO FISHING (see tables for exceptions)
```

Possession quotas = 2 daily quotas.

## Notices

**GARNET LAKE ANGLING CLOSURE** — due to the illegal introduction of largemouth bass, Garnet Lake
has been closed to all angling. Garnet Valley Reservoir will be used as a research lake.

**OKANAGAN LAKE DAM FISH PASSAGE INITIATIVE** Controlled testing is underway to investigate fish
passage options at Okanagan Lake Dam, monitor fish navigation of the ladder, and upstream habitat
utilization of sockeye and Chinook slamon. If you catch a tagged fish, please report it to the
Okanagan Fish & Wildlife office in Penticton at 250-490-8200.

**Syilx OKANAGAN NATION.** The Okanagan is the traditional territory of the Syilx people. The Syilx
people of the Okanagan Nation are a trans-boundary tribe between Canada and the United States. The
Okanagan Nation Alliance (ONA) is committed to the conservation, protection, restoration, and
enhancement of indigenous fisheries and aquatic habitats within Syilx Territory.

**Crayfish trapping.** Many Okanagan anglers enjoy trapping and eating crayfish. Unfortunately,
some styles and sizes of legal traps are very effective at catching and drowning turtles. This is
particularly concerning for the Western Painted Turtle, a native species whose population health is
vulnerable to human activities. Anglers are encouraged to use traps with minimally-sized circular
openings, to reduce the chance of capturing turtles.

---

## Checked against the curated entries

`region-8.json` — 19 entries. **Every quota and sub-limit is present and correct**, including the
nested stream limit (*4 from streams, only 2 over 30 cm*) and the brook trout allowance.

### Missing — one advisory

| # | rule | note |
|---|---|---|
| 1 | **Okanagan Lake Dam Fish Passage Initiative** — report tagged fish | the fourth tagging/marking programme found missing (R1 cutthroat reward tags, R2 Stave River maxillary clipping, R8 this one; R3, R6 and R7A have theirs). **This is now a pattern, not an accident** — the archive dropped tagging programmes systematically. |

### Confirmed droppable

**Garnet Lake closure** — `r8:garnet_lake@8-8` already carries `closure` / *"No fishing"*. The
*reason* (illegal largemouth bass introduction) is not recorded, and `no_fishing.reason` exists for
exactly this.

### What this chapter settles about `bonus_allowance`

The source reads:

```
Trout/char: 5, but not more than
  • 4 from streams (only 2 over 30 cm)
And you may retain:
  20 brook trout from streams
```

**"And you may retain" is a separate heading from the sub-limit list.** Brook trout are not carved
out of the 4-from-streams limit — they sit outside it entirely, with their own allowance of 20.
That confirms the decision to drop `bonus_allowance` as a type: it is a **species replacement**
(`species=[EB], take=20, water=stream`) whose take exceeds the zone limit, which §4.4 already
resolves — the water/species rule governs brook trout and the zone limit re-renders on its residual
species. No additive type is needed.
