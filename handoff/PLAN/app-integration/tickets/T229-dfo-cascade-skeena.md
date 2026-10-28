# T229 — the DFO salmon cascade on the Skeena: how it works, a Cascade map, and what looks wrong

Status: PARTIAL — the explanation and the Cascade map are done; owner questions Q1–Q6 are open (no pipeline change made)

Source: owner 2026-10-28: "make sure the cascade regs in Skeena are correctly done … go over how the cascade works and
make sure I agree. maybe on a map use colors to show the different cascading, multi colour if multiple apply."

## How the cascade works today (`pipeline/deliver/salmon.py`, the release's `salmon` file)

A section's **chain** is every DFO place whose drawn extent covers it, ordered:

1. **Tier.** Named water (a row with a river or lake name) → tidal-area default ("streams flowing into Areas 3–6")
   → section default (B(i) above the CNR bridge at Terrace, B(ii) below, B the whole Skeena) → region default (A, all
   Region 6). Precedence 3 / 2 / 1 / 0 in the curated entry file.
2. **Inside a tier: the location id, alphabetically.** Size or narrowness plays no part (see Q1).

**For each species, on each day, the first place in the chain that has a rule for that species that day answers.**
Places that don't mention the species, or whose dates don't cover the day, are skipped and the next place is asked.
Only one place answers each species-day. If two places both have a rule, the first one wins, even if it is less
strict. Within one place, a row for the species beats an "All" row, and any open row means open.
- **Nothing answers → closed** (owner 2026-10-27: DFO lists openings, so silence means closed). A water DFO never
  lists is closed every day.
- **"Including tributaries" / "mainstem only" / "excluding …" / "other than …"** are part of a place's *extent* (which
  sections it covers). They don't affect its rank. A "Kispiox including tributaries" place and a "Skeena mainstem
  only" place are both tier 3.
- **Gear lines** (bait bans, fly only) come from the first place with any gear line that day. They replace the wider
  places' gear lines; they are not added to them.
- **A place DFO lists for a water but nobody has drawn yet** ("undrawn") puts the whole water on `check`, because we
  can't tell which stretch it covers.
- **Region header notes** (daylight only, aggregate limits, adult-chinook size) are not part of the chain. They show
  on every Region 6 water as "Region rules (DFO)".

In the Skeena (87,260 sections) this gives 62 distinct chains.

## Skeena examples (checked against the chains the release builds)

| # | where | chain (narrowest first) | what it says |
|---|---|---|---|
| 1 | unlisted creek, upper Skeena | Areas 3–6 → B(i) → A | Every species closed all year. Pink after Jun 15: B(i) has no pink rule, so A answers ("no sockeye, pink or chum") → closed |
| 2 | unlisted creek, lower Skeena | Areas 3–6 → B(ii) → A | Every species closed. Chum is closed by B(ii)'s "All: no fishing for **eulachon**" row (Q4) |
| 3 | Skeena mainstem, Terrace to the sea | mainstem below CNR → "mainstem waters only" (a B(i) row, Q3) → Areas 3–6 → B(ii) → A | Coho open Jul 15–Nov 30, sockeye in the FN windows, pink 2/day, chinook and chum closed |
| 4 | Skeena at the Kitsumkalum mouth (Zymagotitz to Hell's Gate) | mainstem below CNR → **near the Kitsumkalum** → … | DFO closes all salmon Jul 1–Aug 31. **The app opens coho, pink and sockeye here** because "downstream-of-cnr" sorts before "near-the-kitsumkalum" (Q1) |
| 5 | Skeena mainstem, Cedarvale to the CNR bridge | mainstem only (pink) → Cedarvale–bridge (chinook closed) → bridge-to-Babine (sockeye FN windows) → bridge-to-Babine (no sockeye) → above the bridge (coho Jul 15–Oct 15) → B(i) → A | Correct today, but only because the ids happen to sort narrowest-first |
| 6 | Kispiox, including tributaries | Kispiox → Areas 3–6 → B(i) → A | Coho open Jul 15–Aug 23 then closed (FN0897), chinook closed, pink closed. DFO's "downstream of the signs near Kispiox River Resort: pink 2/day" is not drawn, so the water shows `check` |
| 7 | Bulkley mainstem below the Morice; the Forks | B(i) → A (the Bulkley's own places are **not drawn**) | Closed, shown as `check`. At the Forks closure (no salmon Jun 16–Aug 15), the "Morice below Lamprey Creek" coho opening wins on the Morice side (Q1) |
| 8 | lakes | Morice Lake (with tributaries) → … ; Treston Lake → … | Morice Lake is closed all year. Treston Lake has coho 2/day Sep 1–Oct 31. Babine Lake's three places are not drawn |

Every chain for the Skeena (or the Nass) can be checked on the map in the review app: DFO salmon extents → **Cascade**.

## Owner questions

- **Q1 — rank inside the named-water tier.** Today it is alphabetical by id. That gives two wrong answers: the
  Kitsumkalum-mouth closure (4 sections, coho/pink/sockeye open when DFO closes them for 48–62 days) and the Morice
  Forks / Gosnell–Lamprey closures (coho open 16–32 days). Proposed fix: inside a tier, the place covering fewer
  sections comes first. A reach then beats its whole river, and a closure zone beats the reach around it.
- **Q2 — the "streams flowing into Areas 3–6" default (chinook closed) sits on 97% of the Skeena and 96% of the
  Nass.** On the DFO page it belongs to section E ("other mainland watersheds, except …"). It has no effect on the
  outcome today, because B(i), B(ii) and C close chinook too. But it outranks B(i)/B(ii), so the card names the wrong
  rule. It would also hide a future B or C chinook opening. Proposed fix: bind it outside the Skeena, Nass, Haida Gwaii
  and Fraser basins, as section E's own binding already is.
- **Q3 — B(i) rows reach below the CNR bridge.** "Skeena: mainstem waters only" (pink 2/day, B(i)) is bound to the
  whole mainstem, so it covers the 119 B(ii) sections too. It loses there only because of the sort. Should every
  named-water place be clipped to the lettered section it is listed under? Proposed: yes.
- **Q4 — "All | No fishing for eulachon" (B(ii)) is typed as an all-salmon closure.** It answers chum on the lower
  Skeena (closed anyway, through A). Proposed: eulachon is not a salmon species, so drop the row.
- **Q5 — a tributary entering exactly at a cut belongs to both sides.** For example, Lamprey Creek's 167 sections are
  in both "Morice below Lamprey Creek" and "Gosnell to Lamprey". Which side should it go to?
- **Q6 — about 30 Skeena places are still undrawn.** These include the Bulkley mainstem below the Morice and the
  Bulkley tributaries, Babine Lake (×3), Kitwanga, the Kitsumkalum/Lakelse/Zymoetz/Kasiks reaches, Kispiox below the
  signs, Sustut and Suskwa including tributaries, and Shegunia. Their waters show `check`. This is the DFO review
  queue's work; confirm it comes before any change to the rank.

Not in question: A is the bottom of every chain (the page says "Section A applies if … not listed"), and B / "all
waters in the Skeena watershed" carry no rules.

## The Cascade map (done)

- Backend `curation-review/backend/cascade.py` + `GET /api/dfo/cascade/{skeena|nass}?min_order=&tol=`. It reads the
  same three inputs as the bundle (curated places, typed feed, reach run's `dfo_salmon.jsonl.gz`) and builds the
  chains with the bundle's own order. Winners come from `salmon.verdict_on` / `gear_on`, called one place at a time,
  which is the same walk. One feature per chain (merged and simplified). Lakes of 25 ha or more are drawn as FWA
  outlines, read in a single table scan: per-lake `_fwa_lake_poly` is a full scan per lake, 17,529 of them in the
  Skeena. Skeena first load takes about 18 s and returns 6.7 MB. Read only.
- Frontend `CascadeView.tsx` (`#cascade/skeena`, a "Cascade" tab in DFO mode):
  - Each place gets one colour. A section with several places is drawn as parallel stripes in chain order.
  - Places under 90% or more of the view (A, B, the Areas 3–6 default) and places with no rules today are listed but
    not striped; tick a place to draw it.
  - Hovering a legend row lights up where that place applies. A dotted black line marks a water with an undrawn place.
  - Clicking a section shows its full ordered chain, who answers each species on the chosen day, the gear line, and a
    year bar per species: which place answers (colour) and what it says (open / release / closed / check).
- Screenshots: `T229-shots/t229-c1.png` (watershed), `t229-c2.png` (an Exchamsiks section's chain + year),
  `t229-c3.png` (stripes, zoomed).
