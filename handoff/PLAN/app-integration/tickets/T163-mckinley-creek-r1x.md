# T163 — McKinley Creek r1x: catch-and-release is "downstream of McKinley Lake", not the whole creek

Status: DROPPED — not a defect — no catalogue change made (lane-T163, 2026-10-23). The premise is wrong: r1x is not the catch and release, it is Region 5's spring-closure lift (RULINGS L13), and it covers the whole listed creek by design. Owner: close, or overrule L13 for this row (see "Finding").

Source: T153 adversarial review 2026-10-23 (spot check of the 10 largest newly bound polygons). The catalogue gives
McKinley Cr rule r1x a whole-water extent, but the book (printed p.46) limits its catch-and-release to "downstream of
McKinley Lake"; the 45 ha pond lake:329184498 (upstream) now wrongly takes it. Fix the extent in the catalogue (curated
entry; through the review tool's fix mode or a reviewed edit), reach rerun, binding diff shows the upstream sections and
the pond leave r1x. Size: S.

## Finding (lane-T163, 2026-10-23) — r1x is the spring-closure lift, not the catch and release

**Book**, printed p.46 = PDF page 52 of `data/source/fishing_synopsis.pdf` (read with pdfplumber), the whole row:
"MCKINLEY CREEK 5-2 Trout/char catch and release downstream of McKinley Lake; bait ban".

**The catalogue row** `r5:mckinley_creek@5-2` (`data/curated/regulations/entries/catalogue/region-5.json`) has three rules:
- `r1`: the catch and release (TROUT_CHAR, take 0), `downstream_of` split `mckinley_creek__mckinley_lake`. Correct.
- `r1x`: ALL_GAME_FISH with **no take**, `exempts: spring_stream_closure`, `whole`, `includes_tributaries: false`. Its
  `verbatim` is the row sentence, but in the bundle it is `dimension: lift`, label **"Spring stream closure lifted"**.
  It lifts p.42 "No fishing in any stream in Fraser River Watershed of Region 5 … Apr 1-June 30, EXCEPT … other streams
  listed in the tables". RULINGS **L13** says that exception exempts **every stream that has a row in the tables**, and
  this row is McKinley Creek, the whole creek. Every Region 5 row with this lift (Baker, Blue Lead, Moffat, Mitchell r3x,
  …, 19 rules) has `whole`, including rows whose sentence names only part of the water. That is deliberate.
- `r2`: bait ban, `whole`. Fine as written; the `; bait ban` reads as a separate clause covering the whole row.

**What the bundle shows today** (`data/generated/bundle/bundle.sqlite`, rulesets that hold r1x):
- set 402 (3 sections, below the lake): r1 + r1x + r2 + `z5 spring_stream_closure.r1`, lifted. C&R applies.
- set 405 (5 sections, above the lake): r1x + r2 + `z5 spring_stream_closure.r1`, **no r1**, so no C&R. The lift keeps them
  open in spring.
- set 1656 (1 section, the pond `lake:329184498`, a river widening at 50.72-53.18 km on gnis:25591, near the headwater McNeil
  Lake): r1x + r2 + the lake defaults. **No r1, and no spring_stream_closure**. The pond gets the bait ban, and r1x
  is an inert lift of a closure that does not apply there. **The pond is not catch and release.** T153's "r1x gives it C&R"
  went by r1x's verbatim and missed its type.

**Why the asked-for change would be wrong**: narrowing r1x to `downstream_of mckinley_creek__mckinley_lake` would put
the z5 spring closure back on the 5 sections above the lake (closed Apr 1-Jun 30). L13 says no, because the creek is listed. It would
only clear an inert note off the pond. No edit was made and no reach run was done. There is nothing to measure.

**Boundary**: if the owner overrules this, the cut to use is split `mckinley_creek__mckinley_lake` (lake_edge on
gnis:25591 at 8.09 km = outlet and 14.9 km = inlet, wbk:329184475). r1 already uses it. `rule_ambiguous_cut` reports both
candidates, and `downstream_of` takes 8090.25, the outlet, which is correct.

**Rest of the row**: r1 is right (the right cut, and the outlet measure is the one used). r2 is right. No other rule
repeats the mistake the ticket describes. A cosmetic point, left alone: x-lift rules carry the row's whole sentence as `verbatim`. A
reviewer reading the verbatim instead of the label will keep reading these as the row's restriction. Same on the other 18.

Rebuild needed: none.
