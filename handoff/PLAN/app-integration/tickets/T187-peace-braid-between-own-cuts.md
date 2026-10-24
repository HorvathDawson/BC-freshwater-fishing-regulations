# T187 — The Peace braid piece between Site C's own two cuts binds no Peace row

Status: DONE (dc46e53e fix + tests + POLICY_VERSION 31 + RULINGS E18; 56981f9b taps). Not promoted: the side
run is `data/generated/scratch/peace/reach`, and the owner promotes it.

## The defect

The post-braid verification (2026-10-24) found that `359005110:3454` binds only zone and navigation rules. It is the
1,838.7 m piece of a Peace braid (blk 359005110, 3,455.0–5,293.7 m), between "boundary signs 1200 m downstream of Site C
dam" and "Site C dam". It should carry `peace_river.r1` ("From Site C dam to boundary signs", No Fishing).

The cause: the T67 sweep cut the braid at BOTH cuts of the window, so the piece's lower bound is the signs split and its
upper bound the dam split. `_by_measure` treats both as the window's own `cut_ids`
(`A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`), so the piece is severed from 359005110:0 and 359005110:5293. Its only other
neighbour is a creek in no item (359322371:0), so it touches nothing of its water and is `detached`. `_bracket` then
judged it by its own line's nearest placed pieces. Both of those are outside, so it was put outside. But each one lies
across a cut, and crossing a cut changes the side. The sister piece 359013560:165 and the Adams middle piece
355996658:1742 were placed correctly, because each has confluence edges onto its mainstem's inside piece.

## The rule as fixed (RULINGS E18, `extent.A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE`)

A detached component lies inside the window when all of these hold:

- the window has two real cuts (`0 < lo`, `hi < ∞`);
- every member is on ONE blue line other than the principal one;
- the members form one unbroken run of that line;
- the run's LOWER end carries an id of the window's lower cut, and its UPPER end an id of its upper cut (ids that name
  both ends are discarded).

Nothing else changes:

- a piece that ends at one cut only, or at another cut merely near the window, still goes to the bracket and then the
  D5 flow path;
- so does a piece lying the wrong way round;
- so do half-line windows (`upstream_of`, `downstream_of`) and a `between` across two lines (its halves each see one cut).

**Census on atlas `full`:** exactly 3 stream pieces are bounded at both ends by two braid-swept cuts off their hosts:

| piece | window | rule |
|---|---|---|
| 355996658:1742 (Adams) | adams_river_downstream.r1 | already inside |
| 359005110:3454 (Peace) | peace_river.r1 | **the defect** |
| 359013560:165 (Peace) | peace_river.r1 | already inside |

Both windows are same-line `between`s.

## Tests (`pipeline/tests/test_reach_braid_cuts.py::test_t187_*`)

- **The fixture** is a host with cuts a (1000) and b (2000). A braid leaves the host above b and rejoins it below a,
  and is cut at both, so its middle piece B:100 touches only an off-item creek. `between(a, b)` holds {H:1000, B:100}
  in either split order.
- **The mutation:** with the flag False, the result is {H:1000} only. This is the defect, reproduced.
- **Near-the-cuts pins:** the wrong way round (b, a), one end only (a, z) and (z, b), and one id at both ends (a, a).
  None of these pulls B:100 in, and each result is identical to the flag-off result.
- **One-cut windows:** `upstream_of` / `downstream_of` on a and on b give the same result as with the flag off.
- `test_reach_builder.py` policy pin updated, with POLICY_VERSION 30 → 31.
- The reach/extent suites were run unfiltered: 355 passed.

## The side binding diff (`pipeline.atlas.reach.binding_diff` reaches/full → scratch/peace/reach)

```
1 rule(s) bind different sections · 0 unresolved change(s) · 0 licensing record(s) moved
  r7:peace_river_from_site_c_dam_to_boundary_signs_1_200m_downstr@7-31::peace_river.r1  +1 -0   (359005110:3454)
```

That is the whole change: the Site C closure gains 1,838.7 m. No downstream row loses anything, because after T67 the
piece was already in no Peace row (it left the downstream rows when E15 went). These files are byte-identical:
`rule_unresolved`, `rule_extent`, `rule_diagnostic`, `licensing_*`, `steelhead_presence`, `tidal`, `outside_bc`.
Report digest: faba79c782e33afe → c62585cb8aae61fb.

## Taps

`app/design/regs-v11/port/taps.py`:

- x22p6's DEFECT entry in PART_RECOUNTED is removed. It becomes PART_MOVED 6 → 2: the same label and 1 section again
  (the Dinosaur Lake part's rule set loses the piece). RECUT_WHY gets a T67 reason.
- x22p2's PART_GONE reason now says the closed parts hold 7 sections (the closure took 359013560:165 and 359005110:3454).

NOT verified against a release: taps.json and the golden under `app/fixtures/golden/` are regenerated after deliver.
If part 2 is not the Dinosaur Lake part there, taps.py will say so and must be adjusted.

## Promote (owner)

```
mv data/generated/reaches/full_prev data/generated/reaches/full_prev.old   # or remove it, your call
mv data/generated/reaches/full data/generated/reaches/full_prev
mv data/generated/scratch/peace/reach data/generated/reaches/full
scripts/heavy.sh deliver 40 -- .venv/bin/python -m pipeline.deliver      # every step, canonical paths
.venv/bin/python app/design/regs-v11/port/taps.py
```
