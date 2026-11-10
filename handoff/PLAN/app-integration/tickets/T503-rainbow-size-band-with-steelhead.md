# T503 — Rainbow size says the band (30–50 cm) where steelhead rules apply too

Status: DONE — 2477ab293; proved by `test_answers_r20i.py` / `test_answers_t475_quota_wording.py` (the band wording) and the releases built from it (r-6dce77a24e6a, r-d14bdb180f55).
Earlier: IN REVIEW — 2477ab293; ships with the next release. Owner 2026-11-08 (phone).

The trout row reads "Up to 2 rainbow trout · 30 cm (12 in) or longer · release any shorter". Where the water also has
steelhead rules (a rainbow over 50 cm is a steelhead), that reads as if any rainbow over 30 cm may be kept. Say the band:
"30–50 cm (12–20 in)", so the size and the count are clear together.

## Do
- Find where the rainbow size line is built (lengths: ordered ranges, first match wins — memory "lengths replaces size
  polarity"); where the page has steelhead rules, the rainbow line's upper bound is the steelhead definition length.
- Pipeline side (cascade in Python); test on a water with both (Vedder) and one without (a lake with no steelhead).
- Check the wording on the owner's next look; no rule changes, only how the size reads.

## Done (2477ab293)
`tree.size_rule`: where a rainbow's top is the steelhead line on a steelhead water it now says the band, "must be
30–50 cm (12–20 in); release any shorter" (was "30 cm (12 in) or longer"). Over the top is still no release (a
steelhead, its own row). Tests r20i + t475 updated; pipeline suite 4683 passed, 1 unrelated failure (T507).
