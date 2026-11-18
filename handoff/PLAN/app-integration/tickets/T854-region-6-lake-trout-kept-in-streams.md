# T854 — Region 6 streams kept lake trout Nov 1-June 30 (book: released, like every trout and char)

Status: PARTIAL — fix built 2026-11-18 (below); left (Claude): the new 0.8.0 data release to staging (rides with T850).
Found by the app chat's Nass check 2026-11-18 (the Nass trout table kept lake trout in November); main chat's area.

## The owner (2026-11-18, verbatim)

- To the main chat's question ("Region 6 streams: should lake trout follow the two stream trout rules …"):
  "Yes, include lake trout (Recommended)".
- To the app chat: "trout always covers chgar unles "char" as a whole is referenced elsewise on its own. per the
  book like you said so yes this should be a release like brook trout i think that ius the intention by the book".
- Then: "one time trout is not applied to char is if there is a seperate char groupo reg".

## Cause

RULINGS C2 (amended this morning for Chilliwack Lake) takes one char out of a "trout" line when the row gives that
char a related rule of its own. Region 6's table gave lake trout two: "Lake trout from Fraser and Skeena Watersheds,
Sept 15-Nov 30" and the share "3 Dolly Varden/bull trout and/or lake trout combined". So both stream lines ("Trout of
any size from streams, Nov 1-June 30", "1 trout from streams July 1-Oct 31") carried `species_except [DV, LT]`: on
every Region 6 stream lake trout were kept Nov 1-June 30 and not held to the 1 from streams in summer.

## Fix

`catalogue_parts/species.trout_char_excluded`: a char's own related rule takes it out of a trout line only
(1) where it stands everywhere the line does (`_covers`: the Fraser/Skeena line stands in two basins, the stream lines
in the whole region), and (2) when it is not a COUNT share inside the group quota (Q23: a share stacks with the other
lines; a SIZE clause inside the quota, the upper Kootenay's "no bull trout under 75 cm", still leaves).
`region-6.json` trout_char_quota.r4 / r7: `species_except [DV]`. Every other line of the 2026-11-18 sweep is
unchanged (test_c2_one_char SWEEP). Test: `test_c2_one_char.py::test_t854_…` (mutation: the lake trout line made
region-wide takes lake trout out again).

## Settled with the owner (2026-11-18 14:33)

Asked whether his last wording also puts bull trout back in Chilliwack Lake's "No wild trout over 50 cm" (and the upper
Kootenay's "no trout under 30 cm"). Answer, verbatim: "the species specific ones still are more specific so it applies to that species but if it is something like bull trout only 1 over 60cm but regional quota is 2 bull trout and 3 trout, then we could keep 2 bull trout which take 2/3 of our trout quota. make sense? so i think this is the mornign decision unless im confused? idk what other char -group one is idk what you meant"

So this morning's ruling stands: a rule for ONE char on the same water is more specific and that char follows it (it
still counts toward the group's quota: "2 bull trout … take 2/3 of our trout quota"); a rule that stands only in part
of the region, or a count share of the group quota, takes no char out. As built.

Then (14:36), verbatim: "i just meant trout means troutt and char unless the group char are explicitly listed on their own like 3 trout and 2 char per day" — the char-GROUP case of C2 (b) (Region 1's "All char", Alta Lake's "1 char"), as built.
