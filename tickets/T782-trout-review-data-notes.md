# T782 — Data faults found by the trout display review (not design)

Status: OPEN (pipeline answers / app core; none is new in r-46e4355a7277).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T782 | Data faults found by the trout display review: Kitimat wild rainbow limit of 1 not on the card, Ross Lake headline 2 against a daily total of 4, eight rule sets with no top line, the Dolly Varden/bull trout picture captioned "Bull trout" | OPEN | — | `git show 828c5f7c4:docs/design/trout-display-review-2026-11-15.md` |

Source: `git show 828c5f7c4:docs/design/trout-display-review-2026-11-15.md` (rounds 1 to 4, "Things noticed in the data" and
"Possible defect to check").

1. **Kitimat River and Humphrys Creek, rainbow.** The stored rows give hatchery rainbow 5 a day (50 cm or less) and
   WILD rainbow 1 a day (30-50 cm); the card shows one line, "Up to 5 rainbow trout, 50 cm (20 in) or less". The wild
   limit is not said. Trace where the origin split is lost (core `keep.ts` / `tree.ts` or the stored headline).
2. **Ross Lake.** The stored headline says 2, `real_daily.n` says 4 (three keep rows). One of them is wrong.
3. **Eight rule sets have no top line** (Kaslo River, Keen Creek, Kitimat x3, Humphrys Creek, Williston Lake zone B,
   Ross Lake): a tier is flagged looser than the region but `headline.also` is empty.
4. **Dolly Varden/bull trout picture caption.** The row shows `bull-trout.webp` captioned "Bull trout"; the fish is
   "Dolly Varden/bull trout" everywhere else (one species entry `DV`).
5. **"See every regulation line"** for the Vedder and Little River trout groups does not list the water's own rule
   (4 hatchery rainbow; Shuswap Lake's 1 a day). Not traced.
6. **Skeena Jul 15:** the tier "Up to 1 brown trout, cutthroat trout, rainbow trout or steelhead combined" comes from a
   rule dated Jul 1 to Oct 31; the card does not show the dates.
(Kootenay Lake's "over 50 cm" was T763, built. The Fraser's October closure line was T762, built.)
