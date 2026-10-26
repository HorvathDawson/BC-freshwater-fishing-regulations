# Overlapping rules — ACCEPTED by owner 2026-10-26 (all 4) and applied; see decisions.md

Owner, 2026-10-26, on `z2:trout_char_quota`: "these 3 could be combined into 2: a steelhead one and an
except-steelhead one. Look for others like that."

**Method.** Every `retention_limit` of every entry (11 catalogue files) was compared pairwise, with
species groups expanded (`species.expand_species`) minus `species_except`. A pair counts as an overlap
when the species nest, and the size band, `when`, `water`, `period` and extents are the same, and
neither rule is a closure or a `within` sub-quota of the other. The candidates fell into these kinds:
- **Real overlaps: 4 entries** (table below).
- **Not overlaps:**
  - daily-vs-possession twins (`period`), e.g. Bowron, Bennett, Tagish, Teslin, Morley, Carbon and Charlie lakes;
  - lift carriers (`r1x`, `ALL_GAME_FISH` with no take);
  - a group quota beside a release of one member (`z7a`/`z7b`/`z8`; Region 8's 20 brook trout from streams
    is already `exempts`);
  - a quota beside its parenthetical sub-clause (Elk, Kootenay upper, Okanagan Lake, Cunningham and others);
  - stream/lake twins (`r7`/`r7b`).
- **No char-vs-bull-trout or kokanee-in-a-group overlap** exists today.

**Does the app answer correctly today? Yes, in all 4 cases.** The reference reader (`ladder.effective_rules`,
AGENTS 53) caps each fish by every rule that binds it, so a restating rule only repeats a cap that is
already there. The app decides no regulation; it lays out the store's tree (`app/.../regs/tree.ts`
header, owner rule 2026-10-21). For Regions 1 and 2, the card's tier is RULINGS Q16 / P31 / P36: "Of these, only 1 may be
over 50 cm (20 in) — 2 if one is a hatchery steelhead". It is built from r2 + r2b
(`rows/conds.py` F17, `rows/groups.py`: "Region 2 prints as two rules (r2 not steelhead, r2b every
fish)"), pinned by `test_rules_round.py::test_q44_at_most_two_over_50_in_total`, and proved by
`card_check`. The overlap shows only in the **rule list**, the curation review and the shipped
`sections` list. There, Region 2 says the steelhead line twice: "Only 2 hatchery steelhead over 50 cm"
(r3) and the r2b total. Khartoum and Lois say "6 hatchery rainbow" and "6 rainbow trout and steelhead
together".

## ⚠ The owner's literal shape conflicts with RULINGS Q16

"A steelhead rule + an except-steelhead rule" would be r2 (non-steelhead: 1 over 50) + r3 (hatchery
steelhead: 2 over 50). That drops Q16's **total of 2**, so 1 rainbow + 2 hatchery steelhead over 50 cm
(3 fish) would become legal. Q16's own example says that is one too many. The two-rule set that keeps
Q16 is **"all trout/char over 50: 2" + "except steelhead over 50: 1"**, i.e. r2b + r2. r3 is the
redundant one: the steelhead's 2 comes from r2b, and "must be hatchery" comes from the wild-steelhead
release (r5/r7 on streams, r5b/r7b on steelhead lakes). Only take the literal shape if Q16 is reversed;
that would change the app's answer and break `test_q44`.

## Proposals

| Entry | Rules now | Proposed | Reason | Conf. |
|---|---|---|---|---|
| `z2:trout_char_quota` (p.21) | r2 TROUT_CHAR except ST, 1 over 50 · r2b TROUT_CHAR, 2 over 50 ("1 over 50 cm (2 hatchery steelhead over 50 cm allowed)") · r3 ST hatchery, 2 over 50, streams | **r2** (except ST, 1 over 50) + **r2b** (all trout/char incl. ST, 2 over 50 = the Q16 total); **retire r3** | r3 = r2b ∩ ST plus "hatchery", and "hatchery" is already r7/r7b (release wild steelhead). Nothing is lost: the 50 cm threshold and `within: r1` (count toward the 4) are on r2b, and there are no dates. Follow-ups: drop `trout_char_quota.r3` from the `exempts` of Khartoum and Lois (they already lift r1/r2/r2b), and re-run `card_check` to check that "— 2 if one is a **hatchery** steelhead" still takes "hatchery" from the steelhead kind's origin rather than from r3. Also seen, keep as is: r6 "Wild trout/char from streams" also covers wild steelhead on streams (r7). It is harmless (release + release), r7 is printed separately, and r7 is r7b's sibling. | High (answer-neutral, wording to verify) |
| `z1:trout_quota` (p.13, Region 1 excl. Haida Gwaii) | r2 TROUT_CHAR except CHAR,ST, 1 over 50 · r2b except CHAR, 2 over 50 · r3 ST hatchery, 2 over 50, streams ("…allowed, see table for exceptions") | **r2** + **r2b**; **retire r3**; move ", see table for exceptions)" onto r2b's verbatim (it is cut at "allowed" today, with no closing paren) | Same shape as Region 2. r3's only extra words are a pointer to the water table, not a rule (AGENTS 48). Same keep note for r6 "All wild trout from streams" vs r5 "All wild steelhead". | High |
| `r2:khartoum_lake@2-12` (p.23) | r1 wild trout/char 2 · r3 hatchery RB = 6 ("(see diagram on page 13)") · r5 RB/ST = 6 in the aggregate (+ r5b wild ST count none, `within` r5) | **r1**, **r5** + **r5b**; **retire r3** | r3 (hatchery RB ≤ 6) is implied by r5 (RB + hatchery ST ≤ 6 together), and wild RB stay capped at 2 by r1/r2. The book prints both lines, so this is a restatement in the book itself. What is lost is the "(see diagram on page 13)" hatchery-ID pointer. If it matters, keep it as an advisory/note rather than a quota. | Medium (book prints both; owner may prefer to mirror the page) |
| `r2:lois_lake@2-12` (p.24) | r1 wild 2 · r3 hatchery RB = 6 · r4 RB/ST = 6 aggregate (+ r4b) | **r1**, **r4** + **r4b**; **retire r3** | Same as Khartoum, without the diagram pointer. | Medium |

**Contrast, already clean:** Region 6's `z6:trout_char_quota.r2` "1 over 50 cm (quota includes hatchery
steelhead)" is one rule over every trout/char (RULINGS S9). This is the single-rule form, used where
there is no separate steelhead number.

Apply via the T185 flow (proposed edit → side rebuild + binding diff → owner accepts → one commit). The
edit must update `test_q44` only if r2b's shape changes (under these proposals it does not), and
remove the Khartoum/Lois `exempts` targets for the retired rules.
