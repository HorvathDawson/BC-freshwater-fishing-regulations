"""`DECISIONS`: every place the page is unclear, or where our reader and the page's ladder name things
differently, written down once (R1 …), as the rows layer resolves it."""
from __future__ import annotations




DECISIONS = [
    "R1 The speakers are the reader's (the `ladder` section, per origin). A rule our ladder "
    "marks `moot` (step 5b, a size clause under an outright release) gets the role `moot` with "
    "its `by` — what the page's own trailing loop gives the same clause under a release winner.",
    "R2 A rule's rung is the reader's place (`read.source_of(rule).rank`; a water row reached by "
    "the tributary walk ranks 1). The page ranks any walked rule of rank >= 0 as 1; the two "
    "agree for every walked rule of rank 0 (all walked rules in the corpus are water rows).",
    "R3 The page's order of a part's rules (its set's reach members, then its trib members, each "
    "by id) is kept: it breaks ties between equal ranks and orders lines and roles.",
    "R4 Fish asked about (5.1) are the page's `speciesAt` plus ST where the part has steelhead, a "
    "rule names ST and the export does not say `steelhead_rules: false`. A fish the page would "
    "ask that the ladder does not answer is a build error, never a skipped row.",
    "R5 Rows of an OPEN SUBJECT (protected species, salmon) come from gate rules that SPEAK when "
    "the reader is asked about the subject's first named fish (the rule's first protected code, "
    "chinook for SALMON, the group itself when it names none): the page's `ctx.active`.",
    "R6 A lift note (`liftNotes`, 'Lifted only when fishing for …') is the page's settle fact: an "
    "`exempts` entry naming a target, a means or sizes, of a rule in force today on this kind of "
    "water (`read.in_force` 'yes'; not in an undrawn part, not one side of the channel), whose own "
    "dates hold — for the winning rule, whatever fish is asked. It decides nothing (the reader "
    "already left the rule standing, partly lifted).",
    "R7 Fish names sort as the page's `localeCompare` does (ICU root collation: case and "
    "punctuation are secondary to letters).",
    "R8 The real daily limit (5.7) reads the general 'only K over X cm' caps structurally: every "
    "general cap condition of the row (no fish list) on a band from X cm up (F5d; the page reads "
    "only the first, by matching its own sentence).",
    # Rows fixes (2026-10-06, ROWS-REVIEW): where the page is wrong, the card follows the book.
    "F1 ONE ITEM PER KIND (review A1): each fish's bands, sub-limit, go-back, own-row reference "
    "and the row's conditions about it are worked out for that fish alone. Fish share an item "
    "when exactly these agree: whether it has lines, its bands, sub-limit, go-back, own-row "
    "reference (rule, number, `against`) and the indexes of the row's `conds` about it. Facts "
    "that are not conditions (steel, caution, also, orphan, tnote, partly, a release line that "
    "moves no band) do NOT split an item: they are not shipped on items, and each one names its "
    "own fish in the row's `everyone`/`groups`. The page builds items from fact groups, so a fish sat in two items and an "
    "item showed one range for fish with different ones (Anderson R., Jul: brook, brown and "
    "cutthroat 'keep 1 over 60 cm'; the book: up to 4, only 1 over 50).",
    "F1b A cap written for every fish of a row but rainbow trout, left out only because a "
    "rainbow over 50 cm is a steelhead there, is general — and counts that steelhead (amended "
    "2026-10-08: it was `except: [RB]`, which the page read as 'steelhead don't count' under "
    "Region 6's '1 over 50 cm (quota includes hatchery steelhead)', p.49).",
    "F2 origin2 (the other origin's own limit) also when only its keep RANGE differs (review A2: "
    "'No wild trout over 50 cm' on Chilliwack, Cultus, Harrison, … lakes), citing the release "
    "line that makes the difference; its condition names its fish (`who`).",
    "F3 A line is about every fish of the row only when its fish SET is the row's (review A3; the "
    "page compares counts: Kitimat R. in Feb lost the cutthroat and brown trout release).",
    "F4 A fish with its own row that is lifted out of (or replaced in) the row's narrowing clause "
    "counts toward the row's OUTER total (`items[].against`), not the narrowed number (review "
    "A4: Vedder hatchery rainbow 4 of Region 2's 4, not 'min(4, 2)').",
    "F5a A slot ('none between X and Y cm') is a keep band of 0 (review A5: Teslin, Tagish, "
    "Morley, Bennett). F5b The zone's outer size cap (`outersize`) caps a water's own bands "
    "like a cap (A6). F5c On a steelhead water a rainbow's keep range ends at 50 cm (A7, as "
    "`origin_lines`). F5d Every general shared cap counts (R8: Region 8's 'only 2 over 30 cm'), "
    "nested caps worked from the largest size down; `shared_cap` lists those that bind.",
    "F6 A possession limit with a number is `possession_cap` (line, role and fact), never a "
    "yearly limit (review B1: 'Per licence year: 1 lake trout in possession'). ONE source: "
    "`display.kind_of` returns `possession_cap`; rows read it as the rule's kind, display ships "
    "it as the rule's kind and words its sentence from it (page bug d10: the page says annual).",
    "F7 `scope.apart`: a water's (or area's) pool counts its fish apart from a wider total only "
    "when it lifts a wider POOL that binds here and no `outer` line is left (review B4: Vedder "
    "r9 and Kitimat r4/r5 lift only a stream share; their fish still count toward the region's).",
    "F8 An area's pool (rank 2: Haida Gwaii, Bowron Lake Park, the Liard watershed) is badged "
    "`of: area` with the area's entry, not 'this water' (review B3).",
    "F9 Every daily limit counts fish kept elsewhere today, a water's own too (the book's daily "
    "quota; review B2): the page's 'Only fish kept on this lake count' is page wording, wrong.",
    "F10 Fish asked about (R4) are the book's: the bundle holds no presence data for any fish but "
    "steelhead (stocking records are not presence), so the group wording stays (brown trout on "
    "Shuswap is named because the zone's trout row names it).",
    "F11 A COUNT limit shared by several kinds gives them that many TOGETHER (user test "
    "2026-10-08, Babine/Skeena): the real daily limit merges the kinds a count clause holds "
    "(the row's stream or lake share, a clause `within` the pool) into one unit worth its take, "
    "innermost first; `real_daily.shared_count` names the limits that bind. Region 6's '1 trout "
    "from streams Jul 1-Oct 31' (p.49): 'Only 1 of the 5 can be brown, cutthroat, rainbow trout "
    "or steelhead', not 3 (each kind's own 1, summed).",
    "F12 Which fish a general size cap does NOT count is decided here (`conds[].except`), never "
    "on the page (it was the page's `exemptOf`): a covered, kept fish whose answers give the cap "
    "no role or line; never a rainbow on a steelhead water where the cap counts steelhead. "
    "Steelhead leave a zone's trout/char size line only where the same table prints its own "
    "steelhead quota for that size (`catalogue.steelhead_scope_problems`): Regions 1 and 2, not "
    "3, 5, 6, 7 or 8.",
    # answers 2.5 (2026-10-10, the angler-eval round)
    "F13 A SIZE RELEASE IS AN ANSWER (bug: Vedder, May 15, p.22 'hatchery rainbow trout catch and "
    "release (50 cm or less)'): where no gate or pool speaks for a fish, a speaking take-0 size band "
    "that releases the fish's whole range on the part (a rainbow's ends at 50 cm on a steelhead "
    "water) decides it: status release, that rule the winner (`_size_release`). Before, the "
    "decided answer was null and the hatchery rainbow vanished from the card. One covering only "
    "part of the range is refused.",
    "F14 GROUP TOTALS ARE THE ROWS' (user 2026-10-10; the page's `groupBlock` derived '2 a day in "
    "total, all kinds together' and 'Kept 2 on another stream? Keep 0 here' from a narrowed "
    "number): a keep row carries `group` — the TOTAL its fish count toward (the row's pool, or the "
    "outer pool its fish are contained by, F4), every kind on the card counting toward it "
    "(steelhead included: user 2026-10-10), each sub-cap of the total that holds here (a stream "
    "share, a count clause, a size clause) with the kinds a water's own rule LIFTS out of it, and "
    "one plain sentence — on ONE row per total; and `elsewhere`, the kept-elsewhere line, qualified "
    "by those lifts ('Keep 0 here, except hatchery rainbow trout 50 cm or less: up to 4 in all').",
    # answers 2.8 (2026-10-12, ROUND9)
    "F15 THE CARD IS WATER-FIRST AND SAYS EACH FACT ONCE (owner 2026-10-12): `group` keeps the decision "
    "(pool, total, kinds, subcaps) and carries the card (`card.card_of`): the headline is this water's "
    "number with no lift, a lift is a positive line naming whose rule, the total a line under a share, "
    "the kinds grouped under the cap they draw from with over 50 cm on their rows; the 2.6 lead and the "
    "2.7 items are gone (they restated the rows). RULINGS Q23b: lifted fish count toward the stream "
    "tally (`elsewhere`, `worked`). Every number is proved by `card_check` against baskets kept under "
    "the rules and the reader's verdicts.",
    # round 9 card gate (2026-10-13)
    "F16 A FISH WHOSE EVERY LENGTH IS RELEASED IS RELEASED (`_range_released`): the take-0 bands of the "
    "rules speaking for it, together, cover its whole range on the part — Marshall (Lonzo) Creek's "
    "'Trout/char 20-30 cm' beside Region 2's char 'none under 60 cm' (no char may be kept), the "
    "Coquitlam's 'Hatchery trout 2 (20-30 cm only)' on a steelhead water (no steelhead). Before, the "
    "card said '1' and '2'. An ORPHAN count clause (another table's, still speaking: Region 5's '1 Dolly "
    "Varden' under Polley Lake's 'trout 8', RULINGS Q2) caps the fish's bands like a sub-cap.",
    # owner ruling 2026-10-21 (the Kitimat, ambiguity (b) of pipeline/tests/test_answers_kitimat_steelhead_lift.py)
    "F17 A STEELHEAD QUOTA THAT LIFTS A SIZE CLAUSE RAISES IT ('+1 for steelhead', `raises`): Region 6's "
    "'1 over 50 cm (quota includes hatchery steelhead)' (p.49) lifted for hatchery steelhead by the Kitimat's "
    "'Hatchery steelhead (>50 cm) daily quota = 2' (p.51) holds the other trout/char to 1 over 50 cm and "
    "counts EVERY fish over 50 cm, the steelhead included, toward one tally of the quota's 2 — the Vedder's "
    "'1 over 50 cm (2 hatchery steelhead over 50 cm allowed)' (p.21, Region 2's r2 + r2b), which the owner "
    "named as the same ('look at the prototype vedder it is similar'). Keep 2 hatchery steelhead and no other "
    "trout over 50 cm; keep 1 and 1 other may be. Before, the steelhead left the clause altogether "
    "(`except: [ST]` and nothing else) and 2 hatchery steelhead + 1 cutthroat over 50 cm was legal. The "
    "tally is said as the quota's (a row condition, a group sub-cap, the card's size tier 'Of these, only 1 "
    "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead', the gate's engine and the cap system).",
]
