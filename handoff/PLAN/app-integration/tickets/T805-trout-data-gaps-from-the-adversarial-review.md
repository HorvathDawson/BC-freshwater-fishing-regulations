# T805 — Trout data gaps found by the adversarial review of the trout redesign: a wild quota stored per row reads 2 + 2 (Lois, Khartoum), Region 7 possession exceptions on no row, Shamrock Creek's impossible steelhead line, the cross-border quota, Echoes Lakes

Status: OPEN (pipeline answers / curated data; these are in today's app, independent of any redesign).

Source: `handoff/PLAN/design/trout-adversarial-review-2026-11-16.md`, finding 3 and "Data gaps".

1. **Wild quota stored once per row (errs OPEN).** Lois Lake and Khartoum Lake: the book (p.24) says "Wild trout/char
   daily quota = 2"; rule `lois_lake.r1` sits separately on two rows (brook / brown / cutthroat, daily 4; rainbow and
   steelhead, daily 6), each with "wild, daily 2". T793's new card sentences therefore read as 2 wild + 2 wild on
   these two lakes. Store the wild quota once across the rows; the card says "only 2 wild fish in all".
2. **Region 7 possession.** p.58 and p.64: "Lake trout: possession quota = 1 daily quota" (and bull trout). Parsed
   (`possession_exceptions.r1`, `r2`) but no trout row refers to them: possession is blank on about 14,200 Region 7
   water entries.
3. **Possession with a size band**: "Lake trout possession quota = 2 (only one over 50 cm)" (Cunningham Lake, p.59):
   parsed, not found on a displayed row.
4. **Cross-border quota**: "Only one daily quota may be retained … regardless of which side of the border" (Atlin
   p.50, Bennett, Tagish, Teslin, Laidlaw, Swift, Rancheria) is an advisory, not a limit.
5. **Shamrock Creek** `blk:356196726` stores "Up to 2 hatchery steelhead, 20–30 cm (8–12 in)"; the book (p.24, Lonzo
   Creek): "Trout/char 20-30 cm in length Jun 16-Jan 31".
6. **Echoes Lakes** (p.36) "in the aggregate from both lakes" lives only in the "fished elsewhere" sentence.
7. Not traced: whether François Lake's outlet-area char rule is bound to the whole lake.

## Owner, 2026-11-16 15:42: start (step 1 of the trout table build, T810)
"Start steps 1 and 2". This ticket is step 1; the spec's tickets B (possession on the fish row), C (one wild limit
across rows; the char stored one way at Dodd, Lois, Khartoum, Horseshoe, Nanton, Windsor, Ireland) and D (the small
lines: Shamrock Creek, the cross-border quota, Echoes Lakes, Ross Lake's headline, the 8 rule sets with no headline,
François Lake) are its parts: `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md` section 5. Possession stays
the page's own line ("Keep the page line"): the fish row carries possession only where a trout's differs. Every
changed answer goes to him as a list (T809's tool) before it ships. Also from the fourth attack: the Bowron park
possession limit that shows only on days lake trout cannot be kept (Istsi, Wolverine part 1, Sootheran part 1).

## Owner, 2026-11-16 16:09: part C go
Shown the part C changed-answers list (3 waters in Region 2, all toward closed: Lois and Khartoum one wild limit of 2
across both cards; Lois and Windsor wild char hatchery only). Picked: "Go (Recommended)". Commit 0f6fe2868 lands with
the full tier. Parts B and D: their own lists, their own go.

## Owner, 2026-11-16 16:37: parts B and D go; Bowron keep today's; Nation Lakes as printed
Part B "Go (Recommended)"; part D "Go (Recommended)"; Bowron lake trout "Keep today's"; Nation Lakes, typed: "i feel
like this possession stuff is just kind of broken but fixed in the new trout format? so maybe we can just work on the
transisiton to the new trout redisgn?" (both lines stay as printed; no more possession patching here). François Lake
outlet (part D question 3): not asked yet; it needs a placement rebuild and its own list.

## Owner, 2026-11-16 19:09: François Lake
"Outlet area only (Recommended)": all seven lines of the p.51 row sit on the outlet area (map 1); the main lake reads
Region 6's rules. Needs a placement rebuild; changed answers to him first.
