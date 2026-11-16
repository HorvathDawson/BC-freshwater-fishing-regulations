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
