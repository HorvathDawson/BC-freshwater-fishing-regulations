# T1020 — answers_diff: 28,714 licence frames differ on `alien_closures[].rule` across the 0.8.4 → production release pair

Status: OPEN.

Source: T809's cutover diff run, 2026-11-22 (lane L13): r-fec3fab4c5aa → r-741815c6090d, same atlas. One "other" group
holds 28,714 sections (314 waters, 26,921 unnamed stretches): path `alien_closures[0].rule` (and `[1].rule`), before and
after both reading as no licence line. Example: Ace Creek (`gnis:36488`), section 360804586:0.
## Work
1. Decide which it is: the tool does not resolve an `alien_closures` rule ref the way it resolves gear and licence refs
   (a tool gap: fix `answers_diff` so rule ids compare), or a real change in what the stored licence frame holds (then
   say which release change did it, T789 / T973 / the packed formats T686).
2. If real: say what a visitor sees (nothing, if both print no line) and whether it needs the owner.
## Acceptance
- A rerun of the same pair shows the group explained (resolved to ids and equal, or named as a real change), with a test
  that plants the difference and sees it reported once.
Depends: —. Size: S. Owner: no.
