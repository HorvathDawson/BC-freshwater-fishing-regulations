# T829 — Salmon species rows in the same look as the other fish table and the new trout table (formatting only)

Status: DONE 45fff5e63 (CI green 2026-11-17; batch-1 Android benches: install, crash-bed, walkthrough, memory, regs-switch PASS; regs-open PASS 2/2 at low load; in 0.8.0).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T829 | Salmon species rows in the same look as the other fish table and the new trout table (formatting only) | DONE | 45fff5e63 | owner 2026-11-16 (app) |

Source: owner 2026-11-16 (verbatim): "Can we also clean up the format for the salmon rows to make it look more
consistent with the other table and the new trout table just a formatting thing same data tho".

## What
The salmon card's species rows (fish picture, name, limit, sizes, source line, Open/Closed) restyled to match the
settled trout table look (trout mockup page round 15, https://claude.ai/artifact/EopBgDtf8eu4LGZpYixSkH; the accepted
design list at the end of T810) and the other-fish list: same row rhythm, the same bigger "rule today" / smaller
"coming change" text sizes, the same status words and colours, the same expand arrow. SAME DATA: no new field, no
word composed in the app (owner 16:50 rule), no pipeline change. First: mockups of two real cards (e.g. the
Kitsumkalum with its eulachon closure line, and a mixed open/closed water like the Skeena below Terrace) beside the
trout table, for his pick. Build after the salmon landing and T828 (they touch the same SalmonCard code).

## Owner pick, 2026-11-16 (app chat dialog, after the mockups)
Three options were mocked on real data (Kitsumkalum Jul 15, Skeena below Terrace Aug 15, each above its trout table;
scratch t829/sheet-A/B/C.png). He picked, verbatim label: "B: trout keep box (Recommended)": open species in the tint
box with each limit in the white box on the right, closed species, the eulachon line and rule-book closures in the trout
"✕ Closed" group under the box; the badge, year bar and bait-ban line stay; T828 fold as mocked. Build after the salmon
landing and T828, sharing the trout table components (ticket O).

## Built (2026-11-17, branch `app-fix/t828-t829-salmon-card`, on top of T828)
- NEW `app/packages/ui-native/src/regs/KeepBox.tsx`, the component ticket O adopts: `KeepBox` (the tint block, no outline:
  strip with head / filled label / strip / tag / notes / strip date, rows on the card colour with the tint rail, the right
  band: a white box per `own` / `shared` / `possession` band spanning its rows, the free words of an `upto` band per
  row, band dates small under them) and `ShutGroup` (release / closed, and `other` for salmon's "Check DFO" / "Varies":
  faded pictures, ONE column word with its mark ↩ / ✕). Props are the contract's fields: `KeepRowVM` = `Pick<TroutRow,
  key | name | label | sub | lines | quiet>` + the DateSet row text + the picture; `KeepBandVM` = `Pick<TroutBand, from |
  to | text | kind>` + its date; `KeepBox` takes `Pick<TroutBox, label | strip | tag>` + head (core re-exports the
  generated `Band` / `Box` / `TRow` as `TroutBand` / `TroutBox` / `TroutRow`). Two text sizes (now: `TYPE.legend` ink;
  next: `TYPE.meta`, `sub`); tokens only; picture presses 44 × 44 (`RegsPress`); each row one label (`role="group"`),
  its texts joined in payload order; at OS text ≥ 1.5× the band folds into bold lines under the rows (a shared band's
  rows on one left rail) and the column word moves above its group.
- `SalmonCard.tsx`: open species in the box (strip = the bucket line, its period limits and a water system's limit as
  notes; each species' quota in its white box with the quota's dates under it; an open species with no number of its
  own shows its status word free); then Check DFO / Varies groups, the release group, and the "✕ Closed" group: closed
  species, then `card.closures` (the eulachon line, mark tile). The badge, chip, year bar (Keep tab) and bait-ban line are
  unchanged. Sizes and "where from" are each row's quiet lines. No new field, no pipeline change; the only app marks are
  ↩ (new `WORDS.RELEASE_MARK`) and ✕ (`WORDS.CLOSED_MARK`). The "Species today" kicker is gone (mockup B has none).
- Tests: `KeepBox.test.tsx` (strip, band groups, two sizes, row labels, 44 px, large-text fold, groups, contrast in light /
  dark / cvd); `SalmonCard.t829.test.tsx` (box holds the open species with their limits; Closed group holds the closed
  species and the eulachon line; badge, bait ban, the Keep tab's year bar; every drawn word is a payload or words value;
  a11y); `tools/salmon-keep-layout.test.ts` (headless Chrome, 360 / 390 px at 1× and 2×: no sideways scroll, no cut or
  crossing lines, names column ≥ half the card at 2×, with a never-fold mutant that must fail); the salmon tests re-pinned
  to the new groups. Mutation-checked (10 mutants, all killed).
- Shots (web, 390, light + dark, before / after): scratch `t829-shots/`. The LATEST release on this machine
  (r-46e4355a7277) predates the T770 closures, so the eulachon words show there as DFO's gear line under the groups; on
  a release with `closures` they are the Closed group's last row (as the tests pin).
- Left: the owner's Android bench; ticket O adopts `KeepBox` for the trout table.
