# T808 — The More tab's "DFO checked" time made every web visit download every salmon file; it now reads only the salmon head

Status: DONE 45fff5e63 (CI green 2026-11-17; regs-open PASS at low load; in 0.8.0).
walk confirms the before and after numbers).

Source: app chat 2026-11-16, found while sizing T796 A6 (the Standard map's finfish closure); the main chat handed it to
the app chat (app code).

## What was wrong
T620 G14 (5aaaaa6ab, 2026-11-10) made `useDfoCheckedAt(p.salmonFeed, loader)` follow the card's pick (`pickSalmon`):
`salmonFeedOf(await loader.salmon())`. `loader.salmon()` with no argument reads the salmon head AND every `salmon-<r>`
shard. The Shell calls it on both platforms once the DFO feed answers, so since 11-10 a web visit has likely fetched every
salmon shard shortly after first paint, whether or not Salmon is ever pressed. The request-walk pin (11-04) predates it,
so nothing caught it. The pick reads only the head (each region's `asof`; `withSalmonFeed` composes the head, never the
slices).

## Fix (2026-11-16 15:14, app chat)
`useDfoCheckedAt` asks `loader.salmon([])`: no regions, so the head only (the loader's `want ? filter : all` keeps `[]`
empty; `salmonFacts` with no slices is valid). `dfoCheckedAt.render.test.tsx`: the hook asks for exactly `[]`. A mutation
back to `salmon()` fails the test. ui tsc clean.

## Left
- Request walk (cold web visit) before and after: which salmon files load, and the measured first-tap bytes. Report both
  numbers to the main chat. The owner raised `first_tap` to 4,400,000 on today's sizes (T796 A5); if this frees room the
  main chat tells him, and nothing is lowered quietly.
- Re-cut `app/tools/request-walk.pin.json` if the walk shows other drift since 11-04.
- Merge with the Android benches like any app change.
