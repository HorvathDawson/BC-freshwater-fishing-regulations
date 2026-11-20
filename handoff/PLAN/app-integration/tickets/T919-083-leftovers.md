# T919 — 0.8.3 leftovers

Status: IN PROGRESS: (a), (b) DONE 2026-11-19 (30e052328, landed at d28344298); (c) one packed download (owner 20:31) and (d) next.

Small finds that did not block 0.8.3. Batch them into the next release.

- (a) K_DOWN near-duplicate labels: "Lake Revelstoke's Tributaries · its tributaries" and "Chilko Lake's tributary streams'
  tributaries" say the same thing twice. Make each read once.
- (b) Elaho River shows rule book p.23 twice: once under Gear sources and once as the T916 page-foot line. Show it once.
- (c) Map assets (776 glyph and sprite files) are still downloaded in-app. Bundle them or fetch them from our servers once,
  per the self-host rule.
- (d) T912 follow-up: a cut data-file system download restarts from zero. The data worker fix (T912 round 3) is deployed on
  staging AND production as of 2026-11-19, so check that the app can now resume a cut download, then close this or fix it.

## Built (main chat, 2026-11-19 evening)
- (a) `decidedWords.ts` `K_DOWN`: a row whose own name already says tributaries ("Lake Revelstoke's Tributaries", "Chilko
  Lake's tributary streams") is the kicker alone. `flow.test.ts` pins both names.
- (b) Cause: the Gear section's one source line (`SourceLineVM.pages`, drawn by `SourceLine.tsx` as "Rule book p.N ↗")
  was not counted by `waterBookOf`'s T60 dedupe, which only saw `{t:"book"}` runs. It now counts every drawn `srcs:` line
  (not the per-row `srcs:gl|…` ones, undrawn since T900). `waterbook.test.ts` pins both. Core regs + WaterBook +
  WaterScreen vitest: 690 passed.
- (c) Measured (live release's `map-assets-1`): 777 files, 13.8 MB; 769 are font glyph ranges (every Unicode block of each
  font stack), 8 are sprites (~100 KB). They already come from our own server (self-host rule met) and are fetched once per
  asset digest (an update whose assets did not change fetches nothing). The cost is the FIRST install: 777 requests paced at
  22/s (the host's 30/s limit) = the 35–160 s seen on the benches, outside the system downloader. Owner question (batched),
  default: pack the folder into ONE verified file that goes through the same system-downloader path as the data files
  (no app-size change, one request); the alternative is shipping it inside the app (+~14 MB, or less if the glyph ranges
  are cut to the characters the tiles' names use).

## Owner 2026-11-19 20:31 (main chat, question batch)
(c) Picked "One packed download (Recommended)": the 777 map font/sprite files become ONE verified file fetched through
the system downloader like the data files; no app-size change. Not shipped inside the app.
