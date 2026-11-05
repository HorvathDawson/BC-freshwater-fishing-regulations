# Ticket audit, 2026-11-05 (owner asked: what else needs doing, combine what fits, keep deferred deferred)

418 tickets: 330 done, 7 dropped, 81 not finished. Below, every unfinished one is placed once. **Proposals until the
owner answers.** "Merge into X" = X absorbs the leftovers, the merged ticket closes pointing at X.

## Deferred — unchanged (owner rule)
T383 time zones · T389 unnamed-reach links · T304 water-page region focus · T401 pan buffer · T307/T308/T310 sign-offs ·
T351 native crash (waits for a 0.5.0+ report) · postponed: T79 imagery corridor, T281 lake in-part areas, T319 store
screenshots.

## Already on a chat list
- **Data and build (this chat):** T324 (running), T416, T412, T413; owner-gated T164, T326; T178 (2026-12-01).
- **Launch and hygiene (next):** T86, T240, T244, T409, T371, T305, T306, T331, T341, T402, T333–T335, T404, T410.

## Proposed merges
| new / surviving ticket | absorbs | why |
|---|---|---|
| **0.6.1 app batch** (T411 + ) | T417 watershed flicker, T418 style-asset errors | the next APK; one phone check |
| **T412 slow feeds** | T199 run-timing feed, T225 "where feeds live / triggers" | T408 decided both (no GitHub schedules, worker/button) |
| **T409 SEO launch** | T188, T196, T405 (and T198 stays owner-blocked inside it) | one water-page speed + search pass before the prod SEO launch |
| **T88 iOS** | T363, T364, T365 | all wait on the Apple account; one ticket to start when it exists |
| **T360 performance pass** | T245 low-end Android, T287 download freeze, T317 map-engine audit | one measured pass on the same emulator method |
| **T410 app structure** | T258 accessibility leftovers, T260 shared components | same files; one code-hygiene pass |
| **T242 reports** | T291 test-tagged reports, T407 (verify done on 0.6.0) | the report/crash pipeline as one |
| **T239 next book** | T414 review filter, T415 Heber regression | both needed before the next synopsis, not before |
| **T240 launch checklist** | T50 emulator parity, T75 basemap in manifest, T80 small open items, T87 sign-off walk | old umbrellas; their leftovers become checklist lines (verify each first) |

## In review — close them (Claude checks each, one pass)
T252 donor paths (Pitt check), T292 sewage/tailings, T293 walkthrough, T315 offline gauge week, T354 PDF, T366 DFO live
refresh, T55 sloughs (T328 done — likely closable).

## Not on any list — features (owner picks which to add)
| ticket | what | size |
|---|---|---|
| T218 | bathymetry: depth + contours for the 28 already-digitised lakes (open data) | M |
| T201 / T219 | bathymetry Depth mode / trace contours from scanned charts | L |
| T200 | stocked lakes: feed the Stocked mode | M |
| T234 | notifications v1.1 (reader-set alerts) | M |
| T390 | in-app "update available" (partial) | S |
| T259 | offline map by region | L |
| T223 | user accounts shared web ↔ phone | L |
| T250 | Expo SDK / dependency upgrade window | M |
| T261 | run timing v2 leftovers | S |

## Owner decisions (2026-11-05) — applied
- **Merged** (absorbed tickets marked `MERGED into X`, survivor carries a "Merged in" note): T412 ← T199, T225 ·
  T409 ← T188, T196, T405, T198 (T198 stays owner-blocked) · T88 ← T363, T364, T365 · T360 ← T245, T287, T317 ·
  T239 ← T414, T415 · T240 ← T50, T75, T80, T87 · T410 ← T258, T260 · T242 ← T291, T407.
- **0.6.1 batch:** T411 + T417 + T418 (kept as separate tickets; one APK when the owner says "build").
- **Out of deferred, onto the lists:** T319, T304, T401, the T307/T308/T310 sign-offs, T351.
- **After 0.6.1:** T390.
- **Hygiene pass:** T410, T242, T250, T261 (+ the in-review closing pass).
- **Soon:** T409, T88 (when the Apple account exists), T360, T239 (before the next book), T240.
- **Deferred:** T218, T201, T219, T200, T234, T223 (+ the earlier T383, T389, T79, T281).
- **Dropped:** T259 offline map by region (owner: saves little space, not necessary).
