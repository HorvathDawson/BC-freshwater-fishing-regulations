# T919 — 0.8.3 leftovers

Status: PARTIAL — (a), (b) DONE 2026-11-19 (30e052328, landed at d28344298); (c) bbab1fce1 and (d) a4eb6110c built 2026-11-22 (lane L9); review fixes 984b1bf8f (release check + ensure_pack hold the pack to its folder, `pack.unpacked` counted by checkRoom, a spent pack removed at boot, the no-fallback rule pinned). left (Claude): the device checks below (a first install from the 0.9.0 data release, which is the first to carry the pack; a cut data file resuming on the bg-update bench).

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

## Built (lane L9, 2026-11-22): (c) bbab1fce1, (d) a4eb6110c

**(c) One packed download.** `pipeline/deliver/tiles/assets.py` `write_pack` writes the assets folder again as ONE file
beside it, `tiles/<v>/map-assets-1.tar.zlib`: a ustar tar (assets.json first, then every file sorted, regular files
only, stamps zeroed: the same folder gives the same bytes) in a zlib stream. The manifest names it ADDITIVELY,
`tiles.assets.pack {url, bytes, sha256}` (`manifest.tiles_block`, which writes the pack for a tile version released
before it: a new file beside the folder, no published byte changed). The upload scripts copy the whole tile folder, so
staging and production carry it with no script change; `prod_prune` keeps a tile folder as one unit.
App: a manifest with `pack` puts it among the install's big files (`install.ts` `bigFiles`, file `assets`; the
notification says "Map labels and icons"): queued with the system downloader before the install starts, resumable, on
with the app closed, counted by the room check. Once down (held to the pack's bytes + sha256) `assets.ts`
`unpackAssets` places it: assets.json held to `files_sha256`, every member to its sha256, nothing extra / missing /
doubled, assets.json last; the pack is removed after the swap. When only the tiles move (same assets id), the held
folder is copied as before. **zlib, not gzip:** `fetchVerified` decodes any body starting with the gzip magic, so a
`.tar.gz` failed the byte check on the in-app download path (the boot test caught it) while the phone's disk path kept it.
**Older apps:** the folder stays published and the manifest only gains a key; an installed app reads `url` +
`files_sha256` file by file as before. No minAppVersion change.
**Measured** (live release r-741815c6090d's `map-assets-1`): before, 777 requests (776 files + assets.json), 13.8 MB,
paced at 22/s under the host's limit = at least 35 s of a first install (35-160 s on the benches, above); after, ONE
request of 6,136,203 bytes (-56 %), on the 250 kB/s bench line ~25 s and on Wi-Fi about a second, beside the map files.
Unpacking (inflate + 777 sha256 + 778 writes): ~200 ms in Node (3 runs: 231 / 192 / 197 ms); NOT measured on a phone.
**Tests:** `test_tiles.py` (the pack is the folder, deterministic; a folder that is not its list is refused; the shared
fixture), `test_release_tiles_block.py` (the block names the pack, written when missing); `app/tools/assets-pack.test.ts` (unpack the
pack Python wrote; untar; zlib; every refusal), `release-boot.test.ts` (a first install from a release with the pack:
one request for the assets, every file in place, the pack gone). One committed fixture both sides read:
`app/packages/data/dev/assets-pack-parity.json` (rule 40). Mutations: `bigFiles` without the pack, no member sha
check, no stray check (app); no sha check in `write_pack`, no `pack` in the block (pipeline): each fails a test.

**(d) A cut download resumes.** Checked 2026-11-22 with curl on both data hosts (`water-1-…sqlite.gz`,
`Accept-Encoding: identity`): 200 with Content-Length 194,992 + ETag + Accept-Ranges, and `Range: bytes=1000-` → 206
`bytes 1000-194991/194992`. So the system downloader can resume a cut data file now. The app still gave such a job
up at once (`bgDownload.ts` `fetch`: "cannot resume (the host sends a data file to it with no length)") and fetched it
again from zero in the app. Fixed: a data-file job paused part way (not waiting for a network) is waited on while the
system retries and resumes it; one with no new byte for `cutWaitMs` (90 s: the downloader's first retry is ~30-60 s) is
still downloaded in the app, said in the log. The big files (`fill`) already waited (T760). Tests in
`bgDownload.test.ts` (a cut job resumed and claimed, nothing downloaded in the app; one that never moves again given up
after 90 s, the log line whole); mutations: `cutWaitMs` 0, no give-up, each fails a test.
**Not verified:** neither on a device. (c) needs a release that names the pack on the test channel (the 0.9.0 data
release); (d) the bg-update scenario on the 250 kB/s bench line (the emulator lane was held by other lanes).

State 2026-11-22 (stopped for credits): done: (c) bbab1fce1 + (d) a4eb6110c + test move/throw fix (branch perf/T919-packed-assets);
left: device checks (first install from the 0.9.0 data release, which must carry the pack; bg-update bench for (d));
measurements so far: 777 requests / 13.8 MB → one 6,136,203-byte file; unpack ~200 ms in Node. Untouched by lane L9:
T740 (fix needs native code: MemoryTrimModule has no JS-callable trim, so a hidden-map trim means a native rebuild +
emulator/simulator pairs), T914 (Lighthouse step 0 script prepared, not run), T862 (search already finds aliases and
says "also <name> · <source>"; slice `alias` for the pane = additive pipeline change + mini-release re-cut, not built).
