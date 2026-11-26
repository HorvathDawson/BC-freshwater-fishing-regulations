# T919 — 0.8.3 leftovers

Status: PARTIAL — (a), (b) DONE 2026-11-19 (30e052328, landed at d28344298); (c) bbab1fce1 and (d) a4eb6110c built 2026-11-22 (lane L9); review fixes 984b1bf8f (release check + ensure_pack hold the pack to its folder, `pack.unpacked` counted by checkRoom, a spent pack removed at boot, the no-fallback rule pinned). left (Claude): the device checks below (a first install from the 0.9.0 data release, which is the first to carry the pack; a cut data file resuming on the bg-update bench).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T919 | 0.8.3 leftovers: K_DOWN near-duplicate labels, Elaho p.23 twice, map assets still downloaded, T912 resume check | PARTIAL — (a), (b) DONE; (c) one packed download bbab1fce1 + (d) a cut data file resumes a4eb6110c built; left (Claude): device checks with the 0.9.0 data release | 30e052328, bbab1fce1, a4eb6110c | found by Claude 2026-11-19 |

Small finds that did not block 0.8.3. Batch them into the next release.

- (a) K_DOWN near-duplicate labels: "Lake Revelstoke's Tributaries · its tributaries" and "Chilko Lake's tributary streams'
  tributaries" say the same thing twice. Make each read once.
- (b) Elaho River shows rule book p.23 twice: once under Gear sources and once as the T916 page-foot line. Show it once.
- (c) Map assets (776 glyph and sprite files) are still downloaded in-app. Bundle them or fetch them from our servers once,
  per the self-host rule.
- (d) T912 follow-up: a cut data-file system download restarts from zero. The data worker fix (T912 round 3) is deployed on
  staging AND production as of 2026-11-19, so check that the app can now resume a cut download, then close this or fix it.

## Owner 2026-11-19 20:31 (main chat, question batch)
(c) Picked "One packed download (Recommended)": the 777 map font/sprite files become ONE verified file fetched through
the system downloader like the data files; no app-size change. Not shipped inside the app.

## Done
- (a) `decidedWords.ts` `K_DOWN`: a row whose own name already says tributaries is the kicker alone; `flow.test.ts`. (b)
  `waterBookOf`'s T60 dedupe counts every drawn `srcs:` line; `waterbook.test.ts`. (30e052328)
- (c) bbab1fce1: `pipeline/deliver/tiles/assets.py` `write_pack` → `tiles/<v>/map-assets-1.tar.zlib` (deterministic ustar
  tar in zlib — not gzip: `fetchVerified` decodes any gzip body), named ADDITIVELY in the manifest as
  `tiles.assets.pack {url, bytes, sha256}`; the app (`install.ts` `bigFiles`, file `assets`, "Map labels and icons") queues
  it with the system downloader, `assets.ts` `unpackAssets` checks every member's sha256. Older apps read the folder as
  before; no minAppVersion change. Measured on r-741815c6090d: 777 requests / 13.8 MB → ONE request of 6,136,203 bytes
  (-56 %); unpack ~200 ms in Node, NOT measured on a phone. Parity fixture `app/packages/data/dev/assets-pack-parity.json`.
- (d) a4eb6110c: both data hosts answer `Range` with 206 (curl 2026-11-22), so `bgDownload.ts` now waits on a cut data-file
  job while the system resumes it; no new byte for `cutWaitMs` (90 s) → downloaded in the app, said in the log; `bgDownload.test.ts`.
- Review fixes 984b1bf8f (pack held to its folder, `pack.unpacked` counted by checkRoom, a spent pack removed at boot).

## Work (left)
- (c) on a device: a first install from the 0.9.0 data release (the first to carry the pack) on the test channel.
- (d) on a device: the bg-update scenario on the 250 kB/s bench line (a cut data file resumes).

Noted by lane L9, not this ticket's: T740 (fix needs native code: MemoryTrimModule has no JS-callable trim), T914
(Lighthouse step 0 script prepared, not run), T862 (search already finds aliases; slice `alias` for the pane = additive
pipeline change + mini-release re-cut, not built).
