# T912 — A regulations data update stops downloading when the app is closed

Status: DONE — shipped in 0.8.3 (merged 6a0545499, 4ebd13ecc; staging + production 2026-11-19).

Owner 2026-11-19 ~10:50 (main chat), verbatim: "also iff i close app it doesnt continue to downlaod the regulation data
updates." (He is on Android, Play 0.8.2 build 37; promoting it to closed testing for himself the same minute.)

## Work
T298 meant the system's downloader to carry the big files in the background (`background (T298): the system's
downloader carries the big files`, tests in packages/data). Reproduce on the emulator with the Play-like build (patched
MapLibre): start an update, swipe the app away / press Home, and watch whether the download job keeps going (adb,
the downloader's logs) and whether the next open picks up the finished files or starts over. Find where the job is
tied to the app process (JS-side fetch instead of the system downloader, a foreground-only task, a job cancelled on
destroy) and fix it so a closed app keeps downloading and the next open applies it. Tests + an emulator walk.

## Outcome (lane app/t912-bg-download, 2026-11-19)
**Cause.** T298 handed the system's downloader (Android DownloadManager) the FIRST install's big files only. An update
(`Booted.stage` → `install`) fetched its changed data files in the app first, one by one, and queued each big file
(tiles, mask, basemap, rule book) only when the install reached it. Closed in that window (the whole update, when it changes
only data files), nothing was queued: the process died and every download with it; the next open started the update again.
Reproduced on the emulator with the code before the fix (T903 lane's APK, release/update code identical to 264d78c5e):
`bg-update` — app killed the moment the update was downloading: 0 system jobs queued, none started in 60 s after the kill;
the reopen downloaded it again in the app.

**Fix.**
- `boot.ts` `stage`: before the install starts, every file the update downloads is queued with the system's downloader
  (`updateItems`: the changed data files first, then the big files); a failed queue is said + reported and the install
  goes on in the app.
- `bgDownload.ts` `fetch` (new): a queued data file is taken from its job — claimed beside its install path, unpacked if
  gzip, sha-verified — and the install places it. No job (a first install's data file, unchanged), a job that failed,
  vanished, stalled `stuckMs`, brought other bytes (also reported) or was CUT part way (paused, not for a network: the
  downloader must ask for unencoded bytes, which the data host sends decoded with no length/ETag, so it cannot resume —
  seen on the emulator at 93 %) → downloaded in the app, the log says why (W46).
- The update service passes its mobile-data rule to the stage (`{ metered }`: a big update without "Always" is queued
  Wi-Fi only), so a download that goes on after the app is closed does not switch to mobile data against the setting
  (before, the lazily queued big files always allowed mobile data).
- Notification line for a data file: "Regulations data · N MB" (`DOWNLOAD_NOTE_WORDS.files.data`).
- Unchanged: the first install (T298, bench-proven) keeps its data files in the app.

**Tests.** `bgDownload.test.ts` (fetch: claimed + verified gzip or plain, not queued → in app, failed / other bytes /
cut / stuck → in app with the words, network wait waited on), `service.test.ts` (the metered rule, mutation pin),
`tools/release-boot.test.ts` (an update over a fake system downloader: all 4 files queued Wi-Fi only before the app fetched
any — mutation pin fails without the queue — the app "closed", the system finishes, the next open claims all 4 with
nothing fetched in the app, the launch after applies). `test-updates.test.mjs` (data-book). `pnpm -C app check:fast` green.

**Emulator** (test bed `bg-update`, new; `test-updates.mjs data-book`: the live `climate` with its SQLite change counter
moved + the rule book with a trailing PDF comment, both under test/; `big`'s basemap-id move downloads nothing — the phone
keeps the file at that path). Fixed APK: both system jobs queued before the kill, "2 files downloading" in the shade with
the app dead, both started and finished with the app not running (line held to 250 kB/s). Final run (APK of 9defa2447,
clean, patched MapLibre) 7/7 PASS: killed 10:20:24 with 2 jobs queued → both finished by 10:21:18, app not running →
reopen claimed `climate-…sqlite.gz` and the rule book from the system (nothing downloaded again) and staged → applied at
the next pause (`release r-379e72fc9524-book4538` in use) → reset to the live release. An earlier fixed run found the data
file's job cut at 93 % and parked "waiting to retry" for good (no length from the host → no resume); fixed in 9defa2447
(the app fetches such a file at once, said). Run dirs: lane scratchpad `t912/before` (repro), `t912/after2` (pass).

**iOS (not done; iOS work waits).** The same JS runs there over the T722 background URLSession module, so an update's
files are now queued with it too: they go on with the app in the background or suspended; iOS cancels them when the app
is swiped away in the app switcher and the next open resumes them (resume data). Not built or walked on a device.

**Not done / open.** Jobs of an update that is abandoned before it is claimed (a newer release supersedes it) stay in the
downloader's folder until the next update of the same file (pre-existing for big files, T298). A cut data file is
fetched again from zero by the app (small).

## Round 2 (owner 2026-11-19 ~11:31, via the main chat): one download path
Owner, verbatim: "pleasse unify these" — the first install's data files still downloading in the app, and the big files
going to the phone's downloader.

**Done (round 2).** `queueItems(m, held | null)` (was `updateItems`) is the one list for every phone install, through one
`handOver` in `boot.ts`: the first install (its data files now go to the system's downloader too, `held` null), an older
app's set installed over (T403: queued after its mobile-data gate, Wi-Fi only when the reader waited for Wi-Fi), and an
update. checkRoom / mobile-data order unchanged. Pinned in `tools/release-boot.test.ts` ("the FIRST INSTALL takes the same
path"): a big-only first-install queue (the T298 list) fails it. Map assets (776 glyph/sprite files) still come in the app
(not part of this; ~35–160 s on the benches, paced by the host). Commits 32c6378a7 (code), f02307630 / 062052af6 (iOS bench).

**Android** (emulator lane 2, APK of f02307630, dirty only in a host-side iOS bench file): `bg-update` 7/7 — 2 jobs queued
before the kill, finished with the app dead, claimed at reopen, applied (the data file's job was cut part way once more on
the 250 kB/s bench line and fetched by the app, said — note below). `bg-install` 5/7: a clean first install on Wi-Fi started
8 system jobs at once, data files included; every data file and big file "done (system download manager)"; app killed,
"4 files downloading", notification tap opened it, nothing queued again from zero, map up. The 2 failures are the bench's
screen read coming back empty while the mobile-data prompt was on screen (screenshot 001 shows it; the app's log has
"mobile data: wifi" then "go"), not the app.

**iPhone** (simulator, iPhone 17 / iOS 26.5, build of 32c6378a7, lane 3; Home then `simctl terminate`, NOT a swipe away —
iOS cancels a force-quit app's transfers by design): `bg-update` steps 1–4 passed on runs 4–7. The data file went through
the background URLSession and was claimed in ~1 s (before the close); the rule book (padded to 312 MB) was at 106–144 MB
when the app was ended; 4–12 s later iOS relaunched the app in the background for its session ("client was launched for
background session"), the transfer finished there and the app's JS claimed, verified and STAGED it in that background
launch (log: staged 313.8 MB, then applied at the next open: run 4 11:55:12 / 11:55:16, run 7 12:31:49 / 12:31:53), with
no file downloaded in the app and nothing queued again. The scripted reopen/apply/reset steps (5–7) fail on a bench
navigation problem, not the app: the Developer card left open at the close comes back up and is missing from the
accessibility tree, so the flow that opens the Updates log fails on the map's chip. The screenshots (`r2-ios4/005`,
`r2-ios7/005`) show the log with "Release staged" and "applied" for the test release. The first install on the simulator
also took its data files from the background session ("water…: done (system download manager)").

**Note, not fixed (main chat, 2026-11-19): a data file's system download cannot resume after a cut.** Android's
DownloadManager always asks `Accept-Encoding: identity`. The data worker passes a gzip-stored file through as stored, with
`Content-Encoding: gzip` (`encodeBody: "manual"`, deploy/staging/data-worker.js), and Cloudflare's EDGE then decodes it for
that client and sends it chunked, with no Content-Length and no ETag (checked with curl: identity → chunked, no length;
gzip → `Content-Length` + ETag, and Range works). So the length is dropped at the edge, not in R2's headers. A cut job then
parks "waiting to retry" for good; the app fetches that file itself (9defa2447). Seen on the bench's 250 kB/s line at
~94 % twice; never on the full line or on iOS. Fix (~1–2 h with a worker test): in the data worker, a request whose
Accept-Encoding does not allow gzip gets the stored gzip bytes WITHOUT the Content-Encoding header (octet-stream), so the
edge passes them with their length and ETag (resumable); the app already unpacks a gzip body (`verifyAsync`). Both data
workers (staging, and production's — owner deploys prod). Browsers and the app's own fetch send gzip: unchanged.
Run dirs (lane scratchpad `t912/`): `r2-android`, `r2-ios4`, `r2-ios7`.

## Round 3 (2026-11-19 ~12:50): the data worker serves a cut data file resumably — NOT DEPLOYED
**Done** (395779f2d, `deploy/staging/data-worker.js`; production's `deploy/prod/data.wrangler.toml` runs the SAME file
(`main = "../staging/data-worker.js"`), so the two cannot differ). A gzip-stored file (`Content-Encoding: gzip` on the R2
object: the release's `*.gz` data files, the run-timing feed) asked for by a client that does NOT take gzip is sent as the
stored bytes WITHOUT `Content-Encoding`: `Content-Type: application/octet-stream`, `Content-Length` = stored size (the
slice's on a 206), the R2 ETag, `Accept-Ranges`, Range → 206 + `Content-Range`, `Vary: Accept-Encoding`; HEAD the same.
**Which Accept-Encoding:** on the edge a worker sees `Accept-Encoding: br, gzip` on EVERY request; the client's own value is
`request.cf.clientAcceptEncoding` (Cloudflare docs, fundamentals/reference/http-headers). The worker reads that, the header
only where `cf` does not carry it (tests, wrangler dev); none at all = no gzip (as the edge treats curl). gzip / x-gzip /
`*` with q > 0 takes gzip. A client that takes gzip (browsers = the web, the app's own fetch on Android (OkHttp sends gzip),
iOS URLSession) gets exactly what it got before: no header changed, no Vary added. Files not stored gzip: untouched.
**The app needs no change.** Both phone paths end in `verifyAsync` (`packages/data/src/fetchVerified.ts`): `ungzipAsync`
unpacks a body that starts with the gzip magic and passes a decoded one through, then length + sha256 against the manifest.
The system download is claimed and read whole into it (`bgDownload.ts` `fetch`); the in-app fetch (`fetchVerifiedResilient`)
verifies the same way and resumes only a body that arrived still gzip (`isGzip(head)`), which is now the consistent case for
a Range request. The web: the browser takes gzip, unchanged. Python readers: `pipeline/runtiming/feed/diff.py` already
unpacks a gzip body; the others read JSON that is not stored gzip.
**Not changed (follow-up, optional):** `bgDownload.ts` `fetch` still gives up at once on a data-file job paused for anything
but a network wait and fetches the file in the app (said in the log); with this worker live on BOTH hosts, that job could be
left to resume like `fill`'s big files. Not done now: the Play build reads production's data host until the owner deploys
prod's worker, and there a cut job still cannot resume. The comments there ("the host sends a data file to it with no
length") describe the hosts until the deploy.
**Tests.** `app/tools/data-worker.test.ts` (4 new): gzip taken (cf `gzip, deflate, br, zstd` / `gzip` / `*`, header-only
`gzip`) → encoded as before, no Vary, no worker-set length; not taken (cf `identity`, `""`, `gzip;q=0, identity`, `br`,
header `identity`, none) → stored bytes, no Content-Encoding, Content-Length 100, ETag, Vary; Range `bytes=40-` → 206,
`bytes 40-99/100`, length 60, the slice; `bytes=10-19` → 206 the slice; HEAD → 100 unencoded; a JSON file untouched.
Mutation checks: the old worker → 3 of the 4 new tests fail (the gzip-taken one passes, as it must); a worker reading only
the header (not `cf.clientAcceptEncoding`) → fails. `pnpm -C app check:fast` green (no data/release link).
**Release step (not run by the lane; neither worker deployed):**
- Staging (main chat): `npx --prefix app wrangler deploy -c deploy/staging/data.wrangler.toml`; then check with
  `curl -sS -D- -o /dev/null -H 'Accept-Encoding: identity' https://data-staging.canifishthis.ca/release/<rid>/climate-<hash>.sqlite.gz`
  (expect no Content-Encoding, Content-Length = the object's size, an ETag) and the same with `-H 'Range: bytes=1000-'`
  (expect 206); with `-H 'Accept-Encoding: gzip'` the headers as before (Content-Encoding: gzip).
- Production (THE OWNER): `npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml` (rehearse with
  `--dry-run --outdir <tmp>`), then the same curls against `https://data.canifishthis.ca`.
**Not verified:** the edge's behaviour live (no deploy): that `cf.clientAcceptEncoding` holds DownloadManager's
`identity` as the docs say, and that the edge then passes the unencoded octet-stream with its length and ETag; a cut system
download actually resuming on the 250 kB/s bench line (needs the staging deploy + an emulator run).
## Staging data worker deployed (main chat, 2026-11-19 ~12:52; version 66203663, from the 0.8.3 integration branch)
Live check on data-staging, `access-859b43cb2641.bin.gz`: before, `Accept-Encoding: identity` got the decoded body with
no Content-Length and a weak ETag; after, it gets the stored 201,515 gzip bytes with Content-Length and the strong ETag,
and `Range: bytes=1000-` gets 206 `bytes 1000-201514/201515`. `Accept-Encoding: gzip` is unchanged. The first requests
in the seconds after the deploy still came from the old version. Production's data worker (same file) is the owner's
deploy: `npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml`, with his 0.8.3 production steps.
