# T253 — background prefetch: the files an unnamed-stream tap reads, fetched ahead on a good connection

Status: DONE (f52055bb, 5b13ab24) — web. Native needs nothing (see below).

Source: owner 2026-10-28 — "tapping an UNNAMED stream is slow the first time (it pulls the lazy files …). Make it fast
by PREFETCHING in the background when it's cheap."

## Built

**Scheduler** `@app/data/release` `background.ts` (`createBackgroundPrefetch`). What: `WEB_BUDGET.first_tap_unnamed`
(the contract's list: core, water, access, regs-<r>, regs-sid, down, salmon, salmon-<r>), per region, only files
the manifest names. Order: the region in view, then its neighbours (`REGION_NEAR`, hand-written and symmetric). Nothing
beyond (owner 2026-10-28: capped at view + neighbours). When the view moves to another region, that region and its
neighbours are fetched, under the same gate; regions already ready stay ready. The shared files come with the first region. Concurrency 1. Each file goes through
`ReleaseLoader.prefetch`. That is the same verified, cached path T231's web `Booted.stage` warms a release through,
and the same one the tap reads (Cache Storage by immutable url, sha-checked). It has no fetch path of its own.
Gate `prefetchHold`: offline, hidden (background), Save-Data, metered without T231's mobile-data setting, low
battery, or slow. Slow means the browser says anything but `4g`. If the browser says nothing, the scheduler measures
its own files and stops below 1.5 Mbit/s. A hold pauses: the file in flight lands, nothing new starts. Clearing the
hold resumes. `cancel()` stops for good (loader change or unmount). A failed file is left for the tap and retried on
the next resume.

**Web wiring** `@app/ui` `prefetch.ts`. `useBackgroundPrefetch` feeds it `navigator.onLine` / `connection` (change
events; `type: "cellular"` = metered, since the web has no mobile-data setting), `visibilitychange`, and
`getBattery()` (≤ 20 % and not charging: the stand-in for battery saver). `useReleasePrefetch` starts it 4 s after
the idle tier (core + wasm keep the network to themselves) and tells it every region the map settles in.
`globalThis.__releasePrefetchBackground = false` turns it off: `request-walk.mjs` sets it, so the walk still pins
a cold visitor.

**Shown.** Updates log (T251): source `Prefetch`, with kinds `prefetched` (file · gz bytes · ms, or "Region N ready"),
`paused` (why), `resumed`, and `failed`. More → Data: a quiet line under Updates, "Offline data: Region 2 ready" /
"Regions 2, 7A ready" / "every region ready" (`offlineDataLine`). Nothing is shown while no region is ready. Shot:
`../T253-shots/phone-more-offline-line.png`.

**Native.** A phone installs the whole release (install.ts), so an unnamed tap reads files already on disk and there
is nothing lazy to prefetch. The gate already models metered + `allowMetered` (T231) and is tested, but it is not
wired on native.

## Measured (2026-10-28, `node app/tools/prefetch-measure.mjs --url http://localhost:39218 --reps 3`)
Web dev build on :39218 over the local release on :39217, headless Chrome 390×844, DevTools "Fast 4G" throttle (9 Mbit/s
×0.9, 165 ms). The tap is the request walk's unnamed stream (Region 2). Measured from the tap to the Regulations
header on screen:

| | tap → regs shown (3 runs) | the tap fetched |
|---|---|---|
| cold (background off) | 5,379 ms median (5,340–5,382) | 4.79 MB: water, down, access, regs-sid, salmon, salmon-2 |
| prefetched | 245 ms median (230–251) | nothing |

Bytes by default for the Chilliwack view: 4.79 MB (gz) until Region 2 was ready (≈5 s at Fast 4G, starting 4 s after
idle). The default total (view + neighbours, capped) is 6.15 MB in 14 files, beyond the idle tier's core + regs-2; Regions
2, 1, 3, 5 and 8 are ready. Before the cap, the whole province was 7.70 MB in 22 files. Chrome kept `effectiveType: 4g`
under this throttle, so there were no holds.

## Tests
`packages/data/src/release/background.test.ts` (contract list, order, neighbours symmetric, the gate table, start /
view / concurrency / done, pause-resume, Save-Data / metered / offline / battery never start, metered + setting goes,
cancel, failure left then retried, measured-slow pauses; the real web source + cache: every file once, the tap's opens
fetch nothing). `packages/ui/src/prefetch.render.test.tsx` (view per region, start only after idle + delay,
scheduler per loader, cancelled on unmount, kill switch, `webNetSignals`, `batteryLow`). `core/src/updates.test.ts`
(the More line, the Data block, log rows). `tsc -b`, boundaries, platform, deps, knip all green.

## Left
- The set is centre + neighbours. The map reports only the region under its centre; a viewport-wide list
  would need the map to report every region it shows.
- On an unknown-speed connection a "slow" measurement holds for the session (until the connection changes).
- The full vitest run has failures that predate this ticket (goldens, the live release moving under the release lane,
  other lanes' words). None of them is in a touched file.

Depends: T186, T182, T231, T251. Size: M. Owner: none.
