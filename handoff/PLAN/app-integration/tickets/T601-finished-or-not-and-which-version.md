# T601 — Every reader knows a build is finished and which version it reads

Status: DONE — items 1, 3, 4 in their own tickets (1db9d5d8a, 31ee818a0, 2f09829fe), item 2 Python half 63a3e6173, JS half ff787ac34 (confirmed 2026-11-12)

Source: T599 findings step 3 (§1 "where a reader guesses a path or a vintage", §5); owner accepted 2026-11-10.

## Work
1. The atlas writes a COMPLETE marker last; promote is safe (never a half-written atlas).
2. One `latest()` for every Python and JS reader of "the live release"; no modified-time fallback.
3. A format tag on the gauge feed; each feed names the release it belongs to.
4. One writer for `clim.json`.
Overlaps T549, T555, T557, T585: do each item in whichever of those tickets already owns it and close the rest here.

## Acceptance
- No reader picks a file by modified time or by guessing a path; a half-built atlas cannot be promoted or released.

Depends: T547; overlaps T549, T555, T557, T585. Size: M. Owner: no.

## Consistent with today's builds (owner 2026-11-10: "need to make sure it is consistent with the builds now")
This step changes where and how data is written, never what is built. Before it starts, keep the current atlas,
bundle, tiles and release as the reference. After it, one full build must give the same outputs: same release file
hashes (or a diff where every difference is explained and expected, e.g. the new input digests in a meta file), same
tiles, same app and web packages. The build, release and staging commands the owner uses keep working unchanged, or
the change is listed in the ticket and in AGENTS.md. A step that cannot show this is not done.

## Done 2026-11-10 (in T557 / T585)
- Item 3: the gauge feed carries `gauge-feed/1` (31ee818a0). It names its HYDAT (`hydat.release`, as before); it names no
  data release (stations are keyed by id and the feed is release-independent) — the app-side comparison of its HYDAT
  with the release's is listed in T585's Left.

## 2026-11-12 (pipeline lane): where each item landed
- Item 1: done in T547 §4 (1db9d5d8a): the atlas writes `COMPLETE.json` last; `pipeline.atlas.promote` refuses a
  candidate without it.
- Item 2: Python half done (63a3e6173, `manifest.live`, see T560 item 6); the JS readers (`app/tools/serve-tiles.mjs`,
  `app/tools/regs-recut/sync.mjs`) still have their own, in the app wave.
- Item 3: T557 (31ee818a0).
- Item 4: T554 item 4 (2f09829fe) kept the two writers on purpose and put a guard on the upload (`clim_guard`: never an
  older HYDAT over a newer one); that is this item's answer, closed here.

## 2026-11-12 (app wave): item 2's JS half confirmed done
`app/conformance/src/latestRelease.mjs` (ff787ac34) is the JS readers' one `latest()`: `<root>/LATEST` names the release, no
pointer is `null` (the caller says "run the release step"), a pointer naming a folder with no manifest throws. The three
node tools use it: `serve-tiles.mjs` (`RELEASE=<id>` if asked, else LATEST; no pointer is a 404), `regs-recut/sync.mjs`
(no pointer: "needs a release", exit 1), `label-shots.mjs`; the conformance suites' `fixtureRelease.ts` too. Pinned by
`tools/latest-release.test.ts` (mutation: LATEST wins over a newer folder). This lane fixed the two comments that still
said "newest" (`serve-tiles.mjs`). Same rule as the Python `manifest.live`.
