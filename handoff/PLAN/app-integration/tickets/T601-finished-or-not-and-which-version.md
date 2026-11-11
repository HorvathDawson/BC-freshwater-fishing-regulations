# T601 — Every reader knows a build is finished and which version it reads

Status: OPEN — item 3's format tag built in T557 (31ee818a0); items 1, 2, 4 and "each feed names its release" left

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
