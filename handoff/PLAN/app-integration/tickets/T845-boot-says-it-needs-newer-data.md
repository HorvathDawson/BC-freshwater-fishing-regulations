# T845 — A first start on data this app cannot read says so plainly

Status: DONE (landed e0434e1b8 with the T810 cutover, CI green 2026-11-18 06:27; 0.8.0).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T845 | A first start on data this app cannot read says so plainly, not "the map data could not be loaded" | DONE | e0434e1b8 | app chat 2026-11-18 (T810 cutover bench) |

Source: app chat 2026-11-18, found by the T810 cutover bench. The 0.8.0 candidate (trout 4+) on the live staging release (trout 1)
failed every start with "The map data could not be loaded. Check the connection and try again." The release order makes
this rare (data release first, then 0.8.0), but the no-silent-fallbacks rule wants the real reason said. The main chat
gave the number and agreed it goes into 0.8.0 if small.

## What changes
- `bootRelease` throws `UnreadableError` (a `BootError`) where the manifest's release is not one this app reads
  (`readsRelease`): the web's start, a first run on a phone, and a phone whose held set is an older app's. The message
  keeps the developer's reason, which format and which file, for the log.
- `useReleaseBoot` says `INSTALL_WORDS.unreadable` for it: "This version of the app can't read the data on the server
  yet. Try again later, or check for an app update." That replaces the connection words, which were wrong for this case.
- Not changed: a release stamped for a newer app (`minAppVersion`) still says "Update the app to get new regulations"
  (T251). An update this app cannot read is still refused in the background, with the held set kept (T403/T555).

## Checks
`app/tools/release-boot.test.ts` (T403 first run: an `UnreadableError`), `packages/ui/src/boot.render.test.tsx` (the
words said, the reason kept), typecheck.
