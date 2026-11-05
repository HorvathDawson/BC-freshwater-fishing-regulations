# T403 — A 0.5.0 phone ran on 0.4.0's data release (r-4e709418ebd8) and its readers threw

Status: DONE 2026-11-05 — 62d96721d (the fix) — staging needs no republish.

Evidence (staging crash store, 2026-11-05 00:00–00:01 UTC, owner's phone, 0.5.0 build 8 9813980, android 37, one
install, release r-4e709418ebd8, non-fatal JS):
1. "salmon: not a salmon/4 file" (decodeSalmonHead, route map).
2. "place: this release has no section_line, so the part on screen cannot be put in its address" (midOf, gnis:19986, regs, part 2).

What r-4e709418ebd8 is: the staging release of 2026-10-31 (0.4.0 era; pruned locally 10-31, kept as "live on staging"
then). It predates salmon/4 (T356, 11-03) and the `lines` file (T369, 11-03).

Cause — the app, not staging: staging `release/manifest.json` is r-5a5999c94588 (minAppVersion 0.5.0, published 11-04),
the same as local LATEST; the APK bundles or seeds no release. A phone opens its HELD set first (boot.ts); the 0.4.0 app
had installed r-4e70, the APK update kept it, and 0.5.0 opened it. The newer set is only fetched in the background (the
update service: Wi-Fi for a large set) and used at the next launch/pause, so 0.5.0 ran for a while on data whose formats
its readers do not read. Nothing compared a held set's formats with the app's.

Fix (62d96721d):
- `readsRelease(m, "web" | "phone")` (data/manifest.ts): every file the manifest names that the app's contract knows is
  in a format its reader takes (`PACK_FILES`; paint/4 too, `PAINT_READS`); a PUBLISHED release names every contract file
  but `PACK_OPTIONAL` (lakes) and, on a phone, `PACK_WEB_ONLY` (the slices). Unpublished sets (fixtures, local builds)
  are partial and held to formats only. `PACK_OPTIONAL` / `PACK_WEB_ONLY` are emitted from contract.py (emit.pack).
- boot.ts: a held set the app cannot read is never opened — the manifest's set is installed over it before the map
  (stages manifest → install → paint, the mobile-data gate asked), the old set pruned once the new one opens; offline it
  fails with the first-run "needs a connection" screen; an app too old for the manifest gets the update prompt. A
  manifest the app cannot read is never installed (first run fails saying why; an update is refused with the reason,
  which the update service reports), never opened (openSet) and never rolled back to.
- Tests: data/src/manifest.readable.test.ts (7), tools/release-boot.test.ts (+2: held set made old → installed over;
  salmon/3 manifest → refused first run + update).

Note: the offline case reuses the first-run words ("The first start needs a connection…"); App.tsx was not touched
(another lane holds it). Owner: on the phone, nothing to do — the fix ships with the next APK.
