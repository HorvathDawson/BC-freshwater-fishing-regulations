# T418 — Map style assets failed to load on the phone (sprite, glyphs, the `outside` source)

Status: DONE in code 2026-11-05 — 370731ad3 (0.6.1 batch; phone check with the next APK). Found reading reports
2026-11-05: every 0.5.0 report from 11-04 17:07 PST carried the same MapLibre Native errors: "Failed to load source outside", "Failed to load sprite" ×2, "Failed to load glyph range 0-255" for Noto
Sans Italic / Regular / Medium. The one 0.6.0 report (11-05 20:21) carries none.

Find whether it was a one-off (network drop, a staging upload mid-way, the old release T403 fixed) or a real gap: a
self-hosted asset (memory self-host-everything) missing from the release or not cached for offline use. If real: the
asset ships with the install, and a failed style asset is reported once (reportFailure), not only logged. Check the
0.6.0 crash/report feed again after 2–3 days of testing. Size: S. Owner: no.

## Done 2026-11-05

**Verdict: a real gap, not a one-off. It is transient: it hits only while an update is applied in-session.**

Evidence (feeds worker `GET /reports?since=2026-11-04` and `GET /crashes?since=2026-11-04`, read 11-05):
- All six 0.5.0 reports (11-05 00:35Z–01:14Z) come from one install, `158017bfdda33566`. Each carries the same single
  burst at 17:07 B.C. (00:07Z): `source outside`, `sprite` ×2, and glyph range 0-255 for Noto Sans Italic, Regular
  and Medium. It is one event, held in the error ring, not a repeating failure.
- The same install filed T403's crashes at 00:00:39Z and 00:01:16Z on **r-4e709418ebd8**. Its reports from 00:35Z are
  on **r-5a5999c94588**. So the burst at 00:07Z is the in-session apply (resume with an update ready → generation
  bump → `bootRelease`). The process never restarted, which is why every later report still holds the same lines.
- That swap changed the tile folder: `822db448a2851809.f98ea27b8e5f` (built 10-31, r-4e70's) → `.a4c3f4eb02bd`
  (built 11-03, r-5a59's). `bootRelease` prunes the replaced set as soon as the new one opens. That is still inside
  the boot, while the App keeps the OLD set on screen until the new boot is ready (T287's warm-up has since made that
  window longer). The old folder held the atlas, `bc_outside.geojson` and `map-assets-1/`, which are exactly the three
  things that failed. A style (re)load or a lazy glyph fetch in that window read deleted `file://` paths. Which
  re-render triggered the reload was not pinned down; the delete under the live map is what was proved.
- The 0.6.0 report (20:21Z, same install) carries none: a cold start opens the new set with nothing on screen.
- Self-hosting itself is fine. On a phone all three files ship with the installed release (`releaseTiles` → the
  installed `file://` folder, every byte verified), and the staging manifest names the mask and the assets folder.

**Fix (370731ad3).**
- `bootRelease` `keepReplaced` (`@app/data` boot.ts). `useReleaseBoot` sets it for an applied update (generation > 0).
  The replaced set is not pruned under the map that draws it: it stays the PREVIOUS pointer, and is pruned by the next
  launch or just before the next `stage`. By then the new set has been drawn.
- A failed style asset is reported once, not only logged (`@app/map` `native/styleAssets.ts`). MapLibre Native says
  "Failed to load sprite / glyph range … / source …" only in its log. The native map now installs a `LogManager.onLog`
  watch that reports each asset once a run (`reportOnce`, key `map-style-asset:<sprite | glyphs:<stack> | source:<id>>`)
  and still prints the line. On a phone these are files of the install, so the failure is never the environment's.

**Tests.** `tools/release-boot.test.ts` +2: an in-session apply keeps the replaced tile folder whole, the next stage
prunes it first, and the next launch prunes what it replaced. Without the flag the first assertion fails.
`map/adapter-contract.render.test.tsx` +1: the 0.5.0 phone's lines, sent twice, give one report per asset; a warning
is ignored; the line is still printed.

**Left.** Read the crash/report feed again after 2–3 days of 0.6.x testing: no `map-style-asset:` report should appear,
including after an in-session update that changes the tile folder. Not checked on a device.

## Phone check 2026-11-05 (owner, 0.6.2 from Play internal): PASS
`node deploy/staging/test-updates.mjs tiles` (new scenario: the tile folder moves, same files) on the test channel;
the update downloaded and applied by itself on opening the app (owner); the map stayed whole: labels, symbols, the outside shading. The reports and
crashes feeds hold nothing since (no `map-style-asset:` report). The apply-on-return path was not tried separately.
Test channel reset after.
