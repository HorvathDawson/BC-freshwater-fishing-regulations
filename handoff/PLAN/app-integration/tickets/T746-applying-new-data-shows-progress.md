# T746 — While new data is applied the app shows a loading mark and a line of text, not a still screen

Status: IN PROGRESS (merged onto `redesign/stream-sections` 2026-11-16 via `merge/held-group1` (70d30e77f); the Android benches (update-freeze, big-update, regs-open, tap-stalls and the rest) all PASS on the merged APK, 2026-11-16 (b60d6436e); left (Claude): a look at the curtain on an Android screen, §4)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T746 | While new data is applied the app shows a loading mark and a line of text, not a still screen (Android, iPhone, web). Next free ticket T747 (T742 to T745 are taken by the Android chat) | IN PROGRESS (merged 70d30e77f via `merge/held-group1` 2026-11-16; Android benches pass, b60d6436e; left (Claude): a look at the curtain on Android) | 889a194ea, 2a0ab83f9, 70d30e77f | owner 2026-11-14, relayed by the Android chat: "also when changes apply to app we want to make it so that it has a loading icon with text like splash so it doesnt looking like it … (detail: ticket file) |

Source: owner 2026-11-14 (evening), relayed by the Android chat, verbatim: "also when changes apply to app we want to make
it so that it has a loading icon with text like splash so it doesnt looking like it is frozen. (applying changes as in
new data regs)".

## What is asked
While a new data release is being applied (after its download: checking, swapping the release in, reopening the
databases, the map reloading its style and tiles), the app shows a splash-like state: a loading mark and one line of
text that says what is happening, in the app's plain words. On Android, iPhone and the web.

Done (889a194ea, 2a0ab83f9; merged 70d30e77f via `merge/held-group1` 2026-11-16, Android benches all PASS on the `g1`
APK, b60d6436e):
- **`ApplyCurtain`** (`ui-native/src/ApplyCurtain.tsx`, drawn by the Shell over everything): takes every tap (and the
  system back) from the moment an in-session apply starts; DRAWN only after 300 ms, then stays at least 400 ms (the
  app's `SPINNER_TIMING`, T257); the boot screen's look (110 px loading fish, one line). Every way in is the same
  generation bump, read by `@app/ui` `useApplyCurtain` (one load path). No `Platform.OS`, no new dependency.
- A cold start that applies says the same line on its boot screen (`BootStage` "apply", `data/release/boot.ts`).
- A failed apply is said ("The new regulations could not be put in place. The app is still using the data it had." + OK)
  and reported (`reportFailure`); the set in use stays; a `warmLike` throw is that failure too.
- Words: owner 2026-11-15, verbatim: "can we do more fish puns here that are directed at this ?" — five lines
  (`APPLY_WORDS.lines`, core `install.ts`): "Reeling in the new regulations…", "Netting the latest regulations…", "Landing
  the new regulations…", "Tying on the new regulations…", "Casting the new regulations into place…"; chosen by core
  `applyLine(id)` (FNV-1a of the `data_release` id; no id: line 1). Owner 2026-11-15 evening, verbatim: "Yes to both
  (Recommended by it)" — all stay (To-Do row `t746-applying-words`); nothing further is asked of the owner.
- Measured (rough, loaded machine): web apply → committed 346 / 164 ms (curtain drawn 307 ms, gone 708 ms; the 164 ms run
  never drawn); iPhone simulator safe moment 702 ms, back from background 880 ms. Android 2026-11-20 (L4, `update-freeze`,
  build of 124d79efa): curtain light and dark PASS (up to down 842 ms light, 658 ms dark); stall check on the apply 0
  over 100 ms while panning; shots `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T746/T746-curtain-up-light.png`
  and `-dark.png`. Pictures of the pun lines: scratchpad `t746/` (build 2a0ab83f9).
- Tests: `ApplyCurtain.render.test.tsx`, `ApplyCurtain.flow.test.tsx`, `ui/boot.render.test.tsx`,
  `tools/release-boot.test.ts`, `core/install.test.ts`.

## Work (left)
- Not verified: a failed apply on a device (unit + render tests only); taps landing on the curtain on a device; a
  release with changed tiles / paint (the map's restyle after the commit), on any platform — the bench releases had the
  same files, so the map reloaded nothing.
- Found, to the follow-ups: the largest standard Dynamic Type size (`content_size extra-extra-extra-large`) wraps
  "Casting the new regulations into place…" to two lines (picture `ios-applying-largest-text-casting.jpg`): goes to T726.
  On the way back from the background the JS thread stalls ~550 ms just after the commit (327 + 175 + 52 ms; not map
  drawing; likely the foreground return's own work). Bench: `ios-bench.mjs` case B (scratchpad `t746/`).

## Acceptance
- No still screen longer than about 300 ms during an apply without the loading state up, on the three platforms; the
  measured times before and after are in this ticket.
- A failed apply is shown and reported; the old release still opens.
- The owner has picked the wording from pictures.

Depends: T722 (install branch). Size: M. Owner: wording pick.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-14, relayed by the Android chat: "also when changes apply to app we want to make it so that it has a loading icon with text like splash so it doesnt looking like it is frozen. (applying changes as in new data regs)". Depends on T722
