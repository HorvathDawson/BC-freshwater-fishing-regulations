# T410 — One clear web / native split in the app code

Status: PARTIAL — items 1-4 done (8374cae0d..cd2be0916; listings 2026-11-06 below; full `pnpm -C app check` green 2026-11-06); the pass over the moved `.native` halves done on the emulator 2026-11-06 ("Native pass" below: 6/6, the spoken step not heard, no TalkBack); owner: see ## Owner (the T258/T260 items listed below). Was: OPEN — with the launch-and-hygiene chat (owner 2026-11-05: "make it so the web and app code are split out a
little better … not sure the standard here but seems like something to clean up").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T410 | One clear web / native split in the app code (platform files, a written rule, a guard) | PARTIAL (items 1-4 done; native pass done on the emulator 11-06, 6/6; T258/T260 owner items left) | 8374cae0d, 05b08c549, 450489c8b, 34e71f6a5, f24c83200, 005346269, 2f48f6623, cd2be0916 | owner: T258 TalkBack/VoiceOver walk + 200 % text (old owner to-do "Accessibility (T243 → T258, 2026-10-29): a TalkBack / VoiceOver walk"); T260 look answered 11-05 (ink fill; rest as is) |

## What
Platform logic lives in platform files (`foo.web.ts` + `foo.native.ts`, the Expo/Metro standard: the other platform's
code is never in the bundle), not inline branches; a guard keeps it from drifting back.

Done:
1. **The rule** — app/README.md "Web and native code" (and MAP.md's gate line): logic that differs is a `foo.web.ts` +
   `foo.native.ts` pair typed by one `foo.d.ts`; a one-value difference (a size, a flag, a word) may stay inline
   (`Platform.select`, or one `Platform.OS` in an allow-listed file); core reads no platform.
2. **Moved into pairs** (8374cae0d, 05b08c549, 450489c8b, 34e71f6a5, f24c83200, 005346269, 2f48f6623): inline checks
   43 `Platform.OS` in 30 files + `typeof window/document` in 10 → one-value reads in 16 allow-listed files; platform
   pairs 27 → 35. One deliberate edge: Walkthrough's no-size fallback on a PHONE is now `{ w: 0, h: 0 }` (was
   `{ w: undefined, h: undefined }`). Web behaviour unchanged.
3. **The guard** — `pnpm platform` (`tools/check-platform.mjs` §4): `Platform.OS` / `typeof window|document|navigator`
   in a shared file is refused unless `INLINE_ALLOWED` names the file with its exact count and reason; tests in
   `check-boundaries.test.ts` (cd2be0916). The walk skips `apps/mobile/ios` and `apps/mobile/android` (generated).
4. **Bundle listings** (2026-11-06, source maps of `expo export --platform web|android`, 79e7f2d2f → e8ebf21f4): no
   `.native.` file in the web bundle and no `.web.` file in the APK bundle, before or after; web +4 kB, native ~4.6 kB.
- Native pass (2026-11-06, emulator, release test-bed APK @ bccf9f668, n1_pixel; shots `../T410-shots/android/`,
  `../T410-shots/android-sticky/`): spot pick banner, Walkthrough window size, a water's view swipe, size-header pin,
  About → Synopsis (our copy), memory trim: 6/6 PASS. The spoken step was NOT heard (no TalkBack on this AVD).

## Work (left: owner)
- **T258** (items 1-3 done in T258, 2d60c72cb): the TalkBack + VoiceOver phone walk (map → water → regs → conditions →
  salmon → menu) and 200 % text (the tab-bar 1.3× cap). Findings become tickets.
- **T260** (Chip, EmptyState/NoneLine, caption token, Runs radii done in T260): look decisions, none typed here:
  Badge (env badge, count badges, "no photo"); SegmentedControl (answered below); Banner / Notice and SectionHeader
  (NetBanner, PartPending, SyncErrorSheet rows, SpotPickBanner, InSeasonBanner; the kicker overrides in LayersSheet /
  SpotCapture / spot parts); `fontWeight` literals (a synthesized bold on the web; each needs its face picked, then widen
  the regs-tokens check), App.tsx boot lines and MapLegend's raw sizes; the icon ladder (close icons 20 vs 22, chevrons
  14/16/18) and IconButton (Hydrograph ±, DayCalendar Btn, SpotCapture Step).

## Owner 2026-11-05 (T260 look)
The SegmentedControl's selected look everywhere = **ink fill** (white text), as Now | Forecast | Change today. The other T260
look items (Badge, Banner/Notice, SectionHeader, weight/icon ladders) stay as they are until asked.

## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T258, T260 — read each file's open items and finish them here; each closes pointing here.

## Acceptance
The rule is written; the guard passes; Android, iOS and web tests green; web and APK bundle contents listed before/after.

Links: T409 (web speed work stays in web files), T333 (hygiene sweep). Size: M. Owner: no.

## Owner

- [ ] 2026-10-29 — On a phone, a TalkBack / VoiceOver walk (map, water, regs, conditions, salmon, menu) at the largest system text; say if the bottom tab labels stopping at 1.3x (Q160) is wrong.
