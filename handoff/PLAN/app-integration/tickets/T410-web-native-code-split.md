# T410 — One clear web / native split in the app code

Status: PARTIAL — items 1-4 done (8374cae0d..cd2be0916; listings 2026-11-06 below; full `pnpm -C app check` green 2026-11-06); left: the owner's phone pass over the moved `.native` halves and the T258/T260 owner items listed below. Was: OPEN — with the launch-and-hygiene chat (owner 2026-11-05: "make it so the web and app code are split out a
little better … not sure the standard here but seems like something to clean up").

## Today (2026-11-05)
- 38 platform files (`*.web.ts(x)`, `*.native.ts(x)`, `*.ios` / `*.android`) — the Expo/Metro standard: the bundler
  picks the file for the platform, so the other platform's code is never even in its bundle.
- Beside them, 43 inline `Platform.OS` checks in 30 files and `typeof document / window` checks in 10 — web and phone
  code mixed in one file, both shipped to both.
- Packages: core (pure logic, no platform), data, map, ui (shared hooks), ui-native (screens, web + phone via
  react-native-web), apps/mobile. The SEO site tools live in app/tools/seo.

## Work
1. Write the rule down (AGENTS.md / app README): logic that differs by platform lives in a platform file
   (`foo.web.ts` + `foo.native.ts` behind one `foo.ts` type), never an inline branch — except a one-value difference
   (a size, a flag), which may use `Platform.select`. `core` stays platform-free.
2. Move each inline branch that is more than a value into platform files (list them first; one commit per area).
3. A guard test: no `Platform.OS` / `typeof document` outside an allow-list, so it cannot drift back.
4. Check both bundles: the web bundle carries no native-only module, the APK no web-only one (bundle listing diff).

## Acceptance
The rule is written; the guard passes; Android, iOS and web tests green; web and APK bundle contents listed before/after.

Links: T409 (web speed work stays in web files), T333 (hygiene sweep). Size: M. Owner: no.


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T258, T260 — read each file's open items and finish them here; each closes pointing here.

## Outcome (2026-11-05)
1. **The rule** — app/README.md "Web and native code" (and MAP.md's gate line): logic that differs is a `foo.web.ts` +
   `foo.native.ts` pair typed by one `foo.d.ts`; a one-value difference (a size, a flag, a word) may stay inline
   (`Platform.select`, or one `Platform.OS` in an allow-listed file); core reads no platform.
2. **Moved into pairs** (one commit per area, no behaviour change intended on either platform):
   - `SpotPickBanner` Escape listener → `useEscapeKey` in `escapeCloses.web/.native` (8374cae0d)
   - `window.innerWidth/innerHeight` reads (Shell tour fit, CoachMarks, Walkthrough) → `windowSize.web/.native`; the
     Walkthrough's spoken step → `announce.native` (web no-op: its live region) (05b08c549)
   - ViewSwipe's by-hand pan activation (T300) → `swipeActivation.native` (450489c8b)
   - the regs prewarm effect (T286) → `shell/prewarm.native`; `memoryTrim.ts` → `memoryTrim.native` + web no-op (34e71f6a5)
   - About's official Synopsis link → `menu/openOfficial.web` (its link) / `.native` (our copy, `openBook`) (f24c83200)
   - GroupCard's pinned size header → `regs/stickyHead.web` (CSS sticky) / `.native` (scroll-driven translate) (005346269)
   - `part()`'s ask → `partAsk.web` (idle, `unguarded`) / `.native` (at once); `unguarded` in its own file (2f48f6623)
   Inline checks: 43 `Platform.OS` in 30 files + `typeof window/document` in 10 → 24 reads in 17 allow-listed files, all
   one-value (variant files and comments aside). Platform pairs 27 → 35.
3. **The guard** — `pnpm platform` (`tools/check-platform.mjs` §4): `Platform.OS` / `typeof window|document|navigator`
   in a shared file (ui, ui-native, core, map, data, apps/mobile; not a variant, test or .d.ts) is refused unless
   `INLINE_ALLOWED` names the file with its exact count and reason (above: new branch; below: lower the line; a
   listed file that is gone: remove it). Tests in `check-boundaries.test.ts` (refused in 4 packages; variant / comment /
   `Platform.select` pass; an allow-listed file held to its count both ways) (cd2be0916).
4. **Not done: the bundle listings** (web export + APK, before/after) need `expo export` / an APK build — the machine
   was held by a heavy job. Expected by construction: each moved native half (`InteractionManager` prewarm,
   `requireOptionalNativeModule("MemoryTrim")`, `AccessibilityInfo.announceForAccessibility`, the manual pan) leaves the
   web bundle, and the DOM listeners leave the APK. Run when free: `pnpm -C app/apps/mobile exec expo export -p web` and
   `-p android` at 79e7f2d2f (before) and cd2be0916 (after), diff the module lists.

One deliberate edge: Walkthrough's no-size fallback on a PHONE (reached only when `useWindowDimensions` reports width 0)
was `{ w: undefined, h: undefined }` (RN's `window` has no `innerWidth`) and is now `{ w: 0, h: 0 }`, the web's
no-window value. Web behaviour unchanged.

Verified: `pnpm typecheck`, `pnpm platform`, `pnpm boundaries`, `pnpm knip` clean; vitest (unfiltered) — 20 render files
(part, a11y.shell, Walkthrough, SpotsScreen, SwipeShield, WaterMode, gestures, Menu, Changelog, regs a11y / rowFrame /
useAnchors / SheetWide / RuleBookLink, Search.shell, MapRail.shell, escapeBack.web, ConditionsPanel, Faces.share,
motion) 271/271, and 31 tools files (check-boundaries, failures, back-layers, the one-* / source-scanning gates) green.
NOT run: the full suite, `pnpm check`, an export, a device. The `.native` halves are proved by tsc only (vitest runs the
web half) — a phone pass over the spot pick, the guide, a water's view swipe, the size header pin, About → Synopsis and
a memory trim is the owner's next APK.

## T258 / T260 leftovers (merged here)
- **T258**: items 1-3 were done in T258 (2d60c72cb). Left, OWNER: the TalkBack + VoiceOver phone walk (map → water →
  regs → conditions → salmon → menu) and 200 % text (the tab-bar 1.3× cap). Findings become tickets.
- **T260**: Chip, EmptyState/NoneLine, caption token, Runs radii done in T260. Every item left is a look decision, so
  none was typed here — OWNER:
  - Badge (env badge, count badges, "no photo" look different on purpose; unifying changes one of them)
  - SegmentedControl: one selected-state language (accent frame, ink fill or tint) — owner Q
  - Banner / Notice and SectionHeader (NetBanner, PartPending, SyncErrorSheet rows, SpotPickBanner, InSeasonBanner;
    the kicker overrides in LayersSheet / SpotCapture / spot parts)
  - `fontWeight` literals (a synthesized bold on the web; each needs its face picked, then widen the regs-tokens check);
    App.tsx boot lines and MapLegend's raw sizes with it
  - the icon ladder (close icons 20 vs 22, chevrons 14/16/18) and IconButton (Hydrograph ±, DayCalendar Btn,
    SpotCapture Step)

## Owner 2026-11-05 (T260 look)
The SegmentedControl's selected look everywhere = **ink fill** (white text), as Now | Forecast | Change today. The other T260
look items (Badge, Banner/Notice, SectionHeader, weight/icon ladders) stay as they are until asked.

## Item 4: the bundle listings (2026-11-06)
Module lists from the source maps of `expo export --platform web|android --source-maps` (staging env), before = 79e7f2d2f,
after = e8ebf21f4 (HEAD; includes T436's tile-ids/me moves). node_modules: identical on both platforms.
- **Web** (1,158 → 1,167 modules; index 3,488 → 3,492 kB raw): gone `memoryTrim.ts`, `map/native/me.ts`,
  `map/tile-ids.web.ts`; new `memoryTrim.web`, `announce.web`, `openOfficial.web`, `partAsk.web`, `platformLine.web`,
  `stickyHead.web`, `prewarm.web`, `swipeActivation.web`, `windowSize.web`, `unguarded.ts`, `map/me.ts`,
  `map/web/tileIds.ts`. **No `.native.` file in the web bundle**, before or after.
- **Android** (1,689 → 1,698 modules; hbc 4,560,260 → 4,564,888 B): gone `memoryTrim.ts`, `map/native/me.ts`; new
  `memoryTrim.native`, `announce.native`, `openOfficial.native`, `partAsk.native`, `platformLine.native`,
  `stickyHead.native`, `prewarm.native`, `swipeActivation.native`, `windowSize.native`, `unguarded.ts`, `map/me.ts`.
  **No `.web.` file in the APK bundle**, before or after.
So each moved half lands only in its platform's bundle, as built. The web bundle grew 4 kB (the web no-op halves and
`unguarded.ts`, which both platforms carry); the native halves add ~4.6 kB of bytecode. Listings: the session scratchpad
`{web,android}-{before,after}.mods`.
