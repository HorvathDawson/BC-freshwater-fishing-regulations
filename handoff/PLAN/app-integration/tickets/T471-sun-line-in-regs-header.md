# T471 — The sun line moves from its own card into the regs pane's header

Status: DONE 2026-11-07 0f4b0063a (owner: "Sunrise 08:04 PT · sunset 17:37 PT type text should be in the header of the pane for regs instead.")

Was: `SunCard` (T382), a tinted card at the top of the scroll. Now: a small secondary line (`regs-sun`, metaSemi, `sub` colour)
inside `RegsHeader`, under the place label, on a phone and on the desktop header. `SunCard.tsx` and its test are removed;
`RegsScreen` hands `sun` to the header. Behaviour unchanged: `useSunLine` null draws nothing, no fallback. One component for web
and native (react-native-web), so no per-platform split (rule 34a).

Tests: `RegsHeader.test.tsx` "the sun line in the header" (shown under the place, desktop, null draws none).
Shots: see T472 (same pane) in the scratchpad, `t471-{before,after}-{390,1440}.png`.
Size: S.
