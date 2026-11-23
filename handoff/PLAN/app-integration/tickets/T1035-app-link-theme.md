# T1035 — `theme=dark` on a `regs=` link does not switch the theme on the phone

Status: DONE — 77d9de76d, 2ddaa795f (theme is parsed, applied in App via `applyAppLink`, SESSION ONLY: never saved, as the web start link's `theme=` is; a bad value drops the key, is reported, the theme is unchanged; not seen on a device).

Source: lane L18, Android emulator, 2026-11-23.

## Finding (lane L15)
The web reads `theme` from its start link once (`START?.theme`, App.tsx). The phone's link path (`onAppLink` →
`resolveAppLink`) lists `theme` as read but returns only `{ item, view }`, so the theme is dropped with no word.

## Work
1. `resolveAppLink` returns the link's `theme` (light|dark; anything else refused loudly) and App applies it (`setTheme`),
   as the web's start link does.
2. Test on the parse + the App hand-off.

Depends: none. Size: S. Owner: no.
