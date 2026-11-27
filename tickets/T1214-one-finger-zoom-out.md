# T1214 — A one-finger way to zoom out on the phone map (a Zoom buttons switch, or a gesture hint)

Status: OPEN — the owner picked option 1 (2026-11-27); not started

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1214 | A one-finger way to zoom out on the phone map (a Zoom buttons switch, or a gesture hint) | OPEN | — | build the Zoom buttons switch (More > Display, off by default; app and website); pictures before merge |

Source: T1212's independent review (2026-11-27): dropping + / − on a phone-wide web window (T1212, owner 2026-11-26:
"definitely drop +/-") is a step back for anyone who cannot pinch. WCAG 2.2 SC 2.5.1 Pointer Gestures (Level A): a
multipoint or path-based gesture needs a single-pointer way without a path; its own example is a map whose pinch has
"plus/minus buttons" beside it (w3.org Understanding 2.5.1).

What a finger has now (T1212's ticket has the file:lines): double tap zooms IN on web and phone; double tap then drag
zooms both ways (web `TapDragZoomHandler`, phone MapLibre Native's quick zoom), but that is a path; a two-finger tap
zooms out on the web (multipoint). The phone app's main map never had + / −; phone-width web had them until T1212.

## Options

1. A "Zoom buttons" switch under More > Display, off by default: on, + / − (44 px) stand at the top of the rail on the
   web and the phone. Meets 2.5.1 for whoever turns it on; nothing changes for anyone else.
2. Small + / − always on phone-width web only (the phone app as now): what the web had before T1212, on the grid.
3. A documented double-tap-then-drag with a one-time hint over the map (and in Help). Cheapest; it is still a path, so it
   does not meet 2.5.1 on its own.

## Work (owner 2026-11-27: option 1)

A "Zoom buttons" switch under More > Display, off by default, on the app and the website. On: + / − (44 px) at the top of
the rail on a phone (the website's phone-wide window and the app); wide screens keep theirs whatever the switch says. The
choice is kept with the other display settings. Pictures (off / on, 320–412, text 1.3, dark) to the owner before merge.

## Acceptance

A render test per platform (web and native): off, no + / − on a phone; on, + / − at the top of the rail, 44 px, named aloud;
wide screens unchanged. At 320–412 wide and text 1.3. Help says how to zoom without pinching.

Depends: T1212. Size: S. Owner: pictures before merge.

## Owner

- [x] 2026-11-27 — how should someone who cannot pinch zoom out on a phone: 1 a "Zoom buttons" switch (More > Display, off
  by default), 2 small + / − always on phone-width web, or 3 a one-time hint for double-tap-then-drag? → "A switch under
  More > Display, off by default (Recommended)" (2026-11-27)
