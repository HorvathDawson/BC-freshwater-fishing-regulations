# T1034 — A `regs=` link opened over another water keeps the old camera (the link's `part` is read and dropped)

Status: PARTIAL — 77d9de76d, 2ddaa795f (the link's `part` opens that part, checked against the water's part count at the open: a part that is not there is reported and the water opens without it; the old-camera symptom was NOT reproduced here); left (Claude): device check of the camera with another water open; the other read-but-dropped keys are T1038.

Source: lane L18, Android emulator, 2026-11-23: a `regs=` link opened while another water was on screen kept the old
camera instead of framing the linked water/part.

## Finding (lane L15, from code; not reproduced on a device)
`parseAppLink` (`@app/ui` appLink.ts) accepts `part`, `whole`, `date`, `tab`, `sheet`, `scroll`, `age`, `res`, `horizon`
in `READ_KEYS` but `resolveAppLink` returns only `{ item, view }`: every other key is silently dropped (no-silent-fallbacks).
The Shell opens the water with `opener.item(item, view)`, no part, so the water map frames the default part, not the one
the link names. Web's start link carries all of them (`parseStartLink`).

## Work
1. The link's `part` reaches the open (`opener.item(item, view, part)`): the water opens on that part, framed, through
   the one `openWater` path search and tap use.
2. The other read-but-dropped keys (date, tab, sheet, scroll, age, res, horizon, whole): carry them or stop listing them
   as read (left for a follow-up ticket).
3. Device check of the camera when another water is open.

Depends: none. Size: S. Owner: no.
