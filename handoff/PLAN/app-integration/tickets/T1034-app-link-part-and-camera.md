# T1034 — A `regs=` link opened over another water keeps the old camera (the link's `part` is read and dropped)

Status: PARTIAL — 77d9de76d, 2ddaa795f, 9520ae1eb (the link's `part` opens that part, checked against the water's part count at the open: a part that is not there is reported and the water opens without it; the old-camera symptom was NOT reproduced here); left (Claude): device check of the camera with another water open, and of the 9520ae1eb crash fix on a build that has it; the other read-but-dropped keys are T1038.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1034 | A `regs=` link opened over another water keeps the old camera (the link's `part` is read and dropped) | PARTIAL — 0.9.0 crash across waters fixed; left (Claude): device check of the camera and the fix; other keys T1038 | 77d9de76d, 2ddaa795f, 9520ae1eb | lane L18 emulator 2026-11-23 |

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

## 0.9.0 device pass (lane L35/L36, 2026-11-24)
- **Crash, 2 of 2 each**, on the 0.9.0 staging APK (release/0.9.0 at 2a9a17f3a): `Error: frame: part N has no part_geo
  row` from `middleOf` (`app/packages/ui/src/frameView.ts`), thrown in useFrameView under WaterMap / WaterScreenBody.
  Repros: Zymoetz River (6 parts) on its Map, "Flows into Skeena River ›" (Skeena part 10); Vedder on Map, link
  `?regs=gnis:26216&part=2&view=map` (Sumas); Vedder on Regulations, link `?regs=gnis:2936&part=11` (Skeena). The same
  links from a cold start worked.
- **Cause:** `useAsync` returns the last key's answer for the one render after its key changes (`async.ts`, "the last
  answer until the effect has run"). `useFrameView` took the new frame's part from the route at once but its `navRows`
  from that stale answer, so water B's part met water A's `part_geo`: a throw when B's part is past A's count, A's
  middle said as B's when it is not. The same class as T444 (`useWaterGroups`).
- **Fix (9520ae1eb):** the nav answer is tagged with the question it answers; another water's is loading ("not known
  yet"), never paired with the frame's part. The throw stays for a part missing on its own water. useWaterMap,
  WaterScreenBody and WaterViews all take the frame from useFrameView; place.ts, carry.ts and core waterNav.ts read
  `navRows` for the ord they asked inside one async read, so they never mixed two waters.
- **Test:** `app/packages/ui/src/frameView.render.test.tsx` (A of 2 parts -> B's part 4, B's nav arriving after the
  route; every render until it arrives is loading). Fails on the unfixed code with the device's error; the mutation
  "stale nav -> ready(null)" fails it too.
- Left: the three repros again on a build with 9520ae1eb.
