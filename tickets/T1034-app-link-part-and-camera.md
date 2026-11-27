# T1034 — A `regs=` link opened over another water keeps the old camera (the link's `part` is read and dropped)

Status: PARTIAL — 77d9de76d, 2ddaa795f, 9520ae1eb (the link's `part` opens that part, checked against the water's part count at the open: a part that is not there is reported and the water opens without it; the old-camera symptom was NOT reproduced here); left (Claude): device check of the camera with another water open, and of the 9520ae1eb crash fix on a build that has it; the other read-but-dropped keys are T1038.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1034 | A `regs=` link opened over another water keeps the old camera (the link's `part` is read and dropped) | PARTIAL — 0.9.0 crash across waters fixed; left (Claude): device check of the camera and the fix; other keys T1038 | 77d9de76d, 2ddaa795f, 9520ae1eb | lane L18 emulator 2026-11-23 |

Source: lane L18, Android emulator, 2026-11-23: a `regs=` link opened while another water was on screen kept the old
camera instead of framing the linked water/part.

Cause found (lane L15, from code): `resolveAppLink` (`@app/ui` appLink.ts) returned only `{ item, view }`, so the
link's `part` (in `READ_KEYS`) was dropped and the Shell's `opener.item(item, view)` framed the default part.
Done: 77d9de76d, 2ddaa795f: the link's `part` reaches the open through the one `openWater` path, checked against the
water's part count (a part not there is reported, the water opens without it). The other read-but-dropped keys (date,
tab, sheet, scroll, age, res, horizon, whole) are T1038.

Depends: none. Size: S. Owner: no.

## 0.9.0 device pass (lane L35/L36, 2026-11-24)
- **Crash, 2 of 2 each**, on the 0.9.0 staging APK (release/0.9.0 at 2a9a17f3a): `Error: frame: part N has no part_geo
  row` from `middleOf` (`app/packages/ui/src/frameView.ts`), thrown in useFrameView under WaterMap / WaterScreenBody.
  Repros: Zymoetz River (6 parts) on its Map, "Flows into Skeena River ›" (Skeena part 10); Vedder on Map, link
  `?regs=gnis:26216&part=2&view=map` (Sumas); Vedder on Regulations, link `?regs=gnis:2936&part=11` (Skeena). The same
  links from a cold start worked.
- Cause: `useAsync` returns the last key's answer for one render after its key changes; `useFrameView` paired the new
  route's part with water A's `navRows` (same class as T444). Fix 9520ae1eb: the nav answer is tagged with its question;
  another water's is loading. Test `app/packages/ui/src/frameView.render.test.tsx` (fails on the unfixed code; mutation
  "stale nav -> ready(null)" fails it too).

## Work (left)
1. Device check of the camera when another water is open.
- Left: the three repros again on a build with 9520ae1eb.
