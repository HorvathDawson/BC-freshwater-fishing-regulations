# T77 — D1 phone testing: dev build now, preview build on R2 staging next (audit N17)

Status: OPEN. Owner needed (the phone, R2 credentials).

Source: handoff/PLAN/app-data/D1-phone-testing.md — owner 2026-10-21: "do both options"; neither done.
implement_progress queue K: "D1 phone testing needs the owner". Every "NOT verified on a device" note waits on this:
S1 photo picking/downscale, IndexedDB, netinfo dev build, Vulkan vs OpenGL, A4/A8 perf budgets, gestures (two-finger
rotate/tilt), a device reading the manifest over the LAN.

## Work
- D1a: dev build + `OWNER-PHONE.md` (developer mode, USB, `pnpm -C app/apps/mobile android`,
  `EXPO_PUBLIC_DATA_BASE=<LAN IP>:39217`) and a checklist (gestures, pan/colour speed, Vulkan vs OpenGL, Keep,
  Conditions, memory with tiles open, S1 photos).
- D1b: a preview profile against R2 staging; the P11 install / resume / update-only-changed / airplane-mode launch /
  rollback script.

## Acceptance
Checklist results filed; each failure becomes a fix or a ticket. A preview APK installs from staging and passes D1b.

Depends: T50; T74 for the colour numbers; D1b also T75 and T86 (R2 bucket). Size: M.
Owner: yes. Default: D1a as soon as the phone is at hand; D1b after the R2 bucket exists.
