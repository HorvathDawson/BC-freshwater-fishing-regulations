order: 490

## 2026-11-20 17:13: production data, one-time agent override (owner)

- Owner, verbatim: "Add one time override then remove it after" (leaving the computer, wants the release out; "We have
  minimal users"). `pipeline/release.py prod()`: an agent may publish the ONE release named in OWNER_ONE_TIME_PROD (=
  CONFIRM_PROD = the id), today only; checked, staged, fingerprint and "staging serves it" all still hold. Reverted
  right after that release ships.
- USED 19:00 for r-fec3fab4c5aa only (published to production), then REVERTED (the guard is human-only again).
