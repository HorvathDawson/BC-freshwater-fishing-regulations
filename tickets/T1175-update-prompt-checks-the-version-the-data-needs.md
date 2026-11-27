# T1175 — The strong update prompt offers Update only when the store's build can read the data

Status: OPEN — 0.10.1, the hardening queue (owner 2026-11-26: "Small fix in 0.10.1")

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1175 | The strong update prompt offers Update only when the store's build can read the data | OPEN — 0.10.1, hardening | — | a lane: compare the offered version with the release's minAppVersion; drop any kept store answer from another app version; tests |

Source: the planning chat's notes `review-of-the-nights-merges`, LOW-2 and LOW-3, on PR #285 (T1073).
- LOW-2: in the strong branch (`app/packages/core/src/appUpdate.ts`, `appUpdateVMBare`), when the store says any
  newer build is available, the prompt says "New data needs the latest app" with Update. It never compares the
  offered version with the release's `minAppVersion` (the view model has only `tooOld`). With two app versions in
  flight (Play offers 0.10.0, the data needs 0.10.1) the button installs an app that still cannot read the data. Rare.
- LOW-3: `stillNewer` (`app/packages/ui/src/appUpdate.ts`) drops a kept "available" answer from another app version
  but keeps a kept "current" answer. After an update, the prompt can say "not available for your phone yet" from
  the old answer until the same opening's store check returns.

## Work
1. The view model gets the offered version; Update shows only when it reaches `minAppVersion`; otherwise the "not
   available for your phone yet" words.
2. `stillNewer` drops any kept answer whose `installed` differs from the running app, whatever it says.
3. Tests for both, each failing without its change.

## Acceptance
Both tests pass and fail without the change; the words never promise an app that cannot read the data.
Severity: low. Model: Sonnet. Size: S.

## Owner

- [x] 2026-11-26 — The strong update prompt's version check (LOW-2, with LOW-3) → "Small fix in 0.10.1" (2026-11-26)
