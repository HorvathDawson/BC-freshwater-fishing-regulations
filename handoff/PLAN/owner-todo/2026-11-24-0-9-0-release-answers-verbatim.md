order: 740

## 2026-11-24: 0.9.0 release answers (verbatim)

- Salmon head size gate (T796, T905): "Raise to 110,000 (Recommended)". 100,000 → 110,000; the shrink is T905.
- The side atlas (T1012, T803): "yes promote if it fixes all the issues??" — promoted only after the four checks (inputs, preflight, binding diff, vintage) pass.
- The main checkout: "Yes, switch and go (Recommended)" — release/0.9.0 for Steps 2–5, back to main after.
- Cold-start tab links (T1049; device pass: tab=gear 0/6, tab=licence 1/6 on a cold start, fine when the app is open): "Ship, fix in 0.9.1 (Rec.)". 0.9.0 ships with it as a known issue; the 0.9.1 chat owns the fix.
- The 0.9.0 What's new bullets (the five in changelog.ts): "OK as written (Rec.)". The draft mark is cleared on release/0.9.0 (the entry is only there until the merge-back).
- The v0.9.0 tag (it was at a98dd68f9, before the WorkManager, crash and tab fixes): "Yes but in future we can use release candidates ?". Moved to the commit the production build uses. Release candidates for later releases go to the planning chat as a runbook item: tag each staging candidate `v<x.y.z>-rc.<n>`, and tag `v<x.y.z>` only at the production commit.
- The production Android build's CI check (docs-ci lookback of 30 runs misses the green ancestor run; the checker bug is with the 0.9.1 chat): "Yes, with the reason (Rec.)". The build runs with a CI_CHECK_SKIP that states the reason.
