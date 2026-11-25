order: 520

## 2026-11-21 evening: build/CI streamlining chat (T993, T978, T994; no release) — open decisions, asked at the end

- **To read (you): the one release runbook and the gates page [T993 item 11, T992].**
  `handoff/PLAN/runbooks/release-by-hand.md` is the whole release in order, each step marked with who runs it;
  `handoff/PLAN/runbooks/release-gates-explained.md` says what each stop means and when an accept is safe, and ends
  with eight simplification ideas to pick or drop. Try it on 0.8.6 and say what reads wrong.
- **T978 MapLibre (answered 2026-11-22):** "Pinned upstream main (Recommended)". From 0.8.6 Android builds use one
  pinned upstream main commit, no patch (measured 20-32% cheaper; 0002 within noise). Claude builds the switch.
- **Old headless Chrome (answered 2026-11-22):** "Yes, stop them (Recommended)". 19 orphaned headless Chrome trees
  (parent gone, 1-3.7 days old) stopped; your own Chrome untouched.
- **Merging (answered):** PRs + required checks into redesign/stream-sections. **Push hook (answered):** versioned
  pre-push hook running the before-push command.
- **T995 done on GitHub (2026-11-22), for you to know:** `main` is the trunk. Direct pushes to `main` are refused,
  yours too: changes go by pull request with the four checks green (you can still merge a PR without green checks as
  admin). `redesign/stream-sections` is read-only until the 0.8.6 release.
- **To do (you):** turn off the Copilot coding agent in the repo's Settings → Copilot (its environment is already
  deleted).
- **T866 test trim (answered 2026-11-22):** "Speedups, remove nothing (Rec.)", "Remove 5 true duplicates", "Fold the 30
  wording files"; not the parallel pipeline tests. Proposal: `handoff/PLAN/audit-2026-11-21/t866-proposal.md`.
- **T998 versioning (answered):** 1.0.0 = "At public launch (Recommended)"; the bump comes from "Conventional Commits"
  (feat → minor, fix → patch, `!` → major; before 1.0 a breaking change bumps minor). You confirm each computed
  version at release.
