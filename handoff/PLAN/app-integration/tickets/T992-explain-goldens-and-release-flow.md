# T992 — Explain how the golden pages, fixture re-cuts and the release command work

Status: IN REVIEW (explanation written: `handoff/PLAN/runbooks/release-gates-explained.md`; simplifications proposed, not built); owner: see ## Owner.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T992 | Explain how golden pages, fixture re-cuts and the release command's gates work (and maybe simplify them) | IN REVIEW (explanation written: `handoff/PLAN/runbooks/release-gates-explained.md`; simplifications proposed, not built; owner: see ## Owner) | | owner 2026-11-21 |

(Was POSTPONED — owner 2026-11-21: a future ticket, not 0.8.5; picked up with T993 plan item 11, 2026-11-22.)

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "add a ticket that is about how all this golden
stuff and release works. dont think about it niow just future ticket".

Context: the 0.8.5 staging release (`python -m pipeline release --staging`) stopped eight times, each at a different
gate (fresh-copy tests, fast-fail, CI, the staging site's app version, `--min-app-version`, a size gate, the binding
diff `--accept-diff`, the golden-page re-cut `--accept-fixtures`: 9 pages whose source indexes only renumbered).

Depends: none. Size: ?. Owner: reads the explanation.

## Done (2026-11-22, lane docs/T993-one-human-runbook)
- `handoff/PLAN/runbooks/release-gates-explained.md`: the release's seven stages; what a golden state / golden page /
  fixture re-cut is and why; the binding diff and when it passes by itself; every gate in `pipeline/release.py` today
  (the fourteen preflight checks, baseline, build, the stage 4 checks with the size gates, the follows stage, staging,
  production, the cut), each with what it protects, why it stops, the fix and whether an escape is safe; a table of
  every accept / escape. Linked from the runbook (`release-by-hand.md`) and `release.md`.
- The renumber-only idea was BUILT already by T993 lane G2 (item 8, 60ed4ac17 / d3272d735); the page says so. Eight
  further simplifications are proposed there, none built.

## Owner

- [ ] 2026-11-21 — Read `handoff/PLAN/runbooks/release-by-hand.md` (the whole release in order, each step marked with who runs it) and `release-gates-explained.md` (what each stop means, when an accept is safe); try them on the next release and say what reads wrong.
