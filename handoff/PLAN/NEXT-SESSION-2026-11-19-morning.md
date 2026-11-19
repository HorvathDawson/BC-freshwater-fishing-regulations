# Handoff 2026-11-19 morning: 0.8.2 is on staging; the owner tests, then production

One chat does everything (as since 2026-11-19 02:50). Read AGENTS.md and CLAUDE.md first.

## State
- 0.8.2 with every item the owner asked for at 02:41 is on STAGING: the data release, the staging website, the staging
  feeds worker and Play internal. The ids, checks and what an angler sees are in the top entry of `handoff/PLAN/STATUS.md`.
- Production is untouched. The owner tests staging (OWNER-TODO "2026-11-19 morning"), answers six questions, then runs
  the production steps himself in the order written there (FEEDS WORKER FIRST: the new app shows no DFO rows for a
  place an old feed carries, T870).
- After his go: deploy the production website from the pushed commit, as the previous restart page
  (`handoff/PLAN/NEXT-SESSION-2026-11-19-one-chat.md`, "Tomorrow: production") says; memory prod-web-deploy-when-tested.

## Next, in order
1. Read the owner's answers (OWNER-TODO, the To-Do page). Record each in its ticket, OWNER-TODO and the page.
2. Any fix he asks for after testing: lane → tier → review → staging again.
3. Production website after his go. Then T319 store uploads if he approves the commands (they were blocked overnight).
4. Next work: T903 retry (paintDefault alone, base not first, ≥6 rounds, explain the pan regression); T891 part two
   (pictures first); T880, T885; T866, T867, T890; the parked list in `handoff/PLAN/PARKED-TICKETS.md`.
