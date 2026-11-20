# Handoff 2026-11-19 evening: 0.8.3 is on production; next chat starts on the after-0.8.3 list

One chat does everything (as since 2026-11-19 02:50). Read AGENTS.md and CLAUDE.md first. Use subagents in parallel
when their files don't collide (Opus for data/pipeline/review, Sonnet for UI/mechanical).

## State (2026-11-19 ~16:30; ids, builds and checks are in the top entry of `handoff/PLAN/STATUS.md`)
- **0.8.3 everywhere:** one data release on staging AND production, both workers on both, the production website from a
  clean commit, Play Closed testing = the 0.8.3 PRODUCTION build (it replaced the 0.8.2 staging build on the owner's
  pick, "Replace 38 now", so Google's review restarted), Play Internal = the 0.8.3 staging build. Play's production
  track: no access yet (12 testers x 14 days on Closed first). 0.8.2 production was skipped (owner plan 12:40).
- **iPhone:** 0.8.2 approved for External testing (owner ~15:42). The 0.8.3 production upload uses T918: it adds itself
  to the External group and goes to Beta App Review by itself (its first real run; result in STATUS).
- **Production website** is built in /Users/dawson.horvath/dev/personal/BC-prod-web (a detached worktree) and deployed
  with `CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh` there.
- **Owner's go for production, this release only:** memory prod-083-owner-go ("Can you do as much of the prod release
  as you can? I give permission"). `release --prod` refuses agents by design; the owner ran it himself. Later releases:
  production is his again unless he says otherwise.

## Rules that bit us (AGENTS.md has all of them)
- Never run the LLM parser or anything that calls the `claude` CLI; give the command.
- Heavy jobs only via `scripts/heavy.sh <label> <pct> -- cmd` (it serialises them). Kill only your own PIDs; never pkill;
  never touch ports 39217/39218.
- Nothing merges into `redesign/stream-sections` without the tier, the Android benches when app code moved, and an
  independent review by a second agent (T918's review found a real silent-success bug).
- Commit with `git commit -- <paths>` after `node app/tools/check-tickets.mjs` (check rc). No stash/reset in the main
  checkout. Read `date` before writing a time. Record owner answers in ticket + OWNER-TODO + To-Do page same turn.
- Production web export = the T910 three steps (export --source-maps, web-shell, sourcemaps keep-web); PRODUCTION.md
  step 6 was fixed 2026-11-19 after a plain export failed build-seo at the end.
- Send the owner pictures, never paths. Don't read `.env`. Never open ~/.canifishthis/ files yourself (scripts read them).

## Next, in order
1. Watch: Google's review of Closed 39; Apple's beta review of 0.8.3; the `[production]` emails and feeds `/status`
   for a day. When Closed is approved: the owner recruits testers (Pacific Angler email sent by him; testers send their
   Google Play email). Apple testers next.
2. **T917** website page build speed (owner asked twice). Baseline measured on the 0.8.3 production build: numbers in
   the T917 ticket ("Baseline"). First lever: the 4-thread cap (it used ~4 of 18 cores).
3. **T920** Conditions: gauge map below the chart (owner's last ask; check the white map box in dark mode).
   **T919** 0.8.3 leftovers (near-duplicate tributary labels, Elaho p.23 twice, in-app map assets, T912 resume check).
4. **T914** web start (mobile Lighthouse), **T906** map smoothness (symbol trace), **T905** salmon feed size,
   **T901** store buttons/app links, T880, T885, T319 store shots; then `handoff/PLAN/PARKED-TICKETS.md`.
5. Owner's open decisions: OWNER-TODO.md and the To-Do page (claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f).
