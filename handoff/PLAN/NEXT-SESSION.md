# Next session: start here

For the owner, opening a new chat after credits reset. Paste the prompt below and Claude picks up from this file.

## Paste this to start the new chat

> Read `handoff/PLAN/NEXT-SESSION.md` and `handoff/PLAN/STATUS.md` (top entry), then follow the session rules in
> NEXT-SESSION.md. Do steps 0 and 1 (inbox, ticket check) first, then work the queue in order. Start ticket work
> without asking; ask me only for decisions. Keep updates short, keep STATUS.md current, and tell me when it's a good
> moment to compact or start a new chat.

## Where things stand (2026-10-31; newer state is in STATUS.md)

- **APK 0.4.0** is on Play internal testing (versionCode 7).
- **Staging** is on release `r-4e709418ebd8`; smoke 18/18. Prod has no release.
- **Tests:** pytest fast 6141 / slow 272 pass; app checks pass.
- **Branch:** `redesign/stream-sections`. Never `main`.
- **Owner-facing log:** `handoff/PLAN/STATUS.md`, newest first. Every decision from 10-30/31 is in it.
- **Ticket ledger:** `handoff/PLAN/app-integration/tickets/README.md`. Tickets are the only source of work.

## Step 0, every session: empty the inbox
Read `handoff/PLAN/INBOX.md`.
- Turn each note under "New notes" into a ticket, or add it to an existing ticket. Check the ledger's highest number first.
- Move the note to "Filed" with its ticket number.
- Ask the owner only about notes that are unclear.
- Owner bugs go ahead of queue item 2.

## Step 1, each new chat: check the ticket ledger
This is a quick pass, not an audit. One cheap agent (Sonnet) reads `tickets/README.md` and the ticket files, then
reports a list of problems; it does not fix them. Look for:
- duplicate numbers or two files for one ticket (T332 happened twice);
- README status disagreeing with the ticket file's `Status:` line;
- tickets marked DONE whose work isn't in git, or IN PROGRESS with nothing happening;
- the same work in two tickets;
- tickets blocked on something already done;
- owner items missing from OWNER-TODO.md.
Fix the mechanical ones directly (status, numbering, ledger rows). Batch anything that needs a decision into one
question for the owner.

## THE PLAN (owner-approved 2026-11-03): `handoff/PLAN/RELEASE-PLAN-0.5.0.md`
Work its batches in order; that file replaces the old queue. Current state is in STATUS.md's top entry.
- Do NOT deploy the feeds worker or upload to staging until app 0.5.0 exists (`SALMON_NEEDS = {3: "0.5.0"}` in
  deploy/staging/upload.sh). New data releases are promoted locally only.
- Audits T333–T335: the owner scopes them first (see each ticket), only when the owner says go.
- Ticket review with every open ticket by area/size: `handoff/PLAN/audit/ticket-review-2026-11-03.md`.

## Managing the context (the owner steers; Claude suggests)

A long chat costs more on every turn, because each message re-reads everything before it. Keeping chats short and
focused is the biggest saving.

**Claude tells the owner when it's a good moment.** It says it plainly, for example "good point to compact" or "good
point for a new chat".

**Start a NEW chat when:**
- a ticket or batch is finished, committed, and recorded in STATUS.md, and the next one is unrelated (for example, the
  slough release is done and next up is iOS);
- the work changes area (pipeline/data → app UI → audits);
- the chat has been through a compaction already and is getting long again;
- after an APK or release ships.

A new chat starts with the same paste-in prompt; NEXT-SESSION.md + STATUS.md carry everything forward.

**Use `/compact` (keep going in the same chat) when:**
- you're midway through one ticket and the earlier turns are mostly tool output (builds, logs, test runs);
- a long debugging thread found its answer and only the conclusion matters now.

Before compacting, Claude writes the current state into the ticket's RESUME section, so nothing lives only in the chat.

**Don't compact or switch while:** a merge is half-done, a heavy job's result is still unread, or a decision is pending
an answer. Finish that step first.

**Keep chats lean:**
- One area per chat.
- Ask for the plain summary, not the full logs.
- Park ideas in INBOX.md instead of discussing them mid-task.

## Session rules (to make the weekly limit last)

These come from what burned credits on 10-30/31.

1. **At most 2 agent lanes at once.**
   - Model by task: Opus for data and pipeline, Sonnet for mechanical or UI work, Haiku for lookups.
   - Prefer doing a small thing inline over spawning an agent.
2. **Heavy jobs run in the main session's background shell, not inside agents.**
   - Builds, test suites and emulator or simulator runs cost no credits while they run.
   - An agent that waits on a 40-minute build burns context the whole time.
   - Pattern: `scripts/heavy.sh <label> 60 -- <cmd> > log 2>&1`, run in the background; read only the tail when it finishes.
3. **Use 60 for heavy.sh's free-memory gate.** This Mac idles at about 67 % free, so 70 waits forever.
4. **Interim agent notifications get no reply.** Report only real results, and only once.
5. **Agent briefs ask for reports of 10 lines or fewer.** Relay the plain meaning to the owner, not the full report.
6. **Iterate with targeted tests.** Run the full suites (fast 21 min, slow ~40 min) once, unfiltered, in the background, before an APK or a merge.
7. **Use worktrees only when two lanes would touch the same files**, or when a build is reading the main checkout.
8. **Batch owner questions into one message**, and only for real decisions.
9. **Ticket numbers:** check the highest number in the ledger before creating one. Lanes have collided twice (T332).
10. **Keep STATUS.md current**, newest first and in plain words, so the owner can come back and understand.

## Standing rules (full list in AGENTS.md and memory)
- Never run the LLM parser or the `claude` CLI; give the owner the command.
- Prod is human-only. Staging uploads, staging worker deploys and local release promotes are allowed.
- `marks.jsonl` is append-only. Reviews write through to the curated files on save, and there is no lock step.
- Ask before filtering, renaming or dropping data content. Anything Claude adds is a proposal.
- Heavy jobs go through `scripts/heavy.sh`, one at a time. Never `pkill`; kill only your own PIDs.
- Commit with `git commit -- <paths>`, ending "Co-Authored-By: Claude …".
- Review app: `bash curation-review/run.sh`, then http://127.0.0.1:5173.
