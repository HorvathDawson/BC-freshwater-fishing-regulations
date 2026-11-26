# T1159 — Before the cut: one owner pack (every look, wording and call, with pictures) and a gate; after the cut only stop-ship questions

Status: OPEN — the first patch's cut is the trial; runbook page by the planning chat, the gate is small release tooling

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1159 | Before the cut: one owner pack (every look, wording and call, with pictures) and a gate; after the cut only stop-ship questions | OPEN — the first patch's cut is the trial; runbook page by the planning chat, the gate is small release tooling | — | planning chat: the "Before the cut / Release night" runbook page + the pack for the first patch; a build lane: `release --cut` lists open owner lines and refuses (escape flag with a reason); S–M |

Source: two read-only passes of 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `fewer-questions.md`,
`release-timeline.md`). Measured: the machine work of a release is small (data build 29-34 min, a clean run of the
checks about 25 min; about 4 h of the 17.1 h from cut to merge-back for 0.9.0); **late scope and late answers
cost the most.** 0.10.0 was cut at 07:20 with production expected 16:30 the same day and was held for three
tickets added after the cut (T1140, T1146, T1145), one forcing a second build; the owner answered more than 20
multiple-choice questions on release night, several for features never in the release (T1108, T1119, T1132);
0.9.0's What's-new words were drafted at 22:26 and cleared at 08:14; his production steps were spread over about
7 h; six chats ran on 0.10.0's release day. Late questions fall into three groups: looks that only exist after a
build, late investigation finds (the only group that cannot be asked earlier), and store or legal answers asked
screen by screen. Many tickets tell him to look at repo folders he cannot open.

## Work
1. A runbook page "Before the cut / Release night" (from the notes' section 4): what must be in hand before
   `release --cut`; the stop-ship definition; after the cut a find goes to the next patch unless it is stop-ship
   (a wrong rule shown, a crash, a store rejection), and any change he asks for after the cut is shown with its
   cost in hours first.
2. The owner pack: one page per release, built before the cut from the open `## Owner` lines of the release's
   tickets, with defaults and the pictures INLINE (a published private page he can open, since he cannot open repo
   paths), the What's-new words, the store answers; one sitting.
3. The gate: `release --cut` lists every ticket in the release with an open owner line, pictures to see or
   What's-new words to clear, and refuses until the list is empty (an escape flag that records a reason).
4. One prepared production sitting: every dry run done, then the ordered commands on one page (final tag, data,
   site, feeds, Android, iPhone), each still his alone.
5. An early "late-finder" audit before each cut (unnamed parts, rule-gap channels, straddlers, colour tokens, the
   store checklist), so finds like T1146, T1149 and T1150 appear before the cut, not after.
6. Release day: only the release chat and the planning chat run; feature scoping questions wait.

## Acceptance
The first patch after 0.10.0 is cut with the pack answered beforehand; the count of owner questions after the cut
and the hours from cut to production are recorded in the ticket and compared with 0.10.0. Target from the
timeline pass: a patch from an evening freeze to production before noon, with about 1.5 h of the owner's time.
Severity: process. Size: S–M. Owner: the picks below.

## Owner

- [x] 2026-11-25 — Picks (defaults first): after the cut only stop-ship questions reach you, with the cost in hours shown for any change; one pre-cut owner pack as a page with inline pictures; agents may decide with a recorded default in the cases the notes list; no multiple-choice questions between 20:00 and morning except stop-ship; only the release and planning chats run on release day; one prepared production sitting. → "what is a cut here? a release? i think recommendation is good but questions uo to midnight is okay then batch them after (unless urgent)" (2026-11-25 23:27; quiet hours start at midnight, not 20:00)
- [x] 2026-11-26 — The four speed items (the web-only release path and the data floor check, T1165; the pre-cut gate, T1159; the first steps of T1133; the public GitHub Release file for the MapLibre library, T1170) → "Yes to all four" (2026-11-26; also "Yes to all four; relay it", 2026-11-26 01:54)
