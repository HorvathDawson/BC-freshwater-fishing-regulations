# T627 — Digest email: an easy-to-read summary of reports and errors

Status: OPEN

Source: owner 2026-11-10 ("we want a nice easy to read summary"), answering T609's question whether a report's words
belong in the digest email or only a count.

## Work
1. The daily digest opens with one plain line per kind: how many problem reports, how many new error kinds, how many
   repeats, and whether any feed run failed.
2. Under it, each problem report as a short block: when (B.C. time), build, the screen or water, and the reporter's
   words trimmed to a few lines. Each new error kind as one line: message, build, count.
3. Keep T609's caps ("and N more"); the full text stays behind GET /reports and /crashes.
4. Plain text first, readable on a phone; no raw JSON, ids only at the end of a block.
5. A test pins the layout from a fixture day (two reports, three error kinds, one failed run).

## Acceptance
- The owner reads one staging digest on his phone and says it is clear.

Depends: T609. Size: S. Owner: one look.

## Owner 2026-11-11: the look
Summary then short blocks: one line of counts, the feeds line, each report as a short block with trimmed words, new errors
one per line, repeats as a count. Build it.
