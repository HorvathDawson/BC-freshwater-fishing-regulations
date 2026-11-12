# T462 — Audit: does every screen show the regulations the release holds, and the synopsis says?

Status: DONE 2026-11-11 — 85ba2e821 (the audit: `handoff/PLAN/app-integration/T462-audit.md`, 30 findings, read-only; no tickets filed yet: the owner decides which findings become work).

Owner: "maybe ultra is good for doing some final audits on the code base and how the regulation data is presented."

## What
A read-only audit across the whole path a rule takes: synopsis → curated → release blob → core reads → hooks → every
screen that shows it (map colour, legend, the water page's Regulations tab, step mode, search results, the PDF jump,
the web water pages). Each claim on screen must trace to the release, and the release to the synopsis page it cites.

## Dimensions (one agent each, then an adversarial verify pass on every finding)
1. **Status:** open/closed/closed-to-a-species on the map colour = the Regulations tab = the web page, for the same
   day and the same part (rule: one load path per screen, T369).
2. **Dates and seasons:** `when` windows, year-round vs seasonal, the day picker, BC clock edges (midnight, DST).
3. **Quotas, sizes, gear:** `lengths`, gear/while/conduct, bait bans, fly-only, barbless; the known gear rework issues.
4. **Base + overrides:** each section = region base + named overrides, every difference attributable
   (base-plus-deltas invariant); zone rules; "both hold" notes; unplaced extent rules (137 bound nowhere).
5. **Geography edge cases:** side channels and braids (T460's Fraser channel), straddling sections, lakes vs their
   river, tidal boundary, region boundaries mid-river.
6. **Words:** every sentence is composed in core and pinned by a test (rule 33). No raw ids or codes shown.
   Citations go to the printed page.
7. **Failures:** a missing or unreadable rule is said and reported, never shown as "open" (rule 46, T333).

## Rules
- **Read-only.** Findings become tickets. No fixes inside the audit.
- **Examples are picked by stream key + start**, never a raw section id.
- **It reads the live release** (data/release/LATEST) and `data/curated/` only. Parser runs are human-only, and
  nothing here needs them.
- **Each finding:** what the screen says, what the release and synopsis say, the file:line, and how many waters it hits.

## When
It can start while other lanes work, since it reads the release, which is fixed, and the regs code, which other lanes
are not changing. If it starts before 0.6.8, re-check its findings against HEAD before filing tickets.

Related: T463 (code review), T369, T333, T387. Size: L (workflow). Owner: decides which findings become work.

## Owner 2026-11-12
G6 Pend d'Oreille River: the printed exemption lifts both rules; barbed and treble allowed (option 1, no longer the stricter default).
