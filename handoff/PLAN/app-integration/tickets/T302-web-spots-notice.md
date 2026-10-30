# T302 — Spots on the web app: a notice instead (phone app only for now; accounts coming)

Status: OPEN (owner 2026-10-30: "make it so spots in the webapp instead just say something about how they work locally on
the phone app but they don't sync to web yet. Coming soon will be accounts etc").

## Work
1. On web, the Spots tab shows a short, friendly notice instead of the spot list/capture: spots are saved on your phone in the
   Can I Fish This? app; they don't sync to the website yet; accounts that sync phone and web are coming. A "Get the app"
   link (Play listing; the test link while internal) — no store badge art we don't have.
2. Web hides "Save a spot" entry points (map long-press / sheet button) or routes them to the same notice.
3. The T293 walkthrough's spot steps are skipped on web (or replaced by one step pointing at the notice); the Help text says
   spots are phone-only for now.
4. Existing web-saved spots (localStorage/IndexedDB from earlier testing): left untouched, just not shown — say so in the ticket.
Words in core tables; phone app unchanged.

## Acceptance
Web screenshots (phone + desktop) of the Spots tab and a save attempt; walkthrough on web skips spots; phone unchanged.

Depends: T293, T294. Size: S. Owner: no.
