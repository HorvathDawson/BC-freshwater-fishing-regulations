# T924 — Layers sheet: a tester found the wording "a little weird"

Status: DONE (landed on redesign/stream-sections 2026-11-20 01:50 in 3c057089f 820fbcf32 32135b5d2; owner approved the pictures).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T924 | Layers sheet wording read "a little weird" (tester via owner) | DONE | 3c057089f 820fbcf32 32135b5d2 | tester 2026-11-19 via owner |

Source: a tester's feedback relayed by the owner 2026-11-19 (`T922-shots/tester-2026-11-19-1.png`, the Layers sheet: Map
"Drawn · works offline", Satellite "Esri imagery · online only", Corridors "Detail imagery along a stream · COMING SOON";
Stocked / Depth "NO DATA YET"). Tester, verbatim: "Will the detailed imagery tab work on the offline mode?" then "This is
worded a little weird to me". Owner: "The goal is to make detail imagery work offline. Basically it would work you
download a corridor for specific rivers you want offline since they are big."

## Work
Ask the owner (batched) which words read weird, with a proposal: plain words per tile, e.g. Satellite "Photo map (needs
internet)", Corridors "Close-up photos along rivers — download for offline (coming soon)", Stocked / Depth "Coming soon"
in place of "NO DATA YET". Pictures before/after.
Depends: none. Size: S. Owner: yes (which words). Default: the proposal above, shown as pictures first.

## Owner 2026-11-20 00:53 (main chat): picked "Plain words (Recommended)"
Map "Works offline"; Satellite "Photo map · needs internet"; Corridors "Close-up photos along rivers, download for
offline" [COMING SOON]; Stocked / Depth tag COMING SOON (was NO DATA YET). The "Needs a connection" tag (offline) stays.
## Built (lane/t924)
`core/src/layers.ts` notes + the no-data tag; layers.test.ts + LayersSheet.test.tsx pins. Pictures before landing.
## Owner 2026-11-20 ~01:40 (main chat): pictures approved
Verbatim: "Layers good".
