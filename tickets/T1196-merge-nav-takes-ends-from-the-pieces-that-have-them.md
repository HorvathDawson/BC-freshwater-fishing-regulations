# T1196 — `mergeNav` takes a joined span's ends from the pieces that have them; then Klinaklini and MacMillan stop reading "Mouth → Source"

Status: OPEN — ship order: the app change first, the pipeline follow-up after.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1196 | `mergeNav` takes a joined span's ends from the pieces that have them; then Klinaklini p1 and MacMillan p2 stop reading "Mouth → Source" | OPEN — app first, then the one-line pipeline follow-up | — | the app change (waterNav.ts, ends.ts), its tests, then the pipeline line |

Source: the owner, 2026-11-26 (planning chat), on the #312 final table, verbatim: Klinaklini / MacMillan "Mouth →
Source": "Plain \"Side channel\" / \"3 side channels\" (Recommended)". #312's round 4 did not build it: the reviewer
confirmed that a null end throws in installed apps when `mergeNav` joins pieces. Related: T1172 / #312 (names), T1176,
T1191 (the naming model; its composer replaces these joins later).

## Work
1. **App (first).** When `mergeNav` joins a span of pieces, take the joined span's start and end from the pieces that
   HAVE them. Today a null end throws (`app/packages/core/src/waterNav.ts` around lines 300 and 421; `ends.ts`
   lines 48-49: `spanName`). A joined span with no end at all reads as the plain count, not as a throw.
2. A test: join a piece with both ends to one with a null end, and two null ends; the first keeps its ends, the
   second gives the plain label, none throws.
3. **Pipeline (after the app has shipped).** The one-line follow-up that stops the data writing "Mouth → Source" on
   Klinaklini p1 and MacMillan p2: the plain "Side channel" / "3 side channels". It must not reach a phone before
   the app change, or an installed 0.9.x app throws. Releases: the app first, the data after (display version stays 20).

## Acceptance
`mergeNav` and `spanName` never throw on a null end (a test fails without the change); the two waters read "Side
channel" / "3 side channels" in the app and on the web after the data follows.

Depends: #312 (merged). Severity: medium (a throw on a real water once the data changes). Model: Sonnet. Size: S (app), XS (pipeline). Owner: no.
