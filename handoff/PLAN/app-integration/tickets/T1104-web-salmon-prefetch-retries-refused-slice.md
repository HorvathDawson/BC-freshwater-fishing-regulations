# T1104 — Web: the salmon prefetch asked for a refused slice up to 4 times in a minute

Status: OPEN.

Source: 0.9.1 chat 2026-11-24 ~23:40, seen by the T1029 lane on the web: a salmon slice the server refused was
requested again up to 4 times within a minute, although the code says no retry before the next resume.

## Work
1. Reproduce with a refused slice (404 / 403) and count the requests; find which callers re-enter (several screens
   asking for the same slice, or the refusal not being remembered across them).
2. One refusal is remembered for the session leg (until the next resume), whoever asks; it is shown and reported
   once (`no-silent-fallbacks`), not once per caller.
3. Test: N callers, one refused slice, one request.

## Acceptance
The test fails without the change; the network panel shows one request per refused slice per resume.

Related: T1091 (the salmon head and the full facts), T1016. Severity: low. Release: 0.9.2. Model: Opus (with T1091,
same code). Size: S. Owner: no.
