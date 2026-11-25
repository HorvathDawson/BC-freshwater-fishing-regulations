# T1057 — `pipeline reports` lists about 40 old items as NEW: tie each id to a ticket or a class

Status: OPEN.

Source: planning chat step 0, 2026-11-24, and the 0.9.1 chat's hand-over ("36 old items, app 0.7.x–0.8.0, up to 2026-11-18, mostly the staging worker"). With both worker tokens: production 5 NEW, staging 35 NEW. Done by the planning chat already: the four production reports of 2026-11-21 are T985's (ids now written there).

## Work
1. Staging crash kinds on 0.7.x–0.8.2 (the `Expected null but found string` style-expression set of 2026-11-18, `in-season matches.json … refused`, `JSON Parse error: Unexpected character: <`, `water map: part 0 … not in its group` (T444?), `no water gnis:1 in the regs store`, `Destination already exists`, `database connection is closing`, `AsyncRequireError`, `Bad response code: 429`, `Error {}`): for each, name the ticket that fixed it or class it, and cite the id in that ticket or in a table here.
2. `06dfbe4725dec36b` ("Update the app to get new regulations", 0.8.2): should be class `expected`, not a defect.
3. `08d0a41bd7a4e79d` NativeExit while foreground, 175 MB, x5 on 0.7.4–0.8.2: say whether anything newer than 0.8.2 still reports it.
4. Two staging reports of 2026-11-10: `442a0ecc670f6fbd` (Indigenous land names show too far out; better satellite: T570?) and `6ecab4230d60ec24` (Fraser conditions look wrong): cite them in their tickets.
5. Tool: a kind seen only on versions older than the oldest supported build should be listed apart, not as NEW.

## Acceptance
`pipeline reports --since 2026-11-10` with both tokens prints 0 NEW apart from items newer than this ticket.

Severity: low (bookkeeping; hides real new items in noise). Release: 0.9.2. Model: Sonnet. Size: S–M. Owner: no.
