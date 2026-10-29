# T236 — legal disclaimer (About, first run, regs foot) and "Regulations as of … · salmon feed … · in-season …"

Status: DONE — cf41b443 (web walked desktop + phone; native style unit-tested, not walked on a device)

Source: owner 2026-10-28 — approved proposal P2 ("Not legal advice — the B.C. synopsis and DFO notices are the
authority"; "Regulations as of <release date> · salmon feed <age> · in-season <age>" on each water page).

## Built
1. **About**: the boxed authority disclaimer now ends "Nothing in this app is legal advice."; About links to Sources and
   licences (T235).
2. **First run**: `ui-native/src/FirstRunNotice.tsx` — "Before you fish": not the official regulations, not legal
   advice, the synopsis / in-season changes / DFO notices are the authority, you are responsible. "I understand" stores
   `FIRST_RUN_VERSION` (`disclaimer-1`; bump to re-ask after a wording change) via `@app/data/spots`
   `openFirstRunStore` (localStorage `canifishthis.firstRun.v1` / `Documents/spots/first-run.txt`). Closing without
   accepting hides it for this run only. The in-season notice is held back while it is up. Mounted in the Shell.
3. **Every Regulations page foot** (`RegsScreen` `regs-legal`, above "Report a problem"): "Not legal advice — the B.C.
   synopsis and DFO notices are the authority." and core `asOfLine`, e.g. "Regulations as of Aug 28, 2026 · salmon feed
   18h ago · in-season 15h ago". The salmon part is left out on a page with no salmon card; "salmon from the release"
   when the release's own file answers.
4. Words + logic in `core/src/menu/legal.ts` (pinned in `legal.test.ts`); renders in `ui-native/src/Legal.test.tsx`.
   Shots: `handoff/PLAN/app-integration/T236-shots/` (first-run desktop/phone, about, regs-foot desktop/phone).

## Release date — what "as of" reads (decision recorded)
The manifest states no release date, and the release step must not stamp its own clock (AGENTS 19, deterministic
builds). `regsAsOfDay` reads a future `released` manifest field when present, else the newest regulation input the
release read (`sources.synopsis.date` / `sources.dfo.date`), else says "release date not stated". Follow-up (pipeline,
owner call): stamp `released` from the publish step (publish.py, which is allowed a clock) into LATEST/manifest, or keep
the input date.

Depends: T235 (Sources link). Size: S. Owner: no (the `released` stamp is optional). Default: the input date.
