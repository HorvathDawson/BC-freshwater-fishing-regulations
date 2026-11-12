# T368 — Failures, fallbacks and reporting, app-wide (in 0.5.0)

Status: DONE — 6fed147f1, 84dfd1098 (84dfd1098 takes back T369 hunks 6fed147f1 swept in). Batch 6c of RELEASE-PLAN-0.5.0.
Split out of T333 (the big audit stays after 0.5.0; this part does not wait for it).

## The rule (owner 2026-11-03)
"We don't like callbacks generally they hide issues. Fallbacks ... Make sure we get reports on failures like this ...
This and minimal fallbacks should be true universally in app ... generally reusable and the reporting and error handling
is consistent across the app."

## Scope (app/: core, data, ui, ui-native, apps/mobile, web; workers only where the app reads their answer)
1. **Find** every `catch`, `.catch(`, `?? fallback` on a failed read, `try { } catch {}` that swallows, `console.warn`
   standing in for handling, and stored closure / fire-and-forget that hides an outcome.
2. **Each one becomes one of three:** rethrown; SHOWN to the reader and REPORTED (`reportFailure`,
   app/packages/ui/src/crashReport.ts); or listed with why it is fine (e.g. a best-effort cache write).
3. **One way to show a failure:** a shared face (what went wrong, Retry where it makes sense) instead of per-screen text.
4. **A net:** a scan test like tools/back-layers.test.ts that fails on a new, unlisted catch/fallback.

Out of scope: the pipeline (stays in T333); changing what data the app shows.

## Method
Find with one read-only pass (list file:line + proposed class for each). The owner sees the list once (one question
round for the unclear ones), then fixes land together with the net. Web and native stay one logic path.

Done first: T354's rule-book open (no pdf-lib or outside-app fallback; viewer failure said + reported).

## Known items (found 2026-11-03)
- `waterMapStartIx` (app/packages/ui/src/waterMap.ts): a part the map does not list → index 0, silently (T345).
- Shell.tsx lazy `import("./later")`: a lazy web chunk; the 0.1.2–0.3.0 "Requiring unknown module" crash was one.
- The bundle's Fraser part 2 short name "2 stretches" (no endpoints): a naming item, not a fallback — owner to see.

## Done (2026-11-04) — audit + owner answers: `../T368-audit.md`
- **One reporter:** core `failure.ts` — `reportFailure`, `reportOnce`, `reported(v)` (report, answer v), `offlineAs(v)`
  (offline answers v, anything else thrown on). The ENVIRONMENT (offline, a dropped/timed-out line, a refused permission,
  an abort; `NetError` marks every kind but `http`) is never reported (owner Q1). ui's `reportFailure` is this one; the
  crash reporter is its sink (`setFailureSink`).
- **X1:** `installCrashHandlers` installs Hermes's promise-rejection tracker in every build (RN does only in dev).
  **X2:** `useAsync` reports every hook's failure, once.
- **RETHROW** rows fixed; **S+R** rows report (most were already shown); the update service reports every non-offline
  source failure in `failSource`; map engine errors go to `mapError` (MapScreen's default `onError`); every link out
  goes through `ui-native` `openLink` (logged + reported); trace tiles that fail are no longer cached.
- **Owner answers:** Q3 no search fallback (an unreadable index fails the search: the failure face + report); Q4 access
  notes reported; Q5 an unreadable tile sidecar is a failure (pairing unknown, reported); Q6 no zone data reported once
  a run; Q7 spot fallbacks kept, non-offline failures reported; Q8 day flow: offline waits, a real failure is said
  ("The flow for that day could not be read.", ErrorState) and reported; Q9 unchanged.
- **One face:** `ErrorState` now also draws PartPending's failed state, the glossary failure and search's failed rows.
- **The net:** `app/tools/failures.test.ts` — every catch/`.catch`/two-argument `.then` throws or reports, or its
  file's `FINE` count + reason lists it; mutation pins; links only through `openLink`; MapScreen defaults to `mapError`.
- Audit corrections: chart.ts's catches are prefetch (FINE: `askOnce` keeps no failure); search near is reported by X2.

## Left / for others
- **T369 lane:** `ui/src/openWater.ts` (2 FINE sites at HEAD + 2 new in its in-flight edit) and `ui/src/place.ts` (2 new)
  fail `tools/failures.test.ts` until that lane reports them (`reported(…)`) or lists them. `openWater`'s group/solo
  lookups should become `reported(…)` there (left alone: route code in flight).
- **Owner (Q6 premise):** the app's "today" is the DEVICE's day (`localDate`, zone = the device's), not BC's; the
  Intl fallback only changes the zone label. A BC-day "today" would be a behaviour change — not done here.
- Not merged into the one face (kept, on purpose): the sync sheet (`SyncErrorSheet`, its own Retry list), the Updates
  log's event rows, `WaterMapView`'s status line (T369's file).

## Owner 2026-11-12
Q6 "today": switch to B.C. time (America/Vancouver) - the behaviour change noted above is wanted.
