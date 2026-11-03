# T368 — Failures, fallbacks and reporting, app-wide (in 0.5.0)

Status: OPEN — batch 6c of RELEASE-PLAN-0.5.0 (owner 2026-11-03: "The reporting / fallback audit should be in 0.5.0").
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
