# T391 — Every web water page filed a fatal "Requiring unknown module" crash

Status: DONE 2026-11-04 (found reading the 0.5.0 crash reports).

**Reports:** kind 5f6c515b (0.5.0 web, 9 then 10, 5 installs: the staging smoke runs and a fresh visit) and e7557774
(0.3.0 web, same cause). Fatal, route water / other.

**Cause:** Metro's `import()` (Expo `asyncRequireModule`) first asks for the module at once and fetches its chunk only
when that throws. The web bundle installs `global.ErrorUtils` (RN's error-guard polyfill), so Metro's
`guardedLoadModule` does not throw the miss: it reports it as a fatal error and returns nothing. The crash reporter
recorded a fatal on every visit that opened a lazy part (`later` chunk, T282) before the idle preload. The smoke test
only WARNs on console errors, so it stayed green.

**Fix:** `part.tsx` `unguarded()`: on the web, the synchronous `import()` call runs with `ErrorUtils` unset (restored
right after), so the miss throws and the chunk is fetched; the module's own code still runs guarded later. Test in
`part.test.tsx`.

**Verified:** the live water page `/water/bench-creek` with the current bundle posts a fatal crash/2 (route water); the
same page with the fixed bundle swapped in posts none, fetches `later-*.js` once, and shows Bench Creek. Live after the
next staging site deploy.
