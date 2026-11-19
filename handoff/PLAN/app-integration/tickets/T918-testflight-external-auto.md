# T918 — TestFlight: a new iPhone build goes to the External group by itself

Status: IN PROGRESS (owner 2026-11-19 ~15:49).

Owner 2026-11-19 (main chat), verbatim: "when we push new apple one will it update the version automatically for testers"
-> told: no, an External group gets each build only when it is added (and the first build of a version waits for Beta
App Review); "can we make it auto update it for the group too?" -> yes, built here.

## What
After `deploy/staging/build-ios-testflight.sh` uploads, a new step (also runnable alone, for a build already uploaded):
wait until App Store Connect has processed the build, add it to the External group (by name), and submit it for Beta App
Review when Apple asks for one (the first build of a version). It uses the API key the script already reads
(~/.canifishthis/asc-api-key.json -> its .p8), signed in Node (ES256 JWT, no new dependency). Claude never opens those
files; only the script reads them at run time. Production and staging builds both; an internal group is left alone.

Testers' phones: TestFlight's per-app "Automatic Updates" (on by default, as far as known) then installs it.
