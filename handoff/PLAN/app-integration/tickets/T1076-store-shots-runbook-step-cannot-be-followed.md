# T1076 — The release runbook's "reshoot store pictures" step cannot be followed: the tool needs a store-shots build of the site

Status: IN REVIEW — PR fix/T1076-store-shots-own-export (code 6cd9531c3): the owner's merge and one real release reshoot with the runbook line.

Source: the 0.9.0 chat, 2026-11-24, during the 0.9.0 release. Runbook Step 7 item 8 says to reshoot the store
pictures (`node app/tools/store-shots/run.mjs`) before the Play listing update, but the tool needs the staging site
built with `EXPO_PUBLIC_STORE_SHOTS=1`, which the normal staging site deploy does not set. The 0.9.0 run failed:
phone/07-saved-spot timed out (spots-screen) and phone/20-help "shows the website's lines, not the app's". Nothing was
delivered; the current Play listing pictures stay. Evidence: the tool's report.json under app/dist/store-shots/play-phone
in the main checkout (not committed).

## Work
1. One of: the tool builds and serves its own store-shots web export locally (preferred: no dependence on what
   staging carries), or the runbook step says exactly how to build and where to point the tool.
2. The tool refuses at the start, with that instruction, when the site it is pointed at is not a store-shots build
   (today it fails two frames later with a timeout).
3. Rerun for the current release and deliver the set (feeds T1072 and T319).

## Acceptance
The runbook step followed as written produces every frame on a fresh checkout; pointing the tool at a normal build
stops at once with the reason.

## Done (2026-11-24, built; merge pending)
`app/tools/store-shots/site.mjs` builds the store-shots web export of the checkout (`EXPO_PUBLIC_STORE_SHOTS=1`, staging app
env as `export:web:staging`, into `app/dist/web-store-shots`), writes the marker `store-shots-build.json`, and serves it on a
free local port it closes at the end. `run.mjs` and `store-shots.mjs` use it when no site is given (`--reuse` skips the build;
`--site`/`STORE_SHOTS_SITE` still work). Any site without the marker is refused before the first scene, naming what to do
(play-shots.mjs has no default site any more). Runbook Step 7 item 8 and PRODUCTION.md say
`scripts/heavy.sh store-shots 30 -- node app/tools/store-shots/run.mjs`. Tests: `store-shots/site.test.ts`.

Depends: none (T319 owns the pictures themselves). Severity: low (listing pictures only). Release: with the store
content chat (T1072), before the next Play listing update. Model: Sonnet. Size: S. Owner: no.
