order: 200

## 2026-11-19 afternoon: 0.8.3 on staging, then 0.8.3 to production (0.8.2 production skipped)

**Owner plan, 2026-11-19 ~12:40:** finish 0.8.3, put it on staging, then take 0.8.3 straight to production (web, Play,
Apple), skipping 0.8.2 production.

**On staging now (15:15):** data release r-6258ffded676 (staging smoke 19 pass, 0 fail), the staging website
(https://staging.canifishthis.ca, app commit 09985e595), the staging feeds worker (gauge pages, T908) and the staging data
worker (T912). Play: 0.8.3 to internal + closed once the benches pass (build number added here). What 0.8.3 brings: faster
pinch-zoom (T903), downloads that go on with the app closed (T912), the simpler trout table (T907), gauge pages (T908),
the steady calendar (T915), the water's own rule-book page at the page foot (T916), desktop map keys as cards (T904), no
bell on the web (T909), Join the test (T911), privacy + speed (T910).

**Production, in this order (all yours; Claude rehearsed every step with --dry-run on 2026-11-19 15:17, all passed):**
1. Feeds worker FIRST (gauge pages, closed-area pieces): `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`
2. Data worker (T912: resumable downloads): `npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml`
3. Data: `CONFIRM_PROD=r-6258ffded676 .venv/bin/python -m pipeline release --prod r-6258ffded676`, then
   `node app/tools/smoke-staging.mjs --production`
4. Android: `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`, then in Play Console that build to Production
   (staged rollout).
5. iPhone: `CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production`, then add it to the
   External group.
6. Say go: Claude deploys the production website from the same commit.
The data must go before or with the phones (0.8.3 reads the new trout row fields).

**Owner 2026-11-19 ~15:25, verbatim: "Can you do as much of the prod release as you can? I give permission".** Done by
Claude: step 1 (feeds worker, version 0a3205f6; gauges, dfo, inseason jobs ok) and step 2 (data worker, version 3c598244).
Step 3 stays yours: `release --prod` refuses any agent and needs a real terminal, by design. Play's production track is
empty (no production access yet), so the production Android build goes to Closed testing, replacing staging build 38.
Owner ~15:42: "apple is now in testing still waiting on android approval" (0.8.2 iPhone approved for external testing;
Play closed build 38 still in Google's review). Asked whether 0.8.3 production should replace 38 on Closed now (likely
restarts the review) or wait for 38's approval: owner picked "Replace 38 now".
**DONE 2026-11-19 ~16:20:** steps 1-4 and 6. Feeds + data workers (Claude), data (you, release --prod; production smoke
18/0), Android production build on Closed testing in place of 38 (Google reviewing it), staging 0.8.3 on Internal, the
production website (Claude; smoke 18/0). Step 5 (iPhone) is uploading; T918 adds it to your External group and sends it
to Apple's review by itself. Yours now: nothing, unless that step reports a problem (then STATUS says what).
