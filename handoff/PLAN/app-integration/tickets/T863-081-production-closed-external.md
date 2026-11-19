# T863 — 0.8.1 to friends: production builds, Play closed testing and TestFlight external

Status: OPEN (app chat + owner; after the 0.8.1 phone build).

Owner 2026-11-18 ~18:00, verbatim (after asking "can i open external iphone testing to send to a few friends to help me
test?"): "lets wait for 0.8.1 then do the prod build then full closed release and external release for andorid and apople".

Order:
1. 0.8.1 phone build (staging data) to Play internal + TestFlight internal (app chat; benches first).
2. The owner promotes the data release to production (human; prod R2) so a production build has its data.
3. PRODUCTION builds of 0.8.1 (data.canifishthis.ca, no environment switch, no map lab): Android
   `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh` (writes the production play.json, so the owner runs it), iOS
   `deploy/staging/build-ios-testflight.sh --production` (owner runs it or says go).
4. The owner, in the consoles: Play closed testing track (testers by email list or Google Group) with the production
   build; TestFlight Test Information (handoff/PLAN/ios/store/testflight-external.md, "What to Test" in the owner's
   voice) + an External group + Beta App Review (about a day for 0.8.1's first build), then a public link or emails.
