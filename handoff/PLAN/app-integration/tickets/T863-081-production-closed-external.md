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

Owner 2026-11-18 ~18:05, verbatim: "okay so first step will be converting the website from staging to prod".
Owner 2026-11-18 ~18:12, verbatim: "we need to get webapp and bucket released to prod them also cut first prod releases for
iPhone and android which will then be closed testing and internal testing respectively. So let's work towards this. I'm
gone for a little bit but then will be back to do my items."

Revised order (main chat + app chat, 18:10):
1. Main chat: the T846 data release (with the province-wide keep audit's findings) to staging.
2. App chat: 0.8.1 web (app/integrate-081-web: 0.8.1 + T858/T859/T860, T864 to follow) to the staging site. The owner checks
   both on staging.
3. Main chat: rehearse every production script (--dry-run; PRODUCTION.md "first transition" step 1) and build the step-6
   pair (web-prod export + SEO on that r-id) from a clean worktree at the app chat's named commit.
4. Owner, the numbered list the main chat hands over: data worker, data (`release --prod`), feeds worker, site, www
   redirect, smoke.
5. App chat: the first production phone builds from the prod web's commit, as 0.8.2 (= 0.8.1 + web changes + T864);
   Tracks: at ~18:00 the owner said Android closed testing + iPhone external; at ~18:12 "closed testing and internal
   testing respectively" for iPhone and Android. Confirm which track each phone goes to before the consoles step.

Owner ~18:20, verbatim: "We will also what to delete the old data for old app version once everything is settled to avoid
being over 10gb"; ~18:23: "That clean up should be automatic" (-> T865, app chat: upload.sh prunes to live + previous; the
v1 layout goes once the new site is live); ~18:25: "when we flip the switch i will get you to clean that stuff up too"
(Claude turns off bc-fishing-cron and main's three update workflows at the switch, on his word).

## 2026-11-18 ~20:24: tracks decided (owner)
Owner, picking option 2 ("iPhone closed, Android internal") in the app chat's dialog, verbatim: "2 for now until I test
iPhone a bit more my self". So the first production builds go: Android -> Play INTERNAL testing; iPhone -> TestFlight
INTERNAL (the owner's own testing). Play closed testing and TestFlight external (friends) come after he has tested the
iPhone build more; ask again then.
State 20:24: 0.8.1 staging on Play internal (versionCode 36, from d63d6417d); the 0.8.1 web on the staging site (from
1a8832024, smoke 19/0/0 on r-fc137c4040fa). The TestFlight staging build is the owner's to run (the script says so).

Owner ~21:47, verbatim: "I wonder if we should do a final 0.8.2 before closed with everything including the smoother
movement we just discussed wtc"; dialog pick "Yes, wait for 0.8.2 (Recommended)": the production website + data tonight;
the first production phone builds for testers wait for 0.8.2 (T868, T877, T872-T875, T878/T879/T881/T882 after his
yes on pictures, T876 smoother zoom). Data / feed fixes are not gated on it.

## 2026-11-19 ~11:24: 0.8.2 staging build to TestFlight internal (owner)
Owner, verbatim: "yes upload 0.8.2 to test flight" (after Claude's timing answer: external TestFlight = Beta App Review,
separate from App Store review; submit the production build to App Store review with "Manually release" to start the
real review early). Claude started `deploy/staging/build-ios-testflight.sh` (staging data, environment switch on) from
928ac6cc9 (same app code as Play build 38). Next, after the owner's iPhone testing: the 0.8.2 (or 0.8.3) PRODUCTION
iPhone build, then TestFlight external + App Store review (manual release) in parallel.
Owner the same message: his Android 0.8.2 (Play build 38) reads staging data — expected: build 38 is the staging build;
production phone builds come after `release --prod` (his).
**Uploaded 11:29:** 0.8.2 as iOS build 39 (staging data) to App Store Connect; version files app/ios/0.8.2.json + appstore.json on the staging bucket. Owner: add 39 to the internal group.
**11:45:** owner filled TestFlight Test Information, made the External group and submitted build 39 for Beta App Review; he tests internal meanwhile and waits for the external approval (owner: "i will wait for external i already testing internal").
