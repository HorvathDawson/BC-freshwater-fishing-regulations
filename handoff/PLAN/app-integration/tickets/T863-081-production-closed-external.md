# T863 — 0.8.1 to friends: production builds, Play closed testing and TestFlight external

Status: IN PROGRESS (app chat + owner) — production builds made: 0.8.3 Android production build 39 on Play Closed testing (in Google's review; replaced 38 on the owner's pick, 2026-11-19); iPhone 0.8.2 build 39 approved for TestFlight External, 0.8.3 build 41 in Beta App Review (added to the External group by T918). Left: Google's approval of Play Closed build 39 and Apple's of build 41 (no action); then the testers are invited (owner).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T863 | 0.8.1 to friends: production builds, Play closed testing and TestFlight external (owner) | IN PROGRESS — production builds on Play Closed (39, Google review) and TestFlight External (0.8.2 approved; 41 in review) | 9caeddba5, 592bba8a5 | owner 2026-11-18 |

Owner 2026-11-18 ~18:00, verbatim (after asking "can i open external iphone testing to send to a few friends to help me
test?"): "lets wait for 0.8.1 then do the prod build then full closed release and external release for andorid and apople".

Owner, verbatim, the same evening (the plan these set is done: production website + data, prod release scripts, the
first production phone builds):
- ~18:05: "okay so first step will be converting the website from staging to prod".
- ~18:12: "we need to get webapp and bucket released to prod them also cut first prod releases for
  iPhone and android which will then be closed testing and internal testing respectively. So let's work towards this. I'm
  gone for a little bit but then will be back to do my items."
- ~18:20: "We will also what to delete the old data for old app version once everything is settled to avoid
  being over 10gb"; ~18:23: "That clean up should be automatic" (-> T865); ~18:25: "when we flip the switch i will get you
  to clean that stuff up too" (Claude turns off bc-fishing-cron and main's three update workflows at the switch, on his word).
- ~20:24, picking option 2 ("iPhone closed, Android internal"): "2 for now until I test iPhone a bit more my self".
- ~21:47: "I wonder if we should do a final 0.8.2 before closed with everything including the smoother
  movement we just discussed wtc"; dialog pick "Yes, wait for 0.8.2 (Recommended)".
- 2026-11-19 ~11:24: "yes upload 0.8.2 to test flight"; 11:45 (build 39 submitted for Beta App Review): "i will wait for
  external i already testing internal".
- 2026-11-21: website "join the Android test" button stays as it is; no Google Group (verbatim: "we will just leave join
  android as it is for now. no google group").

Console steps (owner): Play closed testing track with the production build; TestFlight Test Information
(handoff/PLAN/ios/store/testflight-external.md, "What to Test" in the owner's voice) + the External group + Beta App
Review, then a public link or emails. Submit the production build to App Store review with "Manually release" to start
the real review early.

## Left
Google's approval of Play Closed build 39 and Apple's of build 41 (no action); then the testers are invited (owner).
