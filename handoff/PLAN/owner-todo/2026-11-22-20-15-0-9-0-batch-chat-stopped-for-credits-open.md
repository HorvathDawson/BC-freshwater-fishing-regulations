order: 540

## 2026-11-22 20:15: 0.9.0 batch chat (stopped for credits) — open for you
- **Questions for the next chat's round (pictures in each ticket's shots folder):** T1010 region page look (A, B or C;
  A recommended), T1004 Class II tag (A now / B beside the name; B recommended), T940 satellite colour set (approve?),
  T991 sources pattern (A recommended), T997 Nicomekl labels after shots, T867 notifications inbox (needs your answer
  before it is built), R8 at ~72% in every category: enough? (recommended yes), T981's 5 municipal proposals, the 0.9.0
  "What's new" bullets, and any Play reviews or closed-testing feedback.
- **Production steps (you, CONFIRM_PROD):** T1014 copy the Play version file T1014 names from the staging bucket to production
  R2 (3 production phones get a 404 on their update check); T996 the three APNs secrets on the production feeds worker
  (`npx --prefix app wrangler secret put APNS_KEY -c app/workers/feeds/wrangler.prod.toml`, then APNS_KEY_ID and
  APNS_TEAM_ID), then the phone test on the 0.9.0 TestFlight build (steps in T996).
- **To check (you):** T1015 what Google renders on /water/ pages (Search Console, URL inspection).
- **Optional (you):** T1007 the Chrome "StorageType.persistent" warning is Cloudflare's injected bot script: turn off
  Cloudflare Bots / JavaScript Detections to silence it.
