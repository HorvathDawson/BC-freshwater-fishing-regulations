order: 740.15625

## 2026-11-24: 0.9.1 overnight questions (as asked; the answers are in the section above)

Pictures and options for all eight are on one page: https://claude.ai/artifact/UZhX9gBLs6JiynhgVcWuFR (the 0.9.1 chat asks
them in chat; answers go here verbatim).

- **Know first (no answer needed):** every Android build with R8 on (new in 0.9.0) stripped a WorkManager constructor, so
  the background check never ran with the app closed (no gauge alerts, no update notices). Fixed on main (T1027, PR #109);
  the 0.9.0 chat cherry-picks it into 0.9.0 before its Play build.
- **T905 salmon feed and salmon file sizes: which way down?** Neither gets under its old size without a format change
  that installed 0.9.0 apps refuse. Options 1–4 in the ticket: keep the budgets; move the worker's own state out of the
  feed (safe, about 121,700 B, a worker deploy); new feed and head formats in 0.9.1; the same with the feed waiting.
  Default until answered: budgets stay (135,000 B / 110,000 B gz). Recommended: option 2.
- **T1047 datum changes, the other three: accept / reject?** (08HD006 and 08EF001 answered 2026-11-24, below.)
  07FD001 (+94.826 m from 2026-08-05) and 08KH019 (+1.011 m from 2026-07-07) only after the this-year line is fixed or
  2027 begins (the file refuses them until then). 08MH153: recommended reject (a slow creep, not a step). Default: the
  three stay proposed; nothing changes. Pictures: `handoff/PLAN/app-integration/T1047-shots/`.
- **T1030 "N waters changed": what counts?** A: wording changes too. B (built): rules only. C: rules only, nothing shown
  at 0. With B, 0.9.0 would have read "2 waters changed" though 8,247 waters were reworded. Recommended: C.
- **T1029 one-bank stripe, Q4–Q6** (a prototype in draft PR #112, not merged): the stripe from zoom 13; Q4 no stripe on
  a polygon reach (c); Q5 no stripe on the 12 tributaries; Q6 the rule book's west half gets the stripe. Those are the
  lane's recommendations; nothing is built into the app until answered.
- **T1016 the two notice texts** (merged with these as default): "This page's data was replaced by a newer release. /
  Load the new data ›" and "The app was updated since this page opened. / Reload ›". Pictures:
  `handoff/PLAN/app-integration/T1016-shots/`. Also yours to say go on: the feeds worker deploy that classes these
  reports as expected.
- **T1031 trout rows:** A (merged): every page behind a row on its Source line. B: every rule in the row's breakdown
  too (a Python change and a data release). Pictures: `handoff/PLAN/app-integration/T1031-shots/` (`rowpages-*`).
- **T1036 "All sources":** should a species sheet's own rules, and Possession, be listed in the page's sheet? Built: no.
  Pictures: `handoff/PLAN/app-integration/T1036-shots/`.
- **T1024 stub polygons: look at the after shots** (the ring alone on a selected river polygon whose route is a short
  stub; needs the 0.9.1 tile build). On the page above.
- Store checks 2026-11-24 00:50: TestFlight 6 items, all already ticketed (T982); Play none; production worker nothing
  new since the last check. With the worker tokens the reports command also lists 36 old items (app 0.7.x–0.8.0, up to
  2026-11-18, mostly the staging worker) as never ticketed: handed to the planning chat, not ticketed by the 0.9.1 chat.
