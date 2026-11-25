order: 770

## 2026-11-25 morning: the release is 0.10.0; what is yours today (planning chat)

- **The number.** Asked 03:00, your pick: "0.10.0, follow the rule (Recommended)". The release that was called 0.9.1
  is cut as 0.10.0. Renamed with it: the hardening release "0.9.2" is now 0.10.1, the feature release "0.10" (stocked
  lakes, bathymetry, corridors, boat launches, Mapbox) is now 0.11. The roadmap's top says so; ticket bodies and your
  quoted words keep the old names.
- **Do (you): the production commands for 0.10.0**, in the order the 0.10.0 chat hands them to you with the dry runs:
  data, then the site, then the feeds worker. `pipeline release --prod` refuses in every Claude session by code, so
  no chat ran them. The site deploy is what puts the new privacy page (T1098) live.
- **Do (you): clear the What's-new words** the 0.10.0 chat drafted as a proposal (its morning summary).
- **Do (you): the App Store submission**, with the iOS chat's walk-through (batch 1 started 02:55). The picture set
  is on main and passes Apple's sizes. The final submit waits on the 0.10.0 iPhone build number and the privacy page
  live on production.
- **Closed:** your ponds answer (Q124 / T153) is built as a test in PR #166, merged 02:50. The head-lakes list, the
  privacy sentences, the gauges card and the stripe look were chosen by the 0.10.0 chat on your word ("I told the
  agent they could do these without me"); what it chose is in the item of 2026-11-24 (planning chat) and its morning
  summary, each one a proposal you can change in 0.10.1.
- **The one-time permission of 00:30 was not used by the planning chat:** it ran no production command. It ends
  with this release.
- **12:10 state (from the 0.10.0 chat):** the Mac slept from about 03:50 to 12:06 (lid closed), so the night
  release stopped right after the cut. release/0.10.0 is cut and the new atlas is promoted; nothing is on staging
  yet. Today it runs the rehearsal build, staging, the Android and iPhone builds, then hands you the production
  commands. For a night run: keep the lid open, or the Mac on power with sleep off.
- **12:10, your words: "it is the next day now. we will work on all this. plus repo swap".** The repo-move chat's
  prompt is rewritten in the roadmap (chat 3): it prepares now and changes nothing live; the move itself comes after
  0.10.0 is on production and merged back (the planning chat's order: a move in the middle of a release would change
  the remote under the release branch, CI and the feeds workers' token). T1120, the fresh-clone proof, is its step 4.
- **Do (you), before any store upload: clear 34 old pictures from the main checkout.** They sit under
  handoff/PLAN/app-integration/store/graphics/store/light/ (dated Nov 5, the old set; the store chat confirms they
  are not needed). The upload scripts take every picture in a folder, so from that checkout old pictures would mix
  into the listing (T1127 fixes the scripts). When the 0.10.0 release no longer runs from that checkout: `git clean
  -n` on that one folder to see the list, then `git clean -f` on it; or upload from a clean checkout of main. Then
  `--plan` must show 7 pictures per size. No chat deletes them for you.
  **Answered 12:15, your words: "we can remove old store photos. we want this to be clean".** So it is no longer
  yours to do: the 0.10.0 chat, which holds that checkout, removes them (a dry run first, that one folder only, stop
  if anything else is listed), and the store chat builds T1127 now so a leftover can never ride along again.
- **Answered 12:33, store pictures (T1072), your words: "Drop tablet set. And you can get them to fix picture".** The
  store chat drops the Play tablet sets and redoes frame 3 (it showed "Lillooet watershed" under a Lower Fraser
  creek). If the wrong label comes from the app and not the shot, it becomes a ticket. T1127 (the upload scripts
  refuse stray pictures) is built: PR #207.
