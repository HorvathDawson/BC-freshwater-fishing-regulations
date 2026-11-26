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
- **Closed 13:04: the 34 old store pictures are gone from the main checkout**, deleted by the 0.10.0 chat on your
  direct yes in its chat (dry run rechecked first; after it nothing untracked under that folder, the 35 current
  pictures untouched, the checkout clean).
- **13:04, CI on release/0.10.0 was red** on one pipeline test (a fixed 10-character compare that cuts "0.10.0"; a
  test fault, confirmed by the 0.10.0 chat from the code). Its fix goes to main, then onto the release branch. The
  production builds' CI gate refuses until it is green, so the afternoon times may slip.
- **Answered 13:23 in the iOS chat's walk-through (relayed):** EU trader status = "Not a trader" (in T1071). On content
  rights you said "we will be changing it to mapbox soon": that is T1108 (the feature release, now 0.11); this
  submission answers for Esri as the app ships today.
- **Answered 13:43 in the store chat (relayed 13:48), the Play pictures: "hold play p[ictures until before prods
  release".** No Play picture upload until just before the Play production release; T1128 (the two phone-width
  layout faults visible in frame 3) is fixed and the frame reshot first. A words-only Play update before then
  leaves the pictures alone. The App Store pictures are not held. Also yours that day: "yes" to making the store
  pictures in a GitHub workflow started by hand, and "can be part of new repo transfer": T1131, with the repo-move
  chat after the transfer.
- **Check (you), one word:** "before prods release" for the Play pictures is read by the planning chat as the Play
  production release (the public launch), not today's 0.10.0 data release. Say so if you meant today's.
- **Decide (you), do or drop:** two parts of T1072 were never started and keep it open: the website landing and
  share-page words, and the store video (T709). The store chat's scope is otherwise done (13:50).
- **Answered in the iOS chat's walk-through (relayed 14:06), App Privacy:** you kept "Performance Data" ticked, your
  words: "keep ticked but next release will add to rpivacy page and add the performance monitor". Ticket T1132, in
  0.10.1. Until it ships the App Store label is one data type ahead of the app. Yours then: the picks on a one-page
  design (what is measured, a switch or not, linked to an install or not) and Play's Data safety clicks.
- **14:50, release state (from the 0.10.0 chat):** nothing on staging yet. The staging release stopped three times on
  checks that read the live release (all cleared; ticket T1135 so lanes see them before merge); its 4th run is in
  the follow tests. If it goes through: staging data about 15:35, site and worker about 16:15, your production
  commands about 16:30, Android on Closed about 17:30, iPhone on TestFlight about 18:30 (build 46 expected). Each
  further stop adds about 45 minutes. The binding diff it accepted is exactly your head-lakes list: 46 rules, only
  the Anderson Lake row loses sections.
- **Answer (you), one line:** a file `data/generated/owner1028/one.py` sits untracked in the main checkout and makes
  two scanners fail pushes from there (T1137). Is it yours and still needed? No chat deletes it.
- **App Store (from the iOS chat, relayed):** every screen that needs no build is done and confirmed by you; App
  Privacy is published. Left: select the 0.10.0 build, Add for Review, Submit, after the build is on TestFlight and
  the privacy page is live on production (your site command).
- **14:58, your report with two pictures: the Squamish River link opens on the ecological-reserve part and reads
  "Closed", with nothing saying which part.** Ticket T1140, HIGH, the first patch after 0.10.0. Yours: pick where a
  multi-part water's link lands (A the Map with the parts, your proposal; B a whole-water summary first; C the
  longest part, labelled), from pictures. A list of every water this affects comes first.
- **15:08, T1140's list is in:** 478 waters have more than one part. On 89 the link opens on a closed part while
  another is open (the Squamish). On **156 it opens on an OPEN part while another part is closed today** (the
  Capilano, Chilliwack, Fraser, Skeena, Cowichan and more); most of those do name the stretch they show, but none
  says other parts exist. Proposed: a small first fix (every head says "Part N of M" and "other parts have different
  rules"), web first, then the landing you pick.
- **Answered 15:14 (multiple choice): "Yes, move now, re-cut 0.10.0 after"; T1140 landing "A: Map with the parts"; the
  label "web first right after repo move and make repo move happen asdap".** What happens: the staging run
  finishes; then the transfer click (the repo-move chat hands it to you on the planning chat's word, about 20
  minutes with you); then the merge queue (your OK), the T1140 label fix, the staging step again, and only then
  your production commands, the phone builds and the App Store submit: probably tomorrow, not today. Done as a fix
  on the same release branch, not a second cut (a second cut would collide with the 0.10.0 tag): same result,
  version 0.10.0, with the label in it. Yours on the way: the transfer and the token; the merge-queue OK; a yes on
  the label's words from pictures; then the production commands.
- **15:29, the order after your word to the repo-move chat ("the release candidate structure is first then 0.10.0
  happens right after. with other items"):** the staging run was stopped clean before it could tag 0.10.0 (nothing
  uploaded, nothing lost; what it proved is kept). The release-candidate build goes to main first, then the release
  branch takes it with the link-label fix and the repo's new name, staging ships as 0.10.0-rc.1, and then come your
  production commands, the phone builds and the App Store submit, probably tomorrow. Yours meanwhile: the exact
  expiry date of the new worker token (from your token page), a yes on the label's words from pictures, and the
  clean-up chat's prompt when the planning chat sends it. Disk: 37 GB free on the Mac (T1142).
