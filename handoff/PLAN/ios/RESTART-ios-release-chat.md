# Restart page: the iOS full-release chat (roadmap row 5i, T1071)

Written 2026-11-24 23:20 MST by the iOS release chat. Scope is frozen to T1071: docs and the owner's clicks. This chat
works only in a scratchpad worktree of origin/main, numbers no tickets (the planning chat does), and never types
CONFIRM_PROD. The owner does every App Store Connect click.

## The pages
- `handoff/PLAN/ios/APP-STORE-SUBMISSION.md`: what the owner pastes from, in App Store Connect's order. Section 0 is
  the state table; keep it true.
- `handoff/PLAN/ios/store/listing-090-draft.md`: counts, flagged phrases, the picture check.
- Ticket T1071, section "Owner answers": every answer, the same turn it is given.

## State
| Item | State |
|---|---|
| Paste-from page | written, one review round done (see T1071 Results) |
| Pictures | READY 2026-11-25 02:30 (T1072, PR #192, chat S): direction A, 7 frames, same as Play, in `handoff/PLAN/app-integration/store/graphics/store/light/appstore-6.9/` and `appstore-6.5/`. This chat checked every file: 1320 x 2868 and 1242 x 2688, no alpha. Two nits are with the owner (watershed name under the flow chart on the 6.9" frame 3; tablets are Play only); chat S messages again if a frame is redone. Upload is the owner's (`appstore-screenshots-push.py --commit`, or by hand). |
| Production push (T996) | NOT PROVEN, cause open. 2026-11-24 night: the owner set the three APNs secrets on production (status.json: configured) and sent two wakes: tokens 0. His iPhone XR (iOS 18.5, build 45) never gets a token from iOS ("the request for one was never answered"), Wi-Fi and cellular; other apps on it get pushes. Signing is right on Apple's record; a code read found no defect; simulators have returned tokens. Owner 2026-11-25 00:12: "we should move this to after iphone release I think. we could fix with a patch?" So the proof no longer gates the submission; the second-iPhone test happens when he has one. Then: "registered" there means the XR is the cause, finish the proof on that phone (curl in section 10 of the page, "woken by our server", an hour's watch); a second silent phone means an in-app cause, a ticket from the planning chat and a fix in the 0.10.0 iPhone build. 2026-11-25 00:20: the XR's own log (Console, filter apsd) shows no APNs connection and "asked for publicToken, got (null)": the phone holds no push token at all, so the phone, not the app, is the likely cause. Owner: he tests on a different iPhone on 2026-11-25. Detail in T996. |
| Owner picks 2026-11-24 | linked = strict; all 175 countries; build = newest by then; push proof soon on build 45 (all in T1071). Still his: EU trader status, the Esri imagery call, copyright name, review contact. |
| 0.10.0 iPhone build (was called 0.9.1; renamed by the owner 2026-11-25 03:00, 0.9.2 is now 0.10.1) | relayed by the planning chat 2026-11-24 23:55 (the owner's answer in its chat, not read here): the 0.9.1 chat builds and uploads it after its Play Closed build, with T1098, T1099 and T1101 in one lane before its cut, and tells this chat the build number. The push check is then repeated on that build. |
| iPhone 0.9.0 (45) | uploaded; external TestFlight state WAITING_FOR_BETA_REVIEW at 22:50 |

## Next, in order
1. Privacy page wording (T1098) must be live before the submission; the planning chat routes it.
2. Push proof when the owner has set the secrets: read `https://feeds.canifishthis.ca/status.json`, his phone steps,
   then an hour of watching. Write the result in T996.
3. When chat S says the set is ready: check the files against section 4, then walk the owner through the screens one
   batch at a time, in the page's order (1 account, 2 App Information, 3 App Privacy, 5 age rating, 7 pricing, 4
   version page with pictures, 8 review information, 11 submit), waiting for "done" after each.
4. Tell the planning chat (`bc-freshwater-fishing-regulations-8e`) when it is submitted and when Apple answers; record
   the release in STATUS.

## Step 0 (reports) from a worktree
`<main checkout>/.venv/bin/python -m pipeline reports --no-save` with the main checkout's `.env` loaded. To read the
two workers too, symlink the git-ignored `app/workers/feeds/.run-token` and `.run-token.prod` from the main checkout.
