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
| Walk-through | ALL SCREENS DONE 2026-11-25 14:48 except the last three clicks (select the 0.10.0 build, Add for Review, Submit). State and the owner's answers: section 0 of `APP-STORE-SUBMISSION.md` and T1071 "Owner answers". Apple changed the required screenshot size to 6.1"/6.3" (1206 x 2622); a resized stopgap is uploaded, the native set is the store chat's PR #238 (it messages when merged; offer him the re-upload). Header and Search Results pictures = T1134 (store chat), optional. Submit also waits on the privacy page (T1098) live on production (his site command). |
| Production push (T996) | NOT PROVEN, cause open. 2026-11-24 night: the owner set the three APNs secrets on production (status.json: configured) and sent two wakes: tokens 0. His iPhone XR (iOS 18.5, build 45) never gets a token from iOS ("the request for one was never answered"), Wi-Fi and cellular; other apps on it get pushes. Signing is right on Apple's record; a code read found no defect; simulators have returned tokens. Owner 2026-11-25 00:12: "we should move this to after iphone release I think. we could fix with a patch?" So the proof no longer gates the submission; the second-iPhone test happens when he has one. Then: "registered" there means the XR is the cause, finish the proof on that phone (curl in section 10 of the page, "woken by our server", an hour's watch); a second silent phone means an in-app cause, a ticket from the planning chat and a fix in the 0.10.0 iPhone build. 2026-11-25 00:20: the XR's own log (Console, filter apsd) shows no APNs connection and "asked for publicToken, got (null)": the phone holds no push token at all, so the phone, not the app, is the likely cause. Owner: he tests on a different iPhone on 2026-11-25. Detail in T996. |
| Owner picks 2026-11-24 | linked = strict; all 175 countries; build = newest by then; push proof soon on build 45 (all in T1071). Still his: EU trader status, the Esri imagery call, copyright name, review contact. |
| 0.10.0 iPhone build (was called 0.9.1; renamed by the owner 2026-11-25 03:00, 0.9.2 is now 0.10.1) | relayed by the planning chat 2026-11-24 23:55 (the owner's answer in its chat, not read here): the 0.9.1 chat builds and uploads it after its Play Closed build, with T1098, T1099 and T1101 in one lane before its cut, and tells this chat the build number. The push check is then repeated on that build. |
| iPhone 0.9.0 (45) | uploaded; external TestFlight state WAITING_FOR_BETA_REVIEW at 22:50 |

## Next, in order
Changed 2026-11-25 15:20 by the owner (relayed by the planning chat): the repo moves first and 0.10.0 ships after it,
with the fix for T1140. So no iPhone 0.10.0 build and no privacy page on production on 11-25; probably 11-26.
Every App Store Connect screen stays as saved.
1. Repo transfer to `canifishthis/canifishthis`. The repo-move chat (`bc-freshwater-fishing-regulations-38`) says
   when to update the remote; until then open no new PR from this worktree. Checked 2026-11-25: nothing entered in
   App Store Connect names GitHub (support, marketing and privacy URLs are canifishthis.ca; the description, the
   review notes and the live privacy and about pages hold no GitHub link), so the move changes nothing there.
2. The T1140 fix, then staging again (the 0.10.0 chat).
3. The owner's production commands. The site one puts the privacy page (T1098) live: read
   `https://canifishthis.ca/privacy` afterwards and check the new words are there.
4. The iPhone 0.10.0 build on TestFlight (number 46 expected, not confirmed; the planning chat sends it).
5. The owner's last three clicks: select the build on the version page, Add for Review, Submit (section 11 of
   `APP-STORE-SUBMISSION.md`). Offer him first, if on main: the native 6.3" screenshots (`appstore-6.3`, PR #238)
   in place of the resized stopgap, and the Header and Search Results pictures (T1134); neither is a gate.
6. Tell the planning chat (`bc-freshwater-fishing-regulations-8e`) when it is submitted and when Apple answers; record
   the release in STATUS.
7. After the release: the push proof (T996) on a second iPhone, see the State table.

## Step 0 (reports) from a worktree
`<main checkout>/.venv/bin/python -m pipeline reports --no-save` with the main checkout's `.env` loaded. To read the
two workers too, symlink the git-ignored `app/workers/feeds/.run-token` and `.run-token.prod` from the main checkout.
