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
| Pictures | owner 2026-11-24: one set for both stores, new images, and the submission WAITS for them (T1072, chat S). Chat S messages this chat when the set is ready. The 0.8.2 set (outside the repo, `~/bcfish-session-backup-2026-11-12/store-t319-final-082/`) passes Apple's sizes and is the fallback only if the owner changes his mind. |
| Production push (T996) | not proven: production worker lacks the three APNs secrets. Owner's step; the steps are section 10 of the page. |
| Owner picks 2026-11-24 | linked = strict; all 175 countries; build = newest by then; push proof soon on build 45 (all in T1071). Still his: EU trader status, the Esri imagery call, copyright name, review contact. |
| 0.9.1 iPhone build | relayed by the planning chat 2026-11-24 23:55 (the owner's answer in its chat, not read here): the 0.9.1 chat builds and uploads it after its Play Closed build, with T1098, T1099 and T1101 in one lane before its cut, and tells this chat the build number. The push check is then repeated on that build. |
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
