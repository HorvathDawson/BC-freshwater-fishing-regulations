# T860 — More → Contact names Report a problem beside the email

Status: DONE (d1e1f854b, dea59dca2, 845994f67, d20d7ba63, 902425f66, 0e35d7727, ab5c86008; in 0.8.2 on app/integrate-082, ledger closed 2026-11-19 night).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T860 | More → Contact names Report a problem beside the email (owner wording ask) | DONE | d1e1f854b, dea59dca2, 845994f67, d20d7ba63, 902425f66, 0e35d7727, ab5c86008 | in 0.8.2 (app/integrate-082); ledger closed 2026-11-19 night |

Owner 2026-11-18 ~16:55 (to the main chat, relayed to the app chat), verbatim: "where it says this (Questions, corrections
and ideas are welcome. ) with email we should have also the or send it through the app with the report and issue pathway?
just word it well".

Today: `core/src/menu/words.ts` `MENU_WORDS.contact.intro` = "Questions, corrections and ideas are welcome.", then the
email row (`ContactBlock`, `menu/MoreScreen.tsx`; view model `core/src/menu/contact.ts`).

Wording (proposal, sent to the owner 2026-11-18): "Questions, corrections and ideas are welcome. Email us, or send a note
from the app with Report a problem. A water's regulations have their own Report a problem link, which names the water
for you." "Report a problem" in the Contact block is a link to the report form (the same entry as About's
`MENU_WORDS.report.fromAbout`). The words claim only what the form does: from More it carries no water; the link on a
water's regulations does.

Build: the words in `MENU_WORDS.contact`, the link row in `ContactBlock`, a render test (the link opens the report page),
the placeholder line kept as it is.

## 2026-11-18 ~17:05: the project speaks as one person (owner)
Main chat asked whether the contact text should speak as the owner (a solo project); owner, verbatim: "yes change all".
So every user-facing "we / us / our" that means the project becomes "I / me / my" (no name), in the app (words.ts:
report.intro, noShare, queuedRetry, privacy, "our own server"; help, glossary, notifications, What's new), the web site
pages (app/tools/seo), and the store listing texts. "Our" stays only where it means anglers and the app together.
Contact intro now: "Questions, corrections and ideas are welcome. I read every message. Email me, or send a note from
the app with Report a problem. A water's regulations have their own Report a problem link, which names the water for
you." Still 0.8.1 web.

## 2026-11-18 ~18:44: words, not a second link
The full tier's T60 check (`apps/mobile/src/surfaces.test.tsx`, one surface per action) caught the Contact intro's
"Report a problem" link: it is a second way to the same form on the same More page, where the Report a problem row sits
just above. The intro keeps its words exactly; "Report a problem" in it is plain text, and the row is the one way in.
