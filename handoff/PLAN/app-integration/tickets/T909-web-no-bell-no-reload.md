# T909 — Web app: no bell and no reload in the search bar

Status: IN PROGRESS (main chat; lane app/t909-web-bar; onto staging when checked, owner 07:15 "add stuff to staging as we work").

Owner 2026-11-19 ~07:20 (main chat), verbatim: "also staging still has the bell and reload but bell does nothing on
webapp?" Asked (dialog: remove both on web / bell only / make the bell work on web); owner picked "Remove both on web
(Recommended)".

## Work
The web build's search bar shows neither the bell nor the reload button; phones keep both. Web and native split by
FILE (AGENTS 34a), never a Platform.OS branch in shared code. What's new stays in More → About on the web (T888). Check
nothing else on the web points to the bell (help/tour steps T293/T824 that highlight it, the coach marks, surfaces
tests T60) and that the search bar's width/layout is right without them (390 and 1440). Render tests both platforms.
