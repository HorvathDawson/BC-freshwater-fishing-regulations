# T844 — The web app's opening loader is the new coho, as the phone's is (owner)

Status: OPEN (app chat, app-fix/t844-web-coho; 0.8.0).

Source: owner 2026-11-17 ~22:20, verbatim: "loading on webapp should use the new one too like the app", then "new loader".
Asked whether the small in-app loading spots switch too: "Opening loader only".

## What to make
Wherever the web app's first load still shows the old circling fish (the pre-JS shell or a web-only first-load path; the
shared BootScreen already draws the coho), show the coho's loader instead. Small in-app loading spots keep the circling
fish (T814). A clip of the web loader to the owner before it ships.
