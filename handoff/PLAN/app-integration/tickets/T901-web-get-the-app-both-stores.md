# T901 — The web's "Get the app" offers Google Play and the App Store, each its own button

Status: IN PROGRESS (app chat; lane app/t901-get-app; in 0.8.2 web; pictures to the owner).

Owner 2026-11-19 ~01:14 (app chat), verbatim: "the web app get the app should link to both google play and apple depending
on button you click".

Today: the web's Spots notice (ui-native WebSpotsNotice.tsx, words core/spotWords.ts `getApp` / `appUrl`) has one "Get
the app" button with one link. Build: two buttons, "Google Play" → the Play listing, "App Store" → the App Store page
(https://apps.apple.com/app/id<Apple ID>; the Apple ID read from App Store Connect, the same number build-ios-testflight.sh's
`~/.canifishthis/app-store-url` names), every other "get the app" place on the web and the website (app/tools/seo) the
same. Self-hosted art only (self-host rule); words in the words files; tests. If the App Store page is not public yet
(the app is in TestFlight only), the App Store button says so plainly instead of linking to a 404 — ask nothing, show
both states in the pictures.
