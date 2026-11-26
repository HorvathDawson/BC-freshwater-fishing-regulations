# T1153 — On a phone browser the web app offers the store app (after both apps are fully released)

Status: OPEN — built ahead, switched on when both store apps are public (owner 2026-11-25)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1153 | On a phone browser the web app offers the store app (after both apps are fully released) | OPEN — built ahead, switched on when both store apps are public (owner 2026-11-25) | — | planning chat 2026-11-25; needs the App Store release (T1071) and Play production access; pictures of the prompt to the owner first |

Source: owner 2026-11-25 evening (planning chat), verbatim: "also the webapp should pop up that the user should
download app when on mobile (once we release both apps fully)". Related: T1152, T1071, T1128, T1073 (a notice must
only offer what the phone can actually install), `map-ui-layout-rule`, `self-host-everything`.

## Work
1. iPhone Safari: Apple's own Smart App Banner (`apple-itunes-app` meta with the app id and the current page as
   the app argument, so the app opens the same water). Android Chrome and other browsers: our own small prompt
   linking to the Play listing (and opening the installed app by its app link when it is installed).
2. Our prompt: one line and two buttons (Get the app / Not now), never over the search or the map key, dismissed
   for a set time (ask the owner: 30 days by default), never on a desktop, never inside the store app's own
   web views, never when the page was opened from the app's share link by someone who has the app.
3. One switch per store, read from the release config: iPhone on only when the App Store app is public in
   Canada; Android on only when the Play production track is public (closed testing is not installable by the
   public: T1073's rule). No prompt while its store cannot deliver.
4. Pictures at 390 px (light, dark) to the owner before it ships; wording is a proposal.
5. Tests: phone user agent + switch on shows it; switch off, desktop, dismissed, or in-app shows nothing; the
   banner's link carries the current water.

## Acceptance
The tests above fail without the change (mutation); on a real iPhone and a real Android phone the prompt opens
the right store page, and with the app installed opens the same water in the app. Severity: low–medium.
Model: Sonnet. Size: S. Owner: the look, the dismiss time, and the word to switch each store on.

## Owner

- [ ] 2026-11-25 — When the pictures are ready: pick the look and the dismiss time (default 30 days), and later give the word to switch it on for each store once that store's app is public.
