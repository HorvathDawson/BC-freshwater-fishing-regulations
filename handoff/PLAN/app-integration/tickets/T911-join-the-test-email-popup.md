# T911 — "Join the test" opens a small popup: email me, or copy the address

Status: IN PROGRESS (built on app/t911-join-test; left (Claude): owner sees pictures, then staging).

Owner 2026-11-19 ~07:55 (main chat), verbatim: "on the join the test buttons it should have a popup with either a
straight to email button or a copy email option?"

## Work
The web's "Get the app" Join the test buttons (T901: both stores "coming soon" + Join the test) open a small card
(dialog) instead of going straight to mail: one line saying how to join (email me and I add you to the test), an
"Email me" button (mailto with a subject naming the store, as today) and a "Copy email" button (copies the address,
then says "Copied"), close by ✕, Escape or a click outside (the T859 card pattern). Keyboard focus moves in and back.
Same on the site pages that carry the buttons (app/tools/seo) and in the web app if it shows them. Words in core, owner's
voice (T860: I / me). Pictures 390 + 1440, light + dark, before it lands.

## Built (lane app/t911-join-test, 2026-11-19)
- Core: `WEB_SPOTS_WORDS.join.popup` (line per store in my voice, "Email me", "Copy email", "Copied", the refusal line, "Close");
  `JoinAsk.address` (the contact email, for the copy).
- Web app: `ui-native/JoinTestCard.tsx` (Modal card, scrim, ✕, Escape and the one back; the web Modal moves focus in and back).
  `WebSpotsNotice` opens it on a Join press that has an address; with no address it is still the Report form.
  Copy uses the browser clipboard; a refusal (or no clipboard) says so and shows the address to select (W46).
- Website gauge pages: each Join link is still the mailto (works with no script), beside a closed `<dialog class="join">`;
  `tools/seo/joinClient.ts` (self-hosted `/assets/join.<sha>.js`, no import, all words from data- attributes) opens it as a card:
  ✕, Escape, click outside, focus back on the link; same Copied / refusal behaviour.
- Tests: ui-native/JoinTestCard.test.tsx, tools/seo/joinClient.test.ts (open, Email me, copy, refused clipboard, Escape/✕/outside).
- Pictures: scratchpad/t911-shots/ (site + app, 390 and 1440, light and dark; contact-sheet.png).
