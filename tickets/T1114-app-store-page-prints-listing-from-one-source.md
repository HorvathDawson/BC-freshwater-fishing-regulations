# T1114 — The App Store submission page carries its own copy of the listing words: print them from the one source

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1114 | The App Store submission page carries its own copy of the listing words: print them from the one source | OPEN — before the App Store submission if cheap, else with T1081 | | store content chat 2026-11-24 |

Source: store content chat 2026-11-24 23:50: `app/store-listing/listing.json` is now the one source of the listing
words (PR #170), and `python3 deploy/staging/play-listing-push.py --appstore` prints the App Store fields. The iOS
chat's page `docs/ios/APP-STORE-SUBMISSION.md` (T1071) holds a pasted copy, which will drift. Two drifts
exist already: the App Store category was written two ways in older pages (the source says Navigation, Sports
second), and the first download is "approximately 500 MB" in the description but "about 480 MB" in the review
notes; the store content chat is asking the owner about both.

## Work
1. The submission page stops holding the words: it names the command that prints them, and keeps only what is not
   in the source (privacy answers, compliance, review notes, the click path).
2. Review notes and any number both texts state (the download size) come from the source too, or from one measured
   value.
3. The docs guard (or a small test) fails when the page contains a listing field's text.
4. Delete the older listing drafts the source replaces (git keeps them).

## Acceptance
One place holds each listing word; the page and both stores are filled from it.

Severity: low. Release: before the App Store submission if cheap (the iOS chat or the store content chat), else
with the docs lean-out (T1081). Model: Sonnet. Size: S. Owner: no.
