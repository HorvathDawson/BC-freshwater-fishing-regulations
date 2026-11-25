# T1099 — iPhone: the privacy manifest does not match the App Privacy labels the owner decided

Status: DONE — 4f703f6a2 (app.json manifest matches the labels; `app/tools/privacy-manifest.test.ts` compares them to `handoff/PLAN/ios/APP-STORE-SUBMISSION.md`). Ships with the next iPhone build.

Source: iOS release chat 2026-11-24 ~23:40, its read of build 0.9.0 (45) at 65f84020c for T1071 (App Privacy and review risk). File: `app/apps/mobile/app.json` lines 74–131.

## What differs
- No Product Interaction entry.
- Device ID lacks the Analytics purpose (owner 2026-11-14: Analytics + App Functionality).
- Linked is false on Email and on the types a problem report carries; with an email and the install id in one
  record they are linked (see T1098 a, and the owner's pending pick there: if the install id is left out of a
  report with an email, "not linked" becomes true for Device ID and Crash Data).

Owner 2026-11-24 ~23:25 (iOS chat, relayed): "Strict: linked". So Linked is true for Email, Device ID, Other User
Content, Precise Location, Product Interaction, Crash Data and Other Diagnostic Data; Coarse Location stays false.

## Work
A config edit so the manifest says what the App Store Connect labels say (the iOS chat's page
`handoff/PLAN/ios/` holds the labels); a test that compares the two lists. It ships with the next iPhone build.

## Acceptance
The manifest and the labels page agree, checked by the test; the next upload raises no privacy warning email.

Severity: low (Apple checks only the required-reason APIs at upload).
Release: the next iPhone build (0.9.1 if its app build is not cut yet, else 0.9.2). Model: Sonnet. Size: S. Owner: no.
