# T223 — user accounts shared between the web app and the phone app

Status: OPEN — FAR FUTURE (owner 2026-10-28); default plan below; owner answers the questions first (no build yet).

Source: owner 2026-10-28: one account a person can use in both the web app and the native app.

## Default plan (owner 2026-10-28)
- **Sign-in: a free hosted auth provider does all account work** (passwords/email links, Apple/Google, resets, abuse).
  Default Firebase Auth (free tier; web + native SDKs); Supabase Auth or Clerk are alternatives. Cloudflare has no
  public-user auth (Cloudflare Access is for staff, 50-seat free tier) — not used.
- **Our side: one Cloudflare Worker + R2, no database.** The Worker verifies the provider's ID token and reads/writes
  only `users/<uid>/` in R2: `data.json` (spots, settings) and `photos/` beside it.
- **Delete / export:** delete removes `users/<uid>/` and the provider account (Apple's in-app deletion rule);
  export downloads the same folder.
- Accounts optional; local-first stays; this replaces the Q-T223a default (Worker + D1).

## Today (what an account would replace)
- No accounts and no server state. The app is static files plus R2 (release, gauge feed, DFO feed).
- Spots are the only data a person writes. They live on the device: a file on the phone (`packages/data/src/spots/store.native.ts`)
  and local storage in the browser (`store.web.ts`, which calls itself "NOT durable"). Spot photos are stored the same way
  (`photoStore.*`). `spots/model.ts` records "No cross-device sync (decided)", with export/import as the backup. **This
  ticket reverses that decision.**
- Other per-device state: the layers-sheet switches (one local key), the angler profile (D8: in memory per session,
  B.C. resident 16+ by default), and the regs pins.

## Work (once the owner answers)
1. Sign-in on both platforms with one identity: the same account in a browser and in the Expo app. Signing out leaves
   the device's local copy readable.
2. What syncs, in order of value: spots (notes, captured conditions, `item_id` binding), then spot photos (large; may
   need their own store and quota), saved waters / pins, settings (layers, angler profile: age band, residency).
   Never synced: the release, tiles and feeds (public, already on R2).
3. Offline first: the phone keeps working with no signal (the app's main use case). Writes queue locally and merge
   on reconnect. A spot is a record, so merging is mostly union by id; an edit made on two devices needs a rule.
4. Migration: on first sign-in, offer to upload the spots already on the device. Nothing is lost if they decline.
5. Account deletion and export (a person's notes are theirs; `model.ts` already requires export).

## Open questions for the owner
- **Q-T223a Auth provider.** Options: Cloudflare (Access is not consumer sign-in, so pair Workers + D1/KV with a
  library such as Lucia or Better Auth), Supabase Auth + Postgres, Firebase Auth + Firestore, Clerk. Sign-in methods:
  email magic link, Apple (required on iOS when any other social sign-in is offered), Google. Default: a Cloudflare
  Worker + D1, alongside the R2 hosting (T86); email magic link plus Sign in with Apple.
- **Q-T223b What syncs.** Spots only, or spots + photos + pins + settings? Photos drive storage cost. Default: spots +
  pins + settings first; photos later.
- **Q-T223c Privacy.** Spots are exact fishing locations, which anglers guard. Private by default with no sharing?
  Encrypt on the device so the server cannot read them? Where is the data held (Canada/US)? This needs a privacy policy
  and a contact address (T86). Default: private only, server-side encryption at rest, no sharing features.
- **Q-T223d Offline and conflicts.** Is last-write-wins per spot acceptable, or must an edit made on two devices keep
  both? Default: last write wins per field, with a deleted spot kept as a tombstone.
- **Q-T223e Required or optional.** Does the app still work fully without an account? Default: yes. An account only
  adds sync.
- **Q-T223f Timing.** Before or after the first public release (T86/T88)? Default: after; the spot export/import
  covers the gap.

## Acceptance (when built)
One account signed in on the web and on a phone shows the same spots and settings. A spot written offline on the phone
appears on the web after reconnect. Signing out keeps the local copy. Deleting the account removes the server copy.
Tests cover the merge rule and the offline queue.

Depends: T86 (hosting, contact address), T88 (iOS: Sign in with Apple). Size: L. Owner: yes (Q-T223a–f). Default: as
listed under each question; nothing is built until the owner answers.
