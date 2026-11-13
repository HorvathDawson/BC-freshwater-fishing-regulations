# User accounts: feasibility (research, 2026-11-13)

Research only. No code changed, no ticket added. It builds on **T223** (`app-integration/tickets/T223-user-accounts-web-and-phone.md`,
POSTPONED by the owner 2026-11-05) and its open questions Q-T223a to f. T234 (push) already says it needs no account.
Sections marked **Fact** are checked against code or a cited source. Sections marked **Opinion** are mine.

## 1. What is per-device today (Fact, from the code)

Everything is a file on the phone (`documentDirectory`, via `packages/data/src/spots/store.native.ts`) or browser storage on the web
(`store.web.ts`, which calls itself "NOT durable"). There is no server-side user state at all.

| State | Where | Would an account sync it? |
|---|---|---|
| **Saved spots** (notes, frozen weather and lead-up, visits, ratings) | `spots.spots` file; `spots/core.ts`, `model.ts` | Yes. The only thing a person writes. |
| **Spot photos** | `documentDirectory/spots/photos/`, `photoStore.native.ts` / `.web.ts` | Yes, but heavy (storage cost, upload size). |
| Layers-sheet switches | `layers.json` (`openLayerStore`) | Yes, small |
| Run-species choice, data mode, metered/update setting | `openRunSpeciesStore`, `openDataModeStore`, `updates.json` | Yes, small |
| Recent searches | `openRecentSearchStore` | Optional |
| In-season notification history and "seen" ids | `inseason-inbox.json`, `inseason-seen.json` | Optional. Local-only notifications by design (`notify.native.ts`, owner 2026-11-05). |
| Notification settings | `openNotifyStore` | Optional. Permission is per device anyway. |
| First-run / walkthrough / location-ask flags | `openFirstRunStore`, `openWalkthroughStore`, `openLocationAskStore` | No, per device by nature |
| Crash settings, install id, problem-report outbox | `openCrash*Store`, `openInstallIdStore`, `report/outbox.ts` | No |
| Caches: like-visit forecasts, DFO feed, gauge history, in-season feed | small files | No, public and refetched |
| **Offline release, tiles, synopsis PDF** | `release/drivers/install.native.ts`, `synopsisPdf.native.ts` | No. Public data on R2; re-download per device. |
| Angler profile (age band, residency) | memory per session | Would be a setting if made persistent |

Two further facts:
- `spots/model.ts` records "No cross-device sync (decided)" and says export/import is a requirement. **I found no export/import code in
  `packages/` or `apps/mobile/src`.** T223 assumes it covers the gap; today nothing does.
- The Android manifest has `android:allowBackup="true"` (`apps/mobile/android/app/src/main/AndroidManifest.xml:17`), so Google's
  Auto Backup likely already restores app files on a same-Google-account phone swap. Not verified on a device; worth a test (see 4).

The server side today is the feeds worker (`app/workers/feeds`): R2 buckets (`BUCKET`, `BACKUP`), rate limiters, email send, crons.
No D1, KV or Durable Objects yet.

## 2. Options on this stack

Costs are from third-party summaries unless linked to an official page; check before committing.

| Option | Cost at small scale | Effort | Lock-in | Self-host rule |
|---|---|---|---|---|
| **A. Self-built on Workers + D1** (own sessions, email magic link via the existing email binding, optionally Google/Apple token verification) | Workers Free covers D1 at 5M row reads, 100k writes/day, 5 GB ([summary](https://freetier.co/articles/cloudflare-d1-free-tier-limits-pricing-and-alternatives)); Workers Paid is $5/month and lifts D1 limits ([Cloudflare FAQ](https://developers.cloudflare.com/d1/reference/faq)) | High. Security-critical code a solo dev owns: sessions, token rotation, abuse, email deliverability, recovery | None | Fits fully: our domain, our data |
| **B. Firebase Auth + R2 `users/<uid>/`** (T223's default) | 50,000 MAU free ([guide](https://blog.logto.io/firebase-authentication-pricing)) | Medium. SDK on RN and web, Worker verifies the ID token | Medium. Users live in Google's store; export is possible but awkward | **Breaks the spirit** of the rule (a Google service in the sign-in path, Google SDK in the app). Not covered by the two stated exceptions. |
| C. Supabase or Clerk | Not verified (my search returned only Firebase data) | Medium | Supabase is open-source and self-hostable; Clerk is not | Third-party service; same issue as B |
| **D. Passkeys only** (WebAuthn) | Same as A | High on React Native (native passkey modules, domain association files), plus recovery problem | None | Fits, but device-bound credentials make "lost phone" the hard case |
| **E. Sign in with Google / Apple as the only method** | Free | Medium | Tied to the platform's identity | Third party in the path. On iOS, Apple 4.8 then forces Sign in with Apple too. |
| **F. No account: sync code / file** (section 4) | About zero | Small | None | Fits fully |

Durable Objects are not needed at this scale: per-user data is a few KB to MB, written rarely. KV is the wrong fit for records that need
read-after-write consistency. D1 or R2 objects are enough.

**Opinion:** if accounts are ever built, option A with email magic link only is the one that fits the owner's rules, and it is
mostly an email-deliverability and abuse problem rather than a coding one. T223's default (Firebase) conflicts with
"self-host everything" and should be revisited if the owner wants to keep that rule. Skip social login at first: it pulls in Apple's rule below.

## 3. Store and legal consequences (Fact, with sources)

**Google Play**
- Apps that let users create an account must offer account deletion and give a web link for requesting it, entered in the Data safety form;
  deactivating or freezing does not count; data tied to the account must be deleted. Applies even if parts of the app work without an
  account. ([Play Console help](https://support.google.com/googleplay/android-developer/answer/13327111))
- Our drafted Data safety answers (`handoff/PLAN/play/data-safety.md`) currently say "no account, no sign-in, no user IDs", and answer
  deletion by emailing privacy@canifishthis.ca. With accounts that changes: Personal info (email, user ID), User-generated content
  (spots, notes, photos, precise location inside spots) become "collected", stored server-side; the deletion link must be a real flow.
  Precise location stored on our server is a significant step up from today, where spots never leave the phone.

**Apple (iOS is later)**
- 4.8: an app using a third-party or social login for the primary account must also offer an equivalent login that limits data to name and
  email, lets the user hide the email, and does no ad tracking (in practice Sign in with Apple). Not required if the app exclusively uses
  its own account system. 5.1.1(v): if the app supports account creation, in-app account deletion is required; if there are no
  significant account-based features, let people use it without a login.
  ([Apple guidelines](https://developer.apple.com/app-store/review/guidelines/), [deletion help](https://developer.apple.com/help/app-review/guideline-reference/5-1-1-account-deletion))
- So an own-account magic-link system avoids 4.8. Google or Facebook login would trigger it.

**Canada / BC (basics; these are law-firm and vendor sources, not the statutes. Have the owner confirm with the OIPC-BC or counsel.)**
- Most private organisations in BC fall under BC PIPA; PIPEDA applies to cross-border flows and federally regulated activity. Both need
  meaningful consent, plain-language notice, safeguards, access and deletion on request, and accountability stays with us when a provider
  stores data outside Canada. ([McCarthy, 2026 overview](https://www.mccarthy.ca/en/insights/publications/privacy-laws-doing-business-in-canada-2026);
  [BC PIPA summary](https://www.clym.io/regulations/british-columbia-personal-information-protection-act-bc-pipa))
- Joint OPC / Alberta / BC guidance "Good Privacy Practices for Developing Mobile Apps" is the relevant regulator document; I could not
  fetch it.
- Cloudflare D1 location can be pinned with a jurisdiction hint (not verified here); R2 has jurisdictions too (EU, FedRAMP), **none for Canada
  that I confirmed**. Whether storing in the US is acceptable is a disclosure question, not a ban.

**Privacy page** (`canifishthis.ca/privacy`, T559) would need: what accounts store, where, retention, deletion flow, a sub-processor list
(email sender, Cloudflare), and the sync of spot locations. T559 already ties retention claims to what the system does.

## 4. What accounts buy an angler vs. no-account alternatives

Accounts buy one thing: **the same spots and settings on a second device, and survival of a lost or replaced phone.** Everything else
(regs, maps, feeds, notifications) is public or per-device. The app is mostly used with no signal, so sync has to be offline-first anyway
(T223 item 3). Most users have one phone. The web app is the second device only for people who plan trips at a desk.

| Alternative | Gives | Cost / catch |
|---|---|---|
| **Export / import file** of spots (JSON plus photos zip) via the share sheet and file picker | Backup, phone-to-phone move, web to phone | Small. Already declared a requirement in `model.ts` but **not built**. Manual, no live sync. |
| **Transfer code** (device uploads an encrypted blob to R2 under a random code, other device downloads it, expires in 24 h) | Phone-to-phone and web-to-phone without identity | Small-medium. Needs a Worker route and rate limits; blob is end-to-end encrypted so we never read locations. No account, no deletion-flow duty (data self-expires). Probably still a Data safety "collected" line while it exists. |
| **Android Auto Backup** (`allowBackup=true` already) | Restores on a same-Google-account phone | Free, nothing to build. Limits: 25 MB per app, needs testing; iOS has iCloud backup equivalent. |
| Accounts | Continuous sync, web and phone | Large, plus legal duties above |

**Opinion / recommendation:** do not build accounts after closed testing. Build, in this order:
1. **Export / import file** (size **S**): the owner's own written requirement, zero server, zero store-form change. Also test that Auto Backup
   restores spots (a one-evening check on a spare device or emulator).
2. If testers ask for web-to-phone transfer: the **encrypted transfer code** (size **M**), still no account.
3. Only if there is real demand for continuous multi-device sync, or a feature that needs identity (shared lists, server alerts), build
   **accounts as option A, magic link only, spots + settings first, photos later**. Size **L** for a minimal version (sign-in, D1 schema,
   sync with last-write-wins and tombstones, migration prompt, in-app delete + web delete page, export, Data safety and privacy updates,
   tests). Smallest useful accounts version: spots + settings only, no photos, no social login, optional.

Reason: a solo developer taking on credential security, email deliverability, a permanent deletion/privacy duty, and storing the exact
locations anglers guard (T223 Q-T223c), for a benefit most one-phone users never feel.

## 5. Risks

- Holding precise fishing-spot locations server-side is the biggest trust and breach exposure the project would have had so far.
- Email magic links need a reliable sender domain (SPF/DKIM) and abuse limits; the worker already has an email binding and rate limiters.
- Lost-account recovery is the support burden; with a solo owner it is all on one person.
- Dropping the "no accounts" statement changes the Data safety form and privacy page; a mismatch with behaviour is a Play policy risk.
- Retrofitting: if accounts are wanted later, the local-first stores (`open*Store` pairs) are already a clean seam, so nothing in the
  recommended path blocks it.

## 6. Questions only the owner can answer

1. Is "no account, ever" a feature you want to advertise (listing, privacy label)? That alone may settle it.
2. Is the export/import file (and an Auto Backup check) acceptable as the answer to a lost phone, ahead of any sync?
3. Do you expect anglers to use the web app and the phone for the same spots, or is the web a read-only reference?
4. If accounts happen, is Firebase's Google dependency acceptable, or does the self-host rule win (T223 Q-T223a default conflicts with it)?
5. Spot photos: sync them (storage cost) or leave them on the device?
6. Private-only, or any sharing of spots between people? Should the server be able to read locations, or must they be encrypted on the device?
7. Is storing user data in the US acceptable, or must it be Canadian, and who answers a privacy complaint (name and mailbox)?
8. Is there any feature you want that needs identity (shared lists, server-side alerts, a community layer)? If not, accounts have no payoff.
