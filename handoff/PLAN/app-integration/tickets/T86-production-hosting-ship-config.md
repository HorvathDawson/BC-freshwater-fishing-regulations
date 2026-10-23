# T86 — Production hosting and ship config

Status: OPEN. Owner needed (credentials, billing, name).

Source: needed to ship, tracked nowhere: "The feed only stays current with a SCHEDULED publisher (owner)"
(implement_progress; P04 removed the old workflows); the R2 bucket (D1b; publish.py is human-run); "MUST NOT SHIP
without an ArcGIS Location Platform key" (config.ts; key optional today); `EXPO_PUBLIC_CONTACT` is null; the app name
"Can I Fish This?" is unconfirmed.

## Work
Owner creates the R2 bucket and the ArcGIS key and confirms name + contact. A scheduled publish.py runner (30 min).
Build config refuses a release build with no key or contact.

## Acceptance
A preview build passes against staging; the feed stays < 2 h old for 24 h; the release build fails without the key.

Depends: T72 (LATEST). Size: M.
Owner: yes. Default: keep "Can I Fish This?"; R2 staging bucket first; publisher every 30 min.
