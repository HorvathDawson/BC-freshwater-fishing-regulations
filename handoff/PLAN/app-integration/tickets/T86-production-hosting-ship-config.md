# T86 — Production hosting and ship config

Status: POSTPONED (owner 2026-10-28: prod is a while off). Owner needed (credentials, billing, name).

Source: needed to ship, tracked nowhere: "The feed only stays current with a SCHEDULED publisher (owner)"
(implement_progress; P04 removed the old workflows); the R2 bucket (D1b; publish.py is human-run); `EXPO_PUBLIC_CONTACT`
is null; the app name "Can I Fish This?" is unconfirmed. NOT needed: an ArcGIS key — owner 2026-10-21 (late): Esri World
Imagery WITHOUT a key ("this is a non profit app"); `EXPO_PUBLIC_ARCGIS_API_KEY` stays optional (config.ts says so).

## Work
Owner creates the R2 bucket and confirms name + contact. A scheduled publish.py runner (30 min).
The feed sync (T182 item 5, from T129): `index.json` sent `Content-Encoding: gzip` (352.6 → 46.7 kB, every first
paint) with a short `Cache-Control` (it changes every 30 min, e.g. `public, max-age=300`); the release's own headers
are already right (`deliver/release/publish.py`).
Build config refuses a release build with no contact.

## Acceptance
A preview build passes against staging; the feed stays < 2 h old for 24 h; the release build fails without the contact.

Depends: T72 (LATEST). Size: M.
Owner: yes. Default: keep "Can I Fish This?"; R2 staging bucket first; publisher every 30 min.
