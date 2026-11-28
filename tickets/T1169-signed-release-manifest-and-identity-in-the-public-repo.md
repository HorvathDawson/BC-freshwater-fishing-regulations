# T1169 — Security review 2026-11-25: sign the release manifest, checksum the web database engine, scope the storage keys, and the owner's identity in the repo

Status: OPEN — key scoping in the speed release (with T1160); signing in the next app build; the token's admin scopes go with the agents' own login (T1160); identity left as it is (owner 2026-11-27)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1169 | Security review 2026-11-25: sign the release manifest, checksum the web database engine, scope the storage keys, and the owner's identity in the repo | OPEN — key scoping (with T1160); signing in the next app build; identity left as it is | — | T1160 scopes the R2 tokens and drops the admin scopes; a build lane: Ed25519-signed manifest verified by the app, the sql.js wasm hash check; the OSM client name; M |

Source: the read-only security review of the public repo, 2026-11-25 night (the planning chat holds the full
notes; they are not in the public archive). Done the same night: 7 Google API keys found in the public git
history (old parser scripts, added 2026-01-07 to 01-24, removed from the files 01-28) were revoked by the owner,
and GitHub secret-scanning alerts #1-#7 closed as revoked. No other secret was found in 49,175 text files across
every ref. Checked and sound: no `pull_request_target`, no script injection, read-only workflow tokens, fork PRs
get no secrets, the feeds worker's public routes are validated and rate-limited.

Owner, 2026-11-25, on the storage-key finding: "what is this ... how would we fix it?" (answered in chat; this is
the record).

## Work
1. Storage keys (with T1160): the R2 keys are repo-level secrets usable by a workflow on any pushed branch, and a
   workflow comment says the token "may be account-wide" (`.github/workflows/feeds-runtiming.yml:38-40`; not
   verified in the Cloudflare dashboard). One token per bucket; the production token only inside a production
   Environment that runs from a release tag after the owner's approval; the repo-level ones deleted.
2. Sign the manifest: the app checks every file against the sha256 in `release/manifest.json`
   (`app/packages/data/src/release/drivers/install.native.ts:171`, `manifest.ts:60`), but the manifest is not
   signed, so whoever can write the production bucket can publish a release the apps accept. Sign it at
   `release --prod` with an Ed25519 key only the owner holds; the app verifies with an embedded public key and
   refuses a bad signature with a shown message. It rides an app build; older installs keep working unverified.
3. The web app loads the sql.js WebAssembly engine from the data host with no hash check
   (`app/packages/data/src/release/drivers/open.web.ts:48-52`): verify it like a release file.
4. The shared agent token carries `admin:org` and `repo` (any agent session can change rulesets, secrets, org
   settings): drop the admin scopes now or with T1160's own identity.
5. Identity (his picks): 9,195 of 9,466 commits carry his real name and personal email; his name is in the
   staging worker's workers.dev address (about 50 files and the live web bundle, which also holds the old repo
   path with his GitHub login); his email is the User-Agent sent to OpenStreetMap
   (`pipeline/atlas/waters/added_streams/fetch_osm.py:30`): a generic client name there in any case. A history
   rewrite is only ever his (T1143).

## Acceptance
A release whose manifest is altered after signing is refused by a build of the app (test); the wasm is refused on
a hash mismatch (test); no repo-level storage secret remains. Severity: medium (it needs bucket write first).
Model: Opus. Size: M. Owner: the key, the picks.

## Owner

- [x] 2026-11-25 — The 7 Google API keys in public history → "these have all been revoked"; the alerts → "you can close them" (closed #1-#7 as revoked, 2026-11-25)
- [x] 2026-11-25 — Picks (defaults first): your name and email in new commits: stop adding them from now (a project identity) / rewrite history / leave; LICENSE name: keep / "the authors"; the shared token's admin scopes: drop now / with T1160; sign the release manifest in the next app build: yes / no; the staging worker's address with your name in it: move to a project subdomain / leave. → name in commits: see below; admin scopes: "With the agents' own login"; signing: "Yes, next app build"; LICENSE name and the staging address: "Yes, leave both" (2026-11-27)
- [x] 2026-11-26 — Your name and the project identity → "is it reall yan issue?" (2026-11-25 23:27; answered: a privacy preference, not a security issue) → "Leave it; only fix the OpenStreetMap client name (Recommended)" (2026-11-26 00:45)
