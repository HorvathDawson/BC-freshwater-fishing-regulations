# T1030 — The release manifest carries a changed-waters count (the inbox's "N waters changed")

Status: OPEN — 0.9.0 batch, not small (see Why); left (Claude): design the count, then a release build to prove it

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1030 | The release manifest carries a changed-waters count (the inbox data row's "N waters changed") | OPEN — 0.9.0 batch | T867 | stopped: not small, see the file |

T867 (PR feat/T867-inbox) shows a data-release row in the notifications inbox: "Regulations data updated <day>", and
"N waters changed" once the manifest says it. Nothing in the manifest says it today, so the row omits it (rule 32); the
slot is `InboxTrack.release.changed` in `@app/ui` `useInSeasonInbox` (Shell reads it from the manifest).

Wanted: `manifest.changed_waters` (integer, absent when unknown), the number of waters whose resolved rules differ from the
previous release; app reads it (pack.generated `Manifest`), Shell passes it as `release.changed`.

Why not small (stopped, 2026-11-23, lane L19): (1) the previous release's per-water resolved rules must be available at
build time, and old releases are pruned to live + previous, so the diff needs a stored per-water digest list (the bundle's
`entry_digests` are per ENTRY, not per water); (2) `release_id` is content-hashed (`rebase.id_of`), so a new manifest field
must be excluded from or deliberately part of the id; (3) proving it needs a release build (heavy, ~25 min) and the
manifest/pack.generated/format gates (`feed-formats`, `pack`). Decide the digest source first.
