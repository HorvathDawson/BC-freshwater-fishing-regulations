# T389 — Unnamed reach links without downloading `lines`: the stream key in the map tiles

Status: POSTPONED (deferred, owner 2026-11-06: "might wanna do in future"; needs an atlas/tiles rebuild). Was: OPEN (owner 2026-11-04, after 0.5.0; needs an atlas/tiles rebuild).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T389 | Unnamed reach links write their place from the tiles' stream key (no `lines` download); `lines` only to open a shared unnamed link | POSTPONED (deferred, owner 2026-11-06: maybe later; needs an atlas rebuild) | — | owner 2026-11-04 (after 0.5.0) |

T388 took `lines` (1.2 MB gz) off every NAMED water's link. An UNNAMED reach still fetches all of `lines` to WRITE its
address (tapped section → stream key + metres). If each stream feature in the tiles carried its stream key (`blk`) and
its start metres, writing the place needs no download at all; `lines` is then fetched only to OPEN a shared link to an
unnamed reach (stream key + metres → the section of this release).

Size check first: two integer properties on ~1.9 M stream features (tile bytes, per zoom); keep them at the zooms where
an unnamed stream can be tapped (z10+). Then: atlas/tiles rebuild, app `placeOf` for unnamed from the tapped feature,
the request walk's unnamed tap drops `lines`, a test that writing an unnamed place reads no `lines`.
