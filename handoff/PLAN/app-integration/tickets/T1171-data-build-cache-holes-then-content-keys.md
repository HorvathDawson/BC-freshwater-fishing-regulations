# T1171 — Data build: close four holes where a stale result passes as fresh, then content keys and a hashed store so side builds take minutes

Status: OPEN — the holes first (correctness), in the speed work; the store and the tiles split after

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1171 | Data build: close four holes where a stale result passes as fresh, then content keys and a hashed store so side builds take minutes | OPEN — the holes first (correctness), in the speed work; the store and the tiles split after | — | a build lane (Opus): keys cover non-.py inputs, package + tippecanoe versions, the atlas code; `changed_waters` made independent; then content-hashed keys + a 20 GB store; then split the tiles stage; a no-cache verify per release night |

Source: owner 2026-11-25 night: "also data build could happen only if new data is there? and needs abuild? it can
be a build artifact and ashed properly? just like this (Atlas … Only when its inputs change.)". The read-only deep
look (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `data-build-caching.md`, with its timing scripts) found that most of it exists: every stage
after the atlas has a key built from its inputs and code; a build with no changes takes 2 s (15 such runs in the
build manifest); the deliver steps are cached one by one; the release id is a hash of the output files; deliver is
byte-for-byte repeatable (15 pairs of runs, the same release id each time); an app-only release needs no data
build and the tooling runs none (never actually done: rehearse it once). Related: T1165, T1133, T1142, T606.

## Work
1. The four holes (first; correctness, no behaviour change when nothing is stale):
   - stage keys hold only `.py` files, so a commit that changes only `schema.sql` leaves deliver "up to date"
     (`pipeline/deliver/bundle/build.py:39-40`): the key covers every file the stage reads from the code tree;
   - the tiles and clim keys leave out the installed packages and the tippecanoe version;
   - the atlas records its curated inputs but no code, so an atlas code edit is never noticed: the build names
     the code change too (a person still starts the atlas);
   - the release manifest's `changed_waters` count depends on which release came before.
2. Content keys and a store: each stage keeps only its LAST result and the keys use file times, which cost 115
   min of reruns producing the same bytes in 27 days (95 of them from the in-season loop, already fixed). Keys
   from content hashes and a content-addressed store on the Mac (APFS clones, 20 GB cap, pruned): side runs and
   branch switches drop from 24-34 min to about 1-5 (an estimate).
3. Deliver waits for all 18.2 min of the tiles stage and needs only its first 7.4 min: split the stage, about 6
   min per build (33.9 to 27.9 on 0.10.0).
4. A `--no-cache` verify run once per release night that rebuilds and compares release ids; an app-only release
   says "data unchanged" and proves it by recomputing the keys; the release gate proves the shipped data was
   built from the tagged code. An R2 remote cache: not yet.

## Acceptance
Each hole has a test that changes only the missed input and sees the stage rerun (mutation); a side build of an
unchanged tree after a branch switch is measured before and after; the verify run matches. Severity: medium (a
stale stage could ship). Model: Opus. Size: M (1), M–L (2), S (3). Owner: the picks below.

## Owner

- [ ] 2026-11-25 — Picks (defaults first): order: the holes, then content keys, then provenance, then speed; the atlas build names code changes and a person starts it; a no-cache verify once per release night; store cap 20 GB; no R2 remote cache yet; an app-only release says "data unchanged" and proves it.
