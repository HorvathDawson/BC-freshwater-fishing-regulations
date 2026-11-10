# T602 — Tidy the data layout into fetched, curated, built, released

Status: OPEN

Source: T599 findings step 4 (§4, §5); owner 2026-11-10: "nice split between the different source types".

## Work
1. Move built files out of `source/`; machine review inputs get their own folder (still in git), apart from build
   output; one `generated/tmp/` for scratch, sessions and logs that is safe to delete; prune the `_prev` / `_next` and
   other leftover copies the findings list.
2. Each class has one home and a stated rule: who may write it, whether it is in git, how it is protected. Write the
   rules in one short page and link it from AGENTS.md.
3. Do not move: curated JSON, release files, PMTiles, `bundle.sqlite`, HYDAT's SQLite, `graph.pkl`, the small arrays.
4. Parquet or FlatGeobuf for an intermediate only if `build --status` shows a stage dominated by reading it; measure
   first, and file that as its own ticket.
Paths under `data/curated/review/` and `data/generated/review/` are chat 10's until it is done: plan them, move them last.

## Acceptance
- A path tells you the class; nothing built sits beside a source or a curated file; the rules page exists.

Depends: T600, T601. Size: M. Owner: no.

## Consistent with today's builds (owner 2026-11-10: "need to make sure it is consistent with the builds now")
This step changes where and how data is written, never what is built. Before it starts, keep the current atlas,
bundle, tiles and release as the reference. After it, one full build must give the same outputs: same release file
hashes (or a diff where every difference is explained and expected, e.g. the new input digests in a meta file), same
tiles, same app and web packages. The build, release and staging commands the owner uses keep working unchanged, or
the change is listed in the ticket and in AGENTS.md. A step that cannot show this is not done.
