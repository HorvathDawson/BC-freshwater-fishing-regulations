# T114 — One search key: the SQL filter and the ranking disagree on what matches ("barriere")

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding app 1
(CONFIRMED on `bundle.sqlite`). The pipeline half (one name normaliser) is T61 2.8; this ticket is the app split.

## Decision being unified
"Does this query match this water's name or alias?"

## Where it is decided today
- SQL `LIKE` filters: `app/packages/data/src/release/queries.ts:34-49` and `app/packages/data/src/bundle/queries.ts:73-99`.
- Ranking: `app/packages/core/src/search.ts:33` `normalise` (strips accents and apostrophes).

How they differ: `item.name LIKE '%barriere%'` returns 0 rows and `'%barrière%'` returns 6, so ranking never sees them;
only 1 alias catches "barriere". "childrens" never finds "Children's Fishing Pond".

## Work
- Core `searchKey(text)` (the current `normalise`), and stored `name_key` / `alias_key` columns built in the pipeline by
  the same rule (share the rule with T61 2.8's normaliser; one fixture table both languages test against).
- Both queries filter on the key columns with `searchKey(query)`.
- Pin: an app grep test that no `LIKE` on raw `name`/`alias` remains in the query modules.

## Acceptance
"barriere" finds the 6 Barrière waters; "childrens" finds Children's Fishing Pond (tests on the fixture store).
Parity test: the Python key and `searchKey` agree on the fixture table.

Rebuild: deliver (new columns) + app. Depends: T61 2.8 for the shared rule (or land the key first and let T61 reuse it).
Size: S. Owner: no.
