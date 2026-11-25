# T1127 — The store push scripts upload every picture in a size folder, leftovers included

Status: DONE (8702e420b, PR #207)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1127 | The store push scripts upload every picture in a size folder, leftovers included | DONE (8702e420b, PR #207) | 8702e420b | the 0.10.0 chat clears the 34 old pictures from the main checkout (owner 2026-11-25: "we can remove old store photos") |

Source: the store content chat, 2026-11-25 12:15, after the 0.10.0 chat found 34 untracked pictures dated Nov 5 in
the main checkout under `handoff/PLAN/app-integration/store/graphics/store/light/`. Not checked by the planning chat.
Related: T1072 (the one picture set, PR #192), T1076, T319 (the old set), T1114.

What the store chat found: PR #192 changed the `.gitignore` in `store/graphics/` so `store/light` is tracked; files
that were ignored before (the T319-era set: `02-06-search`, `03-02-regulations`, folders `appstore-5.5`, `-6.7`,
`ipad13`) now show as untracked in any checkout that had them. Both push scripts glob every png in a size folder
(`play-listing-push.py` about lines 156 and 166, `appstore-screenshots-push.py` about line 151). In the main checkout
`play-phone` holds 20 files (7 current, 13 old) and `appstore-6.5` holds 13 (7 current, 6 old): an upload from there
mixes old pictures into the listing or stops on the count. Whether the App Store script ignores folders it has no
slot for is not checked.

## Work
1. The scripts read the list of pictures from the one source (`store-shots.json`), or refuse when a size folder
   holds a file git does not track; either way a leftover stops the run with its name, never rides along
   (`no-silent-fallbacks`).
2. `--plan` / `--check` print the count per size and the names.
3. Test: a stray png in a size folder fails the run (mutation-proved).

Until then (owner page): clear the 34 old pictures from the main checkout, or upload from a clean checkout of main;
`--plan` must show 7 pictures per size.

Severity: medium (wrong pictures on a public listing; only on a machine that has the leftovers). Model: Sonnet.
Size: S. Owner: no (2026-11-25 12:15: "we can remove old store photos. we want this to be clean").

## Done (lane, 2026-11-25)
- `deploy/staging/store_shots_list.py` mirrors store-shots.mjs (the 1-based `NN-<scene>.png` of each target's list) from
  `store-shots.json`; both push scripts call it for every size folder they use. A picture not on the list, or a listed one
  missing, stops every mode by name before any network call; `--shots`/`--tablet*` folders get the same check; `--no-shots`
  unchanged. Plan output prints count and names per size.
- The App Store script reads only `SETS` (appstore-6.9, -6.5): the old `-5.5`, `-6.7`, `-ipad13` folders are never opened.
- Deleted the unread tracked leftovers `graphics/03-conditions.png` and `graphics/quick/*` (only a stale doc line named them).
- Tests: `pipeline/tests/test_store_push_pictures.py`; the pipeline CI area now covers the committed set it reads.
