# Data flow decisions (T408, owner walkthrough 2026-11-05)

One row per question asked. Stream → decision → why → ticket. `data-flow.md` is the map; this is what the owner chose.

## Review loop (marks)
| question | decision | why | ticket |
|---|---|---|---|
| `decided/<domain>.json` middle copy | **Drop it.** Writers read `marks.jsonl` directly and write only the final file. | It is a copy of the marks with review fields; the build never reads it. One fewer copy of every decision. | T266 / T325 |

## Waters
| question | decision | why | ticket |
|---|---|---|---|
| Hand-edited files (`name_variants`, `areas`, `added_lakes`/`added_areas`, `continuations`, `ungazetted`) | **Keep hand edits; add a fast check** (schema + references) on commit and as the build's first step. | Rare edits don't need review-app tabs; a bad edit should fail in seconds, not after the hour-long atlas build. | T324 |
| `waters/sub_lake_areas.json` (worklist, no reader) | **Move it out of curated** into the deferred lake-splits ticket. | Curated holds only files the build reads. | T324 |

## Regulations
| question | decision | why | ticket |
|---|---|---|---|
| `steelhead_waters.json` hand copy from `data/generated/steelhead/` | **Keep the copy as the review moment**, but make it one command that prints the diff (waters added / dropped) and writes the curated file. | Accepting a new list is a decision; a rerun must not change it silently. | T324 |
| Claude-made proposal files (`review/dfo_fix_proposals.json`) | **Move to `data/generated/review/`.** The review app reads them there; the owner's save is the decision. | Curated holds the owner's decisions only (same rule as T330). | T324 |

## DFO salmon
| question | decision | why | ticket |
|---|---|---|---|
| Bindings (`item_ids`) refreshed only by a manual `match apply` | **The 162 matches are done and verified = owner decisions.** Keep the ids in the file; every build checks each id still exists in that build's registry and **fails** naming the place if not. No re-match by name, no manual step. | Owner: "the matching was done and verified already"; only extents are unfinished. The risk is an atlas rebuild renumbering items, not DFO renaming. | T337 |
| A new DFO water name with no match | **Already handled:** the worker holds its rules in `unplaced.json` and emails an "N rules held unplaced" notice; a human matches it. | Owner: a new name needs a human. | — (keep) |
| Worker's compiled-in copy of `wording.json` | **One copy (R2), fail loud.** Missing/invalid → the DFO job fails and reports; the app keeps the last good feed. | No second copy; no fallbacks. | T324 |

## Gauges
| question | decision | why | ticket |
|---|---|---|---|
| `gauges/candidates.json` (regenerable, "never edit by hand") in curated | **Move to `data/generated/gauges/`.** Curated keeps `matches.json` (frozen, accepted), `review.json`, `tidal.json`, `backwater.json`. | Machine output doesn't belong in the owner's layer. | T324 |

## In-season
| question | decision | why | ticket |
|---|---|---|---|
| Worker inputs (in-season matches, DFO places, `wording.json`, roster) reach R2 only by a hand-run `sync-inputs` | **Auto on push, to STAGING R2** (turn on `feeds-sync-inputs.yml`'s push trigger). Prod stays a manual run at the prod switch. | A saved match should reach the worker without a remembered step; prod stays human-only. | T324 |

## Run timing → the "slow feed" pattern
| question | decision | why | ticket |
|---|---|---|---|
| Run timing made two ways (Mac deliver + manual workflow needing an owner-uploaded base) | **One slow-feed pattern:** own R2 folder + `index.json`; a GitHub workflow started by a button or the worker (never a GitHub schedule); `upload.sh` uploads the binding base with every release (no owner step); content-hashed files; publish only on a real change with a diff report; a build-twice test asserts identical bytes. The Mac deliver stops making run timing. Staging automatic; prod the owner's choice. | Owner: a slow feed like HYDAT and later bathymetry; "make sure it is producing same output … deployed to R2 directly". | new ticket; HYDAT clim + bathymetry follow it when touched |

## Naming (owner 2026-11-05)
"Make all the file names more accurate." Renames are collected per stream during the walkthrough and decided in ONE
naming pass at the end, so each file moves once.

## Release → app
| question | decision | why | ticket |
|---|---|---|---|
| Build, checks, promote, staging upload are separate hand steps | **One command, "build once, promote the same artifact":** `pipeline release --staging` = build → checks → promote LATEST → upload staging, stop at the first failure, resumable (finished stages cached), stage-by-stage progress, plain failure message. Prod = `pipeline release --prod <r-id>` re-uploads the SAME checked bytes (human-only). | Owner: "we want to do that"; industry standard. | T295 / T297 |
| Who starts a rebuild after curated changes | **Claude, batched** (one build via heavy.sh after a batch lands, diff summarized), and the **same command is the human's** — a short runbook, readable progress. | Owner: "something a human could run too with a nice workflow". | T297 |
| Worker sees a new HYDAT | **Never a GitHub schedule** (owner: flaky, unpaid, will disappear). Today the worker emails and the owner presses the workflow button; T413 lets the worker start it itself once the owner makes a token that can only start workflows in this repo. | Owner 2026-11-05. | T413 |

### Naming pass — decided (owner 2026-11-05: "I like all changes", names consistent across streams, good docs in the files)
Conventions: `*_calls.json` = the owner's per-item decisions · `*_waters.json` = a list of waters · `*_stations.json` =
a list of gauges. Every JSON keeps an `_about` header (what it holds, who writes it, who reads it); every curated folder
gets a short README. One change set: files moved with `git mv`, code + config + docs + tests updated together.

| now (`data/curated/…`) | becomes |
|---|---|
| `regulations/entries/catalogue/` | `regulations/synopsis/` |
| `regulations/entries/dfo_salmon/` | `dfo_salmon/places/` |
| `regulations/overrides.json` | `regulations/book_name_links.json` |
| `regulations/review/` (marks, closed, …) | `review/` |
| `regulations/steelhead_waters.json` | `waters/steelhead_waters.json` |
| `waters/splits.json` | `waters/cuts.json` |
| `waters/tidal.json` | `waters/tidal_limits.json` |
| `waters/mined_names.json` | `waters/found_names.json` |
| `waters/name_calls.json` | `waters/found_name_calls.json` |
| `waters/areas.json` | `waters/area_closures.json` |
| `waters/ungazetted.json` | `waters/unmapped_waters.json` |
| `sloughs/page-marks-2026-10-28.json` | `waters/slough_page_calls.json` (the `sloughs/` folder goes) |
| `gauges/review.json` | `gauges/match_calls.json` |
| `gauges/tidal.json` | `gauges/tidal_stations.json` |
| `gauges/backwater.json` | `gauges/backwater_stations.json` |
| `inseason/matches.json` | `inseason/notice_waters.json` |
| `runtiming/review.json` | `runtiming/run_calls.json` |
| kept | `waters/continuations.json`, `waters/name_variants.json`, `waters/slough_calls.json`, `waters/added_*`, `dfo_salmon/wording.json`, `gauges/matches.json`, `regulations/reference/` |
| leaves curated (above) | `decided/`, `waters/sub_lake_areas.json`, `review/dfo_fix_proposals.json`, `gauges/candidates.json`, `review/proposal_overrides.json` (folded by T326) |
