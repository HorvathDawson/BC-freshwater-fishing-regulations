# `review/` — the curation review app's state

| file | kind | written by | read by |
|---|---|---|---|
| `marks.jsonl` | reviewed | the review app — APPEND-ONLY, never edited by hand or by a tool | `pipeline.regs.review.apply` (write-through to each domain's final file), the review app |
| `closed.json` | authored | hand (`pipeline.regs.review.closed`) | apply + review app skip a closed domain |
| `proposal_overrides.json` | authored | hand (a proposed cut's confidence) | `marks.proposals`; folded away by T326 |

No build stage reads this folder: a mark changes the build only through the final curated file it writes
(`waters/cuts.json`, `regulations/synopsis/`, `dfo_salmon/places/`, `waters/tidal_limits.json`, …). Claude-made
proposal files the app shows (e.g. `dfo_fix_proposals.json`) are machine output in `data/generated/review/`.
