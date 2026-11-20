# `inseason/` — in-season notices bound to waters

| file | kind | written by | read by |
|---|---|---|---|
| `notice_waters.json` | authored | the review app's In-season tab (its one non-mark write) or hand | `pipeline.regs.inseason.publish` -> R2 `feeds/inseason/matches.json` (the app) |

(T679 #11) Only the owner's matches live here (format `inseason-matches/3`: extents, dates, note, who, history). The
ranges and end tokens a resolve derives on a release are GENERATED, `data/generated/inseason/resolved.json`
(`python -m pipeline.regs.inseason.resolve`; the review app on save and its Re-resolve; the release's `follows` stage),
never committed; `pipeline.regs.inseason.matches.load` reads the two as one.

Informational only: a notice puts a banner on its waters, never changes an answer. Reaches R2 through
`pipeline.deliver.sync_inputs` (on push to the dev branch, staging only; production manual).
