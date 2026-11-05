# `inseason/` — in-season notices bound to waters

| file | kind | written by | read by |
|---|---|---|---|
| `notice_waters.json` | authored | the review app's In-season tab (its one non-mark write) or hand | `pipeline.regs.inseason.publish` -> R2 `feeds/inseason/matches.json` (the app) |

Informational only: a notice puts a banner on its waters, never changes an answer. Reaches R2 through
`pipeline.deliver.sync_inputs` (on push to the dev branch, staging only; production manual).
