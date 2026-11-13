# `gauges/` — where BC's gauges are, and what the owner decided about them

| file | kind | written by | read by |
|---|---|---|---|
| `matches.json` | promoted | `pipeline.gauges.generate.promote` only (one station at a time, `--only`) | atlas, bundle |
| `match_calls.json` | authored | hand (the owner's calls on matches; an INPUT to matching) | `pipeline.gauges.generate.match` |
| `tidal_stations.json` | authored | hand | bundle (`gauge.tidal`) |
| `backwater_stations.json` | authored | hand | bundle (`gauge.backwater`) |
| `diversion_stations.json` | authored | hand | bundle (`gauge.diversion`, the intake placement; T696) |

The review QUEUE (matches promote did not promote) is machine output: `data/generated/gauges/candidates.json`
(committed, so `check_curated --ci` sees it in CI). Never regenerate `matches.json` to "check something":
`python -m pipeline.tools.audit.check_curated` says whether it is stale and prints the command.
