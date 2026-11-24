# T905 — Shrink the live salmon feed back under its old size

Status: OPEN (main chat; measured 2026-11-24, waits for the owner's pick of the options at the end).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T905 | Shrink the live salmon feed back under its old size (owner) | OPEN | main chat | owner 2026-11-19 ~06:40, later |

T870 (2026-11-19) added each place's printed DFO rows (`pr`) to the live salmon feed: index.json grew to ~133 kB
(15.7 kB gzipped) and its size check was raised from 112 kB to 135 kB. Owner 2026-11-19 ~06:40, on that question,
verbatim: "okay we will work to shrink it down later."

## Work
Measure where the bytes are (`pr` ~34 kB raw), then shrink without losing a row: intern repeated row text, or ship the
rows once per DFO page and point to them by index; bring the budget back down in the same change. Python and the
feeds worker twin together (AGENTS 40).

## 2026-11-24: the release's salmon head too (note)

The release file `salmon` (the head) went over its 100,000 B gz gate on the 0.9.0 build (101,480; live 97,723) and the
owner raised it to 110,000 (2026-11-24, "Raise to 110,000 (Recommended)", T796). Shrink it with the feed: measure what
grew since r-741815c6090d (T754 one-bank places and T812 Babine circles are the likely adds), then bring the gate back
down in the same change.

## 2026-11-24 night: measured, STOPPED (0.9.1 lane L1): no backward-safe way reaches either budget

Nothing changed in code. Every shrink that reaches the old budgets changes a shape a 0.9.0 app reads, so it needs a
format bump (the brief: stop and report instead of merging a breaking change). The owner picks one of the options below.

**Feed `index.json`** (the `test_dfo_live` fixture run, the file its 135,000 B check reads; JSON compact already):

| | raw B | gz B |
|---|---:|---:|
| today (`dfo-feed/3`) | 132,986 | 15,760 |
| where: `locs` 58,019 (`pr` 24,611 of it), `verdicts` 38,964 (`tree` 22,995), `regions` 33,384 (`notes` 12,989, `texts` 9,724) | | |
| A: worker state out of the public index (`texts`, `sha256`, `changed`, `failures`, `header`, `unresolved`; no app reads them) | 121,687 | 14,331 |
| B1: `pr` rows interned once (a row table, `pr` as indices), no A | 123,874 | 15,771 |
| A + B1 | 112,575 | 14,413 |
| A + B2 (one row table per DFO page) | 112,644 | 14,602 |

`pr` holds 439 rows, 230 distinct (12,269 of its 22,684 B). A is backward-safe (apps never read those keys) but stops
at ~122 kB; it needs a private worker state file (the worker re-reads `texts` / `sha256` from the last index today) and
a feeds worker deploy. B is not: a 0.9.0 app refuses an index whose `pr` is not a list of strings (`parseSalmonFeed`
returns null, the whole feed), and one where `pr` is absent shows no rows in Source (DFO). So B = `dfo-feed/4` with
`FEED_MIN_APP` 0.9.1: an app with the T676 gate (0.8.5+) keeps its LAST CAPTURE and shows "New data needs the latest
app" (its salmon answers stop following DFO until it updates); an app before the gate is stranded as any bump strands
it. Serving /3 and /4 side by side needs a versioned key, which T676 (owner 2026-11-20) chose against.

**Release head `salmon`** (r-741815c6090d live vs r-0aec88ed3edb, the 0.9.0 candidate; payload JSON, gz as built):

| | raw B | gz B |
|---|---:|---:|
| r-741815c6090d | 840,124 | 97,723 |
| r-0aec88ed3edb | 856,948 | 101,480 |
| what grew: `ca` (T812 Babine circles, NEW: 11 circles, 48-point rings at 6 decimals) | +15,122 | +3,559 |
| the rest (`unmapped` +492, `verdicts` +1,552, `chains` -377, `p` +29) | | ~+200 |
| ring 48 pts, 5 decimals (salmon/4, lossy) | 855,875 | 101,051 |
| ring 32 pts, 5 decimals (salmon/4, 1.9 m sag on 400 m: a look call) | 852,049 | 100,177 |
| ring 24 pts, 5 decimals (salmon/4, 3.4 m sag: a look call) | 850,133 | 99,722 |
| C1: ring as microdegree offsets from the centre (salmon/5, lossless) | 850,830 | 99,987 |
| C1 + `pr` interned + chain `l` as place indices (salmon/5, lossless) | 761,394 | 97,563 |
| no `ca` at all (the floor if the app drew the ring from `c` + `radius`) | 841,820 | 97,747 |

T754 one-bank places add ~27 B (`p[].side`); the growth is the circles. A 0.9.0 app checks every `ca[].g` (a closed
ring of >= 4 pairs) and draws it as given, so a lossless change is `salmon/5` and the 0.9.1 data release then has
`minAppVersion` 0.9.1 (a 0.9.0 app keeps its release and shows the update prompt). Inside salmon/4 only fewer ring
points get under 100,000, a visible coarsening the owner has to pick; even then the margin is under 1 %.

**Options for the owner:** (1) leave both budgets where they are (135,000 / 110,000); (2) do A only (backward-safe,
feed check to ~125,000, a worker deploy, no app change); (3) A + `dfo-feed/4` + `salmon/5` in 0.9.1, shipped behind
the feed and release gates (0.9.0 apps keep their capture / release and show the update prompt), budgets to
~115,000 (A + B1 is 112,575: 112,000 needs one more cut) / 100,000; (4) as (3) but the feed waits for a version where most installs are 0.9.1+.
