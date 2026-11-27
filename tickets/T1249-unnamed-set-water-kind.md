# T1249 — A rule key no named water carries got no water kind in the gear

Status: OPEN — approved for the 0.10.0 re-cut as its own PR; left: the PR's merge

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1249 | A rule key no named water carries got no water kind in the gear | OPEN — approved for the 0.10.0 re-cut | — | the PR's merge |

Owner, 2026-11-27: "Fix in the 0.10.0 re-cut, all three kinds (Recommended)".
Cause: `answers/common.py` took a set's kind from its named items only. On main's bundle r-7d7848399e53: 133 sets
(92 unnamed-stream, 6,584 sections, 7,020 km; 31 wetland, 84,162; 10 lake, 32); 132 on the 0.9-era live bundle.
Gear kept "2 while alone in a boat on a lake" there and listed ice fishing on streams; no open/closed, keep/release,
hook or bait answer moves. Fix (rule 55): gear reads `rule_key.kind` (`common.water_kind`); no kind is refused.
Also cured: the walk's (PR #365) 42 + 25 moved unnamed stretches and four Region 6 streams worded so via T1146.
Re-cut's answers diff: streams/wetlands "toward closed", lakes "unclear": expected. Depends: none. Size: S.
