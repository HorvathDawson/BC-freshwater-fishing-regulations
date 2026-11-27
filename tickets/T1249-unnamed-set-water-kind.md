# T1249 — A rule key no named water carries got no water kind in the gear

Status: OPEN — fix prepared on `fix/T1249-unnamed-set-water-kind`, not merged; waiting on the owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1249 | A rule key no named water carries got no water kind in the gear | OPEN — waiting on the owner | — | owner: in the 0.10.0 re-cut as its own PR (recommended) or 0.11.0; then PR |

Cause: `answers/common.py` took a set's kind from its named items only; 132 keys (6,584 unnamed stream, 84,162
wetland, 32 lake sections) had none, so gear kept "2 while alone in a boat on a lake" on them and listed ice fishing
on streams. Gear only: no open/closed, keep/release, hook or bait answer moves (reader binds by the reach run).
Fix (rule 55): gear reads the bundle's `rule_key.kind` (`common.water_kind`); no kind is refused.
Tests: `test_t1249_key_water_kind.py`. Owner look: the lane's changed-clauses list and pictures.
Depends: none. Size: S. Owner: yes (which release).
