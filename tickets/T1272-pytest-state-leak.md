# T1272 — The name-sources tests leak state into the registry tests

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1272 | The name-sources tests leak state into the registry tests | OPEN | — | find the shared state, restore it with a fixture, pin both orders |

Source: a reviewer, 2026-11-28 (reproduced the same day on origin/main a18c6168b: 27 failed, 16 passed).

## What
`pipeline/tests/test_name_sources_t224.py` leaks state into `pipeline/tests/test_registry.py` when both run in ONE
pytest run: 27 failures in `test_registry.py`. Each file passes alone. Not a 0.10.0 blocker. Command:
`python -m pytest pipeline/tests/test_name_sources_t224.py pipeline/tests/test_registry.py`.

## Work
1. Find the shared state (a module-level cache, a monkeypatch not undone, an env var, a file written).
2. Fix it at the source with a fixture that restores it.
3. Add a guard so the two files pass in either order.

## Acceptance
Both files pass in one run in either order (`rtk proxy`, unfiltered); the guard fails when the restore is removed.

Depends: none. Size: S. Owner: no. Default: n/a.
