# T1221 — Pipeline tests sorted into stage sub-folders

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1221 | Pipeline tests sorted into stage sub-folders | OPEN | — | pure moves into atlas / regs / deliver / gauges / common / review plus one shared path helper; lands late, the planning chat told the hour first |

Source: repo clean chat, 2026-11-27. Owner, 2026-11-26, asked whether to sort the tests into atlas / regs / deliver /
gauges / common / review: "Before the re-cut". On main, `pipeline/tests/` holds 573 `test_*.py` files flat; 191 of
them compute paths from `__file__`, and about 110 non-test files cite test paths. The scanners already search
recursively (`pipeline/tests/anchors.py:311`, `scripts/related_tests.py:70`).

## Work
1. One shared path helper (repo root, fixtures) so a moved test does not count `parents[n]` again.
2. Move the files by stage, no edits beyond the path helper and the citations that name a moved file.
3. Fix the non-test citations (docs, scripts, CI path lists) to the new paths.
4. Land late, in one PR, so open branches merge main once; the planning chat is told the hour first.

## Acceptance
The same tests run and pass before and after (count and names); `python -m pipeline.tests.anchors` and
`scripts/related_tests.py` find every test; `git grep` finds no citation of an old path.

Depends: none (after the re-cut build starts to land). Size: M. Owner: no.
