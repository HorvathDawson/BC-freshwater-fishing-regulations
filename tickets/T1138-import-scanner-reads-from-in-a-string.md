# T1138 — The import scanner reads the word from before a quote inside a string as an import

Status: OPEN — 0.10.1

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1138 | The import scanner reads the word from before a quote inside a string as an import | OPEN — 0.10.1 | | 0.10.0 chat 2026-11-25 |

Source: the 0.10.0 chat, 2026-11-25 14:50, hit while writing the What's-new text for 0.10.0.

`check-boundaries` (the import scanner in `app/tools/`) treats the word "from" followed by a quote inside a
changelog string as an import statement and fails.

Work: the scanner parses imports (or at least skips string and template literals and comments); a test with the
sentence that tripped it. Severity: low. Model: Sonnet. Size: S. Owner: no.
