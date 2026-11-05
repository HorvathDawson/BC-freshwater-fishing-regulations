# `regulations/` — the synopsis corpus and the book

| path | kind | written by | read by |
|---|---|---|---|
| `synopsis/region-*.json` | parsed, then curated | the parser (`run_parse.sh`, HUMAN-ONLY — spends credits); review app write-through (entry / rule marks) | reach run (`CURATED.regulations.synopsis`), bundle, review app |
| `book_name_links.json` | authored | hand | a book water name -> the FWA water(s) it means (matcher) |
| `reference/*.md` | source text | transcribed by hand from the printed synopsis | answers glossary |

Back up `synopsis/region-*.json` before any write (AGENTS rule 2). The files in `synopsis/` and `reference/` are
globbed by their readers, so the folders hold nothing else (no README inside them).
