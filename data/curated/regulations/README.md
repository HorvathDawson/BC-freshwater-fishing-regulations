# `regulations/` — the synopsis corpus and the book

| path | kind | written by | read by |
|---|---|---|---|
| `synopsis/region-*.json` | parsed, then curated | the parser (`run_parse.sh`, HUMAN-ONLY — spends credits); review app write-through (entry / rule marks) | reach run (`CURATED.regulations.synopsis`), bundle, review app |
| `local_closures/region-*.json` | authored (T981: the owner's accepted rows only) | `pipeline.regs.local_closures.write_region` | a second entry source beside `synopsis/` (reach run, bundle): waters a city or park authority closes, each citing that authority |
| `book_name_links.json` | authored | hand | a book water name -> the FWA water(s) it means (matcher) |
| `reference/*.md` | source text | transcribed by hand from the printed synopsis | answers glossary |
| `word_reads.json` | accepted (machine-made, then accepted) | `python -m pipeline.atlas.reach.word_reads --run <reach run> --out …` after reading the moves | reach run: a listed printed-word read that stops matching stops the run (T679 D4) |
| `under_closure_calls.json` | authored (the owner's rulings) | hand | the UI export: a dated water release (`release`) or a water row's quota (`quota`) that speaks / is silenced under a blanket closure is refused unless listed here with why (T679 #12; was `closures.*_UNDER_CLOSURE_KNOWN`) |

Back up `synopsis/region-*.json` before any write (AGENTS A2). The files in `synopsis/` and `reference/` are
globbed by their readers, so the folders hold nothing else (no README inside them).
