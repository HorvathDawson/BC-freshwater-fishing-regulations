# `local_closures/`: waters a local authority closes to fishing (T981)

| file | kind | written by | read by |
|---|---|---|---|
| `region-<n>.json` | authored, ACCEPTED rows only (the owner accepts each row) | `pipeline.regs.local_closures.write_region` (through `curated_write`) | the reach run and the bundle, as a second entry source beside `synopsis/` (`parsing.io.entry_sources`); checked by `python -m pipeline.regs.local_closures --check` |

These are waters a city, park board or other local authority closes to fishing. The synopsis does not list them. Each
entry is one water's whole closure:
- id `r<n>:<slug>_local@<mu>`;
- `matched`: one item;
- one rule, "No Fishing", from a superior authority, so nothing in the book reopens it;
- `authority`: who closed it and where it says so (body, instrument, section, url, quote), in place of a book row's
  pages.

The app says who closed the water and links that page.

A local closure is refused on a water that already has a synopsis row (`local_closures.check_against_book`). Those
waters are owner calls. Rows not yet accepted stay in the ticket (T981) and in
`data/generated/review/t981_local_closures_proposals.json`.
