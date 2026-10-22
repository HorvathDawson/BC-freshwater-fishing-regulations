# The consumer page v35, kept as a reference (never shipped)

> **ARCHIVED 2026-10-22** (audit F12, owner call 2) from `pipeline/deliver/answers/reference/`. Kept as a
> record; nothing runs it. The scripts below still name their old module paths (`pipeline.deliver.answers.reference.*`)
> and read `convert.py` / `page_v35_inputs.json`, which stayed behind for page v36 — to run the harness again,
> copy these files back beside them. The check it pinned (`port.py`: rows.py on the page's own ladder gives the
> page's card, every difference a documented page bug) runs with no harness from a golden fixture:
> `pipeline/tests/answers_v35_port.py` (also how the fixture is made, from a golden this harness wrote) and
> `pipeline/tests/test_answers_v35_port.py`. The sha256 below was pinned by the retired
> `test_answers_reference.py`.

`page_v35.js` is the JavaScript of the consumer's display page **v35** ("Quota Display Studies",
the page described in `handoff/consumer-pipeline.md`), copied **byte for byte** from its
`<script>` block (sha256 `d6855ef640287127f0350de322cba10138fc0edb3a6c6ce6c3d633af546411f2`,
pinned by `pipeline/tests/test_answers_reference.py`). It implements stages 2–8 of that document:
the ladder (Stage 4), today's card (5), "How this was decided" (6), gear and licence (7) and the
Checks tab (8).

It is here **for comparison only**: the answers layer (`pipeline/deliver/answers/`) is verified
1-to-1 against what this page shows before it replaces anything. Nothing in the pipeline imports
or ships it. Never edit `page_v35.js`; a new page version is a new file.

## Files

| file | what |
|---|---|
| `page_v35.js` | the page's script, verbatim |
| `page_v35_inputs.json` | from the page's own blocks: its 28 display waters (in page order) and its 8 own cases with the rule sets they named |
| `convert.py` | Stage 1: the format-2 export → the page's `data` and `cases` blocks (`export_codec.expand`, then §1.2–1.4) |
| `harness.js` | runs `page_v35.js` whole in a node `vm` with a stub DOM, and drives it through its own handlers |
| `checks.js` | the page's Checks tab (`renderCases`) on a build |
| `sample.py` | the waters the golden outputs cover: the 28, plus a deterministic sample by region × kind × own row, plus special shapes |
| `golden.js` | reads what the page shows for every water × part × date × fish × origin × angler profile |
| `manifest.js` | merges the per-slice manifests |
| `regenerate.sh` | all of the above, from the live export, into a directory you name (outside the repo) |
| `compare.py` | the meeting point and the field-by-field diff (`port.py` projects the page's card through it; `python -m …compare GOLDEN ANSWERS_DIR` diffs a `cli page-view` dump) |
| `port.py` | the port isolated: `rows.py` on the page's own golden ladder against the page's card; every difference attributed to a documented page bug or ruling (`PAGE_BUGS`: rows.DECISIONS F1-F6, F11, F16 and ROUND15 3) or `unexplained` |

## Running

```sh
# from the repo root; node ≥ 18; no credits (nothing calls the claude CLI)
pipeline/deliver/answers/reference/regenerate.sh /tmp/page-golden
node pipeline/deliver/answers/reference/checks.js /tmp/page-golden/build28
# the port against the page (slow + needs_bundle): makes the golden outputs itself, once per
# export + tooling vintage, into data/generated/scratch/answers_reference/<stamp>/ (~6.5 min, 4 node
# workers), then runs port.py on every golden card (~1 min); FAILS when the bundle, its
# verdicts.sqlite, the export pair or node is missing
.venv/bin/python -m pytest pipeline/tests/test_answers_reference.py -m slow
```

The pinned test is `port.py`'s: zero `unexplained` differences, every card of the golden manifest
read. The comparison is against **v35**, not v36: v36 renders the answers file and decides
nothing, so comparing the answers layer with it would compare the file with itself.

## How the page is run without rewriting it

The whole script runs as the browser runs it. The vm context adds only what a browser provides:
`document` / elements (they record `innerHTML` and event handlers), `window`, `location`,
`matchMedia`, `getComputedStyle`; the two JSON blocks as `#data` / `#cases` text; and a seeded
`Math.random` (the fish drawings take random svg ids). A second script in the same context reads the
page's top-level bindings (`settle`, `evalSp`, `MODEL`, …), as a second `<script>` tag would.

The page is driven through its own handlers: a water button's `onclick`, the `#part` change event,
the date input's `onchange` (which maps Feb 29 to Feb 28), and the angler profile (set on
`state.who`, then `renderKit()`, which is what the profile `change` handler does).

## What the converter decides that §1 leaves open (checked against the page's own block)

On the current export `convert.py` rebuilds the page's 28-water block with 492/492 rules, 67/67
licensing records, 134/134 entries, 94/94 splits and 36/36 lake names; the differences are the
export's own changes since the page was built.

- Parts are ordered biggest first (stable), as in the page's block.
- A part's `rules` are its set's `reach` then `trib` members; `trib_pending` / `contested` never
  decide (the Checks never run them). Its `lic` carries every via, since `licPlace` reads `contested`.
- `splits` holds the run ends only. §1.2 also names rules' `extents[].splits`, but the page's block
  holds exactly the run ends, and `endName`'s duplicate-name test reads every split it holds.
- A rule's `caught` condition (answers 2.3, Q38: a snagged fish is released) is handed to the
  page as `while`: v35 predates `caught`, and read raw the snag rule (`take: 0`, ALL_FIN_FISH) is
  an outright release of every fish in B.C. (it was `while: [snagging]` when the page was built).
- A water's `uncertain` is the export's (absent today, so `[]`, as in the page's block).
- Our 8 cases: remapped to the part of the case's water whose set holds every rule the case names,
  nearest by the old set's `reach` list.

## Page quirks to know when comparing

- v35's `TODAY` is fixed at Sep 23 (`923`); page v36 reads the viewer's clock (answers 2.2). It decides only the word "today" against "on <date>" in the
  status line and the Today chip; every answer reads `state.md`, which the harness sets.
- Feb 29 does not exist (`DAYS` has 365 days): the date input reads it as Feb 28, and a rule window
  ending Feb 29 would make the ladder's segment breakpoint wrap to Jan 1 (`DAYS.indexOf(229) = -1`).
  No rule or licensing record in the current export has a Feb 29 bound, so this is latent.
- The ladder is cached per segment of days (`ladderAt`), evaluated at the first date read in it.
- `settleLic` takes exemptions from every licensing record in the page's block, wherever placed:
  an answer could depend on which waters were bundled. Today the only exemption is province-wide.
- Parts closed all year are one picker choice; only its first part is reachable from the picker
  (`picker.reachable` in the golden records). Every part is still read.
- The Checks run the ladder with no origin and (our cases) no steelhead water; the slim case rules
  drop `not_yet_mapped`, as §1.2 says.
- Stretch names, the default part and the Other-fish fold are presentation and are recorded only as
  text.
