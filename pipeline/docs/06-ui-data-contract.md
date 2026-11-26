# What the pipeline hands the app

The pipeline settles every question offline and ships the answers; the app looks them up and lays them out (AGENTS
53, 56). Four files cross the line, all read from one `bundle.sqlite` and its `verdicts.sqlite`, all stamped with the
bundle's digests:

| file | what it is | contract |
|---|---|---|
| `ui-rules-export.json` + `ui-rules-guide.json` | every regulation record, where it applies, and a guide to reading it (Part 6) | `pipeline/deliver/export/__init__.py` |
| `status_index.bin` | the map's colour per section and per day (Part 7) | `pipeline/deliver/status_index.py` docstring |
| `ui-rules-answers.json` | the decided answers for every part, day, fish and angler (Part 8) | `pipeline/deliver/answers/ANSWERS-SPEC.md` |

How the app turns the answers into its page (the view model, its inputs, the golden comparison) is
`handoff/PLAN/app-integration/contracts.md`.

`python -m pipeline.deliver` is the chain bundle -> verdicts -> status index -> export -> answers; each step also runs
alone (`python -m pipeline.deliver [all|bundle|status_index|export] …`).

## The rules every file keeps

- **Section handles never leave the bundle** (AGENTS 5). Membership is per rule SET and per named WATER; a closure
  note names a `ruleset` or an `example` key, never a section. The status index carries handles only under the
  bundle's `section_handles` digest, and the app refuses a file whose digest is not its bundle's and tiles'.
- **Never pair files whose digests differ.** Each file carries `about.bundle` (`section_handles`, `reach_digest`); the
  app and every reader refuse a mixed set.
- **The reader runs once.** `verdicts.sqlite` (never shipped) holds the reader's every traced answer; the status
  index, the export and the answers look answers up there. Only two named oracles re-ask the reader on purpose
  (`status_index.status_by_reader`, `export.closures.closures_combine_problems`); `test_dataflow_gates.py` gate 1
  refuses any other call.
- **Refused, never worked around.** A bundle missing a column a step reads, a retired field name in the output
  (`export.contract.RETIRED_ANYWHERE`, `RETIRED_ON_RULE`), or a pair that does not decode back to the model stops the
  run (`test_export_ui_rules.py` pins both with mutations).
- **What is not written is said.** A fish with no rule says so; a blank cell would read as "no limit", the most
  permissive failure.

## Part 6: the export pair

    python -m pipeline.deliver export [--bundle B] [--out OUT] [--guide-out GUIDE] [--pretty]

Written to `data/generated/regs/` (not tracked). `export.model.build()` reads the bundle into one reading model, which
every check in `checks.problems` proves and every word of `guide` / `field_dictionary` describes; it ships ENCODED by
`export/codec.py` (AGENTS 58):

- `ui-rules-export.json`: the data a client reads at run time (entries, rules, licensing records, the interned rule and
  licensing sets, waters with their parts and runs, splits).
- `ui-rules-guide.json`: `guide` (with `cases`), `field_dictionary` (with `encoding`), `species`.

**`export.codec.expand(data, guide)` is the reference decoder**: it returns exactly the model, and `main()` refuses a
pair that does not decode to it or whose integer references do not resolve (`codec.wire_problems`). Every encoding rule
is in `field_dictionary.encoding`, so a decoder in another language needs the dictionary alone
(`test_export_codec.py`: a second decoder written from those words agrees).

- **Every record reads itself**: its generated `label`, its `verbatim`, its `fields` as the bundle ships them, and its
  provenance. Nothing derived that hides reasoning is added: no "reads as", no settled table.
- **A part is keyed by the five-tuple** (ruleset, licensing_set, province_except, anadromous_rainbow, steelhead): two
  parts of one water may share a (ruleset, licensing_set) pair.
- **Sets no named water carries ship too** (they sit on unnamed sections): the app resolves a tapped section's set from
  the bundle and reads the set here, for an unnamed creek as for a named water.
- **`guide.cases`** answers each sample on a section of the case's own water and part, never the first section of the
  rule set anywhere.
- **The guide cannot drift from the code.** Its lists (types, families, slots, clause fields, conduct acts, `Who` axes,
  `Doing` acts, licensing kinds, extent fields and ops) are generated from the model's registries, and `problems()`
  refuses a registry member with no words or words naming a member that is gone. Every example is a live record a
  test finds in the data, never a remembered id.

## Part 7: the status index, `status_index.bin`

    python -m pipeline.deliver.status_index [--bundle B] [--out OUT]

Beside `bundle.sqlite`; the dev server serves it at `/status_index.bin` (`app/tools/serve-tiles.mjs`) and the app fetches
it next to `atlas.pmtiles`. Per SECTION and per DAY, one of three answers; the format, the exact definitions and the
water roll-up are the module's docstring, the decoder is `app/packages/core/src/statusIndex.ts`.

- **closed**: on that day, for every game fish, the reader returns a rule that speaks and is an unconditional closure.
  A closure of some species only, or one that holds beside the others (hours, weekdays, one half of the channel), is
  never `closed`.
- **own**: not closed, and a water table's row (`r<n>:`) binds the section. It keys on the TABLE, not the rule's scope:
  a zone table's rule that names waters is still the base.
- **base**: neither; not in the file (absence means base). `tidal` and `outside` carry their own codes.

The index is a projection of `verdicts.sqlite`. `pipeline/tests/test_status_index.py -m slow` asks `effective_rules`
directly for thousands of (section, date) pairs, including boundary days, Dec 31 / Jan 1 and Feb 29, and requires
agreement; a mutation test flips one stored verdict and requires the check to fail.

## Part 8: the answers, `ui-rules-answers.json`

Built beside the export pair from the same bundle and verdicts (`python -m pipeline.deliver.answers.cli build`). Its
format, the tap and the fields are `pipeline/deliver/answers/ANSWERS-SPEC.md`; its types are
`pipeline/deliver/answers/model/` (strict pydantic, JSON Schema in the file's `schemas`, TypeScript generated into
`app/packages/core/src/answers.generated.ts`). The release turns it into the regs store the app reads.
