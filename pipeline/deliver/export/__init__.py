"""The UI data export: every regulation record in the bundle, and a guide to reading it.

    .venv/bin/python -m pipeline.deliver.export [--bundle B] [--out OUT] [--guide-out GUIDE] [--pretty]

TWO FILES, ONE MODEL. `model.build()` reads the bundle into one reading model — what every check in
`checks.problems` proves and every word of `guide` / `field_dictionary` describes. It ships ENCODED
(`codec`): `ui-rules-export.json` (the data, no indent, integer set members, compact waters) and
`ui-rules-guide.json` (`guide`, `field_dictionary`, `species`), both stamped with the bundle's digests.
`codec.expand` is the reference decoder; `__main__.main` refuses a pair that does not decode back to the
model, or whose integer references do not resolve (`codec.wire_problems`).

WHAT IS IN IT. Everything the bundle holds about regulations, and nothing sampled:

  · every entry (the province, the zone chapters, the areas inside them, and the waters);
  · every rule, keyed `entry_id::rule_id`, with its generated `label`, its `verbatim`, its fields
    exactly as the bundle ships them, and its provenance;
  · every licensing record, keyed `entry_id#record_id`, the same way, with its placement;
  · the document register;
  · where each of them applies, in the bundle's own interned form: rule sets and licensing sets
    (each shared by many sections), and for every named water the sets its sections carry;
  · a `guide` explaining how to read all of it, generated from the model's own registries
    wherever the model has one, so the explanation cannot drift from the code.

WHAT IS NOT. Nothing is settled here: no tables, no verdicts, no "reads as". A record is data;
the guide says how to read it, and the reader applies that. Section handles never leave the
bundle (AGENTS 5), so membership is exported per set and per named water, never per section.

ONE SOURCE. Everything is read from `bundle.sqlite`. A bundle that lacks a column this export
reads, or that ships a retired field, is refused rather than worked around.

THE MODULES, in the order the model is made:

  contract     the columns it needs, the fields it refuses, the default paths
  read         the bundle -> entries, rules, licensing, sets, waters, splits
  steelhead    the provincial steelhead set and where steelhead rules apply
  guide_words  every `*_TEXT` table of the guide and the dictionary
  guide        `guide` (its ladder prose: `ladder`; its checked prose examples: `examples`)
  cases        `guide.cases`: one real test water per mechanism 
  verdicts     the verdict store every reader answer is looked up in
  closures     what only the bundle's sets say about closures; the gotchas they add
  dictionary   `species`, `field_dictionary`, `index`
  model        `build`
  checks       `problems`
  codec        the wire encoding (`compact` / `expand`)
  files        `encode`, `dumps`, `load`
  __main__     write the pair, refused on any problem
"""
