"""THE ANSWERS LAYER — decided facts, computed once, beside the UI export (never inside it).

The display page (the consumer's page v35) re-derives gear precedence, licence documents, rule
kinds, size bands, plain sentences and part labels from the export in JavaScript. Each of those is
computed here instead, from the bundle and the ONE reference reader (`pipeline.deliver.bundle.
read`), so the page only looks them up (handoff ANSWERS/DESIGN.md, the user's constraint: an
EXTRA layer that replaces nothing until verified 1-to-1 against the page).

THE PRODUCERS ARE PURE (no file is written; the answers encoder adds them as sections):

    gear.produce(bundle)              {RuleKey: {start_day: gear answer}}          Stage 7.1-7.6
    licence.produce(path)             {LicenceKey: {start_day: {holds, profiles[60]}}}  7.7, G5
    display.produce_rules(bundle)     {"entry::rule": {kind, closure?, bands?, plain?}}  2.3/2.4/6.3
    display.produce_parts(bundle, doc) {item_id: {parts: {part key: facts}, picker, …}}  3.1/3.2/5.6

    common.py   THE ONE KEYING MODULE: bundle, export pairing, rule order (= the export's
                `rules` array), part and rule keys, calendar, segments, words
    dump.py     `python -m pipeline.deliver.answers.dump --out DIR`: the producers' output
                interned for inspection, with sizes and times (not the shipping encoder)

A part is named by agent C's part key (`display.parts_of_bundle`, `reference/golden.js partKey`);
an undecidable value stops the build with a named error (`PartKeyError`, `UnknownAxis`,
`NoFishToAsk`) or is a documented state, never a default. Nothing here edits the reader or the
export, and nothing reads a curated file.
"""
