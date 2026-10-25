# T189 — Curation review: every rule's extent visible, source pinned, tributary and lake-split calls

Status: DONE (106dbc83). Owner to re-run the pilot (T136) on the new screen.

Source: the owner during the T136 pilot (T184 screen): "I can not see the extent for each rule. The
formatting of the page is really bad. Too much content being repeated. The parse + original src image
should be stickied at top so I don't go up then down then up. Also make sure we can select if
something includes tributaries, also see if the global extent includes tributaries or not + if it has
a carve out. Pre-checks always have a duplicate too." On Dragon Lake: "we need an option to say in the
future this will need a lake split".

## Built

1. **Source pinned.** The row crop and the parsed row (the coverage strip) stick to the top of the
   detail pane under the verify bar (max 46 % of the height, scrolls inside); the page image opens in a
   tab. The verdict bar is pinned at the foot of the pane (it was sticky only inside the checklist box).
2. **One block per rule**: label, verbatim (only when it differs from the label), then WHERE in words
   (`pipeline/regs/review/scope_words.py`, named by the bundle's own place-namer; a cut it cannot
   name reads by its label), the items when not just the entry's water, the splits as chips (click:
   fly to it; to-verify / not-on-the-map badges), an undrawn "only in" part, side, unplaced names,
   tributaries yes/no with why (`classify.wants_tributaries`, the builder's answer), carve-outs
   (`outside_*`, `tributary_excludes`) and exceptions (`exempts`), the binding with its diagnostics,
   the pre-checks on that rule, and "show on map" (draws the rule's reach).
3. **The entry's extent at the top**: waters (click to focus), where, tributaries (yes / no / not
   stated) and how many rules walk, carve-outs and the heading's own "except …".
4. **Tributary calls in the mark**: "should include" / "should not" per rule and for the entry →
   `Mark.tributaries` `{"entry" | rule_id: include | exclude}` (a rule mark calls only its own rule;
   refused on a split mark). In tickets (round-trips through `parse_ticket`).
5. **"Needs a lake split later"** → `Mark.lake_split_later`; the list is `marks.lake_splits_later`
   (marks in force), `GET /api/review/lake-splits`, `python -m pipeline.regs.review.tickets
   --lake-splits` — the deferred lake-split list (RULINGS K5, T100) as reviewers feed it.
   A mark with neither field writes the same line as before (`marks.line`).
6. **Duplicated pre-checks — the causes**: (a) the undrawn-part normaliser wrote its own fact into
   `review_reason` ("Held on the whole water as an undrawn part …"), so `parser_uncertain` repeated
   `undrawn_part` on 126 rules — now one `undrawn_part` finding carrying the parser's why
   (`precheck.UNDRAWN_REASON`; corpus measure 263 → 137); (b) an ambiguous cut was served twice
   (the `ambiguous_cut` finding and `review.ambiguous_cuts`) and both were listed, and the reach
   diagnostics said it a third time — `ambiguous_cuts` is gone and the rule block drops that
   diagnostic; (c) a rule's findings were listed in the pre-check box AND as flags in the rule — they
   now sit only in the rule; the box lists the entry's own and counts the rest.

Also removed as repeats: the `matched` box (= the waters line), the entry-scope JSON, the "Splits
the rules name" list (on each rule now), the per-entry rebuild commands in the verify bar (the
awaiting-build banner says them). Kept: the pilot toggle and its order (2b80b005).

## Checks
`test_review_scope_words.py` (words, tributaries, carve-outs, one finding per fact),
`test_review_marks.py` (calls, refusals, unchanged old lines, the lake-split list, ticket round
trip); the curation-review suites; frontend `tsc -b`. Screenshots: Iskut River (r6), Hatzic Lake
and Slough (r2).

## Follow-up: automatic cuts labelled by kind
On the map, every pipeline-minted cut (length / gauge / area / border / lake edge) was labelled with the
water's name ("Iskut River" x7). `reuse.cut_label` now names it by kind and specifics ("length cut ·
70.3 km · McLymont Creek", "gauge 08HB023 · <station>", "Region 6 line", "<park> boundary", "lake edge ·
<lake>"); item_geojson flags them `minted`. MapPanel draws unnamed ones small and muted (`item-cuts`), no
always-on label, label on hover/tap, with a legend entry. Test: `test_curation_review_cut_labels.py`.
