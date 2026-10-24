"""`DECISIONS`: where the display facts and the page differ or the page is unclear (D1 …), written down
once."""
from __future__ import annotations




DECISIONS = [
    "D1 Part facts are read off the export's DECODED model (`export_codec.expand`, the reference "
    "decoder): the runs, splits and entries are composed from the bundle by the export "
    "(`part_runs`, `spans.compose_runs`), and recomposing them here would be a second "
    "implementation. The pair's `about.bundle` digests must equal the bundle's, or it is refused.",
    "D2 Part labels are computed in the page's part order (most sections first, the export's "
    "order among equals) because the tie-breaks ('· plus …') depend on it; every part is named "
    "by its EXPORT index in the output.",
    "D3 Closed all year is the status index's answer (`status_index.set_profile` all CLOSED: every "
    "game fish under a speaking full closure every day), not the page's re-reading (`isBroad`: "
    "a closure or a federal release naming ALL_GAME_FISH). One definition of closed (AGENTS 56).",
    "D4 The steelhead line (5.6) is the card's: it ships in the `rows` section, read off the rows "
    "exactly as the page does (a steelhead row exists among the card's rows).",
    "D5 `plain` is the page's `sayRule` for the rule's own fish (no row context); null where the "
    "page writes no sentence (gear, conduct, vessel, `while`, a place in words, a side, standing) "
    "and falls back to `label`.",
    "D6 Splits for run-end names are the export's whole `splits` table (the page embeds the subset "
    "its 28 waters name); a '(km N)' tie-break can differ only where an unnamed-by-any-run split "
    "on the same water shares a name.",
]
