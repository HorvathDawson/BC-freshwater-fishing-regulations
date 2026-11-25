"""T1051 (review of #144): the Gear tab's "Treble and barbed hooks allowed" ⓘ opens a term that quotes BOTH halves of the
line, p.80's treble note and p.8's barbed-in-lakes sentence, under a title that does not contradict it."""
from pipeline.deliver.answers import glossary as GL
from pipeline.deliver.bundle.glossary_source import rows_from_book

#: zp:barbless_single_hook_streams as the 0.9.0 export holds it (its printed words and pages)
PRINTED = ("It is unlawful to: Use barbed hooks or a hook with more than one point in any river, stream, creek or slough "
           "in B.C. (Note: the use of barbed hooks in lakes is permitted, unless noted in the Regional Water-Specific "
           "Tables).")


class _Data:
    def entry(self, eid):
        assert eid == "zp:barbless_single_hook_streams"
        return {"printed": PRINTED, "pages": [8]}


def test_the_term_quotes_both_halves_of_the_line():
    t = GL.t_treble_and_barbed(GL.Book(rows_from_book()), _Data())
    assert (t["id"], t["term"], t["pages"]) == ("treble_and_barbed_hooks", "Treble and barbed hooks", [8, 80])
    assert 'use of a treble hook is permitted unless "single hook" is specified' in t["quote"]
    assert "the use of barbed hooks in lakes is permitted" in t["quote"]
    assert "single barbless" not in t["term"].lower()
    assert GL.t_treble_and_barbed in GL.GENERATORS
