"""The BC River Forecast Centre pull — the parts that decide what a reader is shown."""

from __future__ import annotations

from pipeline.hydro import forecast as F


class TestIssuedAt:
    """When a run was made. A comparison, not a caption — see `_issued`."""

    def test_reads_the_centres_own_wording(self):
        # This is the form the live ArcGIS layers actually return, verified against all
        # three services. Comparing it as a raw string sorts on the letter "U".
        assert F._issued("Updated at: 09:38 AM Tue 2026-09-01") == "2026-09-01T09:38:00"
        assert F._issued("Updated at: 12:05 PM Mon 2026-08-31") == "2026-08-31T12:05:00"

    def test_midnight_and_noon_do_not_swap(self):
        # 12 AM is 00 and 12 PM is 12. The naive "+12 for PM" puts noon at 24:05 and
        # midnight at midday — a twelve-hour error in which run is fresher.
        assert F._issued("Updated at: 12:00 AM Tue 2026-09-01") == "2026-09-01T00:00:00"
        assert F._issued("Updated at: 12:00 PM Tue 2026-09-01") == "2026-09-01T12:00:00"

    def test_normalised_forms_sort_chronologically(self):
        a = F._issued("Updated at: 09:38 AM Tue 2026-09-01")
        b = F._issued("Updated at: 10:30 AM Tue 2026-09-01")
        assert (a or "") < (b or "")

    def test_epoch_milliseconds_still_work(self):
        assert F._issued(0).startswith("1970-01-01")

    def test_an_unreadable_stamp_is_kept_not_dropped(self):
        # The Centre's own words beat nothing; the tie-break falls back to model order.
        assert F._issued("sometime tuesday") == "sometime tuesday"
        assert F._issued("") is None
        assert F._issued(None) is None


class TestNum:
    def test_pulls_a_number_out_of_a_text_column(self):
        assert F._num("12.4 m3/s") == 12.4
        assert F._num("-3") == -3.0

    def test_a_blank_is_none_never_zero(self):
        # A zero here would read as a river that has stopped flowing.
        assert F._num("") is None
        assert F._num(None) is None
        assert F._num("n/a") is None


class TestModels:
    def test_every_model_names_the_end_it_is_asked_about(self):
        # A freshet model is asked how HIGH and a low-flow model how LOW. Showing the
        # average of an ELF run would smooth away the question it exists to answer.
        for name, cfg in F.MODELS.items():
            assert cfg["rep"] in ("min", "ave", "max"), name
            assert cfg["rep"] in cfg or f"f{cfg['rep']}" in cfg, name
        assert F.MODELS["ELF"]["rep"] == "min"
        assert F.MODELS["CLEVER"]["rep"] == "max"

    def test_the_attribution_is_the_provinces_own_words(self):
        # Required verbatim wherever a forecast appears. Not ours to reword.
        assert "BC River Forecast Centre" in F.ATTRIBUTION
        assert "at their own risk" in F.ATTRIBUTION
