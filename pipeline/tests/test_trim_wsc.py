"""Unit tests for trim_wsc — WSC zero-padding normalisation."""

from pipeline.utils.wsc import trim_wsc


class TestTrimWsc:
    def test_strips_trailing_zeros(self):
        raw = "930-508366-413291-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000-000000"
        assert trim_wsc(raw) == "930-508366-413291"

    def test_single_trailing_group(self):
        assert trim_wsc("300-123456-000000") == "300-123456"

    def test_no_trailing_zeros(self):
        assert trim_wsc("300-123456-789012") == "300-123456-789012"

    def test_single_segment(self):
        assert trim_wsc("930") == "930"

    def test_all_zeros_except_first(self):
        assert trim_wsc("100-000000-000000") == "100"

    def test_empty_string(self):
        assert trim_wsc("") == ""

    def test_none_like(self):
        # Callers pass str(...) or "", never None — but "" is the canonical empty
        assert trim_wsc("") == ""

    def test_idempotent(self):
        trimmed = "930-508366-413291"
        assert trim_wsc(trimmed) == trimmed

    def test_waterbody_key_passthrough(self):
        # Integer waterbody_keys like "351" have no dashes — unchanged
        assert trim_wsc("351") == "351"

    def test_intermediate_zero_preserved(self):
        # Hypothetical — real FWA data never has this, but trim_wsc is
        # trailing-only by design so intermediate zeros survive.
        assert trim_wsc("100-000000-123456-000000") == "100-000000-123456"
