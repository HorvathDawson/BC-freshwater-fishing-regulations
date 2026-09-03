"""Every curated path resolves, and a wrong one fails loudly.

THE BUG CLASS THIS CLOSES. `ProjectConfig.get_path` returns `Path()` — the current
directory — for a key that is not in `config.yaml`. Hand that to a loader and you get an
empty list, and an empty list of curated cuts is indistinguishable from a build with no cuts
to make. A `--splits` flag with no default silently dropped all 376 curated cuts from three
full builds; one was promoted; nothing looked wrong.

So the property being tested is not "the paths are correct" but **"a wrong path cannot be
quiet"**.
"""

from __future__ import annotations

import yaml
import pytest
from pydantic import ValidationError

from pipeline import curated as C


@pytest.fixture(scope="module")
def tree():
    return C.load()


class TestItResolves:
    def test_every_authored_file_exists(self, tree):
        # FilePath refuses to construct otherwise, so reaching here is the assertion. Listed
        # explicitly anyway: a field silently dropped from the model would still pass a bare
        # `load()`, and the point is that nothing goes missing unnoticed.
        for name in ("splits", "name_variants", "overrides", "areas",
                     "added_lakes", "added_streams"):
            p = getattr(tree, name)
            assert p.is_file(), name

    def test_both_entry_corpora_exist(self, tree):
        assert tree.entries.synopsis.is_dir()
        assert tree.entries.dfo_salmon.is_dir()
        # They are separate FILES with separate models; only the location is shared.
        assert tree.entries.synopsis != tree.entries.dfo_salmon

    def test_the_built_match_artifacts_exist_and_the_unbuilt_are_none(self, tree):
        assert tree.matches.gauge is not None and tree.matches.gauge.is_file()
        # Declared as null on purpose so there is one place to point them when they land.
        # Asserting they are None would break the day they are built; asserting the FIELD
        # exists is the durable check.
        assert "stocking" in type(tree.matches).model_fields
        assert "charts" in type(tree.matches).model_fields

    def test_paths_are_absolute_so_the_working_directory_cannot_matter(self, tree):
        # `pipeline/dfo_salmon/match.py` used a bare Path("pipeline/splits.json"), correct
        # only when run from the repo root. Same class of bug as a missing path, harder to
        # see.
        assert tree.splits.is_absolute()
        assert tree.entries.synopsis.is_absolute()


class TestAWrongPathIsLoud:
    def _blob(self):
        return yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))["curated"]

    def test_a_typo_in_a_filename_refuses_to_load(self):
        blob = self._blob()
        blob["splits"] = "pipeline/typo_splits.json"
        with pytest.raises(ValidationError) as e:
            C.Curated.model_validate(blob)
        assert "splits" in str(e.value)

    def test_a_directory_where_a_file_belongs_is_refused(self):
        blob = self._blob()
        blob["name_variants"] = "pipeline"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_missing_entries_directory_is_refused(self):
        blob = self._blob()
        blob["entries"]["synopsis"] = "pipeline/nope"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_dropped_key_is_refused_rather_than_defaulted(self):
        # The important one. A default here would reintroduce exactly the silent-empty
        # behaviour the model exists to remove.
        blob = self._blob()
        del blob["splits"]
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_config_without_the_tree_says_which_document_explains_it(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "config.yaml"
            p.write_text("output:\n  base: output\n")
            with pytest.raises(KeyError, match="curated"):
                C.load(p)


class TestTheTreeIsImmutable:
    def test_a_path_cannot_be_reassigned_at_runtime(self, tree):
        # Curated locations are decided in config.yaml, never patched by a caller. A module
        # that could repoint `splits` mid-run is how two builds read different files.
        with pytest.raises(ValidationError):
            tree.splits = tree.name_variants
