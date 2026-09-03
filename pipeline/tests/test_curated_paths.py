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

from pipeline.common import curated as C


@pytest.fixture(scope="module")
def tree():
    return C.load()


class TestItResolves:
    def test_every_water_file_exists(self, tree):
        # FilePath refuses to construct otherwise, so reaching here is the assertion. Listed
        # explicitly anyway: a field silently dropped from the model would still pass a bare
        # `load()`, and the point is that nothing goes missing unnoticed.
        for name in ("splits", "name_variants", "areas", "added_lakes",
                     "added_streams", "ungazetted"):
            assert getattr(tree.waters, name).is_file(), name

    def test_ungazetted_is_declared_even_though_nothing_imports_it(self, tree):
        # It has zero code references BY DESIGN — it is the worklist for minting more added
        # lakes, and the minting is a hand process. Declaring it here is what stops the next
        # dead-file sweep deleting curated data.
        assert tree.waters.ungazetted.is_file()

    def test_regulations_paths_exist(self, tree):
        assert tree.regulations.overrides.is_file()

    def test_both_entry_corpora_exist(self, tree):
        e = tree.regulations.entries
        assert e.synopsis.is_dir() and e.dfo_salmon.is_dir()
        # They are separate FILES with separate models; only the location is shared.
        assert e.synopsis != e.dfo_salmon

    def test_the_built_domain_exists_and_the_unbuilt_are_declared(self, tree):
        assert tree.gauges.matches is not None and tree.gauges.matches.is_file()
        assert tree.gauges.review is not None and tree.gauges.review.is_file()
        # Stocking and bathymetry are named before they exist so the freshness gate is
        # already waiting. Asserting they are None would break the day they are built;
        # asserting the DOMAIN resolves is the durable check.
        for name in ("gauges", "stocking", "bathymetry"):
            assert tree.domain(name) is not None, name

    def test_a_review_is_a_separate_file_from_the_matches_it_corrects(self, tree):
        # The matches file is regenerated wholesale; a decision recorded inside it would not
        # survive. Two files is what makes a regenerate PHYSICALLY unable to harm the review.
        assert tree.gauges.matches != tree.gauges.review

    def test_paths_are_absolute_so_the_working_directory_cannot_matter(self, tree):
        # `pipeline/regs/dfo_salmon/match.py` used a bare Path("pipeline/atlas/splits.json"), correct
        # only when run from the repo root. Same class of bug as a missing path, harder to
        # see.
        assert tree.waters.splits.is_absolute()
        assert tree.regulations.entries.synopsis.is_absolute()


class TestAWrongPathIsLoud:
    def _blob(self):
        return yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))["curated"]

    def test_a_typo_in_a_filename_refuses_to_load(self):
        blob = self._blob()
        blob["waters"]["splits"] = "nowhere/at/all.json"
        with pytest.raises(ValidationError) as e:
            C.Curated.model_validate(blob)
        assert "splits" in str(e.value)

    def test_a_directory_where_a_file_belongs_is_refused(self):
        blob = self._blob()
        blob["waters"]["name_variants"] = "pipeline"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_missing_entries_directory_is_refused(self):
        blob = self._blob()
        blob["regulations"]["entries"]["synopsis"] = "pipeline/nope"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_dropped_key_is_refused_rather_than_defaulted(self):
        # The important one. A default here would reintroduce exactly the silent-empty
        # behaviour the model exists to remove.
        blob = self._blob()
        del blob["waters"]["splits"]
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
            tree.waters.splits = tree.waters.name_variants


# ======================================================================================
# generated/ — the tree that replaced `output/`
# ======================================================================================


@pytest.fixture(scope="module")
def gen():
    return C.load_generated()


class TestGeneratedTree:
    """`output/` was retired into `data/generated/`. These pin the two properties that made
    the old tree unsafe, not the paths themselves."""

    def test_the_two_roots_cannot_disagree(self, gen):
        # config.yaml states the generated root twice — `data_tree.generated` (the
        # three-kinds statement) and `generated.base` (the detailed tree). `load_generated`
        # raises if they name different directories, so this is the assertion that the
        # cross-check is wired, not that the strings happen to match today.
        assert gen.base == C.load_data_tree().generated

    def test_a_disagreement_is_refused(self, tmp_path):
        blob = yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))
        blob["generated"]["base"] = "data/somewhere-else"
        bad = tmp_path / "config.yaml"
        bad.write_text(yaml.safe_dump(blob), encoding="utf-8")
        with pytest.raises(ValueError, match="disagrees with itself"):
            C.load_generated(bad)

    def test_nothing_under_generated_is_curated_or_source(self, gen, tree):
        # A generated path landing inside curated/ would mean a rebuild can destroy human
        # work — the one failure this whole layout exists to prevent.
        for p in (gen.atlas.builds, gen.reaches, gen.bundle, gen.tiles,
                  gen.added_streams, gen.scratch, gen.regs.parse, gen.regs.parsing,
                  gen.regs.extraction, gen.regs.dfo_salmon, gen.regs.entries_backup,
                  gen.gauges.feeds):
            assert gen.base in p.parents or p == gen.base, p
            assert tree.base not in p.parents, f"{p} would let a rebuild overwrite curation"

    def test_a_reader_cannot_silently_invent_a_build(self, gen):
        # THE `output/` BUG. A missing output directory was created on demand, so a build
        # could write somewhere new while a reader went on reading the old place and neither
        # said so. `require_build` refuses, and names the command that fixes it.
        with pytest.raises(FileNotFoundError, match="pipeline.atlas.build"):
            gen.require_build("a-build-that-was-never-made")

    def test_default_registry_path_points_at_a_real_build(self):
        # It used to return `output/pipeline/graph/registry.json` — a directory that never
        # existed — and was the declared default of seven parser tools.
        from pipeline.atlas.registry import default_registry_path
        try:
            p = default_registry_path()
        except FileNotFoundError:
            pytest.skip("no full build present")
        assert p.name == "registry.json" and p.parent.is_dir()


class TestNoLiteralOutputPaths:
    """`output/` may not come back as a string literal anywhere in the live tree."""

    def test_no_module_writes_to_a_literal_output_path(self):
        import re
        root = C.REPO_ROOT
        skip = ("archive", ".venv", "node_modules", "graphify-out", "__pycache__",
                "docs/archive")
        # Prose that EXPLAINS the retirement is allowed; a path used as a value is not.
        code = re.compile(r"""["'](?:\./)?output/""")
        bad = []
        for f in root.rglob("*.py"):
            rel = f.relative_to(root).as_posix()
            if any(k in rel for k in skip):
                continue
            for i, line in enumerate(f.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("#") or stripped.startswith("*"):
                    continue
                if code.search(line):
                    bad.append(f"{rel}:{i}: {line.strip()}")
        assert not bad, (
            "literal output/ path(s) — use pipeline.common.curated.GENERATED:\n  "
            + "\n  ".join(bad))
