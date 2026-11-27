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

import re
from pathlib import Path

import yaml
import pytest
from pydantic import ValidationError

from pipeline.common import curated as C
from pipeline.tests import under_test
from pipeline.tests.conftest import need, ATLAS_HINT


@pytest.fixture(scope="module")
def tree():
    return C.load()


class TestItResolves:
    def test_every_water_file_exists(self, tree):
        # FilePath refuses to construct otherwise, so reaching here is the assertion. Listed
        # explicitly anyway: a field silently dropped from the model would still pass a bare
        # `load()`, and the point is that nothing goes missing unnoticed.
        for name in ("cuts", "name_variants", "areas", "added_lakes", "added_streams", "unmapped_waters",
                     "found_name_calls", "found_names", "tidal", "slough_calls", "slough_page_calls", "steelhead_waters"):
            assert getattr(tree.waters, name).is_file(), name

    def test_unmapped_waters_is_declared_even_though_no_build_reads_it(self, tree):
        # It has zero code references BY DESIGN — it is the worklist for minting more added
        # lakes, and the minting is a hand process. Declaring it here is what stops the next
        # dead-file sweep deleting curated data.
        assert tree.waters.unmapped_waters.is_file()

    def test_regulations_paths_exist(self, tree):
        assert tree.regulations.book_name_links.is_file()

    def test_both_entry_corpora_exist(self, tree):
        syn, dfo = tree.regulations.synopsis, tree.dfo_salmon.places
        assert syn.is_dir() and dfo.is_dir()
        # They are separate FILES with separate models (T324: separate folders too).
        assert syn != dfo

    def test_the_gauge_domain_exists(self, tree):
        # (T1192) stocking and bathymetry are no longer declared as `null` "not built" placeholders: a key that is
        # stated names a file, and those two are declared when they ship.
        assert tree.gauges.matches.is_file() and tree.gauges.match_calls.is_file()

    def test_a_review_is_a_separate_file_from_the_matches_it_corrects(self, tree):
        # The matches file is regenerated wholesale; a decision recorded inside it would not
        # survive. Two files is what makes a regenerate PHYSICALLY unable to harm the review.
        assert tree.gauges.matches != tree.gauges.match_calls

    def test_paths_are_absolute_so_the_working_directory_cannot_matter(self, tree):
        # `pipeline/regs/dfo_salmon/match.py` used a bare Path("pipeline/atlas/splits.json"), correct
        # only when run from the repo root. Same class of bug as a missing path, harder to
        # see.
        assert tree.waters.cuts.is_absolute()
        assert tree.regulations.synopsis.is_absolute()


class TestAWrongPathIsLoud:
    def _blob(self):
        return yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))["curated"]

    def test_a_typo_in_a_filename_refuses_to_load(self):
        blob = self._blob()
        blob["waters"]["cuts"] = "nowhere/at/all.json"
        with pytest.raises(ValidationError) as e:
            C.Curated.model_validate(blob)
        assert "cuts" in str(e.value)

    def test_a_directory_where_a_file_belongs_is_refused(self):
        blob = self._blob()
        blob["waters"]["name_variants"] = "pipeline"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_missing_entries_directory_is_refused(self):
        blob = self._blob()
        blob["regulations"]["synopsis"] = "pipeline/nope"
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_a_dropped_key_is_refused_rather_than_defaulted(self):
        # The important one. A default here would reintroduce exactly the silent-empty
        # behaviour the model exists to remove.
        blob = self._blob()
        del blob["waters"]["cuts"]
        with pytest.raises(ValidationError):
            C.Curated.model_validate(blob)

    def test_config_without_the_tree_names_what_is_wrong(self, tmp_path):
        p = tmp_path / "config.yaml"
        p.write_text("output:\n  base: output\n")
        with pytest.raises(C.ConfigError) as e:
            C.load(p)
        lines = str(e.value).splitlines()
        assert f"{p}: unknown key output" in lines and f"{p}: missing key curated" in lines


# ======================================================================================
# T1192: ONE schema for the whole file — unknown, missing and null keys stop the load, naming the key
# ======================================================================================


def _real_blob() -> dict:
    return yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))


def _keys(node, at=()):
    """Every dotted key of the real config.yaml: (keys, is_a_block)."""
    for k, v in node.items():
        yield at + (k,), isinstance(v, dict)
        if isinstance(v, dict):
            yield from _keys(v, at + (k,))


KEYS = [k for k, _ in _keys(_real_blob())]
BLOCKS = [()] + [k for k, block in _keys(_real_blob()) if block]
LEAVES = [k for k, block in _keys(_real_blob()) if not block]


def _edit(tmp_path, keys, fn):
    """A temp config.yaml: the real one with `fn(parent, last)` applied at `keys`."""
    blob = _real_blob()
    node = blob
    for k in keys[:-1]:
        node = node[k]
    fn(node, keys[-1])
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(blob), encoding="utf-8")
    return p


class TestTheWholeFileIsStrict:
    def test_the_real_config_loads_through_every_view(self):
        blob, cfg = C.parse_config()
        assert isinstance(cfg, C.Config) and set(blob) == set(C.Config.model_fields)
        assert cfg.curated == C.load_repo()
        assert cfg.data_tree == C.load_data_tree() and cfg.generated == C.load_generated.__wrapped__()
        assert cfg.generated.atlas.default_build == blob["generated"]["atlas"]["default_build"]

    @pytest.mark.parametrize("at", BLOCKS, ids=lambda k: ".".join(k) or "(top)")
    def test_an_unknown_key_at_any_level_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, (*at, "t1192_typo"), lambda n, last: n.__setitem__(last, "x"))
        dotted = ".".join((*at, "t1192_typo"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).splitlines() == [f"{p}: unknown key {dotted}"]

    @pytest.mark.parametrize("at", KEYS, ids=".".join)
    def test_a_missing_key_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, at, lambda n, last: n.pop(last))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).splitlines() == [f"{p}: missing key {'.'.join(at)}"]

    @pytest.mark.parametrize("at", LEAVES, ids=".".join)
    def test_a_null_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, at, lambda n, last: n.__setitem__(last, None))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).startswith(f"{p}: {'.'.join(at)} is null")

    def test_every_loader_refuses_what_the_schema_refuses(self, tmp_path):
        p = _edit(tmp_path, ("curated", "waters", "cuts"), lambda n, last: n.__setitem__("cutz", n.pop(last)))
        for load in (C.load, C.load_generated.__wrapped__, C.load_data_tree.__wrapped__):
            with pytest.raises(C.ConfigError, match="unknown key curated.waters.cutz") as e:
                load(p)
            assert "missing key curated.waters.cuts" in str(e.value)

    def test_the_real_file_is_named_as_config_yaml(self):
        with pytest.raises(C.ConfigError, match=r"^config\.yaml: missing key curated$"):
            C._validate(C.Config, {k: v for k, v in _real_blob().items() if k != "curated"}, C.CONFIG)

    def test_a_key_stated_twice_is_refused_by_every_reader(self, tmp_path, monkeypatch):
        # PyYAML keeps the LAST of two equal keys without a word: a second `cuts:` repointed the cuts (T1192 review).
        text = C.CONFIG.read_text(encoding="utf-8")
        line = next(l for l in text.splitlines() if l.strip().startswith("cuts:"))
        p = tmp_path / "config.yaml"
        indent = line[: len(line) - len(line.lstrip())]
        other = _real_blob()["curated"]["waters"]["name_variants"]
        p.write_text(text.replace(line, f"{line}\n{indent}cuts: {other}", 1), encoding="utf-8")
        assert yaml.safe_load(p.read_text())["curated"]["waters"]["cuts"] == other     # what PyYAML alone would load
        want = f"{p}: duplicate key curated.waters.cuts"
        with pytest.raises(C.ConfigError, match=re.escape(want)):
            C.parse_config(p)
        from pipeline.common import stepcache as SC
        monkeypatch.setattr(SC, "config_path", lambda: p)
        with pytest.raises(C.ConfigError, match=re.escape(want)):
            SC.config_now(["curated"])

    def test_every_path_of_the_model_is_absolute(self):
        # A field left out of a model's `_abs` validator stays relative (resolved against the cwd) with nothing failing.
        def leaves(m, at=""):
            for name in type(m).model_fields:
                v = getattr(m, name)
                if isinstance(v, C.BaseModel):
                    yield from leaves(v, f"{at}{name}.")
                else:
                    yield f"{at}{name}", v
        got = dict(leaves(C.parse_config()[1]))
        rel = [k for k, v in got.items() if isinstance(v, Path) and not v.is_absolute()]
        assert not rel, rel
        assert [k for k, v in got.items() if not isinstance(v, Path)] == ["generated.atlas.default_build"]

    def test_no_field_has_a_default(self):
        def walk(m, at=""):
            for name, f in m.model_fields.items():
                yield f"{at}{name}", f
                ann = f.annotation
                if isinstance(ann, type) and issubclass(ann, C.BaseModel):
                    yield from walk(ann, f"{at}{name}.")
        defaulted = [k for k, f in walk(C.Config) if not f.is_required()]
        assert not defaulted, defaulted
        assert all(m.model_config.get("extra") == "forbid" for m in C.BaseModel.__subclasses__()
                   if m.__module__ == C.__name__)


class TestGetPath:
    def test_an_unknown_key_raises(self):
        from project_config import ProjectConfig
        cfg = ProjectConfig()
        for keys in (("no", "such", "key"), ("data", "base"), ("data",), ("generated", "atlas", "default_build")):
            with pytest.raises(KeyError, match=".".join(keys)):
                cfg.get_path(*keys)

    def test_it_reads_the_validated_model_and_takes_no_default(self):
        import inspect
        from project_config import ProjectConfig
        cfg = ProjectConfig()
        assert "default" not in inspect.signature(cfg.get_path).parameters
        assert cfg.get_path("data_accessor", "gpkg_path") == cfg._model.data_accessor.gpkg_path
        assert cfg.get_path("data_accessor", "gpkg_path").is_absolute()


class TestTheTreeIsImmutable:
    def test_a_path_cannot_be_reassigned_at_runtime(self, tree):
        # Curated locations are decided in config.yaml, never patched by a caller. A module
        # that could repoint `splits` mid-run is how two builds read different files.
        with pytest.raises(ValidationError):
            tree.waters.cuts = tree.waters.name_variants


# ======================================================================================
# generated/ — the tree that replaced `output/`
# ======================================================================================


@pytest.fixture(scope="module")
def gen():
    # Read fresh from config.yaml, past the cache: fast-fail's `ci_no_build` plugin moves the cached tree's
    # build paths out of the repo on purpose, and these tests pin what the config says.
    return C.load_generated.__wrapped__()


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

    def test_every_generated_path_is_absolute(self, gen):
        # A relative path resolves against the process's cwd: the review backend runs from
        # tools/curation-review/backend, so `sloughs` (left out of the validator) 404'd its tab.
        def walk(m):
            for name in type(m).model_fields:
                v = getattr(m, name)
                if hasattr(type(v), "model_fields"):
                    yield from walk(v)
                elif hasattr(v, "is_absolute"):
                    yield name, v
        rel = [n for n, v in walk(gen) if not v.is_absolute()]
        assert not rel, rel

    def test_nothing_under_generated_is_curated_or_source(self, gen, tree):
        # A generated path landing inside curated/ would mean a rebuild can destroy human
        # work — the one failure this whole layout exists to prevent.
        for p in (gen.atlas.builds, gen.reaches, gen.bundle, gen.tiles,
                  gen.added_streams, gen.scratch, gen.regs.parse,
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

    @pytest.mark.needs_atlas
    def test_default_registry_path_points_at_a_real_build(self):
        # It used to return `output/pipeline/graph/registry.json` — a directory that never
        # existed — and was the declared default of seven parser tools.
        from pipeline.atlas.registry import default_registry_path
        need(None, "atlas", under_test.atlas() / "registry.json", ATLAS_HINT)
        p = default_registry_path()
        assert p.name == "registry.json" and p.parent.is_dir()


class TestNoLiteralOutputPaths:
    """`output/` may not come back as a string literal anywhere in the live tree."""

    def test_no_module_writes_to_a_literal_output_path(self):
        import re
        root = C.REPO_ROOT
        skip = ("archive", ".venv", "node_modules", "graphify-out", "__pycache__",
                "docs/archive")
        # Prose that EXPLAINS the retirement is allowed; a path used as a value is not.
        # TWO shapes, because the first version of this test only caught the first and a
        # real `root / "output" / "tiles" / "basemap.pmtiles"` survived the sweep:
        #   "output/tiles/x"      a path inside one string
        #   "output" / "tiles"    the same path as joined Path segments
        code = re.compile(r"""["'](?:\./)?output/|["']output["']\s*/""")
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
