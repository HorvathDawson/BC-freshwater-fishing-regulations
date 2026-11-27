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
from pathlib import Path, PurePosixPath

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
        for name in ("cuts", "name_variants", "area_closures", "added_lakes", "added_streams", "unmapped_waters",
                     "found_name_calls", "found_names", "tidal_limits", "slough_calls", "slough_page_calls",
                     "steelhead_waters"):
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
        return C.read_config()["curated"]         # (T1192) the names live in the schema: config.yaml + `At`

    def _validate(self, blob):
        """The `curated:` block validated as the loader does: its names joined to the curated root."""
        return C.Curated.model_validate(blob, context={"roots": {"curated": C.load_roots().curated}})

    def test_a_typo_in_a_filename_refuses_to_load(self):
        blob = self._blob()
        blob["waters"]["cuts"] = "nowhere/at/all.json"
        with pytest.raises(ValidationError) as e:
            self._validate(blob)
        assert "cuts" in str(e.value)

    def test_a_directory_where_a_file_belongs_is_refused(self):
        blob = self._blob()
        blob["waters"]["name_variants"] = "waters"
        with pytest.raises(ValidationError):
            self._validate(blob)

    def test_a_missing_entries_directory_is_refused(self):
        blob = self._blob()
        blob["regulations"]["synopsis"] = "regulations/nope"
        with pytest.raises(ValidationError):
            self._validate(blob)

    def test_a_dropped_key_is_refused_rather_than_defaulted(self):
        # The important one. A default here would reintroduce exactly the silent-empty
        # behaviour the model exists to remove.
        blob = self._blob()
        del blob["waters"]["cuts"]
        with pytest.raises(ValidationError):
            self._validate(blob)

    def test_config_without_the_tree_names_what_is_wrong(self, tmp_path):
        p = tmp_path / "config.yaml"
        p.write_text(yaml.safe_dump({"roots": _real_blob()["roots"], "output": {"base": "output"}}))
        with pytest.raises(C.ConfigError) as e:
            C.load(p)
        lines = str(e.value).splitlines()
        assert f"{p}: unknown key output" in lines and f"{p}: missing key atlas_build" in lines


# ======================================================================================
# T1192: ONE schema for the whole file — unknown, missing and null keys stop the load, naming the key
# ======================================================================================


def _real_blob() -> dict:
    """config.yaml as the file states it (the roots, the gpkg's name, `atlas_build`)."""
    return yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))


def _resolved() -> dict:
    """What every reader sees: config.yaml with the schema's names (`curated.read_config`)."""
    return C.read_config()


def _keys(node, at=()):
    """Every dotted key of the real config.yaml: (keys, is_a_block)."""
    for k, v in node.items():
        yield at + (k,), isinstance(v, dict)
        if isinstance(v, dict):
            yield from _keys(v, at + (k,))


KEYS = [k for k, _ in _keys(_real_blob())]
BLOCKS = [()] + [k for k, block in _keys(_real_blob()) if block]
LEAVES = [k for k, block in _keys(_real_blob()) if not block]


#: the refusal's hint for a key stated in a block of the schema's names (`C.DECLARE_HERE`, typed out: the pin)
DECLARE = (" (a new file's name is declared in pipeline/common/curated.py: a field there with `At(\"<name>\")` and "
           "its note)")


def _edit(tmp_path, keys, fn):
    """A temp config.yaml: the real one with `fn(parent, last)` applied at `keys` (a block the file does not state, as
    `curated`, is stated for it)."""
    blob = _real_blob()
    node = blob
    for k in keys[:-1]:
        node = node.setdefault(k, {})
    fn(node, keys[-1])
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(blob), encoding="utf-8")
    return p


class TestTheWholeFileIsStrict:
    def test_the_real_config_loads_through_every_view(self):
        blob, cfg = C.parse_config()
        assert isinstance(cfg, C.Config) and set(blob) == set(C.Config.model_fields)
        assert cfg.curated == C.load_repo()
        assert cfg.roots == C.load_roots() and cfg.generated == C.load_generated.__wrapped__()
        assert cfg.atlas_build == _real_blob()["atlas_build"]
        assert cfg.generated.build() == cfg.generated.atlas.builds / cfg.atlas_build

    @pytest.mark.parametrize("at", BLOCKS, ids=lambda k: ".".join(k) or "(top)")
    def test_an_unknown_key_at_any_level_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, (*at, "t1192_typo"), lambda n, last: n.__setitem__(last, "x"))
        dotted = ".".join((*at, "t1192_typo"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        # `source` holds the schema's names beside the gpkg: a new key there is a file stated in the wrong place
        assert str(e.value).splitlines() == [f"{p}: unknown key {dotted}" + (DECLARE if at == ("source",) else "")]

    @pytest.mark.parametrize("at", KEYS, ids=".".join)
    def test_a_missing_key_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, at, lambda n, last: n.pop(last))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        want = {"source": "source.fwa_gpkg"}.get(".".join(at), ".".join(at))   # the block's other names are the schema's
        assert str(e.value).splitlines() == [f"{p}: missing key {want}"]

    @pytest.mark.parametrize("at", [("curated", "waters"), ("generated",), ("generated", "regs"), ("cache",)],
                             ids=".".join)
    def test_an_unknown_key_under_a_block_of_schema_names_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, (*at, "t1192_typo"), lambda n, last: n.__setitem__(last, "x"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).splitlines() == [f"{p}: unknown key {'.'.join((*at, 't1192_typo'))}{DECLARE}"]

    def test_a_new_file_stated_in_config_yaml_is_told_where_files_are_declared(self, tmp_path):
        """A person adding a file the way config.yaml once took one (`curated.waters.<stem>: "waters/<file>"`) is
        refused with where it goes now; a key outside the schema's blocks gets no such hint."""
        p = _edit(tmp_path, ("curated", "waters", "new"), lambda n, last: n.__setitem__(last, "waters/new.json"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == f"{p}: unknown key curated.waters.new{DECLARE}"
        p = _edit(tmp_path, ("roots", "new"), lambda n, last: n.__setitem__(last, "x"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == f"{p}: unknown key roots.new"

    @pytest.mark.parametrize("at", [("curated",), ("cache",), ("generated",), ("curated", "waters"),
                                    ("generated", "atlas")], ids=".".join)
    @pytest.mark.parametrize("value", [{}, None], ids=["empty", "null"])
    def test_a_block_of_only_schema_names_stated_empty_or_null_is_refused_naming_it(self, tmp_path, at, value):
        """A block config.yaml has nothing of its own in states nothing when empty or null: refused by name, for the
        right reason, by every reader (never merged into the schema's names as if it were not there)."""
        p = _edit(tmp_path, at, lambda n, last: n.__setitem__(last, value))
        what = "null" if value is None else "an empty block"
        for read in (C.parse_config, C.read_config):
            with pytest.raises(C.ConfigError) as e:
                read(p)
            assert str(e.value) == (f"{p}: {'.'.join(at)} is {what}: remove it (every name in it is declared in "
                                    "pipeline/common/curated.py)")

    def test_a_block_with_keys_of_its_own_says_what_it_needs(self, tmp_path):
        p = _edit(tmp_path, ("source",), lambda n, last: n.__setitem__(last, {}))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == f"{p}: missing key source.fwa_gpkg"
        p = _edit(tmp_path, ("source",), lambda n, last: n.__setitem__(last, None))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == f"{p}: source is null: state its keys"
        p = _edit(tmp_path, ("curated",), lambda n, last: n.__setitem__(last, "waters"))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == f"{p}: curated is a block of keys, never a value (got 'waters')"

    @pytest.mark.parametrize("value", ["../x", "/abs", "", ".", "..", "full/next", "a\\b"])
    def test_atlas_build_is_a_bare_build_name(self, tmp_path, value):
        """`atlas_build` names one directory under the builds; a path, `.`, `..` or empty is refused naming the key."""
        p = _edit(tmp_path, ("atlas_build",), lambda n, last: n.__setitem__(last, value))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value) == (f"{p}: atlas_build: Value error, a bare build name under GENERATED.atlas.builds (as "
                                f"\"full\"), never a path, `.`, `..` or empty (got {value!r})")

    @pytest.mark.parametrize("at", [("curated", "waters", "cuts"), ("generated", "atlas", "builds"),
                                    ("source", "coast_gpkg"), ("cache", "dfo_salmon")], ids=".".join)
    def test_a_name_the_schema_declares_is_refused_in_config_yaml(self, tmp_path, at):
        """(T1192 Option A) one name per file, in one place: config.yaml stating a name the schema declares (`At`) is
        refused naming the key, never merged and never preferred (a second home for the file list)."""
        p = _edit(tmp_path, at, lambda n, last: n.__setitem__(last, "elsewhere"))
        for read in (C.parse_config, C.read_config):
            with pytest.raises(C.ConfigError) as e:
                read(p)
            assert str(e.value) == (f"{p}: unknown key {'.'.join(at)} (its name is declared in "
                                    "pipeline/common/curated.py, not in config.yaml)")

    @pytest.mark.parametrize("at", LEAVES, ids=".".join)
    def test_a_null_is_refused_naming_it(self, tmp_path, at):
        p = _edit(tmp_path, at, lambda n, last: n.__setitem__(last, None))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).startswith(f"{p}: {'.'.join(at)} is null")

    def test_every_loader_refuses_what_the_schema_refuses(self, tmp_path):
        p = _edit(tmp_path, ("curated", "waters", "cutz"), lambda n, last: n.__setitem__(last, "waters/cuts.json"))
        blob = yaml.safe_load(p.read_text())
        blob.pop("atlas_build")
        p.write_text(yaml.safe_dump(blob))
        for load in (C.load, C.load_generated.__wrapped__, C.load_roots):
            with pytest.raises(C.ConfigError, match="unknown key curated.waters.cutz") as e:
                load(p)
            assert "missing key atlas_build" in str(e.value)

    def test_the_real_file_is_named_as_config_yaml(self):
        with pytest.raises(C.ConfigError, match=r"^config\.yaml: missing key curated$"):
            C._validate_config({k: v for k, v in _resolved().items() if k != "curated"}, C.CONFIG)

    def test_a_key_stated_twice_is_refused_by_every_reader(self, tmp_path, monkeypatch):
        # PyYAML keeps the LAST of two equal keys without a word: a second `cuts:` repointed the cuts (T1192 review).
        text = C.CONFIG.read_text(encoding="utf-8")
        line = next(l for l in text.splitlines() if l.strip().startswith("fwa_gpkg:"))
        p = tmp_path / "config.yaml"
        indent = line[: len(line) - len(line.lstrip())]
        other = "other.gpkg"
        p.write_text(text.replace(line, f"{line}\n{indent}fwa_gpkg: {other}", 1), encoding="utf-8")
        assert yaml.safe_load(p.read_text())["source"]["fwa_gpkg"] == other     # what PyYAML alone would load
        want = f"{p}: duplicate key source.fwa_gpkg"
        with pytest.raises(C.ConfigError, match=re.escape(want)):
            C.parse_config(p)
        from pipeline.common import stepcache as SC
        monkeypatch.setattr(SC, "config_path", lambda: p)
        with pytest.raises(C.ConfigError, match=re.escape(want)):
            SC.config_now(["source"])

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
        assert [k for k, v in got.items() if not isinstance(v, Path)] == ["atlas_build"]

    def test_every_path_has_one_name_in_one_place(self):
        """(T1192 Option A) every path leaf of the trees has its name declared on its field (`At`) or, for the one named
        source file that is a choice, a key of config.yaml: never both, never neither. `NAMES` is read off the fields,
        so a name with no field cannot exist; a field with no name fails here (and at load: a missing key)."""
        def walk(m, at=()):
            for name, f in m.model_fields.items():
                ann = f.annotation
                if isinstance(ann, type) and issubclass(ann, C.BaseModel):
                    yield from walk(ann, (*at, name))
                else:
                    yield (*at, name), [x.name for x in f.metadata if isinstance(x, C.At)]
        names, stated = C.NAMES, _real_blob()
        def has(blob, keys):
            for k in keys:
                if not isinstance(blob, dict) or k not in blob:
                    return False
                blob = blob[k]
            return True
        got = {".".join(k): (bool(at), has(stated, k)) for k, at in walk(C.Config) if k[0] in C.TREES}
        assert len(got) == 78 and all(has(names, k.split(".")) == a for k, (a, _s) in got.items())
        assert {k for k, (a, s) in got.items() if s} == {"source.fwa_gpkg"}          # the file's one path
        assert not [k for k, (a, s) in got.items() if a == s], "a path named in both places, or in neither"

    def test_no_field_has_a_default(self):
        def walk(m, at=""):
            for name, f in m.model_fields.items():
                yield f"{at}{name}", f
                ann = f.annotation
                if isinstance(ann, type) and issubclass(ann, C.BaseModel):
                    yield from walk(ann, f"{at}{name}.")
        defaulted = [k for k, f in walk(C.Config) if not f.is_required()]
        assert not defaulted, defaulted
        def every(cls):                 # (T1192) the tree models sit under `_Tree`: every subclass, at any depth
            for m in cls.__subclasses__():
                yield m
                yield from every(m)
        mine = [m for m in every(C.BaseModel) if m.__module__ == C.__name__]
        assert {"Waters", "Curated", "GeneratedRegs", "Generated", "Source", "Config"} <= {m.__name__ for m in mine}
        assert all(m.model_config.get("extra") == "forbid" for m in mine)


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
        assert cfg.get_path("cache", "dfo_salmon") == cfg._model.cache.dfo_salmon
        assert cfg.get_path("cache", "dfo_salmon").is_absolute()


#: (T1222) one fresh process per entry: `dotenv.load_dotenv` stubbed to count its calls, then the reads; prints the
#: calls, then whether a real `dotenv` was imported (a second run with no stub: `sys.modules` after the reads)
_ENV_PROBE = """
import json, sys, types
calls = []
stubbed = sys.argv[1] == "stub"
if stubbed:
    stub = types.ModuleType("dotenv")
    stub.load_dotenv = lambda path=None, *a, **k: calls.append(str(path))
    sys.modules["dotenv"] = stub
import pipeline.common.curated, project_config
for read in sys.argv[2:]:
    if read == "source":
        from pipeline.common.curated import SOURCE
        SOURCE.fwa_gpkg
    elif read == "curated":
        from pipeline.common.curated import CURATED
        CURATED.waters.cuts
    elif read == "get_config":
        from project_config import get_config
        get_config()
print(json.dumps({"calls": calls, "dotenv_imported": not stubbed and "dotenv" in sys.modules}))
"""


@pytest.mark.parametrize("reads", [["source"], ["curated"], ["get_config"], ["source", "curated", "get_config"]],
                         ids="+".join)
def test_no_config_read_loads_a_dotenv_file(reads):
    """(T1222, owner 2026-11-27) The pipeline reads no `.env`: no config read calls `load_dotenv`, and importing the
    config modules and reading config does not import `dotenv` at all. Until T1222 `parse_config` loaded the repo's
    `.env` (T1192); a run-time setting now comes from the shell environment. Mutation: put the load back -> fails."""
    import json
    import os
    import subprocess
    import sys
    env = {k: v for k, v in os.environ.items() if k != C.ROOT_ENV}
    env["PYTHONPATH"] = str(C.REPO_ROOT)
    for mode in ("stub", "real"):
        got = subprocess.run([sys.executable, "-c", _ENV_PROBE, mode, *reads], cwd=C.REPO_ROOT, env=env, check=True,
                             capture_output=True, text=True)
        assert json.loads(got.stdout.strip().splitlines()[-1]) == {"calls": [], "dotenv_imported": False}, mode


class TestNamedSourceFiles:
    """(T1192) config.yaml `source:`: each named source file has ONE name, relative to the source root, read as
    `SOURCE.<name>` by the fetcher that writes it and by every reader."""

    def test_each_file_is_named_once_in_the_config(self):
        blob = _resolved()
        leaves = [k for k, block in _keys(blob) if not block]
        for name, rel in blob["source"].items():
            spelt = [".".join(k) for k in leaves if isinstance(v := _at(blob, k), str) and v.endswith(rel)]
            assert spelt == [f"source.{name}"], (name, spelt)

    def test_a_named_file_is_its_name_under_the_source_root(self):
        blob, root = _resolved(), C.load_roots().source
        assert set(blob["source"]) == set(C.Source.model_fields)
        for name, rel in blob["source"].items():
            assert getattr(C.SOURCE, name) == root / rel and getattr(C.SOURCE, name).is_absolute()
        assert C.SOURCE / "bc_places.json" == root / "bc_places.json" and str(C.SOURCE) == str(root)

    def test_the_same_names_under_another_source_tree(self, tmp_path):
        blob = _real_blob()
        assert C.SOURCE.under(tmp_path).fwa_gpkg == tmp_path / blob["source"]["fwa_gpkg"]
        assert C.SOURCE.under(tmp_path) / "x.json" == tmp_path / "x.json"

    def test_a_read_records_its_own_name_and_the_root_only(self, monkeypatch):
        from pipeline import _trace
        monkeypatch.setattr(_trace, "_config_seen", {})
        C.SOURCE.fwa_gpkg
        assert set(_trace._config_seen) == {"source.fwa_gpkg", "roots.source"}
        monkeypatch.setattr(_trace, "_config_seen", {})
        C.SOURCE.under("/elsewhere").coast_gpkg
        assert set(_trace._config_seen) == {"source.coast_gpkg"}

    @pytest.mark.parametrize("bad", ["/abs/x.gpkg", "../x.gpkg", "sub/../../x.gpkg", "", "."])
    def test_a_name_outside_the_source_root_is_refused_naming_it(self, tmp_path, bad):
        p = _edit(tmp_path, ("source", "fwa_gpkg"), lambda n, last: n.__setitem__(last, bad))
        with pytest.raises(C.ConfigError, match=r"source\.fwa_gpkg: Value error, a name under the source root"):
            C.parse_config(p)

    def test_a_name_the_block_does_not_declare_is_refused(self):
        with pytest.raises(AttributeError, match=r"names no 'gpkg' \(it names fwa_gpkg, coast_gpkg"):
            C.SOURCE.gpkg

    def test_get_path_refuses_a_named_source_file(self):
        from project_config import ProjectConfig
        with pytest.raises(KeyError, match=r"source\.fwa_gpkg is a named source file: read it as "
                                           r"`SOURCE\.fwa_gpkg`"):
            ProjectConfig().get_path("source", "fwa_gpkg")

    @pytest.mark.parametrize("old,new", [("fwa_data_gpkg", "SOURCE.fwa_gpkg"), ("fwa_coast_gpkg", "SOURCE.coast_gpkg"),
                                         ("fetch_output_gpkg_path", "SOURCE.fwa_gpkg"),
                                         ("fetch_temp_dir", "SOURCE.fetch_temp"),
                                         ("synopsis_pdf_path", "SOURCE.synopsis_pdf")])
    def test_the_retired_accessors_name_their_replacement(self, old, new):
        from project_config import get_config
        with pytest.raises(AttributeError, match=rf"ProjectConfig\.{old} was retired .* import SOURCE` and {new}\."):
            getattr(get_config(), old)


def _at(blob, keys):
    for k in keys:
        blob = blob[k]
    return blob


class TestOneRootPerTree:
    """(T1192) ONE `roots:` block; every other path value is a name under the root of its block (`curated.TREES`)."""

    def test_every_path_is_its_root_joined_with_its_name(self):
        blob, cfg = C.parse_config()
        roots = blob["roots"]
        leaves = [k for k, block in _keys(blob) if not block and k[0] in C.TREES]
        for keys in leaves:
            got = getattr(cfg, keys[0])
            for k in keys[1:]:
                got = getattr(got, k)
            assert got == C.ROOT / roots[keys[0]] / _at(blob, keys), keys
        assert len(leaves) == 78
        assert set(roots) == set(C.Roots.model_fields) == {*C.TREES, "release"}

    @pytest.mark.parametrize("at", [("curated", "waters", "cuts"), ("curated", "review", "marks"),
                                    ("generated", "reaches"), ("generated", "atlas", "builds"),
                                    ("cache", "dfo_salmon"), ("source", "fwa_gpkg")], ids=".".join)
    @pytest.mark.parametrize("bad", ["/abs/x", "../x", "a/../../x", "", "."])
    def test_a_name_that_leaves_its_root_is_refused_naming_it(self, at, bad):
        blob = _resolved()                         # a schema-side name (`At`) as much as config.yaml's own
        node = blob
        for k in at[:-1]:
            node = node[k]
        node[at[-1]] = bad
        with pytest.raises(C.ConfigError, match=rf"{re.escape('.'.join(at))}: Value error, a name under the "
                                                rf"{at[0]} root \(roots\.{at[0]}\)"):
            C._validate_config(blob, C.CONFIG)

    def test_the_roots_are_read_first_and_alone(self, tmp_path):
        p = _edit(tmp_path, ("roots",), lambda n, last: n.pop(last))
        with pytest.raises(C.ConfigError) as e:
            C.parse_config(p)
        assert str(e.value).splitlines() == [f"{p}: missing key roots"]       # never a line per path it would hold

    def test_the_curated_base_is_its_root_and_no_key(self, tmp_path):
        assert C.load_repo().base == C.load_roots().curated == C.ROOT / _real_blob()["roots"]["curated"]
        p = _edit(tmp_path, ("curated", "base"), lambda n, last: n.__setitem__(last, "anywhere"))
        with pytest.raises(C.ConfigError, match=r"unknown key curated\.base" + re.escape(DECLARE) + "$"):
            C.parse_config(p)

    def test_the_live_curated_root_has_no_second_door(self):
        # `roots.curated` is the live root even under CURATED_ROOT: ROOTS and get_path refuse it, naming CURATED
        from project_config import ProjectConfig
        with pytest.raises(AttributeError, match=r"ROOTS\.curated .* read CURATED\.base: it honours CURATED_ROOT"):
            C.ROOTS.curated
        for keys, door in ((("curated", "waters", "cuts"), "CURATED.waters.cuts"), (("curated",), "CURATED"),
                           (("roots", "curated"), "CURATED.base")):
            with pytest.raises(KeyError, match=rf"{re.escape('.'.join(keys))} is a curated path: read it as "
                                               rf"`{re.escape(door)}` .* it honours CURATED_ROOT"):
                ProjectConfig().get_path(*keys)

    def test_one_root_moves_its_whole_tree(self, tmp_path):
        # a tree moves by one line: every path of it follows, and no other tree's
        p = _edit(tmp_path, ("roots", "generated"), lambda n, last: n.__setitem__(last, str(tmp_path / "gen")))
        _blob, moved = C.parse_config(p)
        _blob, real = C.parse_config()
        assert moved.generated.reaches == tmp_path / "gen" / "reaches"
        assert moved.generated.gauges.match == tmp_path / "gen" / "gauges" / "match.json"
        assert (moved.curated, moved.source, moved.cache) == (real.curated, real.source, real.cache)

    def test_a_read_records_its_root(self, tmp_path, monkeypatch):
        from pipeline import _trace
        from project_config import ProjectConfig
        monkeypatch.setattr(_trace, "_config_seen", {})
        C.ROOTS.release
        assert set(_trace._config_seen) == {"roots.release"}
        monkeypatch.setattr(_trace, "_config_seen", {})
        tree = C._load.__wrapped__(None, None)              # (T1192) loading reads no value: each read records its own
        assert set(_trace._config_seen) == set()
        tree.waters.cuts
        assert set(_trace._config_seen) == {"curated.waters.cuts", "roots.curated"}
        monkeypatch.setattr(_trace, "_config_seen", {})
        gen = C.load_generated.__wrapped__()
        assert set(_trace._config_seen) == set()
        gen.tiles
        assert set(_trace._config_seen) == {"generated.tiles", "roots.generated"}
        monkeypatch.setattr(_trace, "_config_seen", {})
        ProjectConfig().get_path("cache", "dfo_salmon")
        assert set(_trace._config_seen) == {"cache.dfo_salmon", "roots.cache"}
        with pytest.raises(AttributeError, match=r"`roots:` names no 'base'"):
            C.ROOTS.base

    def test_a_curated_key_is_its_files_name_in_its_groups_folder(self):
        # the two legacy keys (`waters.areas` for area_closures.json, `waters.tidal` for tidal_limits.json) are renamed
        cur = _resolved()["curated"]
        wrong = [f"curated.{g}.{k} = {v}" for g, block in cur.items() for k, v in block.items()
                 if PurePosixPath(v).parent != PurePosixPath(g) or PurePosixPath(v).name.split(".")[0] != k]
        assert not wrong, wrong

    def test_the_files_that_may_be_absent_are_exactly_these(self):
        # a plain `Path` (no FilePath / DirectoryPath check) is the schema saying "the key must be stated, the file may be
        # absent"; every other curated path must exist
        loose = {f"{g}.{k}" for g, f in C.Curated.model_fields.items()
                 for k, sub in f.annotation.model_fields.items()
                 if sub.annotation is Path and not [m for m in sub.metadata if not isinstance(m, C.At)]}
        assert loose == {"review.marks", "review.closed", "review.owner_carries", "review.recheck",
                         "dfo_salmon.closed_areas", "bathymetry.matches", "bathymetry.contour_matches",
                         "stocking.matches"}

    def test_a_generated_folder_is_made_only_for_a_declared_key(self, tmp_path):
        p = _edit(tmp_path, ("roots", "generated"), lambda n, last: n.__setitem__(last, str(tmp_path / "gen")))
        gen = C.parse_config(p)[1].generated
        with pytest.raises(KeyError, match="has no folder 'nowhere'"):
            gen.mkdir("nowhere")
        with pytest.raises(KeyError, match="has no folder 'atlas'"):     # a sub-block, not a folder of its own
            gen.mkdir("atlas")
        made = gen.mkdir("scratch", "x")
        assert made == tmp_path / "gen" / "scratch" / "x" and made.is_dir()
        assert not (tmp_path / "gen" / "nowhere").exists()


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

    def test_the_generated_root_is_stated_once(self, tmp_path):
        # It was stated twice (`data_tree.generated` and `generated.base`), with a cross-check to catch the two
        # drifting apart. Now `roots.generated` is the one place: a second spelling is an unknown key.
        assert "base" not in C.Generated.model_fields and "release" not in C.Generated.model_fields
        for key in ("base", "release"):
            p = _edit(tmp_path, ("generated", key), lambda n, last: n.__setitem__(last, "x"))
            with pytest.raises(C.ConfigError, match=rf"unknown key generated\.{key}{re.escape(DECLARE)}$"):
                C.load_generated(p)

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
        base = C.load_roots().generated
        for p in (gen.atlas.builds, gen.reaches, gen.bundle, gen.tiles,
                  gen.added_streams, gen.scratch, gen.regs.parse,
                  gen.regs.extraction, gen.regs.dfo_salmon, gen.regs.entries_backup, gen.regs.export,
                  gen.gauges.feeds, gen.gauges.hydat_daily, gen.bathymetry, gen.stocking, gen.debug, gen.synopsis,
                  gen.steelhead, gen.curated_writes):
            assert base in p.parents, p
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


class TestEachReadRecordsItsLeaf:
    """(T1192 PR 5) A read of the `curated:` / `generated:` tree records the one value it reads and the root it is
    joined to (`curated._Tree`), never the block: a build stage is keyed only on the config keys it reads."""

    @staticmethod
    def seen(monkeypatch, read) -> set:
        from pipeline import _trace
        monkeypatch.setattr(_trace, "_config_seen", {})
        read()
        return set(_trace._config_seen)

    def test_a_leaf_records_its_key_and_root_and_a_group_records_nothing(self, monkeypatch):
        live, gen = C.load_repo(), C.load_generated()
        assert self.seen(monkeypatch, lambda: live.waters) == set()
        assert self.seen(monkeypatch, lambda: live.waters.cuts) == {"curated.waters.cuts", "roots.curated"}
        assert self.seen(monkeypatch, lambda: live.dfo_salmon.places) == {"curated.dfo_salmon.places", "roots.curated"}
        assert self.seen(monkeypatch, lambda: gen.regs.extraction) == {"generated.regs.extraction", "roots.generated"}
        assert self.seen(monkeypatch, lambda: getattr(live.waters, "no_such_key", None)) == set()

    def test_what_a_caller_gets_back_is_unchanged(self, monkeypatch):
        import pickle
        live, (blob, cfg) = C.load_repo(), C.parse_config()
        assert isinstance(live, C.Curated) and isinstance(live.waters, C.Waters) and type(live.waters.cuts) is type(
            cfg.curated.waters.cuts)
        assert live.waters.cuts == cfg.curated.waters.cuts and live == cfg.curated and hash(live) == hash(cfg.curated)
        assert live.model_dump() == cfg.curated.model_dump()
        again = pickle.loads(pickle.dumps(live))                   # a worker that gets the tree records its own reads
        assert again == live and self.seen(monkeypatch, lambda: again.gauges.matches) == {"curated.gauges.matches",
                                                                                         "roots.curated"}

    def test_the_record_is_per_access_never_per_load(self, monkeypatch):
        """The loaders are cached: a read after a cache hit (in whichever process reads) still records, and an earlier
        unrelated read in the same process adds nothing to a later one's record (AGENTS 19)."""
        C.load_repo().waters.cuts                                  # loaded and read once already
        assert self.seen(monkeypatch, lambda: C.load_repo().gauges.matches) == {"curated.gauges.matches",
                                                                                "roots.curated"}
        assert self.seen(monkeypatch, lambda: C.load_repo().waters.cuts) == {"curated.waters.cuts", "roots.curated"}

    def test_loading_and_validating_record_nothing(self, monkeypatch):
        """Validation stats every curated file (FilePath): that is no read by the stage, so it records nothing."""
        assert self.seen(monkeypatch, lambda: (C._load.__wrapped__(None, None), C.load_generated.__wrapped__())) == set()

    def test_taking_a_whole_node_records_the_whole_node(self, monkeypatch):
        live, gen = C.load_repo(), C.load_generated()
        assert self.seen(monkeypatch, live.model_dump) == {"curated", "roots.curated"}
        assert self.seen(monkeypatch, live.waters.model_dump_json) == {"curated.waters", "roots.curated"}
        assert self.seen(monkeypatch, lambda: dict(live.review)) == {"curated.review", "roots.curated"}
        assert self.seen(monkeypatch, gen.model_dump) == {"generated", "roots.generated"}

    def test_derived_reads_record_what_they_touch(self, monkeypatch, tmp_path):
        live, gen = C.load_repo(), C.load_generated()
        assert self.seen(monkeypatch, lambda: live.base) == {"roots.curated"}
        assert self.seen(monkeypatch, gen.build) == {"generated.atlas.builds", "atlas_build", "roots.generated"}
        assert self.seen(monkeypatch, lambda: gen.build("t1192-named")) == {"generated.atlas.builds", "roots.generated"}
        with pytest.raises(FileNotFoundError):
            gen.registry("t1192-no-such-build")
        assert self.seen(monkeypatch, lambda: pytest.raises(FileNotFoundError, gen.require_build, "t1192-none")) == {
            "generated.atlas.builds", "roots.generated"}
        assert self.seen(monkeypatch, lambda: pytest.raises(KeyError, gen.mkdir, "atlas")) == set()
        # `mkdir` on a declared key reads that key (on a tree bound to a temp root, so nothing is created here)
        blob, cfg = C.parse_config()
        moved = C._bind(cfg.generated.model_copy(update={"scratch": tmp_path / "scratch"}), "generated",
                        blob["generated"], ("roots.generated", blob["roots"]["generated"]))
        assert self.seen(monkeypatch, lambda: moved.mkdir("scratch", "x")) == {"generated.scratch", "roots.generated"}
        assert (tmp_path / "scratch" / "x").is_dir()
        assert self.seen(monkeypatch, lambda: C.SOURCE / "x.json") == {"roots.source"}

    def test_a_sandboxed_tree_records_its_leaf_not_the_live_root(self, monkeypatch, tmp_path):
        """Under `CURATED_ROOT` the root is the copy, no config value: a read records the name only (as `SOURCE.under`)."""
        root = C.make_root(tmp_path / "copy")
        tree = C._load.__wrapped__(None, root)
        assert tree.waters.cuts == root / _resolved()["curated"]["waters"]["cuts"]
        assert self.seen(monkeypatch, lambda: tree.waters.cuts) == {"curated.waters.cuts"}
        assert self.seen(monkeypatch, lambda: tree.base) == set()

    def test_another_config_file_records_nothing(self, monkeypatch, tmp_path):
        p = tmp_path / "config.yaml"
        p.write_bytes(C.CONFIG.read_bytes())
        assert self.seen(monkeypatch, lambda: C._load.__wrapped__(p, None).waters.cuts) == set()


# ======================================================================================
# every name the schema declares, frozen: a rename is made in two places, on purpose
# ======================================================================================

#: Every `At("…")` of the schema (`C.NAMES`, flattened), typed out. Taken from the names when they moved out of
#: config.yaml and checked then against that file (`git show <the commit before #358>:config.yaml`: all 77 equal, none
#: missing), so this is the file list as it was, not a copy of a typo. `At("side")` -> `At("sides")` repoints a folder
#: with every other test passing; here it fails until the same rename is made below.
FROZEN_NAMES = {
    "source.coast_gpkg": "fwa_coast.gpkg",
    "source.synopsis_pdf": "fishing_synopsis.pdf",
    "source.fetch_temp": "temp_bc_data",
    "curated.waters.cuts": "waters/cuts.json",
    "curated.waters.name_variants": "waters/name_variants.json",
    "curated.waters.found_names": "waters/found_names.json",
    "curated.waters.found_name_calls": "waters/found_name_calls.json",
    "curated.waters.continuations": "waters/continuations.json",
    "curated.waters.tidal_limits": "waters/tidal_limits.json",
    "curated.waters.shore_held": "waters/shore_held.json",
    "curated.waters.tidal_split_authority": "waters/tidal_split_authority.json",
    "curated.waters.slough_calls": "waters/slough_calls.json",
    "curated.waters.pond_calls": "waters/pond_calls.json",
    "curated.waters.slough_page_calls": "waters/slough_page_calls.json",
    "curated.waters.steelhead_waters": "waters/steelhead_waters.json",
    "curated.waters.area_closures": "waters/area_closures.json",
    "curated.waters.added_areas": "waters/added_areas.geojson",
    "curated.waters.added_lakes": "waters/added_lakes.geojson",
    "curated.waters.added_streams": "waters/added_streams.json",
    "curated.waters.added_stream_calls": "waters/added_stream_calls.json",
    "curated.waters.unmapped_waters": "waters/unmapped_waters.json",
    "curated.waters.unplaced_piece_calls": "waters/unplaced_piece_calls.json",
    "curated.regulations.synopsis": "regulations/synopsis",
    "curated.regulations.book_name_links": "regulations/book_name_links.json",
    "curated.regulations.reference": "regulations/reference",
    "curated.regulations.word_reads": "regulations/word_reads.json",
    "curated.regulations.under_closure_calls": "regulations/under_closure_calls.json",
    "curated.regulations.local_closures": "regulations/local_closures",
    "curated.review.marks": "review/marks.jsonl",
    "curated.review.proposal_overrides": "review/proposal_overrides.json",
    "curated.review.closed": "review/closed.json",
    "curated.review.owner_carries": "review/owner_carries.json",
    "curated.review.recheck": "review/recheck.json",
    "curated.gauges.matches": "gauges/matches.json",
    "curated.gauges.match_calls": "gauges/match_calls.json",
    "curated.gauges.tidal_stations": "gauges/tidal_stations.json",
    "curated.gauges.backwater_stations": "gauges/backwater_stations.json",
    "curated.gauges.diversion_stations": "gauges/diversion_stations.json",
    "curated.gauges.datum_change_stations": "gauges/datum_change_stations.json",
    "curated.runtiming.run_calls": "runtiming/run_calls.json",
    "curated.inseason.notice_waters": "inseason/notice_waters.json",
    "curated.dfo_salmon.places": "dfo_salmon/places",
    "curated.dfo_salmon.wording": "dfo_salmon/wording.json",
    "curated.dfo_salmon.tidal_calls": "dfo_salmon/tidal_calls.json",
    "curated.dfo_salmon.scope_places": "dfo_salmon/scope_places.json",
    "curated.dfo_salmon.closed_areas": "dfo_salmon/closed_areas.json",
    "curated.bathymetry.matches": "bathymetry/matches.json",
    "curated.bathymetry.contour_matches": "bathymetry/contour_matches.json",
    "curated.stocking.matches": "stocking/matches.json",
    "generated.atlas.builds": "atlas",
    "generated.reaches": "reaches",
    "generated.bundle": "bundle",
    "generated.tiles": "tiles",
    "generated.added_streams": "added_streams",
    "generated.scratch": "scratch",
    "generated.regs.extraction": "regs/extraction",
    "generated.regs.parse": "regs/parse",
    "generated.regs.dfo_salmon": "regs/dfo_salmon",
    "generated.regs.entries_backup": "regs/entries_backup",
    "generated.regs.export": "regs/ui-rules-export.json",
    "generated.gauges.feeds": "gauges/feeds",
    "generated.gauges.match": "gauges/match.json",
    "generated.gauges.candidates": "gauges/candidates.json",
    "generated.gauges.datum_proposals": "gauges/datum_proposals.json",
    "generated.gauges.hydat_daily": "gauges/hydat_daily",
    "generated.runtiming": "runtiming",
    "generated.inseason": "inseason",
    "generated.review": "review",
    "generated.sloughs": "sloughs",
    "generated.side": "side",
    "generated.bathymetry": "bathymetry",
    "generated.stocking": "stocking",
    "generated.debug": "debug",
    "generated.synopsis": "synopsis",
    "generated.steelhead": "steelhead",
    "generated.curated_writes": "curated_writes",
    "cache.dfo_salmon": "dfo_salmon",
}


def _flat(node: dict, at: tuple = ()) -> dict:
    out = {}
    for k, v in node.items():
        out |= _flat(v, (*at, k)) if isinstance(v, dict) else {".".join((*at, k)): v}
    return out


def test_every_schema_name_is_pinned():
    """A name added, removed or renamed in the schema (source, curated, generated, cache) fails until it is made
    here too: a file never moves by one keystroke in one place."""
    now = _flat(C.NAMES)
    assert {k.split(".")[0] for k in now} == {"source", "curated", "generated", "cache"}
    assert sorted(set(now) - set(FROZEN_NAMES)) == [], "a new name: add it to FROZEN_NAMES on purpose (and its file)"
    assert sorted(set(FROZEN_NAMES) - set(now)) == [], "a name gone: remove it from FROZEN_NAMES on purpose"
    assert {k: v for k, v in now.items() if FROZEN_NAMES[k] != v} == {}, (
        "a renamed file: rename it here too, on purpose, and move the data file too (the live one and the mini copy)")


@pytest.mark.parametrize("key, home", [
    ("source.fwa_gpkg", "config.yaml"), ("roots.curated", "config.yaml"), ("roots", "config.yaml"),
    ("atlas_build", "config.yaml"), ("*", "config.yaml"), ("source.coast_gpkg", "pipeline/common/curated.py"),
    ("curated.waters.cuts", "pipeline/common/curated.py"), ("curated.waters", "pipeline/common/curated.py"),
    ("generated", "pipeline/common/curated.py"), ("generated.atlas.default_build", "pipeline/common/curated.py"),
    ("curated.waters.t1192_gone", "pipeline/common/curated.py")])
def test_a_key_is_named_where_it_is_stated(key, home):
    """`declared_in` (the atlas-behind line's label): config.yaml only for the keys it states (the roots, the gpkg's
    name, `atlas_build`, the whole file); a schema name, a whole block of them and a key gone since are curated.py's."""
    assert C.declared_in(key) == home
