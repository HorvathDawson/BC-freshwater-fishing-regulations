"""T600: ONE WRITER FOR CURATED DATA (`pipeline.common.curated_write`).

The writer's contract on a sandboxed tree (`CURATED_ROOT`, never the live one): refuse outside the tree, validate,
back up, write atomically, log — and nothing at all for an unchanged file or a refused write. Then the GUARD: every
production module that reads the curated tree (or the old atomic helper) AND writes files is classified here with
what it writes; a new one fails until it is, and a classified one that stopped writing makes the list stale.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from pipeline.common import curated_write as W
from pipeline.tests.conftest import curated_root

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def tree(tmp_path, monkeypatch):
    """A whole curated tree copy with `CURATED_ROOT` on it (the one sanctioned way to move curated paths)."""
    return curated_root(monkeypatch, tmp_path / "curated")


def _log(root: Path) -> list[dict]:
    p = root.parent / f"{root.name}.writes" / "log.jsonl"
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.exists() else []


def test_a_write_is_validated_backed_up_atomic_and_logged(tree):
    p = tree / "regulations" / "word_reads.json"
    old = p.read_bytes()
    doc = json.loads(old)
    new = json.dumps({**doc}, indent=2) + "\n"
    rec = W.write(p, new, tool="test", by="tester", source="unit")
    assert p.read_text(encoding="utf-8") == new
    assert rec["file"] == "regulations/word_reads.json" and rec["by"] == "tester" and rec["tool"] == "test"
    backup = tree.parent / f"{tree.name}.writes" / rec["backup"]
    assert backup.read_bytes() == old
    assert _log(tree) == [rec]
    # the same bytes again: nothing written, backed up or logged
    assert W.write(p, new, tool="test") is None and len(_log(tree)) == 1


def test_a_bad_write_changes_nothing(tree):
    p = tree / "regulations" / "word_reads.json"
    before = p.read_bytes()
    with pytest.raises(W.CuratedWriteRefused, match="not valid JSON"):
        W.write(p, "{not json", tool="test")
    doc = json.loads(before)
    if isinstance(doc, dict) and "_about" in doc:
        with pytest.raises(W.CuratedWriteRefused, match="_about"):
            W.write(p, json.dumps({k: v for k, v in doc.items() if k != "_about"}), tool="test")
    with pytest.raises(W.CuratedWriteRefused, match="schema says no"):
        W.write(p, before.decode() + " ", tool="test", validate=lambda t: (_ for _ in ()).throw(ValueError("schema says no")))
    assert p.read_bytes() == before and _log(tree) == []


def test_a_waters_file_must_pass_check_curated(tree):
    p = tree / "waters" / "name_variants.json"
    before = p.read_bytes()
    with pytest.raises(W.CuratedWriteRefused, match="check_curated --waters"):
        W.write(p, json.dumps({"not": "a list"}), tool="test")
    assert p.read_bytes() == before


def test_outside_the_tree_is_refused_and_write_any_is_plain(tree, tmp_path):
    out = tmp_path / "scratch" / "x.json"
    with pytest.raises(W.CuratedWriteRefused, match="not under the curated tree"):
        W.write(out, "{}", tool="test")
    assert not out.exists()
    assert W.write_any(out, "{}\n", tool="test") is None and out.read_text() == "{}\n" and _log(tree) == []


def test_backups_keep_the_last_n(tree, monkeypatch):
    monkeypatch.setattr(W, "BACKUPS_KEPT", 3)
    p = tree / "regulations" / "word_reads.json"
    doc = json.loads(p.read_text(encoding="utf-8"))
    for i in range(5):
        W.write(p, json.dumps(doc, indent=i + 2) + "\n", tool="test")
    d = tree.parent / f"{tree.name}.writes" / "backups" / "regulations" / "word_reads.json"
    assert len(list(d.iterdir())) == 3


def test_parsing_atomic_write_is_the_one_copy():
    """`parsing.io.atomic_write` delegates (no temp+replace of its own)."""
    tree = ast.parse((ROOT / "pipeline/regs/parsing/io.py").read_text(encoding="utf-8"))
    calls = {f"{n.func.value.id}.{n.func.attr}" for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)}
    assert not calls & {"tempfile.mkstemp", "os.replace", "os.rename"}


# ------------------------------------------------------------------------------------------------ the guard

#: Every production module that reads the curated tree (or names `atomic_write`) and writes files, with WHAT it
#: writes. "curated via the writer": its curated writes go through `curated_write`; the raw writes the scan sees are
#: generated / scratch / release files. HANDED OVER: the review app's own write path (T600 item 2: chat 10's paths,
#: listed, not edited) — the marks store and the in-season matches, through `curation-review/backend/writes.py`.
CLASSIFIED = {
    "pipeline/common/curated_write.py": "THE writer",
    "pipeline/common/curated.py": "make_root: copies the live tree to a sandbox root (never into the live tree)",
    # --- curated via the writer (T600) + generated side files (gauges.generate.promote, reach.word_reads, the
    # dfo_salmon entries / wording, steelhead.accept, inseason.resolve, bathymetry.accept, gauges.review, review.apply /
    # apply_proposals / closed and parsing.io.write_entryfile left the scan when their raw curated write went)
    "pipeline/atlas/waters/added_streams/build_dataset.py": "curated via the writer; candidates + report to generated",
    # --- handed over (the review app's write path)
    "pipeline/regs/review/marks.py": "HANDED OVER: marks.append, the marks store (atomic_write under the store lock)",
    "curation-review/backend/writes.py": "HANDED OVER: writes.commit, marks + in-season matches, its own backup/rollback",
    "curation-review/backend/inputs_sync.py": "its own log under generated/review (open 'a')",
    # --- build stages: write their build / run / bundle / tiles / release dirs only (read-only tree: test_atlas_inputs)
    "pipeline/build.py": "the build manifest and stage logs (generated)",
    "pipeline/release.py": "the release candidate, ledger and LATEST (data/release, generated)",
    "pipeline/atlas/build.py": "the atlas build dir",
    "pipeline/atlas/inputs.py": "graph.pkl.meta.json input_hashes in the build dir",
    "pipeline/atlas/sidecars.py": "the build dir's reports",
    "pipeline/atlas/splits/coast.py": "the build dir's tidal record",
    "pipeline/atlas/splits/tidal.py": "the build dir's resolved tidal rows",
    "pipeline/atlas/reach/dfo_salmon.py": "the reach run dir",
    "pipeline/deliver/__main__.py": "the deliver out dir and release staging",
    "pipeline/deliver/bundle/build.py": "continuations_held.json beside the bundle",
    "pipeline/deliver/sync_inputs.py": "copies curated inputs INTO a staging dir (reads curated)",
    # --- tools whose outputs are generated (review inputs, feeds, proposals, worklists)
    "pipeline/bathymetry/contours.py": "generated bathymetry contours",
    "pipeline/bathymetry/propose.py": "generated bathymetry proposals",
    "pipeline/gauges/matches.py": "write_match refuses a curated path (refuse_curated); a build-dir stamp",
    "pipeline/regs/dfo_salmon/match.py": "a worklist at --out (else stdout); curated places via entries.save (the writer)",
    "pipeline/regs/dfo_salmon/preverify.py": "generated review inputs",
    "pipeline/regs/dfo_salmon/suggest.py": "generated review inputs",
    "pipeline/regs/parsing/batch_exporter.py": "the parse work dir (generated)",
    "pipeline/regs/parsing/ingest_catalogue.py": "its ledger in the parse work dir; curated via write_entryfile (the writer)",
    "pipeline/regs/review/precheck.py": "json to stdout",
    "pipeline/regs/review/proposals.py": "side copies outside the curated tree (write_side refuses curated) + the proposals dir",
    "pipeline/regs/review/split_suggest.py": "generated review input",
    "pipeline/regs/review/tickets.py": "ticket files at --out / stdout",
    "pipeline/runtiming/parse.py": "data/source/runtiming/timing.json (fetched-and-built, T602)",
    "pipeline/stocking/propose.py": "generated stocking proposals",
    "pipeline/tools/audit/tidal_candidates.py": "generated review/tidal",
    "pipeline/tools/audit/tidal_pass2.py": "generated review/tidal",
    "pipeline/tools/audit/tidal_words.py": "generated review/tidal",
    "pipeline/tools/audit/unnamed_trib_sweep.py": "the committed sweep fixture under pipeline/tests (T57)",
}


def raw_writes(tree: ast.AST) -> set[str]:
    """The file-writing calls a module makes, by kind."""
    out: set[str] = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and n.attr == "atomic_write" and not isinstance(n.ctx, ast.Store):
            out.add("atomic_write")
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        if isinstance(f, ast.Name) and f.id == "atomic_write":
            out.add("atomic_write")
        if isinstance(f, ast.Attribute):
            if f.attr in ("write_text", "write_bytes"):
                out.add(f.attr)
            base = f.value.id if isinstance(f.value, ast.Name) else None
            if base == "json" and f.attr == "dump":
                out.add("json.dump")
            if base == "os" and f.attr in ("replace", "rename"):
                out.add(f"os.{f.attr}")
            if base == "shutil" and f.attr in ("copy", "copy2", "copyfile", "move", "copytree"):
                out.add(f"shutil.{f.attr}")
        if (isinstance(f, ast.Name) and f.id == "open") or (isinstance(f, ast.Attribute) and f.attr in ("open", "fdopen")):
            mode = None
            if len(n.args) >= 2 and isinstance(n.args[1], ast.Constant):
                mode = n.args[1].value
            if isinstance(f, ast.Attribute) and f.attr == "open" and n.args and isinstance(n.args[0], ast.Constant):
                mode = n.args[0].value
            for k in n.keywords:
                if k.arg == "mode" and isinstance(k.value, ast.Constant):
                    mode = k.value.value
            if isinstance(mode, str) and any(c in mode for c in "wax+"):
                out.add(f"open({mode})")
    return out


def reads_curated(tree: ast.AST) -> bool:
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and n.id in ("CURATED", "CURATED_ROOT"):
            return True
        if isinstance(n, ast.Attribute) and n.attr in ("curated", "CURATED"):
            return True
    return False


def in_scope(src: str) -> bool:
    t = ast.parse(src)
    w = raw_writes(t)
    return bool(w) and (reads_curated(t) or "atomic_write" in w)


def _scope() -> set[str]:
    out = set()
    for top in ("pipeline", "curation-review/backend"):
        for p in sorted((ROOT / top).rglob("*.py")):
            rel = p.relative_to(ROOT).as_posix()
            if "/tests/" in rel or "node_modules" in rel:
                continue
            if in_scope(p.read_text(encoding="utf-8")):
                out.add(rel)
    return out


def test_every_module_that_reads_curated_and_writes_is_classified():
    got = _scope()
    assert sorted(got - set(CLASSIFIED)) == [], "a module reads the curated tree and writes files: route its " \
        "curated writes through pipeline.common.curated_write and classify it here with what it writes"
    assert sorted(set(CLASSIFIED) - got) == [], "classified but no longer in scope: drop it from CLASSIFIED"


def test_mutation_a_raw_write_beside_a_curated_read_is_caught():
    assert in_scope("from pipeline.common.curated import CURATED\nCURATED.waters.cuts.write_text('[]')\n")
    assert in_scope("from pipeline.regs.parsing.io import atomic_write\natomic_write(p, t)\n")
    assert not in_scope("from pipeline.common.curated import CURATED\nprint(CURATED.waters.cuts.read_text())\n")
    assert not in_scope("from pipeline.common import curated_write\ncurated_write.write(p, t, tool='x')\n")
