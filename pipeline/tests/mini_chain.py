"""THE MINI CHAIN, BUILT ONCE PER TEST RUN (T810 ticket L): the real pipeline over the invented world
(`pipeline.tools.mini_release.build`, the stages the committed mini release is cut by) into a temp folder, shared by every
test module that needs its bundle, export pair or answers — and the answers AS THE TROUT BUILDER READ THEM.

Anchor (a) of the test rules: data the tests build. No marker: the chain needs only the committed inputs
(`pipeline/tests/fixtures/mini/`).

WHY THE BUILDER'S INPUT IS BUILT HERE. Since the cutover (rows 34) the rows frames a release ships are cut over
(`answers.cut_over`): the rows the trout table covers carry no `group` and no `elsewhere`, and the table builder reads
those. A test that runs the builder (`trout.table.compose_key`) again therefore needs the rows from before the cut, which
no shipped file holds: `trout_inputs` builds the answers again from the chain's bundle and export pair (in a child
process, under the chain's environment) with the cutover pass off (`answers.A_CUT_OVER`, its mutation pin), every other
pass as built. Its trout section is the shipped one (the table is composed before the cut), so a test can hold the
two together.

    from pipeline.tests import mini_chain
    d = mini_chain.built(tmp_path_factory)            # the build folder: deliver/, release/, logs/
    m = mini_chain.trout_inputs(tmp_path_factory)     # answers.Model, rows before the cut
"""
from __future__ import annotations

from pathlib import Path

_CACHE: dict = {}


def built(tmp_path_factory) -> Path:
    """The mini chain's build folder (`deliver/` holds the bundle, the export pair, the answers and the card gate's stamp;
    `release/` the release), built on first use in this process."""
    if "dir" not in _CACHE:
        from pipeline.tools import mini_release as M
        root = tmp_path_factory.mktemp("mini-chain")
        M.build(root / "b")
        _CACHE["dir"] = root / "b"
    return _CACHE["dir"]


def deliver(tmp_path_factory) -> Path:
    return built(tmp_path_factory) / "deliver"


def release(tmp_path_factory) -> Path:
    from pipeline.deliver.release.manifest import latest
    return latest(built(tmp_path_factory) / "release")


#: the in-process build of the answers with the cutover pass off, run in a CHILD PROCESS under the mini chain's own
#: environment (`mini_release._env`: `CURATED_ROOT` at the mini tree, its claims' book), so nothing of this test process
#: (the curated root, the book's highlights, a module's cache) is changed or read by it; the Model comes back pickled
_PRE = """
import pickle, sys
from pathlib import Path
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.export.claims import load
d, claims, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
c = load(claims)
data, guide = C.load_export(d)
A.A_CUT_OVER = False
m = A.build(str(d / "bundle.sqlite"), d, workers=1, export=(data, guide), verdicts=str(d / "verdicts.sqlite"),
            book=(c.synopsis, c.row_boxes), log=lambda *_: None)
out.write_bytes(pickle.dumps(m))
"""


def trout_inputs(tmp_path_factory):
    """The chain's answers (`answers.Model`) built again with the cutover pass off (`answers.A_CUT_OVER`): every rows
    frame as the trout builder read it (covered rows with their `group` and `elsewhere`, mixed rows whole)."""
    if "pre" not in _CACHE:
        import pickle
        import subprocess
        import sys
        from pipeline.tools import mini_release as M
        b = built(tmp_path_factory)
        out = b / "pre-cutover.pickle"
        r = subprocess.run([sys.executable, "-c", _PRE, str(b / "deliver"), str(b / "fed" / "claims.json"), str(out)],
                           cwd=M.REPO_ROOT, env=M._env(), capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(f"mini_chain: the pre-cutover answers failed:\n{r.stderr[-3000:]}")
        _CACHE["pre"] = pickle.loads(out.read_bytes())
    return _CACHE["pre"]


def export(tmp_path_factory):
    """The chain's export pair (data, guide) and the trout builder's rule table over it."""
    if "export" not in _CACHE:
        from pipeline.deliver.answers import common as C
        from pipeline.deliver.answers.trout import table as T
        from pipeline.deliver.export.codec import expand
        data, guide = C.load_export(deliver(tmp_path_factory))
        _CACHE["export"] = (data, guide, T.Rules.from_export(data, expand(data, guide)))
    return _CACHE["export"]
