"""(T1193) THE TOOL READMES NAME EVERY RUNNABLE. A person finds a script in `scripts/README.md` and a tool in
`app/tools/README.md`; a new file those pages do not name fails here, so the lists cannot rot silently. Add it to the
README, in its group, with one line from its header."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
APP_TOOLS = ROOT / "app" / "tools"
AUDITS = ROOT / "pipeline" / "tools" / "audit"


def test_every_file_in_scripts_is_named_in_its_readme():
    readme = (SCRIPTS / "README.md").read_text(encoding="utf-8")
    files = sorted(p.relative_to(SCRIPTS).as_posix() for p in SCRIPTS.rglob("*")
                   if p.is_file() and p.name != "README.md" and "__pycache__" not in p.parts and p.suffix != ".pyc")
    missing = [f for f in files if f"`{f}`" not in readme and f"`{Path(f).name}`" not in readme]
    assert not missing, f"scripts/README.md does not name: {missing}; add each with one line"


def test_every_top_level_mjs_in_app_tools_is_named_in_its_readme():
    readme = (APP_TOOLS / "README.md").read_text(encoding="utf-8")
    missing = sorted(p.name for p in APP_TOOLS.glob("*.mjs") if f"`{p.name}`" not in readme)
    assert not missing, f"app/tools/README.md does not name: {missing}; add each, in its group, with one line"


def test_every_audit_module_is_named_in_the_tools_readme():
    readme = (ROOT / "pipeline" / "tools" / "README.md").read_text(encoding="utf-8")
    missing = sorted(p.stem for p in AUDITS.glob("*.py") if p.stem != "__init__" and f"`{p.stem}`" not in readme)
    assert not missing, f"pipeline/tools/README.md does not name these audits: {missing}; add each with one line"
