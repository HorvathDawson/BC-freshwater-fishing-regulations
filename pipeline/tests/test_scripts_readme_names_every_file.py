"""(T1193) scripts/README.md NAMES EVERY FILE IN scripts/. A person finds a script there; a new file the page does not name
fails here, so the list cannot rot silently. Add it to the README, with one line."""
from __future__ import annotations

from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def test_every_file_in_scripts_is_named_in_its_readme():
    readme = (SCRIPTS / "README.md").read_text(encoding="utf-8")
    files = sorted(p.relative_to(SCRIPTS).as_posix() for p in SCRIPTS.rglob("*")
                   if p.is_file() and p.name != "README.md" and "__pycache__" not in p.parts and p.suffix != ".pyc")
    missing = [f for f in files if f not in readme and Path(f).name not in readme]
    assert not missing, f"scripts/README.md does not name: {missing}; add each with one line"
