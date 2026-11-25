"""The one list of store pictures both push scripts upload (T1127).

app/tools/store-shots.json is the source; app/tools/store-shots.mjs writes `<target>/<NN>-<scene>.png`, where NN is the
1-based place in the target's list (the shots without an `only`, or whose `only` names the target), two digits.
This module mirrors that rule. A size folder is checked against it: a picture file that is not on the list, or a listed
one that is missing, is refused by name (never skipped, never globbed in).
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SHOTS_JSON = ROOT / "app/tools/store-shots.json"
PICTURE_SUFFIXES = (".png", ".jpg", ".jpeg")


def expected(target: str) -> list[str]:
    """The file names store-shots.mjs writes into the target's folder, in order."""
    src = json.loads(SHOTS_JSON.read_text(encoding="utf-8"))
    if target not in src["targets"]:
        raise ValueError(f"store-shots.json has no target {target!r} (has: {', '.join(src['targets'])})")
    shots = [s for s in src["shots"] if not s.get("only") or target in s["only"]]
    return [f"{i:02d}-{s['scene']}.png" for i, s in enumerate(shots, 1)]


def pictures(folder: Path, target: str) -> list[Path]:
    """The folder's pictures, in file-name order, once they are exactly the target's list; otherwise ValueError naming
    every stray and every missing file."""
    want = expected(target)
    if not folder.is_dir():
        raise ValueError(f"{folder}: no such folder; it must hold {len(want)} pictures for {target}: {', '.join(want)}")
    have = sorted(p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in PICTURE_SUFFIXES)
    stray = [p.name for p in have if p.name not in want]
    missing = [n for n in want if n not in {p.name for p in have}]
    if stray or missing:
        parts = []
        if stray:
            parts.append(f"not in app/tools/store-shots.json's list for {target}: {', '.join(stray)}")
        if missing:
            parts.append(f"missing: {', '.join(missing)}")
        raise ValueError(f"{folder}: {'; '.join(parts)} (clear old pictures or rerun store-shots.mjs; nothing is skipped)")
    return have
