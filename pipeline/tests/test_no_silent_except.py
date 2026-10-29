"""T61 2.13: no SILENT `except Exception` in pipeline/ — a handler whose whole body is `pass`,
`continue` or a bare/None `return` swallows every bug. Each one left says why on its own line
(`# noqa: BLE001 — <reason>`); a new silent one fails here."""
import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _silent(h: ast.ExceptHandler) -> bool:
    if not (isinstance(h.type, ast.Name) and h.type.id in ("Exception", "BaseException")) and h.type is not None:
        return False
    if len(h.body) != 1:
        return False
    b = h.body[0]
    return isinstance(b, (ast.Pass, ast.Continue)) or (
        isinstance(b, ast.Return) and (b.value is None or (isinstance(b.value, ast.Constant) and b.value.value is None)))


def silent_handlers(src: str) -> list[int]:
    lines = src.splitlines()
    return [h.lineno for h in ast.walk(ast.parse(src)) if isinstance(h, ast.ExceptHandler)
            and _silent(h) and "noqa: BLE001" not in lines[h.lineno - 1]]


def test_every_silent_broad_except_says_why():
    bad = []
    for p in sorted((REPO / "pipeline").rglob("*.py")):
        if "tests" in p.parts:
            continue
        bad += [f"{p.relative_to(REPO)}:{n}" for n in silent_handlers(p.read_text(encoding="utf-8"))]
    assert bad == []


def test_the_scan_bites():
    """Mutation pin: each silent shape is caught; a reason or a narrow type is not."""
    for body in ("pass", "continue", "return", "return None"):
        src = f"for x in y:\n    try:\n        f()\n    except Exception:\n        {body}\n"
        assert silent_handlers(src) == [4], body
    assert silent_handlers("try:\n    f()\nexcept:\n    pass\n") == [3]
    assert silent_handlers("try:\n    f()\nexcept Exception:  # noqa: BLE001 — why\n    pass\n") == []
    assert silent_handlers("try:\n    f()\nexcept OSError:\n    pass\n") == []
    assert silent_handlers("try:\n    f()\nexcept Exception:\n    log(); return None\n") == []
