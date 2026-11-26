"""(T1135) THE RELEASE PINS, the pipeline half (the app half: `app/conformance/src/pins.ts`, same contract).

A value a test copies from a release (a sentence, a count) lives in ONE file, `pipeline/tests/fixtures/release-pins.json`,
re-cut with the fixtures by `node app/tools/regs-recut/sync.mjs` (its last step, `pins.mjs`). A test asks

    pin("answers26.williston.applies", g5["applies"])

- compare (the default): the value must equal the file's; a name the file does not hold FAILS (never a pass);
- record (`PIN_RECORD=<dir>`, set by the re-cut alone): the value is written to `<dir>/pipeline/<name>.json` and nothing
  is asserted. The re-cut writes the file from the records and drops (and names) a pin no test asked for.

Each pin is `{"answers": bool, "value": ...}`: `answers: true` is words or pages an angler reads (a moved one stops the
release for a look, like a golden page: `release_follows.moved_answer_pins`); `false` is a count or an id (committed
with the release). Budgets and limits are never pins: they stay typed in their tests.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

PINS_FILE = Path(__file__).with_name("fixtures") / "release-pins.json"
RECUT = "node app/tools/regs-recut/sync.mjs"
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
SUITE = "pipeline"


def _rel(p: Path) -> str:
    try:
        return str(Path(p).resolve().relative_to(Path(REPO_ROOT).resolve()))
    except ValueError:
        return str(p)


def read(file: Path) -> dict:
    """{name: {"answers", "value"}} of a pins file; raises naming the file when it is not one."""
    doc = json.loads(Path(file).read_text(encoding="utf-8"))
    pins = doc.get("pins") if isinstance(doc, dict) else None
    if not isinstance(pins, dict):
        raise ValueError(f"{_rel(file)}: no \"pins\" object")
    return pins


def _json(name: str, actual):
    """`actual` as the file can hold it (tuples become lists); refuses what JSON cannot hold."""
    try:
        return json.loads(json.dumps(actual, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as e:
        raise AssertionError(f"pin {name}: {actual!r} is not a JSON value ({e})") from None


def make_pin(file: Path, record: str | None):
    """A `pin(name, actual)` over `file`, recording into `record` when it is set (the env `PIN_RECORD`)."""
    cache: dict = {}

    def pin(name: str, actual) -> None:
        if not NAME.fullmatch(name):
            raise AssertionError(f"pin {name!r}: a pin name is letters, digits, '.', '_' or '-'")
        value = _json(name, actual)
        if record:
            d = Path(record) / SUITE
            d.mkdir(parents=True, exist_ok=True)
            out = d / f"{name}.json"
            body = json.dumps({"name": name, "value": value}, ensure_ascii=False, sort_keys=True) + "\n"
            if out.is_file() and out.read_text(encoding="utf-8") != body:
                raise AssertionError(f"pin {name}: recorded twice with different values ({out})")
            out.write_text(body, encoding="utf-8")
            return
        if "pins" not in cache:
            cache["pins"] = read(file)
        pins = cache["pins"]
        if name not in pins:
            raise AssertionError(f"pin {name}: not in {_rel(file)} — add its row by hand with its answers flag "
                                 f"({{\"answers\": true|false, \"value\": ...}}); the re-cut keeps it: {RECUT}")
        want = pins[name].get("value")
        if value != want:
            raise AssertionError(f"pin {name} ({_rel(file)}): the release says {value!r}, the pins file {want!r}. If "
                                 f"the release is right, re-cut ({RECUT}); a release re-cuts it itself")
    return pin


pin = make_pin(PINS_FILE, os.environ.get("PIN_RECORD") or None)
