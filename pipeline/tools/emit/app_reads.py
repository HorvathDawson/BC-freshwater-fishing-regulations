"""WHAT EACH APP VERSION READS (T555 item 4): the table the release's app-version gate is derived from.

    python -m pipeline.tools.emit.app_reads            # record the CURRENT app version's row (app.json's version)
    python -m pipeline.tools.emit.app_reads --check    # fail when that row is missing or is not what the code reads
    python -m pipeline.tools.emit.app_reads --backfill # rows for the versions before the table existed, proved by git

WHY. The gate knew two hand tables (`PAINT_NEEDS`, `SALMON_NEEDS`: "add a row whenever paint.py's FORMAT moves"), and
every other format bump relied on someone remembering the release's `minAppVersion` (water/3 and geo/2 went out stamped
0.7.0 by hand). Now the table is every file of the release contract, per app version, and it is kept by a check:

    pipeline/deliver/release/app_reads.json   {"apps": {"<version>": {"reads": {"<file>": ["<format>", …]}, …}}}

A row is what the app's `readsRelease` accepts: the contract's own format of each file (`contract.FILES`, the source of
the app's `pack.generated.ts`) and the older formats a reader still decodes (the app's `ALSO_READS`: paint's
`PAINT_READS`). `pipeline.deliver.release.app_gate` reads the table and derives the oldest app that reads ALL of a
manifest's formats; a stamp below it is refused.

A RECORDED ROW IS NEVER CHANGED QUIETLY. `--check` runs in `pnpm check` (so in fast-fail, before a release or an app
build): once a version is recorded, moving a format without moving the version fails — an app already built as that
version cannot read the new format, and the gate would let it try. The fix is to bump the version in
`app/apps/mobile/app.json` and record again. Only when no build of that version has left this machine:
`--replace "<why>"` rewrites the row and prints the reason.

`--backfill` (run once, 2026-11-11): for every version `app.json` has carried, the contract at the commit that set the
version (`git show <commit>:…/pack.generated.ts`, `…/paint.ts`). A version's builds are made after its bump, so its
bump commit is the oldest code a build of it can hold: a format that arrived later in that version's time is credited
to the NEXT version (paint/5 → 0.3.0, salmon/4 → 0.5.0, water/3 and geo/2 → 0.7.0, as the hand tables and the stamps
had it). Rows it writes say `"backfilled": true`.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

TABLE = Path(__file__).resolve().parents[2] / "deliver" / "release" / "app_reads.json"
APP_JSON = "app/apps/mobile/app.json"
PACK_TS = "app/packages/data/src/pack.generated.ts"
PAINT_TS = "app/packages/core/src/paint.ts"
MANIFEST_TS = "app/packages/data/src/manifest.ts"
#: every ALSO_READS entry this reads: {file: (the constant, the file that declares it as a literal list)}. (T975) paint
#: only: T686's near / down / climate ALSO_READS went with the old formats.
_ALSO_SOURCES = {"paint": ("PAINT_READS", PAINT_TS)}
_BY = "python -m pipeline.tools.emit.app_reads"
ABOUT = ("What each app version reads: per release file, the formats its reader accepts (T555 item 4). Written by "
         f"`{_BY}` (the current version; `--backfill` proved the older rows by git), kept by its `--check` in "
         "`pnpm check`. Read by pipeline/deliver/release/app_gate.py and site_gate.py. Never edit by hand; never "
         "change a recorded row without bumping the app version (see the module).")

_PAINT_READS = re.compile(r"export const PAINT_READS: readonly string\[\] = (\[[^\]]*\]);")
_ALSO_READS = re.compile(r"const ALSO_READS: Readonly<Record<string, readonly string\[\]>> = \{([^}]*)\};")
_PACK_FILES = "export const PACK_FILES: Record<string, PackFile> = "


def ver(s: str) -> tuple:
    return tuple(int(x) for x in s.split("-")[0].split("+")[0].split("."))


def _paint_reads(paint_ts: str) -> list[str] | None:
    m = _PAINT_READS.search(paint_ts)
    return list(json.loads(m.group(1))) if m else None


def _const_reads(src: str, const: str) -> list[str] | None:
    """`export const <const>: readonly string[] = [...]` → the list (None: not declared that way)."""
    m = re.search(rf"export const {const}: readonly string\[\] = (\[[^\]]*\]);", src)
    return list(json.loads(m.group(1))) if m else None


def reads_of(pack_files: dict, also: dict[str, list[str]]) -> dict[str, list[str]]:
    """{file: [the contract's format, then every older one its reader still takes]} — the app's `readsRelease` rule."""
    out = {}
    for name in sorted(pack_files):
        own = pack_files[name]["format"]
        out[name] = [own] + sorted(f for f in also.get(name, ()) if f != own)
    return out


def current_reads(root: Path = REPO_ROOT) -> dict[str, list[str]]:
    """What the app in this tree reads: the contract (Python, the source of pack.generated.ts) + the app's ALSO_READS."""
    from pipeline.deliver.release import contract as C
    return reads_of(C.FILES, _also_reads(lambda rel: (root / rel).read_text(encoding="utf-8")))


def reads_in(show) -> dict[str, list[str]]:
    """(T1048) what the app in ANOTHER tree reads (`show(<repo path>)` -> its text, e.g. a commit's): the contract as
    that tree's app compiles it (pack.generated.ts, held to contract.py by `emit.pack --check`) + its ALSO_READS. A
    file that is not there raises, naming it (never an empty row)."""
    def read(rel: str) -> str:
        text = show(rel)
        if text is None:
            raise SystemExit(f"app_reads: the tree has no {rel}: what its app reads cannot be told")
        return text
    pack = read(PACK_TS)
    if _PACK_FILES not in pack:
        raise SystemExit(f"app_reads: {PACK_TS} no longer declares `{_PACK_FILES.strip()}`: teach "
                         "pipeline/tools/emit/app_reads.py its new shape")
    files, _ = json.JSONDecoder().raw_decode(pack[pack.index(_PACK_FILES) + len(_PACK_FILES):])
    return reads_of(files, _also_reads(read))


def _also_reads(read) -> dict[str, list[str]]:
    """The app's ALSO_READS (`read(<repo path>)` -> text): {file: the older formats its reader still takes}."""
    also_src = _ALSO_READS.search(read(MANIFEST_TS))
    if not also_src:
        raise SystemExit(f"app_reads: {MANIFEST_TS} no longer declares ALSO_READS the way this reads it: teach "
                         "pipeline/tools/emit/app_reads.py its new shape (an older format a reader takes must be in the table)")
    also: dict[str, list[str]] = {}
    for part in (p.strip() for p in also_src.group(1).split(",") if p.strip()):
        name, _, value = (x.strip() for x in part.partition(":"))
        if _ALSO_SOURCES.get(name, (None,))[0] != value:
            raise SystemExit(f"app_reads: ALSO_READS has an entry this does not know ({part!r}): teach "
                             "pipeline/tools/emit/app_reads.py where its formats are listed (`_ALSO_SOURCES`)")
        const, where = _ALSO_SOURCES[name]
        got = _const_reads(read(where), const)
        if got is None:
            raise SystemExit(f"app_reads: {where} no longer declares {const} as a literal list")
        also[name] = got
    return also


def app_version(root: Path = REPO_ROOT) -> str:
    return json.loads((root / APP_JSON).read_text(encoding="utf-8"))["expo"]["version"]


def load(path: Path = TABLE) -> dict:
    """The committed table. Missing is a bug, never an empty table (the gate would pass everything)."""
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing: `{_BY} --backfill` then `{_BY}` write it")
    return json.loads(path.read_text(encoding="utf-8"))


def dump(doc: dict, about: str = ABOUT) -> str:
    # one line per version, oldest first: a recorded row that changes is one changed line in the diff. `about`: the
    # cut (T1048) keeps its base's own _about, so its version commit adds one line and changes none
    rows = [f"  {json.dumps(v)}: {json.dumps(doc['apps'][v], ensure_ascii=False, sort_keys=True)}"
            for v in sorted(doc["apps"], key=ver)]
    return '{\n "_about": ' + json.dumps(about, ensure_ascii=False) + ',\n "apps": {\n' + ",\n".join(rows) + "\n }\n}\n"


def check_problems(doc: dict, version: str, reads: dict[str, list[str]]) -> list[str]:
    """[] when the current version's row is recorded and is what the code reads."""
    row = doc["apps"].get(version)
    if row is None:
        return [f"app {version} has no row in {TABLE.relative_to(REPO_ROOT)}: run `{_BY}` and commit the file"]
    if row["reads"] == reads:
        return []
    moved = sorted(n for n in set(row["reads"]) | set(reads) if row["reads"].get(n) != reads.get(n))
    said = "; ".join(f"{n}: recorded {row['reads'].get(n) or 'nothing'}, the code reads {reads.get(n) or 'nothing'}"
                     for n in moved)
    return [f"app {version} is recorded reading other formats than the code now reads ({said}). An app already built "
            f"as {version} cannot read the new ones, and the release gate would let it try: bump the version in "
            f"{APP_JSON}, then `{_BY}`. Only if no build of {version} has left this machine: "
            f"`{_BY} --replace \"<why>\"`"]


def record(doc: dict, version: str, reads: dict[str, list[str]], replace: str | None = None) -> tuple[dict, str]:
    """(the table with the current version's row, what happened). Raises SystemExit on a row that would change."""
    row = doc["apps"].get(version)
    if row is not None and row["reads"] == reads:
        return doc, f"app {version}: already recorded, unchanged"
    if row is not None and not replace:
        raise SystemExit("app_reads: refused — " + check_problems(doc, version, reads)[0])
    apps = {**doc["apps"], version: {"reads": reads}}
    what = (f"app {version}: row REPLACED ({replace})" if row is not None else f"app {version}: recorded")
    return {**doc, "apps": apps}, what


def _moved(old: dict[str, list[str]], new: dict[str, list[str]]) -> str:
    """'' when the two rows read the same; else each file that differs, old -> new."""
    return "; ".join(f"{n}: {old.get(n) or 'nothing'} -> {new.get(n) or 'nothing'}"
                     for n in sorted(set(old) | set(new)) if old.get(n) != new.get(n))


def for_cut(doc: dict, version: str, reads: dict[str, list[str]], reads_moved: str | None = None) -> tuple[dict, str]:
    """(T1048) THE ROW THE CUT'S VERSION COMMIT ADDS: (the table with `version`'s row, what was decided). `reads` is what
    the app on the cut's base reads (`reads_in`). The row is the same formats as the newest version recorded before
    `version`; when the app's reads MOVED since it, the cut stops and says which files, unless `reads_moved` says why
    the move is meant (`release --cut --reads-moved "<why>"`, the owner's call). A row already recorded for `version`
    stays when it is what the app reads and is refused when it is not (a recorded row never changes quietly)."""
    row = doc["apps"].get(version)
    if row is not None:
        if row["reads"] == reads:
            return doc, f"app {version}: already recorded, the formats the app reads"
        raise CutRefused(f"app {version} is recorded already reading other formats than the app reads "
                         f"({_moved(row['reads'], reads)}): fix the table on {version}'s base before the cut")
    before = [v for v in doc["apps"] if ver(v) < ver(version)]
    if not before:
        raise CutRefused(f"no app version before {version} is recorded: `{_BY} --backfill` writes the table")
    prev = max(before, key=ver)
    moved = _moved(doc["apps"][prev]["reads"], reads)
    if moved and not reads_moved:
        raise CutRefused(f"what the app reads moved since {prev} ({moved}): an app built as {version} reads other "
                         f"formats than {prev}. If that is meant: python -m pipeline release --cut {version} "
                         f"--reads-moved \"<why>\"; if not, land the fix before the cut. Nothing was made")
    what = (f"app {version}: the same formats as {prev}" if not moved
            else f"app {version}: the formats moved since {prev} ({moved}): {reads_moved}")
    return {**doc, "apps": {**doc["apps"], version: {"reads": reads}}}, what


class CutRefused(RuntimeError):
    """(T1048) the cut cannot write the new version's app_reads row; the message says why and what to do."""


TABLE_REL = TABLE.relative_to(REPO_ROOT).as_posix()


def _git(root: Path, *args: str) -> str | None:
    r = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def backfill(root: Path = REPO_ROOT) -> dict[str, dict]:
    """{version: row} for every version app.json has carried, from the contract at the commit that set it."""
    rows: dict[str, dict] = {}
    commits = (_git(root, "log", "--reverse", "--format=%H", "--", APP_JSON) or "").split()
    for c in commits:
        src = _git(root, "show", f"{c}:{APP_JSON}")
        try:
            version = json.loads(src)["expo"]["version"] if src else None
        except (ValueError, KeyError):
            version = None
        if not version or version in rows:
            continue
        pack = _git(root, "show", f"{c}:{PACK_TS}")
        if not pack or _PACK_FILES not in pack:
            continue                                   # before the release contract was emitted: nothing to prove
        files, _ = json.JSONDecoder().raw_decode(pack[pack.index(_PACK_FILES) + len(_PACK_FILES):])
        paint = _paint_reads(_git(root, "show", f"{c}:{PAINT_TS}") or "")
        rows[version] = {"reads": reads_of(files, {"paint": paint} if paint else {}), "backfilled": True, "commit": c[:9]}
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--replace", metavar="WHY", help="rewrite the current version's recorded row (no build of it has shipped)")
    a = ap.parse_args(argv)
    version, reads = app_version(), current_reads()
    if a.check:
        bad = check_problems(load(), version, reads)
        for p in bad:
            print(f"app_reads: {p}", file=sys.stderr)
        return 1 if bad else 0
    doc = load() if TABLE.is_file() else {"apps": {}}
    if a.backfill:
        new = {v: r for v, r in backfill().items() if v not in doc["apps"]}
        doc = {**doc, "apps": {**doc["apps"], **new}}
        print(f"app_reads: backfilled {len(new)} version(s) from git: {', '.join(sorted(new, key=ver)) or 'none'}")
    else:
        doc, what = record(doc, version, reads, a.replace)
        print(f"app_reads: {what}")
    TABLE.write_text(dump(doc), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
