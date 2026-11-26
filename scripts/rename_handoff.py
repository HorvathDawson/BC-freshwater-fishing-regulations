#!/usr/bin/env python3
"""(T1081) THE ONE SCRIPTED RENAME: `handoff/PLAN/` becomes `docs/`, `handoff/PLAN/app-integration/tickets/` becomes `tickets/`.

    python3 scripts/rename_handoff.py --dry-run     what it would move and rewrite, nothing touched
    python3 scripts/rename_handoff.py               move, rewrite, print what is left (changes staged, not committed)
    python3 scripts/rename_handoff.py --commit      the same, then ONE commit (the message is fixed; --message overrides)

What it does, in this order (stdlib only; idempotent: a second run finds nothing to move and nothing to rewrite):
  1. refuses a dirty tree (a half-edited tree would mix the owner's edits into a commit of mechanical ones);
  2. checks, before touching anything, that every explicit edit below still finds its text (a tree that moved on since
     this script was written fails loudly, nothing changed);
  3. `git mv` handoff/PLAN/app-integration/tickets -> tickets, then handoff/PLAN -> docs (the ignored views/ moves with it);
  4. rewrites every tracked TEXT file that spells the old paths, longest prefix first:
       handoff/PLAN/app-integration/tickets -> tickets    handoff/PLAN -> docs    handoff/ -> docs/
     (also the `\\/` spelling inside JS regex literals), the explicit edits (path constants BUILT by joins, which no
     string search finds: EDITS below), a relative `app-integration/tickets` -> `tickets`, and the relative markdown links
     of every moved page (re-pointed from where the page now sits);
     A line marked `rename_handoff: keep` names the old path on purpose (ledger.mjs LEGACY_TICKETS_DIR: the folder's git
     history, and branches not yet rebased) and is left alone.
  5. NEVER rewrites: anything under data/ (the owner's review data), binary files, tickets/closed.tsv (history: its titles
     and its tar), CHANGELOG.md (past entries), this script;
  6. prints every remaining `handoff` in tracked text outside that set, for a human to judge, and the sites where a bare
     `handoff/` (not `handoff/PLAN`) was rewritten, which may have been history prose.

Someone else's branch breaks (every path it touches moved): run this script on it, commit, then rebase or merge the trunk;
the textual rewrites are deterministic, so the rename commit then merges as a no-op on the lines both sides already agree on.
"""
from __future__ import annotations

import argparse
import posixpath
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SELF = "scripts/rename_handoff.py"

OLD_TICKETS, NEW_TICKETS = "handoff/PLAN/app-integration/tickets", "tickets"
OLD_PLAN, NEW_PLAN = "handoff/PLAN", "docs"
OLD_ROOT = "handoff"

#: paths (prefixes) whose files are never rewritten, with why
NEVER = {
    "data/": "the owner's review data spells handoff/ in his notes",
    "CHANGELOG.md": "past entries are history",
    f"{OLD_TICKETS}/closed.tsv": "closed tickets' titles are history",
    f"{NEW_TICKETS}/closed.tsv": "closed tickets' titles are history",
    SELF: "this script names the old paths",
}

#: (file, old, new, replace_all): the places a path is BUILT from parts, or spelled in a way the prefix rules do not reach.
#: Found by `git grep -nE "['\"]handoff['\"]"` and `git grep -n "app-integration"` outside handoff/ and data/ (2026-11-25).
EDITS = [
    # path joins in code
    ("app/tools/adipose-shots.test.ts", '"..", "handoff", "PLAN", "app-integration", "T38-shots"', '"..", "docs", "app-integration", "T38-shots"', False),
    ("app/tools/fish-looks-shots.test.ts", '"..", "handoff", "PLAN", "app-integration", "T46-shots"', '"..", "docs", "app-integration", "T46-shots"', False),
    ("app/tools/rowpages-shots.test.ts", '"..", "handoff", "PLAN", "app-integration", "T1031-shots"', '"..", "docs", "app-integration", "T1031-shots"', False),
    ("pipeline/reports.py", 'REPO / "handoff" / "PLAN" / "app-integration" / "tickets"', 'REPO / "tickets"', False),
    ("pipeline/tests/test_before_push.py", 'here / "handoff"', 'here / "docs"', True),
    ("pipeline/tests/test_ci_paths.py", 'tmp_path / "handoff"', 'tmp_path / "docs"', True),
    ("pipeline/tests/test_workflow_pins.py", 'p.startswith("handoff")', 'p.startswith(("docs", "tickets"))', False),
    # path filters: the docs folder is two top-level folders now
    (".github/workflows/docs-ci.yml", '"handoff/**"', '"docs/**", "tickets/**"', False),
    ("scripts/ci_paths.py", '"handoff/**"', '"docs/**", "tickets/**"', False),
    ("pipeline/tests/test_ci_paths.py", '":(exclude)handoff/**"', '":(exclude)docs/**", ":(exclude)tickets/**"', False),
    ("pipeline/tests/test_workflow_pins.py", 'assert "handoff/**" in _on(_load("docs-ci.yml"))[ev]["paths"], ev',
     'assert {"docs/**", "tickets/**"} <= set(_on(_load("docs-ci.yml"))[ev]["paths"]), ev', False),
    ("pipeline/tests/test_workflow_pins.py", '("test_workflow_pins.py", "handoff/**"): "the docs filter\'s own entry, compared as text",',
     '("test_workflow_pins.py", "docs/**"): "the docs filter\'s own entry, compared as text",\n    ("test_workflow_pins.py", "tickets/**"): "the docs filter\'s own entry, compared as text",', False),
    # alternations of top-level folders: the tickets folder is a top-level folder now
    ("pipeline/tests/test_workflow_pins.py", r'curation-review|handoff|pipeline)"', r'curation-review|docs|tickets|pipeline)"', False),
    ("app/tools/check-tickets.mjs", r'scripts|handoff|curation-review', r'scripts|docs|tickets|curation-review', False),
    # a key that is a path RELATIVE to the planning folder: the tickets folder no longer sits inside it
    ("app/tools/ledger.mjs", '"app-integration/tickets/T281-files"', '"tickets/T281-files"', False),
    # a listing of the planning folder that held the tickets folder
    ("app/tools/ledger.test.ts", '.toEqual(["T11-shots", "T4-shots", "tickets"])', '.toEqual(["T11-shots", "T4-shots"])', False),
    # prose that now needs both folders
    ("README.md", "| `handoff/` | the work: tickets and their ledger, runbooks, plans, the owners' asks (each ticket's `## Owner` block) |",
     "| `docs/` | the plans, runbooks, restart pages and decisions |\n| `tickets/` | the open tickets, and the index of the closed ones |", False),
]

#: bare `handoff/` (after the two longer prefixes) -> docs/. The two longer prefixes are matched first, in both spellings.
RULES = [
    (re.compile(r"(?<![\w-])handoff/PLAN/app-integration/tickets(?![\w-])"), "tickets"),
    (re.compile(r"(?<![\w-])handoff\\/PLAN\\/app-integration\\/tickets(?![\w-])"), "tickets"),
    (re.compile(r"(?<![\w-])handoff/PLAN(?![\w-])"), "docs"),
    (re.compile(r"(?<![\w-])handoff\\/PLAN(?![\w-])"), "docs"),
]
BARE = [(re.compile(r"(?<![\w-])handoff/"), "docs/"), (re.compile(r"(?<![\w-])handoff\\/"), "docs\\/")]
#: a path relative to the planning folder, spelled without the prefix
RELATIVE = (re.compile(r"(?<![\w/.-])app-integration/tickets(?![\w-])"), "tickets")
MD_LINK = re.compile(r"(\]\()([^)\s#]+)(#[^)\s]*)?(\))")

COMMIT_MESSAGE = """chore(T1081): handoff/PLAN becomes docs/, the tickets move to the top level

One scripted commit (scripts/rename_handoff.py): handoff/PLAN/app-integration/tickets/ -> tickets/,
handoff/PLAN/ -> docs/, every tracked text file that spelled the old paths rewritten, path joins in code
edited by name. data/, CHANGELOG.md and closed.tsv are untouched.

Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>"""


def git(*args: str, check: bool = True) -> str:
    return subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=check).stdout


def tracked() -> list[str]:
    return [p for p in git("ls-files", "-z").split("\0") if p]


def map_path(p: str) -> str:
    """Where a repo path lives after the rename (identity for anything not under handoff/)."""
    for old, new in ((OLD_TICKETS, NEW_TICKETS), (OLD_PLAN, NEW_PLAN), (OLD_ROOT, NEW_PLAN)):
        if p == old or p.startswith(old + "/"):
            return new + p[len(old):]
    return p


def never(path: str) -> str | None:
    for pre, why in NEVER.items():
        if path == pre or (pre.endswith("/") and path.startswith(pre)):
            return why
    return None


def read_text(path: Path) -> str | None:
    """The file's text, or None when it is binary / not UTF-8 / a link (never rewritten)."""
    if path.is_symlink() or not path.is_file():
        return None
    raw = path.read_bytes()
    if b"\0" in raw[:8192]:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def fix_links(text: str, old: str, new: str, files: set[str]) -> str:
    """Re-point a moved page's relative markdown links: the target stays where it is (or moves with the rename), the page moved."""
    if old == new:
        return text
    odir, ndir = posixpath.dirname(old), posixpath.dirname(new)

    def one(m: re.Match) -> str:
        target = m.group(2)
        if re.match(r"([a-z][a-z0-9+.-]*:|/|handoff/)", target) or target.startswith("#"):
            return m.group(0)
        resolved = posixpath.normpath(posixpath.join(odir, target))
        if resolved not in files and not any(f.startswith(resolved.rstrip("/") + "/") for f in files):
            return m.group(0)       # not a tracked path: leave it for a human
        rel = posixpath.relpath(map_path(resolved), ndir or ".")
        return f"{m.group(1)}{rel}{'/' if target.endswith('/') else ''}{m.group(3) or ''}{m.group(4)}"
    return MD_LINK.sub(one, text)


#: a line carrying this marker names the OLD path on purpose (git history of the folder, branches not yet rebased): never rewritten
KEEP = "rename_handoff: keep"


def rewrite(text: str, old: str, new: str, files: set[str], bare_hits: list[str]) -> str:
    for f, a, b, every in EDITS:
        if f == old:
            text = text.replace(a, b) if every else text.replace(a, b, 1)
    out = []
    for line in text.split("\n"):
        if KEEP not in line:
            if new.endswith(".md") and old != new:
                line = fix_links(line, old, new, files)
            for rx, to in RULES:
                line = rx.sub(to, line)
            for rx, to in BARE:
                if rx.search(line):
                    bare_hits.append(old)
                line = rx.sub(to, line)
            line = RELATIVE[0].sub(RELATIVE[1], line)
        out.append(line)
    return "\n".join(out)


def preflight(dry: bool) -> list[str]:
    """What stops the run before anything changes."""
    bad = []
    if git("status", "--porcelain"):
        msg = "the tree is dirty (commit or stash first)"
        (print(f"note: {msg}; --dry-run goes on") if dry else bad.append(msg))
    moved = not (ROOT / OLD_ROOT).exists()
    for f, a, b, _ in EDITS:
        text = read_text(ROOT / f)
        if text is None:
            bad.append(f"{f}: an explicit edit names it, but it is missing or not text")
        elif a not in text and b not in text:
            bad.append(f"{f}: the explicit edit's text is gone (neither {a[:50]!r} nor its replacement): update EDITS")
        elif a not in text and not moved:
            bad.append(f"{f}: already edited but handoff/ still exists: a half-run? look first")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="print the summary; touch nothing")
    ap.add_argument("--commit", action="store_true", help="commit the result as one commit")
    ap.add_argument("--message", help="the commit message (default: the fixed one)")
    args = ap.parse_args()

    bad = preflight(args.dry_run)
    if bad:
        print("refusing to run:\n  " + "\n  ".join(bad), file=sys.stderr)
        return 2

    paths = tracked()
    files = set(paths)
    pairs = [(p, map_path(p)) for p in paths]
    moves = [(a, b) for a, b in ((OLD_TICKETS, NEW_TICKETS), (OLD_PLAN, NEW_PLAN)) if (ROOT / a).exists()]
    moved_files = sum(1 for o, n in pairs if o != n)
    print(f"move: {', '.join(f'{a} -> {b}' for a, b in moves) or 'nothing (already moved)'}; {moved_files} tracked file(s) change path")
    for a, b in moves:
        if (ROOT / b).exists():
            print(f"refusing: {b} already exists", file=sys.stderr)
            return 2
    if not args.dry_run:
        for a, b in moves:
            git("mv", a, b)
        left = ROOT / OLD_ROOT
        if left.is_dir() and not any(left.iterdir()):
            left.rmdir()

    changed, skipped, bare_hits = [], [], []
    for old, new in pairs:
        why = never(old) or never(new)
        if why:
            skipped.append((new, why))
            continue
        src = ROOT / (old if args.dry_run else new)
        text = read_text(src)
        if text is None:
            continue
        out = rewrite(text, old, new, files, bare_hits)
        if out != text:
            changed.append(new)
            if not args.dry_run:
                src.write_bytes(out.encode("utf-8"))
    print(f"rewrite: {len(changed)} tracked text file(s)")
    nonempty = sorted({w for w, _ in skipped})
    print(f"never rewritten: {len(skipped)} file(s) ({', '.join(sorted({why for _, why in skipped}))})")
    if bare_hits:
        print(f"a bare `handoff/` (not handoff/PLAN) was rewritten to docs/ in {len(set(bare_hits))} file(s); history prose may now misname a folder:")
        for f in sorted(set(bare_hits)):
            print(f"  {map_path(f)}")
    _ = nonempty

    # what still spells the old name, outside the never-rewrite set
    print("remaining `handoff` in tracked text (a human judges these):")
    n = 0
    for old, new in pairs:
        if never(old) or never(new):
            continue
        text = read_text(ROOT / (old if args.dry_run else new))
        if text is None:
            continue
        if args.dry_run:
            text = rewrite(text, old, new, files, [])
        for i, line in enumerate(text.split("\n"), 1):
            if re.search(r"handoff", line, re.I) and KEEP not in line:
                n += 1
                print(f"  {new}:{i}: {line.strip()[:140]}")
    print(f"  ({n} line(s))")

    if args.commit and not args.dry_run:
        git("add", "-A")
        out = subprocess.run(["git", "-C", str(ROOT), "commit", "-q", "-m", args.message or COMMIT_MESSAGE], capture_output=True, text=True)
        print(out.stdout + out.stderr)
        return out.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
