"""THE APP VERSION FROM THE COMMITS (T998, owner 2026-11-21): SemVer 2.0.0, the bump read from Conventional Commits.

    python -m pipeline.release_version            # the version the commits since the last v* tag compute, and why
    python -m pipeline.release_version --at REV   # at another commit (the release command asks at origin/main)

THE RULE (`app/tools/versioning.json` `bump`, the file commitlint's config reads too): `feat` -> minor; `fix`, `perf` ->
patch; a `!` after the type or a `BREAKING CHANGE:` footer -> major, which BEFORE 1.0.0 bumps minor (the 0.x reading,
release-please's `bump-minor-pre-major`); every other type -> no bump. 1.0.0 is the owner's call, the first open Play /
App Store release: the one version set by hand that the commits do not decide (`accepts`).

WHICH COMMITS: those since the newest `v<x.y.z>` tag (the base is its version), or, before the first tag, since the
cutover (`versioning.json` `cutover`, base 0.8.5); the cutover, every `exempt` commit and their ancestors never count.
ONLY THE APP'S COMMITS BUMP THE APP (a monorepo's per-package rule, as release-please's `packages` paths): a commit
counts when it changes a path under `app/` other than `NOT_APP` (the feeds worker, which deploys on its own, and the
fixtures the release re-cuts itself: `release_follows.FOLLOWS`). A pipeline / data / docs-only commit is the data
release's, never the app's: so "no app commit bumps" IS "a data-only release" (`--cut --data-only`).

HOW A MESSAGE READS is commitlint's reading (conventional-changelog-conventionalcommits' header and note patterns, and
commitlint's default ignore list), pinned by one shared contract, `app/tools/commits-contract.json`, which
`app/tools/check-commits.test.ts` holds commitlint to and `pipeline/tests/test_release_version.py` holds this to. A
merge commit whose own subject is not in the format (git's `Merge branch …`) counts by the commits it merged; a
non-merge commit not in the format is `bad` and stops the release, named (CI refuses one on every PR, so one here
joined before the check: add it to `exempt` with why).
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

VERSIONING_FILE = Path(REPO_ROOT) / "app" / "tools" / "versioning.json"
VERSIONING: dict = json.loads(VERSIONING_FILE.read_text(encoding="utf-8"))
#: type -> "minor" | "patch" | None (no bump)
BUMP: dict[str, str | None] = VERSIONING["bump"]
#: the levels, lowest first
LEVELS = ("none", "patch", "minor", "major")

# conventional-changelog-conventionalcommits (createParserOpts) and conventional-commits-parser's note regex, as
# commitlint parses: JS `\w` is ASCII; a note keyword starts a body / footer line, in any case
HEADER = re.compile(r"^(\w*)(?:\((.*)\))?!?: (.*)$", re.ASCII)
BREAKING_HEADER = re.compile(r"^(\w*)(?:\((.*)\))?!: (.*)$", re.ASCII)
NOTE = re.compile(r"^(?:\*\s+)?(BREAKING CHANGE|BREAKING-CHANGE):\s*(.*)", re.IGNORECASE)
# @commitlint/is-ignored's default wildcards (lib/defaults.js), applied to the trimmed message
_IGNORED = (
    re.compile(r"^((Merge pull request)|(Merge (.*?) into (.*?)|(Merge branch (.*?)))(?:\r?\n)*$)", re.M),
    re.compile(r"^(Merge tag (.*?))(?:\r?\n)*$", re.M),
    re.compile(r"^(R|r)evert (.*)"),
    re.compile(r"^(R|r)eapply (.*)"),
    re.compile(r"^(amend|fixup|squash)!"),
    re.compile(r"^(Merged (.*?)(in|into) (.*)|Merged PR (.*): (.*))"),
    re.compile(r"^Merge remote-tracking branch(\s*)(.*)"),
    re.compile(r"^Automatic merge(.*)"),
    re.compile(r"^Auto-merged (.*?) into (.*)"),
)
#: semver.valid, as is-ignored's `isSemver` asks it of the header (a `chore(x): 1.2.3` subject is ignored)
_SEMVER = re.compile(r"^[v=]?\s*(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$")
VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")


@dataclass(frozen=True)
class Parsed:
    type: str
    scope: str | None
    subject: str
    breaking: bool


def header_of(message: str) -> str:
    return message.lstrip("\r\n").split("\n", 1)[0].rstrip("\r")


def parse(message: str) -> Parsed | None:
    """The message's type, scope, subject and whether it is breaking; None when the header is not in the format."""
    header = header_of(message)
    m = HEADER.match(header)
    if not m:
        return None
    body = message.lstrip("\r\n").split("\n")[1:]
    breaking = bool(BREAKING_HEADER.match(header)) or any(NOTE.match(ln) for ln in body)
    return Parsed(m.group(1), m.group(2), m.group(3), breaking)


def _is_semver_header(message: str) -> bool:
    first = message.split("\n", 1)[0]
    stripped = re.sub(r"^chore(\([^)]+\))?:", "", first)
    stripped = re.sub(r"\[(skip|ci)(-|\s)(ci|skip)\]", "", stripped, flags=re.I)
    stripped = re.sub(r"\((skip|ci)(-|\s)(ci|skip)\)", "", stripped, flags=re.I).strip()
    return bool(_SEMVER.match(stripped))


def is_ignored(message: str) -> bool:
    """commitlint skips it (a merge, a revert, a fixup, a bare version)."""
    m = message.rstrip()
    return any(rx.search(m) for rx in _IGNORED) or _is_semver_header(m)


@dataclass(frozen=True)
class Commit:
    sha: str
    parents: tuple[str, ...]
    message: str
    paths: tuple[str, ...] = ()

    @property
    def subject(self) -> str:
        return header_of(self.message)


def reading(c: Commit) -> str:
    """major | minor | patch | none (an in-format commit's level, before the 0.x rule), ignored (commitlint skips it),
    merge (a merge commit whose own subject is not in the format: it counts by what it merged), bad (not in the format)."""
    if is_ignored(c.message):
        return "ignored"
    p = parse(c.message)
    if p is not None and p.type in BUMP:
        return "major" if p.breaking else (BUMP[p.type] or "none")
    return "merge" if len(c.parents) > 1 else "bad"


def _follows_paths() -> tuple[str, ...]:
    from pipeline.release_follows import FOLLOWS
    return tuple(p for f in FOLLOWS for p in f.paths)


#: the paths under app/ that are not the app (what no store build or site ships): the feeds worker (it deploys on its
#: own, T431), the dev tools and gates with their tests, the conformance suite, the fixtures, and what the release
#: re-cuts itself (release_follows.FOLLOWS: a data release's own commits never bump the app)
NOT_APP: tuple[str, ...] = ("app/workers/", "app/tools/", "app/conformance/", "app/fixtures/", *_follows_paths())
#: under NOT_APP but shipped: the website's page builder (the site goes out with each app version, its version matches)
#: and (T998 review) the lean MapLibre patch every store build ships (T420/T893: build.sh makes the AAR from it)
APP_AGAIN: tuple[str, ...] = ("app/tools/seo/", "app/tools/build-seo.mjs",
                              "app/tools/maplibre-patch/feature-state-deltas-lean.patch",
                              "app/tools/maplibre-patch/build.sh")
#: a test file or a document anywhere under app/ ships in nothing
NOT_SHIPPED = re.compile(r"\.test\.[cm]?[jt]sx?$|(^|/)__tests__/|\.md$")


def _under(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path == p.rstrip("/") or path.startswith(p.rstrip("/") + "/") for p in prefixes)


def is_app_path(path: str) -> bool:
    """A path an app store build or the website ships (app/, minus NOT_APP — APP_AGAIN back in — and test / doc files)."""
    if not path.startswith("app/") or NOT_SHIPPED.search(path):
        return False
    return _under(path, APP_AGAIN) or not _under(path, NOT_APP)


def touches_app(c: Commit) -> bool:
    return any(is_app_path(p) for p in c.paths)


def parse_version(v: str) -> tuple[int, int, int]:
    m = VERSION.fullmatch(v)
    if not m:
        raise ValueError(f"{v!r} is not x.y.z")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def bump(version: str, level: str) -> str | None:
    """The next version after `version` for a change of `level` (None: no bump). Before 1.0.0 a major bumps minor."""
    major, minor, patch = parse_version(version)
    if level == "major" and major == 0:
        level = "minor"
    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    if level == "patch":
        return f"{major}.{minor}.{patch + 1}"
    return None


@dataclass
class Decision:
    """What the commits since `since` decide. `version` None: no app release (no app commit bumps)."""
    base: str                                  # the version they start from (the last v* tag's, or the cutover's)
    since: str                                 # "v0.9.0" | "the cutover b5dd8ade3"
    level: str                                 # none | patch | minor | major (the raw level, before the 0.x rule)
    version: str | None
    deciding: list[Commit] = field(default_factory=list)       # the app commits at that level
    counted: dict[str, list[Commit]] = field(default_factory=dict)   # every app commit, by reading
    not_app: list[Commit] = field(default_factory=list)        # in the range, but no app path
    bad: list[Commit] = field(default_factory=list)            # app commits not in the format: the release stops

    def said(self) -> str:
        """The decision in plain lines: the version, the commits that decided it, what else was counted."""
        n = sum(len(v) for v in self.counted.values())
        if self.bad:
            head = (f"CANNOT COMPUTE the version: {len(self.bad)} app commit(s) since {self.since} are not Conventional "
                    "Commits")
        elif self.version is None:
            head = (f"no app release: none of the {n} app commit(s) since {self.since} bumps the app (no feat / fix / "
                    f"perf / breaking change; {len(self.not_app)} more change no app file): it stays {self.base}, a "
                    "data-only release (--cut --data-only)")
        else:
            pre = " (before 1.0.0 a breaking change bumps minor)" if self.level == "major" and self.version.startswith("0.") else ""
            head = f"{self.version}: {self.base} + {self.level}{pre}, from {n} app commit(s) since {self.since}"
        lines = [head]
        for c in self.bad:
            lines.append(f"  not in the format: {c.sha[:12]} {c.subject}")
        if self.deciding and not self.bad:
            lines.append(f"  decided by ({self.level}):")
            lines += [f"    {c.sha[:12]} {c.subject}" for c in self.deciding[:20]]
            if len(self.deciding) > 20:
                lines.append(f"    … and {len(self.deciding) - 20} more")
        counts = ", ".join(f"{len(v)} {k}" for k, v in sorted(self.counted.items()) if v and k not in ("bad",))
        if counts:
            lines.append(f"  counted: {counts}; {len(self.not_app)} commit(s) change no app file (data / pipeline / docs)")
        return "\n".join(lines)


def decide(base: str, since: str, commits: list[Commit]) -> Decision:
    """The version the app commits of `commits` compute from `base`."""
    counted: dict[str, list[Commit]] = {}
    not_app, bad = [], []
    for c in commits:
        if not touches_app(c):
            not_app.append(c)
            continue
        r = reading(c)
        counted.setdefault(r, []).append(c)
        if r == "bad":
            bad.append(c)
    level = max((r for r in counted if r in LEVELS), key=LEVELS.index, default="none")
    version = None if bad else bump(base, level)
    return Decision(base=base, since=since, level=level, version=version,
                    deciding=list(counted.get(level, [])) if level != "none" else [], counted=counted,
                    not_app=not_app, bad=bad)


def accepts(version: str, d: Decision) -> tuple[bool, str]:
    """(T998) Is `version`, set by hand (`--cut X.Y.Z`, app.json), the one the commits compute? The one exception is the
    owner's: 1.0.0 from a 0.x base, the first open store release. (ok, why)"""
    if d.bad:
        return False, d.said().splitlines()[0]
    if version == d.version:
        return True, f"{version} is the version the commits compute"
    if version == "1.0.0" and d.base.startswith("0.") and (d.version is None or d.version.startswith("0.")):
        return True, (f"1.0.0: the first open Play / App Store release, the owner's call (T998); the commits alone "
                      f"compute {d.version or d.base}")
    if d.version is None:
        return False, f"{version} is set by hand, but no app commit since {d.since} bumps the app (it stays {d.base})"
    return False, f"{version} is set by hand, but the commits compute {d.version} ({d.base} + {d.level})"


# ------------------------------------------------------------------------------------------------------------------ git

def _git(*args: str) -> str:
    got = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True)
    if got.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed (rc={got.returncode}): {got.stderr.strip()[-300:]}")
    return got.stdout


def last_tag() -> tuple[str, str] | None:
    """(tag, version) of the newest `v<x.y.z>` tag in this clone, by version (fetch the tags first), or None."""
    best = None
    for t in _git("tag", "--list", "v*").split():
        m = VERSION.fullmatch(t[1:])
        if m and (best is None or parse_version(t[1:]) > parse_version(best[1])):
            best = (t, t[1:])
    return best


def commits_since(rev: str, not_: list[str]) -> list[Commit]:
    """Every commit on `rev` that is none of `not_` nor before them, oldest first, with the paths each changes (a merge:
    against its first parent, the change it brought)."""
    out = _git("log", "--reverse", "--diff-merges=first-parent", "--name-only",
               "--format=%x1e%H%x1f%P%x1f%B%x1f", rev, "--not", *not_)
    commits = []
    for rec in out.split("\x1e"):
        if not rec.strip():
            continue
        sha, parents, message, paths = rec.split("\x1f", 3)
        commits.append(Commit(sha, tuple(parents.split()), message.strip(),
                              tuple(p for p in paths.split("\n") if p.strip())))
    return commits


def decide_at(rev: str = "HEAD") -> Decision:
    """The decision at `rev`: since the newest v* tag, or since the cutover (versioning.json)."""
    exempt = [VERSIONING["cutover"], *VERSIONING.get("exempt", {})]
    tag = last_tag()
    if tag:
        return decide(tag[1], tag[0], commits_since(rev, [tag[0], *exempt]))
    return decide(VERSIONING["base"], f"the cutover {VERSIONING['cutover'][:9]}", commits_since(rev, exempt))


# ------------------------------------------------------------------------------------------------------- the changelog

#: the developer changelog's sections, in order (CHANGELOG.md, grouped by type)
SECTIONS = (("breaking", "Breaking changes"), ("feat", "Features"), ("fix", "Fixes"), ("perf", "Performance"),
            ("other", "Other (docs, chores, CI, tests, refactors, builds)"))
CHANGELOG_TITLE = "# Changelog (developers)"


def changelog_section(version: str, date: str, d: Decision) -> str:
    """The developer changelog entry for `version` (CHANGELOG.md): the app commits that went into it, by type. The
    anglers' notes are app/packages/core/src/changelog.ts, written by hand."""
    groups: dict[str, list[str]] = {k: [] for k, _ in SECTIONS}
    for r, cs in d.counted.items():
        if r in ("ignored", "merge", "bad"):
            continue
        for c in cs:
            p = parse(c.message)
            key = "breaking" if p.breaking else p.type if p.type in ("feat", "fix", "perf") else "other"
            scope = f"**{p.scope}** " if p.scope else ""
            groups[key].append(f"- {scope}{p.subject} (`{c.sha[:9]}`)")
    lines = [f"## {version} ({date})", "",
             f"From {d.base}: {sum(len(v) for v in groups.values())} app commit(s) since {d.since}."]
    for key, title in SECTIONS:
        if groups[key]:
            lines += ["", f"### {title}", "", *groups[key]]
    return "\n".join(lines) + "\n"


def changelog_with(text: str | None, section: str, version: str) -> str:
    """CHANGELOG.md with `section` as its newest entry (an entry of the same version is replaced)."""
    head = (f"{CHANGELOG_TITLE}\n\nWritten by `python -m pipeline release --cut` (T998) from the commits each app version "
            "took, newest first. The anglers' notes are `app/packages/core/src/changelog.ts`.\n")
    if not text:
        return head + "\n" + section
    parts = re.split(r"(?m)^(?=## )", text)
    intro, entries = parts[0], [p for p in parts[1:] if not p.startswith(f"## {version} ")]
    return intro.rstrip("\n") + "\n\n" + section + ("\n" + "".join(entries) if entries else "")


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="python -m pipeline.release_version", description=__doc__.split("\n")[0])
    ap.add_argument("--at", default="HEAD", help="the commit to ask at (default HEAD)")
    a = ap.parse_args(argv)
    d = decide_at(a.at)
    print(d.said())
    return 1 if d.bad else 0


if __name__ == "__main__":
    sys.exit(main())
