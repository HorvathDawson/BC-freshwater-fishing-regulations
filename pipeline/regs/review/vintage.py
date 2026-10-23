"""ONE VINTAGE ON SCREEN (curation-review PLAN §4 phase 2, T133): is THIS entry built?

A reviewer signs off on an entry by looking at where its rules bind and what an angler is told.
Both come from builds — the atlas, the reach run, the bundle, the release — and on 2026-10-22 the
reach run (17:08), the bundle (15:49) and the release (15:53) were three different builds. A mark
taken then is held against a binding and an answer nobody can say belong to the entry on screen.

So the tool asks, per entry, whether every artifact was made from THIS entry as it is on disk now:

    the reach run   `report.json` `entry_digests[entry]` == the entry's digest now   (`io.entry_digest`)
    the bundle      `meta` `entry_digests[entry]`        == the entry's digest now
                    and, of the same corpus as the reach run, made FROM it (`bundle_rebound`)
    the release     `manifest.json` `bundle`             == the bundle's `entries_digest` + `reach_digest`
                    (a release cut from this bundle carries the bundle's entries, so this entry's digest)
    the atlas       the reach run and the bundle were made on the SERVED atlas (handles + registry), and
                    every split the entry's binding reads (`fingerprint.entry_split_ids`) is in it as
                    authored now (`splits.ANCHORS_FILE`; an atlas built before that record can only say
                    which split ids it holds — a moved split is not seen there, `Built.anchors` None)

Any "no" makes the entry AWAITING BUILD, with every reason (`Waiting`) and the command that builds
it (`rebuild_commands`; the tool prints it, never runs it). While it waits, a reviewer can still
answer T (text) and R (rules) — they read the file — but not P (place) or A (answer), so a VERIFIED
mark is refused (`refuse_unbuilt`). No bundle at all is awaiting build too (T97 owner Q4: a mark
made with no bundle holds no bundle half, so nothing would ever say it went stale).

The state is a pure function of digests (`entry_state`); `read_built` reads the digests off disk, and
the tool re-reads them whenever one of those files changes, so a rebuild clears "awaiting build"
with no restart.

"BUNDLE MATCHES" (`bundle_matches`) is the per-rule check beside it: the bundle's copy of each rule
against the file now — its label AND the number of sections it binds, against the builder run live
on the served atlas. Labels alone said "matches" while Tacheeda, Twin and Main Lake bound one lake
of two in the bundle (7 rules in a 200-entry sample, PLAN §4).
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Mapping, Optional

CURRENT = "current"
AWAITING = "awaiting build"
#: what a reviewer may answer while an entry awaits its build: the checks that read the FILE
CHECKS_WHILE_AWAITING = ("T", "R")


class Waiting(str, Enum):
    """Why an entry (or a split) is awaiting build — which artifact is not this entry's."""
    no_reach_run = "no reach run"
    reach_predates = "the reach run predates per-entry digests"
    reach_atlas = "the reach run was made on another atlas"
    reach_entry = "the entry changed since the reach run"
    no_bundle = "no bundle"
    bundle_predates = "the bundle predates per-entry digests"
    bundle_atlas = "the bundle was made on another atlas"
    bundle_entry = "the entry changed since the bundle"
    bundle_reach = "the bundle was made from another reach run of the same corpus"
    no_release = "no release"
    release_predates = "the release predates its bundle digests"
    release_bundle = "the release was cut from another bundle"
    split_unbuilt = "a split it reads is not in the served atlas"
    split_moved = "a split it reads moved since the atlas was built"


#: the reasons only an ATLAS build answers (then reach and deliver)
ATLAS_REASONS = frozenset({Waiting.split_unbuilt, Waiting.split_moved, Waiting.reach_atlas,
                           Waiting.bundle_atlas})


@dataclass(frozen=True)
class Built:
    """What the artifacts on disk were made from (`read_built`). A missing artifact is None."""
    #: the served atlas: section-handle and registry digests, the split ids it resolved
    #: (`splits.resolved.json`), and the anchors it recorded (`splits.ANCHORS_FILE`; None when the
    #: atlas predates the record)
    atlas: Optional[tuple[str, str]] = None
    split_ids: frozenset = frozenset()
    anchors: Optional[Mapping[str, str]] = None
    #: report.json: {handles, registry_digest, digest, entries_digest, entry_digests | None}
    reach: Optional[Mapping] = None
    #: bundle meta: {section_handles, registry_digest, reach_digest, entries_digest,
    #: entry_digests | None}
    bundle: Optional[Mapping] = None
    #: the release on disk (`release.manifest.latest`): {id, bundle: {entries_digest, reach_digest} | None}
    release: Optional[Mapping] = None

    def summary(self) -> dict:
        """What the tool shows of the vintage: each artifact's identity, never the per-entry maps."""
        def cut(d, keys):
            return None if d is None else {k: d.get(k) for k in keys}
        return {"atlas": None if self.atlas is None else {"handles": self.atlas[0],
                                                         "registry": self.atlas[1]},
                "anchors_recorded": self.anchors is not None,
                "reach": cut(self.reach, ("digest", "entries_digest", "handles", "registry_digest")),
                "bundle": cut(self.bundle, ("reach_digest", "entries_digest", "section_handles",
                                            "registry_digest")),
                "release": cut(self.release, ("id", "bundle"))}


# --------------------------------------------------------------------------------------------
# The one decision
# --------------------------------------------------------------------------------------------

def _split_reasons(anchors_now: Mapping[str, str], built: Built) -> tuple[set, list[str]]:
    got: set[Waiting] = set()
    which: list[str] = []
    for sid, anchor in sorted(anchors_now.items()):
        if built.anchors is not None:
            if sid not in built.anchors:
                got.add(Waiting.split_unbuilt)
                which.append(sid)
            elif built.anchors[sid] != anchor:
                got.add(Waiting.split_moved)
                which.append(sid)
        elif sid not in built.split_ids:
            got.add(Waiting.split_unbuilt)
            which.append(sid)
    return got, which


def _state(got: set, splits: list[str], built: Built, target: Optional["Target"]) -> dict:
    reasons = [w for w in Waiting if w in got]
    return {"state": AWAITING if reasons else CURRENT, "reasons": [w.value for w in reasons],
            "splits": splits, "splits_checked": "anchors" if built.anchors is not None else "ids",
            "commands": rebuild_commands(reasons, target)}


def entry_state(entry_id: str, digest_now: str, anchors_now: Mapping[str, str],
                built: Built, target: Optional["Target"] = None) -> dict:
    """THE ONE ANSWER to "is this entry built": {state, reasons, splits, splits_checked, commands}.
    `digest_now` is `io.entry_digest` of the entry on disk; `anchors_now` is split id ->
    `splits.anchor_digest` now, for every split the entry reads (`fingerprint.entry_split_ids`).
    `state` is CURRENT only when the reach run, the bundle and the release were all made from this
    entry as it is now, on the served atlas, with every split it reads built as authored.
    `target`: where the printed `commands` rebuild (`rebuild_commands`; default the live tree)."""
    got, splits = _split_reasons(anchors_now, built)
    R, B, L = built.reach, built.bundle, built.release
    if R is None:
        got.add(Waiting.no_reach_run)
    else:
        if (R.get("handles"), R.get("registry_digest")) != built.atlas:
            got.add(Waiting.reach_atlas)
        if R.get("entry_digests") is None:
            got.add(Waiting.reach_predates)
        elif R["entry_digests"].get(entry_id) != digest_now:
            got.add(Waiting.reach_entry)
    if B is None:
        got.add(Waiting.no_bundle)
    else:
        if (B.get("section_handles"), B.get("registry_digest")) != built.atlas:
            got.add(Waiting.bundle_atlas)
        if B.get("entry_digests") is None:
            got.add(Waiting.bundle_predates)
        elif B["entry_digests"].get(entry_id) != digest_now:
            got.add(Waiting.bundle_entry)
        if R is not None and bundle_rebound(R, B):
            got.add(Waiting.bundle_reach)
    if L is None:
        got.add(Waiting.no_release)
    elif not L.get("bundle"):
        got.add(Waiting.release_predates)
    elif B is not None and release_is_of(L, B) is False:
        got.add(Waiting.release_bundle)
    return _state(got, splits, built, target)


def split_state(split_id: str, anchor_now: str, built: Built,
                target: Optional["Target"] = None) -> dict:
    """Is this split built as authored now? A split mark answers P on the map, which draws the
    served atlas: a split it does not hold, or holds elsewhere, is awaiting build."""
    got, splits = _split_reasons({split_id: anchor_now}, built)
    return _state(got, splits, built, target)


def bundle_rebound(reach: Mapping, bundle: Mapping) -> bool:
    """The reach run REBOUND the corpus the bundle was made from: the same corpus
    (`entries_digest`), other bindings (`digest` != the bundle's `reach_digest`) — a reach-code or
    steelhead-list change, a run on another build. No entry changed, so no entry digest says so,
    yet every binding on screen (the builder's now) may differ from the bundle's: every entry
    waits for deliver. (A run of ANOTHER corpus is per entry — T133 owner Q6: an entry untouched
    by an edit to another stays current; its answer going stale is phase 1's "answer changed".)"""
    return (reach.get("entries_digest") == bundle.get("entries_digest")
            and reach.get("digest") != bundle.get("reach_digest"))


def release_is_of(release: Mapping, bundle: Mapping) -> bool:
    """A release was cut from this bundle: it states the bundle's corpus and reach-run digests."""
    said = release.get("bundle") or {}
    return (said.get("entries_digest"), said.get("reach_digest")) == \
        (bundle.get("entries_digest"), bundle.get("reach_digest"))


@dataclass(frozen=True)
class Target:
    """WHERE A REBUILD READS AND WRITES (T136): the review tool's own paths — the corpus and
    splits.json it edits, and the atlas, reach run, bundle, release root and tile set it serves.
    `canonical()` is the live tree; the tool pointed at COPIES (`CURATION_*`, a scratch review)
    is another target, and the commands it prints must rebuild THOSE, never the live data."""
    entries: Path
    splits: Path
    build: Path
    reach: Path
    bundle: Path
    release: Path
    tiles: Path

    #: what a rebuild WRITES (the corpus and splits.json are only read)
    OUTPUTS = ("build", "reach", "bundle", "release", "tiles")

    @classmethod
    def canonical(cls) -> "Target":
        from pipeline.common.curated import CURATED, GENERATED
        b = GENERATED.build()
        return cls(entries=CURATED.regulations.entries.catalogue, splits=CURATED.waters.splits,
                   build=b, reach=GENERATED.reaches / b.name, bundle=GENERATED.bundle / "bundle.sqlite",
                   release=GENERATED.release, tiles=GENERATED.tiles)

    def _same(self, other: "Target", name: str) -> bool:
        return Path(getattr(self, name)).resolve() == Path(getattr(other, name)).resolve()

    def kind(self) -> str:
        """`canonical` (every path the live one), `scratch` (every OUTPUT elsewhere), or `mixed`:
        some output live and some not — a rebuild from it would write the live tree from copies
        (or a copy from half the live tree), so no command is printed for it."""
        live = Target.canonical()
        same = {n: self._same(live, n) for n in ("entries", "splits", *self.OUTPUTS)}
        if all(same.values()):
            return "canonical"
        if not any(same[n] for n in self.OUTPUTS):
            return "scratch"
        return "mixed"


def _shown(p: Path) -> str:
    """A path as a printed command names it: repo-relative under the repo, else absolute (never
    through a symlink — a worktree's data/ may link to the main checkout's)."""
    import os
    from pipeline.common.curated import REPO_ROOT
    p = Path(os.path.abspath(p))
    try:
        return str(p.relative_to(os.path.abspath(REPO_ROOT)))
    except ValueError:
        return str(p)


def _next(p: Path) -> Path:
    return Path(p).with_name(Path(p).name + "_next")


#: the binding diff's line: what to read before promoting (AGENTS 63)
_DIFF_WHY = ("  # per-rule bindings, live vs the side atlas's reach run — read it before promoting "
             "(AGENTS 63)")


def rebuild_commands(reasons: Iterable, target: Optional[Target] = None) -> list[str]:
    """What the human runs to clear `reasons` (PLAN §5 — the tool prints it, never runs it), FOR
    `target` (default the live tree): an entry fix needs reach then deliver; a split (or a reach
    run / bundle made on another atlas) needs the atlas built beside the served one, a reach run
    on it and its per-rule binding diff read against the served run (AGENTS 63), the promote, then
    reach, TILES and deliver — a new atlas is a new sid space, and `deliver all` refuses a bundle
    whose tiles are another atlas's (so the old "--promote --no-tiles" could never finish).

    A SCRATCH target (`Target.kind`: every output a copy) gets the same chain spelled out step by
    step with every path its own, so a rehearsal never writes the live tree; a MIXED one gets no
    command, only why."""
    rs = {Waiting(r) for r in reasons}
    if not rs:
        return []
    t = target or Target.canonical()
    kind = t.kind() if target is not None else "canonical"
    if kind == "mixed":
        return ["# no command: the tool serves some live artifacts and some copies, so a rebuild "
                "would write one from the other — point every CURATION_* output (build, reach, "
                "bundle, release, tiles) at the live tree or at copies"]
    s = {k: _shown(getattr(t, k)) for k in ("entries", "splits", "build", "reach", "bundle",
                                             "release", "tiles")}
    nb, nr = _shown(_next(t.build)), _shown(_next(t.reach))
    atlas = bool(rs & ATLAS_REASONS)
    if kind == "canonical":
        if not atlas:
            return ["scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles"]
        return ["scripts/heavy.sh atlas 30 -- python -m pipeline build --atlas",
                f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {nb} "
                f"--out {nr}",
                f"python -m pipeline.atlas.reach.binding_diff {s['reach']} {nr}{_DIFF_WHY}",
                "scripts/heavy.sh build 30 -- python -m pipeline build --promote"]
    out_dir = Path(t.bundle).parent
    if Path(t.release).resolve() != (out_dir / "release").resolve():
        return [f"# no command: `pipeline.deliver all --out-dir {_shown(out_dir)}` writes the "
                f"release into {_shown(out_dir / 'release')}, but the tool reads releases from "
                f"{s['release']} — point CURATION_RELEASE_ROOT there"]
    reach = (f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {s['build']} "
             f"--entries {s['entries']} --out {s['reach']}")
    deliver = (f"scripts/heavy.sh deliver 30 -- python -m pipeline.deliver all --build {s['build']} "
               f"--reaches {s['reach']} --entries {s['entries']} --tiles {s['tiles']} "
               f"--out-dir {_shown(out_dir)}")
    if not atlas:
        return [reach, deliver]
    return [f"scripts/heavy.sh atlas 30 -- python -m pipeline.atlas.build --full --splits "
            f"{s['splits']} --out {nb}",
            f"scripts/heavy.sh reach 30 -- python -m pipeline.atlas.reach.cli --build {nb} "
            f"--entries {s['entries']} --out {nr}",
            f"python -m pipeline.atlas.reach.binding_diff {s['reach']} {nr}{_DIFF_WHY}",
            f"python -m pipeline.atlas.promote {nb} --target {s['build']} --tiles {s['tiles']} "
            f"--bundle {s['bundle']}",
            reach,
            f"scripts/heavy.sh tiles 30 -- python -m pipeline.deliver.tiles --build {s['build']} "
            f"--out {s['tiles']} --release-root {s['release']} --bundle {s['bundle']}",
            deliver]


def reach_run_commands(target: Optional[Target] = None) -> list[str]:
    """What makes a reach run (and the rest) for `target` — the queue's words when there is none."""
    return rebuild_commands([Waiting.no_reach_run], target)


def refuse_unbuilt(scope: str, answers: Mapping[str, str], state: Mapping) -> None:
    """A mark on a target AWAITING BUILD may answer only T and R (what the file says), never P or
    A (where it binds, what an angler is told) — so a verified mark is refused, and a split mark
    (P only) is refused outright. Raises ValueError naming why and what to run."""
    if state.get("state") == CURRENT:
        return
    held = sorted(k for k in answers if k not in CHECKS_WHILE_AWAITING)
    if held or scope == "split":
        cmds = "; then ".join(state.get("commands") or [])
        why = (" — a split is confirmed on the built map, so it cannot be marked yet"
               if scope == "split" else f" — {', '.join(held)} cannot")
        raise ValueError(
            f"awaiting build ({', '.join(state.get('reasons') or [])}): only "
            f"{' and '.join(CHECKS_WHILE_AWAITING)} can be answered until it is built{why}"
            + (f". Run: {cmds}" if cmds else ""))


# --------------------------------------------------------------------------------------------
# Bundle matches
# --------------------------------------------------------------------------------------------

def bundle_matches(labels_now: Mapping[str, Optional[str]], labels_bundle: Mapping[str, Optional[str]],
                   counts_now: Mapping[str, Optional[int]], counts_bundle: Mapping[str, int]) -> dict:
    """Does the bundle's copy of the entry say what the file says now, rule by rule? A rule
    MATCHES when it is in both, its label is the same, and it binds the same number of sections
    (`fingerprint.rule_section_counts` in the bundle; the builder run live on the served atlas
    now). A rule whose live count is None (the builder could not answer for one entry alone: a
    complement or the steelhead set needs the corpus) is `unchecked`, never a match by default.
    Returns {matches, rules: {rule_id: [what differs]}, unchecked: [rule_id]}."""
    out: dict[str, list[str]] = {}
    unchecked: list[str] = []
    for rid in sorted(set(labels_now) | set(labels_bundle)):
        why = []
        if rid not in labels_bundle:
            why.append("not in the bundle")
        elif rid not in labels_now:
            why.append("not in the file")
        else:
            if labels_now[rid] != labels_bundle[rid]:
                why.append("label")
            n = counts_now.get(rid)
            if n is None:
                unchecked.append(rid)
            elif n != counts_bundle.get(rid, 0):
                why.append(f"sections: {counts_bundle.get(rid, 0)} in the bundle, {n} now")
        if why:
            out[rid] = why
    return {"matches": not out, "rules": out, "unchecked": unchecked}


# --------------------------------------------------------------------------------------------
# Reading the digests off disk
# --------------------------------------------------------------------------------------------

def _reach(reach_dir: Path) -> Optional[dict]:
    p = Path(reach_dir) / "report.json"
    if not p.exists():
        return None
    rep = json.loads(p.read_text(encoding="utf-8"))
    return {k: rep.get(k) for k in ("handles", "registry_digest", "digest", "entries_digest",
                                    "entry_digests")}


def _bundle(bundle_path: Path) -> Optional[dict]:
    p = Path(bundle_path)
    if not p.exists():
        return None
    db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        meta = dict(db.execute("SELECT k, v FROM meta WHERE k IN ('section_handles', "
                               "'registry_digest', 'reach_digest', 'entries_digest', "
                               "'entry_digests')"))
    finally:
        db.close()
    ed = meta.get("entry_digests")
    return dict(meta, entry_digests=json.loads(ed) if ed else None)


def _release(release_root: Path) -> Optional[dict]:
    from pipeline.deliver.release.manifest import latest
    d = latest(release_root)
    if d is None:
        return None
    m = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
    return {"id": m.get("data_release") or d.name, "bundle": m.get("bundle")}


def _atlas(build_dir: Path) -> tuple[Optional[tuple[str, str]], frozenset, Optional[dict]]:
    from pipeline.atlas.splits.splits import read_anchor_record
    from pipeline.common.section_handles import digest_for, registry_digest_for
    b = Path(build_dir)
    if not (b / "registry.json").exists():
        return None, frozenset(), None
    resolved = b / "splits.resolved.json"
    ids = frozenset(r["split_id"] for r in json.loads(resolved.read_text(encoding="utf-8"))) \
        if resolved.exists() else frozenset()
    return (digest_for(b), registry_digest_for(b)), ids, read_anchor_record(b)


def read_built(build_dir: Path, reach_dir: Path, bundle_path: Path, release_root: Path,
               *, atlas=None) -> Built:
    """Every digest `entry_state` compares, read off disk. `atlas` reuses an already-read atlas
    triple (`_atlas`): its registry digest reads 100 MB, so the tool keys it on the atlas's own
    files and re-reads the cheap three on every change."""
    a = atlas if atlas is not None else read_atlas(build_dir)
    return Built(atlas=a[0], split_ids=a[1], anchors=a[2], reach=_reach(reach_dir),
                 bundle=_bundle(bundle_path), release=_release(release_root))


def read_atlas(build_dir: Path):
    """The served atlas's identity: ((handles, registry) | None, split ids, anchors | None)."""
    return _atlas(build_dir)


def stamp_paths(build_dir: Path, reach_dir: Path, bundle_path: Path, release_root: Path) -> list[Path]:
    """The files whose (mtime, size) say the vintage on disk changed — a new reach run, bundle or
    release, a promoted atlas — so the tool re-reads it (no restart)."""
    from pipeline.atlas.splits.splits import ANCHORS_FILE
    root = Path(release_root)
    rels = sorted(root.glob("r-*/manifest.json")) if root.is_dir() else []
    b = Path(build_dir)
    return [b / "registry.json", b / "section_handles.txt", b / "splits.resolved.json",
            b / ANCHORS_FILE, Path(reach_dir) / "report.json", Path(bundle_path), *rels]
