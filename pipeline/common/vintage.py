"""Do the shipped tiles and the shipped bundle come from one atlas?

A section is an integer handle — an index into the atlas's `section_handles.txt` — carried by
BOTH artifacts. Build them from different atlases and they do not miss each other: they agree
on a number that means two different rivers, so every lookup succeeds and every answer is
about the wrong water.

THIS IS NOT HYPOTHETICAL. The tiles were rebuilt with new handles while `bundle.sqlite` was
left behind at an older build, and the app's Conditions map painted every river as
unmeasured — a state it draws on purpose, so it read as a quiet feed rather than a broken
pair. Nothing errored at any layer.

The app refuses to colour a mismatched pair (see `useVintage`). This is the same check one
step earlier, against what is actually ON DISK at the paths that get shipped, so a build says
so at the moment the pair diverges rather than a person finding out by looking at a map.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def tile_vintage(tiles_dir: Path) -> str | None:
    """The handle digest the tile sidecar publishes, or None if there is no sidecar."""
    p = Path(tiles_dir) / "atlas.meta.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("section_handles") or None
    except (OSError, ValueError):
        return None


def bundle_vintage(bundle: Path) -> str | None:
    """The handle digest a built bundle records in `meta`, or None if it predates the field."""
    p = Path(bundle)
    if not p.exists():
        return None
    try:
        db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            row = db.execute("SELECT v FROM meta WHERE k = 'section_handles'").fetchone()
        finally:
            db.close()
        return (row[0] or None) if row else None
    except sqlite3.Error:
        return None


def registry_vintages(tiles_dir: Path, bundle: Path) -> tuple[str | None, str | None]:
    """The registry digests (P2) the tile sidecar and the bundle record — None where absent."""
    t = b = None
    p = Path(tiles_dir) / "atlas.meta.json"
    if p.exists():
        try:
            t = json.loads(p.read_text(encoding="utf-8")).get("registry") or None
        except (OSError, ValueError):
            t = None
    if Path(bundle).exists():
        try:
            db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
            try:
                row = db.execute("SELECT v FROM meta WHERE k = 'registry_digest'").fetchone()
            finally:
                db.close()
            b = (row[0] or None) if row else None
        except sqlite3.Error:
            b = None
    return t, b


def report(tiles_dir: Path, bundle: Path, *, strict: bool = False) -> tuple[bool, str]:
    """`(ok, message)` for the pair that would actually ship.

    `ok` is True only when BOTH artifacts exist and agree, or when one of them has not been
    built at all — a checkout that has not produced both is not a mismatch. `strict` (P2: the
    rebuild command's last stage, where both were just built) refuses that too: a missing
    artifact, or a pair where neither records a digest, is a failure, never "not checked".

    ONE DIGEST AND NOT THE OTHER IS A MISMATCH, not an absence. That is the shape the real
    failure took: the tiles were rebuilt carrying handles while the shipped bundle still
    keyed sections by string, and treating "no digest" as "nothing to compare" would let
    exactly that pair through. An artifact with no digest predates the handle scheme, so it
    cannot be the partner of one that has it.
    """
    tp, bp = Path(tiles_dir) / "atlas.pmtiles", Path(bundle)
    if not tp.exists() or not bp.exists():
        missing = tp if not tp.exists() else bp
        return not strict, f"  vintage: not checked — {missing} has not been built"
    t, b = tile_vintage(tiles_dir), bundle_vintage(bundle)
    if t is None and b is None:
        return not strict, "  vintage: not checked — neither artifact records a handle digest"
    if t is None or b is None:
        stale = "tiles" if t is None else "bundle"
        return False, (
            f"  ⚠️  VINTAGE MISMATCH — the {stale} predates the section handle table\n"
            f"        tiles  {tp}  {t or 'no digest'}\n"
            f"        bundle {bp}  {b or 'no digest'}\n"
            f"      One of these keys sections by an integer handle and the other by a\n"
            f"      string. Rebuild the older one.")
    if t == b:
        # PAIRED ON THE HANDLES ALONE (P18, MINIMAL-PACKAGING §6.1). The tiles are geometry; the only names in them are
        # map labels, and a name fix is a data release. So a registry difference is reported, never refused: the
        # labels are older than the data's names until the next tile release (`pipeline build --labels`).
        rt, rb = registry_vintages(tiles_dir, bundle)
        if rt and rb and rt != rb:
            return True, (f"  vintage: tiles and bundle agree ({t}); note: the tile labels are from registry {rt}, "
                          f"the data from {rb} — refresh with `pipeline build --labels` when the names matter")
        return True, f"  vintage: tiles and bundle agree ({t}, registry {rt or rb or 'unrecorded'})"
    return False, (
        f"  ⚠️  VINTAGE MISMATCH — these two must not ship together\n"
        f"        tiles  {Path(tiles_dir) / 'atlas.pmtiles'}  {t}\n"
        f"        bundle {bundle}  {b}\n"
        f"      A section is an index into the atlas's handle table, so a mixed pair does\n"
        f"      not fail to match — it matches the WRONG SECTION. Rebuild whichever is\n"
        f"      older, or copy the bundle you just built over the one that ships.")


def shipped_digests(tiles_dir: Path, bundle: Path) -> dict[str, str]:
    """{handle digest: the shipped artifact carrying it} — the atlases a derivative on disk was
    cut from. Empty when neither artifact exists or records one."""
    out: dict[str, str] = {}
    t = tile_vintage(tiles_dir)
    if t:
        out[t] = str(Path(tiles_dir) / "atlas.pmtiles")
    b = bundle_vintage(bundle)
    if b:
        out.setdefault(b, str(bundle))
    return out


def promoted_atlas(build_dir: Path, tiles_dir: Path, bundle: Path) -> str | None:
    """The shipped artifact (tiles or bundle) that carries the handle digest of the atlas at
    `build_dir`, or None when nothing shipped was cut from it.

    A PROMOTED ATLAS IS IMMUTABLE. Every derivative — the reach run, the bundle, the status index,
    the export, the tiles — is keyed by the atlas's `section_handles.txt` digest, and the atlas
    build is not deterministic (memory `atlas-build-not-deterministic`): rebuilding INTO the
    directory a shipped bundle or tile set names rewrites the handle table under them. Afterwards
    the tiles and the bundle still agree with each other, and both silently disagree with the atlas
    on disk, until the next bundle build finds no reach run for it — hours later, as the only
    symptom. So a build refuses such an `--out` (`pipeline.atlas.build --force` overrides), and the
    curation app builds to `<build>_next` and promotes by an explicit step (`pipeline.atlas.promote`).
    """
    from pipeline.common.section_handles import FILENAME, digest_for
    build_dir = Path(build_dir)
    if not (build_dir / FILENAME).exists():
        return None
    try:
        have = digest_for(build_dir)
    except Exception:            # noqa: BLE001 — an unreadable table is not a promoted atlas
        return None
    return shipped_digests(tiles_dir, bundle).get(have)


# --------------------------------------------------------------------------------------------
# P07: EVERY ARTIFACT OF A DELIVERY, not only tiles ↔ bundle
# --------------------------------------------------------------------------------------------
#
# One `deliver all` cuts the status index, the export pair, the answers, the card gate's stamp and the regs store from ONE
# bundle. Each records the digests it was cut from: section_handles, reach_digest, rule_ids_sha256, and, for the store
# and the stamp, the sha256 of the exact answers bytes. `report_all` holds every one of them to the bundle's (and the
# bundle's handles to the tiles', through `report`), so a delivery that mixes two runs is refused naming the artifact.
# The release package (P08) joins this list when it exists.

from dataclasses import dataclass


@dataclass(frozen=True)
class Artifacts:
    """Where one delivery's artifacts are."""
    bundle: Path
    status_index: Path
    export: Path            # ui-rules-export.json; the guide, the answers and the stamp sit beside it
    store: Path

    @property
    def guide(self) -> Path:
        return self.export.with_name("ui-rules-guide.json")

    @property
    def answers(self) -> Path:
        return self.export.with_name("ui-rules-answers.json")

    @property
    def stamp(self) -> Path:
        from pipeline.deliver.answers.card_check import STAMP_NAME
        return self.export.with_name(STAMP_NAME)

    @classmethod
    def in_dir(cls, d: Path) -> "Artifacts":
        """`deliver all --out-dir d`'s layout: every file in one directory."""
        d = Path(d)
        return cls(d / "bundle.sqlite", d / "status_index.bin", d / "ui-rules-export.json", d / "regs.sqlite")

    @classmethod
    def canonical(cls) -> "Artifacts":
        from pipeline.common.curated import GENERATED
        from pipeline.deliver.store import OUT as STORE_OUT
        from pipeline.tools.export_ui_rules import OUT as EXPORT_OUT
        return cls(GENERATED.bundle / "bundle.sqlite", GENERATED.bundle / "status_index.bin", EXPORT_OUT, STORE_OUT)


def _meta(p: Path) -> dict:
    db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        return dict(db.execute("SELECT k, v FROM meta"))
    finally:
        db.close()


def report_all(tiles_dir: Path, arts: Artifacts, *, strict: bool = False) -> tuple[bool, str]:
    """`(ok, message)` for EVERY artifact of the delivery at `arts`, against the bundle's digests and the tiles'.
    `strict` (the end of `deliver all` and `pipeline build`) refuses a missing artifact; otherwise it is "not checked"."""
    import hashlib
    ok, first = report(tiles_dir, arts.bundle, strict=strict)
    lines, bad = [first], [] if ok else ["tiles/bundle"]
    if not arts.bundle.exists():
        return ok, "\n".join(lines)
    from pipeline.deliver.bundle.build import BUNDLE_FORMAT, schema_sha256
    B = _meta(arts.bundle)
    want = {"section_handles": B.get("section_handles"), "reach_digest": B.get("reach_digest")}

    def fail(name: str, why: str) -> None:
        bad.append(name)
        lines.append(f"  ⚠️  {name}: {why}")

    def same(name: str, got: dict, keys=("section_handles", "reach_digest")) -> None:
        diff = {k: (got.get(k), want[k]) for k in keys if got.get(k) != want[k]}
        if diff:
            fail(name, "cut from another bundle — " + ", ".join(f"{k} {g!r} ≠ bundle's {w!r}" for k, (g, w) in diff.items()))

    # the bundle's own format
    for k, v in (("format", BUNDLE_FORMAT), ("schema_sha256", schema_sha256())):
        if B.get(k) != v and (strict or B.get(k) is not None):
            fail("bundle.sqlite", f"meta {k} {B.get(k)!r} is not this code's {v!r} — rebuild the bundle")
    missing = []
    for p in (arts.status_index, arts.export, arts.guide, arts.answers, arts.stamp, arts.store):
        if not p.exists():
            missing.append(p.name)
    if missing:
        (fail if strict else lambda n, w: lines.append(f"  vintage: not checked — {w}"))(
            ", ".join(missing), "not built")
    if arts.status_index.exists():
        from pipeline.deliver import status_index as SI
        ix = SI.Index(arts.status_index.read_bytes())
        same(arts.status_index.name, {"section_handles": ix.handles, "reach_digest": ix.reach_digest})
    rids = B.get("rule_ids_sha256")
    for p in (arts.export, arts.guide):
        if p.exists():
            doc = json.loads(p.read_text(encoding="utf-8"))
            same(p.name, doc["about"]["bundle"])
            if "rule_ids" in doc:
                from pipeline.deliver.answers.encode import rule_ids_digest
                if rule_ids_digest(doc["rule_ids"]) != rids:
                    fail(p.name, f"rule_ids are not the bundle's ({rids})")
    sha = None
    if arts.answers.exists():
        raw = arts.answers.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        about = json.loads(raw)["about"]
        same(arts.answers.name, about["bundle"])
        if about["export"]["rule_ids_sha256"] != rids:
            fail(arts.answers.name, f"rule_ids_sha256 {about['export']['rule_ids_sha256']!r} ≠ the bundle's {rids!r}")
    if arts.stamp.exists() and sha:
        st = json.loads(arts.stamp.read_text(encoding="utf-8")).get("card_check", {})
        if st.get("problems") != 0:
            fail(arts.stamp.name, f"the card gate found {st.get('problems')} problems")
        if st.get("answers_sha256") != sha:
            fail(arts.stamp.name, "answers_sha256 is not the answers file's: the gate proved other bytes")
    if arts.store.exists():
        S = _meta(arts.store)
        same(arts.store.name, S)
        if S.get("rule_ids_sha256") != rids:
            fail(arts.store.name, f"rule_ids_sha256 {S.get('rule_ids_sha256')!r} ≠ the bundle's {rids!r}")
        if sha and S.get("answers_sha256") != sha:
            fail(arts.store.name, "answers_sha256 is not the answers file's: the store holds other answers")
    if not bad:
        lines.append(f"  vintage: every artifact agrees (handles {want['section_handles']}, reach {want['reach_digest']}, "
                     f"rule ids {rids})")
    return not bad, "\n".join(lines)
