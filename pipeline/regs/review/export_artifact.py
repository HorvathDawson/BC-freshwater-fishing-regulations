"""THE REVIEW, EXPORTED FOR THE SHAREABLE PAGE (T184 phase 1): everything the review-mode screen reads
for a list of targets, written once per vintage so a reviewer with no backend can answer T R P A.

    python -m pipeline.regs.review.export_artifact --pilot handoff/PLAN/curation-review/pilot.json \
        --out curation-review/artifact/out
    -> <out>/export.json          one JSON: per target its review payload, rules, pre-checks, book
                                  evidence (page + crop rectangles), map geometry, the angler's-page
                                  preview, the marks in force; the check / problem lists; and every
                                  markable unit's PRINTS keyed by their digest (`fp`)
    -> <out>/book/p<printed>.webp each cited printed page, rendered ONCE (crops are rectangles over it)

THE SAME FUNCTIONS AS THE SCREEN, NOT HTTP: the backend's own route functions and modules are imported
(`app.get_entry`, `app.entry_review` → `review.review_view`, `app.entry_reaches`, `app.item_geojson`,
`app.rule_resolved`, `app.item_tributary_geojson`, `app.get_mode`, `review.split_queue`,
`evidence.locate`, `answer.effective`), so the page shows what the local screen shows.

THE PRINTS ARE THE MARK'S (T97): what `POST /api/marks` would stamp on a mark made now —
`fingerprint.entry_fingerprints(e, ctx, rule_id=…)` / `split_fingerprints` on the vintage on disk.
The page carries only their digest; `import_artifact_marks` looks the prints up here, so a mark made on
the page holds exactly the vintage the reviewer saw, and reads stale (typed reasons) if it moved since.

READ ONLY. Writes nothing but `--out` (never data/curated, data/generated, the bundle or the tiles).
Section ids never leave this module (AGENTS 5): the map is projected coordinates, the answer preview
carries no `sid`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Iterable, Mapping, Sequence

#: map: the SVG's width in its own units; the height follows the water's aspect (clamped)
MAP_W = 1000
MAP_ASPECT = (0.45, 1.5)
#: simplification tolerance, in map units (1/1000 of the width): sub-pixel on a phone, fine at 4x zoom
MAP_TOL = 0.35
#: a rule binding more sections than this is not drawn section by section (a zone rule binds 10^5–10^6):
#: the page states the count instead
MAP_MAX_SECTIONS = 20000
#: one level of tributaries, capped (whole streams only — `item_tributary_geojson`)
TRIB_LIMIT = 3000
#: the book: px per PDF point of the page images (2.0 = 144 dpi: the book's 6.5 pt row text is 13 px,
#: legible at 2x pinch) and the WebP quality
PAGE_SCALE = 2.0
WEBP_QUALITY = 50
#: the fish the answer preview asks about when an entry's rules name none (a leaf code)
DEFAULT_FISH = "RB"
MAX_FISH = 3


def fp_of(prints: Mapping) -> str:
    """THE DIGEST A PAGE MARK CARRIES: sha256 of the prints' canonical JSON (16 hex)."""
    blob = json.dumps(prints, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------------------------
# the map: GeoJSON (lon/lat) -> one SVG path per layer, projected and simplified
# --------------------------------------------------------------------------------------------

class Projection:
    """Equirectangular at the frame's middle latitude, scaled into a MAP_W-wide box (y down)."""

    def __init__(self, bounds: tuple[float, float, float, float]):
        x0, y0, x1, y1 = bounds
        self.k = math.cos(math.radians((y0 + y1) / 2))
        w = max((x1 - x0) * self.k, 1e-6)
        h = max(y1 - y0, 1e-6)
        aspect = min(max(h / w, MAP_ASPECT[0]), MAP_ASPECT[1])
        self.W, self.H = MAP_W, round(MAP_W * aspect)
        pad = 0.06
        s = min(self.W * (1 - 2 * pad) / w, self.H * (1 - 2 * pad) / h)
        self.s = s
        self.ox = self.W / 2 - ((x0 + x1) / 2) * self.k * s
        self.oy = self.H / 2 + ((y0 + y1) / 2) * s
        #: metres per map unit (the scale bar)
        self.m_per_unit = 111_320.0 / s

    def __call__(self, x, y, z=None):
        return (x * self.k * self.s + self.ox, self.oy - y * self.s)


def _bounds(feats: Iterable[Mapping]):
    from shapely.geometry import shape
    xs0, ys0, xs1, ys1 = [], [], [], []
    for f in feats:
        g = f.get("geometry")
        if not g:
            continue
        b = shape(g).bounds
        xs0.append(b[0]); ys0.append(b[1]); xs1.append(b[2]); ys1.append(b[3])
    if not xs0:
        return None
    return (min(xs0), min(ys0), max(xs1), max(ys1))


def _path(geoms: Sequence, proj: Projection) -> str:
    """One SVG path `d` for many geometries (lines and polygon rings), projected, merged, simplified."""
    from shapely.geometry import LineString, MultiLineString
    from shapely.ops import linemerge, transform, unary_union
    lines, rings = [], []
    for g in geoms:
        g = transform(proj, g)
        for part in getattr(g, "geoms", [g]):
            if part.geom_type == "LineString":
                lines.append(part)
            elif part.geom_type == "Polygon":
                rings.append(part.exterior)
    out = []
    if lines:
        merged = linemerge(unary_union(lines)) if len(lines) > 1 else lines[0]
        merged = merged.simplify(MAP_TOL, preserve_topology=False)
        for part in getattr(merged, "geoms", [merged]):
            cs = list(part.coords)
            if len(cs) >= 2:
                out.append("M" + "L".join(f"{x:.1f} {y:.1f}" for x, y in cs))
    for r in rings:
        r = LineString(r.coords).simplify(MAP_TOL)
        cs = list(r.coords)
        if len(cs) >= 3:
            out.append("M" + "L".join(f"{x:.1f} {y:.1f}" for x, y in cs) + "Z")
    return "".join(out).replace(".0 ", " ").replace(".0L", "L").replace(".0M", "M").replace(".0Z", "Z")


def _shapes(feats: Iterable[Mapping]):
    from shapely.geometry import shape
    return [shape(f["geometry"]) for f in feats if f.get("geometry")]


def build_map(item_feats: list[dict], rule_sets: Mapping[str, dict], tribs: list[dict],
              focus_splits: Iterable[str] = ()) -> dict | None:
    """The map one target draws: the water (streams, side channels, lakes), its tributaries (one level),
    each rule's bound sections (`rule_sets[rid]` = {"in": node ids on the item, "extra": features off
    it, "excluded": features its carve-outs removed, "n": sections, "drawn": bool}), and the split
    points (those named in `focus_splits` ringed). Layers are de-duplicated: two rules binding the same
    sections share one path."""
    from shapely.geometry import shape
    by_node = {}
    base = {"stream": [], "side_channel": [], "waterbody": []}
    splits = []
    for f in item_feats:
        p = f.get("properties") or {}
        k = p.get("kind")
        if k == "split":
            splits.append(f)
            continue
        if p.get("node_id"):
            by_node[p["node_id"]] = f
        base.setdefault(k if k in base else "stream", []).append(f)
    extra_all = [f for r in rule_sets.values() for f in r.get("extra", [])]
    frame = _bounds(item_feats + extra_all) or _bounds(tribs)
    if frame is None:
        return None
    proj = Projection(frame)
    paths: list[dict] = []
    seen: dict[str, int] = {}

    def add(kind: str, geoms) -> int | None:
        d = _path(geoms, proj) if geoms else ""
        if not d:
            return None
        key = kind + "|" + d
        if key not in seen:
            seen[key] = len(paths)
            paths.append({"k": kind, "d": d})
        return seen[key]

    layers = {k: add(k, _shapes(v)) for k, v in base.items()}
    trib_ix = add("trib", _shapes(tribs))
    rules = {}
    for rid, r in rule_sets.items():
        if not r.get("drawn"):
            rules[rid] = {"n": r.get("n", 0), "drawn": False}
            continue
        geoms = [shape(by_node[n]["geometry"]) for n in r.get("in", []) if n in by_node]
        geoms += _shapes(r.get("extra", []))
        lakes = [g for g in geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        lines = [g for g in geoms if g not in lakes]
        rules[rid] = {"n": r.get("n", 0), "drawn": True,
                      "on": [i for i in (add("reach", lines), add("reach_lake", lakes)) if i is not None],
                      "out": [i for i in (add("excluded", _shapes(r.get("excluded", []))),) if i is not None]}
    focus = set(focus_splits)
    pts = []
    for f in splits:
        g = shape(f["geometry"])
        if g.geom_type != "Point":
            continue
        x, y = proj(g.x, g.y)
        p = f.get("properties") or {}
        pts.append({"x": round(x, 1), "y": round(y, 1), "id": p.get("split_id"), "label": p.get("label") or "",
                    "focus": p.get("split_id") in focus, "auto": bool(p.get("auto"))})
    return {"w": proj.W, "h": proj.H, "m_per_unit": round(proj.m_per_unit, 2), "paths": paths,
            "layers": {k: v for k, v in layers.items() if v is not None}, "trib": trib_ix,
            "rules": rules, "splits": pts}


# --------------------------------------------------------------------------------------------
# the book
# --------------------------------------------------------------------------------------------

class Book:
    """The cited pages, rendered once each (WebP), and each target's rectangles over them."""

    def __init__(self, out: Path, scale: float = PAGE_SCALE, quality: int = WEBP_QUALITY):
        self.dir = out / "book"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.scale, self.quality = scale, quality
        self.pages: dict[int, dict] = {}

    def page(self, printed: int, pdf_page: int) -> dict:
        if printed in self.pages:
            return self.pages[printed]
        import evidence                                     # the backend's renderer (pdfium, locked)
        img = evidence._render(pdf_page, self.scale)
        name = f"p{printed:03d}.webp"
        img.save(self.dir / name, format="WEBP", quality=self.quality, method=6)
        w_pt, h_pt = img.width / self.scale, img.height / self.scale
        got = {"printed": printed, "pdf_page": pdf_page, "image": f"book/{name}",
               "w": round(w_pt, 2), "h": round(h_pt, 2), "bytes": (self.dir / name).stat().st_size}
        self.pages[printed] = got
        return got


def book_evidence(view_ev: Mapping, source_pages: Sequence, book: Book, entry_id: str) -> dict:
    """The review payload's evidence (`review.review_view` → `evidence.locate`) as rectangles over a
    rendered page: `strips` (the crop, stacked) and `boxes` (marked on the page), in PDF points."""
    import synopsis_pages
    ev = {k: v for k, v in view_ev.items() if k not in ("crop_url", "page_url", "row_image")}
    printed = ev.get("printed")
    pdf = ev.get("pdf_page")
    if printed is None:
        printed = next((p for p in source_pages if isinstance(p, int)), None)
    if printed is not None and pdf is None:
        pdf = synopsis_pages.pdf_page(printed)
    if printed is not None and pdf is not None:
        ev["page"] = book.page(int(printed), int(pdf))
    if ev.get("kind") in ("row", "passage"):
        import review
        # the strips are what `evidence.crop_png` stacks (the review payload leaves them out):
        # `review.located` is the crop endpoint's own call
        ev["strips"] = review.located(entry_id)["strips"]
    return ev


# --------------------------------------------------------------------------------------------
# the answer preview (the angler's page, for the part the screen's link opens)
# --------------------------------------------------------------------------------------------

def _rep_sid(item: str, part: int) -> int | None:
    import answer
    db = answer._db()
    try:
        row = db.execute("SELECT p.rep_sid FROM part p JOIN item i ON i.ord = p.ord "
                         "WHERE i.item_id = ? AND p.part_ix = ?", (item, part)).fetchone()
    finally:
        db.close()
    return int(row[0]) if row else None


def _fish_for(rules: Iterable[Mapping]) -> list[str]:
    from pipeline.regs.parsing import catalogue as C
    groups = set(getattr(C, "SPECIES_GROUPS", {}) or {})
    named = []
    for r in rules:
        for s in r.get("species") or []:
            if s not in named and s not in groups and s.isupper() and "_" not in s:
                named.append(s)
    if DEFAULT_FISH not in named:
        named.append(DEFAULT_FISH)
    return named[:MAX_FISH]


def answer_preview(link: Mapping | None, rules: Sequence[Mapping], day: str) -> dict:
    """What the angler's page says on the part the screen's `o` link opens (`answer.app_part`): the
    effective answer (`answer.effective`, the bundle's own reader) for a few fish on the review day.
    No `sid` leaves: it is a bundle handle (AGENTS 5)."""
    import answer
    if not link or not link.get("url"):
        return {"why": (link or {}).get("why") or "no page to open", "previews": []}
    sid = _rep_sid(link["item"], int(link["part"]))
    out = {"item": link["item"], "part": link["part"], "sections": link.get("sections"),
           "via": link.get("via"), "local_url": link["url"], "previews": []}
    if sid is None:
        out["why"] = "the bundle has no such part"
        return out
    for fish in _fish_for(rules):
        try:
            got = answer.effective(sid, day, fish)
        except ValueError as ex:
            out["previews"].append({"fish": fish, "error": str(ex)})
            continue
        got.pop("sid", None)
        got.pop("bundle", None)
        out["previews"].append(got)
    return out


# --------------------------------------------------------------------------------------------
# one target
# --------------------------------------------------------------------------------------------

def _slim_rule(r: Mapping) -> dict:
    keep = ("rule_id", "type", "label", "verbatim", "species", "species_except", "obligation",
            "includes_tributaries", "tributaries_only", "extents", "extent_text", "unresolved_locators",
            "undrawn_part", "review_reason", "tributary_excludes")
    return {k: r[k] for k in keep if r.get(k) not in (None, [], "")}


def _slim_diag(d: Mapping) -> dict:
    return {k: (v if not isinstance(v, list) or len(v) <= 12 else v[:12] + [f"… {len(v) - 12} more"])
            for k, v in d.items() if k not in ("sections", "section_ids", "nodes")}


def export_entry(t: Mapping, day: str, book: Book, prints: dict) -> dict:
    import app
    import reuse
    from pipeline.regs.review import fingerprint as F
    eid = t["id"]
    detail = app.get_entry(eid)
    view = app.entry_review(eid, day)
    e_raw = reuse._find(eid)[1]
    e = detail["entry"]
    zone = view["kind"] == "zone"
    ctx = reuse._review_context()
    units = []
    for rid in ([r["rule_id"] for r in e.get("rules") or []] if zone else [None]):
        pr = F.entry_fingerprints(e_raw, ctx, rule_id=rid)
        pr = json.loads(json.dumps(pr))
        fp = fp_of(pr)
        prints[fp] = pr
        units.append({"scope": "rule" if zone else "entry", "rule_id": rid, "fp": fp})
    gate = reuse.check_entry(e_raw, detail["region"])["errors"]

    reach = app.entry_reaches(eid)
    verdict = reach.get("verdict") or {}
    rule_sets = {}
    if not zone and detail.get("item"):
        own = set()
        for iid in [detail["item"]["id"], *[a["id"] for a in detail.get("also_items") or []]]:
            it = reuse._registry().get(iid)
            if it:
                own |= set(it.section_ids)
        for r in e.get("rules") or []:
            v = verdict.get(r["rule_id"]) or {}
            secs = v.get("sections") or []
            if len(secs) > MAP_MAX_SECTIONS:
                rule_sets[r["rule_id"]] = {"n": len(secs), "drawn": False}
                continue
            got = {"n": len(secs), "drawn": True, "in": [s for s in secs if s in own]}
            if any(s not in own for s in secs) or r.get("tributary_excludes"):
                res = app.rule_resolved(eid, r["rule_id"], limit=MAP_MAX_SECTIONS)
                fs = (res.get("geojson") or {}).get("features") or []
                got["extra"] = [f for f in fs if f["properties"].get("kind") == "reach_extra"]
                got["excluded"] = [f for f in fs if f["properties"].get("kind") == "reach_excluded"]
            rule_sets[r["rule_id"]] = got
    the_map = None
    if detail.get("item"):
        feats = []
        for iid in [detail["item"]["id"], *[a["id"] for a in detail.get("also_items") or []]]:
            feats += app.item_geojson(iid)["features"]
        tribs = []
        wants = any(r.get("includes_tributaries") or r.get("tributaries_only") for r in e.get("rules") or []) \
            or e.get("includes_tributaries")
        if wants:
            tribs = app.item_tributary_geojson(detail["item"]["id"], limit=TRIB_LIMIT)["features"]
        named = {s for r in e.get("rules") or [] for x in r.get("extents") or []
                 for s in (x.get("splits") or [])}
        the_map = build_map(feats, rule_sets, tribs, focus_splits=named)

    rules_out = []
    for r in e.get("rules") or []:
        v = verdict.get(r["rule_id"]) or {}
        row = _slim_rule(r)
        row["binding"] = {"outcome": v.get("outcome"), "reason": v.get("reason"), "detail": v.get("detail"),
                          "n_sections": v.get("n_sections"), "tributaries_pending": v.get("tributaries_pending"),
                          "diagnostics": [_slim_diag(d) for d in v.get("diagnostics") or []][:20]}
        vr = next((x for x in view["rules"] if x["rule_id"] == r["rule_id"]), {})
        if zone:
            row["impact"] = vr.get("impact")
            row["answer"] = answer_preview(vr.get("answer"), [r], day)
        rules_out.append(row)

    return {
        "tier": t["tier"], "kind": "entry", "id": eid, "hash": t.get("hash", eid), "zone": zone,
        "name": view["name"], "display_name": view.get("display_name"), "region": view["region"],
        "risks": view.get("risks") or [], "prechecks": view.get("prechecks") or [],
        "ambiguous_cuts": view.get("ambiguous_cuts") or {}, "parse_findings": view.get("parse_findings") or [],
        "coverage": view.get("coverage"), "vintage": view["vintage"]["state"],
        "vintage_reasons": view["vintage"].get("reasons") or [],
        "regs_verbatim": e.get("regs_verbatim") or "", "source_pages": e.get("source_pages") or [],
        "symbols": e.get("symbols") or [], "includes_tributaries": e.get("includes_tributaries"),
        "scope_note": e.get("scope_note"),
        "item": detail.get("item") and {k: detail["item"][k] for k in ("id", "name", "kind")},
        "also_items": [{"id": a["id"], "name": a.get("name")} for a in detail.get("also_items") or []],
        "related": [{"entry_id": r["entry_id"], "name": r["name"], "pointer": r.get("pointer")}
                    for r in detail.get("related_entries") or []],
        "rules": rules_out,
        "licensing": [{k: x.get(k) for k in ("id", "kind", "label", "verbatim") if x.get(k)}
                      for x in e.get("licensing") or []],
        "book": book_evidence(view["evidence"], e.get("source_pages") or [], book, eid),
        "map": the_map,
        "answer": None if zone else answer_preview(view.get("answer"), e.get("rules") or [], day),
        "gate_errors": [x.get("msg", str(x)) if isinstance(x, Mapping) else str(x) for x in gate],
        "units": units,
        "marks_now": [{k: m.get(k) for k in ("scope", "rule_id", "verdict", "status", "reasons", "reviewer",
                                              "date", "note", "problem")} for m in detail.get("marks") or []],
        "pages": [p for p in e_raw.get("source_pages") or [] if isinstance(p, int)],
    }


def export_split(t: Mapping, cards: Mapping[str, Mapping], prints: dict) -> dict:
    import app
    import reuse
    from pipeline.regs.review import fingerprint as F
    sid = t["id"]
    card = cards.get(sid) or {}
    pr = json.loads(json.dumps(F.split_fingerprints(sid, reuse._review_context())))
    fp = fp_of(pr)
    prints[fp] = pr
    sp = app.get_split(sid) or {}
    the_map = None
    if card.get("item_id"):
        the_map = build_map(app.item_geojson(card["item_id"])["features"], {}, [], focus_splits=[sid])
    meta = reuse.split_meta(sid)
    return {
        "tier": t["tier"], "kind": "split", "id": sid, "hash": t.get("hash", f"split:{sid}"),
        "name": t.get("name") or sid, "label": card.get("label"), "waterbody": card.get("waterbody"),
        "split_kind": card.get("kind"), "grade": card.get("grade"), "note": card.get("note") or "",
        "item_id": card.get("item_id"), "status": card.get("status"),
        "vintage": (card.get("vintage") or {}).get("state"),
        "anchor": {k: v for k, v in (sp.get("split") or sp).items() if k in ("anchor", "kind", "label")}
        if isinstance(sp, Mapping) else None,
        "meta": {k: meta.get(k) for k in ("anchor_type", "concern", "picked_up", "offset_m") if meta.get(k)},
        "review": (sp or {}).get("review"),
        "map": the_map, "units": [{"scope": "split", "rule_id": None, "fp": fp}], "pages": [],
    }


# --------------------------------------------------------------------------------------------
# the whole export
# --------------------------------------------------------------------------------------------

def export(targets: Sequence[Mapping], out: Path, day: str, *, scale: float = PAGE_SCALE,
           quality: int = WEBP_QUALITY, log=print) -> dict:
    import app
    import review
    import reuse
    out.mkdir(parents=True, exist_ok=True)
    book = Book(out, scale, quality)
    prints: dict[str, dict] = {}
    cards = {c["split_id"]: c for c in review.split_queue(only_to_verify=False)}
    rows = []
    for i, t in enumerate(targets, 1):
        got = export_split(t, cards, prints) if t["kind"] == "split" else export_entry(t, day, book, prints)
        rows.append(got)
        log(f"  {i:2d}/{len(targets)} {t['tier']:5s} {t['id']}")
    mode = app.get_mode()
    return {
        "about": "T184: the curation review exported for the shareable page "
                 "(pipeline/regs/review/export_artifact.py). Marks made on the page come back through "
                 "pipeline/regs/review/import_artifact_marks.py.",
        "made": dt.date.today().isoformat(), "date": day,
        "vintage": reuse.built().summary(), "pdf_sha256": reuse._pdf_sha256(),
        "checks": mode["checks"], "problems": mode["problems"], "precheck_codes": mode["prechecks"],
        "commands": mode["commands"],
        "pages": {str(k): v for k, v in sorted(book.pages.items())},
        "targets": rows, "prints": prints,
    }


def main(argv: list[str] | None = None) -> int:
    from pipeline.regs.review.gate import _app
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.export_artifact",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--pilot", type=Path, required=True, help="a target list (`gate pilot --json`)")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--date", default=None, help="the day the answer is read on (default today)")
    ap.add_argument("--scale", type=float, default=PAGE_SCALE)
    ap.add_argument("--quality", type=int, default=WEBP_QUALITY)
    for k in ("marks", "entries", "splits", "build", "reach", "bundle", "release"):
        ap.add_argument(f"--{k}", type=Path, default=None)
    a = ap.parse_args(argv)
    _app(a)
    day = a.date or dt.date.today().isoformat()
    targets = json.loads(a.pilot.read_text(encoding="utf-8"))["targets"]
    got = export(targets, a.out, day, scale=a.scale, quality=a.quality)
    p = a.out / "export.json"
    p.write_text(json.dumps(got, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    imgs = sum(v["bytes"] for v in got["pages"].values())
    print(f"{p}: {p.stat().st_size / 1e6:.2f} MB, {len(got['targets'])} targets, "
          f"{len(got['prints'])} markable units; book: {len(got['pages'])} pages, {imgs / 1e6:.2f} MB "
          f"(largest {max((v['bytes'] for v in got['pages'].values()), default=0) / 1e3:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
