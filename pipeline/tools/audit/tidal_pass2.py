"""T212 PASS 2 — DFO's own area/boundary descriptions (owner 2026-10-28): every DFO Pacific page that
describes an area in words or coordinates near a river mouth, read for what it says about the
freshwater / tidal boundary on the FWA stream. READ ONLY; proposals only (the owner still decides).

    # 1. the page cache (raw HTML, read-only source) — fetched through web.archive.org because
    #    pac.dfo-mpo.gc.ca does not resolve from here; manifest.json carries each snapshot + date:
    #    data/source/dfo_tidal_areas/<date>/{fm-gp/..., laws-lois/SOR-2007-77-FullText.html, manifest.json}
    # 2. the entries (seconds): every description naming a river/creek/estuary/mouth, a tidal
    #    boundary, a fishing boundary sign, or carrying a lat/lon line
    .venv/bin/python -m pipeline.tools.audit.tidal_pass2 extract --cache data/source/dfo_tidal_areas/2026-10-28
    # 3. the proposals (~1-2 min, the build gpkg's streams + the basemap's roads): each entry located on
    #    the FWA line(s) it names or crosses, read for what it implies, and held against the T212
    #    candidate and the owner's tidal marks (never overriding a mark)
    .venv/bin/python -m pipeline.tools.audit.tidal_pass2 propose --basemap data/source/bc.pmtiles

WHAT AN ENTRY IMPLIES (`implies`):
  is          the words name the tidal boundary itself ("tidal boundary signs", "tidal water boundary",
              "tidal waters of X downstream of <landmark>") — the located point IS the boundary
  at_or_above a line in tidal water (a closure "shoreward of a line", a subarea / closure line, a PFMA
              subarea edge): the water at the line is tidal, so the boundary is at or above where the
              line meets the stream (a lower bound); a line seaward of the FWA mouth says only that the
              mouth is tidal (`mouth_only`)
  at_or_below a freshwater page naming a limit: the freshwater reaches down to it, so the boundary is
              at or below (an upper bound)
  context     the words name the stream but nothing locates a boundary (a restriction row, an
              unlocatable landmark)
`page_type` weighs them: pfma_regulation (law: a subarea line drawn at a mouth), tidal_area (DFO's
per-Area page: area descriptions, closures), shellfish_closure (sanitary / biotoxin lines, drawn for
shellfish, not for the boundary), freshwater, protected_area, rec_other.
"""
from __future__ import annotations

import argparse
import html as _html
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "handoff/PLAN/curation-review/tidal/pass2"
CACHE = ROOT / "data/source/dfo_tidal_areas/2026-10-28"
HOST = "https://www.pac.dfo-mpo.gc.ca"
PFMA_URL = "https://laws-lois.justice.gc.ca/eng/regulations/SOR-2007-77/FullText.html"
#: within this (m) a located point agrees with the T212 proposal / an owner mark
AGREE_M = 300.0
#: a line crossing a stream less than this (m) above its FWA mouth is drawn along the shore
ABOVE_MOUTH_M = 75.0
#: a line within this (m) of a stream's FWA mouth (not crossing it) is seaward of it
SEAWARD_M = 5000.0
#: a parallel (a latitude, drawn across the named stream) counts only where it meets the stream at the
#: tidal polygon's edge: away from it the latitude means some other water ("south of Coldwell Creek")
PARALLEL_AT_M = 300.0
#: "near (the) mouth (of the river)" — of THIS water, not "the mouth of Boulder Creek"
NEAR_MOUTH_RE = re.compile(r"(?:near|at) (?:the )?mouth(?: of (?:the )?(?:river|creek))?\b(?! of)", re.I)
#: "signs located near the mouth" puts the point within this (m) of the FWA mouth
NEAR_MOUTH_M = 1000.0
#: a crossing this far (m) outside the tidal polygon is not in the water the line describes
FAR_OUTSIDE_M = 2000.0
#: a named stream is looked for within this (m) of the entry's geometry / its PFMA area
NAME_NEAR_M = 25_000.0

KEYWORDS = re.compile(r"\b(river|creek|estuary|estuaries|mouth|slough|brook|tidal (?:water )?boundar|"
                      r"boundary sign|tidal waters? of)", re.I)
NAME_RE = re.compile(r"\b((?:Mc|Mac)?[A-Z][a-z'’]+(?:[ -](?:(?:Mc|Mac)?[A-Z][a-z'’]+|de|la))*)\s+"
                     r"(River|Creek|Slough|Brook)\b")
NOT_NAMES = {"Rivers", "The", "This", "That", "Tidal", "North", "South", "East", "West", "Upper", "Lower",
             "Fishing", "All", "Any", "Those", "Waters", "Portion", "A", "An", "Of", "From", "To", "And",
             "Its", "Of The", "At", "In", "On"}
BOUNDARY_RE = re.compile(r"(?<![a-z])tidal[ -](?:water[s]? )?(?:fishing )?boundar|"
                         r"(?<![a-z])tidal waters? of (?:the )?[A-Z][^.]{0,60}?(?:River|Creek)\b[^.]{0,120}?"
                         r"(?:downstream of|below|seaward of|from\b[^.]{0,80}?downstream)", re.I)
FRESH_RE = re.compile(r"non-?tidal|fresh ?water", re.I)

# --------------------------------------------------------------------------- coordinates

#: 52°03.86' N / 51°42.304'N / 127°52.05' west longitude / 49°10'30"N
COORD_RE = re.compile(
    r"(\d{2,3})\s*[°º]\s*(\d{1,2}(?:\.\d+)?)\s*['′’]?\s*(?:(\d{1,2}(?:\.\d+)?)\s*[\"″”]\s*)?"
    r"(?:,?\s*)(N(?:orth)?(?: latitude)?|S|W(?:est)?(?: longitude)?|E)?\b", re.I)


#: "53 degrees 59.600 minutes N" -> 53°59.600' N (same length is not kept: offsets are into the rewritten text)
DEG_WORDS = re.compile(r"(\d{2,3})\s*(?:degrees?|minutes?)\s*(\d{1,2}(?:\.\d+)?)\s*minutes?(?:\s*(\d{1,2}(?:\.\d+)?)\s*seconds?)?", re.I)


def norm_deg(text: str) -> str:
    return DEG_WORDS.sub(lambda m: f"{m.group(1)}°{m.group(2)}'" + (f"{m.group(3)}\"" if m.group(3) else ""), text)


def coords_in(text: str) -> list[dict]:
    """Every degree-minute token in order: {val (signed lon or lat), axis lat|lon, start, end}."""
    out = []
    text = DEG_WORDS.sub(lambda m: f"{m.group(1)}°{m.group(2)}'" + (f"{m.group(3)}\"" if m.group(3) else ""), text)
    for m in COORD_RE.finditer(text):
        d, mi, se, hemi = m.group(1), m.group(2), m.group(3), (m.group(4) or "").strip().lower()
        v = int(d) + float(mi) / 60 + (float(se) / 3600 if se else 0.0)
        if hemi.startswith(("n", "s")):
            axis = "lat"
        elif hemi.startswith(("w", "e")):
            axis = "lon"
        else:
            axis = "lat" if 46 <= v <= 61 else "lon" if 113 <= v <= 141 else None
        if axis is None:
            continue
        out.append({"val": -v if axis == "lon" else v, "axis": axis, "start": m.start(), "end": m.end()})
    return out


def _nad27(lonlat: list[float]) -> list[float]:
    from pyproj import Transformer
    global _T27
    try:
        _T27
    except NameError:
        _T27 = Transformer.from_crs(4267, 4326, always_xy=True)
    x, y = _T27.transform(*lonlat)
    return [round(x, 6), round(y, 6)]


def geometry_of(text: str) -> dict:
    """The entry's geometry: points (lon, lat), the straight LINES between consecutive points (a
    "following the shoreline" stretch breaks the line — it is not straight), and parallels (a lone
    latitude: "the parallel passing through 50°21.679′N"). NAD27 converted when the text says so."""
    text = norm_deg(text)
    toks = coords_in(text)
    nad27 = "NAD27" in text or "NAD 27" in text
    pts, parallels, i = [], [], 0
    while i < len(toks):
        t = toks[i]
        if t["axis"] == "lat" and i + 1 < len(toks) and toks[i + 1]["axis"] == "lon" \
                and toks[i + 1]["start"] - t["end"] < 40:
            p = [round(toks[i + 1]["val"], 6), round(t["val"], 6)]
            pts.append({"lonlat": _nad27(p) if nad27 else p, "start": t["start"], "end": toks[i + 1]["end"]})
            i += 2
            continue
        if t["axis"] == "lat":
            ctx = text[max(0, t["start"] - 80):t["start"]].lower()
            if "parallel" in ctx or "latitude" in ctx or "north of" in ctx or "south of" in ctx:
                parallels.append({"lat": round(t["val"], 6), "words": text[max(0, t["start"] - 80):t["end"] + 80]})
        i += 1
    lines, cur = [], []
    for k, p in enumerate(pts):
        if cur:
            between = text[pts[k - 1]["end"]:p["start"]].lower()
            # a stretch along the shore, or a new sentence / new line, is not a straight segment
            b2 = re.sub(r"\[[^\]]*\]", " ", between).replace("and then", "then")
            if (len(b2) > 200 or re.search(r"begin|commenc|shore|follow|\band\b|;|\.\s|parallel|meridian|except", b2)):
                if len(cur) > 1:
                    lines.append(cur)
                cur = []
        cur.append(p["lonlat"])
    if len(cur) > 1:
        lines.append(cur)
    return {"points": [p["lonlat"] for p in pts], "lines": lines, "parallels": parallels,
            "datum": "NAD27 (converted)" if nad27 else "NAD83"}


# --------------------------------------------------------------------------- the pages

class _Blocks(HTMLParser):
    """Table rows and prose blocks (p / li / dd), each under its latest heading."""
    BLOCK = {"p", "li", "dd", "dt"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.stack, self.heading, self.hid, self.summary = [], [], "", "", ""
        self.cells, self.cell, self.row_id, self.buf, self.in_head = None, None, None, None, None
        self.skip = 0
        self.ncols, self.last_first, self.th = None, None, False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag in ("script", "style"):
            self.skip += 1
        elif tag in ("h1", "h2", "h3", "h4", "summary"):
            self.in_head = (tag, a.get("id") or "", [])
        elif tag == "table":
            self.ncols, self.last_first, self.spans = None, None, {}
        elif tag == "tr":
            self.cells, self.row_id, self.th = [], a.get("id"), False
        elif tag in ("td", "th") and self.cells is not None:
            self.cell = []
            self.th = self.th or tag == "th"
            try:
                self.rowspan = max(1, int(a.get("rowspan") or 1))
            except ValueError:
                self.rowspan = 1
        elif tag in self.BLOCK and self.cells is None and self.buf is None:
            self.buf = []
        elif tag == "br":
            self.handle_data("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = max(0, self.skip - 1)
        elif self.in_head and tag == self.in_head[0]:
            t = " ".join("".join(self.in_head[2]).split())
            if tag == "summary":
                self.summary = t
            else:
                self.heading, self.hid = t, self.in_head[1]
            self.in_head = None
        elif tag in ("td", "th") and self.cell is not None and self.cells is not None:
            self.cells.append((" ".join("".join(self.cell).split()), self.rowspan))
            self.cell = None
        elif tag == "tr" and self.cells is not None:
            # ROWSPANS: a cell spanning rows below is carried into each of them at its column
            row, col, spans = [], 0, getattr(self, "spans", {})
            for text, rs in self.cells:
                while col in spans:
                    t, left = spans[col]
                    row.append(t)
                    spans[col] = (t, left - 1)
                    if left - 1 <= 0:
                        del spans[col]
                    col += 1
                row.append(text)
                if rs > 1:
                    spans[col] = (text, rs - 1)
                col += 1
            for c in sorted(k for k in spans if k >= col):
                t, left = spans[c]
                row.append(t)
                spans[c] = (t, left - 1)
                if left - 1 <= 0:
                    del spans[c]
            self.spans = spans
            self.cells = row
            if any(self.cells) and not self.th:
                self.out.append({"kind": "row", "cells": self.cells, "row_id": self.row_id,
                                 "heading": self.heading, "hid": self.hid, "summary": self.summary})
            self.cells = None
        elif tag in self.BLOCK and self.buf is not None:
            t = " ".join("".join(self.buf).split())
            if t:
                self.out.append({"kind": "prose", "cells": [t], "row_id": None, "heading": self.heading,
                                 "hid": self.hid, "summary": self.summary})
            self.buf = None

    def handle_data(self, data):
        if self.skip:
            return
        if self.in_head:
            self.in_head[2].append(data)
        if self.cell is not None:
            self.cell.append(data)
        elif self.buf is not None:
            self.buf.append(data)


def page_type(path: str) -> str:
    if "/tidal-maree/a-s" in path:
        return "tidal_area"
    if "fresh-douce" in path:
        return "freshwater"
    if "contamination" in path or "shellfish" in path:
        return "shellfish_closure"
    if "restricted" in path or "rca" in path or "protected" in path or "sponge" in path:
        return "protected_area"
    return "rec_other"


def _area_of(path: str, title: str) -> list[str]:
    m = re.search(r"a-s(\d+)", path)
    nums = re.findall(r"\b(\d{1,3})\b", title.split(":")[0]) if "area" in title.lower() else []
    out = [m.group(1)] if m else []
    return sorted({*out, *nums}, key=lambda s: int(s))


def _title(raw: str) -> str:
    m = re.search(r"<title>(.*?)</title>", raw, re.S | re.I)
    return " ".join(_html.unescape(m.group(1)).split()) if m else ""


def names_in(text: str) -> list[str]:
    """"X River" names; "Gingolx (Kincolith) River" gives both."""
    out = []
    for m in re.finditer(r"([A-Z][\w'’]+)\s+\(([A-Z][\w'’]+)\)\s+(River|Creek)\b", text):
        for nm in (m.group(1), m.group(2)):
            if f"{nm} {m.group(3)}" not in out:
                out.append(f"{nm} {m.group(3)}")
    for m in NAME_RE.finditer(text):
        nm = m.group(1)
        words = nm.split()
        while words and words[0] in NOT_NAMES:
            words = words[1:]
        if not words:
            continue
        full = f"{' '.join(words)} {m.group(2)}"
        if full not in out:
            out.append(full)
    return out


def tags_of(text: str) -> list[str]:
    t, low = [], text.lower()
    for k, pat in (("mouth", r"\bmouth"), ("estuary", r"estuar"), ("tidal_boundary", r"(?<![a-z])tidal[ -](?:water[s]? )?boundar"),
                   ("boundary_sign", r"boundary sign"), ("bridge", r"bridge"), ("shoreward", r"shoreward"),
                   ("seaward", r"seaward"), ("upstream", r"upstream|above"), ("downstream", r"downstream|below"),
                   ("non_tidal", r"non-?tidal"), ("tidal_portion", r"tidal portion")):
        if re.search(pat, low):
            t.append(k)
    return t


def implies_of(text: str, ptype: str, geo: dict) -> str:
    if re.search(r"(?:downstream|down) to the tidal boundary", text, re.I):
        return "at_or_below"        # a freshwater reach ending AT the boundary, named from above: it is below the landmark
    if BOUNDARY_RE.search(text) and not re.search(r"unless otherwise specified", text, re.I):
        return "is"
    if ptype == "freshwater" and (FRESH_RE.search(text) or "tidal" not in text.lower()):
        return "at_or_below"
    if geo["lines"] or geo["parallels"]:
        return "at_or_above"
    return "context"


def _entry(eid: str, text: str, ptype: str, src: dict, label: str | None, section: str) -> dict:
    geo = geometry_of(text)
    return {"id": eid, "page_type": ptype, "area": src.get("area") or [], "label": label, "section": section,
            "text": text, "names": names_in(f"{label or ''} {text}"),
            "primary": (names_in(label or "") or names_in(text) or [None])[0], "tags": tags_of(text),
            "implies": implies_of(text, ptype, geo), "geometry": geo,
            "region": src.get("region"),
            "sources": [{k: v for k, v in src.items() if k not in ("area", "region")}]}


def extract_pages(cache: Path) -> list[dict]:
    man = json.loads((cache / "manifest.json").read_text()) if (cache / "manifest.json").exists() else {}
    by_text: dict[str, dict] = {}
    for path, meta in sorted(man.items()):
        f = cache / path.lstrip("/")
        if meta.get("status") != 200 or not f.exists():
            continue
        raw = f.read_text(encoding="utf-8", errors="replace")
        title = _title(raw)
        ptype = page_type(path)
        p = _Blocks()
        p.feed(raw)
        area = _area_of(path, title)
        for b in p.out:
            cells = b["cells"]
            # an area-description row is (label, map, description); a closure row (id, map, description);
            # a restriction row (species, area, type, details)
            if ptype == "freshwater" and len(cells) > 2:
                # (water, where, species, dates, limit): the place is the second cell
                label, text = cells[0], (cells[1] or cells[0])
                if not cells[1]:
                    continue
            else:
                desc = max(cells, key=len)
                label = cells[0] if len(cells) > 1 and cells[0] != desc else None
                text = desc
            if not (KEYWORDS.search(f"{label or ''} {text}") or coords_in(text)):
                continue
            if len(text) < 25 and not coords_in(text):
                continue
            section = " / ".join(x for x in (b["summary"], b["heading"]) if x)
            key = re.sub(r"\s+", " ", f"{label or ''}|{text}".lower())
            src = {"url": HOST + path + (f"#{b['row_id']}" if b.get("row_id") else ""),
                   "archive_url": meta.get("archive_url"), "snapshot": meta.get("snapshot"),
                   "fetched": meta.get("fetched"), "page_title": title, "section": section,
                   "row_kind": b["kind"], "area": area}
            if key in by_text:
                e = by_text[key]
                e["area"] = sorted({*e["area"], *area}, key=lambda s: int(s) if s.isdigit() else 0)
                e["sources"].append({k: v for k, v in src.items() if k != "area"})
                if ptype != e["page_type"]:
                    e.setdefault("also_page_types", [])
                    if ptype not in e["also_page_types"]:
                        e["also_page_types"].append(ptype)
                continue
            import hashlib
            if ptype == "freshwater":
                rm = re.search(r"region(\d)", path)
                src["region"] = rm.group(1) if rm else None
            by_text[key] = _entry(f"dfo2:{hashlib.sha1(key.encode()).hexdigest()[:10]}", text, ptype, src,
                                  label, section)
    return list(by_text.values())


def extract_pfma(cache: Path) -> list[dict]:
    """The PFMA regulation (SOR/2007-77, Schedule 2): one entry per Area / Subarea definition."""
    f = cache / "laws-lois/SOR-2007-77-FullText.html"
    if not f.exists():
        return []
    s = re.sub(r"<[^>]+>", " ", f.read_text(encoding="utf-8", errors="replace"))
    s = _html.unescape(re.sub(r"\s+", " ", s))
    out = []
    parts = re.split(r"(?=\bSubarea \d{1,3}-\d{1,2} (?:Those|That|The))", s)
    for part in parts[1:]:
        m = re.match(r"Subarea (\d{1,3})-(\d{1,2}) ", part)
        body = re.split(r"\s\(b\) Area \d|\s\d{1,3} \(a\) Area \d", part)[0].strip()
        body = body[:6000]
        sub = f"{m.group(1)}-{m.group(2)}"
        src = {"url": PFMA_URL, "archive_url": None, "snapshot": None, "fetched": "2026-10-28",
               "page_title": "Pacific Fishery Management Area Regulations, 2007 (SOR/2007-77), Schedule 2",
               "section": f"Subarea {sub}", "row_kind": "regulation", "area": [m.group(1)]}
        e = _entry(f"pfma:{sub}", body, "pfma_regulation", src, f"Subarea {sub}", f"Subarea {sub}")
        out.append(e)
    return out


def _gkey(name: str) -> str:
    k = re.sub(r"[^a-z0-9 ]+", " ", name.casefold())
    k = re.sub(r"\bsouthern\b", "south", re.sub(r"\bnorthern\b", "north", k))
    k = re.sub(r"\b(of|to|the)\b", " ", k)
    return " ".join(k.split())


#: a landmark that is a POINT (a headland, a light, an entrance) — not an island, a river or a sound
POINTLIKE = re.compile(r"\b(Point|Pt\.?|Light|Head|Spit|Bluff|Rock|Rocks|Islet|entrance|Narrows|Bridge|Wharf|Dock|Breakwater)\b", re.I)


def gazetteer(ents: list[dict]) -> dict[str, list[float]]:
    """NAMED LANDMARKS WITH COORDINATES from the texts themselves: every "<lat> <lon> [Name]" (the PFMA
    regulation's style) and "Name at/located at <lat> ... <lon>"; key = normalised name -> (lon, lat),
    the first seen."""
    gz: dict[str, list[float]] = {}
    for e in ents:
        t = norm_deg(e["text"])
        toks = coords_in(t)
        pairs = []
        for i in range(len(toks) - 1):
            if toks[i]["axis"] == "lat" and toks[i + 1]["axis"] == "lon" and toks[i + 1]["start"] - toks[i]["end"] < 40:
                pairs.append((toks[i]["start"], toks[i + 1]["end"], [round(toks[i + 1]["val"], 6), round(toks[i]["val"], 6)]))
        for a, b, ll in pairs:
            m = re.match(r"\s*\[([^\]]{3,60})\]", t[b:])
            if m and POINTLIKE.search(m.group(1)) and not re.search(r"in water|mainland|shore|^near\b", m.group(1), re.I):
                gz.setdefault(_gkey(m.group(1)), ll)
            m2 = re.search(r"(?:^|[,;] |from |to )((?:[A-Z][\w'’]+ ){1,3}(?:Point|Island|Light|Head|Spit|Bluff|Rock))"
                           r"(?: on [A-Z][\w ]+?)?(?: located)? at\s*$", t[max(0, a - 80):a])
            if m2:
                gz.setdefault(_gkey(m2.group(1)), ll)
    return gz


def resolve_landmarks(e: dict, gz: dict) -> None:
    """An entry with no coordinate line: a line between the named landmarks it gives (in order), or a
    "due north/east/south/west" ray of RAY_M from one; recorded in `geometry.landmarks`."""
    g = e["geometry"]
    if g["lines"] or g["points"] or not re.search(r"\bline\b", e["text"], re.I):
        return
    key = _gkey(e["text"])
    hits = []
    for name, ll in gz.items():
        if len(name) < 6:
            continue
        i = (" " + key + " ").find(" " + name + " ")
        if i >= 0:
            hits.append((i, name, ll))
    hits.sort()
    # drop a name contained in a longer one found at the same place
    hits = [h for h in hits if not any(o is not h and h[1] in o[1] and o[0] <= h[0] < o[0] + len(o[1]) for o in hits)]
    if len(hits) >= 2:
        g["lines"] = [[h[2] for h in hits[:2]]]
        g["landmarks"] = [h[1] for h in hits[:2]]
        return
    m = re.search(r"due (north|south|east|west)", e["text"], re.I)
    if len(hits) == 1 and m:
        lon, lat = hits[0][2]
        d = RAY_M / 111_000
        dx = d / math.cos(math.radians(lat))
        end = {"north": [lon, lat + d], "south": [lon, lat - d], "east": [lon + dx, lat], "west": [lon - dx, lat]}[m.group(1).lower()]
        g["lines"] = [[[lon, lat], [round(end[0], 6), round(end[1], 6)]]]
        g["landmarks"] = [hits[0][1], f"due {m.group(1).lower()} {RAY_M:.0f} m"]


#: DFO's name -> the FWA name, where they differ
NAME_ALIASES = {"gingolx river": "Ksi Gingolx", "kincolith river": "Ksi Gingolx"}

#: a "due north to the shoreline" ray from a named landmark is drawn this long (m)
RAY_M = 3000.0


def extract(cache: Path) -> dict:
    ents = extract_pages(cache) + extract_pfma(cache)
    gz = gazetteer(ents)
    for e in ents:
        resolve_landmarks(e, gz)
        if e["geometry"].get("landmarks") and e["implies"] == "context":
            e["implies"] = "at_or_above"
    man = json.loads((cache / "manifest.json").read_text()) if (cache / "manifest.json").exists() else {}
    pages = {p: {"status": m.get("status"), "snapshot": m.get("snapshot"), "type": page_type(p)}
             for p, m in man.items()}
    return {"_about": "T212 pass 2 (pipeline/tools/audit/tidal_pass2.py extract): every DFO Pacific page "
                      "description (and SOR/2007-77 subarea definition) that names a river/creek/estuary/mouth, "
                      "a tidal boundary or a fishing boundary sign, or carries a lat/lon line; geometry parsed "
                      "(lon, lat; NAD27 converted). Pages read through web.archive.org (pac.dfo-mpo.gc.ca does "
                      "not resolve from here); raw HTML cached under data/source/dfo_tidal_areas/.",
            "cache": str(cache.relative_to(ROOT)) if cache.is_relative_to(ROOT) else str(cache),
            "pages": {"total": len(pages), "ok": sum(1 for v in pages.values() if v["status"] == 200),
                      "by_type": dict(Counter(v["type"] for v in pages.values() if v["status"] == 200))},
            "counts": {"entries": len(ents), "by_page_type": dict(Counter(e["page_type"] for e in ents)),
                       "by_implies": dict(Counter(e["implies"] for e in ents)),
                       "with_lines": sum(1 for e in ents if e["geometry"]["lines"] or e["geometry"]["parallels"])},
            "gazetteer": {"n": len(gz), "about": "named landmarks with coordinates found in the texts (PFMA brackets)"},
            "entries": ents}


# --------------------------------------------------------------------------- locating

def _albers():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True).transform


def _lonlat():
    from pyproj import Transformer
    return Transformer.from_crs(3005, 4326, always_xy=True).transform


def _ground_m(a, b) -> float:
    lat = math.radians((a[1] + b[1]) / 2)
    return math.hypot((a[0] - b[0]) * 111_320 * math.cos(lat), (a[1] - b[1]) * 110_540)


#: a straight segment longer than this (m) that runs mostly over land is a connector the text does not
#: draw (a polygon's "then to" across an island / a subarea's far corner), not a line in the water
OVERLAND_M = 1500.0


#: ids of the parallels' lines drawn this run (shapely geometries carry no attributes)
_PARALLELS: set[int] = set()


def entry_lines(e: dict, st: "Streams | None" = None, parallel_at=None):
    """The entry's straight lines in EPSG:3005, one LineString per segment; with `st`, a segment longer
    than OVERLAND_M lying mostly outside the tidal polygon is dropped (a land connector). Parallels
    are drawn only at `parallel_at` (lon of a named stream's mouth, +-0.1 deg): a lone latitude means
    nothing away from the stream the words name."""
    from shapely.geometry import LineString
    import shapely
    to = _albers()
    g = e["geometry"]
    out = []
    _PARALLELS.clear()      # ids are only meaningful while this call's lines live
    poly = st.polygon() if st is not None else None
    for ln in g["lines"]:
        xy = [to(*p) for p in ln]
        for a, b in zip(xy, xy[1:]):
            seg = LineString([a, b])
            if poly is not None and seg.length > OVERLAND_M:
                pts = [seg.interpolate(i / 20, normalized=True) for i in range(21)]
                wet = sum(bool(shapely.contains_xy(poly, p.x, p.y)) for p in pts) / len(pts)
                if wet < 0.5:
                    continue
            out.append(seg)
    if parallel_at is not None:
        for par in g["parallels"]:
            ln = LineString([to(parallel_at - 0.1, par["lat"]), to(parallel_at + 0.1, par["lat"])])
            _PARALLELS.add(id(ln))
            out.append(ln)
    return out


GENERIC_ROAD_WORDS = {"Street", "Road", "Highway", "River", "Creek", "Avenue", "Railway", "Logging", "The"}
LANDMARK_RE = re.compile(  # noqa: the named group is case-SENSITIVE (scoped flag)
    r"(?:(?P<dist>\d+(?:\.\d+)?)\s*(?P<unit>km|m|metres|meters|kilometres)\s+(?P<dir>upstream|downstream|above|below|from)\s+"
    r"(?:of\s+)?)?(?:the\s+|a\s+)?(?P<what>(?:Highway|Hwy\.?|Hwy #|Highway #)\s*#?\s*(?P<ref>\d+[A-Z]?)\s*bridge|"
    r"(?P<rail>CNR|CPR|CN|CP|railway|rail(?:road)?)\s+bridge|(?P<street>\d+)(?:st|nd|rd|th)?\s+(?:Street|Avenue|Ave\.?)\s+bridge|(?P<named>(?-i:[A-Z][\w'’]+(?:\s+[A-Z][\w'’]+)*))\s+(?:Road\s+)?[Bb]ridge|"
    r"(?:first\s+)?bridge)", re.I)


def landmark_of(text: str) -> dict | None:
    """The first bridge-like landmark the words name, with its offset: {kind road|rail, match, offset_m,
    pick, words}."""
    for m in LANDMARK_RE.finditer(text):
        kind = "rail" if m.group("rail") else "road"
        generic = not (m.group("rail") or m.group("ref") or m.group("named") or m.group("street"))
        if generic and not re.search(r"(?:near|at) (?:the )?mouth", text[m.end():m.end() + 60], re.I):
            continue          # "the bridge" / "6 mile Bridge": which one is not said
        if m.group("named") and m.group("named").split()[0] in GENERIC_ROAD_WORDS:
            continue
        match = m.group("street") or m.group("ref") or (m.group("named") if m.group("named") and m.group("named").lower() not in
                                   ("the", "a", "first", "highway") else None)
        off = 0.0
        if m.group("dist"):
            off = float(m.group("dist")) * (1000 if m.group("unit").lower().startswith("k") else 1)
            if m.group("dir").lower() in ("downstream", "below"):
                off = -off
            elif m.group("dir").lower() == "from":
                off = abs(off)      # "180 m from a bridge at the mouth": the sign is seaward/landward — unknown side
        pick = "lowest" if re.search(r"first\s+bridge|at the mouth", text[max(0, m.start() - 20):m.end() + 30], re.I) else "near"
        return {"kind": kind, "match": match, "offset_m": off, "pick": pick, "words": m.group(0)}
    return None


class Streams:
    """The build gpkg's FWA streams (read only): named blue lines near a point/line, a blue line's
    segments + geometry, its mouth."""

    def __init__(self, gpkg: Path):
        self.con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
        self._line: dict[str, tuple] = {}
        self._areas = None

    def blks_named(self, name: str, near_xy, within: float = NAME_NEAR_M) -> list[str]:
        from pipeline.tools.audit import tidal_candidates as TC
        x, y = near_xy
        rows = self.con.execute(
            "select distinct s.BLUE_LINE_KEY, s.GNIS_NAME from streams s join rtree_streams_geom r on r.id=s.fid "
            "where r.maxx>=? and r.minx<=? and r.maxy>=? and r.miny<=? and s.GNIS_NAME is not null",
            (x - within, x + within, y - within, y + within)).fetchall()
        key = TC._name_key(name)
        return [str(b) for b, n in rows if TC._name_key(n) == key]

    def line(self, blk: str):
        from pipeline.tools.audit import tidal_candidates as TC
        if blk not in self._line:
            segs = [{"fid": f, "blk": b, "down": d or 0.0, "length": ln or 0.0, "name": n, "wsc": w, "gnis": gi}
                    for f, b, d, ln, n, w, gi in self.con.execute(
                        "select fid, BLUE_LINE_KEY, DOWNSTREAM_ROUTE_MEASURE, LENGTH_METRE, GNIS_NAME, "
                        "FWA_WATERSHED_CODE, GNIS_ID from streams where BLUE_LINE_KEY=?", (int(blk),))]
            self._line[blk] = (segs, TC._geoms(self.con, segs) if segs else {})
        return self._line[blk]

    def mouth(self, blk: str):
        from pipeline.tools.audit import tidal_candidates as TC
        segs, geoms = self.line(blk)
        if not segs:
            return None, None
        m0 = min(s["down"] for s in segs)
        return m0, TC.point_at(segs, geoms, m0)

    def polygon(self):
        """The tidal polygon (the owner's edited DFO polygon, `tidal_boundary`), prepared."""
        if getattr(self, "_poly", None) is None:
            import shapely
            from pipeline.tools.audit import tidal_candidates as TC
            self._poly = TC.load_polygon(self.con)
            shapely.prepare(self._poly)
        return self._poly

    def area_polys(self) -> dict[str, object]:
        if self._areas is None:
            from pipeline.tools.audit import tidal_candidates as TC
            self._areas = {}
            for a, b in self.con.execute("select MANAGEMENT_AREA, geom from pfma_areas"):
                g = TC._gpkg_geom(b)
                k = str(a)
                self._areas[k] = g if k not in self._areas else self._areas[k].union(g)
        return self._areas

    def crossed(self, ln, above_m: float = ABOVE_MOUTH_M) -> list[dict]:
        from pipeline.tools.audit import tidal_triage as TT
        return [s for s in TT.streams_crossed(self.con, ln, above_m) if s.get("name")]


def locate(e: dict, blk: str, st: Streams, basemap, named: bool = False, primary: bool = False) -> dict:
    """Where the entry puts its point on blue line `blk`: {measure, above_mouth_m, coord, located_by,
    how: line|parallel|landmark|seaward|none}."""
    from pipeline.tools.audit import tidal_candidates as TC
    segs, geoms = st.line(blk)
    if not segs:
        return {"how": "none", "located_by": "no FWA line"}
    m0, mp = st.mouth(blk)
    ll = _lonlat()
    lines = entry_lines(e, st, ll(mp.x, mp.y)[0] if named and mp is not None else None)
    hits, seaward = [], None
    for ln in lines:
        for s in segs:
            g = geoms.get(s["fid"])
            if g is None or not g.intersects(ln):
                continue
            x = g.intersection(ln)
            for p in getattr(x, "geoms", [x]):
                q = p if p.geom_type == "Point" else p.representative_point()
                # a crossing far outside the tidal polygon is a line drawn over land, not in the water
                if st.polygon().distance(q) <= (PARALLEL_AT_M if id(ln) in _PARALLELS else FAR_OUTSIDE_M):
                    hits.append(TC._measure_at(s, g, q))
        if mp is not None:
            d = ln.distance(mp)
            if d <= SEAWARD_M and (seaward is None or d < seaward):
                seaward = d
    def out(m, how, by):
        pt = TC.point_at(segs, geoms, m)
        return {"measure": round(m, 1), "above_mouth_m": round(m - m0, 1),
                "coord": [round(v, 6) for v in ll(pt.x, pt.y)] if pt is not None else None,
                "how": how, "located_by": by}
    hits = [h for h in hits if h - m0 > ABOVE_MOUTH_M]
    if hits:
        kind = "parallel" if e["geometry"]["parallels"] and not e["geometry"]["lines"] else "line"
        return out(max(hits) if e["implies"] == "at_or_above" else min(hits), kind,
                   f"the DFO {'parallel' if kind == 'parallel' else 'line'} crosses the FWA line ({len(hits)}x)")
    lm = landmark_of(e["text"]) if primary and e["implies"] in ("is", "at_or_below") else None
    if lm and e["page_type"] == "freshwater":
        lm["pick"] = "lowest"      # a freshwater limit named by a bridge: the first one up from the sea
    if lm and basemap is not None:
        loc = TC.locate_marker(segs, geoms, basemap, lm["kind"], None, ll, match=lm["match"], pick=lm["pick"],
                               offset_m=lm["offset_m"], mouth_m=m0)
        if loc and (not lm["match"] or loc.get("named_match")):
            return out(loc["measure"], "landmark",
                       f"OSM {lm['kind']} crossing '{loc['osm']}' @ {loc['crossing_measure']:.0f} m"
                       + (f" {'+' if lm['offset_m'] > 0 else ''}{lm['offset_m']:.0f} m" if lm["offset_m"] else "")
                       + f" (words: \"{lm['words']}\")")
        if loc:
            return {**out(loc["measure"], "landmark_unnamed",
                          f"no OSM way named '{lm['match']}' crosses; nearest {lm['kind']} crossing '{loc['osm']}'"),
                    "weak": True}
    if primary and e["implies"] in ("is", "at_or_below") and NEAR_MOUTH_RE.search(e["text"]):
        return {**out(m0, "near_mouth", "\"near the mouth\": the FWA mouth, within NEAR_MOUTH_M"), "tol_m": NEAR_MOUTH_M}
    if seaward is not None:
        return {**out(m0, "seaward", f"the DFO line passes {seaward:.0f} m from the FWA mouth without crossing the "
                                     "stream: the mouth is tidal (no limit upstream)"), "mouth_only": True}
    return {"how": "none", "located_by": "nothing in the words places it on the line"}


# --------------------------------------------------------------------------- proposals

def _marks() -> dict:
    from pipeline.regs.review import marks as M
    p = ROOT / "data/curated/regulations/review/marks.jsonl"
    out = {}
    for m in M.latest(M.load(p)).values():
        if m.scope != "tidal":
            continue
        d = json.loads(m.model_dump_json())
        ns = (d.get("new_splits") or [{}])[0]
        out[m.target] = {"verdict": d.get("verdict"), "answers": d.get("answers"), "measure": ns.get("measure"),
                         "coord": ns.get("coord"), "blk": ns.get("blk"), "note": d.get("note")}
    return out


def _registry_items() -> dict[str, list[str]]:
    reg = json.loads((ROOT / "data/generated/atlas/full/registry.json").read_text())["items"]
    out: dict[str, list[str]] = defaultdict(list)
    for it in reg:
        if it["kind"] != "stream":
            continue
        for sid in it["section_ids"]:
            b = sid.split(":")[0]
            if it["id"] not in out[b]:
                out[b].append(it["id"])
    return out


def _conf(e: dict, loc: dict) -> str:
    """high: the words name the boundary and a DFO line/parallel places it; medium: the boundary named
    and placed by a landmark, or a tidal-area / PFMA / freshwater line bound; low: shellfish lines,
    seaward-only lines, weak or no placement."""
    how = loc.get("how")
    if how in ("none", None, "seaward") or loc.get("weak"):
        return "low"
    if e["implies"] == "is":
        return "high" if how in ("line", "parallel") else "medium"
    return "medium" if e["page_type"] in ("tidal_area", "pfma_regulation", "freshwater") else "low"


def relation(implies: str, p: float | None, ref: float | None, tol: float = AGREE_M) -> str | None:
    """How a located point `p` (measure) bears on a reference cut `ref`: agrees | consistent | moves."""
    if p is None or ref is None:
        return None
    if implies == "is":
        return "agrees" if abs(p - ref) <= tol else "moves"
    if implies == "at_or_above":
        return ("agrees" if abs(p - ref) <= tol else "consistent") if ref >= p - tol else "moves"
    if implies == "at_or_below":
        return ("agrees" if abs(p - ref) <= tol else "consistent") if ref <= p + tol else "moves"
    return None


def streams_for(e: dict, st: Streams, cands_by_blk: dict, cands_by_name: dict) -> list[tuple[str, str]]:
    """(blk, why) of every stream the entry bears on: the named ones (disambiguated by geometry or the
    entry's PFMA area) and every named blue line its lines cross above the mouth."""
    from pipeline.tools.audit import tidal_candidates as TC
    to = _albers()
    out: dict[str, str] = {}
    lines = entry_lines(e, st)
    ref_pts = [to(*p) for p in e["geometry"]["points"]]
    areas = st.area_polys()
    apolys = [areas[a] for a in e.get("area") or [] if a in areas]
    # a freshwater row is about ITS water; a boundary named by a landmark is the first stream's
    names = [e["primary"]] if (e["page_type"] == "freshwater" or e["implies"] == "is") and e.get("primary") else e["names"]
    for nm in names:
        key = TC._name_key(NAME_ALIASES.get(TC._name_key(nm), nm))
        pool = [c for c in cands_by_name.get(key, [])]
        best = None
        for c in pool:
            mxy = to(*c["mouth"]["coord"])
            if ref_pts:
                d = min(math.hypot(mxy[0] - x, mxy[1] - y) for x, y in ref_pts)
            elif apolys:
                from shapely.geometry import Point
                d = min(a.distance(Point(*mxy)) for a in apolys)
            else:
                d = None
            if d is not None and d <= (NAME_NEAR_M if ref_pts else 3000) and (best is None or d < best[0]):
                best = (d, c["blk"])
        if best is None and e.get("region"):
            # a freshwater region page: the candidate of that name in one of the region's MUs
            inreg = [c for c in pool if any(str(z).split("-")[0] == e["region"] for z in c.get("zones") or [])]
            if len(inreg) == 1:
                best = (0.0, inreg[0]["blk"])
        if best:
            out.setdefault(best[1], f"names {nm}")
            continue
        if e.get("region"):
            continue
        # not a candidate: an FWA blue line of that name near the entry
        anchor = ref_pts[0] if ref_pts else (apolys[0].centroid.coords[0] if apolys else None)
        if anchor is None:
            continue
        found = st.blks_named(NAME_ALIASES.get(TC._name_key(nm), nm), anchor, NAME_NEAR_M if ref_pts else 60_000)
        if apolys and not ref_pts:
            from shapely.geometry import Point
            keep = []
            for b in found:
                _m0, mp = st.mouth(b)
                if mp is not None and min(a.distance(mp) for a in apolys) <= 3000:
                    keep.append(b)
            found = keep
        # one per name: the longest-mouthed nearest — prefer a 9xx ocean mainstem
        if found:
            found.sort(key=lambda b: (b not in cands_by_blk, len(st.line(b)[0]) == 0, -len(st.line(b)[0])))
            out.setdefault(found[0], f"names {nm} (FWA name match, not a T212 candidate)")
    for ln in lines:
        for s in st.crossed(ln):
            out.setdefault(s["blk"], f"its line crosses {s['name']} {s['crossing_m']:.0f} m up the FWA line")
    return list(out.items())


def propose(entries_doc: dict, gpkg: Path, basemap_path: Path | None) -> dict:
    from pipeline.tools.audit import tidal_candidates as TC
    cands = json.loads((ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json").read_text())["candidates"]
    by_blk = {str(c["blk"]): c for c in cands}
    by_name: dict[str, list] = defaultdict(list)
    for c in cands:
        by_name[TC._name_key(c["water"])].append(c)
    marks = _marks()
    items = _registry_items()
    bm = TC.Basemap(basemap_path) if basemap_path and basemap_path.exists() else None
    st = Streams(gpkg)
    ll = _lonlat()
    per_stream: dict[str, list] = defaultdict(list)
    for e in entries_doc["entries"]:
        if e["implies"] == "context" and not e["names"]:
            continue
        for blk, why in streams_for(e, st, by_blk, by_name):
            loc = locate(e, blk, st, bm, named=why.startswith("names"),
                         primary=bool(e.get("primary")) and why.startswith(f"names {e['primary']}"))
            e.setdefault("streams", []).append({"blk": blk, "why": why, "how": loc.get("how"),
                                                "above_mouth_m": loc.get("above_mouth_m")})
            per_stream[blk].append({"entry": e["id"], "page_type": e["page_type"], "area": e["area"],
                                    "label": e["label"], "implies": e["implies"], "why": why,
                                    "text": e["text"][:1500], "url": e["sources"][0]["url"],
                                    "archive_url": e["sources"][0].get("archive_url"),
                                    "n_pages": len(e["sources"]), "located": loc,
                                    "confidence": _conf(e, loc)})
    props = []
    for blk, evs in per_stream.items():
        c = by_blk.get(blk)
        segs, _g = st.line(blk)
        name = c["water"] if c else next((s["name"] for s in segs if s["name"]), None)
        if not name:
            continue
        # a stream with no T212 candidate is new only where a DFO line in tidal water crosses it (a
        # freshwater row or a bare name inland is not a tidal stream)
        if c is None and not any(ev["located"].get("how") in ("line", "parallel") and ev["page_type"] != "freshwater"
                                 for ev in evs):
            continue
        m0, mp = st.mouth(blk)
        t212 = (c or {}).get("proposed", {}).get("measure")
        mk = marks.get(c["id"]) if c else None
        mk_m = mk.get("measure") if mk and str(mk.get("blk") or blk) == blk else None
        # a seaward-only line or a weak locate says nothing about where on the line: context
        for ev in evs:
            lo = ev["located"]
            eff = ("context" if lo.get("how") in ("none", None) or lo.get("mouth_only") or lo.get("weak")
                   else ev["implies"])
            ev["effective"] = eff
            tol = lo.get("tol_m") or AGREE_M
            ev["vs_t212"] = relation(eff, lo.get("measure"), t212, tol) if eff != "context" else None
            ev["vs_mark"] = relation(eff, lo.get("measure"), mk_m, tol) if eff != "context" else None
        located = [ev for ev in evs if ev["effective"] != "context"]
        rank = {"high": 0, "medium": 1, "low": 2}
        is_ev = sorted([ev for ev in located if ev["effective"] == "is"], key=lambda v: rank[v["confidence"]])
        lows = [ev for ev in located if ev["effective"] == "at_or_above"]
        ups = [ev for ev in located if ev["effective"] == "at_or_below"]
        lb = max(lows, key=lambda v: v["located"]["measure"]) if lows else None
        ub = min(ups, key=lambda v: v["located"]["measure"]) if ups else None
        if is_ev:
            pick, basis = is_ev[0], "the words name the tidal boundary here"
        elif lb and t212 is not None and t212 < lb["located"]["measure"] - AGREE_M:
            pick, basis = lb, "tidal water reaches at least this far (a DFO line in tidal water crosses here): raise the cut to it, or above"
        elif ub and t212 is not None and t212 > ub["located"]["measure"] + AGREE_M:
            pick, basis = ub, "a DFO freshwater limit sits here: the cut can be no higher"
        elif lb and t212 is None:
            pick, basis = lb, "tidal water reaches at least this far (no T212 candidate to compare)"
        else:
            pick, basis = (lb or ub or (located[0] if located else None)), "consistent with the T212 proposal"
        conflicts = [ev["entry"] for ev in located if ev.get("vs_mark") == "moves"]
        if c is None:
            status = "new"
        elif pick is None:
            status = "context"
        else:
            rels = [ev["vs_t212"] for ev in located]
            status = ("moves" if "moves" in rels else "agrees" if "agrees" in rels
                      else "consistent" if "consistent" in rels else "context")
        pr = None
        if pick is not None:
            lo = pick["located"]
            pr = {"measure": lo["measure"], "above_mouth_m": lo["above_mouth_m"], "coord": lo["coord"],
                  "located_by": lo["located_by"], "implies": pick["effective"], "entry": pick["entry"],
                  "confidence": pick["confidence"], "basis": basis, "source_text": pick["text"],
                  "url": pick["url"], "page_type": pick["page_type"]}
        elif c is None and mp is not None:
            pr = {"measure": round(m0, 1), "above_mouth_m": 0.0,
                  "coord": [round(v, 6) for v in ll(mp.x, mp.y)], "located_by": "the FWA mouth (nothing located)",
                  "implies": "context", "entry": evs[0]["entry"], "confidence": "low",
                  "basis": "DFO names this stream at tidal water; nothing places the boundary: the FWA mouth",
                  "source_text": evs[0]["text"], "url": evs[0]["url"], "page_type": evs[0]["page_type"]}
        props.append({
            "id": c["id"] if c else f"tidal:{blk}", "water": name, "blk": blk,
            "in_t212": c is not None, "status": status,
            "mark_conflict": bool(conflicts), "mark_conflict_entries": conflicts,
            "t212": ({"measure": t212, "above_mouth_m": c["proposed"].get("above_mouth_m"),
                      "confidence": c["confidence"], "triage": (c.get("triage") or {}).get("class")} if c else None),
            "owner_mark": mk, "proposed": pr,
            "mouth": {"measure": m0, "coord": [round(v, 6) for v in ll(mp.x, mp.y)] if mp is not None else None},
            "items": (c or {}).get("items") or items.get(blk, []),
            "wsc": (c or {}).get("wsc") or next((s["wsc"] for s in segs if s["wsc"]), None),
            "gnis_id": (c or {}).get("gnis_id") or next((s["gnis"] for s in segs if s["gnis"]), None),
            "evidence": evs})
    order = {"moves": 0, "new": 1, "agrees": 2, "consistent": 3, "context": 4}
    props.sort(key=lambda p: (not p["mark_conflict"], order.get(p["status"], 9), p["water"]))
    # a new stream nobody names is only interesting when DFO words put tidal water on it
    by_area = defaultdict(Counter)
    for e in entries_doc["entries"]:
        for a in e["area"] or ["?"]:
            by_area[a][e["page_type"]] += 1
    return {"_about": "T212 pass 2 PROPOSALS, NOT CURATED DATA (pipeline/tools/audit/tidal_pass2.py propose): per FWA "
                      "stream, what DFO's own area/closure/PFMA descriptions imply for its tidal boundary. `status` "
                      "vs the T212 candidate: agrees (within 300 m) | consistent (a bound the T212 cut satisfies) | "
                      "moves (the T212 cut breaks it: `proposed` is where it should go, or at least go) | new (no T212 "
                      "candidate) | context (named, not located). `mark_conflict`: a located entry the owner's tidal "
                      "mark breaks — listed, never overridden. `evidence` keeps every entry with its location and "
                      "`implies` (is / at_or_above / at_or_below / context).",
            "counts": {"streams": len(props), "by_status": dict(Counter(p["status"] for p in props)),
                       "mark_conflicts": sum(1 for p in props if p["mark_conflict"]),
                       "by_confidence": dict(Counter((p["proposed"] or {}).get("confidence") for p in props)),
                       "entries_by_area": {a: dict(v) for a, v in sorted(by_area.items(), key=lambda kv: (not kv[0].isdigit(), int(kv[0]) if kv[0].isdigit() else 0))}},
            "proposals": props}


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a1 = sub.add_parser("extract")
    a1.add_argument("--cache", type=Path, default=CACHE)
    a1.add_argument("--out", type=Path, default=OUT)
    a2 = sub.add_parser("propose")
    a2.add_argument("--gpkg", type=Path, default=ROOT / "data/source/bc_fisheries_data.gpkg")
    a2.add_argument("--basemap", type=Path, default=None)
    a2.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    if a.cmd == "extract":
        doc = extract(a.cache)
        (a.out / "entries.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
        print(json.dumps({"pages": doc["pages"], "counts": doc["counts"]}, indent=1))
    else:
        ents = json.loads((a.out / "entries.json").read_text())
        doc = propose(ents, a.gpkg, a.basemap)
        (a.out / "entries.json").write_text(json.dumps(ents, indent=1, ensure_ascii=False) + "\n")
        (a.out / "proposals.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
        print(json.dumps(doc["counts"], indent=1))


if __name__ == "__main__":
    main()
