"""Prove the minted dataset propagates correctly before it is written.

Every added stream must resolve to a real root — an FWA stream (inherit its drainage) or a 900-style
tidal root — with its WSC prefix-descending its receiver's the whole way down. This is the guarantee
that primary drainage codes (100 Fraser, 900 Coastal, …) are inherited, never invented. `build_dataset`
refuses to write if `validate_propagation` returns any violation.

A stream record (dict) carries: ``blk, wsc, klass, receiver_kind ('fwa'|'added'|'tidal'),
receiver_blk, receiver_wsc``.
"""

from __future__ import annotations

from collections import Counter

from pipeline.atlas.waters.added_streams.coastal import is_coastal_wsc
from pipeline.common.utils.wsc import trim_wsc
from pipeline.common.utils import wsc as _wsc
from pipeline.common.names import compact_key


def _prefixes(wsc: str) -> str:
    return trim_wsc((wsc or "").strip())      # normalize padding so prefix checks align with mint_wsc


def _canon(name: str) -> str:
    return compact_key(name)                     # the one name key, spaces dropped (T61 2.8)


def validate_propagation(streams: list[dict]) -> list[str]:
    """Return a list of human-readable violations (empty == valid)."""
    by_blk = {str(s["blk"]): s for s in streams}
    # a name carried by 2+ records is a braided/slough stream: every piece SHARES the mainstem's wsc
    # (same name => same wsc, different blk), so a braid legitimately carries a code more senior than its
    # immediate receiver's. Its code validity is proven by the mainstem member that minted it; here we
    # only require the primary drainage code be inherited and the receiver chain bottom out at a root.
    name_counts = Counter(_canon(s.get("name")) for s in streams if _canon(s.get("name")))
    violations: list[str] = []

    def bad(s, msg):
        violations.append(f"blk {s['blk']} ({s.get('name') or '?'}): {msg}")

    seen_neg: set[str] = set()
    for s in streams:
        blk, wsc, kind = str(s["blk"]), _prefixes(s["wsc"]), s["receiver_kind"]

        # blk sign vs class
        is_neg = blk.startswith("-")
        if s["klass"] == "extension" and is_neg:
            bad(s, "extension must reuse the FWA (positive) blk, got a negative blk")
        if s["klass"] in ("novel",) and not is_neg and kind != "extension":
            bad(s, "novel stream must have a minted negative blk")
        if is_neg:
            if blk in seen_neg:
                bad(s, "duplicate negative blk")
            seen_neg.add(blk)

        if kind == "tidal":
            if not is_coastal_wsc(wsc):
                bad(s, f"tidal stream must have a coastal (9xx) wsc, got {wsc!r}")
            continue

        rwsc = _prefixes(s.get("receiver_wsc", ""))
        if not rwsc:
            bad(s, f"receiver_kind={kind} but no receiver_wsc recorded")
            continue

        name_shared = name_counts.get(_canon(s.get("name")), 0) > 1   # a braid/slough sharing one wsc
        if kind == "fwa" and s["klass"] == "extension":
            if wsc != rwsc:
                bad(s, f"extension wsc {wsc!r} must equal the FWA stream's wsc {rwsc!r}")
        elif name_shared:
            # shares the mainstem's senior code; only the primary drainage code must be inherited
            if _wsc.root(wsc) != _wsc.root(rwsc):
                bad(s, f"primary code {_wsc.root(wsc)} != receiver primary {_wsc.root(rwsc)} (not inherited)")
        else:
            if not _wsc.is_at_or_below(wsc, rwsc):
                bad(s, f"wsc {wsc!r} does not prefix-descend receiver wsc {rwsc!r}")
            if _wsc.root(wsc) != _wsc.root(rwsc):
                bad(s, f"primary code {_wsc.root(wsc)} != receiver primary {_wsc.root(rwsc)} (not inherited)")

        # the receiver chain must terminate at an fwa/tidal root without cycles
        if kind == "added":
            seen: set[str] = {blk}
            cur = by_blk.get(str(s["receiver_blk"]))
            while cur is not None and cur["receiver_kind"] == "added":
                cb = str(cur["blk"])
                if cb in seen:
                    bad(s, f"connect_to cycle through blk {cb}")
                    break
                seen.add(cb)
                cur = by_blk.get(str(cur["receiver_blk"]))
            if cur is None:
                bad(s, f"receiver blk {s['receiver_blk']} not found (dangling)")
            elif cur["receiver_kind"] not in ("fwa", "tidal"):
                bad(s, "receiver chain does not bottom out at an FWA/tidal root")
    return violations


# ---- (T1246 / T1150) a city line never takes an FWA line's watershed code -------------------------------------------
# A city line that drains straight to the sea mints its code from the NEAREST coastal FWA stream
# (`coastal.estimate_coastal_wsc`) and is bumped only past other CITY codes, so it could land on an FWA river's exact
# code. The atlas groups lines by code, so the city creek then became a part of that river: Turner Creek (Port Moody)
# a part of Noons Creek, Tunnel Creek (Burnaby) of the Seymour River, Capitol Creek of "Squatters Creek". The check
# below refuses such a line BY NAME, in the atlas build (which reads the stored file) and in the generator (so a bad
# file is never written). It also refuses a tidal root whose code has an FWA line UNDER it (that line would be walked
# as the city creek's tributary).

#: (T806) the city lines that share an FWA code ON PURPOSE: the line CONTINUES that FWA line from its mouth.
FWA_CODE_CONTINUATIONS: dict[str, str] = {
    "-2400000001": "Sheldens Creek: the hand-drawn estuary channel continues FWA 360846083 from its mouth (T806)",
}

#: (T1150, open) the city lines KNOWN to share an FWA line's code, waiting for the hardening release's refile. Each is
#: named; the whole-province build refuses an entry that no longer shares a code (remove it the day it is refiled).
#: A NEW collision is never added here: it is refiled (own code, as T1246 did for Turner, Tunnel and Capitol creeks).
FWA_CODE_SHARED_PENDING: dict[str, str] = {
    "-2000000091": "Aliceville Creek (Burnaby), shares FWA 360731231's 900-059187",
    "-2000000243": "Kask's Camp Creek (Burnaby), shares FWA 360788071's 900-053873",
    "-2000000277": "Rainbow Creek (Burnaby), shares FWA 360455980's 900-076392",
    "-2100000079": "Mamquam Blind Channel Wetland (Squamish), shares FWA 360767895's 900-103611",
    "-2200000296": "Kyle Creek (Port Moody), shares School House Brook's (FWA 360728050) 900-056279",
    "-2200000395": "Village Creek (Port Moody), shares FWA 360827018's 900-058384",
    "-2200000420": "Wilkes Creek (Port Moody), shares Hett Creek's (FWA 360724731) 900-057925",
}


def fwa_code_collisions(streams: list[dict], fwa_lines, *, whole_province: bool) -> list[str]:
    """Every city line (`klass` "novel") whose code is an FWA line's code, and every tidal root with an FWA line
    under its code: one human-readable violation each (empty == clean). ``fwa_lines`` = (trimmed wsc, blue line key)
    of the FWA lines in the build, read once. Extensions share their FWA line's code by design (`validate_propagation`
    pins that), and so does a line that joins the very line whose code it carries. ``whole_province``: the FWA
    lines are all of B.C., so an exempted line that no longer shares a code is stale and refused too (an exempted blk
    the file does not hold is not judged: the mini release's invented world; the curated pin test holds them to the
    file); a bbox build sees only its own FWA lines and checks no staleness."""
    exempt = {**FWA_CODE_CONTINUATIONS, **FWA_CODE_SHARED_PENDING}
    # a line that carries the code of the very line it JOINS is a piece of that water by connection (a braid, a same-
    # name piece pinned to the FWA line it extends): not filed by an estimate. The tidal roots have no receiver code.
    novel = [s for s in streams if s.get("klass") == "novel"
             and not (s.get("receiver_kind") in ("fwa", "added") and _prefixes(s.get("receiver_wsc", "")) == _prefixes(s["wsc"]))]
    by_code: dict[str, list[dict]] = {}
    for s in novel:
        by_code.setdefault(_prefixes(s["wsc"]), []).append(s)
    roots = {_prefixes(s["wsc"]): s for s in novel if s.get("receiver_kind") == "tidal"}
    depths = sorted({len(_wsc.groups(c)) for c in roots})   # a code under a root shares its first groups, has more
    same: dict[str, set[str]] = {}                 # city blk -> FWA blks holding its exact code
    under: dict[str, set[str]] = {}                # tidal-root blk -> FWA blks coded under it
    for code, blk in fwa_lines:                    # one pass (the full build: ~4.9M FWA lines)
        for s in by_code.get(code, ()):
            same.setdefault(str(s["blk"]), set()).add(str(blk))
        if not depths:
            continue
        ngroups = len(_wsc.groups(code))
        for d in depths:
            if ngroups > d:
                r = roots.get(_wsc.head(code, d))
                if r is not None:
                    under.setdefault(str(r["blk"]), set()).add(str(blk))
    out: list[str] = []
    named = {str(s["blk"]): s for s in novel}
    for blk in sorted(set(same) | set(under)):
        if blk in exempt:
            continue
        s = named[blk]
        lines = ", ".join(sorted(same.get(blk) or under[blk])[:4])   # the first four FWA blue line keys, for the message
        where = (f"has FWA line(s) {lines}'s exact code {_prefixes(s['wsc'])}" if blk in same
                 else f"has FWA line(s) {lines} coded under its {_prefixes(s['wsc'])}")
        out.append(f"blk {blk} ({s.get('name') or '?'}, {s.get('source') or '?'}) {where}: the atlas would file it "
                   "under that FWA water. Give it a code of its own (T1246)")
    if whole_province:
        present = {str(s["blk"]) for s in streams}
        for blk, why in sorted(exempt.items()):
            if blk not in present:
                continue                            # another file (the mini release's world): not this file's line
            if blk not in same:
                out.append(f"blk {blk} ({why}) is exempted in validate.py but shares no FWA code now: remove the entry")
    return out
