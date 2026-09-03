"""Prove the minted dataset propagates correctly before it is written.

Every added stream must resolve to a real root — an FWA stream (inherit its drainage) or a 900-style
tidal root — with its WSC prefix-descending its receiver's the whole way down. This is the guarantee
that primary drainage codes (100 Fraser, 900 Coastal, …) are inherited, never invented. `build_dataset`
refuses to write if `validate_propagation` returns any violation.

A stream record (dict) carries: ``blk, wsc, klass, receiver_kind ('fwa'|'added'|'tidal'),
receiver_blk, receiver_wsc``.
"""

from __future__ import annotations

import re
from collections import Counter

from pipeline.atlas.waters.added_streams.coastal import is_coastal_wsc
from pipeline.common.utils.wsc import trim_wsc


def _prefixes(wsc: str) -> str:
    return trim_wsc((wsc or "").strip())      # normalize padding so prefix checks align with mint_wsc


def _canon(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


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
            if wsc[:3] != rwsc[:3]:
                bad(s, f"primary code {wsc[:3]} != receiver primary {rwsc[:3]} (not inherited)")
        else:
            if not wsc.startswith(rwsc):
                bad(s, f"wsc {wsc!r} does not prefix-descend receiver wsc {rwsc!r}")
            if wsc[:3] != rwsc[:3]:
                bad(s, f"primary code {wsc[:3]} != receiver primary {rwsc[:3]} (not inherited)")

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
