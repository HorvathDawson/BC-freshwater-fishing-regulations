"""Mint an FWA watershed code for an added (non-FWA) stream.

FWA watershed codes are hierarchical: a tributary's code is the RECEIVING stream's code concatenated
with a 6-digit segment giving the proportional distance UPSTREAM along the receiving stream from its
mouth (BC FWA/WSA spec). Example from the spec: Stream B joins Stream A 528.8 m up a 2,479.5 m
stream → 528.8 / 2479.5 = 0.2133 → segment ``213300`` → ``100-115004-213300``.

We reproduce that so an added stream's code is a proper prefix-descendant of the stream it flows
into — which is what makes the tributary walk, the WSC→gnis map, and the nested registry ids work.
"""

from __future__ import annotations

from pipeline.common.utils.wsc import trim_wsc

_SEG_WIDTH = 6                       # FWA code segments are 6 digits
_SEG_SCALE = 10 ** _SEG_WIDTH        # a fraction 0..1 -> a 6-digit segment
_PCT_DECIMALS = 2                    # FWA quotes the proportion to 2 decimal places of PERCENT
                                     # (spec: 528.8/2479.5 -> 21.33% -> segment 213300)


def proportion_segment(d: float, length: float) -> str:
    """6-digit FWA segment for a confluence ``d`` metres up a receiving stream of ``length`` metres.

    Per the FWA/WSA spec the proportion is taken to 2 decimal places of percent, then encoded as the
    percentage x 10000 (so 21.33% -> ``213300``, matching the spec's Stream B example). Clamped to
    ``[0, 999999]`` and zero-padded. Zero/negative length yields ``"000000"`` (degenerate; prefer an
    override)."""
    if length <= 0:
        return "0" * _SEG_WIDTH
    pct = round(d / length * 100.0, _PCT_DECIMALS)     # e.g. 21.33
    seg = round(pct * (_SEG_SCALE / 100.0))            # 21.33 -> 213300
    seg = max(0, min(_SEG_SCALE - 1, seg))             # clamp: mouth-at-source can't overflow 6 digits
    return f"{seg:0{_SEG_WIDTH}d}"


def mint_wsc(parent_wsc: str, d: float, length: float) -> str:
    """Receiving stream's (trimmed) WSC + the proportional-distance segment for a tributary joining
    it ``d`` m from its mouth (receiver total length ``length`` m). Returns a trimmed FWA code that
    prefix-descends ``parent_wsc``. Empty ``parent_wsc`` yields just the segment (no real parent)."""
    parent = trim_wsc(parent_wsc or "")
    seg = proportion_segment(d, length)
    if seg == "0" * _SEG_WIDTH:
        seg = "0" * (_SEG_WIDTH - 1) + "1"     # never a trailing all-zero group: trim_wsc would eat it
                                               # when this code is later used as a parent, breaking nesting
    return f"{parent}-{seg}" if parent else seg
