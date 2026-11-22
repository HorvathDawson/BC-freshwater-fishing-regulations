"""(T994 #27) A page whose general text will not parse tells the owner (one urgent notice), and the last text stands.

`live._read_texts` returned on any parse failure; its comment said the failure "is told", but no notice was raised for
this path (the page had typed, so nothing held it), unlike `_read_header`. Mutation: drop the notice -> fails.
"""
from __future__ import annotations

from pipeline.regs.dfo_salmon import live as L


def test_a_text_parse_failure_is_one_urgent_notice_and_keeps_the_last_text(monkeypatch):
    def boom(html, slug):
        raise ValueError("no preamble")
    monkeypatch.setattr(L, "page_texts", boom)
    reg = {"sha256": "abcdef0123456789", "texts": ["the last general text"]}
    notices: list[dict] = []
    L._read_texts("region-2", "<html/>", reg, {"texts": ["the last general text"]}, notices)
    assert notices == [{"key": "dfo-text-parse:region-2:abcdef012345", "urgent": True,
                        "text": "DFO Region region-2: the page's general text did not parse (ValueError: no preamble); "
                                "the last one stands"}]
    assert reg["texts"] == ["the last general text"]


def test_a_text_that_parses_and_did_not_change_says_nothing(monkeypatch):
    monkeypatch.setattr(L, "page_texts", lambda html, slug: ["same"])
    reg: dict = {"sha256": "x"}
    notices: list[dict] = []
    L._read_texts("region-2", "<html/>", reg, {"texts": ["same"]}, notices)
    assert notices == [] and reg["texts"] == ["same"]
