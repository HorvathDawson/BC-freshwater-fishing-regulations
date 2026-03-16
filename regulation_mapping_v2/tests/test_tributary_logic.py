"""Tests for tributary assignment logic — Phase 2 + Phase 3 integration.

Uses a minimal placeholder graph and mock atlas to verify:
- Normal regulation: assigns to matched features only
- includes_tributaries: assigns to matched features + BFS upstream
- tributary_only: assigns ONLY to BFS-discovered tributaries, NOT the lake
"""

from __future__ import annotations

import pickle
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Dict

import igraph
import pytest

from regulation_mapping_v2.enrichment.models import (
    FeatureAssignment,
    RegulationRecord,
    ResolvedRegulation,
)
from regulation_mapping_v2.enrichment.tributary_enricher import TributaryEnricherV2
from regulation_mapping_v2.matching.match_table import BaseEntry
from regulation_mapping_v2.matching.reg_models import MatchCriteria


# ===================================================================
# Tiny graph topology (5 nodes, 4 edges)
#
#  FWA igraph convention: source=upstream, target=downstream.
#  Water flows from source → target.
#  reverse_adj[edge.target] = edges arriving at that downstream node.
#  BFS uses reverse_adj[edge.source] to find edges further upstream.
#
#  Physical layout (water flows left to right):
#       n4 → n3 → n2 → n1 → n0
#    (trib)  (jct) (creek) (lake) (outlet)
#
#  Edges (source=upstream, target=downstream):
#       e0: (n1, n0) fid="100"  downstream of lake
#       e1: (n2, n1) fid="200"  under-lake stream, wbk=WBK_LAKE
#       e2: (n3, n2) fid="300"  seed creek
#       e3: (n4, n3) fid="400"  upstream tributary
#
# ===================================================================


def _build_graph() -> igraph.Graph:
    """Build a tiny 5-node, 4-edge directed graph."""
    g = igraph.Graph(n=5, directed=True)

    # Edges: (source=upstream, target=downstream)
    g.add_edges([(1, 0), (2, 1), (3, 2), (4, 3)])

    g.es["linear_feature_id"] = ["100", "200", "300", "400"]
    g.es["waterbody_key"] = ["", "WBK_LAKE", "", ""]
    g.es["fwa_watershed_code"] = ["WSC_MAIN", "WSC_LAKE", "WSC_SEED", "WSC_TRIB"]
    g.es["edge_type"] = ["1000", "1000", "1000", "1000"]
    g.es["gnis_id"] = ["", "", "GNIS_CREEK", ""]

    return g


@pytest.fixture()
def enricher(tmp_path: Path) -> TributaryEnricherV2:
    """Pickle the tiny graph and load an enricher from it."""
    graph_path = tmp_path / "test_graph.pkl"
    with open(graph_path, "wb") as f:
        pickle.dump({"graph": _build_graph()}, f)
    return TributaryEnricherV2(graph_path)


@pytest.fixture()
def mock_atlas() -> SimpleNamespace:
    """Minimal atlas-like object with streams, under_lake_streams, lakes."""
    # StreamRecord-like objects — only need fwa_watershed_code for _get_seed_wscs
    def _stream(fid: str, wsc: str, wbk: str = "") -> SimpleNamespace:
        return SimpleNamespace(
            fid=fid,
            fwa_watershed_code=wsc,
            waterbody_key=wbk,
            blk="BLK1",
        )

    streams = {
        "100": _stream("100", "WSC_MAIN"),
        "300": _stream("300", "WSC_SEED"),
        "400": _stream("400", "WSC_TRIB"),
    }
    under_lake_streams = {
        "200": _stream("200", "WSC_LAKE", wbk="WBK_LAKE"),
    }
    lakes = {
        "WBK_LAKE": SimpleNamespace(waterbody_key="WBK_LAKE"),
    }

    return SimpleNamespace(
        streams=streams,
        under_lake_streams=under_lake_streams,
        lakes=lakes,
        manmade={},
    )


def _make_record(reg_id: str, **parsed_overrides) -> RegulationRecord:
    """Build a minimal RegulationRecord."""
    parsed = {
        "includes_tributaries": False,
        "tributary_only": False,
        **parsed_overrides,
    }
    return RegulationRecord(
        index=0,
        reg_id=reg_id,
        water="TEST CREEK",
        region="Region 1",
        mu=("1-1",),
        raw_regs="Test",
        symbols=(),
        page=1,
        image="p1.png",
        match_entry=BaseEntry(
            criteria=MatchCriteria(
                name_verbatim="TEST CREEK", region="Region 1", mus=["1-1"]
            ),
            gnis_ids=["GNIS_CREEK"],
            link_method="natural_search",
        ),
        parsed=parsed,
        parse_status="success",
    )


# ===================================================================
# Tests
# ===================================================================


class TestTributaryAssignment:
    """Verify Phase 2 assignment + Phase 3 BFS for three regulation types."""

    def test_normal_no_tributaries(self, enricher, mock_atlas):
        """Normal reg: assigned to matched fids/wbks only, no BFS."""
        record = _make_record("REG_NORMAL")
        assignments = FeatureAssignment()

        # Phase 2: assign directly to the seed stream
        assignments.assign_fid("300", "REG_NORMAL", phase=2)

        resolved = [
            ResolvedRegulation(
                record=record,
                matched_stream_fids=frozenset({"300"}),
                matched_waterbody_keys=frozenset(),
                includes_tributaries=False,
                tributary_only=False,
                tributary_stream_seeds=("300",),
                lake_outlet_fids=(),
            )
        ]

        # Phase 3: should be a no-op (includes_tributaries=False)
        enricher.enrich_tributaries(resolved, assignments, mock_atlas)

        assert "300" in assignments.fid_to_reg_ids
        assert "REG_NORMAL" in assignments.fid_to_reg_ids["300"]
        # Tributaries should NOT have the reg
        assert "REG_NORMAL" not in assignments.fid_to_reg_ids.get("400", set())
        # Lake should NOT have the reg
        assert "REG_NORMAL" not in assignments.wbk_to_reg_ids.get("WBK_LAKE", set())

    def test_includes_tributaries(self, enricher, mock_atlas):
        """includes_tributaries: assigned to matched features + BFS upstream."""
        record = _make_record("REG_INCL_TRIBS", includes_tributaries=True)
        assignments = FeatureAssignment()

        # Phase 2: assign directly to the seed stream
        assignments.assign_fid("300", "REG_INCL_TRIBS", phase=2)

        resolved = [
            ResolvedRegulation(
                record=record,
                matched_stream_fids=frozenset({"300"}),
                matched_waterbody_keys=frozenset(),
                includes_tributaries=True,
                tributary_only=False,
                tributary_stream_seeds=("300",),
                lake_outlet_fids=(),
            )
        ]

        # Phase 3: BFS upstream from seed "300" should find "400"
        enricher.enrich_tributaries(resolved, assignments, mock_atlas)

        # Seed stream has reg (from Phase 2)
        assert "REG_INCL_TRIBS" in assignments.fid_to_reg_ids["300"]
        # Tributary discovered by BFS
        assert "REG_INCL_TRIBS" in assignments.fid_to_reg_ids.get("400", set())
        # Lake should NOT have the reg (stream-seeded, not lake-seeded)
        assert "REG_INCL_TRIBS" not in assignments.wbk_to_reg_ids.get(
            "WBK_LAKE", set()
        )

    def test_tributary_only_lake_seeds(self, enricher, mock_atlas):
        """tributary_only: NOT assigned to lake, only BFS-discovered tribs.

        This tests the core fix: when tributary_only=True, Phase 2 skips
        assign_wbk(), but lake_outlet_fids still seeds Phase 3 BFS.
        """
        record = _make_record(
            "REG_TRIB_ONLY", includes_tributaries=True, tributary_only=True
        )
        assignments = FeatureAssignment()

        # Phase 2: tributary_only=True → NO assign_wbk, NO assign_fid
        # (feature_resolver skips these when tributary_only is set)

        resolved = [
            ResolvedRegulation(
                record=record,
                matched_stream_fids=frozenset(),  # empty — trib_only
                matched_waterbody_keys=frozenset(),  # empty — trib_only
                includes_tributaries=True,
                tributary_only=True,
                tributary_stream_seeds=(),
                lake_outlet_fids=(("WBK_LAKE", ("200",)),),  # seeds from lake outlets
            )
        ]

        # Phase 3: BFS from lake outlet (fid=200) goes upstream → finds 300, 400
        enricher.enrich_tributaries(resolved, assignments, mock_atlas)

        # Lake polygon should NOT have the reg
        assert "REG_TRIB_ONLY" not in assignments.wbk_to_reg_ids.get(
            "WBK_LAKE", set()
        )
        # Tributaries upstream of lake SHOULD have the reg
        assert "REG_TRIB_ONLY" in assignments.fid_to_reg_ids.get("300", set())
        assert "REG_TRIB_ONLY" in assignments.fid_to_reg_ids.get("400", set())
        # Under-lake stream itself should NOT (it's a seed, seeds are excluded)
        assert "REG_TRIB_ONLY" not in assignments.fid_to_reg_ids.get("200", set())
        # Downstream stream should NOT
        assert "REG_TRIB_ONLY" not in assignments.fid_to_reg_ids.get("100", set())

    def test_includes_tributaries_lake_seeded(self, enricher, mock_atlas):
        """includes_tributaries with lake: lake gets reg + BFS finds tribs."""
        record = _make_record("REG_LAKE_TRIBS", includes_tributaries=True)
        assignments = FeatureAssignment()

        # Phase 2: direct assignment to lake
        assignments.assign_wbk("WBK_LAKE", "REG_LAKE_TRIBS", phase=2)

        resolved = [
            ResolvedRegulation(
                record=record,
                matched_stream_fids=frozenset(),
                matched_waterbody_keys=frozenset({"WBK_LAKE"}),
                includes_tributaries=True,
                tributary_only=False,
                tributary_stream_seeds=(),
                lake_outlet_fids=(("WBK_LAKE", ("200",)),),
            )
        ]

        # Phase 3: BFS from lake outlet (fid=200)
        enricher.enrich_tributaries(resolved, assignments, mock_atlas)

        # Lake has reg (from Phase 2)
        assert "REG_LAKE_TRIBS" in assignments.wbk_to_reg_ids["WBK_LAKE"]
        # Upstream tributaries found by BFS
        assert "REG_LAKE_TRIBS" in assignments.fid_to_reg_ids.get("300", set())
        assert "REG_LAKE_TRIBS" in assignments.fid_to_reg_ids.get("400", set())
