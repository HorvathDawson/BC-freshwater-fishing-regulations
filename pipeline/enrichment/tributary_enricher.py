"""Phase 3 — Tributary enrichment via graph BFS.

Port of v1 TributaryEnricher using igraph upstream traversal.
Walks the FWA stream network graph to find upstream tributaries
for regulations that include tributaries.

Key concepts:
    - **Stream seeds**: linear_feature_ids used directly as BFS start edges.
    - **Lake seeds**: waterbody_key → outlet stream fids → BFS start edges.
    - **Excluded WSCs**: mainstem watershed codes excluded to prevent
      backtracking on the seed stream itself.
    - **Lake barriers**: stop at next regulated lake to avoid over-propagation.
    - **Edge type 2300**: include but don't traverse through non-matching types.
"""

from __future__ import annotations

import logging
import pickle
from collections import deque, defaultdict
from pathlib import Path
from typing import Dict, FrozenSet, List, Set, Tuple

from tqdm import tqdm

from pipeline.atlas.freshwater_atlas import FreshWaterAtlas
from pipeline.utils.wsc import trim_wsc

from .models import FeatureAssignment, ResolvedRegulation

logger = logging.getLogger(__name__)

# Edge types to include but stop traversal beyond
EXCLUDED_EDGE_TYPES: frozenset = frozenset({"2300"})


class TributaryEnricherV2:
    """Graph-based tributary enrichment for the v2 pipeline.

    Loads the igraph from pickle, builds O(1) lookup indices, and
    provides BFS upstream traversal with caching.
    """

    def __init__(self, graph_path: Path) -> None:
        logger.info("Loading graph from %s", graph_path)
        with open(graph_path, "rb") as f:
            graph_data = pickle.load(f)

        self.graph = graph_data["graph"]
        logger.info(
            "Graph loaded: %s nodes, %s edges",
            f"{self.graph.vcount():,}",
            f"{self.graph.ecount():,}",
        )

        # Build lookup indices
        self.linear_feature_id_to_edge_idx: Dict[str, int] = {}
        self.waterbody_key_to_edge_indices: Dict[str, List[int]] = defaultdict(list)
        self.reverse_adj: Dict[int, List[int]] = defaultdict(list)

        for edge in self.graph.es:
            lfid = edge["linear_feature_id"]
            if lfid:
                self.linear_feature_id_to_edge_idx[str(lfid)] = edge.index

            wbk = edge["waterbody_key"]
            if wbk:
                self.waterbody_key_to_edge_indices[str(wbk)].append(edge.index)

            self.reverse_adj[edge.target].append(edge.index)

        logger.info(
            "  Indexed %s linear features, %s waterbody keys, %s reverse-adj nodes",
            f"{len(self.linear_feature_id_to_edge_idx):,}",
            f"{len(self.waterbody_key_to_edge_indices):,}",
            f"{len(self.reverse_adj):,}",
        )

        # BFS result cache: (frozenset(seeds), frozenset(excl_wsc), frozenset(excl_wbk)) → fids
        self._cache: Dict[Tuple[FrozenSet, FrozenSet, FrozenSet], Set[str]] = {}
        self._stats = {"requests": 0, "cache_hits": 0, "tributaries_found": 0}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_tributaries(
        self,
        resolved: List[ResolvedRegulation],
        assignments: FeatureAssignment,
        atlas: FreshWaterAtlas,
    ) -> None:
        """Expand assignments with tributary fids for includes_tributaries regs.

        Mutates ``assignments`` in-place (phase=3).

        For each ResolvedRegulation with includes_tributaries=True:
        1. Stream seeds → BFS upstream excluding mainstem WSCs
        2. Lake seeds → BFS upstream from lake outlets, excluding lake barriers
        """
        tribs_regs = [r for r in resolved if r.includes_tributaries]
        if not tribs_regs:
            logger.info("Phase 3: no regulations with includes_tributaries — skipping")
            return

        logger.info(
            "Phase 3: enriching tributaries for %d / %d regulations",
            len(tribs_regs),
            len(resolved),
        )

        # Pre-compute lake barrier set: all regulated lake waterbody_keys
        lake_barrier_wbks = self._build_lake_barriers(resolved, atlas)

        for res in tqdm(tribs_regs, desc="  Phase 3: tributaries", leave=False):
            reg_id = res.record.reg_id

            # --- Stream seeds ---
            if res.tributary_stream_seeds:
                # Exclude mainstem WSCs so BFS doesn't backtrack
                excluded_wscs = self._get_seed_wscs(res.tributary_stream_seeds, atlas)

                trib_fids = self._bfs_from_seeds(
                    linear_feature_ids=list(res.tributary_stream_seeds),
                    excluded_watershed_codes=excluded_wscs,
                    excluded_waterbody_keys=frozenset(),
                )
                for fid in trib_fids:
                    assignments.assign_fid(fid, reg_id, phase=3)

            # --- Lake seeds ---
            for wbk, outlet_fids in res.lake_outlet_fids:
                # No WSC exclusion — side tributaries share WSCs with the
                # under-lake edges, so excluding them would kill all results.
                trib_fids = self._bfs_from_seeds(
                    linear_feature_ids=list(outlet_fids),
                    excluded_watershed_codes=frozenset(),
                    excluded_waterbody_keys=lake_barrier_wbks - {wbk},
                )
                for fid in trib_fids:
                    assignments.assign_fid(fid, reg_id, phase=3)

        logger.info(
            "Phase 3 complete: %d requests, %d cache hits, %d tributary fids added",
            self._stats["requests"],
            self._stats["cache_hits"],
            self._stats["tributaries_found"],
        )

    # ------------------------------------------------------------------
    # Internal: BFS
    # ------------------------------------------------------------------

    def _bfs_from_seeds(
        self,
        linear_feature_ids: List[str],
        excluded_watershed_codes: FrozenSet[str],
        excluded_waterbody_keys: FrozenSet[str],
    ) -> Set[str]:
        """BFS upstream from seed edges.  Returns tributary fids (excludes seeds)."""
        self._stats["requests"] += 1

        # Resolve seeds to edge indices
        seed_edges: List[int] = []
        for lfid in linear_feature_ids:
            idx = self.linear_feature_id_to_edge_idx.get(str(lfid))
            if idx is not None:
                seed_edges.append(idx)

        if not seed_edges:
            return set()

        # Cache check
        cache_key = (
            frozenset(seed_edges),
            excluded_watershed_codes,
            excluded_waterbody_keys,
        )
        if cache_key in self._cache:
            self._stats["cache_hits"] += 1
            return self._cache[cache_key]

        # BFS
        tributary_edges = self._traverse_upstream(
            seed_edges, excluded_watershed_codes, excluded_waterbody_keys
        )

        # Convert edge indices → fids
        result: Set[str] = set()
        for edge_idx in tributary_edges:
            lfid = self.graph.es[edge_idx]["linear_feature_id"]
            if lfid:
                result.add(str(lfid))

        self._cache[cache_key] = result
        self._stats["tributaries_found"] += len(result)
        return result

    def _traverse_upstream(
        self,
        seed_edges: List[int],
        excluded_wscs: FrozenSet[str],
        excluded_wbks: FrozenSet[str],
    ) -> Set[int]:
        """BFS upstream from seed edges.  Same algorithm as v1.

        Returns set of tributary edge indices (seeds excluded).
        """
        tributaries: Set[int] = set()
        visited: Set[int] = set()
        queue: deque = deque()

        for edge_idx in seed_edges:
            queue.append(edge_idx)
            visited.add(edge_idx)

        while queue:
            edge_idx = queue.popleft()
            edge = self.graph.es[edge_idx]

            current_edge_type = edge["edge_type"]
            if not current_edge_type:
                raise ValueError(f"Edge {edge.index} missing edge_type attribute")
            current_edge_type = str(current_edge_type)
            current_is_excluded_type = current_edge_type in EXCLUDED_EDGE_TYPES

            source_node = edge.source
            for upstream_idx in self.reverse_adj.get(source_node, []):
                if upstream_idx in visited:
                    continue

                upstream_edge = self.graph.es[upstream_idx]
                visited.add(upstream_idx)

                # Check waterbody_key exclusions (lake barriers)
                upstream_wbk = str(upstream_edge["waterbody_key"] or "")
                if upstream_wbk and upstream_wbk in excluded_wbks:
                    continue

                # Check watershed code exclusions (mainstem avoidance)
                upstream_wsc = trim_wsc(str(upstream_edge["fwa_watershed_code"] or ""))
                if upstream_wsc and upstream_wsc in excluded_wscs:
                    continue

                # Edge type transition handling
                upstream_type = upstream_edge["edge_type"]
                if not upstream_type:
                    raise ValueError(f"Edge {upstream_idx} missing edge_type attribute")
                upstream_type = str(upstream_type)
                upstream_is_excluded_type = upstream_type in EXCLUDED_EDGE_TYPES
                if current_is_excluded_type and not upstream_is_excluded_type:
                    continue

                tributaries.add(upstream_idx)
                queue.append(upstream_idx)

        return tributaries

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_parent_wscs(wsc: str) -> Set[str]:
        """Return parent watershed codes by zeroing sections right-to-left.

        Prevents BFS from traversing onto a parent stream (e.g. the Fraser
        mainstem) and then back down into unrelated tributaries.

        Example::

            "100-123456-789000" → {"100-123456", "100"}

        Works on trimmed WSCs (no trailing ``-000000`` padding).
        """
        if not wsc:
            return set()
        sections = wsc.split("-")
        parents: Set[str] = set()
        for i in range(len(sections) - 1, 0, -1):
            if sections[i] != "000000":
                parent = "-".join(sections[:i])
                parents.add(parent)
        return parents

    def _get_seed_wscs(
        self, seed_fids: tuple, atlas: FreshWaterAtlas
    ) -> FrozenSet[str]:
        """Get watershed codes for seed fids to exclude during BFS.

        Includes both the exact seed WSCs and all parent WSCs so that
        BFS never climbs onto a parent stream and leaks into unrelated
        tributaries.
        """
        wscs: Set[str] = set()
        for fid in seed_fids:
            rec = atlas.streams.get(str(fid))
            if rec and rec.fwa_watershed_code:
                wscs.add(rec.fwa_watershed_code)
                wscs.update(self._get_parent_wscs(rec.fwa_watershed_code))
        return frozenset(wscs)

    def _build_lake_barriers(
        self,
        resolved: List[ResolvedRegulation],
        atlas: FreshWaterAtlas,
    ) -> FrozenSet[str]:
        """Collect waterbody_keys of all regulated lakes.

        Used as barriers during lake-seeded BFS to prevent traversing
        through another lake that has its own regulations.
        """
        barrier_wbks: Set[str] = set()
        for res in resolved:
            for wbk in res.matched_waterbody_keys:
                if wbk in atlas.lakes:
                    barrier_wbks.add(wbk)
            # tributary_only regs have empty matched_waterbody_keys but
            # still reference lakes via lake_outlet_fids — include those.
            for wbk, _ in res.lake_outlet_fids:
                if wbk in atlas.lakes:
                    barrier_wbks.add(wbk)
        return frozenset(barrier_wbks)
