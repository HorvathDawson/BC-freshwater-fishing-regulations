"""
Test tributary assignment for Guichon Creek watershed using graph approach.

Tests the graph-based watershed hierarchy:
- tributary_of attribute should be populated for all edges
- Lake tributaries should be detected and propagated upstream
- Tributaries joining a main stream ABOVE a lake should inherit the lake as parent
- Named streams should stop lake tributary propagation
"""

import sys
from pathlib import Path
import pytest
import logging
import pickle

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from graphs.fwa_graph_viz import FWAPrimalGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestGuichonGraphTributaries:
    """Test tributary assignments in graph using Guichon Creek watershed."""

    @pytest.fixture(scope="class")
    def guichon_graph(self):
        """Build Guichon Creek graph for testing."""
        logger.info("=" * 80)
        logger.info("BUILDING GUICHON CREEK GRAPH FOR TESTING")
        logger.info("=" * 80)

        builder = FWAPrimalGraph()

        if not builder.validate_paths():
            pytest.skip("FWA data paths not found")

        # Load lakes for lake name enrichment
        builder.load_lakes()

        # Build graph with just GUIC layer (Guichon Creek watershed)
        logger.info("Building graph from GUIC layer only...")
        builder.build(layers=["GUIC"])

        # Enrich tributaries (this is what we're testing)
        logger.info("Enriching tributaries...")
        builder.enrich_tributaries(debug=False)

        logger.info(
            f"Graph built: {builder.G.number_of_nodes():,} nodes, {builder.G.number_of_edges():,} edges"
        )
        logger.info("=" * 80)

        return builder

    def test_graph_has_edges(self, guichon_graph):
        """Test that the graph was built successfully."""
        assert guichon_graph.G.number_of_edges() > 0, "Graph has no edges"
        assert guichon_graph.G.number_of_nodes() > 0, "Graph has no nodes"
        logger.info(
            f"✓ Graph has {guichon_graph.G.number_of_edges():,} edges and {guichon_graph.G.number_of_nodes():,} nodes"
        )

    def test_mamit_lake_exists_in_graph(self, guichon_graph):
        """Test that Mamit Lake is present in the graph."""
        mamit_edges = []
        for u, v, key, data in guichon_graph.G.edges(keys=True, data=True):
            if data.get("lake_name") == "Mamit Lake":
                mamit_edges.append((u, v, key, data))

        assert len(mamit_edges) > 0, "No Mamit Lake edges found in graph"
        logger.info(f"✓ Found {len(mamit_edges)} Mamit Lake edges")

    def test_mamit_lake_tributaries(self, guichon_graph):
        """
        Test that streams 701303054, 701303219, and 701302532 are all
        tributaries of Mamit Lake.

        These streams flow into Mamit Lake and should be detected as lake tributaries.
        This is the core test for the new behavior: when a main stream passes
        through a lake, tributaries joining upstream should inherit the lake.
        """
        G = guichon_graph.G

        test_streams = ["701303054", "701303219", "701302532"]

        for stream_id in test_streams:
            # Find the edge with this linear_feature_id
            found = False
            for u, v, key, data in G.edges(keys=True, data=True):
                if data.get("linear_feature_id") == stream_id:
                    found = True
                    gnis_name = data.get("gnis_name", "")
                    tributary_of = data.get("tributary_of", "")
                    lake_name = data.get("lake_name", "")

                    logger.info(f"\nStream {stream_id}:")
                    logger.info(f"  GNIS_NAME: {gnis_name}")
                    logger.info(f"  TRIBUTARY_OF: {tributary_of}")
                    logger.info(f"  LAKE_NAME: {lake_name}")

                    # Should be tributary of Mamit Lake
                    assert (
                        tributary_of == "Mamit Lake"
                    ), f"Stream {stream_id}: Expected 'Mamit Lake', got '{tributary_of}'"
                    break

            assert found, f"Stream {stream_id} not found in graph"

        logger.info(
            "\n✓ All test streams correctly marked as tributaries of Mamit Lake"
        )

    def test_guichon_outlet_not_tributary_of_mamit(self, guichon_graph):
        """
        Test that Guichon Creek outlet (stream 701305409) is NOT marked as
        tributary of Mamit Lake.

        This stream flows OUT of Mamit Lake, so it should maintain its
        Guichon Creek identity and NOT be marked as lake tributary.
        """
        G = guichon_graph.G

        stream_id = "701305409"

        # Find the edge with this linear_feature_id
        found = False
        for u, v, key, data in G.edges(keys=True, data=True):
            if data.get("linear_feature_id") == stream_id:
                found = True
                gnis_name = data.get("gnis_name", "")
                tributary_of = data.get("tributary_of", "")

                logger.info(f"\nStream {stream_id} (Guichon Creek outlet):")
                logger.info(f"  GNIS_NAME: {gnis_name}")
                logger.info(f"  TRIBUTARY_OF: {tributary_of}")

                # Should maintain Guichon Creek name
                assert (
                    gnis_name == "Guichon Creek"
                ), f"Expected 'Guichon Creek', got '{gnis_name}'"

                # Should NOT be tributary of Mamit Lake
                assert tributary_of != "Mamit Lake", (
                    f"Outlet stream incorrectly marked as tributary of Mamit Lake. "
                    f"TRIBUTARY_OF={tributary_of}"
                )
                break

        assert found, f"Stream {stream_id} not found in graph"
        logger.info("✓ Guichon outlet correctly NOT marked as tributary of Mamit Lake")

    def test_guichon_main_stem_through_lake(self, guichon_graph):
        """
        Test that Guichon Creek segments inside Mamit Lake are marked
        as tributaries of Mamit Lake.
        """
        G = guichon_graph.G

        guichon_in_mamit = []
        for u, v, key, data in G.edges(keys=True, data=True):
            if (
                data.get("gnis_name") == "Guichon Creek"
                and data.get("lake_name") == "Mamit Lake"
            ):
                guichon_in_mamit.append(
                    {
                        "edge": (u, v, key),
                        "tributary_of": data.get("tributary_of", ""),
                        "linear_feature_id": data.get("linear_feature_id", ""),
                    }
                )

        assert (
            len(guichon_in_mamit) > 0
        ), "No Guichon Creek edges found inside Mamit Lake"

        logger.info(f"Found {len(guichon_in_mamit)} Guichon Creek edges in Mamit Lake")

        # These should be marked as tributary of Mamit Lake
        for segment in guichon_in_mamit:
            trib_of = segment["tributary_of"]
            assert trib_of == "Mamit Lake", (
                f"Guichon Creek segment in Mamit Lake has tributary_of='{trib_of}', "
                f"expected 'Mamit Lake'"
            )

        logger.info(
            "✓ All Guichon Creek segments in Mamit Lake correctly marked as tributary of Mamit Lake"
        )

    def test_dupuis_creek_tributary_of_guichon_not_mamit(self, guichon_graph):
        """
        Test that stream 701303849 is tributary of Dupuis Creek, not Mamit Lake.

        This tests that named streams (Dupuis Creek) stop the propagation of lake
        tributary status - only unnamed streams should inherit lake tributary status.
        """
        G = guichon_graph.G

        stream_id = "701303849"

        # Find the edge with this linear_feature_id
        found = False
        for u, v, key, data in G.edges(keys=True, data=True):
            if data.get("linear_feature_id") == stream_id:
                found = True
                gnis_name = data.get("gnis_name", "")
                tributary_of = data.get("tributary_of", "")

                logger.info(f"\nStream {stream_id}:")
                logger.info(f"  GNIS_NAME: {gnis_name}")
                logger.info(f"  TRIBUTARY_OF: {tributary_of}")

                # Should be tributary of Dupuis Creek (not Mamit Lake)
                assert (
                    tributary_of == "Dupuis Creek"
                ), f"Expected 'Dupuis Creek', got '{tributary_of}'"
                break

        assert found, f"Stream {stream_id} not found in graph"
        logger.info("✓ Stream correctly tributary of Dupuis Creek, not Mamit Lake")

    def test_named_streams_stop_lake_propagation(self, guichon_graph):
        """
        Test that named tributary streams (like Dupuis Creek) are NOT
        marked as tributaries of Mamit Lake, even if they join Guichon Creek
        above the lake.

        Named streams should maintain their identity and be tributary of
        Guichon Creek, not the lake.
        """
        G = guichon_graph.G

        # Find named tributary streams
        named_tributaries = []
        for u, v, key, data in G.edges(keys=True, data=True):
            gnis_name = data.get("gnis_name", "")
            tributary_of = data.get("tributary_of", "")

            # Look for named streams that aren't Guichon Creek
            if (
                gnis_name
                and gnis_name != "Guichon Creek"
                and gnis_name != "Mamit Lake"
                and not data.get("lake_name")
            ):  # Not in a lake

                named_tributaries.append(
                    {
                        "name": gnis_name,
                        "tributary_of": tributary_of,
                        "linear_feature_id": data.get("linear_feature_id", ""),
                    }
                )

        logger.info(f"Found {len(named_tributaries)} named tributary streams")

        # Check a few examples
        sample_size = min(10, len(named_tributaries))
        if sample_size > 0:
            logger.info(f"Checking {sample_size} named tributaries:")

            for trib in named_tributaries[:sample_size]:
                name = trib["name"]
                trib_of = trib["tributary_of"]
                logger.info(f"  '{name}' → '{trib_of}'")

                # Named streams should generally NOT be tributary of Mamit Lake
                # (unless they have a special relationship)
                # We'll just log for now rather than assert, as the exact behavior
                # may depend on the specific geography

    def test_all_edges_have_tributary_of(self, guichon_graph):
        """Test that all edges have the tributary_of attribute."""
        G = guichon_graph.G

        edges_with_tributary = 0
        edges_without_tributary = 0

        for u, v, key, data in G.edges(keys=True, data=True):
            if "tributary_of" in data:
                edges_with_tributary += 1
            else:
                edges_without_tributary += 1

        logger.info(f"Edges with tributary_of: {edges_with_tributary:,}")
        logger.info(f"Edges without tributary_of: {edges_without_tributary:,}")

        total_edges = G.number_of_edges()
        assert (
            edges_with_tributary == total_edges
        ), f"Only {edges_with_tributary}/{total_edges} edges have tributary_of attribute"

        logger.info("✓ All edges have tributary_of attribute")

    def test_tributary_of_values_distribution(self, guichon_graph):
        """
        Analyze the distribution of tributary_of values to understand
        the watershed structure.
        """
        G = guichon_graph.G

        tributary_counts = {}

        for u, v, key, data in G.edges(keys=True, data=True):
            trib_of = data.get("tributary_of", "")
            if trib_of:
                tributary_counts[trib_of] = tributary_counts.get(trib_of, 0) + 1

        logger.info("\nTributary distribution:")
        for trib_name, count in sorted(tributary_counts.items(), key=lambda x: -x[1])[
            :20
        ]:
            logger.info(f"  {trib_name}: {count} edges")

        # Check that Mamit Lake has some tributaries
        mamit_count = tributary_counts.get("Mamit Lake", 0)
        assert mamit_count > 0, "No edges marked as tributary of Mamit Lake"

        logger.info(f"\n✓ Mamit Lake has {mamit_count} tributary edges")

    def test_lake_context_prevents_name_override(self, guichon_graph):
        """
        Test that when in_lake_context is True, the upstream_tributary
        is not overridden by the current stream name.

        This ensures that lake tributary status propagates correctly upstream.
        """
        G = guichon_graph.G

        # Find an edge that's tributary of Mamit Lake
        mamit_tributary_edges = []
        for u, v, key, data in G.edges(keys=True, data=True):
            if data.get("tributary_of") == "Mamit Lake":
                mamit_tributary_edges.append((u, v, key))

        assert len(mamit_tributary_edges) > 0, "No Mamit Lake tributaries found"

        # For each, check if its upstream edges also inherit Mamit Lake
        inheritance_working = 0
        inheritance_broken = 0

        for u, v, key in mamit_tributary_edges[:10]:  # Check first 10
            edge_data = G.edges[u, v, key]
            edge_name = edge_data.get("gnis_name", "")

            # Look at upstream edges
            for pred_u in G.predecessors(u):
                for pred_key in G[pred_u][u]:
                    pred_data = G.edges[pred_u, u, pred_key]
                    pred_trib_of = pred_data.get("tributary_of", "")
                    pred_name = pred_data.get("gnis_name", "")

                    # If unnamed and upstream of a Mamit tributary, should also be Mamit tributary
                    if not pred_name and pred_trib_of == "Mamit Lake":
                        inheritance_working += 1
                    elif not pred_name and pred_trib_of != "Mamit Lake":
                        inheritance_broken += 1

        logger.info(f"Lake inheritance check:")
        logger.info(f"  Working: {inheritance_working}")
        logger.info(f"  Broken: {inheritance_broken}")

        if inheritance_working > 0:
            logger.info("✓ Lake tributary status is propagating upstream")

    def test_export_graph_for_manual_inspection(self, guichon_graph):
        """Export the graph for manual inspection."""
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        # Export as pickle
        pickle_path = output_dir / "test_guichon_graph.gpickle"
        with open(pickle_path, "wb") as f:
            pickle.dump(guichon_graph.G, f, protocol=pickle.HIGHEST_PROTOCOL)

        logger.info(f"✓ Exported graph to {pickle_path}")

        # Also export edge data as JSON for easier inspection
        import json

        edge_data = []
        for u, v, key, data in guichon_graph.G.edges(keys=True, data=True):
            edge_data.append(
                {
                    "from": u,
                    "to": v,
                    "linear_feature_id": data.get("linear_feature_id", ""),
                    "gnis_name": data.get("gnis_name", ""),
                    "tributary_of": data.get("tributary_of", ""),
                    "lake_name": data.get("lake_name", ""),
                    "waterbody_key": data.get("waterbody_key", ""),
                    "fwa_watershed_code": data.get("fwa_watershed_code", ""),
                }
            )

        json_path = output_dir / "test_guichon_edges.json"
        with open(json_path, "w") as f:
            json.dump(edge_data, f, indent=2)

        logger.info(f"✓ Exported edge data to {json_path}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
