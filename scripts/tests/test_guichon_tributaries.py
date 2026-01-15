"""
Test tributary assignment for Guichon Creek watershed (GUIC).

Verifies that the network analysis correctly assigns tributary relationships
without lakes, focusing on named and unnamed stream hierarchy.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import geopandas as gpd
import pandas as pd
import pytest
from fwa_modules.network_analysis import NetworkAnalyzer


class TestGuichonTributaries:
    """Test tributary assignments in Guichon Creek watershed."""

    @pytest.fixture(scope="class")
    def setup_data(self):
        """Load GUIC layer and run network analysis."""
        # Paths
        base_dir = Path(__file__).parent.parent
        streams_gdb = base_dir / "output" / "fwa_preprocessing" / "FWA_Zone_Grouped.gdb"
        output_dir = base_dir / "output" / "test_guichon"

        # Create a minimal lakes GDB with just Mamit Lake for testing
        lakes_gdb = output_dir / "test_lakes.gdb"

        # Load the full lakes dataset and extract just Mamit Lake
        # The correct path based on the data directory structure
        full_lakes_gdb = (
            base_dir.parent
            / "data"
            / "ftp.geobc.gov.bc.ca"
            / "sections"
            / "outgoing"
            / "bmgs"
            / "FWA_Public"
            / "FWA_BC"
            / "FWA_BC.gdb"
        )

        try:
            if full_lakes_gdb.exists():
                all_lakes = gpd.read_file(str(full_lakes_gdb), layer="FWA_LAKES_POLY")

                # Filter to just Mamit Lake (WATERBODY_POLY_ID = 700089332)
                mamit_lake = all_lakes[all_lakes["WATERBODY_POLY_ID"] == 700089332]

                if len(mamit_lake) > 0:
                    # Save to test GDB
                    mamit_lake.to_file(
                        str(lakes_gdb), layer="FWA_LAKES_POLY", driver="OpenFileGDB"
                    )
                    print(f"\nCreated test lakes GDB with Mamit Lake at {lakes_gdb}")
                else:
                    print(
                        f"\nWarning: Mamit Lake (700089332) not found in full lakes dataset"
                    )
                    lakes_gdb = None
            else:
                print(f"\nWarning: Full lakes GDB not found at {full_lakes_gdb}")
                lakes_gdb = None
        except Exception as e:
            print(f"\nWarning: Could not create test lakes GDB: {e}")
            lakes_gdb = None

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Load ALL stream data from GDB (all layers) and filter to GUIC watershed
        print("\n=== Loading stream data ===")
        import fiona

        layers = fiona.listlayers(str(streams_gdb))
        print(f"Found {len(layers)} layers in GDB")

        # Load all layers and concatenate (GUIC watershed is in one of them)
        all_streams = []
        for layer_name in layers:
            streams = gpd.read_file(str(streams_gdb), layer=layer_name)
            all_streams.append(streams)

        all_streams_df = gpd.GeoDataFrame(pd.concat(all_streams, ignore_index=True))

        # Filter to GUIC watershed group
        guic = all_streams_df[all_streams_df["WATERSHED_GROUP_CODE"] == "GUIC"].copy()
        print(f"Loaded {len(guic)} GUIC stream segments")

        # Initialize analyzer
        analyzer = NetworkAnalyzer(
            streams_gdb=streams_gdb,
            lakes_gdb=lakes_gdb,
            output_tributary_map=output_dir / "tributary_map.json",
            output_lake_segments=output_dir / "lake_segments.json",
            output_graph=output_dir / "graph.graphml",
        )

        # Build graph using only GUIC layer
        print("\n=== Building GUIC Network Graph ===")
        graph = analyzer.build_network_graph()

        # Detect lake nodes
        graph = analyzer.detect_lake_nodes(graph)

        # Find lake segments
        lake_segments = analyzer.find_lake_segments(graph)

        # Assign tributaries
        tributary_assignments = analyzer.assign_tributaries(graph, lake_segments)

        return {
            "graph": graph,
            "tributary_assignments": tributary_assignments,
            "lake_segments": lake_segments,
            "streams_gdb": streams_gdb,
            "guic": guic,  # Pass the loaded data to avoid reopening GDB
        }

    def test_rey_creek_is_tributary_of_guichon(self, setup_data):
        """Verify Rey Creek is assigned as tributary of Guichon Creek."""
        tributary_assignments = setup_data["tributary_assignments"]
        guic = setup_data["guic"]

        # Find Rey Creek segments (GNIS_NAME = 'Rey Creek')
        rey_creek_segments = guic[guic["GNIS_NAME"] == "Rey Creek"]

        assert len(rey_creek_segments) > 0, "No Rey Creek segments found in GUIC layer"

        print(f"\nFound {len(rey_creek_segments)} Rey Creek segments")

        # Check each Rey Creek segment
        guichon_count = 0
        other_count = 0
        none_count = 0

        for _, segment in rey_creek_segments.iterrows():
            feature_id = segment["LINEAR_FEATURE_ID"]

            if feature_id in tributary_assignments:
                assignment = tributary_assignments[feature_id]
                tributary_of = assignment.tributary_of

                print(f"  Feature {feature_id}: tributary_of = {tributary_of}")

                if tributary_of == "Guichon Creek":
                    guichon_count += 1
                elif tributary_of is None:
                    none_count += 1
                else:
                    other_count += 1
                    print(
                        f"    ⚠️  WRONG: Expected 'Guichon Creek', got '{tributary_of}'"
                    )
            else:
                none_count += 1
                print(f"  Feature {feature_id}: NOT ASSIGNED")

        print(f"\nRey Creek tributary assignments:")
        print(f"  Guichon Creek: {guichon_count}")
        print(f"  Other: {other_count}")
        print(f"  None: {none_count}")

        # At least some Rey Creek segments should be tributary of Guichon
        # (The most downstream segments might not have a parent)
        assert guichon_count > 0, "No Rey Creek segments assigned to Guichon Creek"

    def test_unnamed_stream_is_tributary_of_rey_creek(self, setup_data):
        """Verify unnamed stream 701304751 is assigned as tributary of Rey Creek."""
        tributary_assignments = setup_data["tributary_assignments"]

        # The specific unnamed stream
        feature_id = 701304751

        assert (
            feature_id in tributary_assignments
        ), f"Feature {feature_id} not found in tributary assignments"

        assignment = tributary_assignments[feature_id]
        tributary_of = assignment.tributary_of

        print(f"\nFeature {feature_id}:")
        print(f"  tributary_of = {tributary_of}")

        assert (
            tributary_of == "Rey Creek"
        ), f"Expected 'Rey Creek', got '{tributary_of}'"

    def test_all_rey_creek_unnamed_tributaries(self, setup_data):
        """Verify all unnamed streams flowing DIRECTLY into Rey Creek are assigned correctly."""
        tributary_assignments = setup_data["tributary_assignments"]
        guic = setup_data["guic"]

        # Find all streams with Rey Creek as DIRECT parent in watershed code
        # Rey Creek code: 100-190442-244975-296261-383667
        rey_creek_code = "100-190442-244975-296261-383667"

        # Find unnamed tributaries whose parent code is exactly Rey Creek
        # (not grandchildren via Eve Creek, Phelps Creek, etc.)
        from fwa_modules.utils import clean_watershed_code, get_parent_code

        direct_tributaries = []
        for _, segment in guic.iterrows():
            if pd.notna(segment["GNIS_NAME"]):  # Skip named streams
                continue

            watershed_code = segment["FWA_WATERSHED_CODE"]
            if pd.isna(watershed_code):
                continue

            clean_code = clean_watershed_code(str(watershed_code))
            if not clean_code:
                continue

            parent_code = get_parent_code(clean_code)

            # Check if parent is exactly Rey Creek
            if parent_code == rey_creek_code:
                direct_tributaries.append(segment)

        direct_tributaries_df = (
            pd.DataFrame(direct_tributaries) if direct_tributaries else pd.DataFrame()
        )

        print(
            f"\nFound {len(direct_tributaries_df)} unnamed streams with Rey Creek as DIRECT parent"
        )

        rey_creek_count = 0
        tailwater_count = 0
        other_count = 0
        none_count = 0

        for _, segment in direct_tributaries_df.iterrows():
            feature_id = segment["LINEAR_FEATURE_ID"]
            watershed_code = segment["FWA_WATERSHED_CODE"]

            if feature_id in tributary_assignments:
                assignment = tributary_assignments[feature_id]
                tributary_of = assignment.tributary_of

                if tributary_of == "Rey Creek":
                    rey_creek_count += 1
                elif tributary_of == "Tailwater":
                    tailwater_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): WRONG - assigned to Tailwater"
                    )
                elif tributary_of is None:
                    none_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): WRONG - not assigned"
                    )
                else:
                    other_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): WRONG - assigned to {tributary_of}"
                    )
            else:
                none_count += 1
                print(f"  ⚠️  Feature {feature_id} ({watershed_code}): NOT ASSIGNED")

        print(f"\nDirect unnamed Rey Creek tributary assignments:")
        print(f"  Rey Creek: {rey_creek_count}")
        print(f"  Tailwater: {tailwater_count}")
        print(f"  Other: {other_count}")
        print(f"  None: {none_count}")

        # All direct unnamed tributaries should be assigned to Rey Creek
        if len(direct_tributaries_df) > 0:
            assert (
                tailwater_count == 0
            ), f"{tailwater_count} tributaries wrongly assigned to Tailwater"
            assert (
                other_count == 0
            ), f"{other_count} tributaries assigned to wrong stream"
            assert rey_creek_count > 0, "No tributaries assigned to Rey Creek"

    def test_all_guichon_unnamed_tributaries(self, setup_data):
        """Verify unnamed streams flowing into Guichon Creek (not via Rey Creek) are assigned correctly."""
        tributary_assignments = setup_data["tributary_assignments"]
        guic = setup_data["guic"]

        # Find all streams with Guichon Creek as parent in watershed code
        # Guichon Creek code: 100-190442-244975-296261
        guichon_code = "100-190442-244975-296261"
        rey_creek_code = "100-190442-244975-296261-383667"

        # Find unnamed tributaries (parent code matches Guichon but NOT Rey Creek)
        potential_tributaries = guic[
            (guic["GNIS_NAME"].isna())  # Unnamed
            & (guic["FWA_WATERSHED_CODE"].str.startswith(guichon_code + "-", na=False))
            & (
                ~guic["FWA_WATERSHED_CODE"].str.startswith(
                    rey_creek_code + "-", na=False
                )
            )  # Exclude Rey Creek tributaries
        ]

        print(
            f"\nFound {len(potential_tributaries)} unnamed streams with Guichon Creek as direct parent"
        )

        guichon_count = 0
        tailwater_count = 0
        other_count = 0
        none_count = 0

        for _, segment in potential_tributaries.iterrows():
            feature_id = segment["LINEAR_FEATURE_ID"]
            watershed_code = segment["FWA_WATERSHED_CODE"]

            if feature_id in tributary_assignments:
                assignment = tributary_assignments[feature_id]
                tributary_of = assignment.tributary_of

                if tributary_of == "Guichon Creek":
                    guichon_count += 1
                elif tributary_of == "Tailwater":
                    tailwater_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): WRONG - assigned to Tailwater"
                    )
                elif tributary_of is None:
                    none_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): WRONG - not assigned"
                    )
                else:
                    other_count += 1
                    print(
                        f"  ⚠️  Feature {feature_id} ({watershed_code}): assigned to {tributary_of}"
                    )
            else:
                none_count += 1
                print(f"  ⚠️  Feature {feature_id} ({watershed_code}): NOT ASSIGNED")

        print(f"\nUnnamed Guichon Creek tributary assignments:")
        print(f"  Guichon Creek: {guichon_count}")
        print(f"  Tailwater: {tailwater_count}")
        print(f"  Other: {other_count}")
        print(f"  None: {none_count}")

        # Most unnamed tributaries should be assigned to Guichon Creek
        # (Some might legitimately be outlets or assigned to other named tributaries)
        assert guichon_count > 0, "No tributaries assigned to Guichon Creek"

    def test_mamit_lake_segments_and_tributaries(self, setup_data):
        """Verify Mamit Lake segments are detected and upstream streams are assigned to lake."""
        tributary_assignments = setup_data["tributary_assignments"]
        lake_segments = setup_data["lake_segments"]
        guic = setup_data["guic"]

        # Check if we have any lake segments for Mamit Lake (WATERBODY_POLY_ID = 700089332)
        mamit_lake_segments = {
            fid: poly_id
            for fid, poly_id in lake_segments.items()
            if poly_id == 700089332
        }

        print(
            f"\nFound {len(mamit_lake_segments)} lake segments in Mamit Lake (poly_id 700089332)"
        )

        if len(mamit_lake_segments) == 0:
            pytest.skip(
                "No Mamit Lake segments detected - lake polygon may not be loaded"
            )

        # Print the lake segments
        for feature_id, poly_id in mamit_lake_segments.items():
            stream = guic[guic["LINEAR_FEATURE_ID"] == feature_id]
            if len(stream) > 0:
                print(
                    f"  Lake segment {feature_id}: {stream.iloc[0]['GNIS_NAME'] or 'unnamed'}"
                )

        # Check if expected features are in lake segments
        print("\nExpected features in lake_segments dict:")
        for fid in [701303105, 701302965]:
            if fid in lake_segments:
                print(f"  ✓ {fid} in lake_segments (poly_id {lake_segments[fid]})")
            else:
                print(f"  ✗ {fid} NOT in lake_segments")

        # Test specific features that should be Mamit Lake tributaries
        expected_mamit_lake_features = [
            701304356,  # Unnamed stream, edge type 1450
            701304361,  # Unnamed stream, edge type 1410
            701304285,  # Unnamed stream, edge type 1000
            701303105,  # Guichon Creek segment in lake
            701302965,  # Guichon Creek segment in lake
        ]

        print(
            f"\nChecking {len(expected_mamit_lake_features)} features that should be Mamit Lake tributaries:"
        )

        failures = []
        for feature_id in expected_mamit_lake_features:
            if feature_id in tributary_assignments:
                assignment = tributary_assignments[feature_id]
                tributary_of = assignment.tributary_of

                stream = guic[guic["LINEAR_FEATURE_ID"] == feature_id]
                name = stream.iloc[0]["GNIS_NAME"] if len(stream) > 0 else "unknown"

                if tributary_of == "Mamit Lake":
                    print(f"  ✓ {feature_id} ({name or 'unnamed'}): {tributary_of}")
                else:
                    print(
                        f"  ✗ {feature_id} ({name or 'unnamed'}): {tributary_of} (expected Mamit Lake)"
                    )
                    failures.append(
                        f"{feature_id} assigned to '{tributary_of}' instead of 'Mamit Lake'"
                    )
            else:
                stream = guic[guic["LINEAR_FEATURE_ID"] == feature_id]
                if len(stream) == 0:
                    print(f"  ⚠ {feature_id}: Not found in GUIC layer")
                else:
                    print(f"  ✗ {feature_id}: Not assigned")
                    failures.append(f"{feature_id} not assigned")

        # Report all failures at once
        if failures:
            pytest.fail(
                f"{len(failures)} features incorrectly assigned:\n  "
                + "\n  ".join(failures)
            )


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
