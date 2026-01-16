"""
Test tributary assignment for Guichon Creek watershed (GUIC).

Tests the simplified watershed hierarchy approach:
- TRIBUTARY_OF field should be populated for all streams
- Originally unnamed streams get renamed to "X Tributary"
- Lake tributaries detected via WATERBODY_KEY
- Braided streams inherit names from main channel
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import geopandas as gpd
import pandas as pd
import pytest
import fiona
import logging

# Import processing functions
from fwa_preprocessing import FWAProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestGuichonTributaries:
    """Test tributary assignments using simplified approach."""

    @pytest.fixture(scope="class")
    def setup_data(self):
        """Build test data on-the-fly from raw FWA for GUIC watershed only."""
        base_dir = Path(__file__).parent.parent.parent
        
        # Paths to raw data
        streams_gdb = base_dir / "data" / "ftp.geobc.gov.bc.ca" / "sections" / "outgoing" / "bmgs" / "FWA_Public" / "FWA_STREAM_NETWORKS_SP" / "FWA_STREAM_NETWORKS_SP.gdb"
        fwa_bc_gdb = base_dir / "data" / "ftp.geobc.gov.bc.ca" / "sections" / "outgoing" / "bmgs" / "FWA_Public" / "FWA_BC" / "FWA_BC.gdb"
        
        if not streams_gdb.exists():
            pytest.skip(f"Raw FWA streams not found: {streams_gdb}")
        if not fwa_bc_gdb.exists():
            pytest.skip(f"Raw FWA BC not found: {fwa_bc_gdb}")
        
        logger.info("=" * 80)
        logger.info("LOADING GUICHON CREEK WATERSHED (GUIC) FOR TESTING")
        logger.info("=" * 80)
        
        # Load GUIC streams only
        logger.info("Loading GUIC streams...")
        try:
            streams_gdf = gpd.read_file(str(streams_gdb), layer="GUIC")
            logger.info(f"✓ Loaded {len(streams_gdf):,} streams from GUIC")
        except Exception as e:
            pytest.skip(f"Could not load GUIC layer: {e}")
        
        # Load lakes from FWA_BC.gdb
        logger.info("Loading lakes...")
        try:
            # Load all lakes, then filter to GUIC watershed
            lakes_gdf = gpd.read_file(str(fwa_bc_gdb), layer="FWA_LAKES_POLY")
            logger.info(f"✓ Loaded {len(lakes_gdf):,} lakes")
            
            # Filter to GUIC watershed codes
            guic_codes = streams_gdf["FWA_WATERSHED_CODE"].unique()
            # Extract watershed group (first 8 chars) from codes
            guic_groups = set(code[:8] if isinstance(code, str) and len(code) >= 8 else None for code in guic_codes)
            guic_groups.discard(None)
            
            # Filter lakes to GUIC watershed
            lakes_gdf = lakes_gdf[
                lakes_gdf["FWA_WATERSHED_CODE"].apply(
                    lambda x: x[:8] if isinstance(x, str) and len(x) >= 8 else None
                ).isin(guic_groups)
            ].copy()
            logger.info(f"✓ Filtered to {len(lakes_gdf):,} GUIC lakes")
            
            # Filter to just named lakes for speed
            lakes_gdf = lakes_gdf[
                (lakes_gdf["GNIS_NAME_1"].notna()) & 
                (lakes_gdf["GNIS_NAME_1"].str.strip() != "")
            ].copy()
            logger.info(f"✓ Filtered to {len(lakes_gdf):,} named lakes")
        except Exception as e:
            logger.warning(f"Could not load lakes: {e}")
            lakes_gdf = gpd.GeoDataFrame()
        
        # Create a temporary processor just to use its methods
        logger.info("Processing streams...")
        processor = FWAProcessor(
            streams_gdb=str(streams_gdb),
            lakes_gdb=str(fwa_bc_gdb),
            wildlife_gpkg="",
            kml_path="",
            output_gpkg=""
        )
        
        # Run stream enrichment
        streams_gdf = processor.enrich_streams(streams_gdf, lakes_gdf)
        
        logger.info(f"✓ Processing complete: {len(streams_gdf):,} streams")
        logger.info("=" * 80)
        
        return {"streams": streams_gdf, "lakes": lakes_gdf}

    def test_guichon_outlet_streams_not_tributary_of_mamit(self, setup_data):
        """
        Test that Guichon Creek outlet (stream 701305409) is NOT marked as
        tributary of Mamit Lake.

        This stream flows OUT of Mamit Lake, so it should maintain its
        Guichon Creek identity and NOT be marked as lake tributary.
        """
        streams = setup_data["streams"]

        # Find the outlet stream
        outlet = streams[streams["LINEAR_FEATURE_ID"] == 701305409]

        assert len(outlet) > 0, "Outlet stream 701305409 not found"

        outlet_row = outlet.iloc[0]
        gnis_name = outlet_row["GNIS_NAME"]
        tributary_of = outlet_row["TRIBUTARY_OF"]

        print(f"\nStream 701305409 (Guichon Creek outlet):")
        print(f"  GNIS_NAME: {gnis_name}")
        print(f"  TRIBUTARY_OF: {tributary_of}")

        # Should maintain Guichon Creek name
        assert (
            gnis_name == "Guichon Creek"
        ), f"Expected 'Guichon Creek', got '{gnis_name}'"

        # Should NOT be tributary of Mamit Lake
        assert tributary_of != "Mamit Lake", (
            f"Outlet stream incorrectly marked as tributary of Mamit Lake. "
            f"TRIBUTARY_OF={tributary_of}"
        )

    def test_stream_701305652_is_tributary_of_gypsum_lake(self, setup_data):
        """
        Test that stream 701305652 is tributary of Gypsum Lake.

        This stream should be detected as a Gypsum Lake tributary via WATERBODY_KEY.
        """
        streams = setup_data["streams"]

        # Find the stream
        stream = streams[streams["LINEAR_FEATURE_ID"] == 701305652]

        assert len(stream) > 0, "Stream 701305652 not found"

        stream_row = stream.iloc[0]
        gnis_name = stream_row["GNIS_NAME"]
        tributary_of = stream_row["TRIBUTARY_OF"]

        print(f"\nStream 701305652:")
        print(f"  GNIS_NAME: {gnis_name}")
        print(f"  TRIBUTARY_OF: {tributary_of}")

        # Should be tributary of Gypsum Lake (not Guichon Creek)
        assert (
            tributary_of == "Gypsum Lake"
        ), f"Expected 'Gypsum Lake', got '{tributary_of}'"

    def test_mamit_lake_tributaries(self, setup_data):
        """
        Test that streams 701303054, 701303219, and 701302532 are all
        tributaries of Mamit Lake.

        These streams flow into Mamit Lake and should be detected as lake tributaries.
        """
        streams = setup_data["streams"]

        test_streams = [701303054, 701303219, 701302532]
        
        for stream_id in test_streams:
            stream = streams[streams["LINEAR_FEATURE_ID"] == stream_id]
            
            assert len(stream) > 0, f"Stream {stream_id} not found"
            
            stream_row = stream.iloc[0]
            gnis_name = stream_row["GNIS_NAME"]
            tributary_of = stream_row["TRIBUTARY_OF"]
            watershed_code = stream_row["FWA_WATERSHED_CODE"]
            waterbody_key = stream_row["WATERBODY_KEY"]
            parent_code = stream_row.get("parent_code", None)
            
            print(f"\nStream {stream_id}:")
            print(f"  GNIS_NAME: {gnis_name}")
            print(f"  TRIBUTARY_OF: {tributary_of}")
            print(f"  FWA_WATERSHED_CODE: {watershed_code}")
            print(f"  WATERBODY_KEY: {waterbody_key}")
            print(f"  parent_code: {parent_code}")
            
            # Check if parent exists and what it is
            if parent_code:
                parent = streams[streams["clean_code"] == parent_code]
                if not parent.empty:
                    print(f"  Parent stream TRIBUTARY_OF: {parent.iloc[0]['TRIBUTARY_OF']}")
            
            # Should be tributary of Mamit Lake
            assert (
                tributary_of == "Mamit Lake"
            ), f"Stream {stream_id}: Expected 'Mamit Lake', got '{tributary_of}'"

    def test_stream_701303849_is_tributary_of_dupuis_creek(self, setup_data):
        """
        Test that stream 701303849 is tributary of Dupuis Creek, not Mamit Lake.
        
        This tests that named streams (Dupuis Creek) stop the propagation of lake
        tributary status - only unnamed streams should inherit lake tributary status.
        """
        streams = setup_data["streams"]

        # Find the stream
        stream = streams[streams["LINEAR_FEATURE_ID"] == 701303849]

        assert len(stream) > 0, "Stream 701303849 not found"

        stream_row = stream.iloc[0]
        gnis_name = stream_row["GNIS_NAME"]
        tributary_of = stream_row["TRIBUTARY_OF"]

        print(f"\nStream 701303849:")
        print(f"  GNIS_NAME: {gnis_name}")
        print(f"  TRIBUTARY_OF: {tributary_of}")

        # Should be tributary of Dupuis Creek (not Mamit Lake)
        assert (
            tributary_of == "Dupuis Creek"
        ), f"Expected 'Dupuis Creek', got '{tributary_of}'"

    def test_guichon_creek_braid_701305931_not_tailwater(self, setup_data):
        """
        Test that braid segment 701305931 has same tributary assignment as
        main channel 701305928.

        Braids should inherit the same TRIBUTARY_OF as their main channel.
        """
        streams = setup_data["streams"]

        # Find braid and main channel
        braid = streams[streams["LINEAR_FEATURE_ID"] == 701305931]
        main = streams[streams["LINEAR_FEATURE_ID"] == 701305928]

        assert len(braid) > 0, "Braid stream 701305931 not found"
        assert len(main) > 0, "Main channel stream 701305928 not found"

        braid_row = braid.iloc[0]
        main_row = main.iloc[0]

        braid_name = braid_row["GNIS_NAME"]
        braid_trib = braid_row["TRIBUTARY_OF"]
        main_name = main_row["GNIS_NAME"]
        main_trib = main_row["TRIBUTARY_OF"]

        print(f"\nBraid 701305931:")
        print(f"  GNIS_NAME: {braid_name}")
        print(f"  TRIBUTARY_OF: {braid_trib}")
        print(f"\nMain 701305928:")
        print(f"  GNIS_NAME: {main_name}")
        print(f"  TRIBUTARY_OF: {main_trib}")

        # Both should have Guichon Creek name
        assert (
            braid_name == main_name
        ), f"Braid name '{braid_name}' != main name '{main_name}'"

        # Both should have same TRIBUTARY_OF
        assert (
            braid_trib == main_trib
        ), f"Braid TRIBUTARY_OF '{braid_trib}' != main TRIBUTARY_OF '{main_trib}'"

        assert (
            braid_trib != "Mamit Lake"
        ), f"Expected TRIBUTARY_OF not to be 'Mamit Lake', got '{braid_trib}'"

    def test_all_streams_have_tributary_of(self, setup_data):
        """
        Test that ALL streams have a TRIBUTARY_OF field.

        Even streams without parents should have the field (possibly NULL/None).
        """
        streams = setup_data["streams"]

        # Check that TRIBUTARY_OF column exists
        assert (
            "TRIBUTARY_OF" in streams.columns
        ), "TRIBUTARY_OF column missing from streams"

        print(f"\n✓ All {len(streams):,} streams have TRIBUTARY_OF field")

        # Count how many have values
        has_tributary = streams["TRIBUTARY_OF"].notna().sum()
        no_tributary = streams["TRIBUTARY_OF"].isna().sum()

        print(f"  With TRIBUTARY_OF: {has_tributary:,}")
        print(f"  Without TRIBUTARY_OF: {no_tributary:,}")

        # Most streams should have a parent
        assert has_tributary > 0, "No streams have TRIBUTARY_OF assigned"

    def test_originally_named_streams_not_renamed(self, setup_data):
        """
        Test that streams with original GNIS names are NOT renamed.

        Only originally unnamed streams should get "X Tributary" names.
        """
        streams = setup_data["streams"]

        # Sample some known named streams
        guichon_streams = streams[streams["GNIS_NAME"] == "Guichon Creek"]

        assert len(guichon_streams) > 0, "No Guichon Creek segments found"

        print(f"\n✓ Found {len(guichon_streams):,} Guichon Creek segments")

        # None should have " Tributary" suffix (they're the main river)
        for idx, row in guichon_streams.iterrows():
            gnis_name = row["GNIS_NAME"]
            assert (
                " Tributary" not in gnis_name
            ), f"Named stream incorrectly renamed: {gnis_name}"

    def test_unnamed_tributaries_have_parent_name(self, setup_data):
        """
        Test that streams renamed to "X Tributary" have matching TRIBUTARY_OF.

        If GNIS_NAME is "Guichon Creek Tributary", TRIBUTARY_OF should be "Guichon Creek".
        """
        streams = setup_data["streams"]

        # Find streams with " Tributary" suffix
        tributary_streams = streams[
            streams["GNIS_NAME"].str.contains(" Tributary", na=False)
        ]

        print(f"\n✓ Found {len(tributary_streams):,} streams with ' Tributary' suffix")

        if len(tributary_streams) > 0:
            # Check first 10
            sample = tributary_streams.head(10)
            mismatches = 0

            for idx, row in sample.iterrows():
                gnis_name = row["GNIS_NAME"]
                tributary_of = row["TRIBUTARY_OF"]

                # Extract parent name from "X Tributary"
                expected_parent = gnis_name.replace(" Tributary", "")

                if tributary_of != expected_parent:
                    print(
                        f"  ⚠️ Mismatch: '{gnis_name}' has TRIBUTARY_OF='{tributary_of}'"
                    )
                    mismatches += 1

            assert (
                mismatches == 0
            ), f"{mismatches} tributary names don't match TRIBUTARY_OF"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
