"""
Diagnostic tool to investigate why streams inside lakes are not being excluded.

Usage:
    python -m fwa_modules.tools.diagnose_lake_stream_exclusion \
        --waterbody-key 329083341 \
        --linear-id 700740026
"""

import argparse
import json
import sys
import pickle
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fwa_modules.linking.regulation_mapper import RegulationMapper
from fwa_modules.linking.metadata_gazetteer import MetadataGazetteer, FeatureType
from fwa_modules.linking.linker import WaterbodyLinker
from fwa_modules.linking.scope_filter import ScopeFilter
from fwa_modules.linking.tributary_enricher import TributaryEnricher
from fwa_modules.linking.name_variations import (
    NAME_VARIATIONS,
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNMARKED_WATERBODIES,
    ManualCorrections,
)


class LakeStreamDiagnostics:
    """Diagnose lake-stream exclusion issues."""

    def __init__(
        self,
        mapper: RegulationMapper,
        gazetteer: MetadataGazetteer,
    ):
        self.mapper = mapper
        self.gazetteer = gazetteer
        self.merged_groups = mapper.merged_groups
        self.feature_to_regs = mapper.feature_to_regs

    def analyze_waterbody(self, waterbody_key: str) -> Dict:
        """Analyze a specific waterbody and its streams."""
        print(f"\n{'=' * 80}")
        print(f"ANALYZING WATERBODY: {waterbody_key}")
        print(f"{'=' * 80}\n")

        # Get lake metadata
        lake_meta = None
        lake_id = None
        for metadata_type in ["lakes", "wetlands", "manmade"]:
            for poly_id, meta in self.gazetteer.metadata.get(metadata_type, {}).items():
                if str(meta.get("waterbody_key")) == str(waterbody_key):
                    lake_meta = meta
                    lake_id = poly_id
                    print(f"Found waterbody in '{metadata_type}' collection:")
                    print(f"  Polygon ID: {poly_id}")
                    print(f"  GNIS Name: {meta.get('gnis_name', 'N/A')}")
                    print(f"  Blue Line Key: {meta.get('blue_line_key', 'N/A')}")
                    print(f"  Area (hectares): {meta.get('area_ha', 'N/A')}")
                    break
            if lake_meta:
                break

        if not lake_meta:
            print(f"❌ Waterbody {waterbody_key} not found in metadata!")
            return {}

        # Find all streams with this waterbody_key
        streams_in_lake = []
        for linear_id, meta in self.gazetteer.metadata.get("streams", {}).items():
            if str(meta.get("waterbody_key")) == str(waterbody_key):
                streams_in_lake.append(
                    {
                        "linear_id": linear_id,
                        "gnis_name": meta.get("gnis_name", "N/A"),
                        "blue_line_key": meta.get("blue_line_key", "N/A"),
                        "stream_order": meta.get("stream_order", 0),
                        "stream_magnitude": meta.get("stream_magnitude", 0),
                        "edge_type": meta.get("edge_type", "N/A"),
                        "length_m": meta.get("length", 0),
                    }
                )

        print(f"\n{'─' * 80}")
        print(f"STREAMS IN WATERBODY: {len(streams_in_lake)} streams found")
        print(f"{'─' * 80}")

        for i, stream in enumerate(streams_in_lake, 1):
            print(f"\n  Stream #{i}:")
            print(f"    Linear ID: {stream['linear_id']}")
            print(f"    GNIS Name: {stream['gnis_name']}")
            print(f"    Blue Line Key: {stream['blue_line_key']}")
            print(f"    Stream Order: {stream['stream_order']}")
            print(f"    Stream Magnitude: {stream['stream_magnitude']}")
            print(f"    Edge Type: {stream['edge_type']}")
            print(f"    Length (m): {stream['length_m']:.2f}")

        # Check if streams are in feature_to_regs
        print(f"\n{'─' * 80}")
        print("REGULATION MAPPING STATUS")
        print(f"{'─' * 80}")

        for stream in streams_in_lake:
            linear_id = stream["linear_id"]
            has_regs = linear_id in self.feature_to_regs
            regs = self.feature_to_regs.get(linear_id, [])
            print(f"\n  {linear_id} ({stream['gnis_name']}):")
            print(f"    Has regulations: {has_regs}")
            if has_regs:
                print(f"    Regulation count: {len(regs)}")
                print(
                    f"    Regulation IDs: {', '.join(regs[:5])}{'...' if len(regs) > 5 else ''}"
                )

        # Check merged groups
        print(f"\n{'─' * 80}")
        print("MERGED GROUPS ANALYSIS")
        print(f"{'─' * 80}")

        groups_with_lake_streams = []
        for group_id, group in self.merged_groups.items():
            # Check if any stream in this group has the waterbody_key
            group_stream_ids = [
                fid
                for fid in group.feature_ids
                if not fid.startswith("LAKE_")
                and not fid.startswith("WETLAND_")
                and not fid.startswith("MANMADE_")
            ]

            for fid in group_stream_ids:
                meta = self.gazetteer.get_stream_metadata(fid)
                if meta and str(meta.get("waterbody_key")) == str(waterbody_key):
                    groups_with_lake_streams.append(
                        {
                            "group_id": group_id,
                            "group": group,
                            "feature_id": fid,
                        }
                    )
                    break

        print(
            f"\nFound {len(groups_with_lake_streams)} groups containing streams from this lake:"
        )

        for item in groups_with_lake_streams:
            group = item["group"]
            print(f"\n  Group {item['group_id']}:")
            print(f"    GNIS Name: {group.gnis_name or 'N/A'}")
            print(f"    Feature Type: {group.feature_type}")
            print(f"    Waterbody Key: {group.waterbody_key}")
            print(f"    Blue Line Key: {group.waterbody_key}")
            print(f"    Feature Count: {group.feature_count}")
            print(f"    Regulation Count: {len(group.regulation_ids)}")
            print(
                f"    Feature IDs (first 5): {', '.join(list(group.feature_ids)[:5])}"
            )

            # Check if this group should be excluded
            has_waterbody_key = group.waterbody_key is not None
            waterbody_key_matches = (
                str(group.waterbody_key) == str(waterbody_key)
                if group.waterbody_key
                else False
            )

            print(f"\n    Exclusion Logic:")
            print(f"      group.waterbody_key is not None: {has_waterbody_key}")
            print(f"      group.waterbody_key == target: {waterbody_key_matches}")
            print(f"      Would be excluded: {has_waterbody_key}")

        # Check linked waterbody keys
        print(f"\n{'─' * 80}")
        print("LINKED WATERBODY KEYS")
        print(f"{'─' * 80}")

        is_linked = str(waterbody_key) in self.mapper.linked_waterbody_keys
        print(f"\nWaterbody key {waterbody_key} in linked_waterbody_keys: {is_linked}")
        print(f"Total linked waterbody keys: {len(self.mapper.linked_waterbody_keys)}")

        # Show some context
        linked_list = list(self.mapper.linked_waterbody_keys)
        print(
            f"Sample linked keys: {', '.join(linked_list[:10])}{'...' if len(linked_list) > 10 else ''}"
        )

        return {
            "waterbody_key": waterbody_key,
            "lake_meta": lake_meta,
            "streams_count": len(streams_in_lake),
            "streams": streams_in_lake,
            "groups_count": len(groups_with_lake_streams),
            "is_linked": is_linked,
        }

    def analyze_linear_feature(self, linear_id: str) -> Dict:
        """Analyze a specific linear feature."""
        print(f"\n{'=' * 80}")
        print(f"ANALYZING LINEAR FEATURE: {linear_id}")
        print(f"{'=' * 80}\n")

        # Get stream metadata
        meta = self.gazetteer.get_stream_metadata(linear_id)
        if not meta:
            print(f"❌ Linear feature {linear_id} not found in metadata!")
            return {}

        print("Stream Metadata:")
        print(f"  GNIS Name: {meta.get('gnis_name', 'N/A')}")
        print(f"  Waterbody Key: {meta.get('waterbody_key', 'N/A')}")
        print(f"  Blue Line Key: {meta.get('blue_line_key', 'N/A')}")
        print(f"  Watershed Code: {meta.get('fwa_watershed_code', 'N/A')}")
        print(f"  Stream Order: {meta.get('stream_order', 0)}")
        print(f"  Stream Magnitude: {meta.get('stream_magnitude', 0)}")
        print(f"  Edge Type: {meta.get('edge_type', 'N/A')}")
        print(f"  Length (m): {meta.get('length', 0):.2f}")

        # Check regulations
        has_regs = linear_id in self.feature_to_regs
        regs = self.feature_to_regs.get(linear_id, [])

        print(f"\nRegulation Mapping:")
        print(f"  Has regulations: {has_regs}")
        if has_regs:
            print(f"  Regulation count: {len(regs)}")
            print(
                f"  Regulation IDs: {', '.join(regs[:10])}{'...' if len(regs) > 10 else ''}"
            )

        # Find groups containing this feature
        containing_groups = []
        for group_id, group in self.merged_groups.items():
            if linear_id in group.feature_ids:
                containing_groups.append((group_id, group))

        print(f"\nMerged Groups: Found in {len(containing_groups)} group(s)")

        for group_id, group in containing_groups:
            print(f"\n  Group {group_id}:")
            print(f"    GNIS Name: {group.gnis_name or 'N/A'}")
            print(f"    Feature Type: {group.feature_type}")
            print(f"    Waterbody Key: {group.waterbody_key}")
            print(f"    Feature Count: {group.feature_count}")
            print(f"    Regulation Count: {len(group.regulation_ids)}")

            has_waterbody_key = group.waterbody_key is not None
            print(f"\n    Would be excluded from streams layer: {has_waterbody_key}")

        # If waterbody_key exists, analyze the waterbody
        if waterbody_key := meta.get("waterbody_key"):
            print(f"\n{'─' * 60}")
            print(f"Stream is inside waterbody: {waterbody_key}")
            print(f"{'─' * 60}")
            self.analyze_waterbody(str(waterbody_key))

        return {
            "linear_id": linear_id,
            "meta": meta,
            "has_regulations": has_regs,
            "group_count": len(containing_groups),
        }

    def find_problematic_cases(self) -> List[Dict]:
        """Find all cases where streams with waterbody_key are in merged groups."""
        print(f"\n{'=' * 80}")
        print("FINDING ALL PROBLEMATIC CASES")
        print(f"{'=' * 80}\n")

        problematic = []

        for group_id, group in self.merged_groups.items():
            if group.feature_type != "stream":
                continue

            # Check each stream in the group
            for fid in group.feature_ids:
                if (
                    fid.startswith("LAKE_")
                    or fid.startswith("WETLAND_")
                    or fid.startswith("MANMADE_")
                ):
                    continue

                meta = self.gazetteer.get_stream_metadata(fid)
                if meta and meta.get("waterbody_key"):
                    problematic.append(
                        {
                            "group_id": group_id,
                            "linear_id": fid,
                            "waterbody_key": meta.get("waterbody_key"),
                            "group_waterbody_key": group.waterbody_key,
                            "gnis_name": meta.get("gnis_name", "N/A"),
                            "has_regs": len(group.regulation_ids) > 0,
                        }
                    )
                    break  # Only count once per group

        print(f"Found {len(problematic)} groups with streams inside waterbodies:\n")

        # Group by waterbody
        by_waterbody = defaultdict(list)
        for item in problematic:
            by_waterbody[item["waterbody_key"]].append(item)

        for wb_key, items in sorted(
            by_waterbody.items(), key=lambda x: len(x[1]), reverse=True
        ):
            print(f"\nWaterbody {wb_key}: {len(items)} stream group(s)")
            for item in items[:5]:  # Show first 5
                print(
                    f"  - Group {item['group_id']}: {item['gnis_name']} (Linear ID: {item['linear_id']})"
                )
                print(f"    Group waterbody_key: {item['group_waterbody_key']}")
                print(f"    Has regulations: {item['has_regs']}")

        return problematic


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose lake-stream exclusion issues"
    )
    parser.add_argument(
        "--waterbody-key",
        help="Waterbody key to analyze",
    )
    parser.add_argument(
        "--linear-id",
        help="Linear feature ID to analyze",
    )
    parser.add_argument(
        "--find-all",
        action="store_true",
        help="Find all problematic cases",
    )
    parser.add_argument(
        "--regulations",
        default="output/parse_synopsis/parsed_results.json",
        help="Path to parsed regulations JSON",
    )
    parser.add_argument(
        "--metadata",
        default="output/fwa_modules/stream_metadata.pickle",
        help="Path to metadata pickle file",
    )
    parser.add_argument(
        "--graph",
        default="output/fwa_modules/fwa_graph.gpickle",
        help="Path to FWA graph (pickle file)",
    )

    args = parser.parse_args()

    # Load data
    print("Loading data...")

    regulations_path = Path(args.regulations)
    if not regulations_path.exists():
        print(f"❌ Regulations file not found: {regulations_path}")
        return 1

    with open(regulations_path, "r") as f:
        regulations = json.load(f)

    metadata_path = Path(args.metadata)
    graph_path = Path(args.graph)

    print(f"Loading metadata from {metadata_path}...")
    gazetteer = MetadataGazetteer(metadata_path)

    graph_data = None
    if graph_path.exists():
        print(f"Loading graph from {graph_path}...")
        with open(graph_path, "rb") as f:
            graph_data = pickle.load(f)
        graph = graph_data.get("graph") if isinstance(graph_data, dict) else graph_data
        print(f"✓ Graph loaded: {graph.vcount():,} nodes, {graph.ecount():,} edges")
    else:
        print(f"⚠️  Graph file not found: {graph_path}")
        graph = None

    # Initialize pipeline components
    print("Initializing pipeline...")
    manual_corrections = ManualCorrections(
        name_variations=NAME_VARIATIONS,
        direct_matches=DIRECT_MATCHES,
        skip_entries=SKIP_ENTRIES,
        unmarked_waterbodies=UNMARKED_WATERBODIES,
    )
    linker = WaterbodyLinker(gazetteer, manual_corrections)
    scope_filter = ScopeFilter(gazetteer)
    tributary_enricher = (
        TributaryEnricher(graph_data, gazetteer) if graph_data else None
    )

    mapper = RegulationMapper(linker, scope_filter, tributary_enricher)

    # Process regulations
    print(f"Processing {len(regulations)} regulations...")
    mapper.process_all_regulations(regulations)
    mapper.merge_features(mapper.feature_to_regs)

    print(f"✓ Processing complete")
    print(f"  Features with regulations: {len(mapper.feature_to_regs)}")
    print(f"  Merged groups: {len(mapper.merged_groups)}")

    # Run diagnostics
    diagnostics = LakeStreamDiagnostics(mapper, gazetteer)

    if args.find_all:
        diagnostics.find_problematic_cases()
    elif args.waterbody_key:
        diagnostics.analyze_waterbody(args.waterbody_key)
    elif args.linear_id:
        diagnostics.analyze_linear_feature(args.linear_id)
    else:
        print("\n⚠️  Please specify --waterbody-key, --linear-id, or --find-all")
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
