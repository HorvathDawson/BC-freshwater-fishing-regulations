#!/usr/bin/env python3
"""
Inspect specific linear feature IDs in both graph and metadata.
"""
import pickle
from pathlib import Path
from pprint import pprint

# Feature IDs to inspect
FEATURE_IDS = [
    "868144368",
    "863099181",
    "706967374",
    "868144369",
]


def load_graph(graph_path):
    """Load the graph from pickle."""
    print(f"Loading graph from {graph_path}...")
    with open(graph_path, "rb") as f:
        data = pickle.load(f)
    return data


def load_metadata(metadata_path):
    """Load stream metadata from pickle."""
    print(f"Loading metadata from {metadata_path}...")
    with open(metadata_path, "rb") as f:
        metadata = pickle.load(f)
    return metadata


def inspect_feature(linear_id, graph_data, metadata):
    """Inspect a single feature in both graph and metadata."""
    print("\n" + "=" * 80)
    print(f"LINEAR FEATURE ID: {linear_id}")
    print("=" * 80)
    
    # --- GRAPH ATTRIBUTES ---
    print("\n--- GRAPH ATTRIBUTES ---")
    graph = graph_data.get("graph")
    
    if graph:
        # Find edge with this linear_feature_id
        found = False
        for edge in graph.es:
            if str(edge["linear_feature_id"]) == str(linear_id):
                found = True
                print(f"Edge Index: {edge.index}")
                print("\nAll edge attributes:")
                for attr in edge.attributes():
                    value = edge[attr]
                    print(f"  {attr}: {value}")
                break
        
        if not found:
            print(f"❌ Not found in graph")
    else:
        print("❌ Graph not loaded")
    
    # --- METADATA ATTRIBUTES ---
    print("\n--- METADATA ATTRIBUTES ---")
    
    # Check in streams section
    streams = metadata.get("streams", {})
    if str(linear_id) in streams:
        meta = streams[str(linear_id)]
        print("Found in metadata['streams']:")
        pprint(meta, indent=2)
    else:
        print(f"❌ Not found in metadata['streams']")
        
        # Show what keys exist in metadata
        if streams:
            print(f"\n   Total streams in metadata: {len(streams):,}")
            print(f"   Sample keys: {list(streams.keys())[:10]}")
    
    print("\n")


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parents[2]  # tools -> fwa_modules -> scripts -> project_root
    
    # Paths
    graph_path = project_root / "scripts/output/fwa_modules/fwa_bc_primal_full.gpickle"
    metadata_path = project_root / "scripts/output/fwa_modules/stream_metadata.pickle"
    
    # Check if files exist
    if not graph_path.exists():
        print(f"❌ Graph not found: {graph_path}")
        return
    
    if not metadata_path.exists():
        print(f"❌ Metadata not found: {metadata_path}")
        return
    
    # Load data
    graph_data = load_graph(graph_path)
    metadata = load_metadata(metadata_path)
    
    print(f"\nGraph: {graph_data['graph'].vcount():,} nodes, {graph_data['graph'].ecount():,} edges")
    print(f"\nMetadata structure:")
    print(f"  zones: {len(metadata.get('zone_metadata', {}))}")
    print(f"  streams: {len(metadata.get('streams', {})):,}")
    print(f"  lakes: {len(metadata.get('lakes', {})):,}")
    print(f"  wetlands: {len(metadata.get('wetlands', {})):,}")
    print(f"  manmade: {len(metadata.get('manmade', {})):,}")
    
    # Inspect each feature
    for feature_id in FEATURE_IDS:
        inspect_feature(feature_id, graph_data, metadata)


if __name__ == "__main__":
    main()
