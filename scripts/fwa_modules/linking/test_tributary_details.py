#!/usr/bin/env python3
"""
Test Tributary Details

Inspect tributary enrichment for a specific waterbody to validate results.
Shows base features, excluded watershed codes, and found tributaries.
"""

import argparse
import json
from pathlib import Path
from .metadata_gazetteer import MetadataGazetteer
from .linker import WaterbodyLinker
from .name_variations import (
    NAME_VARIATIONS,
    DIRECT_MATCHES,
    SKIP_ENTRIES,
    UNMARKED_WATERBODIES,
    ManualCorrections,
)
from .tributary_enricher import TributaryEnricher


def main():
    parser = argparse.ArgumentParser(
        description="Inspect tributary enrichment for a specific waterbody"
    )
    parser.add_argument("waterbody_name", help="Name of waterbody to inspect")
    parser.add_argument(
        "--gazetteer",
        type=Path,
        default=Path("output/fwa_modules/stream_metadata.pickle"),
        help="Path to stream metadata pickle file",
    )
    parser.add_argument(
        "--graph",
        type=Path,
        default=Path("output/fwa_modules/fwa_bc_primal_full.gpickle"),
        help="Path to FWA graph pickle file",
    )

    args = parser.parse_args()

    print("=" * 80)
    print(f"TRIBUTARY ENRICHMENT TEST: {args.waterbody_name}")
    print("=" * 80)

    # Load gazetteer
    print(f"\nLoading gazetteer from: {args.gazetteer}")
    gazetteer = MetadataGazetteer(args.gazetteer)

    # Load graph
    print(f"Loading graph from: {args.graph}")
    tributary_enricher = TributaryEnricher(graph_source=args.graph)

    # Link waterbody
    manual_corrections = ManualCorrections(
        name_variations=NAME_VARIATIONS,
        direct_matches=DIRECT_MATCHES,
        skip_entries=SKIP_ENTRIES,
        unmarked_waterbodies=UNMARKED_WATERBODIES,
    )
    linker = WaterbodyLinker(gazetteer=gazetteer, manual_corrections=manual_corrections)

    print(f"\nLinking '{args.waterbody_name}'...")
    link_result = linker.link_waterbody(args.waterbody_name)

    if link_result.status.value != "success":
        print(f"ERROR: Failed to link waterbody - {link_result.status.value}")
        return

    base_features = link_result.features
    print(f"✓ Found {len(base_features)} base features")

    # Collect watershed codes
    watershed_codes = set()
    for f in base_features:
        wc = getattr(f, "fwa_watershed_code", None)
        if wc:
            watershed_codes.add(wc)

    print(f"\nBase Feature Details:")
    print(f"  Total features: {len(base_features)}")
    print(f"  Unique watershed codes: {len(watershed_codes)}")
    print(f"  Watershed codes: {sorted(watershed_codes)[:5]}")
    if len(watershed_codes) > 5:
        print(f"    ... and {len(watershed_codes) - 5} more")

    # Show sample base features
    print(f"\nSample Base Features (first 5):")
    for i, f in enumerate(base_features[:5]):
        print(f"  {i+1}. {f.fwa_id} - {f.gnis_name or 'unnamed'}")
        print(f"     Watershed: {f.fwa_watershed_code}")
        print(f"     Type: {f.geometry_type}")

    # Enrich with tributaries
    print(f"\nEnriching with tributaries...")
    tributaries = tributary_enricher.enrich_with_tributaries(base_features, {}, {})

    print(f"✓ Found {len(tributaries)} tributary features")

    if tributaries:
        # Analyze tributary watershed codes
        trib_watershed_codes = set()
        trib_names = set()
        for t in tributaries:
            wc = t.get("fwa_watershed_code")
            if wc:
                trib_watershed_codes.add(wc)
            name = t.get("gnis_name")
            if name:
                trib_names.add(name)

        print(f"\nTributary Details:")
        print(f"  Total tributaries: {len(tributaries)}")
        print(f"  Unique watershed codes: {len(trib_watershed_codes)}")
        print(f"  Unique names: {len(trib_names)}")

        # Verify no overlap with base watershed codes
        overlap = watershed_codes & trib_watershed_codes
        if overlap:
            print(
                f"\n⚠️  WARNING: {len(overlap)} watershed codes appear in BOTH base and tributaries!"
            )
            print(
                f"  This should not happen - tributaries should have different watershed codes"
            )
            print(f"  Overlapping codes: {sorted(overlap)[:5]}")
        else:
            print(f"\n✓ No watershed code overlap (correct)")

        # Show sample tributaries
        print(f"\nSample Tributaries (first 10):")
        for i, t in enumerate(tributaries[:10]):
            print(
                f"  {i+1}. {t['linear_feature_id']} - {t.get('gnis_name') or 'unnamed'}"
            )
            print(f"     Watershed: {t['fwa_watershed_code']}")
            print(f"     Length: {t.get('length', 0):.0f}m")

        # Group by name
        if trib_names:
            print(f"\nNamed Tributaries Found:")
            name_counts = {}
            for t in tributaries:
                name = t.get("gnis_name")
                if name:
                    name_counts[name] = name_counts.get(name, 0) + 1

            for name, count in sorted(
                name_counts.items(), key=lambda x: x[1], reverse=True
            )[:20]:
                print(f"  {name}: {count} segments")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Base features:        {len(base_features)}")
    print(f"Watershed codes:      {len(watershed_codes)}")
    print(f"Tributaries found:    {len(tributaries)}")
    print(f"Tributary enrichment: {len(tributaries) / len(base_features):.1f}x")


if __name__ == "__main__":
    main()
