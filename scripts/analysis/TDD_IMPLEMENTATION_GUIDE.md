# TDD Implementation Guide - Waterbody Linking MVP

## Overview

This guide walks through implementing the MVP waterbody linking system using **Test-Driven Development (TDD)**. We build incrementally, writing tests first, then implementing minimal code to pass.

---

## TDD Philosophy

```
1. RED:   Write a failing test
2. GREEN: Write minimal code to make it pass
3. REFACTOR: Clean up code while keeping tests passing
4. REPEAT
```

**Benefits**:
- **Confidence**: Every feature has tests
- **Design**: Tests drive good API design
- **Documentation**: Tests show how to use the code
- **Regression prevention**: Changes don't break existing features

---

## Implementation Roadmap

### Phase 1: Foundation (CURRENT)
**Goal**: Basic waterbody linking with single matches

**Status**: ✅ IMPLEMENTED

**What We Built**:
```
fwa_modules/linking/
├── __init__.py
├── waterbody_linker.py
    ├── FWAFeature (dataclass)
    ├── LinkingResult (dataclass)
    ├── LinkStatus (enum)
    ├── FWAGazetteer (class)
    │   ├── __init__(features)
    │   ├── search(name, region)
    │   └── add_feature(feature)
    └── WaterbodyLinker (class)
        ├── __init__(gazetteer, name_variations)
        ├── link_waterbody(waterbody_key, region)
        ├── _apply_name_variations(key, region)
        └── _normalize_name(name)
```

**Test Coverage**: 22 tests covering:
- ✅ Single match success
- ✅ Multiple match disambiguation
- ✅ Not found cases
- ✅ Name variations
- ✅ Name normalization
- ✅ Edge cases

**How to Run Tests**:
```bash
cd scripts
pytest tests/test_waterbody_linker.py -v
```

Expected output:
```
test_waterbody_linker.py::test_link_single_match_exact_name PASSED
test_waterbody_linker.py::test_link_single_match_case_insensitive PASSED
...
===================== 22 passed in 0.5s =====================
```

---

### Phase 2: Real FWA Gazetteer Integration (COMPLETE)
**Goal**: Load waterbody data from metadata pickle file

**TDD Steps**:

**Phase 2 is complete** - We use the existing `stream_metadata.pickle` file!

No additional work needed. The `MetadataGazetteer` already loads all waterbody data from the metadata file.

---

### Phase 3: Tributary Enrichment (CURRENT)
**Goal**: Implement graph traversal to find all tributary segments

**Data Sources**:
- **Graph topology**: `fwa_bc_primal_full.gpickle` (network structure)
- **Feature metadata**: `stream_metadata.pickle` (names, zones, etc.)
- **Note**: Do NOT use `stream_tributary_of` or `lake_tributary_of` fields from metadata - these won't work for our purpose. Use graph traversal only.

**TDD Steps**:

#### Step 1: Write Tests for Graph Loader

Create `tests/test_graph_loader.py`:

```python
import pytest
from fwa_modules.linking import FWANetworkGraph
from pathlib import Path

def test_load_graph_from_pickle():
    """Test: Can load network graph from pickle file."""
    graph_path = Path("output/fwa_modules/fwa_bc_primal_full.gpickle")
    
    graph = FWANetworkGraph.from_pickle(graph_path)
    
    assert graph.node_count() > 0
    assert graph.edge_count() > 0

def test_find_upstream_tributaries():
    """Test: Can find all upstream tributaries of a waterbody."""
    graph_path = Path("output/fwa_modules/fwa_bc_primal_full.gpickle")
    graph = FWANetworkGraph.from_pickle(graph_path)
    
    # Get segments for Elk River (from metadata)
    elk_river_segments = ["100001", "100002"]  # Example IDs
    
    tributaries = graph.find_upstream_segments(elk_river_segments)
    
    # Should return upstream segment IDs
    assert len(tributaries) > 0
    assert all(isinstance(seg_id, str) for seg_id in tributaries)

def test_exclude_main_stem_for_tributaries_only():
    """Test: TRIBUTARIES_ONLY excludes main stem."""
    graph_path = Path("output/fwa_modules/fwa_bc_primal_full.gpickle")
    graph = FWANetworkGraph.from_pickle(graph_path)
    
    main_stem_segments = ["100001", "100002"]
    
    tributaries = graph.find_upstream_segments(
        main_stem_segments, 
        exclude_source=True
    )
    
    # Main stem segments should not be included
    for seg_id in main_stem_segments:
        assert seg_id not in tributaries
```

#### Step 4: Implement Graph Loader

Create `fwa_modules/linking/graph_loader.py`:

```python
import igraph as ig
import pickle
from typing import List, Set
from pathlib import Path
from .waterbody_linker import FWAFeature

class FWANetworkGraph:
    """
    Network graph of FWA stream connectivity.
    
    Enables upstream/downstream traversal for tributary enrichment.
    Uses the existing fwa_bc_primal_full.gpickle file.
    
    IMPORTANT: Do NOT use stream_tributary_of or lake_tributary_of fields 
    from metadata - these won't work for our purpose. Use graph topology only.
    """
    
    def __init__(self, graph: ig.Graph):
        self.graph = graph
    
    @classmethod
    def from_pickle(cls, pickle_path: Path) -> 'FWANetworkGraph':
        """Load graph from existing FWA pickle file."""
        with open(pickle_path, 'rb') as f:
            graph_data = pickle.load(f)
        return cls(graph_data)
    
    def find_upstream_tributaries(self, 
                                  waterbody: FWAFeature,
                                  exclude_mainstem: bool = False) -> List[FWAFeature]:
        """
        Find all upstream tributary segments.
        
        Args:
            waterbody: Starting waterbody
            exclude_mainstem: If True, exclude main stem (for TRIBUTARIES_ONLY)
        
        Returns:
            List of tributary FWAFeature objects
        """
        visited = set()
        tributaries = []
        
        # BFS upstream traversal
        # (Implementation details)
        
        return tributaries
```

---

### Phase 4: Exclusion Processing (AFTER Phase 3)
**Goal**: Link and categorize exclusions (MVP doesn't apply them yet)

**TDD Steps**:

#### Step 1: Write Tests for Exclusion Processing

Create `tests/test_exclusion_processor.py`:

```python
def test_process_exclusions_single_match_ready():
    """Test: Single match exclusion marked as ready_to_activate."""
    parsed_waterbody = create_test_parsed_waterbody(
        waterbody_key="ELK RIVER",
        exclusions=[
            ScopeObject(
                type="WHOLE_SYSTEM",
                waterbody_key="MICHEL CREEK",
                includes_tributaries=True,
                ...
            )
        ]
    )
    
    processor = ExclusionProcessor(linker)
    result = processor.process_exclusions(parsed_waterbody)
    
    assert len(result["ready_to_activate"]) == 1
    assert result["ready_to_activate"][0]["waterbody_key"] == "MICHEL CREEK"
    assert len(result["needs_review"]) == 0

def test_process_exclusions_ambiguous_needs_review():
    """Test: Ambiguous exclusion marked as needs_review."""
    parsed_waterbody = create_test_parsed_waterbody(
        exclusions=[
            ScopeObject(
                waterbody_key="MILL CREEK",  # Ambiguous
                ...
            )
        ]
    )
    
    processor = ExclusionProcessor(linker)
    result = processor.process_exclusions(parsed_waterbody)
    
    assert len(result["ready_to_activate"]) == 0
    assert len(result["needs_review"]) == 1
    assert result["needs_review"][0]["reason"] == "ambiguous"
```

#### Step 2: Implement Exclusion Processor

Create `fwa_modules/linking/exclusion_processor.py`:

```python
from typing import Dict, Any, List
from synopsis_pipeline.models import ParsedWaterbody, ScopeObject
from .waterbody_linker import WaterbodyLinker, LinkStatus

class ExclusionProcessor:
    """
    Process exclusions from parsed regulations.
    
    MVP: Link and categorize only (don't apply).
    """
    
    def __init__(self, linker: WaterbodyLinker):
        self.linker = linker
    
    def process_exclusions(self, 
                          parsed_waterbody: ParsedWaterbody) -> Dict[str, Any]:
        """
        Link and categorize exclusions.
        
        Returns:
            {
                "ready_to_activate": [...],  # Single match exclusions
                "needs_review": [...]        # Ambiguous/not found
            }
        """
        ready = []
        needs_review = []
        
        for exclusion_scope in parsed_waterbody.identity.exclusions:
            result = self.linker.link_waterbody(
                exclusion_scope.waterbody_key,
                parsed_waterbody.identity.location_descriptor
            )
            
            if result.is_success:
                ready.append({
                    "scope_object": exclusion_scope,
                    "fwa_feature": result.matched_feature,
                    "waterbody_key": exclusion_scope.waterbody_key,
                    "includes_tributaries": exclusion_scope.includes_tributaries
                })
            else:
                needs_review.append({
                    "scope_object": exclusion_scope,
                    "waterbody_key": exclusion_scope.waterbody_key,
                    "candidates": result.candidate_features,
                    "reason": result.status.value
                })
        
        return {
            "ready_to_activate": ready,
            "needs_review": needs_review
        }
```

---

## Current Status & Next Steps

### ✅ Completed (Phase 1)
- [x] FWAFeature data model
- [x] LinkingResult with status tracking
- [x] In-memory FWAGazetteer
- [x] WaterbodyLinker with name variations
- [x] Name normalization (possessive, abbreviations)
- [x] 22 comprehensive tests
- [x] **MetadataGazetteer** - Loads from stream_metadata.pickle
- [x] Integration with existing FWA metadata pipeline

### 🚧 Next: Phase 2 (Real FWA Integration) - UPDATED

**Good news**: We already have real FWA data through `stream_metadata.pickle`!

The metadata file (generated by `fwa_modules/metadata_builder.py`) contains:
- **Streams**: All stream edges with names, zones, tributary relationships
- **Lakes**: Polygon features with names and zones
- **Wetlands**: Wetland features
- **Manmade**: Manmade waterbodies
- **Zone metadata**: Management units and regions

**To use the metadata gazetteer**:

1. **Ensure metadata file exists**:
   ```bash
   ls -la scripts/output/fwa_modules/stream_metadata.pickle
   ```

2. **Create gazetteer from metadata**:
   ```python
   from pathlib import Path
   from fwa_modules.linking import MetadataGazetteer, WaterbodyLinker
   
   # Load gazetteer from metadata
   metadata_path = Path("output/fwa_modules/stream_metadata.pickle")
   gazetteer = MetadataGazetteer(metadata_path)
   
   # Create linker
   linker = WaterbodyLinker(gazetteer)
   
   # Link waterbody
   result = linker.link_waterbody("Elk River", region="Region 4")
   
   if result.is_success:
       # Get full metadata
       stream_meta = gazetteer.get_stream_metadata(result.matched_feature.fwa_id)
       print(f"Zones: {stream_meta['zones']}")
       print(f"Management Units: {stream_meta['mgmt_units']}")
   ```

3. **Run tests**:
   ```bash
   # Test in-memory gazetteer
   pytest tests/test_waterbody_linker.py -v
   
   # Test metadata gazetteer
   pytest tests/test_metadata_gazetteer.py -v
   ```

**Phase 2 is now simplified** - Instead of building FWA database connection, we:
- ✅ Use existing metadata pipeline
- ✅ Fast in-memory lookups (pickle file is pre-indexed)
- ✅ Already have zone assignments
- ✅ Already have tributary relationships
- ✅ Can proceed directly to Phase 3 (Graph Traversal)

### 📋 Upcoming: Phase 3 & 4
- Graph builder for tributary traversal
- Exclusion processor
- End-to-end integration tests
- Performance optimization

---

## Running the Tests

### All Tests
```bash
cd scripts
pytest tests/ -v
```

### Specific Test File
```bash
pytest tests/test_waterbody_linker.py -v
```

### Single Test
```bash
pytest tests/test_waterbody_linker.py::test_link_single_match_exact_name -v
```

### With Coverage Report
```bash
pytest tests/ --cov=fwa_modules --cov-report=html
```

---

## Development Workflow

### Adding a New Feature

1. **Write Test First** (RED):
   ```python
   def test_new_feature():
       """Test description."""
       linker = WaterbodyLinker(...)
       result = linker.new_method(...)
       assert result == expected
   ```

2. **Run Test** (should fail):
   ```bash
   pytest tests/test_waterbody_linker.py::test_new_feature -v
   ```

3. **Implement Feature** (GREEN):
   ```python
   def new_method(self, ...):
       # Minimal implementation to pass test
       return ...
   ```

4. **Run Test** (should pass):
   ```bash
   pytest tests/test_waterbody_linker.py::test_new_feature -v
   ```

5. **Refactor** (keep tests passing):
   - Clean up code
   - Add documentation
   - Optimize if needed
   - Run tests after each change

6. **Commit**:
   ```bash
   git add fwa_modules/ tests/
   git commit -m "feat: add new_feature with tests"
   ```

---

## Key TDD Principles for This Project

1. **Test First**: Always write the test before implementing
2. **One Test at a Time**: Focus on one failing test
3. **Minimal Code**: Only write code needed to pass the test
4. **Refactor Fearlessly**: Tests give you confidence to improve code
5. **Fast Tests**: Keep tests fast so you run them often
6. **Isolated Tests**: Each test should be independent
7. **Clear Names**: Test names should describe what they test
8. **Arrange-Act-Assert**: Structure tests clearly

---

## Success Metrics

### Phase 1 (Current)
- ✅ 22 tests passing
- ✅ 100% coverage of WaterbodyLinker core methods
- ✅ All MVP linking scenarios covered

### Phase 2 (Target)
- ✅ FWA database connection working
- ✅ Can search real FWA data
- ✅ Integration tests with real waterbodies
- ✅ Performance: < 100ms per lookup

### Phase 3 (Target)
- ✅ Graph traversal working
- ✅ Can find all tributaries of Elk River
- ✅ TRIBUTARIES_ONLY scope implemented
- ✅ Performance: < 1s for 5000-segment network

### Phase 4 (Target)
- ✅ Exclusion linking working
- ✅ Ready vs needs_review categorization
- ✅ End-to-end test with full ParsedWaterbody
- ✅ Complete MVP feature set operational

---

## Troubleshooting

### Tests Failing After Changes
```bash
# Run with verbose output to see details
pytest tests/test_waterbody_linker.py -v -s

# Run with debugger on failure
pytest tests/test_waterbody_linker.py --pdb
```

### Import Errors
```bash
# Make sure you're in the scripts directory
cd scripts

# Add parent directory to PYTHONPATH if needed
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Metadata and Graph File Issues
```bash
# Check FWA metadata pickle file exists
ls -la scripts/output/fwa_modules/stream_metadata.pickle

# Check FWA graph pickle file exists  
ls -la scripts/output/fwa_modules/fwa_bc_primal_full.gpickle

# Verify pickle files can be loaded
python -c "import pickle; f = open('scripts/output/fwa_modules/stream_metadata.pickle', 'rb'); data = pickle.load(f); print(data.keys())"
```
