# Project Restructure Summary

## Overview
The project has been reorganized from a nested `scripts/` structure to a clean, top-level pipeline architecture for better usability and clarity.

## New Directory Structure

```
BC-freshwater-fishing-regulations/
├── synopsis_pipeline/          # Extract and parse fishing synopsis PDF
│   ├── extract_synopsis.py
│   ├── parse_synopsis.py
│   ├── compare_sessions.py
│   └── prompts/
│
├── fwa_pipeline/              # Build FWA graph and metadata
│   └── graph/
│       ├── graph_builder.py   # Build primal graph from FWA data
│       └── metadata_builder.py # Build metadata gazetteer
│
├── regulation_mapping/        # Link regulations to geography
│   ├── linker.py
│   ├── regulation_mapper.py
│   ├── regulation_pipeline.py
│   ├── metadata_gazetteer.py
│   ├── scope_filter.py
│   ├── tributary_enricher.py
│   ├── geo_exporter.py
│   └── test_*.py
│
├── webapp/                    # Can I Fish This? web application (canifishthis.ca)
│   ├── src/
│   ├── public/
│   ├── index.html
│   └── package.json
│
├── tests/                     # All tests
│   ├── test_graph_builder.py
│   ├── test_full_pipeline.py
│   └── ...
│
├── output/                    # Centralized outputs
│   ├── synopsis/             # Extract + parse outputs
│   │   ├── extract_synopsis/
│   │   ├── parse_synopsis/
│   │   └── fishing_synopsis.pdf
│   ├── fwa/                  # Graph, metadata, geo exports
│   │   ├── fwa_bc_primal_full.gpickle
│   │   ├── stream_metadata.pickle
│   │   ├── regulations_merged.gpkg
│   │   ├── regulations_merged.pmtiles
│   │   ├── regulations.json
│   │   ├── search_index.json
│   │   └── temp/
│   └── regulation_mapping/   # Future regulation mapping outputs
│
├── config.yaml               # Centralized configuration
├── project_config.py         # Configuration manager (imported by all pipelines)
├── requirements.txt          # Python dependencies
├── pytest.ini               # Test configuration
└── README.md
```

## Key Changes

### 1. **Flattened Structure**
- **Before:** `scripts/synopsis_pipeline/`, `scripts/fwa_modules/graph/`, `scripts/fwa_modules/linking/`
- **After:** `synopsis_pipeline/`, `fwa_pipeline/`, `regulation_mapping/` at top level

### 2. **Organized Outputs**
- **Before:** `scripts/output/extract_synopsis/`, `scripts/output/parse_synopsis/`, `scripts/output/fwa_modules/`
- **After:** `output/synopsis/`, `output/fwa/`, `output/regulation_mapping/`

### 3. **Centralized Configuration**
- Created `config.yaml` at top level with all path configurations
- Updated `synopsis_pipeline/config.yaml` paths to match new structure
- All hardcoded paths updated to new locations

### 4. **Updated Imports**
All Python imports have been updated:
- `from fwa_modules.graph_builder import ...` → `from fwa_pipeline.graph_builder import ...`
- `from fwa_modules.linking import ...` → `from regulation_mapping import ...`
- Path references updated in config loaders

### 5. **Moved Files**
- `scripts/requirements.txt` → `requirements.txt`
- `scripts/pytest.ini` → `pytest.ini`
- `scripts/tests/` → `tests/`
- Config files remain at appropriate levels

## Benefits

1. **Clearer Organization:** Three distinct pipelines are immediately visible
2. **Easier Execution:** Run pipelines from top level without navigating nested folders
3. **Better Output Management:** Outputs organized by pipeline stage in `output/`
4. **Simpler Paths:** No more `../output/` or `scripts/output/` confusion
5. **Professional Structure:** Standard Python project layout

## Running Pipelines

All pipeline scripts now display clear input/output file information when run, showing:
- 📁 Input files being processed
- 📁 Output files that will be created
- ⚙️ Configuration settings being used

### Synopsis Pipeline
```bash
# Extract raw data from PDF
python -m synopsis_pipeline.extract_synopsis

# Parse regulations using LLM
python -m synopsis_pipeline.parse_synopsis

# Resume parsing (continues from last save point)
python -m synopsis_pipeline.parse_synopsis --resume
```

### FWA Pipeline
```bash
# Build stream network graph from GDB
python -m fwa_pipeline.graph_builder

# Extract metadata and assign zones
python -m fwa_pipeline.metadata_builder
```

### Regulation Mapping
```bash
# Full pipeline with all exports (default)
python -m regulation_mapping.regulation_pipeline

# Mapping only (no geometry export) - shows detailed statistics
python -m regulation_mapping.regulation_pipeline --map-only --verbose

# Export merged geometries only
python -m regulation_mapping.regulation_pipeline --merged-only

# Export individual geometries only
python -m regulation_mapping.regulation_pipeline --individual-only

# Show all options
python -m regulation_mapping.regulation_pipeline --help
```

## Configuration

The main `config.yaml` contains all path settings. Individual pipelines can override with their own configs:
- `synopsis_pipeline/config.yaml` - LLM settings, API keys, output paths
- Per-pipeline settings can be added as needed

## Notes

- Tests may need path updates (marked for later)
- Old `scripts/` directory has been removed
- All code logic remains unchanged - only structure and paths modified
- `.geom_cache` moved to top level for shared access
