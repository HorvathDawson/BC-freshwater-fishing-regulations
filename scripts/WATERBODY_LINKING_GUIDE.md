# Waterbody Linking Guide

Links fishing regulations from the synopsis to GIS waterbody features.

## Features

✅ **Modular Design** - Run entire pipeline or individual steps  
✅ **GDB Export** - Export matched features to geodatabase for GIS software  
✅ **Reverse Index** - Look up regulations by feature ID  
✅ **Name Corrections** - Region-specific mappings for spelling variations  
✅ **Validation** - Management unit matching and warning logs  

## Quick Start

```bash
# Run full pipeline (matching + reverse index + GDB export)
python waterbody_linking.py

# Run only matching step
python waterbody_linking.py --step match

# Export to GDB (requires matched_waterbodies.json)
python waterbody_linking.py --step export-gdb

# Create reverse index only
python waterbody_linking.py --step reverse-index

# Skip GDB export in full pipeline
python waterbody_linking.py --skip-gdb
```

## Pipeline Steps

### 1. **Matching** (`--step match`)

Links waterbody regulations to GIS features:
- Applies name corrections from `NAME_CORRECTIONS` dictionary
- Matches across management unit zones
- Validates management unit consistency
- Tracks KML point matching

**Outputs:**
- `matched_waterbodies.json` - Full matching results
- `unmatched_waterbodies.csv` - Waterbodies without GIS matches
- `matching_warnings.log` - MU validation warnings
- `linking_warnings.log` - KML and correction target issues

### 2. **Reverse Index** (`--step reverse-index`)

Creates feature_id → regulations lookup:

```json
{
  "FWLKP_123456": [
    {
      "region": "REGION 2 - Lower Mainland",
      "waterbody_key": "ALOUETTE LAKE",
      "original_names": ["Alouette Lake"],
      "regulations": [...],
      "feature_type": "lake",
      "feature_zone": "2",
      "matched_on_name": "alouette lake"
    }
  ]
}
```

**Output:**
- `feature_regulation_index.json`

**Use case:** Given a feature ID from a map click, look up the applicable regulations.

### 3. **GDB Export** (`--step export-gdb`)

Creates geodatabase with three feature classes:
- **streams** - LineString/MultiLineString features
- **points** - Point features (KML labeled lakes)
- **polygons** - Polygon/MultiPolygon features (lakes, wetlands, manmade)

**Key attributes:**
- `FEATURE_ID` - Unique identifier
- `REGION` - Regulation region
- `WATERBODY` - Waterbody name from regulations
- `ORIG_NAMES` - Original name variations
- `FEATURE_TYP` - Type (lake, stream, wetland, manmade, point)
- `REGS_JSON` - Full regulation data as JSON string
- `GNIS_NAME` - Official geographic name
- `WB_POLY_ID` - Polygon ID for linked features

**Output:**
- `matched_waterbodies.gdb/` - Geodatabase folder

**Requirements:**
```bash
pip install fiona shapely
```

## Input Files

### Required
- `output/llm_parser/grouped_results.json` - Parsed regulations grouped by waterbody
- `output/fwa_preprocessing/waterbody_index.json` - GIS feature index with geometries

### Configuration
- `NAME_CORRECTIONS` dictionary in script - Region-specific name mappings

## Output Files

```
output/waterbody_linking/
├── matched_waterbodies.json          # Full matching results
├── unmatched_waterbodies.csv         # Unmatched waterbodies with suggestions
├── matching_warnings.log             # MU validation warnings
├── linking_warnings.log              # KML point and correction issues
├── feature_regulation_index.json     # Reverse index (feature → regs)
└── matched_waterbodies.gdb/          # Geodatabase export
    ├── streams                        # Stream features
    ├── points                         # Point features
    └── polygons                       # Lake/wetland/manmade polygons
```

## Name Corrections

Region-specific mappings in `NAME_CORRECTIONS` handle:
- **Spelling variations** - "Toquart Lake" → "Toquaht Lake"
- **Name order** - "Stowell Lake" → "Lake Stowell"
- **Plural variations** - "Connor Lake" → "Connor Lakes"
- **Split entries** - "Chilliwack / Vedder Rivers" → separate rivers
- **Wildcard patterns** - "* Lake's Tributaries" → "* lake tributary"

### Adding a correction:

```python
NAME_CORRECTIONS = {
    "REGION 2 - Lower Mainland": {
        "JONES LAKE": NameCorrection(
            target_names=["wahleach lake"],
            note="Labelled as Wahleach Lake in GIS"
        ),
    }
}
```

### Ignoring a waterbody:

```python
"SEVEN MILE RESERVOIR": NameCorrection(
    target_names=[],
    note="Dammed portion of Pend d'Oreille River - covered by river regulations",
    ignored=True
),
```

## Advanced Usage

### Custom input file
```bash
python waterbody_linking.py --input path/to/custom_grouped_results.json
```

### Run matching only, then review before export
```bash
python waterbody_linking.py --step match
# Review matched_waterbodies.json
python waterbody_linking.py --step export-gdb
```

### Skip GDB export (faster for large datasets)
```bash
python waterbody_linking.py --skip-gdb
```

## Using the GDB in ArcGIS/QGIS

1. **Open the GDB** in your GIS software
2. **Add layers** - streams, points, polygons
3. **Query regulations** - Parse `REGS_JSON` field or use attribute table
4. **Filter by region** - Use `REGION` field
5. **Spatial queries** - Find features intersecting an area

### Example: View regulations in QGIS

1. Add `polygons` layer from GDB
2. Open attribute table
3. Add calculated field to parse JSON:
   ```python
   json_extract(REGS_JSON, '$.0.open_season')
   ```

## Reverse Index Usage

```python
import json

# Load reverse index
with open("output/waterbody_linking/feature_regulation_index.json") as f:
    index = json.load(f)

# Look up feature by ID
feature_id = "FWLKP_123456"
regulations = index.get(feature_id, [])

for reg_link in regulations:
    print(f"Waterbody: {reg_link['waterbody_key']}")
    print(f"Region: {reg_link['region']}")
    for reg in reg_link['regulations']:
        print(f"  Season: {reg.get('open_season')}")
        print(f"  Quota: {reg.get('quota')}")
```

## Troubleshooting

### "Error: fiona and shapely required"
```bash
conda install -c conda-forge fiona shapely
# or
pip install fiona shapely
```

### "Skipping X features with missing/invalid geometry"
- Check waterbody_index.json has valid WKT geometries
- Verify feature IDs match between matched results and index

### High unmatched count
1. Check `unmatched_waterbodies.csv` for suggestions
2. Add corrections to `NAME_CORRECTIONS`
3. Review `linking_warnings.log` for systematic issues

### MU validation warnings
- Feature is in correct zone but MU field doesn't match regulation
- May indicate data quality issues or zone boundary changes
- Review `matching_warnings.log` for patterns

## Performance

- **Matching**: ~30 seconds for full BC dataset (~1000 waterbodies)
- **Reverse index**: ~5 seconds
- **GDB export**: ~2-5 minutes (depends on feature count and geometry complexity)

## Data Flow

```
grouped_results.json ──┐
                       ├──> MATCHING ──> matched_waterbodies.json ──┬──> REVERSE INDEX ──> feature_regulation_index.json
waterbody_index.json ──┘                                            └──> GDB EXPORT ──────> matched_waterbodies.gdb
```
