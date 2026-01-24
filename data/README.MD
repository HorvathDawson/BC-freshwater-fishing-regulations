# Data Directory

## Getting the Data

To download the FWA (Freshwater Atlas) data from GeoBC, run this command from the `data/` directory:

```bash
cd data/
wget -r --no-parent ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/
```

This command will recursively download all files from the FWA Public directory without ascending to parent directories. It will create the directory structure `ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/` with all the data files inside.

## Extracting Required Files

After downloading, you need to extract the following zip files that are used by the processing scripts:

### 1. Extract FWA_BC (Lakes and Waterbodies)
```bash
cd ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/
unzip FWA_BC.zip
```

This creates the `FWA_BC/FWA_BC.gdb` geodatabase containing waterbody polygon layers.

### 2. Extract FWA_STREAM_NETWORKS_SP (Stream Networks)
```bash
unzip FWA_STREAM_NETWORKS_SP.zip
```

This creates the `FWA_STREAM_NETWORKS_SP/FWA_STREAM_NETWORKS_SP.gdb` geodatabase containing stream network data.

### 3. Wildlife Management Units (Already Extracted)
The file `WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg` is already in GeoPackage format and doesn't need extraction.

### Required Files Summary
The following files are needed by the processing scripts:
- `FWA_BC/FWA_BC.gdb` - Waterbody polygons (lakes, rivers, etc.)
- `FWA_STREAM_NETWORKS_SP/FWA_STREAM_NETWORKS_SP.gdb` - Stream network polylines
- `WAA_WILDLIFE_MGMT_UNITS_SVW.gpkg` - Wildlife management unit boundaries

These files are used by:
- `scripts/fwa_modules/graph_builder.py` - Builds network graph from stream data
- `scripts/fwa_modules/metadata_builder.py` - Enriches waterbody metadata

