# Satellite Imagery Sources — Investigation Notes

## Current Provider: ESRI World Imagery

| Property    | Value |
|-------------|-------|
| URL         | `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}` |
| Type        | XYZ raster tiles |
| Tile size   | 256 px |
| Max zoom    | 18 |
| Latency     | ~130–170 ms/tile |
| Licence     | Esri Master License Agreement (free for non-commercial display) |

Config lives in `src/components/Map.tsx` → `SATELLITE_CONFIG`.  
Change the four fields there to swap providers.

---

## BC Government Imagery Services (Investigated May 2025)

### 1. BCGW Public WMS (`openmaps.gov.bc.ca/geo/pub/wms`)
- **893 layers**, but ALL are vector/metadata overlays (cadastral, admin boundaries, etc.).
- **No satellite or aerial raster imagery** available through this endpoint.

### 2. SPOT 15m Authenticated WMS
| Property    | Value |
|-------------|-------|
| Endpoint    | `https://openmaps.gov.bc.ca/imagex/ecw_wms.dll?wms_spot15m` |
| GetCapabilities | Works publicly — lists layers |
| GetMap      | **Fails**: returns `CNCSSecurity::Init Authentication server is not set` |
| Licence     | **"Access Only"** — NOT Open Government Licence (OGL-BC) |
| Catalogue   | https://catalogue.data.gov.bc.ca/dataset/bcad8f9e-274b-4a29-9e06-d05def3a50c2 |

### 3. Landsat Authenticated WMS
- Endpoint: `ecw_wms.dll?wms_landsat`
- Same auth error as SPOT 15m. Also "Access Only" licence.

### 4. ERDAS Base WMS (publicly accessible — workaround)
| Property    | Value |
|-------------|-------|
| Endpoint    | `https://openmaps.gov.bc.ca/imagex/ecw_wms.dll` (no module param) |
| Layer       | `bc_s5l5_bc_2004_2006_bcalb_15m_panb321_enh` |
| Works?      | Yes — tiles render without auth |
| Latency     | **200–450 ms/tile** (too slow for interactive use) |
| Vintage     | 2004–2006 imagery |
| Max zoom    | ~13 (15m resolution, imagery degrades quickly) |

**Verdict**: Functional but unusably slow and outdated compared to ESRI.

### 5. USGS National Map (USA only)
- Fast XYZ tiles, but coverage stops at the border.
- Zoom 10+: blank 2.4 KB tiles for BC. Zoom 14+: 404 errors.

---

## How to Switch to BC Gov SPOT 15m (Once Access Is Granted)

1. **Contact the data custodian** (see below) to get WMS access credentials.
2. Update `SATELLITE_CONFIG` in `Map.tsx`:

```ts
const SATELLITE_CONFIG = {
    url: `https://openmaps.gov.bc.ca/imagex/ecw_wms.dll?wms_spot15m`
        + '&service=WMS&version=1.1.1&request=GetMap'
        + '&layers=LAYER_NAME_FROM_CAPABILITIES'
        + '&styles=default&format=image/jpeg'
        + '&srs=EPSG:3857&width=256&height=256'
        + '&bbox={bbox-epsg-3857}',
    tileSize: 256,
    attribution: 'Imagery: <a href="https://www2.gov.bc.ca/gov/content/data/geographic-data-services">GeoBC</a> SPOT 15m',
    maxzoom: 15,  // adjust based on actual resolution
};
```

3. If tile latency is poor, the `wms-cache-worker/` directory contains a ready-to-deploy
   Cloudflare Worker with KV caching that proxies WMS tiles. Deploy it and set
   `VITE_WMS_CACHE_URL` in the env files to route through the cache.

---

## Contact for BC Gov Imagery Access

| Field       | Value |
|-------------|-------|
| **Name**    | Angus Christian |
| **Email**   | Angus.Christian@gov.bc.ca |
| **Role**    | Custodian, SPOT Satellite Imagery |
| **Branch**  | GeoBC, Ministry of Citizens' Services |
| **Ask for** | Access to the SPOT 15m WMS (`ecw_wms.dll?wms_spot15m`) under the "Access Only" licence. Explain your use case (non-commercial fishing regulation map). Ask whether OGL-BC re-licensing is possible. |

The catalogue entry with full metadata and contact details:  
https://catalogue.data.gov.bc.ca/dataset/bcad8f9e-274b-4a29-9e06-d05def3a50c2
