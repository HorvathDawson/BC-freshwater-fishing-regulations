# Can I Fish This? — Mobile (iOS + Android)

A React Native (Expo) port of the [`webapp/`](../webapp) that renders the **same
UI** and **same data** as the web experience, but runs **fully offline** by
querying the on-device SQLite database produced by the pipeline
([`pipeline/deploy/mobile_sharder.py`](../pipeline/deploy/mobile_sharder.py)).

> **Design rule: port, don't reinvent.** Every screen, component, and piece of
> business logic mirrors its `webapp/src` counterpart 1:1. Framework-agnostic
> code (types, regulation resolution, feature helpers) is copied verbatim; only
> the *rendering* layer (DOM → React Native primitives) and the *data source*
> (HTTP `/api/resolve` → local SQLite) are re-implemented.

---

## 1. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  App.tsx  (single-screen shell, mirrors webapp App → Map)      │
│                                                                │
│  ┌────────────────────┐   overlays (absolute-positioned)       │
│  │   MapView          │   ┌───────────────┐ ┌────────────────┐ │
│  │  MapLibre Native   │   │  SearchBar     │ │  InfoPanel      │ │
│  │  + PMTiles (R2)    │   └───────────────┘ └────────────────┘ │
│  │  + Protomaps style │   ┌───────────────┐ ┌────────────────┐ │
│  └────────────────────┘   │ Disclaimer     │ │ Disambiguation │ │
│                           └───────────────┘ └────────────────┘ │
│                           ┌───────────────┐ ┌────────────────┐ │
│                           │ PdfViewer      │ │ SourceImage     │ │
│                           └───────────────┘ └────────────────┘ │
└──────────────────────────────────────────────────────────────┘
        │ tap fid / search                       │ read
        ▼                                        ▼
┌──────────────────────┐              ┌───────────────────────────┐
│  waterbodyDataService │─── SQL ────▶│  regulations.sqlite        │
│  regulationsService   │             │  (bundled asset, opened    │
│  searchService (FTS5) │             │   with expo-sqlite)        │
└──────────────────────┘              └───────────────────────────┘
```

### Data sources (parity with web)

| Concern            | Web (`webapp/`)                         | Mobile (this app)                              |
| ------------------ | --------------------------------------- | ---------------------------------------------- |
| Search index + regs| `tier0.json` (loaded at startup)        | `search` + `regulations`/`reg_sets` tables     |
| Tap resolution     | `/api/resolve` Worker (fids/reaches)    | `fids`/`polys`/`reaches` tables (local SQL)     |
| Map tiles          | PMTiles range-read from R2              | PMTiles range-read from R2 (MapLibre Native)   |
| Basemap style      | `@protomaps/basemaps`                   | same style JSON, reused                         |
| Bathymetry PDFs    | fetched `bathymetry/<pdf>` from R2      | bundled asset **or** fetched from R2 (fallback) |
| Source page PNGs   | fetched from R2                         | bundled asset **or** fetched from R2 (fallback) |

The SQLite schema is the single source of truth — see the docstring in
[`mobile_sharder.py`](../pipeline/deploy/mobile_sharder.py). The mobile data
services here reconstruct the exact same in-memory shapes that
`webapp/src/services/waterbodyDataService.ts` produces, so the ported UI
components consume identical props.

---

## 2. Tech stack

| Area        | Choice                                             | Why                                                        |
| ----------- | -------------------------------------------------- | ---------------------------------------------------------- |
| Runtime     | **Expo (bare-workflow / dev client)**              | Native modules (map, sqlite, pdf) need a custom dev build  |
| Language    | **TypeScript** (strict), same config family as web | 1:1 type reuse                                             |
| Map         | **`@maplibre/maplibre-react-native`**              | MapLibre Native == mobile twin of web `maplibre-gl`        |
| PMTiles     | MapLibre Native `pmtiles://` remote source         | reuse the exact R2 PMTiles, no re-tiling                   |
| DB          | **`expo-sqlite`** (FTS5 enabled)                   | opens the bundled `regulations.sqlite`, offline queries    |
| PDF         | **`react-native-pdf`**                             | render bathymetry depth-map sheets                         |
| Icons       | **`lucide-react-native`** + Iconify SVGs           | same icon set as web (`lucide-react`, `@iconify`)          |
| Search      | **`fuse.js`** (fuzzy) over the `search` table      | identical ranking logic to web SearchBar                   |
| Navigation  | none (single screen w/ overlays)                   | mirrors the web single-page layout                         |

> **Expo Go is not sufficient** — the map, sqlite, and pdf modules require a
> custom **dev client** (`npx expo run:ios` / `run:android`).

---

## 3. Folder layout

```
mobile/
├── app.json               # Expo config (name, icons, asset bundling, plugins)
├── package.json           # deps + scripts
├── tsconfig.json          # strict TS, path aliases
├── babel.config.js        # expo preset + reanimated
├── metro.config.js        # allow .sqlite/.pmtiles/.pdf asset extensions
├── index.ts               # registerRootComponent
├── App.tsx                # root shell (port of webapp App + Map orchestration)
├── assets/
│   ├── db/regulations.sqlite      # ← copied by scripts/sync-assets.mjs
│   ├── bathymetry/*.pdf           # ← optional bundled PDFs
│   └── source/*.png               # ← optional bundled synopsis page PNGs
├── scripts/
│   └── sync-assets.mjs    # copies sqlite/pdfs/pngs from ../output into assets/
└── src/
    ├── config.ts          # R2 base URL, shard version, feature flags
    ├── theme/tokens.ts    # colors/spacing ported from webapp CSS variables
    ├── types/             # ported TS types (regulations, reaches, search)
    ├── utils/             # featureUtils, sectionLabel (RN-safe ports)
    ├── data/
    │   ├── database.ts            # open/copy bundled sqlite
    │   ├── waterbodyDataService.ts# facade matching web contract
    │   ├── regulationsService.ts  # reg resolution + provenance (port)
    │   └── searchService.ts       # FTS5 + fuse.js search (port)
    ├── map/
    │   ├── style.ts       # protomaps basemap + waterbody overlay style
    │   └── MapView.tsx    # MapLibre Native + PMTiles + tap handling
    └── components/        # InfoPanel, SearchBar, Disclaimer,
                           # DisambiguationMenu, PdfViewer, SourceImageViewer,
                           # IssueReport  (RN ports of webapp/src/components)
```

Each component file header cites the exact `webapp/src/...` source it ports.

---

## 4. Component port map (webapp → mobile)

| webapp/src                              | mobile/src                          | Notes                                        |
| --------------------------------------- | ----------------------------------- | -------------------------------------------- |
| `App.tsx` + `components/Map.tsx`        | `App.tsx` + `map/MapView.tsx`       | orchestration + map                          |
| `components/InfoPanel.tsx`              | `components/InfoPanel.tsx`          | regulation cards; ScrollView + bottom sheet  |
| `components/SearchBar.tsx`              | `components/SearchBar.tsx`          | fuse.js ranking + "X km from town" (same)    |
| `components/DisambiguationMenu.tsx`     | `components/DisambiguationMenu.tsx` | overlapping-feature picker                    |
| `components/Disclaimer.tsx`             | `components/Disclaimer.tsx`         | first-run legal modal                         |
| `components/PdfViewer.tsx`              | `components/PdfViewer.tsx`          | `react-native-pdf`                            |
| `components/SourceImageViewer.tsx`      | `components/SourceImageViewer.tsx`  | pinch-zoom `Image`                            |
| `components/IssueReport.tsx`            | `components/IssueReport.tsx`        | feedback → same Worker email endpoint         |
| `services/waterbodyDataService.ts`      | `data/waterbodyDataService.ts`      | HTTP → SQLite                                 |
| `services/regulationsService.ts`        | `data/regulationsService.ts`        | copied ~verbatim                              |
| `utils/featureUtils.ts`                 | `utils/featureUtils.ts`             | `window.innerWidth` → `Dimensions`            |
| `utils/sectionLabel.ts`                 | `utils/sectionLabel.ts`             | verbatim                                      |
| `map/styles.ts`                         | `map/style.ts`                      | reused, RN source syntax                      |

---

## 5. Build & run

```bash
cd mobile
npm install

# 1. Copy the pipeline-built data + assets into the app bundle
npm run sync-assets            # copies regulations.sqlite (+ optional pdfs/pngs)

# 2. Launch a native dev build (Expo Go will NOT work — native modules)
npm run ios                    # or: npm run android
```

`sync-assets` reads from `../output/pipeline/deploy/mobile/v<N>/` (the mobile
shard) and `../output/pipeline/deploy/bathymetry` / `.../source`. Regenerate
those with `python -m pipeline --step enrich` before syncing.

---

## 6. Isolation guarantee

This folder is **fully self-contained**. It has its own `package.json` /
`node_modules` and does **not** import from, modify, or is imported by
`webapp/`, `r2-worker/`, `pipeline/`, or any build. Deleting `mobile/` leaves
the rest of the repo untouched. The only shared artifacts are **read-only**:
the R2 PMTiles/PDFs and the pipeline's `mobile/` SQLite output.

---

## 7. Status / roadmap

- [x] Project scaffold, config, plan
- [x] Ported types, theme tokens, feature utils
- [x] SQLite data layer (schema-faithful services)
- [x] Component skeletons (structural ports of every webapp component)
- [ ] MapLibre Native + PMTiles wiring verified on device
- [ ] `react-native-pdf` + image viewer verified
- [ ] Asset bundling size tuning (bundle vs. remote fetch policy)
- [ ] End-to-end parity pass vs. `webapp`
