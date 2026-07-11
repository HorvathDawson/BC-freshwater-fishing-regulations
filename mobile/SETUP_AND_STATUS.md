# Mobile app — setup, status & session findings

_Last updated during the v2 re-point + webapp→mobile parity session._

This document captures everything established in this working session: the parsed-regs
v2 re-point, the enrichment run, the recommended build toolchains, and the exact state
of the mobile app's feature parity with the web app.

---

## 1. Parsed regulations → v2 re-point (DONE)

The enrichment pipeline now consumes the **v2 agent-parsed** regulations instead of the
older Gemini-parsed set.

- **Promoted:** `output/pipeline/parsing/session_state.json` is now the v2 data —
  **1395 / 1395** synopsis entries parsed successfully.
- **Backups (same directory):**
  - `session_state.gemini-backup-20260710T164017.json`
  - `synopsis_parsed.gemini-backup-20260710T164017.json`
- **Originals** remain in `output/pipeline/agent_parsing_full_v2/`.

### Is enrichment stuck? No.

`enrich` (`PYTHONPATH="$PWD" .venv/bin/python -m pipeline --step enrich`) is **CPU-bound,
not hung**. What looked like a frozen "87%" was a static `tqdm` line while Phase 4 ran a
two-pass spatial join:

```
base_reg_assigner: Two-pass MU-stream query: 4,147,914 streams × 225 MUs
Phase 4 complete → phase4_base: 106,078,013   (done in ~1498s / ~25 min)
```

At last check it had **finished Phase 4** and moved into the **bathymetry matcher**
(reading FWA `lakes` / `wetlands` layers). It is running at ~97% CPU and advancing
normally. `py-spy` is not installed, so progress was confirmed via advancing CPU time and
log stage transitions.

---

## 2. Recommended build toolchains

The mobile app is an **Expo 52 / React Native 0.76.5 dev-client** with native modules
(`@maplibre/maplibre-react-native`, `react-native-pdf`, `expo-sqlite`). Because of the
native modules it **cannot** run in Expo Go or Expo Web — it needs a native dev build.

| Rank | Toolchain | Command | Notes |
| ---- | --------- | ------- | ----- |
| **#1** | Android Studio + physical Android phone | `npx expo run:android` | Best local loop; USB-debug the phone you already have. |
| **#2** | Xcode + iOS Simulator | `npx expo run:ios` | Requires macOS + Xcode (not installed on this Mac). |
| Alt | EAS cloud dev build | `eas build --profile development` | No local SDK needed; builds in the cloud, install the artifact on device. |

> **Node version:** pin to **20 or 22 LTS**. This Mac has Node v25.9.0, which is outside
> Expo 52's supported range (18–22) and may cause Metro/CLI failures. Use `nvm use 20`.

### This machine's constraints
No Xcode and no Android SDK are installed here, so the app **cannot be device-tested from
this Mac**. The plan is to build on a Linux laptop with Android Studio + the physical
Android phone. **Typecheck (`npx tsc --noEmit`) is currently the only validation gate** —
all ported components compile cleanly but are **not yet device-tested**.

---

## 3. Exact run steps

```bash
cd mobile
nvm use 20                       # ensure Node 20/22 LTS
npm install                      # deps already installed (~971 pkgs)

# Bundle local assets. sync-assets expects the v2 SQLite shard at
#   output/pipeline/deploy/mobile/v2/regulations.sqlite
npm run sync-assets              # or: npm run sync-assets -- --no-db  (fetch DB from R2 at runtime)

npx expo run:android             # #1 toolchain — physical phone / emulator
# or
npx expo run:ios                 # #2 toolchain — macOS + Xcode only
```

### Shard v1 vs v2 note
- `mobile/scripts/sync-assets.mjs` is pinned to `SHARD_VERSION = 'v2'` and copies
  `output/pipeline/deploy/mobile/v2/regulations.sqlite` → `assets/db/`.
- The **v2** shard now exists locally — the enrich run completed and wrote
  `output/pipeline/deploy/mobile/v2/regulations.sqlite` (**1120.1 MB**, 291.9 MB gzip;
  4,147,914 fids / 2,245,671 reaches / 721,351 polys). `npm run sync-assets` will pick it
  up directly.
- The older **v1** shard also remains at `output/pipeline/deploy/mobile/v1/regulations.sqlite`.
- To have the app fetch from R2 at runtime instead of bundling the DB, run with `--no-db`.

### First-run download gate
When no dev-bundled DB exists (the normal case for a shipped binary — 1.2 GB can't live in
the app package), the app now **gates startup behind an explicit, explained download**
instead of silently pulling a gigabyte:
- `databaseNeedsDownload()` in `src/data/database.ts` probes disk + dev-bundled asset.
- `src/components/DataGate.tsx` renders the states: `checking` → `needs-download`
  (explanation + "Download data" button) → `downloading` (progress bar + %) → `loading`
  → app, plus an `error` state with "Try again".
- `App.tsx` probes on boot: if data is already present/bundled it initialises immediately;
  otherwise it shows the gate and only downloads after the user taps.

---

## 4. Webapp → mobile feature parity

### Ported this session (typecheck-clean, not device-tested)

| Component | File | Web reference |
| --------- | ---- | ------------- |
| Map (forwardRef + `flyTo` + multi-feature tap) | `src/map/MapView.tsx` | `webapp/src/components/Map.tsx` |
| Info panel (bathymetry + source links + report + tablet dock) | `src/components/InfoPanel.tsx` | `webapp/src/components/InfoPanel.tsx` |
| Search bar (debounced, fuse.js) | `src/components/SearchBar.tsx` | `webapp/src/components/SearchBar.tsx` |
| Disambiguation menu (multi-feature taps) | `src/components/DisambiguationMenu.tsx` | `webapp` DisambiguationMenu |
| Disclaimer (first-run legal modal) | `src/components/Disclaimer.tsx` | `webapp` Disclaimer |
| Source-image viewer (pinch/pan/double-tap) | `src/components/SourceImageViewer.tsx` | synopsis page viewer |
| PDF viewer (depth maps) | `src/components/PdfViewer.tsx` | bathymetry PDF viewer |
| Issue report (feedback form) | `src/components/IssueReport.tsx` | `webapp` IssueReport |
| Disclaimer persistence helper | `src/utils/disclaimerStorage.ts` | web `localStorage` flag |
| First-run data download gate | `src/components/DataGate.tsx` | (mobile-only; web streams from edge) |

### Orchestration — `src/App.tsx` (rewritten)
`App.tsx` is now the full orchestrator (mobile analogue of the web app's `Map.tsx`, which
holds all state — the web `App.tsx` is just a thin shell). It wires:

- **Responsive layout** via `useWindowDimensions`: `isTablet = width >= 768`. Phones get a
  bottom sheet; tablets get a right-docked 400 dp info panel and a fixed-width search bar.
- **Map ref** (`MapViewHandle.flyTo`) for search fly-to.
- **Feature taps** → `onFeatures` array: one hit resolves directly, multiple hits open the
  disambiguation menu.
- **Search** → `flyTo(bbox, min_zoom)` + resolve reach from the first segment → info panel.
- **Info-panel actions** → depth-map `PdfViewer`, synopsis `SourceImageViewer`, and the
  `IssueReport` form (context pre-filled with waterbody name/type + `SHARD_VERSION`).
- **First-run disclaimer** via `hasAcceptedDisclaimer` / `setDisclaimerAccepted`.

### Remaining parity gaps (future work)
- **Map controls:** satellite basemap toggle, opacity slider, and URL/deep-link state from
  `webapp/src/components/Map.tsx` (2128 lines) are **not** ported.
- **Info-panel visual polish:** the web `InfoPanel.tsx` (1159 lines) has grouped rule-card
  sections, severity badges, icons, and collapse states. The mobile port is functional
  (rule text / restriction details / provenance per reg + bathymetry + source + report)
  but visually simplified.
- **Lint:** `npm run lint` fails — `mobile/` has no `eslint.config.js` (ESLint v9 needs a
  flat config). Copy/adapt `webapp/eslint.config.js`.
- **Device testing:** nothing has been run on a device/simulator yet.

---

## 5. Validation performed
- `cd mobile && npx tsc --noEmit` → **exit 0** (clean) after the App.tsx rewrite and all
  component ports.
- No lint run (no flat config yet — see gap above).
- No runtime/device test (no local Android SDK / Xcode on this Mac).
