import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Throwaway internal tool: dev server proxies /api -> the FastAPI backend on :8787
// so the frontend can call `/api/...` with relative URLs. `CURATION_API` points it at another
// backend (a headless check runs one against a temp copy of the catalogue on another port).
declare const process: { env: Record<string, string | undefined> };  // no @types/node here
const API = process.env.CURATION_API ?? "http://127.0.0.1:8787";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // LOOPBACK ONLY, IPv4 (T96). `true` bound every interface, so anyone on the network could
    // reach the editor; `false` (Vite's `localhost`) resolves to ::1 on macOS and refuses the
    // `http://127.0.0.1:5173` run.sh prints. The backend's CORS admits exactly this origin.
    host: "127.0.0.1",
    strictPort: true,
    proxy: {
      "/api": {
        target: API,
        changeOrigin: true,
      },
      // the PMTiles basemap (data/bc.pmtiles) is served by the backend with Range support
      "/basemap": {
        target: API,
        changeOrigin: true,
      },
    },
  },
});
