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
    // Bind BOTH stacks. Vite's default `localhost` resolves to ::1 on macOS, so the
    // `http://127.0.0.1:5173` that run.sh prints was refusing the connection.
    host: true,
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
