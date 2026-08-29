import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Throwaway internal tool: dev server proxies /api -> the FastAPI backend on :8787
// so the frontend can call `/api/...` with relative URLs.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // Bind BOTH stacks. Vite's default `localhost` resolves to ::1 on macOS, so the
    // `http://127.0.0.1:5173` that run.sh prints was refusing the connection.
    host: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
      },
      // the PMTiles basemap (data/bc.pmtiles) is served by the backend with Range support
      "/basemap": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
      },
    },
  },
});
