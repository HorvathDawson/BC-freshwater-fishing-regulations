import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { MarksHealthBanner } from "./components/MarksHealthBanner";
import { RefusedSavesBanner } from "./components/RefusedSavesBanner";
import { CuratedSaveBanner } from "./components/CuratedSaveBanner";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
    <MarksHealthBanner />
    <RefusedSavesBanner />
    <CuratedSaveBanner />
  </React.StrictMode>,
);
