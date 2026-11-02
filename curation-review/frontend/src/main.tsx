import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { MarksHealthBanner } from "./components/MarksHealthBanner";
import { RefusedSavesBanner } from "./components/RefusedSavesBanner";
import { CuratedSaveBanner } from "./components/CuratedSaveBanner";
import { NoticeDock } from "./components/NoticeStack";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <div className="shell">
      <div className="shell-main"><App /></div>
      <NoticeDock>
        <RefusedSavesBanner />
        <MarksHealthBanner />
        <CuratedSaveBanner />
      </NoticeDock>
    </div>
  </React.StrictMode>,
);
