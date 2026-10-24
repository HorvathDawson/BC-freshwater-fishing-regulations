/**
 * (T131) YOUR SPOTS on the web map: the marks `spotMarkers` gives at this zoom, each the app's face (`spots.face`) in a
 * 44 px box over the map — a layer of its own above the canvas, so a tap on one never reaches the map (the water under
 * it does not open). Regrouped as the camera moves, re-rendered only when the marks change; each frame only moves the
 * boxes. Returns the layer, drawn as the map's sibling.
 */
import { SPOT_PIN, spotMarkers, type SpotMarker } from "@app/core";
import { useEffect, useRef, useState } from "react";
import type { MapProps } from "../map-props";
import { pressSpot, spotsKey } from "../spots";
import type { WebMapRefs } from "./refs";

export function useSpots({ map, renderer }: WebMapRefs, spots: MapProps["spots"], at: MapProps["at"]) {
  const spotsNow = useRef(spots);
  spotsNow.current = spots;
  const [spotMarks, setSpotMarks] = useState<readonly SpotMarker[]>([]);
  const spotSig = useRef("");
  const spotBoxes = useRef(new globalThis.Map<string, HTMLDivElement>());
  const placeSpot = (el: HTMLDivElement, mk: SpotMarker) => {
    const p = map.current?.project([mk.lon, mk.lat]);
    if (p) el.style.transform = `translate(${p.x - SPOT_PIN.hit / 2}px, ${p.y - SPOT_PIN.hit / 2}px)`;
  };
  const spotKey = spotsKey(spots?.points, spots?.selected ?? null);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const regroup = () => {
      const s = spotsNow.current;
      const next = s ? spotMarkers(s.points, m.getZoom(), s.selected) : [];
      const sig = JSON.stringify(next);
      if (sig !== spotSig.current) { spotSig.current = sig; setSpotMarks(next); }
      for (const mk of next) { const el = spotBoxes.current.get(mk.key); if (el) placeSpot(el, mk); }
    };
    regroup();
    m.on("move", regroup);
    return () => { m.off("move", regroup); };
  }, [spotKey, at.atlas, at.basemap, at.glyphs, at.sprite, at.assets]);
  return spots && spotMarks.length ? (
    <div data-spots="" style={{ position: "absolute", inset: 0, overflow: "hidden", pointerEvents: "none" }}>
      {spotMarks.map((mk) => (
        <div key={mk.key} data-spot={mk.key}
             ref={(el) => { if (el) { spotBoxes.current.set(mk.key, el); placeSpot(el, mk); } else spotBoxes.current.delete(mk.key); }}
             style={{ position: "absolute", left: 0, top: 0, width: SPOT_PIN.hit, height: SPOT_PIN.hit,
                      pointerEvents: "auto", zIndex: mk.selected ? 1 : 0 }}>
          {spots.face(mk, () => pressSpot(mk, spotsNow.current ?? spots,
                                          (c, o) => renderer.current?.easeTo(c, o)))}
        </div>))}
    </div>) : null;
}
