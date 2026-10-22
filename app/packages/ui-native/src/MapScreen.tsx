/**
 * The map, and the furniture that floats over it. Drawn to `design/riffle.html`.
 *
 * The map is home: you arrive here and every other tab is a way back to one. This component
 * decides nothing about how water is coloured — it names a VIEW and per-layer MODES, and
 * @app/map resolves those through the generated style, so the phone and the desktop cannot
 * colour the same river differently.
 */
import { SEARCH_MAP, type SectionKey } from "@app/core";
import { Text, View } from "react-native";
import { Map, type Camera, type MapProps, type TileEndpoints } from "@app/map";
import type { Horizon, HorizonChipVM } from "@app/ui";
import { Pill } from "./Chrome";
import { MapRail } from "./MapRail";
import { MenuButton } from "./menu/MenuButton";
import { TYPE } from "./type";
import type { Palette } from "./theme";


export function MapScreen({ at, palette, theme, view, modes, groups, hide, show, camera,
                            onLayers, layersChanged = 0, onSpots, onLongPress, onPressFeature, onMoved, onMapPoint, horizons,
                            highlight, marker, data, status, standing, lakes, gauges, onVisible, onError,
                            fit, basemap, onImageryError, onBack, water, marks, onPressMark }: {
  at: TileEndpoints; palette: Palette; theme: string; view: string;
  groups?: Record<string, boolean>;
  /** Layers this view does not draw — see `hiddenLayers`. Not a user toggle. */
  hide?: readonly string[];
  /** Layers only this view draws — see `shownLayers`. Absent: none of them. */
  show?: readonly string[];
  modes?: Record<string, string>;
  onPressFeature?: (layerId: string, featureId: SectionKey,
                    lat?: number, lon?: number) => void;
  onMoved?: (at: Camera) => void;
  /** A raw tapped coordinate — for picking a PLACE rather than a feature. */
  onMapPoint?: (lat: number, lon: number) => void;
  /** Feature ids to draw as selected. */
  highlight?: readonly SectionKey[];
  /** The rest of the selected part's water (P15) — see MapProps.water. */
  water?: MapProps["water"];
  /** A water's boundary marks (P15) — see MapProps.marks. */
  marks?: MapProps["marks"];
  onPressMark?: MapProps["onPressMark"];
  /** Per-feature values a colour mode reads. The map's own channel — see MapProps.data. */
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /** The day's status, on a proven pair only — see MapProps.status. */
  status?: MapProps["status"];
  /** The Conditions standings, labelled per tile, on a proven pair only — see MapProps.standing. */
  standing?: MapProps["standing"];
  /** The lake modes' values (Stocked, Depth), labelled per tile, on a proven pair only — see MapProps.lakes. */
  lakes?: MapProps["lakes"];
  /** Gauge points as GeoJSON. Only the Conditions view draws them. */
  gauges?: string;
  onVisible?: (sections: readonly SectionKey[]) => void;
  /** A point to mark — where the user tapped. */
  marker?: { lat: number; lon: number } | null;
  camera: Camera;
  onLayers?: () => void;
  /** How many layer choices differ from the defaults (`layersVM().changed`): the Layers button's badge. */
  layersChanged?: number;
  /** The rail's Spots button (owner 2026-10-21): opens the Spots menu. Absent -> no button. */
  onSpots?: () => void;
  /** A long press on the map, where (owner 2026-10-21: drops a provisional spot) — see MapProps.onLongPress. */
  onLongPress?: MapProps["onLongPress"];
  /**
   * The forecast horizons, on the Conditions tab only: `horizonChips` from @app/ui (core's
   * days and words) and the Shell's one `pick`, the same pair the panel header draws.
   */
  horizons?: { chips: readonly HorizonChipVM[]; onPick: (d: Horizon) => void };
  onError?: (e: Error) => void;
  /** Send the camera to a chosen water — see MapProps.fit. */
  fit?: MapProps["fit"];
  /** Drawn ground or satellite — the Layers sheet's Basemap choice. */
  basemap?: MapProps["basemap"];
  /** An imagery tile failed (satellite is online-only) — see MapProps.onImageryError. */
  onImageryError?: MapProps["onImageryError"];
  /** Present while the map shows a water search's eye sent it to: a pill back to search. */
  onBack?: () => void;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.tint }}>
      <Map at={at} theme={theme} view={view} modes={modes} groups={groups} hide={hide} show={show}
           initial={camera}
           onPressFeature={onPressFeature} onMoved={onMoved} onMapPoint={onMapPoint} onLongPress={onLongPress}
           highlight={highlight} water={water} marks={marks} onPressMark={onPressMark} marker={marker} data={data} status={status} standing={standing} lakes={lakes}
           gauges={gauges}
           onVisible={onVisible} onError={onError} fit={fit} basemap={basemap} onImageryError={onImageryError} />

      {/*
        ZOOM, COMPASS AND SCALE ARE THE MAP'S OWN (see @app/map/controls.css). What is left
        here is the chrome that is genuinely ours: the forecast horizons and the layers —
        neither of which MapLibre has an opinion about.

        The horizons have the top-left corner to themselves, on the Conditions tab only:
        conditions are NOW, and what a person wants there is where the water is heading.
        P16 removed them as a second copy of the panel header's control; the owner put them
        back (2026-10-20) with +10. They are no longer a copy: the chips are `horizonChips`
        (core's words, the panel header's too) and `onPick` is the Shell's one horizon state,
        so the map and the panel cannot disagree. (The corner held a date pill before, for
        seasonal regulations; it went with them.)
      */}
      {/*
        THE TOP-LEFT CORNER, a column: the menu row (the hamburger, owner 2026-10-21, on every browse map, and the way
        back to search beside it), then the horizons on a row of their own. Not one row: the hamburger in front of the
        chips pushed +10d under MapLibre's zoom (top-right) at 390 wide and off a 360 screen (review 2026-10-21). Under
        it, the chips start at the corner's left edge, where they fitted before the menu came.
      */}
      <View style={{ position: "absolute", top: 14, left: 14, flexDirection: "column", gap: 8,
                     alignItems: "flex-start" }} pointerEvents="box-none">
      <View testID="map-corner-row" style={{ flexDirection: "row", gap: 6, alignItems: "center" }}
            pointerEvents="box-none">
      <MenuButton palette={palette} />

      {/* NO satellite button here. MapLibre's own control stack lives in this corner and
          ours overlapped it — and the choice already exists in the Layers sheet under
          Basemap, where it sits beside the other things that change what the map is
          showing. Two controls for one setting is one too many. */}

      {/*
        THE WAY BACK TO SEARCH, while the map shows the water search's eye sent it to (route `show`). Top-left, the
        corner MapLibre leaves empty; the system back and the browser's back do the same (one back rule).
      */}
      {onBack && (
        <Pill palette={palette} onPress={onBack} label={SEARCH_MAP.backLabel}>
          <Text style={{ ...TYPE.micro, fontSize: 13.5, fontWeight: "600", color: palette.accent }}>
            {`‹ ${SEARCH_MAP.back}`}
          </Text>
        </Pill>
      )}
      </View>
      {horizons && (
        <View testID="map-horizons" style={{ flexDirection: "row", gap: 6 }}>
          {horizons.chips.map((h) => (
            <Pill key={h.day} palette={palette} onPress={() => horizons.onPick(h.day)}
                  // FILLED WHEN CHOSEN, not merely tinted. These sit over a moving map at
                  // 13px, and a text-colour change alone is not a state a reader notices —
                  // the whole map is repainted by this control, so which one is on has to
                  // be readable at a glance and from the corner of an eye.
                  style={h.on ? { backgroundColor: palette.accent,
                                  borderColor: palette.accent } : undefined}
                  label={h.label}>
              <Text style={{ ...TYPE.micro, fontSize: 13, fontWeight: "600",
                             color: h.on ? palette.onAccent : palette.ink }}>
                {h.chip}
              </Text>
            </Pill>
          ))}
        </View>
      )}
      </View>

      {/*
        THE RAIL (owner 2026-10-21): Layers moved here from a bottom-left pill, with Spots under it — the map's own
        buttons in one stack on the right, under MapLibre's zoom stack (`MapRail`).
      */}
      <MapRail palette={palette} onLayers={onLayers} layersChanged={layersChanged} onSpots={onSpots} />
    </View>
  );
}
