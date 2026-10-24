/**
 * The map, and the furniture that floats over it. Drawn to `design/riffle.html`.
 *
 * The map is home: you arrive here and every other tab is a way back to one. This component
 * decides nothing about how water is coloured — it names a VIEW and per-layer MODES, and
 * @app/map resolves those through the generated style, so the phone and the desktop cannot
 * colour the same river differently.
 */
import type { SectionKey } from "@app/core";
import { Pressable, Text, View } from "react-native";
import { Map, type Camera, type MapProps, type TileEndpoints } from "@app/map";
import type { Horizon, HorizonChipVM } from "@app/ui";
import { hitProps } from "./regs/hit";
import { MapRail } from "./MapRail";
import { TYPE } from "./type";
import type { Palette } from "./theme";


/** a horizon segment: at least 46 wide and 33 tall by its style (8 + 17 + 8), grown to 44 × 44 to the touch */
const HORIZON_HIT = hitProps({ w: 46, h: 33 });

export function MapScreen({ at, palette, theme, view, modes, groups, hide, show, camera,
                            onLayers, layersChanged = 0, onLongPress, onPressFeature, onMoved, onMapPoint, horizons,
                            highlight, marker, pins, data, status, standing, lakes, gauges, onVisible, onError,
                            fit, basemap, onImageryError, water, waterLine, selection, marks, onPressMark, spots,
                            insetTop = 0 }: {
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
  /** (T151) How `highlight` is marked — see MapProps.selection. */
  selection?: MapProps["selection"];
  /** (T175) The water's own line at every zoom — see MapProps.waterLine. */
  waterLine?: MapProps["waterLine"];
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
  /** (T48) Marked points with their own colour — a searched town's pin, in the town's colour. See MapProps.pins. */
  pins?: MapProps["pins"];
  /** (T131) The reader's saved spots, each a mark that opens its spot — see MapProps.spots (the Shell's `spotLayer`). */
  spots?: MapProps["spots"];
  /**
   * (T48) The bottom edge of the app's bar over the map (the search field), in px: the corner's chips, the rail and the
   * web map's own controls start under it. 0: no bar.
   */
  insetTop?: number;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.tint }}>
      <Map at={at} theme={theme} view={view} modes={modes} groups={groups} hide={hide} show={show}
           initial={camera}
           onPressFeature={onPressFeature} onMoved={onMoved} onMapPoint={onMapPoint} onLongPress={onLongPress}
           highlight={highlight} pins={pins} spots={spots} insetTop={insetTop} water={water} waterLine={waterLine} selection={selection} marks={marks} onPressMark={onPressMark} marker={marker} data={data} status={status} standing={standing} lakes={lakes}
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
        THE TOP-LEFT CORNER, under the app's bar (T48: the search field across the top). The hamburger that sat here moved
        to the bottom bar's More tab, and search's way-back pill went with the eye: search opens over this same map now.
        NO satellite button here either: the choice is the Layers sheet's Basemap — two controls for one setting is one
        too many.
      */}
      <View testID="map-corner" style={{ position: "absolute", top: insetTop + 8, left: 14, flexDirection: "column",
                                          gap: 8, alignItems: "flex-start" }} pointerEvents="box-none">
      {horizons && (
        /*
          (refinement pass) ONE CONTROL, DRAWN AS ONE: a joined segmented strip — a single card with hairline dividers — in
          place of five loose floating pills, five shadows and five borders over the map for what is one choice of day.
          The chosen day is FILLED, not merely tinted: this control repaints the whole map, so which one is on has to be
          readable at a glance and from the corner of an eye. Each segment is a 44 × 44 press (`hitProps`).
        */
        <View testID="map-horizons"
              style={{ flexDirection: "row", backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2,
                       borderRadius: palette.r.pill, overflow: "hidden", ...palette.lift }}>
          {horizons.chips.map((h, i) => (
            <Pressable key={h.day} onPress={() => horizons.onPick(h.day)} accessibilityRole="button"
                       accessibilityLabel={h.label} {...HORIZON_HIT}>
              <View style={{ minWidth: 46, paddingVertical: h.note ? 4 : 8, paddingHorizontal: 10,
                             alignItems: "center", justifyContent: "center",
                             backgroundColor: h.on ? palette.accent : palette.card,
                             borderLeftWidth: i ? 1 : 0, borderLeftColor: palette.line2 }}>
                <Text style={{ ...TYPE.micro, fontSize: 13, lineHeight: h.note ? 14 : 17, fontWeight: "600",
                               color: h.on ? palette.onAccent : h.note ? palette.sub : palette.ink }}>
                  {h.chip}
                </Text>
                {/* (T130) a day this quantity does not answer: greyed, with core's note, in about the same height */}
                {h.note && (
                  <Text style={{ ...TYPE.micro, fontSize: 10, lineHeight: 11, letterSpacing: 0, color: palette.sub }}>
                    {h.note}
                  </Text>
                )}
              </View>
            </Pressable>
          ))}
        </View>
      )}
      </View>

      {/*
        THE RAIL (owner 2026-10-21): Layers moved here from a bottom-left pill — the map's own buttons in one stack on the
        right, under MapLibre's zoom stack (`MapRail`). Layers alone since T48: Spots is a bottom-bar tab again.
      */}
      <MapRail palette={palette} onLayers={onLayers} layersChanged={layersChanged} top={insetTop} />
    </View>
  );
}
