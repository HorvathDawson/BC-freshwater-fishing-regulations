/**
 * The map, and the furniture that floats over it. Drawn to `design/riffle.html`.
 *
 * The map is home: you arrive here and every other tab is a way back to one. This component
 * decides nothing about how water is coloured — it names a VIEW and per-layer MODES, and
 * @app/map resolves those through the generated style, so the phone and the desktop cannot
 * colour the same river differently.
 */
import type { SectionKey } from "@app/core";
import { Text, View } from "react-native";
import { Map, type Camera, type TileEndpoints } from "@app/map";
import { Pill } from "./Chrome";
import { LayersIcon } from "./icons";
import { TYPE } from "./type";
import { mapChrome, type Palette } from "./theme";


export function MapScreen({ at, palette, theme, view, modes, groups, hide, camera,
                            onLayers, onPressFeature, onMoved, onMapPoint, horizons,
                            highlight, marker, data, gauges, onVisible, onError }: {
  at: TileEndpoints; palette: Palette; theme: string; view: string;
  groups?: Record<string, boolean>;
  /** Layers this view does not draw — see `hiddenLayers`. Not a user toggle. */
  hide?: readonly string[];
  modes?: Record<string, string>;
  onPressFeature?: (layerId: string, featureId: SectionKey,
                    lat?: number, lon?: number) => void;
  onMoved?: (at: Camera) => void;
  /** A raw tapped coordinate — for picking a PLACE rather than a feature. */
  onMapPoint?: (lat: number, lon: number) => void;
  /** Feature ids to draw as selected. */
  highlight?: readonly SectionKey[];
  /** Per-feature values a colour mode reads. The map's own channel — see MapProps.data. */
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /** Gauge points as GeoJSON. Only the Conditions view draws them. */
  gauges?: string;
  onVisible?: (sections: readonly SectionKey[]) => void;
  /** A point to mark — where the user tapped. */
  marker?: { lat: number; lon: number } | null;
  camera: Camera;
  onLayers?: () => void;
  /** Forecast horizons. Present only on the Conditions tab. */
  horizons?: { days: readonly number[]; value: number; onPick: (d: number) => void };
  onError?: (e: Error) => void;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.tint }}>
      <Map at={at} theme={theme} view={view} modes={modes} groups={groups} hide={hide}
           initial={camera}
           onPressFeature={onPressFeature} onMoved={onMoved} onMapPoint={onMapPoint}
           highlight={highlight} marker={marker} data={data} gauges={gauges}
           onVisible={onVisible} onError={onError}
           chrome={mapChrome(palette, theme)} />

      {/*
        ZOOM, COMPASS AND SCALE ARE THE MAP'S OWN (see @app/map/controls.css). What is left
        here is the chrome that is genuinely ours: the forecast horizons and the layers —
        neither of which MapLibre has an opinion about.

        The horizons have the top-left corner to themselves, on the Conditions tab only:
        conditions are NOW, and what a person wants there is where the water is heading.
        (The corner used to hold a date pill, which existed for seasonal regulations; it went
        with them.)
      */}
      {horizons && (
        <View style={{ position: "absolute", top: 14, left: 14, flexDirection: "row", gap: 6 }}>
          {horizons.days.map((d) => {
            const on_ = d === horizons.value;
            return (
              <Pill key={d} palette={palette} onPress={() => horizons.onPick(d)}
                    // FILLED WHEN CHOSEN, not merely tinted. These sit over a moving map
                    // at 13px, and a text-colour change alone is not a state a reader
                    // notices — the whole map is repainted by this control, so which one
                    // is on has to be readable at a glance and from the corner of an eye.
                    style={on_ ? { backgroundColor: palette.accent,
                                   borderColor: palette.accent } : undefined}
                    label={(d === 0 ? "Conditions now"
                                    : `Forecast ${d} day${d === 1 ? "" : "s"} ahead`)
                           + (on_ ? ", showing" : "")}>
                <Text style={{ ...TYPE.micro, fontSize: 13, fontWeight: "600",
                               color: on_ ? palette.onAccent : palette.ink }}>
                  {d === 0 ? "Now" : `+${d}d`}
                </Text>
              </Pill>
            );
          })}
        </View>
      )}

      {/* NO satellite button here. MapLibre's own control stack lives in this corner and
          ours overlapped it — and the choice already exists in the Layers sheet under
          Basemap, where it sits beside the other things that change what the map is
          showing. Two controls for one setting is one too many. */}

      {onLayers && (
      <View style={{ position: "absolute", bottom: 14, left: 14 }}>
        <Pill palette={palette} onPress={onLayers} label="Choose layers">
          <LayersIcon colour={palette.ink} size={15} />
          <Text style={{ ...TYPE.micro, fontSize: 13.5, fontWeight: "600",
                         color: palette.ink }}>Layers</Text>
        </Pill>
      </View>
      )}
    </View>
  );
}
