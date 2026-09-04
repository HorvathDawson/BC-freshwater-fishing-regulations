/**
 * The map, and the furniture that floats over it. Drawn to `design/riffle.html`.
 *
 * The map is home: you arrive here and every other tab is a way back to one. This component
 * decides nothing about how water is coloured — it names a VIEW ("regulations",
 * "conditions") and @app/map resolves that through the generated style, so the phone and
 * the desktop cannot colour the same river differently.
 */
import { Text, View } from "react-native";
import { Map, type Camera, type TileEndpoints } from "@app/map";
import type { PlainDate } from "@app/core";
import { Pill } from "./Chrome";
import { LayersIcon } from "./icons";
import { TYPE } from "./type";
import { mapChrome, type Palette } from "./theme";

const MONTH = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
               "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

export function MapScreen({ at, palette, theme, view, modes, groups, on, camera,
                            onDate, onLayers, onPressFeature, onMoved, onMapPoint, horizons,
                            highlight, marker, data, gauges, onVisible, onError }: {
  at: TileEndpoints; palette: Palette; theme: string; view: string;
  groups?: Record<string, boolean>;
  modes?: Record<string, string>;
  onPressFeature?: (layerId: string, featureId: string,
                    lat?: number, lon?: number) => void;
  onMoved?: (at: Camera) => void;
  /** A raw tapped coordinate — for picking a PLACE rather than a feature. */
  onMapPoint?: (lat: number, lon: number) => void;
  /** Feature ids to draw as selected. */
  highlight?: readonly string[];
  /** Per-feature values a colour mode reads. The map's own channel — see MapProps.data. */
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /** Gauge points as GeoJSON. Only the Conditions view draws them. */
  gauges?: string;
  onVisible?: (sections: readonly string[]) => void;
  /** A point to mark — where the user tapped. */
  marker?: { lat: number; lon: number } | null;
  on: PlainDate; camera: Camera;
  onDate?: () => void; onLayers?: () => void;
  /**
   * Forecast horizons, INSTEAD of the date control. Present only on the Conditions tab —
   * see the comment at the control itself for why the two are exclusive.
   */
  horizons?: { days: readonly number[]; value: number; onPick: (d: number) => void };
  onError?: (e: Error) => void;
}) {
  return (
    <View style={{ flex: 1, backgroundColor: palette.tint }}>
      <Map at={at} theme={theme} view={view} modes={modes} groups={groups} initial={camera}
           onPressFeature={onPressFeature} onMoved={onMoved} onMapPoint={onMapPoint}
           highlight={highlight} marker={marker} data={data} gauges={gauges}
           onVisible={onVisible} onError={onError}
           chrome={mapChrome(palette, theme)} />

      {/*
        ZOOM, COMPASS AND SCALE ARE THE MAP'S OWN (see @app/map/controls.css). What is left
        here is the chrome that is genuinely ours: the date, the layers, the basemap toggle
        — none of which MapLibre has an opinion about.

        The date pill has the top-left corner to ITSELF. It used to sit under the scale
        bar, which put the two things that qualify every answer on this screen — WHEN and
        HOW FAR — in one stack, with the scale clipped against the top edge. The scale has
        moved to the bottom corner where every map puts one; this is the app's own question
        and it reads better alone.
      */}
      <View style={{ position: "absolute", top: 14, left: 14 }}>
        {/*
          THE DATE, OR THE HORIZON — never both, because they are not both questions this
          screen can answer.

          A regulation applies ON a date, and the map asks which. Conditions are NOW: there
          is no reading for last Tuesday and no rule to look up, so a date picker on that
          tab offers a question with no answer behind it. What a person actually wants
          there is the other direction — where the water is heading — so the same corner
          carries the forecast horizons instead.
        */}
        {horizons ? (
          <View style={{ flexDirection: "row", gap: 6 }}>
            {horizons.days.map((d) => {
              const on_ = d === horizons.value;
              return (
                <Pill key={d} palette={palette} onPress={() => horizons.onPick(d)}
                      label={d === 0 ? "Conditions now"
                                     : `Forecast ${d} day${d === 1 ? "" : "s"} ahead`}>
                  <Text style={{ ...TYPE.micro, fontSize: 13, fontWeight: "600",
                                 color: on_ ? palette.accent : palette.ink }}>
                    {d === 0 ? "Now" : `+${d}d`}
                  </Text>
                </Pill>
              );
            })}
          </View>
        ) : (
          <Pill palette={palette} onPress={onDate} label="Change the date">
            <Text style={{ ...TYPE.micro, fontSize: 13.5, fontWeight: "600",
                           color: palette.ink }}>
              {on.day} {MONTH[on.month - 1]}
            </Text>
            <Text style={{ ...TYPE.micro, fontSize: 9, color: palette.faint }}>▼</Text>
          </Pill>
        )}
      </View>

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
