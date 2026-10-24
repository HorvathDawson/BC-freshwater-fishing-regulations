/** A saved spot's weather window: the three hours around the visit, and what changed across them. */
import { Text, View } from "react-native";
import { weatherTrend, type SpotWeather, type WeatherSample } from "@app/data/spots";
import { SPOT_WINDOW } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";

/**
 * The three hours around the visit.
 *
 * A TABLE, not a sentence, because four measures across three hours is a shape the eye
 * reads and prose does not. The trend line under it names the two changes that actually
 * decide an afternoon — the sky and the barometer — and only when both ends are known.
 *
 * Renders nothing when there is no window. An empty grid of dashes would imply we looked.
 */
export function Window({ weather, palette }: { weather: SpotWeather; palette: Palette }) {
  const win = weather.window;
  if (!win || (!win.before && !win.at && !win.after)) return null;

  const cols: [string, WeatherSample | null][] =
    [[SPOT_WINDOW.before, win.before], [SPOT_WINDOW.at, win.at], [SPOT_WINDOW.after, win.after]];
  const rows: [string, (s: WeatherSample) => number | null, string][] = [
    ["Cloud", (s) => s.cloudPct, "%"],
    ["Pressure", (s) => s.pressureHpa, ""],
    ["Humidity", (s) => s.humidityPct, "%"],
    ["Temp", (s) => s.tempC, "°"],
  ];

  const fig = (v: number | null, unit: string) =>
    v == null ? "—" : `${Math.round(v)}${unit}`;

  return (
    <View style={{ gap: 8, paddingTop: 4 }}>
      <View style={{ flexDirection: "row" }}>
        <View style={{ width: 76 }} />
        {cols.map(([label]) => (
          <Text key={label}
                style={{ ...TYPE.micro, flex: 1, fontSize: 11, textAlign: "right",
                         color: label === "visit" ? palette.ink : palette.faint }}>
            {label}
          </Text>
        ))}
      </View>
      {rows.map(([label, get, unit]) => (
        <View key={label} style={{ flexDirection: "row", alignItems: "baseline" }}>
          <Text style={{ ...TYPE.small, width: 76, fontSize: 12, color: palette.sub }}>
            {label}
          </Text>
          {cols.map(([col, s]) => (
            <Text key={col} accessibilityLabel={`${label} ${col}`}
                  style={{ ...TYPE.figure, flex: 1, fontSize: 13, textAlign: "right",
                           color: col === "visit" ? palette.ink : palette.sub }}>
              {s ? fig(get(s), unit) : "—"}
            </Text>
          ))}
        </View>
      ))}
      <Trend win={win} palette={palette} />
    </View>
  );
}

/** What changed across the window, in words — `weatherTrend` in @app/data/spots says it, or says nothing. */
function Trend({ win, palette }: {
  win: NonNullable<SpotWeather["window"]>; palette: Palette;
}) {
  const said = weatherTrend(win);
  if (said === null) return null;
  return (
    <Text testID="spot-trend" style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
      {said}
    </Text>
  );
}
