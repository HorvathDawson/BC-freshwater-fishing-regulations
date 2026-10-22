/**
 * The week before a visit, as a compact strip (S1): seven columns of weather, the station's daily means beneath them
 * with their place in the envelope, and one line for each half's direction.
 *
 * Draws `leadUpView` (@app/ui) and nothing else: every string is composed in `@app/data/spots` words, and each column
 * carries its day said whole as its label, so a screen reader hears "Mon Aug 24: rain, high 17°C …" rather than a
 * grid of numbers.
 */
import { Text, View } from "react-native";
import type { LeadUpView } from "@app/ui";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function LeadUpStrip({ view, palette }: { view: LeadUpView; palette: Palette }) {
  const note = (testID: string, s: string) => (
    <Text testID={testID} style={{ ...TYPE.small, color: palette.sub }}>{s}</Text>
  );
  const caption = (testID: string, s: string) => (
    <Text testID={testID} style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>{s}</Text>
  );
  return (
    <View style={{ gap: 10 }}>
      {view.weather ? (
        <View style={{ gap: 6 }}>
          <View style={{ flexDirection: "row" }}>
            {view.weather.days.map((d) => (
              <View key={d.day} accessible accessibilityLabel={d.label}
                    style={{ flex: 1, alignItems: "center", gap: 2 }}>
                <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{d.day}</Text>
                <Text style={{ ...TYPE.small, fontSize: 11, color: palette.ink }}>{d.sky}</Text>
                <Text style={{ ...TYPE.figure, fontSize: 10.5, color: palette.ink }}>{d.temps}</Text>
                <Text style={{ ...TYPE.figure, fontSize: 10.5, color: palette.sub }}>{d.rain}</Text>
              </View>
            ))}
          </View>
          {view.weather.rain ? note("leadup-rain", view.weather.rain) : null}
          {view.weather.pending ? note("leadup-weather-pending", view.weather.pending) : null}
          {caption("leadup-weather-source", view.weather.source)}
        </View>
      ) : view.weatherNote ? note("leadup-weather-note", view.weatherNote) : null}

      {view.gauge ? (
        <View style={{ gap: 6 }}>
          <Text testID="leadup-unit" style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>
            {view.gauge.unit}
          </Text>
          <View style={{ flexDirection: "row" }}>
            {view.gauge.days.map((d) => (
              <View key={d.day} accessible accessibilityLabel={d.label}
                    style={{ flex: 1, alignItems: "center", gap: 2 }}>
                <Text style={{ ...TYPE.figure, fontSize: 11, color: palette.live }}>{d.value}</Text>
                <Text style={{ ...TYPE.micro, fontSize: 9.5, color: palette.faint }}>{d.place}</Text>
              </View>
            ))}
          </View>
          {view.gauge.trend ? note("leadup-trend", view.gauge.trend) : null}
          {view.gauge.pending ? note("leadup-gauge-pending", view.gauge.pending) : null}
          {caption("leadup-gauge-meta", view.gauge.meta)}
        </View>
      ) : view.gaugeNote ? note("leadup-gauge-note", view.gaugeNote) : null}
    </View>
  );
}
