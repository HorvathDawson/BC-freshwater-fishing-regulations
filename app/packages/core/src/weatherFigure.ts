/**
 * (T620 G13) ONE WEATHER NUMBER, in its unit, wherever a spot's or a visit's weather is said: the record's big figure,
 * the list row, the "like your visit" comparison, the public gauge page's weather (T1039). A 13.6 °C visit is "14°C" on every one of them (it was "13.6°C" in the
 * record and "14°C" beside it). Temperature, wind, cloud and pressure are whole numbers; rain keeps one decimal.
 */
export type WeatherKind = "temp" | "rain" | "wind" | "cloud" | "pressure" | "humidity";

const round1 = (v: number) => Math.round(v * 10) / 10;

export function weatherFigure(kind: WeatherKind, v: number): string {
  switch (kind) {
    case "temp": return `${Math.round(v)}°C`;
    case "rain": return `${round1(v)} mm`;
    case "wind": return `${Math.round(v)} km/h`;
    case "cloud": return `${Math.round(v)}%`;
    case "pressure": return `${Math.round(v)} hPa`;
    // (T1039) the relative humidity a spot's weather window shows, whole, as the gauge page says it
    case "humidity": return `${Math.round(v)}%`;
  }
}
