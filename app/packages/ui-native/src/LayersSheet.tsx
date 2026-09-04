/**
 * The Layers panel, rebuilt to `design/riffle.html`.
 *
 * WHAT I HAD WRONG, because it matters structurally and not just visually:
 *
 *  - Streams and lakes are coloured INDEPENDENTLY. Rules on the rivers while the lakes
 *    show Stocked is a normal thing to want — they answer different questions. I had one
 *    global "view" forcing both, which is a preset, not a control.
 *  - The choices are BUTTONS CARRYING THEIR OWN COLOURS, not switches. "Stocked" means
 *    nothing as a word; three swatches in the stocking ramp say what the map will look
 *    like before you commit to it.
 *  - Each section states its own scale — "6,967 REACHES", "35 SURVEYED" — so the reader
 *    knows how much map a choice affects.
 *
 * The palette picker stays here (Riffle has no colour-blind mode; this app does, and it
 * belongs with the other rendering choices rather than behind a debug-looking control).
 */
import { ScrollView, Text, View } from "react-native";
import { Choice, Sheet } from "./Sheet";
import { OptionRow, type Option } from "./OptionRow";
import { TYPE } from "./type";
import type { Palette, ThemeName } from "./theme";
import { ago, count } from "./format";

const THEME_OPTIONS = [
  { k: "light" as ThemeName, t: "Light" },
  { k: "dark" as ThemeName, t: "Dark" },
  { k: "cvd" as ThemeName, t: "Colour-blind" },
];

/**
 * What a Layers choice DOES. Not every option is a colour.
 *
 * "Depth" is the one that made this necessary: it reads like a colouring but it is a
 * layer — the contours are their own tile layer, and the lake underneath goes plain so
 * they are visible. Passing "depth" through as a colour mode threw
 * `layer "lake" has no colour mode "depth"` from inside a render effect, which takes the
 * whole tree down. A choice that is not a mode must not be typed as one.
 */
export interface LayerChoice {
  k: string;
  t: string;
  /** The colour mode to put on the layer. Every one of these must exist in the style. */
  mode: string;
  /** A layer group to switch on while this choice is active. */
  group?: string;
  swatch: readonly [string, string, string];
}

/** How recently a lake was stocked, coarsest question first. Mirrors Riffle's SBANDS. */
export const STOCK_BANDS = [
  { days: 120, label: "this season" },
  { days: 365, label: "this year" },
  { days: 1095, label: "1–3 years" },
  { days: 3650, label: "3–10 years" },
  { days: Infinity, label: "over 10 years" },
] as const;

export interface LayersState {
  stream: string;
  lake: string;
  basemap: "map" | "satellite";
}

export const streamChoices = (p: Palette): LayerChoice[] => [
  { k: "rules", t: "Rules", mode: "closure",
    swatch: [p.closed, p.restricted, p.open] },
  { k: "plain", t: "Plain", mode: "plain", swatch: [p.live, p.live, p.live] },
];

export const lakeChoices = (p: Palette): LayerChoice[] => [
  { k: "rules", t: "Rules", mode: "closure", swatch: [p.closed, p.restricted, p.open] },
  // Depth is BOTH: the contour layer turns on, and the lakes themselves colour by whether
  // they were surveyed at all. Colouring by digitised contours alone showed a near-empty
  // map — 2,741 bathymetric sheets exist and only a fraction were ever traced — which tells
  // a reader the province was never surveyed. A scanned sheet is still a surveyed lake.
  { k: "depth", t: "Depth", mode: "surveyed", group: "depth",
    swatch: [p.quiet, "#7BA4B8", p.live] },
  { k: "stocked", t: "Stocked", mode: "stocked",
    swatch: [p.stock[0], p.stock[2], p.stock[4]] },
  { k: "plain", t: "Plain", mode: "plain", swatch: [p.quiet, p.quiet, p.quiet] },
];

export function LayersSheet({ open, onClose, palette, state, onState, theme, onTheme,
                             reaches, surveyed, stations, fetchedAt, attribution }: {
  open: boolean; onClose: () => void; palette: Palette;
  state: LayersState; onState: (s: LayersState) => void;
  theme: ThemeName; onTheme: (t: ThemeName) => void;
  reaches?: number; surveyed?: number; stations?: number;
  fetchedAt?: string | null;
  attribution?: readonly string[];
}) {
  const streamOpts = streamChoices(palette);
  const lakeOpts = lakeChoices(palette);

  return (
    <Sheet open={open} onClose={onClose} title="Layers" palette={palette}>
      <ScrollView style={{ maxHeight: 520 }}>
        <Section palette={palette} title="Streams" note={count(reaches, "reaches")}>
          <OptionRow palette={palette} shape="line" options={streamOpts} value={state.stream}
                     onChange={(k) => onState({ ...state, stream: k })} label="Colour streams by" />
          <Fine palette={palette}>
            Flow standing lives in the Conditions tab — it is a different question, not a
            paint job.
          </Fine>
        </Section>

        <Section palette={palette} title="Lakes" note={count(surveyed, "surveyed")}>
          <OptionRow palette={palette} shape="square" options={lakeOpts} value={state.lake}
                     onChange={(k) => onState({ ...state, lake: k })} label="Colour lakes by" />
          {state.lake === "stocked" && (
            <View style={{ marginTop: 10 }}>
              {STOCK_BANDS.map((b, i) => (
                <View key={b.label}
                      style={{ flexDirection: "row", alignItems: "center", gap: 12,
                               paddingVertical: 11, borderTopWidth: 1,
                               borderTopColor: palette.line }}>
                  <View style={{ width: 9, height: 9, borderRadius: 2,
                                 backgroundColor: palette.stock[i] }} />
                  <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
                    Stocked {b.label}
                  </Text>
                </View>
              ))}
            </View>
          )}
        </Section>

        <Section palette={palette} title="Basemap">
          <OptionRow palette={palette} shape="square" value={state.basemap}
                     onChange={(k) => onState({ ...state, basemap: k as "map" | "satellite" })}
                     label="Basemap"
                     options={[
                       { k: "map", t: "Map",
                         swatch: [palette.tint, "#AED3E2", "#9CC2D6"] },
                       { k: "satellite", t: "Satellite",
                         swatch: ["#2A333B", "#3C5A2E", "#6E7F53"] },
                     ]} />
        </Section>

        <Section palette={palette} title="Live data" note="every value has an age">
          <SourceRow palette={palette} title="Stream gauges"
               sub={`${stations ?? 0} stations · Environment Canada`}
               age={fetchedAt ? ago(fetchedAt) : "—"} />
          <SourceRow palette={palette} title="Stocking releases"
               sub="Province of BC · Fisheries Inventory" age="per season" />
        </Section>

        <Section palette={palette} title="Palette">
          <Choice palette={palette} label="Palette" value={theme} onChange={onTheme}
                  options={THEME_OPTIONS} />
          <Fine palette={palette}>
            Every outcome keeps its word as well as its colour, so the colour-blind palette
            changes the hues and nothing else.
          </Fine>
        </Section>

        {attribution && attribution.length > 0 && (
          <View style={{ paddingHorizontal: 20, paddingBottom: 22, gap: 4 }}>
            {attribution.map((a) => (
              <Text key={a} style={{ ...TYPE.small, fontSize: 10.5, color: palette.faint }}>
                {a}
              </Text>
            ))}
          </View>
        )}
      </ScrollView>
    </Sheet>
  );
}

function Section({ palette, title, note, children }: {
  palette: Palette; title: string; note?: string; children: React.ReactNode;
}) {
  return (
    <View style={{ paddingHorizontal: 20, paddingTop: 16, paddingBottom: 2 }}>
      <View style={{ flexDirection: "row", alignItems: "baseline", marginBottom: 11 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint }}>{title.toUpperCase()}</Text>
        {note && (
          <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                         color: palette.faint, marginLeft: "auto" }}>
            {note.toUpperCase()}
          </Text>
        )}
      </View>
      {children}
    </View>
  );
}

/** A named data source, what it is, and how old it is. */
function SourceRow({ palette, title, sub, age }: {
  palette: Palette; title: string; sub: string; age: string;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 12,
                   borderTopWidth: 1, borderTopColor: palette.line }}>
      <View style={{ flex: 1 }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{title}</Text>
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{sub}</Text>
      </View>
      <Text style={{ ...TYPE.micro, fontSize: 11, color: palette.faint }}>{age}</Text>
    </View>
  );
}

function Fine({ palette, children }: { palette: Palette; children: React.ReactNode }) {
  return (
    <Text style={{ ...TYPE.small, fontSize: 11, lineHeight: 16.5, color: palette.faint,
                   paddingTop: 10 }}>
      {children}
    </Text>
  );
}
