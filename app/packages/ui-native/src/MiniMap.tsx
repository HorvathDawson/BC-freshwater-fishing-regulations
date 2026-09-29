/**
 * The small map under a search result, and under "the water above you".
 *
 * Deliberately the SAME map component and the same tiles as the full screen — a minimap
 * built from a static image or a second renderer is a second map, and two maps drift. What
 * it drops is chrome and interaction, not fidelity: no date pill, no layer switcher, and
 * the "plain" view unless the caller asks otherwise, so a thumbnail never argues with the
 * screen it sits under.
 */
import type { SectionKey } from "@app/core";
import { Text, View } from "react-native";
import { Map, type Camera, type MapProps, type TileEndpoints } from "@app/map";
import { TYPE } from "./type";
import { mapChrome, type Palette } from "./theme";

export function MiniMap({ at, palette, theme, camera, height = 190, view = "plain",
                          hint, data, highlight, marker, pins, bare, groups, fit,
                          basemap, style }: {
  at: TileEndpoints; palette: Palette; theme: string; camera: Camera;
  height?: number; view?: string; hint?: string;
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /** Reaches to draw as selected — the route panel lights the chain down to the gauge. */
  highlight?: readonly SectionKey[];
  marker?: { lat: number; lon: number } | null;
  pins?: readonly { lat: number; lon: number; tone?: string; title?: string }[];
  /** Strip the renderer's zoom stack, compass, scale bar and attribution — see MapProps. */
  bare?: boolean;
  /** Which layer groups to draw. Omitted, the style's own defaults apply. */
  groups?: Record<string, boolean>;
  /** Send the camera to a subject — the search map opens on the best match. */
  fit?: MapProps["fit"];
  basemap?: MapProps["basemap"];
  /** Replaces the fixed height — e.g. `{ flexGrow: 1 }` to take what the list leaves. */
  style?: object;
}) {
  return (
    <View style={[style ? null : { height },
                  { backgroundColor: palette.tint, overflow: "hidden" }, style]}>
      <Map at={at} theme={theme} view={view} initial={camera} data={data}
           highlight={highlight} marker={marker} pins={pins} bare={bare} groups={groups}
           fit={fit} basemap={basemap}
           chrome={mapChrome(palette, theme)} />
      {hint && (
        <View style={{ position: "absolute", left: 12, bottom: 12, borderRadius: palette.r.pill,
                       paddingVertical: 7, paddingHorizontal: 13,
                       backgroundColor: palette.card, borderWidth: 1,
                       borderColor: palette.line, ...palette.lift }}>
          <Text style={{ ...TYPE.micro, fontSize: 11.5, color: palette.sub }}>{hint}</Text>
        </View>
      )}
    </View>
  );
}
