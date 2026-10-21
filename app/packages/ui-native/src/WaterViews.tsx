/**
 * The Conditions and Map views of a water in water mode (the Regulations view is `WaterScreen`). Each draws the
 * water's header (`FaceBar`, through `WaterViewsContext`) above its body, so the back row and the view strip sit in
 * the same place on all three views and a swipe on that row moves between them.
 *
 *   Conditions  the Conditions screen (the same one the Conditions tab drills into — one screen, AGENTS rule 23) on
 *               the reach the reader TAPPED, with the tap's "you are here"; opened from search or a list there is no
 *               tap, and it opens on the water's first reach until P14 supplies the part's midpoint (`mid_sid`).
 *   Map         the water framed and lit on the map (`usePickFocus`, the search's framing); the parts and their steps
 *               are P15's. The canvas pans, so here the swipe listens on the header only.
 */
import { Text, View } from "react-native";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { useWater } from "@app/ui";
import { ConditionsScreen } from "./ConditionsScreen";
import { FaceBar } from "./Faces";
import { FishSpinner } from "./FishSpinner";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

type ScreenProps = Omit<React.ComponentProps<typeof ConditionsScreen>, "section" | "source" | "palette">;

export function WaterConditions({ source, item, tapSid, palette, ...screen }: ScreenProps & {
  source: RegsSource; item: ItemId; tapSid: SectionId | null; palette: Palette;
}) {
  // the water is read only when there is no tap to open on
  const sheet = useWater(source, tapSid === null ? item : null);
  const section = tapSid ?? (sheet.state === "ready" ? sheet.value?.sections[0] ?? null : null);
  if (section === null) {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card, paddingHorizontal: GUTTER, paddingTop: SPACE.s3 }}>
        <FaceBar palette={palette} face="conditions" onFace={() => {}} />
        {sheet.state === "loading" && (
          <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
            <FishSpinner palette={palette} size={110} label="Loading this water" />
          </View>
        )}
      </View>
    );
  }
  return <ConditionsScreen source={source} section={section} palette={palette} {...screen} />;
}

export function WaterMap({ source, item, palette, children }: {
  source: RegsSource; item: ItemId; palette: Palette;
  /** The map. */
  children: React.ReactNode;
}) {
  const sheet = useWater(source, item);
  const name = sheet.state === "ready" ? sheet.value?.name ?? null : null;
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      {/* THE SAME INSETS AS THE OTHER TWO VIEWS, so the header does not move when the view changes. */}
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s2, gap: SPACE.s2 }}>
        <FaceBar palette={palette} face="regulations" onFace={() => {}} />
        {name && <Text style={{ ...TYPE.title, color: palette.ink }} numberOfLines={1}>{name}</Text>}
      </View>
      <View style={{ flex: 1 }}>{children}</View>
    </View>
  );
}
