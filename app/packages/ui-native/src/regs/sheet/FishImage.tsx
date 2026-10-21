/**
 * The fish sheet's pictures (prototype sp: `.pic` + `.cred`) and the hatch sheet's two adipose figures (page v36
 * adiposeHtml). The images are handed down (ui-native registers no asset — layers.json): a code with no source, or an
 * adipose figure the app has not registered (T38), draws a plain ground of the same size. No logic.
 */
import { Image, Text, View, type ImageSourcePropType } from "react-native";
import { FISH, type Line } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";
import type { FishImages } from "../types";

export function FishImage({ code, file, alt, credit, width, height, fish, palette }: {
  code: string; file: string; alt: string; credit: string; width: number; height: number; fish?: FishImages; palette: Palette;
}) {
  // FishImages holds a code's pictures in the manifest's order (emit-fish.mjs): this picture is the one at its file's place
  const src = fish?.[code]?.[Math.max(0, FISH[code]?.findIndex((f) => f.file === file) ?? 0)];
  return (
    <View style={{ marginTop: 8 }}>
      {/* the box keeps the picture's own proportions; the image fills it (an Image's own aspectRatio is not kept on web) */}
      <View accessibilityLabel={alt} style={{ width: "100%", aspectRatio: width / height, backgroundColor: src ? undefined : palette.tint }}>
        {src ? <Image source={src} accessibilityIgnoresInvertColors resizeMode="contain" style={{ width: "100%", height: "100%" }} /> : null}
      </View>
      <Text style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{credit}</Text>
    </View>
  );
}

export function Adipose({ figures, images, palette, actions }: {
  figures: readonly { variant: "wild" | "clipped"; caption: Line }[];
  images?: { wild: ImageSourcePropType; clipped: ImageSourcePropType }; palette: Palette; actions: KeepActions;
}) {
  return (
    <View style={{ flexDirection: "row", gap: 12, marginTop: 8 }}>
      {figures.map((f) => (
        <View key={f.variant} style={{ flex: 1 }}>
          {images ? <Image source={images[f.variant]} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: "100%", aspectRatio: 2 }} />
            : <View style={{ width: "100%", aspectRatio: 2, backgroundColor: palette.tint }} />}
          <LineText line={f.caption} style={TYPE.meta} color={palette.ink} palette={palette} actions={actions} />
        </View>
      ))}
    </View>
  );
}
