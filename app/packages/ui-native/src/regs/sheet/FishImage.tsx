/**
 * The fish sheet's pictures (prototype sp: `.pic` + `.cred`); the hatch sheet's two drawings are `AdiposeArt` (T38).
 * The images are handed down (ui-native registers no asset — layers.json): a code with no source draws a plain ground
 * of the same size. No logic.
 *
 * (T46) A FISH'S LOOKS (ocean / spawning, sea-run / resident …: the sheet's consecutive `image` blocks) are drawn as one
 * group, `FishLooks`: side by side when there is room (a tablet or a desktop: a row that wraps when a look would fall
 * under FISH_PICTURE.minWidth), stacked on a phone, each picture at most FISH_PICTURE.maxWidth across so it never scales
 * with the window. Each look's caption is its block's `credit` — the manifest's caption · phase · credit, composed in
 * core (`fishSheet`), never written here. No ground behind a fish (T39): a picture's box paints nothing of its own.
 */
import { Image, Text, View } from "react-native";
import { FISH, type SheetBlock } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { TYPE } from "../../type";
import { FISH_PICTURE, type Palette } from "../../theme";
import type { FishImages } from "../types";

type ImageBlock = Extract<SheetBlock, { k: "image" }>;

export function FishImage({ code, file, alt, credit, width, height, fish, palette }: {
  code: string; file: string; alt: string; credit: string; width: number; height: number; fish?: FishImages; palette: Palette;
}) {
  // FishImages holds a code's pictures in the manifest's order (emit-fish.mjs): this picture is the one at its file's place
  const src = fish?.[code]?.[Math.max(0, FISH[code]?.findIndex((f) => f.file === file) ?? 0)];
  return (
    <View testID="fish-look" style={{ marginTop: 8, width: "100%", maxWidth: FISH_PICTURE.maxWidth }}>
      {/* the box keeps the picture's own proportions; the image fills it (an Image's own aspectRatio is not kept on web) */}
      <View testID="fish-look-box" accessible accessibilityRole="image" accessibilityLabel={alt}
            style={{ width: "100%", aspectRatio: width / height, backgroundColor: src ? undefined : palette.tint }}>
        {src ? <Image source={src} accessibilityIgnoresInvertColors resizeMode="contain" style={{ width: "100%", height: "100%" }} /> : null}
      </View>
      <Text style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{credit}</Text>
    </View>
  );
}

/** A fish's looks: a row on a tablet / desktop (wrapping), a column on a phone. */
export function FishLooks({ looks, layout, fish, palette }: {
  looks: readonly ImageBlock[]; layout: LayoutClass; fish?: FishImages; palette: Palette;
}) {
  const wide = layout !== "phone";
  return (
    <View testID="fish-looks" style={wide ? { flexDirection: "row", flexWrap: "wrap", columnGap: FISH_PICTURE.gap } : { flexDirection: "column" }}>
      {looks.map((b) => {
        const one = <FishImage key={b.file} code={b.code} file={b.file} alt={b.alt} credit={b.credit} width={b.width} height={b.height}
                               fish={fish} palette={palette} />;
        // beside another: grows from the least width up to the most, so two share a row and one never passes the token
        return wide
          ? <View key={b.file} style={{ flexGrow: 1, flexShrink: 1, flexBasis: FISH_PICTURE.minWidth, maxWidth: FISH_PICTURE.maxWidth }}>{one}</View>
          : one;
      })}
    </View>
  );
}
