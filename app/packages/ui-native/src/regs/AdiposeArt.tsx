/**
 * T38 — the "Hatchery or wild?" sheet's two drawings: Wild · adipose fin there / Hatchery · clipped, a healed scar
 * instead, the adipose fin ringed (prototype regs-v11.html sheetHtml hatch: `FISHART.draw('RB', {marks:false,
 * adiMark:true, clipped: i++ === 1})` at the start of each `figure.fishart`, then its figcaption — page v36 adiposeHtml's
 * words). The drawings are pre-rendered WebP cut-outs (tools/emit-adipose.mjs) registered by apps/mobile
 * (`ADIPOSE_IMAGES`, emit-fish) and handed down: ui-native registers no asset (layers.json). No ground behind them
 * (D4, r20h-picture-surface): the drawing carries its own light body; on the dark card it reads as a cut-out, like the
 * fish photos. An app that registered none draws an empty box of the same size (the fish picture's fallback).
 *
 * Each figure is one image to a screen reader, named by its caption. The caption's first run is the figure's name,
 * set bold as adiposeHtml's `<b>Wild</b>`; the words are core's (`hatchSheet`), never written here. No logic.
 *
 * (T46) Each figure at most the fish picture's measure; on a tablet / desktop the two sit side by side (wrapping),
 * as a fish's looks do (FishLooks); on a phone one above the other, as the prototype.
 */
import { Image, Text, View, type ImageSourcePropType } from "react-native";
import { plain, type Line } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { TYPE } from "../type";
import { FISH_PICTURE, type Palette } from "../theme";
import type { AdiposeImages } from "./types";

/** A caption's runs as drawn, spaces kept (`plain` trims: " · adipose fin there" must keep its leading space). */
const words = (l: Line) => l.map((r) => (r.t === "text" ? r.s : plain([r]))).join("");

/** The drawings' own proportions (tools/emit-adipose.mjs SIZE: 800 × 300). */
const ASPECT = 800 / 300;

export function AdiposeArt({ figures, images, palette, layout = "phone" }: {
  figures: readonly { variant: "wild" | "clipped"; caption: Line }[];
  images?: AdiposeImages;
  palette: Palette;
  layout?: LayoutClass;
}) {
  const wide = layout !== "phone";
  return (
    <View testID="adipose-art" style={wide ? { flexDirection: "row", flexWrap: "wrap", columnGap: FISH_PICTURE.gap } : { flexDirection: "column" }}>
      {figures.map((f) => {
        const src: ImageSourcePropType | undefined = images?.[f.variant];
        const [name, ...rest] = f.caption;
        const fig = (
          <View key={f.variant} testID="adipose-figure" style={{ marginTop: 8, width: "100%", maxWidth: FISH_PICTURE.maxWidth }}>
            {/* the box keeps the drawing's proportions; the image fills it (an Image's own aspectRatio is not kept on web).
                (T40) the drawing is the image, named by its caption; the caption under it stays text (a label on the figure
                would hide it) */}
            <View testID="adipose-picture" accessible accessibilityRole="image" accessibilityLabel={plain(f.caption)}
                  style={{ width: "100%", aspectRatio: ASPECT, backgroundColor: src ? undefined : palette.tint }}>
              {src ? <Image source={src} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: "100%", height: "100%" }} /> : null}
            </View>
            <Text testID="adipose-caption" style={[TYPE.meta, { color: palette.ink, marginTop: 4 }]}>
              {name ? <Text style={TYPE.metaBold}>{words([name])}</Text> : null}{words(rest)}
            </Text>
          </View>
        );
        return wide
          ? <View key={f.variant} style={{ flexGrow: 1, flexShrink: 1, flexBasis: FISH_PICTURE.minWidth, maxWidth: FISH_PICTURE.maxWidth }}>{fig}</View>
          : fig;
      })}
    </View>
  );
}
