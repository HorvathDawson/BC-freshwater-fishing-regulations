/**
 * T38 — the "Hatchery or wild?" sheet's two drawings: Wild · adipose fin there / Hatchery · clipped, a healed scar
 * instead, the spot ringed (prototype regs-v11.html sheetHtml hatch, then each figcaption — page v36 adiposeHtml's words).
 *
 * (T886, owner 2026-11-18: "the how to tell hatchery fish is very smile creepy … Could use the salmon general shape from
 * loading maybe?") The fish is the loading screen's coho, at rest with its mouth closed (`tools/build-adipose.mjs`,
 * `fish-loader/adipose_art.py`), PRINTED in this theme's tokens (`palette.coho.ink` / `.red`, the ring `palette.accent`)
 * as the loader's sheet is: one pair per theme (`adipose.generated.ts`), picked by the palette, never tinted. Transparent
 * ground: the card shows through (D4, no tile behind a picture).
 *
 * Each figure is one image to a screen reader, named by its caption. The caption's first run is the figure's name,
 * set bold as adiposeHtml's `<b>Wild</b>`; the words are core's (`hatchSheet`), never written here. No logic.
 *
 * (T46) Each figure at most the fish picture's measure; on a tablet / desktop the two sit side by side (wrapping),
 * as a fish's looks do (FishLooks); on a phone one above the other, as the prototype.
 */
import { Image, View } from "react-native";
import { Text } from "../Text";
import { plain, type Line } from "@app/core";
import type { LayoutClass } from "@app/ui";
import { TYPE } from "../type";
import { FISH_PICTURE, THEMES, type Palette, type ThemeName } from "../theme";
import { ADIPOSE } from "../adipose.generated";

/** A caption's runs as drawn, spaces kept (`plain` trims: " · adipose fin there" must keep its leading space). */
const words = (l: Line) => l.map((r) => (r.t === "text" ? r.s : plain([r]))).join("");

/** The drawings' own proportions (tools/build-adipose.mjs). */
const ASPECT = ADIPOSE.size.width / ADIPOSE.size.height;

/**
 * (T886) The pair printed in this palette's coho ink + red and accent: the theme whose tokens those are. A palette no theme
 * wears has none, and says so (a drawing in other colours would be a guess), as `cohoSheet`.
 */
export function adiposeArt(palette: Palette): (typeof ADIPOSE.art)[ThemeName] {
  const name = (Object.keys(THEMES) as ThemeName[]).find((t) => THEMES[t].coho.ink === palette.coho.ink
    && THEMES[t].coho.red === palette.coho.red && THEMES[t].accent === palette.accent);
  if (!name) throw new Error(`no hatch drawing is printed in ${palette.coho.ink} + ${palette.coho.red} + ${palette.accent} (pnpm adipose:build)`);
  return ADIPOSE.art[name];
}

export function AdiposeArt({ figures, palette, layout = "phone" }: {
  figures: readonly { variant: "wild" | "clipped"; caption: Line }[];
  palette: Palette;
  layout?: LayoutClass;
}) {
  const wide = layout !== "phone";
  const art = adiposeArt(palette);
  return (
    <View testID="adipose-art" style={wide ? { flexDirection: "row", flexWrap: "wrap", columnGap: FISH_PICTURE.gap } : { flexDirection: "column" }}>
      {figures.map((f) => {
        const src = art[f.variant];
        const [name, ...rest] = f.caption;
        const fig = (
          <View key={f.variant} testID="adipose-figure" style={{ marginTop: 8, width: "100%", maxWidth: FISH_PICTURE.maxWidth }}>
            {/* the box keeps the drawing's proportions; the image fills it (an Image's own aspectRatio is not kept on web).
                (T40) the drawing is the image, named by its caption; the caption under it stays text (a label on the figure
                would hide it) */}
            <View testID="adipose-picture" accessible accessibilityRole="image" accessibilityLabel={plain(f.caption)}
                  style={{ width: "100%", aspectRatio: ASPECT }}>
              <Image source={src} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: "100%", height: "100%" }} />
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
