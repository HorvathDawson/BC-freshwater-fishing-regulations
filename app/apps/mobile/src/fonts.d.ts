/**
 * The shared declaration for `./fonts` (T910): the app's type faces as `useFonts` takes them (App.tsx), one source per
 * face in `@app/ui-native` `FACE`. `fonts.native.ts` is each face's TTF from its expo-google-fonts weight folder (T741:
 * never a family's index); `fonts.web.ts` is each face's WOFF2 at `/assets/fonts/` (`webFonts.generated.ts`), drawn with
 * `font-display: swap`. Bundlers pick by platform extension, TypeScript does not.
 */
import type { FontSource } from "expo-font";

export declare const FONT_SOURCES: Readonly<Record<string, FontSource>>;
