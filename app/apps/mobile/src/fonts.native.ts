/**
 * The phone's faces (T910 moved them here from App.tsx): each from its own weight's folder — a family's index `require`s
 * every face it has, and Metro ships every one required (T741: 41 faces, 3.9 MB, where these 8 are drawn).
 * `tools/font-imports.test.ts` holds it.
 */
import type { FontSource } from "expo-font";
import { BricolageGrotesque_700Bold } from "@expo-google-fonts/bricolage-grotesque/700Bold";
import { BricolageGrotesque_800ExtraBold } from "@expo-google-fonts/bricolage-grotesque/800ExtraBold";
import { Archivo_400Regular } from "@expo-google-fonts/archivo/400Regular";
import { Archivo_500Medium } from "@expo-google-fonts/archivo/500Medium";
import { Archivo_600SemiBold } from "@expo-google-fonts/archivo/600SemiBold";
import { Archivo_700Bold } from "@expo-google-fonts/archivo/700Bold";
import { Archivo_800ExtraBold } from "@expo-google-fonts/archivo/800ExtraBold";
import { JetBrainsMono_500Medium } from "@expo-google-fonts/jetbrains-mono/500Medium";

export const FONT_SOURCES: Readonly<Record<string, FontSource>> = {
  BricolageGrotesque_700Bold, BricolageGrotesque_800ExtraBold,
  Archivo_400Regular, Archivo_500Medium, Archivo_600SemiBold, Archivo_700Bold, Archivo_800ExtraBold,
  JetBrainsMono_500Medium,
};
