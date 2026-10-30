/**
 * (T302, owner 2026-10-30) SPOTS ON THE WEB: a short notice in place of the Spots tab and the spot capture — spots are
 * saved in the phone app, they don't show on the website yet, accounts are coming — with a link to the app. The phone
 * app never draws this. Spots a browser saved earlier stay where they are, unshown. Words: `@app/core` WEB_SPOTS_WORDS.
 */
import { Linking, Platform, ScrollView, Text, View } from "react-native";
import { WEB_SPOTS_WORDS as W } from "@app/core";
import { Button } from "./Button";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

/**
 * Does this build show the notice in place of spots? The web app does; the phone app never. The unit tests run on
 * react-native-web, so they draw the phone's spots unless a test asks for the web's (`showWebSpotsNotice`).
 */
let forced: boolean | null = null;
export const webSpotsNotice = (): boolean =>
  forced ?? (Platform.OS === "web" && !(typeof process !== "undefined" && process.env?.VITEST));
/** (tests) draw the web's notice, or (null) as the platform says */
export const showWebSpotsNotice = (on: boolean | null) => { forced = on; };

export function WebSpotsNotice({ palette, onDone }: {
  palette: Palette;
  /** shown where a spot was being added: the way back to the map; absent (the Spots tab): no such button */
  onDone?: () => void;
}) {
  return (
    <View testID="web-spots-notice" style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 16, paddingBottom: 12, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{W.title}</Text>
      </View>
      <ScrollView contentContainerStyle={{ padding: GUTTER, gap: SPACE.s3, maxWidth: 560 }}>
        <Text accessibilityRole="header" style={{ ...TYPE.name, color: palette.ink }}>{W.lead}</Text>
        {W.body.map((t, i) => <Text key={i} style={{ ...TYPE.body, color: palette.ink }}>{t}</Text>)}
        <View style={{ gap: SPACE.s2, marginTop: SPACE.s2 }}>
          <View testID="web-spots-get-app">
            <Button palette={palette} label={W.getApp}
                    onPress={() => { void Linking.openURL(W.appUrl); }} />
          </View>
          {onDone && <Button palette={palette} kind="ghost" label={W.back} onPress={onDone} />}
        </View>
      </ScrollView>
    </View>
  );
}
