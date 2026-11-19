/**
 * (T302, owner 2026-10-30) SPOTS ON THE WEB: a short notice in place of the Spots tab and the spot capture — spots are
 * saved in the phone app, they don't show on the website yet, accounts are coming — with a link to the app. The phone
 * app never draws this. Spots a browser saved earlier stay where they are, unshown. Words: `@app/core` WEB_SPOTS_WORDS.
 */
import { Platform, ScrollView, View } from "react-native";
import { Text } from "./Text";
import { appStoreLink, PLAY_STORE_URL, WEB_SPOTS_WORDS as W } from "@app/core";
import { Button } from "./Button";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";
import { linkTo } from "./openLink";

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
  const appStore = appStoreLink();
  return (
    <View testID="web-spots-notice" style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: 16, paddingBottom: 12, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{W.title}</Text>
      </View>
      {/* (T367, owner 2026-11-04) in the middle of the pane, as the regs page's wait is */}
      <ScrollView contentContainerStyle={{ flexGrow: 1, padding: GUTTER, alignItems: "center", justifyContent: "center" }}>
       <View testID="web-spots-body" style={{ width: "100%", maxWidth: 560, gap: SPACE.s3, alignItems: "center" }}>
        <Text accessibilityRole="header" style={{ ...TYPE.name, color: palette.ink, textAlign: "center" }}>{W.lead}</Text>
        {W.body.map((t, i) => <Text key={i} style={{ ...TYPE.body, color: palette.ink, textAlign: "center" }}>{t}</Text>)}
        <View style={{ gap: SPACE.s2, marginTop: SPACE.s2, alignItems: "center" }}>
          {/* (T901) one button per store; the App Store's is a disabled "coming" until its page is public (core storeLinks) */}
          <Text style={{ ...TYPE.body, color: palette.ink, textAlign: "center" }}>{W.getApp}</Text>
          <View testID="web-spots-get-app" style={{ flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2, justifyContent: "center" }}>
            <Button palette={palette} label={W.playButton} grow={false} testID="web-spots-play" onPress={linkTo(PLAY_STORE_URL)} />
            {appStore
              ? <Button palette={palette} label={W.appStoreButton} grow={false} testID="web-spots-app-store" onPress={linkTo(appStore)} />
              : <Button palette={palette} label={W.appStoreSoon} grow={false} testID="web-spots-app-store-soon" />}
          </View>
          {onDone && <Button palette={palette} kind="ghost" label={W.back} onPress={onDone} />}
        </View>
       </View>
      </ScrollView>
    </View>
  );
}
