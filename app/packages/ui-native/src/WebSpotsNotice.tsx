/**
 * (T302, owner 2026-10-30) SPOTS ON THE WEB: a short notice in place of the Spots tab and the spot capture — spots are
 * saved in the phone app, they don't show on the website yet, accounts are coming — with a link to the app. The phone
 * app never draws this. Spots a browser saved earlier stay where they are, unshown. Words: `@app/core` WEB_SPOTS_WORDS.
 */
import { useState } from "react";
import { Platform, ScrollView, View } from "react-native";
import { Text } from "./Text";
import { appStoreLink, getAppBlock, TESTFLIGHT_URL, type JoinStore, playStoreLink, WEB_SPOTS_WORDS as W } from "@app/core";
import { Button } from "./Button";
import { JoinTestCard } from "./JoinTestCard";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";
import { linkTo } from "./openLink";
import { phoneScreensOnWeb } from "./storeShots";

/**
 * Does this build show the notice in place of spots? The web app does; the phone app never. The unit tests run on
 * react-native-web, so they draw the phone's spots unless a test asks for the web's (`showWebSpotsNotice`).
 */
let forced: boolean | null = null;
export const webSpotsNotice = (): boolean =>
  forced ?? (Platform.OS === "web" && !phoneScreensOnWeb() && !(typeof process !== "undefined" && process.env?.VITEST));
/** (tests) draw the web's notice, or (null) as the platform says */
export const showWebSpotsNotice = (on: boolean | null) => { forced = on; };

/** equal-width slots: side by side where two fit, else each the full width */
const SLOT = { flexBasis: 200, minWidth: 200 } as const;

export function WebSpotsNotice({ palette, onDone, contact = null, onReport, testflight = TESTFLIGHT_URL }: {
  palette: Palette;
  /** (T921) the public iPhone test invite (core's `TESTFLIGHT_URL`); null: the iPhone join is the email ask */
  testflight?: string | null;
  /** (T901) the app's contact value (More → Contact): "Join the test" emails it */
  contact?: string | null;
  /** (T901) no contact address: "Join the test" opens the Report form with this text in it (absent: no such press) */
  onReport?: (ask: string) => void;
  /** shown where a spot was being added: the way back to the map; absent (the Spots tab): no such button */
  onDone?: () => void;
}) {
  // (T911) a Join the test press with an address opens the card (email me / copy email); without one it is the Report form
  const [joining, setJoining] = useState<JoinStore | null>(null);
  const block = getAppBlock(playStoreLink(), appStoreLink(), contact, onReport !== undefined, testflight);
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
          {/* (T901, owner 2026-11-19) one plain line for what is still coming, then one button per platform in the same
              spot: its store link once public, else "Join the test" (an email, or the Report form) */}
          <Text style={{ ...TYPE.name, color: palette.ink, textAlign: "center" }}>{W.getApp}</Text>
          {block.coming && <Text testID="web-spots-coming" style={{ ...TYPE.body, color: palette.sub, textAlign: "center" }}>{block.coming}</Text>}
          {block.lead && <Text style={{ ...TYPE.small, color: palette.sub, textAlign: "center" }}>{block.lead}</Text>}
          <View testID="web-spots-get-app" style={{ width: "100%", maxWidth: 440, flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 }}>
            {block.slots.map((x) => x.kind === "store"
              ? <Button key={x.store} palette={palette} label={x.label} style={SLOT} testID={x.store === "android" ? "web-spots-play" : "web-spots-app-store"}
                        onPress={linkTo(x.url)} />
              : <Button key={x.store} palette={palette} label={x.label} style={SLOT} testID={`web-spots-join-${x.store}`}
                        onPress={x.ask.mailto !== null ? () => setJoining(x.store) : () => onReport?.(`${x.ask.subject}\n\n${x.ask.body}`)} />)}
          </View>
          {joining !== null && (() => {
            const slot = block.slots.find((x) => x.kind === "join" && x.store === joining);
            return slot && slot.kind === "join"
              ? <JoinTestCard store={joining} ask={slot.ask} palette={palette} onClose={() => setJoining(null)} /> : null;
          })()}
          {onDone && <Button palette={palette} kind="ghost" label={W.back} onPress={onDone} />}
        </View>
       </View>
      </ScrollView>
    </View>
  );
}
