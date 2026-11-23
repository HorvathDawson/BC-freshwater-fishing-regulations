/**
 * (T901, T1039) THE "GET THE APP" BLOCK: the heading, one plain line for what is still coming, the "try it early" lead, and
 * per platform one button in the same spot (its store link once public, else "Join the test": the email card, or the Report
 * form), decided by core's `getAppBlock`. Drawn by the web app's Spots notice (`WebSpotsNotice`) and by the public gauge page
 * (tools/seo `gaugeChart.tsx`; owner 2026-11-23: "chart should use same buttons as spots too for now"): one block, two callers.
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { appStoreLink, getAppBlock, playStoreLink, TESTFLIGHT_URL, type JoinStore, WEB_SPOTS_WORDS as W } from "@app/core";
import { Button } from "./Button";
import { JoinTestCard } from "./JoinTestCard";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";
import { linkTo } from "./openLink";

/** equal-width slots: side by side where two fit, else each the full width */
const SLOT = { flexBasis: 200, minWidth: 200 } as const;

export function GetAppBlock({ palette, contact = null, onReport, testflight = TESTFLIGHT_URL }: {
  palette: Palette;
  /** the app's contact value (More → Contact): "Join the test" emails it */
  contact?: string | null;
  /** no contact address: "Join the test" opens the Report form with this text in it (absent: no such press) */
  onReport?: (ask: string) => void;
  /** (T921) the public iPhone test invite (core's `TESTFLIGHT_URL`); null: the iPhone join is the email ask */
  testflight?: string | null;
}) {
  // (T911) a Join the test press with an address opens the card (email me / copy email); without one it is the Report form
  const [joining, setJoining] = useState<JoinStore | null>(null);
  const block = getAppBlock(playStoreLink(), appStoreLink(), contact, onReport !== undefined, testflight);
  return (
    <View testID="get-app" style={{ gap: SPACE.s2, alignItems: "center" }}>
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
    </View>
  );
}
