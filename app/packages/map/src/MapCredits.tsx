/**
 * (T424) THE MAP'S CREDITS, ONE COMPONENT FOR BOTH MAPS: a square "©" chip at the map's bottom-right that opens a card
 * naming every source drawn (`creditLines(look)`, credits.ts — the only list). Neither renderer's own attribution control
 * is on any more (the web's HTML one and the binding's "i" differed in what they listed and how), so the wording, order
 * and look are the same on the web and the phone, and the licences' wording is ours to keep right: ODbL's
 * "© OpenStreetMap contributors" is linked in the card's Basemap and Streams-and-rules lines, one tap from the map;
 * Esri's "Powered by Esri" stays visible beside the chip while its imagery is the ground (Esri's own requirement).
 *
 * WHERE: bottom-right, `GEOMETRY.right` from the edge and `insetBottom` over the foot block (the legend and its bar, a
 * sheet). The scale bar is bottom-LEFT (`GEOMETRY.scaleLeft`), so the two can never meet: `GEOMETRY` is what a test checks
 * at the narrowest width. A bare map (an illustration) draws none; the page that carries it credits the data.
 * The map UI rule: square corners, a card, our type, no loose text.
 *
 * SHOWN FIRST (OSMF attribution guideline, checked 2026-11-06): the credit must be visible when a map first shows, and
 * may then fold away — on a dismiss, or after five seconds — as long as the chip still finds it. So the first map of a
 * session shows "© OpenStreetMap contributors" beside the chip for `STARTUP_MS`, then folds to the chip; later maps in
 * the same session start folded (the guideline does not ask for it again on every map or on return from background).
 */
import { useEffect, useState } from "react";
import { Pressable, Text, View, useWindowDimensions } from "react-native";
import { ESRI_BADGE, GEOMETRY } from "./chrome";
import { creditLines, type CreditPart } from "./credits";
import type { Look } from "./look";
import type { MapChrome } from "./map-props";

const TEXT = "Archivo_400Regular", SEMI = "Archivo_600SemiBold";

/** How long the startup credit shows before it folds to the chip (OSMF: "automatically after five seconds"). */
export const STARTUP_MS = 5000;
export const STARTUP_CREDIT = "© OpenStreetMap contributors";
/** One startup credit per session: true once any map has shown it. */
let shownThisSession = false;
/** Tests only: start a new session. */
export function resetStartupCredit(): void { shownThisSession = false; }

/** A link leaves through the app's one way (`onOpenLink`); with none given the licence is named, not linked. */
function Parts({ parts, chrome, onOpenLink }: { parts: readonly CreditPart[]; chrome: MapChrome;
                                                onOpenLink: ((url: string) => void) | undefined }) {
  return (
    <>
      {parts.map((p, i) => typeof p === "string" || !onOpenLink
        ? <Text key={i}>{typeof p === "string" ? p : p.text}</Text>
        : <Text key={i} accessibilityRole="link" onPress={() => onOpenLink(p.href)}
                style={{ color: chrome.accent, textDecorationLine: "underline" }}>{p.text}</Text>)}
    </>
  );
}

export function MapCredits({ look, chrome, insetBottom = 0, onOpenLink, onLongPress }: {
  look: Pick<Look, "view" | "modes" | "groups" | "basemap">; chrome: MapChrome; insetBottom?: number;
  onOpenLink?: ((url: string) => void) | undefined;
  /** (T420) a lab build's way into the map lab (`MapLab`); none in any other build */
  onLongPress?: (() => void) | undefined;
}) {
  const [on, setOn] = useState(false);
  const [startup, setStartup] = useState(() => !shownThisSession);
  useEffect(() => {
    if (!startup) return;
    shownThisSession = true;
    const t = setTimeout(() => setStartup(false), STARTUP_MS);
    return () => clearTimeout(t);
  }, [startup]);
  const lines = creditLines(look);
  const G = GEOMETRY;
  const { width } = useWindowDimensions();
  const bottom = G.bottom + insetBottom;
  return (
    <>
      <View testID="map-credits" pointerEvents="box-none"
            style={{ position: "absolute", right: G.right, bottom, flexDirection: "row", alignItems: "flex-end", gap: G.gap }}>
        {look.basemap === "satellite" && (
          <View testID="esri-badge" pointerEvents="none"
                style={{ height: G.chip, justifyContent: "center", paddingHorizontal: 6, backgroundColor: chrome.card,
                         borderWidth: 1, borderColor: chrome.ink }}>
            <Text style={{ color: chrome.ink, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>{ESRI_BADGE}</Text>
          </View>
        )}
        <Pressable testID="map-credits-chip" accessibilityRole="button" accessibilityLabel="Map credits"
                   aria-expanded={on} onPress={() => setOn((v) => !v)} onLongPress={onLongPress} hitSlop={8}
                   style={{ width: G.chip, height: G.chip, alignItems: "center", justifyContent: "center",
                            backgroundColor: on ? chrome.tint : chrome.card, borderWidth: 1, borderColor: chrome.ink }}>
          <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 15, lineHeight: 18 }}>©</Text>
        </Pressable>
      </View>
      {startup && !on && (
        // above the chip, where the card opens: the bottom-left scale bar is never in reach, badge or not
        <Pressable testID="map-credits-startup" accessibilityRole="button" accessibilityLabel={STARTUP_CREDIT}
                   onPress={() => setStartup(false)}
                   style={{ position: "absolute", right: G.right, bottom: bottom + G.chip + G.gap, paddingHorizontal: 6,
                            paddingVertical: 3, backgroundColor: chrome.card, borderWidth: 1, borderColor: chrome.ink }}>
          <Text style={{ color: chrome.ink, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>{STARTUP_CREDIT}</Text>
        </Pressable>
      )}
      {on && (
        <View testID="map-credits-card" accessibilityRole="summary" accessibilityLabel="Map credits"
              style={{ position: "absolute", right: G.right, bottom: bottom + G.chip + G.gap, width: Math.min(G.cardMax, width - 2 * G.right),
                       backgroundColor: chrome.card, borderWidth: 1,
                       borderColor: chrome.ink, padding: 10, gap: 6, boxShadow: `2px 2px 0 ${chrome.shadow}` }}>
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
            <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 13, lineHeight: 16 }}>Map credits</Text>
            <Pressable testID="map-credits-close" accessibilityRole="button" accessibilityLabel="Close the credits"
                       onPress={() => setOn(false)} hitSlop={10}
                       style={{ paddingHorizontal: 6, paddingVertical: 2, borderWidth: 1, borderColor: chrome.ink }}>
              <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 12, lineHeight: 15 }}>Close</Text>
            </Pressable>
          </View>
          {lines.map((l) => (
            <Text key={l.id} testID={`map-credit-${l.id}`}
                  style={{ color: chrome.sub, fontFamily: TEXT, fontSize: 12, lineHeight: 16 }}>
              <Text style={{ color: chrome.ink, fontFamily: SEMI }}>{`${l.label}: `}</Text>
              <Parts parts={l.parts} chrome={chrome} onOpenLink={onOpenLink} />
            </Text>
          ))}
        </View>
      )}
    </>
  );
}
