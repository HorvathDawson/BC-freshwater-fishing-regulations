/**
 * (T424) THE MAP'S CREDITS, ONE COMPONENT FOR BOTH MAPS: a square "©" chip at the map's bottom-right that opens a card
 * naming every source drawn (`creditLines(look)`, credits.ts — the only list). Neither renderer's own attribution control
 * is on any more (the web's HTML one and the binding's "i" differed in what they listed and how), so the wording, order
 * and look are the same on the web and the phone, and the licences' wording is ours to keep right: ODbL's
 * "© OpenStreetMap contributors" is linked in the card's Basemap and Streams-and-rules lines, one tap from the map;
 * the imagery provider's badge (`IMAGERY.badge`; Esri's "Powered by Esri") stays visible beside the chip while its imagery
 * is the ground (Esri's own requirement).
 *
 * THE OPEN CARD is a Modal (T729), centred over everything, never under the legend. WHERE (the chip): bottom-right, `GEOMETRY.right` from the edge and `insetBottom` over the foot block (the legend and its bar, a
 * sheet). The scale bar is bottom-LEFT (`GEOMETRY.scaleLeft`), so the two can never meet: `GEOMETRY` is what a test checks
 * at the narrowest width. A bare map (an illustration) draws none; the page that carries it credits the data.
 * The map UI rule: square corners, a card, our type, no loose text.
 *
 * SHOWN FIRST (OSMF attribution guideline, checked 2026-11-06): the credit must be visible when a map first shows, and
 * may then fold away — on a dismiss, or after five seconds — as long as the chip still finds it. So the first map of a
 * session shows "© OpenStreetMap contributors" beside the chip for `STARTUP_MS`, then folds to the chip; later maps in
 * the same session start folded (the guideline does not ask for it again on every map or on return from background).
 */
import { useEffect, useState, type ComponentType, type ReactNode } from "react";
import { Modal, Pressable, ScrollView, Text, View, useWindowDimensions } from "react-native";
import { IMAGERY } from "@app/core";
import { GEOMETRY } from "./chrome";
import { creditLines, type CreditPart } from "./credits";
import type { Look } from "./look";
import type { MapChrome } from "./map-props";

const TEXT = "Archivo_400Regular", SEMI = "Archivo_600SemiBold";

/** How long the startup credit shows before it folds to the chip (OSMF: "automatically after five seconds"). */
export const STARTUP_MS = 5000;
export const STARTUP_CREDIT = "© OpenStreetMap contributors";
/**
 * (T1212) the opening credit line's widest: from the © edge to `G.gap` past the scale bar's widest end (194 px at 320 wide;
 * the line wraps inside it); never under 140, a window not yet measured
 */
export function startupMaxWidth(width: number): number {
  return Math.max(140, width - GEOMETRY.right - (GEOMETRY.scaleLeft + GEOMETRY.scaleMax + GEOMETRY.gap));
}
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

const ownBack = (_open: boolean, close: () => void) => close;
/** No platform back around the card (the map alone): its children as they are. */
function NoWrap({ children }: { onBack: () => void; children: ReactNode }) { return <>{children}</>; }

export function MapCredits({ look, chrome, insetBottom = 0, footBottom = 0, onOpenLink, onLongPress, useBackLayer = ownBack, modalBack: Back = NoWrap }: {
  look: Pick<Look, "view" | "modes" | "groups" | "basemap">; chrome: MapChrome; insetBottom?: number;
  /**
   * (T1212) the height of the block over the map's foot (MapProps.insetBottom), when the © itself sits lower, beside a
   * floating key: the opening credit line stands above that block, so it never meets the key chip or its isolate row
   */
  footBottom?: number;
  onOpenLink?: ((url: string) => void) | undefined;
  /** (T729) the app's one back for the open card (MapProps.useBackLayer); absent: the card's own close */
  useBackLayer?: ((open: boolean, close: () => void) => () => void) | undefined;
  /** (T723) the platform's back around the open card (MapProps.modalBack: the iOS left-edge swipe); absent: none */
  modalBack?: ComponentType<{ onBack: () => void; children: ReactNode }> | undefined;
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
  const requestClose = useBackLayer(on, () => setOn(false));
  // (T729) THE CARD DESCRIBES THE LOOK IT WAS OPENED ON: a tab change (Map, Conditions, Runs) changes the look, and the card
  // goes with it, scrim and all. One rule: if the card is not what the reader asked for, nothing of it is in the tree
  // (the Modal is rendered only while `on`, so a scrim can never outlive its card).
  const lookSig = JSON.stringify([look.view, look.modes, look.groups, look.basemap]);
  useEffect(() => { setOn(false); }, [lookSig]);
  const lines = creditLines(look);
  const G = GEOMETRY;
  const { width, height } = useWindowDimensions();
  // (T1212) the chip's own foot, on the grid; its 44 px press reaches `out` further each way (the box is that much further out)
  const bottom = G.bottom + insetBottom;
  const out = (G.target - G.chip) / 2;
  const badge = look.basemap === "satellite" && IMAGERY.badge !== null;
  // (T729) the card's spacing from the window's edge, its own (not the chip's grid)
  const EDGE = 8;
  return (
    <>
      {/* (T1212, owner 2026-11-26: "the attribution moved to botton corner") THE © IN THE CORNER: bottom-right, `G.right` in
          and `G.bottom` up from the map's foot or from a bar across it (`insetBottom`), a 44 px press; Esri's badge, while
          its imagery is drawn, stands over it rather than beside it, so it never reaches the key chip at the left */}
      <View testID="map-credits" pointerEvents="box-none"
            style={{ position: "absolute", right: G.right - out, bottom: bottom - out, flexDirection: "column", alignItems: "flex-end" }}>
        {badge && (
          <View testID="esri-badge" pointerEvents="none"
                style={{ height: G.chip, justifyContent: "center", paddingHorizontal: 6, backgroundColor: chrome.card, marginRight: out,
                         borderWidth: 1, borderColor: chrome.ink }}>
            <Text style={{ color: chrome.ink, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>{IMAGERY.badge}</Text>
          </View>
        )}
        <Pressable testID="map-credits-chip" accessibilityRole="button" accessibilityLabel="Map credits"
                   aria-expanded={on} onPress={() => setOn((v) => !v)} onLongPress={onLongPress}
                   style={{ width: G.target, height: G.target, alignItems: "center", justifyContent: "center" }}>
          <View testID="map-credits-face" style={{ width: G.chip, height: G.chip, alignItems: "center", justifyContent: "center",
                                                   backgroundColor: on ? chrome.tint : chrome.card, borderWidth: 1, borderColor: chrome.ink }}>
            {/* the glyph grows with the system text to 1.3 (a phone tab bar's cap, ui-native type.ts LABEL_SCALE): 20 px in a 28 px chip */}
            <Text maxFontSizeMultiplier={1.3} style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 15, lineHeight: 18 }}>©</Text>
          </View>
        </Pressable>
      </View>
      {startup && !on && (
        // above the chip, where the card opens, and (T1212) above the foot block when the © sits beside a floating key, so
        // it never touches the key chip or its isolate row; its width stops short of the bottom-left scale bar (it wraps
        // rather than reach it, at any width or text size): the attribution stays whole and readable
        <Pressable testID="map-credits-startup" accessibilityRole="button" accessibilityLabel={STARTUP_CREDIT}
                   onPress={() => setStartup(false)}
                   style={{ position: "absolute", right: G.right,
                            bottom: Math.max(bottom + G.chip + G.gap + (badge ? G.chip + out : 0), footBottom + G.bottom),
                            maxWidth: startupMaxWidth(width), paddingHorizontal: 6,
                            paddingVertical: 3, backgroundColor: chrome.card, borderWidth: 1, borderColor: chrome.ink }}>
          <Text style={{ color: chrome.ink, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>{STARTUP_CREDIT}</Text>
        </Pressable>
      )}
      {on && (
        // (T729) THE OPEN CARD IS IN A MODAL: drawn inside the map it sat under the app's own overlays (the legend and the
        // keys in the foot block are siblings of the map, painted over it), so a tap on the © opened a card nobody could
        // read. A Modal is above every one of them, whatever the legend's height or state; centred and capped to the
        // window, so it is inside the screen and whole. Closing is as before (Close, the chip is under it) plus the dim
        // and the system back.
        // (T723) inside it, the platform's back (the iOS left-edge swipe) calls the card's one back, as Android's back does
        // through `onRequestClose`; without it the swipe only closed the card by landing on the scrim as a tap
        <Modal visible transparent animationType="none" statusBarTranslucent onRequestClose={requestClose}>
          <Back onBack={requestClose}>
          <Pressable testID="map-credits-scrim" accessibilityLabel="Close the credits" onPress={() => setOn(false)}
                     style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: EDGE * 2,
                              backgroundColor: chrome.scrim }}>
            <View testID="map-credits-card" accessibilityRole="summary" accessibilityLabel="Map credits"
                  onStartShouldSetResponder={() => true}
                  style={{ width: Math.min(G.cardMax, width - 4 * EDGE), maxHeight: Math.max(160, height - 8 * EDGE),
                           backgroundColor: chrome.card, borderWidth: 1,
                           borderColor: chrome.ink, boxShadow: `2px 2px 0 ${chrome.shadow}` }}>
              <ScrollView contentContainerStyle={{ padding: 10, gap: 6 }}>
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
              </ScrollView>
            </View>
          </Pressable>
          </Back>
        </Modal>
      )}
    </>
  );
}
