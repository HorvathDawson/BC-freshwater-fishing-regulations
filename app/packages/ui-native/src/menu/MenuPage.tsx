/**
 * One menu page, full screen over the app: "‹ Back" and the page's title, then the page. The water screen's header
 * shape (FaceBar's back link, the screen title face) so a menu page reads as part of the same app.
 *
 * A back layer while open (G1): the ‹, Android's back (the Modal's onRequestClose) and the browser's back all close it.
 *
 * (T859, owner 2026-11-18) AT DESKTOP WIDTH (`layoutClassFor` "desktop", ≥ 1280 px) the page is a CARD over the page
 * instead (`MenuCard`): centred, `MENU_CARD` wide, at most `MENU_CARD.maxHeight` of the window tall with its own scroll,
 * over the palette's scrim. A press on the scrim and the card's ✕ close the page (`onClose`, every level at once);
 * Escape (the web Modal's own, its onRequestClose) and the browser's back are the one back, so they close ONE level (a
 * glossary term, then the page). Focus: the web Modal moves it into the card (its first control, the ✕) and back to what
 * opened it (the More row) when it closes. Phones and tablets draw the full-window page exactly as before.
 */
import { Modal, ScrollView, View, useWindowDimensions } from "react-native";
import { Selectable, Text } from "../Text";
import { Pressable } from "../Pressable";
import { WindowInsets } from "../WindowInset";
import { MENU_WORDS, SHEET_WORDS } from "@app/core";
import { useBackLayer, useLayoutClass } from "@app/ui";
import { Icon, IconText } from "../icons";
import { TYPE } from "../type";
import { GUTTER, MENU_CARD, MORE_WIDE, SPACE, type Palette } from "../theme";
import { hitProps } from "../regs/hit";
import { ModalBack } from "../ModalBack";

interface MenuPageProps {
  open: boolean; title: string; onBack: () => void; palette: Palette; children: React.ReactNode;
  /** (T859) close the whole page, every level at once (the desktop card's ✕ and scrim); absent: `onBack` */
  onClose?: () => void;
  /** where the ‹ goes, when not simply back (a glossary term: "Glossary") */
  backLabel?: string;
  /** false: the page scrolls itself (a long list) */
  scroll?: boolean;
  testID?: string;
}

export function MenuPage(props: MenuPageProps) {
  const { open, title, onBack, palette, children, scroll = true, testID, backLabel } = props;
  const requestClose = useBackLayer(open, onBack);
  const layout = useLayoutClass();
  if (layout === "desktop") return <MenuCard {...props} requestClose={requestClose} />;
  // (T167) on a wide screen the page reads in a centred column (`MORE_WIDE.page`), its head's words on the same edge
  const column = layout !== "phone"
    ? { width: "100%" as const, maxWidth: MORE_WIDE.page, alignSelf: "center" as const } : null;
  return (
    <Modal visible={open} animationType="slide" onRequestClose={requestClose} aria-label={title}>
      {/* (T50) its own window: clear of the bars on every edge (RN's SafeAreaView padded iOS only) */}
      {/* (T723 F7) iOS: the left-edge swipe is the back here, as Android's back is (the Modal is its own window) */}
      <ModalBack onBack={requestClose}>
      <WindowInsets style={{ flex: 1, backgroundColor: palette.card }}>
        <View testID={testID} style={{ flex: 1 }}>
          <MenuPageHead title={title} onBack={onBack} palette={palette} backLabel={backLabel} column={column} />
          {scroll
            // (T243) focusable: a page with nothing to press (the web's Offline maps) still scrolls from the keyboard (axe
            // scrollable-region-focusable)
            // (T723 F2, G10) iOS lays its keyboard over the window: the scroll keeps the focused field above it
            // (`automaticallyAdjustKeyboardInsets`, iOS-only: Android resizes its window and ignores it), and a tap on a
            // button with the keyboard up acts at once instead of only closing the keyboard ("handled")
            ? <ScrollView focusable automaticallyAdjustKeyboardInsets keyboardShouldPersistTaps="handled" contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 40, gap: SPACE.s4,
                                                   ...column }}>
                <Selectable>{children}</Selectable>
              </ScrollView>
            : <View style={{ flex: 1, ...column }}><Selectable>{children}</Selectable></View>}
        </View>
      </WindowInsets>
      </ModalBack>
    </Modal>
  );
}

/** (T859) the scrim is a press (it closes the page): as large as the window it covers */
const SCRIM_HIT = hitProps({ w: 320, h: 200 });
const CLOSE_HIT = hitProps({ w: 40, h: 40 });

/**
 * (T859) THE DESKTOP CARD: the page centred over the scrim, sized for reading. The scrim follows the card in the tree
 * (drawn under it by zIndex) and takes no keyboard focus, so the web Modal's focus lands on the card's first control
 * and a Tab never stops on it: Escape and the ✕ are the keyboard's closes. A page that scrolls itself (the glossary's
 * list, `scroll={false}`) gets the card's full height; any other is as tall as its content, up to that.
 */
function MenuCard({ open, title, onBack, onClose = onBack, requestClose, palette, children, scroll = true, testID, backLabel }:
                  MenuPageProps & { requestClose: () => void }) {
  const { height: windowHeight } = useWindowDimensions();
  // no window height yet (0: not measured) caps nothing
  const cap = windowHeight > 0 ? windowHeight * MENU_CARD.maxHeight : undefined;
  return (
    <Modal visible={open} transparent animationType="fade" onRequestClose={requestClose} aria-label={title}>
      <ModalBack onBack={requestClose}>
      <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: MENU_CARD.inset, zIndex: 0 }}>
        <View testID={testID} accessibilityViewIsModal
              style={{ width: "100%", maxWidth: MENU_CARD.maxWidth, maxHeight: cap, height: scroll ? undefined : cap,
                       backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, zIndex: 1 }}>
          <View testID="menu-card-head" style={{ flexDirection: "row", alignItems: "flex-start", gap: SPACE.s3,
                                            paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3,
                                            borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
            <View style={{ flex: 1, gap: SPACE.s1, justifyContent: "center", minHeight: 40 }}>
              {backLabel
                ? <Pressable {...hitProps({ w: 60, h: 40 })} onPress={onBack} accessibilityRole="button" accessibilityLabel={backLabel}
                             style={{ alignSelf: "flex-start", minHeight: 40, justifyContent: "center" }}>
                    <IconText name="back" text={backLabel} style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }} />
                  </Pressable>
                : null}
              <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
            </View>
            <Pressable {...CLOSE_HIT} testID="menu-card-close" onPress={onClose} accessibilityRole="button"
                       accessibilityLabel={SHEET_WORDS.close}
                       style={{ width: 40, height: 40, alignItems: "center", justifyContent: "center", marginRight: -SPACE.s2 }}>
              <Icon name="close" colour={palette.ink} size={22} />
            </Pressable>
          </View>
          {scroll
            ? <ScrollView focusable keyboardShouldPersistTaps="handled"
                          style={{ flexGrow: 0, flexShrink: 1, flexBasis: "auto" }}
                          contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: SPACE.s4 * 2,
                                                   gap: SPACE.s4 }}>
                <Selectable>{children}</Selectable>
              </ScrollView>
            : <View style={{ flex: 1 }}><Selectable>{children}</Selectable></View>}
        </View>
        <Pressable {...SCRIM_HIT} testID="menu-card-scrim" onPress={onClose} tabIndex={-1} accessibilityRole="button"
                   accessibilityLabel={SHEET_WORDS.close}
                   style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0, zIndex: 0, backgroundColor: palette.scrim }} />
      </View>
      </ModalBack>
    </Modal>
  );
}

/** "‹ Back" over the title, ruled off in ink. On a glossary term the ‹ says where it goes ("‹ Glossary"). */
function MenuPageHead({ title, onBack, palette, backLabel = MENU_WORDS.back, column }: {
  title: string; onBack: () => void; palette: Palette; backLabel?: string;
  column: { width: "100%"; maxWidth: number; alignSelf: "center" } | null;
}) {
  return (
    <View style={{ borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, gap: SPACE.s1, ...column }}>
        <Pressable {...hitProps({ w: 60, h: 40 })} onPress={onBack} accessibilityRole="button" accessibilityLabel={backLabel}
                   style={{ alignSelf: "flex-start", minHeight: 40, justifyContent: "center" }}>
          <IconText name="back" text={backLabel} style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }} />
        </Pressable>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
      </View>
    </View>
  );
}
