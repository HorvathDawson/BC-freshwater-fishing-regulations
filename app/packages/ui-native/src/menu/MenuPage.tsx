/**
 * One menu page, full screen over the app: "‹ Back" and the page's title, then the page. The water screen's header
 * shape (FaceBar's back link, the screen title face) so a menu page reads as part of the same app.
 *
 * A back layer while open (G1): the ‹, Android's back (the Modal's onRequestClose) and the browser's back all close it.
 *
 * (T859, owner 2026-11-18) ON THE WEB AT DESKTOP WIDTH (`layoutClassFor` "desktop", ≥ 1280 px) the page is a CARD over
 * the page instead: centred, `MENU_CARD` wide, at most `MENU_CARD.maxHeight` of the window tall with its own scroll,
 * over the palette's scrim. A press on the scrim and the card's ✕ close the page (`onClose`, every level at once);
 * Escape (the web Modal's own, its onRequestClose) and the browser's back are the one back, so they close ONE level (a
 * glossary term, then the page). Focus: the web Modal moves it into the card (its first control, the ✕) and back to what
 * opened it (the More row) when it closes. Every phone and tablet — a native window of any width included — and the
 * web below 1280 px draw the full-window page exactly as before.
 *
 * ONE TREE across the breakpoint: the card and the full page are the same elements with other styles (the scrim is a
 * last sibling, added or dropped), so a window resized across 1280 px keeps the page mounted — a report's typed draft,
 * the scroll — rather than swapping one component for another.
 */
import { Modal, Platform, ScrollView, View, useWindowDimensions } from "react-native";
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

/** (T859) the card is the desktop WEB's: a native window keeps the full page at any width */
const CARD_PLATFORM = Platform.select({ web: true, default: false });

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

/** (T859) the scrim is a press (it closes the page): as large as the window it covers */
const SCRIM_HIT = hitProps({ w: 320, h: 200 });

export function MenuPage({ open, title, onBack, onClose = onBack, palette, children, scroll = true, testID, backLabel }: MenuPageProps) {
  const requestClose = useBackLayer(open, onBack);
  const layout = useLayoutClass();
  const card = CARD_PLATFORM && layout === "desktop";
  const { height: windowHeight } = useWindowDimensions();
  // no window height yet (0: not measured) caps nothing
  const cap = windowHeight > 0 ? windowHeight * MENU_CARD.maxHeight : undefined;
  // (T167) on a tablet-wide full page the page reads in a centred column (`MORE_WIDE.page`), its head's words on the
  // same edge; the card is that column already
  const column = !card && layout !== "phone"
    ? { width: "100%" as const, maxWidth: MORE_WIDE.page, alignSelf: "center" as const } : null;
  return (
    <Modal visible={open} transparent={card} animationType={card ? "fade" : "slide"} onRequestClose={requestClose}
           aria-label={title}>
      {/* (T50) its own window: clear of the bars on every edge (RN's SafeAreaView padded iOS only) */}
      {/* (T723 F7) iOS: the left-edge swipe is the back here, as Android's back is (the Modal is its own window) */}
      <ModalBack onBack={requestClose}>
      <WindowInsets style={card
        ? { flex: 1, alignItems: "center", justifyContent: "center", padding: MENU_CARD.inset, zIndex: 0 }
        : { flex: 1, backgroundColor: palette.card }}>
        {/* the card: the scrim follows it in the tree (drawn under it by zIndex) and takes no tab stop, so the web
            Modal's focus lands on the card's first control and a Tab never stops on the scrim */}
        <View testID={testID} accessibilityViewIsModal={card || undefined}
              style={card
                ? { width: "100%", maxWidth: MENU_CARD.maxWidth, maxHeight: cap, height: scroll ? undefined : cap,
                    backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, zIndex: 1 }
                : { flex: 1 }}>
          <MenuPageHead title={title} onBack={onBack} onClose={card ? onClose : null} palette={palette}
                        backLabel={backLabel} column={column} />
          {scroll
            // (T243) focusable: a page with nothing to press (the web's Offline maps) still scrolls from the keyboard (axe
            // scrollable-region-focusable)
            // (T723 F2, G10) iOS lays its keyboard over the window: the scroll keeps the focused field above it
            // (`automaticallyAdjustKeyboardInsets`, iOS-only: Android resizes its window and ignores it), and a tap on a
            // button with the keyboard up acts at once instead of only closing the keyboard ("handled")
            // (T859) in the card it is as tall as its content, up to the card's cap
            ? <ScrollView focusable automaticallyAdjustKeyboardInsets keyboardShouldPersistTaps="handled"
                          style={card ? { flexGrow: 0, flexShrink: 1, flexBasis: "auto" } : undefined}
                          contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4,
                                                   paddingBottom: card ? SPACE.s4 * 2 : 40, gap: SPACE.s4, ...column }}>
                <Selectable>{children}</Selectable>
              </ScrollView>
            : <View style={{ flex: 1, ...column }}><Selectable>{children}</Selectable></View>}
        </View>
        {card
          ? <Pressable {...SCRIM_HIT} testID="menu-card-scrim" onPress={onClose} tabIndex={-1} accessibilityRole="button"
                       accessibilityLabel={SHEET_WORDS.close}
                       style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0, zIndex: 0,
                                backgroundColor: palette.scrim }} />
          : null}
      </WindowInsets>
      </ModalBack>
    </Modal>
  );
}

/**
 * "‹ Back" over the title, ruled off in ink. On a glossary term the ‹ says where it goes ("‹ Glossary"). (T859) In the
 * desktop card (`onClose` given) the title shares its row with the card's ✕, and the ‹ is drawn only where it goes
 * somewhere other than out (a term's "‹ Glossary"): the ✕ is the way out.
 */
function MenuPageHead({ title, onBack, onClose, palette, backLabel, column }: {
  title: string; onBack: () => void; onClose: (() => void) | null; palette: Palette; backLabel?: string;
  column: { width: "100%"; maxWidth: number; alignSelf: "center" } | null;
}) {
  const back = onClose === null || backLabel !== undefined;
  const label = backLabel ?? MENU_WORDS.back;
  return (
    <View style={{ borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, flexDirection: "row",
                     alignItems: "flex-start", gap: SPACE.s3, ...column }}>
        <View style={{ flex: 1, gap: SPACE.s1, justifyContent: "center", minHeight: 40 }}>
          {back
            ? <Pressable {...hitProps({ w: 60, h: 40 })} onPress={onBack} accessibilityRole="button" accessibilityLabel={label}
                         style={{ alignSelf: "flex-start", minHeight: 40, justifyContent: "center" }}>
                <IconText name="back" text={label} style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }} />
              </Pressable>
            : null}
          <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
        </View>
        {onClose
          ? <Pressable {...hitProps({ w: 40, h: 40 })} testID="menu-card-close" onPress={onClose} accessibilityRole="button"
                       accessibilityLabel={SHEET_WORDS.close}
                       style={{ width: 40, height: 40, alignItems: "center", justifyContent: "center", marginRight: -SPACE.s2 }}>
              <Icon name="close" colour={palette.ink} size={22} />
            </Pressable>
          : null}
      </View>
    </View>
  );
}
