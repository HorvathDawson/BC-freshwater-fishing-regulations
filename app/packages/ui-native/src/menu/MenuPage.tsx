/**
 * One menu page, full screen over the app: "‹ Back" and the page's title, then the page. The water screen's header
 * shape (FaceBar's back link, the screen title face) so a menu page reads as part of the same app.
 *
 * A back layer while open (G1): the ‹, Android's back (the Modal's onRequestClose) and the browser's back all close it.
 */
import { Modal, Pressable, ScrollView, Text, View } from "react-native";
import { WindowInsets } from "../WindowInset";
import { MENU_WORDS } from "@app/core";
import { useBackLayer, useLayoutClass } from "@app/ui";
import { IconText } from "../icons";
import { TYPE } from "../type";
import { GUTTER, MORE_WIDE, SPACE, type Palette } from "../theme";

export function MenuPage({ open, title, onBack, palette, children, scroll = true, testID, backLabel }: {
  open: boolean; title: string; onBack: () => void; palette: Palette; children: React.ReactNode;
  /** where the ‹ goes, when not simply back (a glossary term: "Glossary") */
  backLabel?: string;
  /** false: the page scrolls itself (a long list) */
  scroll?: boolean;
  testID?: string;
}) {
  const requestClose = useBackLayer(open, onBack);
  // (T167) on a wide screen the page reads in a centred column (`MORE_WIDE.page`), its head's words on the same edge
  const column = useLayoutClass() !== "phone"
    ? { width: "100%" as const, maxWidth: MORE_WIDE.page, alignSelf: "center" as const } : null;
  return (
    <Modal visible={open} animationType="slide" onRequestClose={requestClose}>
      {/* (T50) its own window: clear of the bars on every edge (RN's SafeAreaView padded iOS only) */}
      <WindowInsets style={{ flex: 1, backgroundColor: palette.card }}>
        <View testID={testID} style={{ flex: 1 }}>
          <MenuPageHead title={title} onBack={onBack} palette={palette} backLabel={backLabel} column={column} />
          {scroll
            ? <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 40, gap: SPACE.s4,
                                                   ...column }}>
                {children}
              </ScrollView>
            : <View style={{ flex: 1, ...column }}>{children}</View>}
        </View>
      </WindowInsets>
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
        <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel={backLabel} hitSlop={8}
                   style={{ alignSelf: "flex-start", minHeight: 32, justifyContent: "center" }}>
          <IconText name="back" text={backLabel} style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }} />
        </Pressable>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
      </View>
    </View>
  );
}
