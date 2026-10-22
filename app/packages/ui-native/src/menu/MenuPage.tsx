/**
 * One menu page, full screen over the app: "‹ Back" and the page's title, then the page. The water screen's header
 * shape (FaceBar's back link, the screen title face) so a menu page reads as part of the same app.
 *
 * A back layer while open (G1): the ‹, Android's back (the Modal's onRequestClose) and the browser's back all close it.
 */
import { Modal, Pressable, SafeAreaView, ScrollView, Text, View } from "react-native";
import { MENU_WORDS } from "@app/core";
import { useBackLayer } from "@app/ui";
import { TYPE } from "../type";
import { GUTTER, SPACE, type Palette } from "../theme";

export function MenuPage({ open, title, onBack, palette, children, scroll = true, testID, backLabel }: {
  open: boolean; title: string; onBack: () => void; palette: Palette; children: React.ReactNode;
  /** where the ‹ goes, when not simply back (a glossary term: "Glossary") */
  backLabel?: string;
  /** false: the page scrolls itself (a long list) */
  scroll?: boolean;
  testID?: string;
}) {
  const requestClose = useBackLayer(open, onBack);
  return (
    <Modal visible={open} animationType="slide" onRequestClose={requestClose}>
      <SafeAreaView style={{ flex: 1, backgroundColor: palette.card }}>
        <View testID={testID} style={{ flex: 1 }}>
          <MenuPageHead title={title} onBack={onBack} palette={palette} backLabel={backLabel} />
          {scroll
            ? <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 40, gap: SPACE.s4 }}>
                {children}
              </ScrollView>
            : children}
        </View>
      </SafeAreaView>
    </Modal>
  );
}

/** "‹ Back" over the title, ruled off in ink. On a glossary term the ‹ says where it goes ("‹ Glossary"). */
function MenuPageHead({ title, onBack, palette, backLabel = MENU_WORDS.back }: {
  title: string; onBack: () => void; palette: Palette; backLabel?: string;
}) {
  return (
    <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, gap: SPACE.s1,
                   borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel={backLabel} hitSlop={8}
                 style={{ alignSelf: "flex-start", minHeight: 32, justifyContent: "center" }}>
        <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }}>{`‹  ${backLabel}`}</Text>
      </Pressable>
      <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{title}</Text>
    </View>
  );
}
