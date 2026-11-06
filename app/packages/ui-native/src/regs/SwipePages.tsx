/**
 * (T344, owner 2026-10-31: "if you clicked one that has multiple we should be able to swipe between them, like
 * whitefish (2 types)") A GROUP'S FISH, ONE PAGE EACH: a sideways pager (one page wide, snapping), the page's name and
 * "1 of 2" over it, a dot per page. A screen reader adjusts it (next / previous) instead of swiping. Core's `swipe`
 * block decides the pages; this draws them.
 */
import { useState, type ReactNode } from "react";
import { ScrollView, View, type LayoutChangeEvent, type NativeScrollEvent, type NativeSyntheticEvent, type ScrollViewInstance } from "react-native";
import { WORDS } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import { round, type Palette } from "../theme";

export function SwipePages({ titles, palette, children }: {
  titles: readonly string[]; palette: Palette; children: (i: number) => ReactNode }) {
  const [w, setW] = useState(0);
  const [at, setAt] = useState(0);
  const [ref, setRef] = useState<ScrollViewInstance | null>(null);
  const go = (i: number) => {
    const j = Math.max(0, Math.min(titles.length - 1, i));
    setAt(j);
    ref?.scrollTo({ x: j * w, animated: true });
  };
  const onEnd = (e: NativeSyntheticEvent<NativeScrollEvent>) => {
    if (w > 0) setAt(Math.round(e.nativeEvent.contentOffset.x / w));
  };
  return (
    <View testID="swipe-pages" onLayout={(e: LayoutChangeEvent) => setW(e.nativeEvent.layout.width)}
          accessible accessibilityRole="adjustable" accessibilityLabel={titles[at]} accessibilityHint={WORDS.PAGE_HINT}
          accessibilityValue={{ text: WORDS.PAGE_OF(at + 1, titles.length) }}
          accessibilityActions={[{ name: "increment" }, { name: "decrement" }]}
          onAccessibilityAction={(e) => go(at + (e.nativeEvent.actionName === "increment" ? 1 : -1))}>
      <View style={{ flexDirection: "row", alignItems: "baseline", justifyContent: "space-between", marginTop: 8 }}>
        <Text testID="swipe-title" style={[TYPE.lead, { color: palette.ink, flex: 1 }]}>{titles[at]}</Text>
        <Text testID="swipe-of" style={[TYPE.meta, { color: palette.sub }]}>{WORDS.PAGE_OF(at + 1, titles.length)}</Text>
      </View>
      <ScrollView ref={setRef} horizontal pagingEnabled showsHorizontalScrollIndicator={false} onMomentumScrollEnd={onEnd}
                  onScrollEndDrag={onEnd} scrollEventThrottle={16}>
        {titles.map((t, i) => <View key={t} testID={`swipe-page-${i}`} style={{ width: w || undefined }}>{children(i)}</View>)}
      </ScrollView>
      <View style={{ flexDirection: "row", justifyContent: "center", gap: 6, marginTop: 6 }}>
        {titles.map((t, i) => (
          <View key={t} style={{ width: 7, height: 7, borderRadius: round(7), backgroundColor: i === at ? palette.ink : palette.line2 }} />
        ))}
      </View>
    </View>
  );
}
