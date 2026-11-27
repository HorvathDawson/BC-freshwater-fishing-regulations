/**
 * (T1194) A GAUGE'S OWN SCREEN, opened by a tap on a gauge dot of the Conditions map (or `?gauge=<station>`): the name, the
 * shared reading and chart (`GaugeReading`, the same piece the site's gauge page mounts), the waters this gauge serves
 * (each opens that water's Conditions view; Back returns here), and the sources at the foot, as every page has them.
 *
 * The app-only parts are here: Back, the waters it serves (the release, so it opens offline), the plain line for a gauge that
 * serves no water, the tide's note on Level. It decides nothing: the words are core's `GAUGE_PAGE_WORDS`, the facts `useGaugePage`.
 */
import { useContext, useState } from "react";
import { ScrollView, View } from "react-native";
import { GAUGE_PAGE_WORDS, GAUGE_SERVES_SHOWN, conditionsSourcesOf, gaugeNotes, gaugeUrl } from "@app/core";
import type { StationId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { useGaugePage, useGaugeShare } from "@app/ui";
import { Button } from "./Button";
import { ErrorState } from "./ErrorState";
import { linkTo } from "./openLink";
import { SharePortContext } from "./SharePortContext";
import { GaugeReading, type GaugeReadingSource } from "./GaugeReading";
import { LoadingState } from "./LoadingState";
import { Pressable } from "./Pressable";
import { SourceRow } from "./SourceRow";
import { Text } from "./Text";
import { Icon } from "./icons";
import { hitProps } from "./regs/hit";
import { LABEL_PROPS, LABEL_SCALE, TYPE, labelMinWidth } from "./type";
import { useScaleUpTo } from "./textScale";
import { GUTTER, SPACE, type Palette } from "./theme";

export function GaugeScreen({ station, source, loader, palette, theme, feed, onBack, onOpenWater }: {
  station: StationId; source: GaugeReadingSource; loader: ReleaseLoader | null; palette: Palette; theme: string;
  feed?: React.ComponentProps<typeof GaugeReading>["feed"];
  onBack: () => void;
  /** a served water's Conditions view (the Shell's one opener) */
  onOpenWater: (item: string) => void;
}) {
  const page = useGaugePage(loader, station);
  const [all, setAll] = useState(false);
  const labelScale = useScaleUpTo(LABEL_SCALE);
  const ports = useContext(SharePortContext);
  // the website's page for this gauge (only where the site has one: the same rule as Share) — its Open and Share
  const share = useGaugeShare(ports.port, { station, name: page.state === "ready" && page.value ? page.value.name : null }, ports.hasGaugePage);
  const facts = page.state === "ready" ? page.value : null;
  return (
    <View testID="gauge-screen" style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, minHeight: 56, justifyContent: "center" }}>
        <Pressable testID="gauge-back" onPress={onBack} accessibilityRole="button" accessibilityLabel={GAUGE_PAGE_WORDS.back}
                   {...hitProps({ w: 44, h: 44 })}
                   // (T1201 rule, review 2026-11-27) a label in a fixed control follows the text size to `LABEL_SCALE`, one line, whole: the
                   // control GROWS to hold it (`labelMinWidth`), never cuts it
                   style={{ minWidth: Math.max(44, 20 + SPACE.s1 + labelMinWidth(GAUGE_PAGE_WORDS.back, TYPE.bodyStrong, labelScale, 0, 0.6)),
                            minHeight: 44, flexDirection: "row", alignItems: "center", alignSelf: "flex-start", gap: SPACE.s1 }}>
          <Icon name="back" colour={palette.accent} size={20} />
          <Text {...LABEL_PROPS} style={{ ...TYPE.bodyStrong, color: palette.accent }}>{GAUGE_PAGE_WORDS.back}</Text>
        </Pressable>
      </View>
      <ScrollView contentContainerStyle={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s4 * 3, gap: SPACE.s4 }}>
        {page.state === "loading" && <LoadingState palette={palette} size={72} label="Loading this gauge" style={{ alignItems: "center", paddingVertical: SPACE.s4 }} />}
        {page.state === "failed" && <ErrorState palette={palette} size="body" testID="gauge-release-failed" message={GAUGE_PAGE_WORDS.releaseFailed} />}
        {page.state === "ready" && facts === null && (
          <ErrorState palette={palette} size="body" testID="gauge-unknown" message={GAUGE_PAGE_WORDS.unknown(station)} />
        )}
        {facts && (
          <>
            <View style={{ gap: SPACE.s1 }}>
              <Text testID="gauge-title" accessibilityRole="header" style={{ ...TYPE.title, color: palette.ink }}>{facts.name}</Text>
              <Text testID="gauge-station" style={{ ...TYPE.meta, color: palette.sub }}>{GAUGE_PAGE_WORDS.stationLine(station)}</Text>
            </View>
            <GaugeReading source={source} station={station} palette={palette} theme={theme} feed={feed}
                          notes={(q) => gaugeNotes(facts, q)} />
            <View testID="gauge-serves" style={{ gap: SPACE.s2 }}>
              <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{GAUGE_PAGE_WORDS.servesHeading}</Text>
              {facts.serves.length === 0
                ? <Text testID="gauge-not-matched" style={{ ...TYPE.body, color: palette.sub }}>{GAUGE_PAGE_WORDS.notMatched}</Text>
                : (all ? facts.serves : facts.serves.slice(0, GAUGE_SERVES_SHOWN)).map((w) => (
                  <Pressable key={w.item} testID="gauge-serves-row" onPress={() => onOpenWater(w.item)} accessibilityRole="button"
                             accessibilityLabel={GAUGE_PAGE_WORDS.open(w.name)} {...hitProps({ w: 120, h: 44 })}
                             style={{ minHeight: 44, paddingVertical: SPACE.s2, flexDirection: "row", alignItems: "center", gap: SPACE.s2, borderTopWidth: 1, borderTopColor: palette.line }}>
                    {/* reading text: it wraps and follows the text size in full; nothing is cut */}
                    <Text style={{ ...TYPE.body, color: palette.ink, flex: 1 }}>{w.name}</Text>
                    <Text style={{ ...TYPE.meta, color: palette.sub, maxWidth: "40%", textAlign: "right" }}>{GAUGE_PAGE_WORDS.trust[w.trust]}</Text>
                    <Icon name="next" colour={palette.accent} size={16} />
                  </Pressable>
                ))}
              {facts.serves.length > GAUGE_SERVES_SHOWN && (
                <Pressable testID="gauge-serves-fold" onPress={() => setAll(!all)} accessibilityRole="button" aria-expanded={all}
                           {...hitProps({ w: 120, h: 44 })} style={{ minHeight: 44, justifyContent: "center", alignSelf: "flex-start" }}>
                  <Text style={{ ...TYPE.metaBold, color: palette.accent }}>
                    {all ? GAUGE_PAGE_WORDS.fewer : GAUGE_PAGE_WORDS.more(facts.serves.length - GAUGE_SERVES_SHOWN)}</Text>
                </Pressable>
              )}
            </View>
            {/* (T1194, owner 2026-11-26: "Variant B") ONE CLOSING CARD, as the website's: the sources, then (only where the site has a page for this gauge) one full-width primary and a full-width outline Share */}
            <View testID="gauge-foot" style={{ marginTop: SPACE.s4, padding: SPACE.s4, gap: SPACE.s3, alignItems: "center", borderWidth: 1,
                                               borderColor: palette.line, borderRadius: palette.r.box, backgroundColor: palette.wash }}>
              <SourceRow vm={conditionsSourcesOf()} palette={palette} testID="gauge-sources" center />
              {share && (
                <View style={{ alignSelf: "stretch", gap: SPACE.s3 }}>
                  <Button palette={palette} label={GAUGE_PAGE_WORDS.openOnSite} testID="gauge-open-site" style={{ alignSelf: "stretch" }}
                          onPress={linkTo(gaugeUrl(station, facts.name))} />
                  <Button palette={palette} kind="ghost" label={share.label} onPress={share.press} busy={share.busy} testID="gauge-share" style={{ alignSelf: "stretch" }} />
                  {share.said && <Text testID="gauge-share-said" accessibilityLiveRegion="polite" style={{ ...TYPE.meta, color: palette.sub, textAlign: "center" }}>{share.said}</Text>}
                </View>
              )}
            </View>
          </>
        )}
      </ScrollView>
    </View>
  );
}
