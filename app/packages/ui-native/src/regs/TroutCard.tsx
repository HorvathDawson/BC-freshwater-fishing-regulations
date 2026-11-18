/**
 * (T810 O) THE TROUT AND CHAR TABLE: N's `TroutVM` (core `troutOf`) laid out, as the
 * build spec's section 3 and the mockup's round 15 draw it (`handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`,
 * contract 2.12 "Draw"). Every string is the payload's, drawn unchanged; the app joins none and decides nothing: which
 * rows, boxes, dates, labels and bars exist is Python's. The app's own words are navigation only (`TROUT_NAV`, the shape
 * marks ↩ / ✕ that `ShutGroup` adds).
 *
 * In order: the kicker line (the entry's name only when no folded row heads the table, and "How to tell a hatchery fish ›"
 * when `hatchLink` is set), the top line (bigger) with its second half and its date (smaller), the card line, the keep
 * boxes (T829's `KeepBox`: one component for the salmon and trout tables), the release and closed groups under their head
 * word (`ShutGroup`), the all-together line, the year bar when `year` is set, then "Sources ›" and, only when `elsewhere`
 * holds lines, "Fished elsewhere today? ›", which unfolds them in place.
 *
 * Presses (T810 P): a row's picture opens the row's fish sheet (`tf:<row key>`: its answer, "Here, today", how to tell it,
 * its sources); the hatchery link opens the existing "Hatchery or wild?" sheet (`hatch`); "Sources ›" opens the table's
 * every rule (`src:trout`, `TroutTable.src` in Python's order).
 */
import { useState } from "react";
import { PixelRatio, View, useWindowDimensions } from "react-native";
import { TROUT_NAV, TROUT_SOURCES_KEY, WORDS, type TroutBoxVM, type TroutRowVM, type TroutVM } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { KeepBox, ShutGroup, type KeepRowVM } from "./KeepBox";
import type { KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { TroutYearBar } from "./TroutYearBar";
import type { FishImages } from "./types";

const PRESS = { w: 44, h: 44 } as const;
/** the OS text scale from which the top line steps down a size, so a long lead wraps in words, not letters (spec 3.5) */
const BIG_AT = 1.5;

/** A table row as the keep box draws it: the payload's texts, its own date line, core's one label, the fish's picture. */
function rowOf(r: TroutRowVM, actions: KeepActions, images: FishImages | undefined): KeepRowVM {
  return {
    key: r.key, name: r.name, label: r.label, sub: r.sub, lines: r.lines, quiet: r.quiet, date: r.date, origin: r.origin, a11y: r.a11y,
    pic: { source: images?.[r.fish]?.[0]?.thumb, more: TROUT_NAV.MORE, label: WORDS.LOOKS_LIKE(r.name), testID: `pic:${r.key}`,
           onPress: () => actions.onSheet(`tf:${r.key}`) },
  };
}

function Box({ b, i, palette, actions, images }: { b: TroutBoxVM; i: number; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const rows = b.rows.map((r) => rowOf(r, actions, images));
  if (b.kind === "keep")
    return <KeepBox testID={`trout-box:${i}`} head={b.head} label={b.label} strip={b.strip} possession={b.possession} tag={b.tag} date={b.date} rows={rows}
                    bands={b.bands} palette={palette} />;
  return <ShutGroup testID={`trout-${b.kind}`} kind={b.kind} head={b.head} date={b.date} rows={rows} palette={palette} />;
}

export function TroutCard({ t, palette, actions, images, kicker = false, todayAt = null, months = [] }: {
  t: TroutVM; palette: Palette; actions: KeepActions; images?: FishImages;
  /** the entry's name over the table: only where no folded row heads it (the at-a-glance table off) */
  kicker?: boolean;
  /** the page strip's day marker and month initials, for the year bar */
  todayAt?: number | null; months?: readonly string[];
}) {
  useWindowDimensions();
  const big = (PixelRatio.getFontScale() || 1) >= BIG_AT;
  const [away, setAway] = useState(false);
  /** a navigation link: accent words, a 44 px press; `expanded` set: it unfolds lines in place (aria-expanded) */
  const link =(text: string, testID: string, onPress: () => void, expanded?: boolean) => (
    <RegsPress testID={testID} accessibilityRole="button" {...(expanded !== undefined ? { "aria-expanded": expanded } : {})} onPress={onPress} box={PRESS}
               style={{ minHeight: 44, justifyContent: "center", alignSelf: "flex-start" }}>
      <Text style={[TYPE.metaBold, { color: palette.accent }]}>{text}</Text>
    </RegsPress>
  );
  return (
    <View testID="keep-trout" style={{ paddingHorizontal: GUTTER, paddingTop: 4, paddingBottom: 8 }}>
      {(kicker || t.hatchLink) && (
        <View testID="trout-kline" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", columnGap: 12 }}>
          {kicker ? <Text testID="trout-kicker" accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub }]}>{t.fold.name}</Text> : <View />}
          {t.hatchLink && link(t.hatchLink, "trout-hatch", () => actions.onSheet("hatch"))}
        </View>
      )}
      <Text selectable testID="trout-top" style={[big ? TYPE.lead : TYPE.screen, { color: palette.ink, marginTop: 4 }]}>{t.top.lead}</Text>
      {t.top.second && <Text selectable testID="trout-second" style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 2 }]}>{t.top.second}</Text>}
      {t.top.date && <Text selectable testID="trout-top-date" style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{t.top.date}</Text>}
      {t.card && <Text selectable testID="trout-card-line" style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{t.card}</Text>}
      <View testID="trout-boxes" style={{ marginTop: 4 }}>
        {t.boxes.map((b, i) => <Box key={i} b={b} i={i} palette={palette} actions={actions} images={images} />)}
      </View>
      {t.together && <Text selectable testID="trout-together" style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 10 }]}>{t.together.text}</Text>}
      {t.year && <TroutYearBar year={t.year} palette={palette} todayAt={todayAt} months={months} />}
      <View testID="trout-links" style={{ marginTop: 8, flexDirection: "row", flexWrap: "wrap", justifyContent: "space-between", columnGap: 16 }}>
        {link(t.sourcesLink, "trout-sources", () => actions.onSheet(TROUT_SOURCES_KEY))}
        {t.elsewhereLink && link(t.elsewhereLink, "trout-elsewhere", () => setAway(!away), away)}
      </View>
      {t.elsewhereLink && away && (
        <View testID="trout-elsewhere-lines" style={{ rowGap: 4, paddingBottom: 4 }}>
          {t.elsewhere.map((l) => <Text selectable key={l} testID="trout-elsewhere-line" style={[TYPE.meta, { color: palette.ink }]}>{l}</Text>)}
        </View>
      )}
    </View>
  );
}
