/**
 * (T810 O) THE TROUT AND CHAR TABLE: N's `TroutVM` (core `troutOf`) laid out, as the
 * build spec's section 3 and the mockup's round 15 draw it (`docs/design/trout-table-contract.md` section 3,
 * contract 2.12 "Draw"). Every string is the payload's, drawn unchanged; the app joins none and decides nothing: which
 * rows, boxes, dates, labels and bars exist is Python's. The app's own words are navigation only (`TROUT_NAV`, the shape
 * marks ↩ / ✕ that `ShutGroup` adds).
 *
 * In order: the kicker line (the entry's name only when no folded row heads the table, and "How to tell a hatchery fish ›"
 * when `hatchLink` is set), the top line (bigger), its sub (trout 4: the exception, the top line's face a size down, as a
 * subtitle), its second half and its date (smaller), the card line (the card-level date, grey), the keep
 * boxes (T829's `KeepBox`: one component for the salmon and trout tables), the release and closed groups under their head
 * word (`ShutGroup`), the all-together line, the year bar when `year` is set, then "Sources ›" and, only when `elsewhere`
 * holds lines, "Fished elsewhere today? ›", which unfolds them in place.
 *
 * Presses (T810 P, T1031): a row opens the row's species sheet on Sources (`tf:<row key>`: its answer, "Here, today", how
 * it was decided, where each rule comes from), its picture on Identification (`tf:<row key>|id`); the hatchery link opens the existing "Hatchery or wild?" sheet (`hatch`); "Sources ›" opens the table's
 * every rule (`src:trout`, `TroutTable.src` in Python's order).
 */
import { useState } from "react";
import { PixelRatio, View, useWindowDimensions } from "react-native";
import { OPEN, TROUT_NAV, WORDS, type TabSourcesVM, type TroutBoxVM, type TroutRowVM, type TroutVM } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { KeepBox, ShutGroup, type KeepRowVM } from "./KeepBox";
import type { KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { TroutYearBar } from "./TroutYearBar";
import { SourceRow } from "../SourceRow";
import type { FishImages } from "./types";

const PRESS = { w: 44, h: 44 } as const;
/** the OS text scale from which the top line steps down a size, so a long lead wraps in words, not letters (spec 3.5) */
const BIG_AT = 1.5;

/** A table row as the keep box draws it: the payload's texts, its own date line, core's one label, the fish's picture. */
function rowOf(r: TroutRowVM, actions: KeepActions, images: FishImages | undefined): KeepRowVM {
  return {
    key: r.key, name: r.name, label: r.label, tag: r.tag, sub: r.sub, lines: r.lines, quiet: r.quiet, date: r.date, origin: r.origin, a11y: r.a11y,
    pic: { source: images?.[r.fish]?.[0]?.thumb, more: TROUT_NAV.MORE, label: WORDS.LOOKS_LIKE(r.name), testID: `pic:${r.key}`,
           onPress: () => actions.onSheet(OPEN.troutLooks(r.key)) },
    // (T1031) the row opens the fish's species sheet on Sources, its picture on Identification
    open: () => actions.onSheet(OPEN.troutRow(r.key)),
  };
}

function Box({ b, i, palette, actions, images }: { b: TroutBoxVM; i: number; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const rows = b.rows.map((r) => rowOf(r, actions, images));
  if (b.kind === "keep")
    return <KeepBox testID={`trout-box:${i}`} head={b.head} label={b.label} strip={b.strip} possession={b.possession} tag={b.tag} date={b.date} rows={rows}
                    bands={b.bands} inner={b.inner} palette={palette} />;
  return <ShutGroup testID={`trout-${b.kind}`} kind={b.kind} head={b.head} date={b.date} rows={rows} palette={palette} />;
}

export function TroutCard({ t, palette, actions, images, kicker = false, todayAt = null, months = [], sources }: {
  t: TroutVM; palette: Palette; actions: KeepActions; images?: FishImages;
  /** the entry's name over the table: only where no folded row heads it (the at-a-glance table off) */
  kicker?: boolean;
  /** the page strip's day marker and month initials, for the year bar */
  todayAt?: number | null; months?: readonly string[];
  /** (T1031 item 3) the table's Source line (core `troutSourcesOf(page)`, computed by the host with the foot row) */
  sources: TabSourcesVM;
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
          {kicker ? <Text testID="kicker:trout" accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub }]}>{t.fold.name}</Text> : <View />}
          {t.hatchLink && link(t.hatchLink, "trout-hatch", () => actions.onSheet(OPEN.hatch))}
        </View>
      )}
      <Text selectable testID="trout-top" style={[big ? TYPE.lead : TYPE.screen, { color: palette.ink, marginTop: 4 }]}>{t.top.lead}</Text>
      {t.top.sub && <Text selectable testID="trout-sub" style={[TYPE.lead, { color: palette.ink, marginTop: 2 }]}>{t.top.sub}</Text>}
      {t.top.second && <Text selectable testID="trout-second" style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 2 }]}>{t.top.second}</Text>}
      {t.top.date && <Text selectable testID="trout-top-date" style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{t.top.date}</Text>}
      {t.card && <Text selectable testID="trout-card-line" style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{t.card}</Text>}
      <View testID="trout-boxes" style={{ marginTop: 4 }}>
        {t.boxes.map((b, i) => <Box key={i} b={b} i={i} palette={palette} actions={actions} images={images} />)}
      </View>
      {t.together && <Text selectable testID="trout-together" style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 10 }]}>{t.together.text}</Text>}
      {t.year && <TroutYearBar year={t.year} palette={palette} todayAt={todayAt} months={months} />}
      {t.elsewhereLink && (
        <View testID="trout-links" style={{ marginTop: 8 }}>{link(t.elsewhereLink, "trout-elsewhere", () => setAway(!away), away)}</View>
      )}
      {t.elsewhereLink && away && (
        <View testID="trout-elsewhere-lines" style={{ rowGap: 4, paddingBottom: 4 }}>
          {t.elsewhere.map((l) => <Text selectable key={l} testID="trout-elsewhere-line" style={[TYPE.meta, { color: palette.ink }]}>{l}</Text>)}
        </View>
      )}
      {/* (T1031 item 3) the table's last line: the one Source line, its rules' pages then "Who says so ›" (the every-rule
          sheet `src:trout`, what "Sources ›" opened) */}
      <SourceRow vm={sources} palette={palette} onBook={actions.onBook} onSheet={actions.onSheet} testID="trout-source" sheetTestID="trout-sources" />
    </View>
  );
}
