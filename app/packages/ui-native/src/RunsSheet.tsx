/**
 * (T199, T261) A TAPPED SECTION ON THE RUNS MAP: which runs are here, how (spawning / passing / surveyed), their dates in
 * their source's terms, a YEAR CHART per run (the pentads it is in the water, in the map's colours, the day shown
 * marked), whether each is in the water on the slider's days, its SIZE where DFO counted it (a bar per year on the
 * legend's log scale, the latest count and the trend — "No counts" where there are none, never a guess), and whose data
 * it is. The words are `@app/core` `runSheet`'s, whole; this draws them. A bottom sheet at every width, like the tidal
 * and obstacle sheets. "Open this water" continues to the regulations for the same section.
 */
import type { RunCountsVM, RunRowVM, RunSheetVM } from "@app/core";
import { MONTH_PENTADS, RUN_WORDS } from "@app/core";
import { runStateColour } from "@app/map";
import { useEffect, useState } from "react";
import { Image, Pressable, ScrollView, Text, View } from "react-native";
import { Sheet } from "./Sheet";
import type { FishImages } from "./regs/types";
import { TYPE } from "./type";
import type { Palette } from "./theme";

const MONTH_LETTER = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"];

export function RunsSheet({ vm, palette, theme = "light", fishImages, onClose, onOpenWater }: {
  vm: RunSheetVM | null; palette: Palette; theme?: string; fishImages?: FishImages; onClose: () => void;
  onOpenWater?: () => void;
}) {
  // the other species' runs fold away again for each new tap
  const [all, setAll] = useState(false);
  const key = vm?.rows.map((r) => r.key).join("|") ?? "";
  useEffect(() => { setAll(false); }, [key]);
  return (
    <Sheet open={vm !== null} onClose={onClose} title={vm?.title ?? ""} sub={null} palette={palette}>
      {vm && (
        <ScrollView testID="runs-sheet" style={{ maxHeight: 520 }}
                    contentContainerStyle={{ paddingHorizontal: 20, paddingTop: 12, paddingBottom: 16, gap: 14 }}>
          {vm.noneOfSpecies && <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.noneOfSpecies}</Text>}
          {/* the chosen species' runs; the other species' fold behind one line (a big river carries dozens) */}
          {vm.rows.slice(0, all ? vm.rows.length : vm.mine).map((r) => (
            <RunRow key={r.key} r={r} pentad={vm.pentad} palette={palette} theme={theme}
                    art={r.code ? fishImages?.[r.code]?.[0] : undefined} />
          ))}
          {vm.moreWords && (
            <Pressable testID="runs-more" accessibilityRole="button" aria-expanded={all} onPress={() => setAll(!all)}
                       style={{ minHeight: 44, justifyContent: "center" }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{all ? RUN_WORDS.fewer : vm.moreWords}</Text>
            </Pressable>
          )}
          {vm.riverEntry && <Text style={{ ...TYPE.small, color: palette.sub }}>
            {vm.km ? `${vm.riverEntry} ${vm.km}` : vm.riverEntry}</Text>}
          {vm.countsNote && <Text testID="runs-counts-note" style={{ ...TYPE.small, color: palette.sub }}>{vm.countsNote}</Text>}
          <Text testID="runs-credit" style={{ ...TYPE.caption, color: palette.sub }}>{vm.credit}</Text>
          {onOpenWater && (
            <Pressable accessibilityRole="button" onPress={onOpenWater} style={{ paddingVertical: 8, minHeight: 44 }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{RUN_WORDS.openWater}</Text>
            </Pressable>
          )}
        </ScrollView>
      )}
    </Sheet>
  );
}

function RunRow({ r, pentad, palette, theme, art }: {
  r: RunRowVM; pentad: number; palette: Palette; theme: string; art: FishImages[string][number] | undefined;
}) {
  return (
    <View testID="runs-row" style={{ gap: 4, borderBottomWidth: 1, borderBottomColor: palette.line, paddingBottom: 12 }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
        {art && <Image source={art} resizeMode="contain" accessibilityIgnoresInvertColors style={{ width: 44, height: 22 }} />}
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink, flexShrink: 1, flexGrow: 1 }}>{r.title}</Text>
        <Text style={{ ...TYPE.small, color: r.now ? palette.ink : palette.sub, fontWeight: r.now ? "600" : "400" }}>
          {r.nowWords}</Text>
      </View>
      <Text style={{ ...TYPE.small, color: palette.sub }}>{r.population} · {r.relation}</Text>
      <YearChart r={r} pentad={pentad} palette={palette} theme={theme} />
      {r.dates && <Text style={{ ...TYPE.small, color: palette.ink }}>{r.dates}</Text>}
      {r.survey && <Text style={{ ...TYPE.small, color: palette.sub }}>{r.survey}</Text>}
      <Counts c={r.counts} palette={palette} />
    </View>
  );
}

/** The run's year: 73 pentads, in the water in the map's colour for how it is here, the slider's days marked. */
function YearChart({ r, pentad, palette, theme }: { r: RunRowVM; pentad: number; palette: Palette; theme: string }) {
  const on = runStateColour(theme, r.kind);
  return (
    <View testID="runs-year" accessibilityElementsHidden importantForAccessibility="no-hide-descendants"
          style={{ marginTop: 2 }}>
      <View style={{ flexDirection: "row", height: 12, backgroundColor: palette.wash, borderRadius: palette.r.chip, overflow: "hidden" }}>
        {r.year.map((w, p) => (
          <View key={p} style={{ flex: 1, backgroundColor: w ? on : "transparent",
                                 opacity: w && r.kind === "pass" && p % 2 ? 0.55 : 1 }} />
        ))}
        <View style={{ position: "absolute", top: -2, bottom: -2, left: `${(pentad / 73) * 100}%`, width: 2,
                       backgroundColor: palette.ink }} />
      </View>
      <View style={{ height: 11 }}>
        {MONTH_PENTADS.map((p, m) => (
          <Text key={m} style={{ ...TYPE.month, fontSize: 9, position: "absolute", left: `${(p / 73) * 100}%`,
                                 color: palette.sub }}>{MONTH_LETTER[m]}</Text>
        ))}
      </View>
    </View>
  );
}

/** The run's size: each labelled series (total, adult) as a bar per counted year on the legend's scale, its latest and
 *  trend, one under the other, never combined — or "No counts". */
function Counts({ c, palette }: { c: RunCountsVM | null; palette: Palette }) {
  if (!c) return <Text testID="runs-no-counts" style={{ ...TYPE.small, color: palette.sub }}>{RUN_WORDS.noCounts}</Text>;
  return (
    <View testID="runs-counts" style={{ gap: 8, marginTop: 2 }}>
      {c.series.map((s) => (
        <View key={s.label} testID="runs-series" style={{ gap: 3 }}>
          <Text style={{ ...TYPE.small, color: palette.ink, fontWeight: "600" }}>{s.label}</Text>
          <View style={{ flexDirection: "row", alignItems: "flex-end", gap: 3, height: 26 }}
                accessibilityLabel={`${s.label}: ${s.bars.map((b) => b.label).join(", ")}`}>
            {s.bars.map((b) => (
              <View key={b.year} style={{ width: 12, height: Math.max(1, Math.round(b.h * 24)), backgroundColor: palette.ink,
                                          opacity: b.n > 0 ? 0.75 : 0.25 }} />
            ))}
            <Text style={{ ...TYPE.month, fontSize: 9, color: palette.sub, marginLeft: 4 }}>
              {`${s.bars[0]!.year}–${s.bars[s.bars.length - 1]!.year}`}</Text>
          </View>
          <Text style={{ ...TYPE.small, color: palette.ink }}>{s.latest}</Text>
          {s.trend && <Text style={{ ...TYPE.small, color: palette.sub }}>{s.trend}</Text>}
        </View>
      ))}
    </View>
  );
}
