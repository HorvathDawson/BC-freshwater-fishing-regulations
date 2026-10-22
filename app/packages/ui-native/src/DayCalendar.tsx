/**
 * THE DAY CALENDAR (T49, owner 2026-10-21: "we need to fix date picker on regs map. could just be a calendar icon.").
 *
 * ONE component, two entry points: the map's calendar button (`MapDayButton`, in the Map tab's legend strip) and the
 * Regulations page's date pill both open a sheet (`regs/DaySheet`) holding this. It moves THE day — the Shell's
 * `mapDay`, null = today (decisions D7) — through `onDay` and keeps no day of its own, so the map's colours and the
 * page never disagree. It keeps only which MONTH it is showing.
 *
 * A month grid (today ringed, the chosen day filled), ‹ › months, "Today", and the quick ‹ › day steps for tomorrow and
 * yesterday. Days outside the data's calendar year (`@app/core` `dayRange`) are disabled, and the reason is said under
 * the grid — never a silently wrong day. Every control is a 44 px target, every day cell says its day in words
 * ("Saturday, June 15"), and on the web the grid takes the arrow keys (roving focus: only the chosen day is in the tab
 * order). The words and the grid are `@app/core` `dayPicker.ts`; this only draws them.
 */
import { useEffect, useRef, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { chooseDay, DAY_PICKER, dayButtonVM, monthVM, showingLine, stepDay, type DayRange } from "@app/core";
import { CalendarIcon } from "./icons";
import type { Palette } from "./theme";
import { TYPE } from "./type";

/** A thumb's target: every button here is at least this tall and wide. */
const TARGET = 44;

/** The map's calendar button: the icon and "Today" or the day it shows ("Sat, Jun 15"); a press opens the date sheet. */
export function MapDayButton({ palette, day, onPress, now }: {
  palette: Palette; day: Date | null; onPress: () => void;
  /** for tests; the device's clock otherwise */
  now?: Date;
}) {
  const vm = dayButtonVM(day, now ?? new Date());
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={vm.label}
               style={({ pressed }) => ({
                 minHeight: TARGET, minWidth: TARGET, flexDirection: "row", alignItems: "center", gap: 6,
                 paddingHorizontal: 8,
                 // the strip's own padding is 7 above and the row gap 5 below: the target reaches into both, so the
                 // strip is no taller than the stepper it replaces
                 marginTop: -7, marginBottom: -5, marginLeft: -8,
                 backgroundColor: pressed ? palette.tint : day ? palette.accent : "transparent",
               })}>
      <CalendarIcon colour={day ? palette.onAccent : palette.ink} size={17} />
      <Text testID="map-day" style={{ ...TYPE.bodyStrong, fontSize: 13, color: day ? palette.onAccent : palette.ink }}>
        {vm.text}
      </Text>
    </Pressable>
  );
}

export function DayCalendar({ palette, day, onDay, range, now: nowProp }: {
  palette: Palette;
  /** THE day (the Shell's `mapDay`); null = today */
  day: Date | null;
  onDay: (d: Date | null) => void;
  /** the days the data covers (`dayRange(now, manifest.regulations.valid_until)`) */
  range: DayRange;
  /** for tests; the device's clock otherwise */
  now?: Date;
}) {
  const now = nowProp ?? new Date();
  const shown = day ?? now;
  const [view, setView] = useState({ year: shown.getFullYear(), month: shown.getMonth() });
  // the month follows the day when the day moves (a step, a key, Today) — IN THE SAME RENDER, not an effect after it:
  // the focus effect below must see the new month's cells in the commit the day lands, or a key across a month edge
  // drops focus to the page (the old cell is unmounted) and the grid takes no more keys
  const shownKey = `${shown.getFullYear()}-${shown.getMonth()}-${shown.getDate()}`;
  const [followed, setFollowed] = useState(shownKey);
  let shownView = view;
  if (followed !== shownKey) {
    shownView = { year: shown.getFullYear(), month: shown.getMonth() };
    setFollowed(shownKey);
    setView(shownView);
  }
  const m = monthVM(shownView.year, shownView.month, day, now, range);

  // the keyboard moved the day: focus follows it to its cell once drawn
  const cellRefs = useRef(new Map<string, { focus?: () => void } | null>());
  const keyed = useRef(false);
  useEffect(() => {
    if (!keyed.current) return;
    keyed.current = false;
    const iso = m.weeks.flat().find((c) => c?.chosen)?.iso;
    if (iso) cellRefs.current.get(iso)?.focus?.();
  });

  const step = (n: number, months = false) => {
    const next = stepDay(day, n, now, range, months);
    if (next !== undefined) onDay(next);
  };
  const KEYS: Record<string, () => void> = {
    ArrowLeft: () => step(-1), ArrowRight: () => step(1), ArrowUp: () => step(-7), ArrowDown: () => step(7),
    PageUp: () => step(-1, true), PageDown: () => step(1, true), Home: () => onDay(null),
  };
  /*
   * THE KEYS, on the grid: a focused day cell's key bubbles here (react-native-web forwards `onKeyDown` to the DOM;
   * RN 0.86 reads it from a hardware keyboard). The typings this package compiles against do not list the prop, so it
   * is spread rather than written.
   */
  const keys = {
    onKeyDown: (e: { nativeEvent: { key: string }; preventDefault?: () => void }) => {
      const go = KEYS[e.nativeEvent.key];
      if (!go) return;
      e.preventDefault?.();          // the arrows and Page keys would scroll the page under the sheet
      keyed.current = true;
      go();
    },
  } as object;
  const canEarlier =stepDay(day, -1, now, range) !== undefined;
  const canLater = stepDay(day, 1, now, range) !== undefined;
  const moveMonth = (n: number) => setView((v) => {
    const d = new Date(v.year, v.month + n, 1);
    return { year: d.getFullYear(), month: d.getMonth() };
  });
  // the tab stop: the chosen day when it is in this month, else its first open day
  const cellsShown = m.weeks.flat().filter((c) => c !== null);
  const tabStop = (cellsShown.find((c) => c!.chosen) ?? cellsShown.find((c) => !c!.disabled))?.iso;

  const Btn = ({ text, label, onPress, enabled = true, strong = false }:
    { text: string; label: string; onPress: () => void; enabled?: boolean; strong?: boolean }) => (
    <Pressable onPress={onPress} disabled={!enabled} accessibilityRole="button" accessibilityLabel={label}
               aria-disabled={!enabled}
               style={({ pressed }) => ({
                 minHeight: TARGET, minWidth: TARGET, paddingHorizontal: 10, alignItems: "center",
                 justifyContent: "center", backgroundColor: pressed ? palette.tint : "transparent",
                 borderWidth: strong ? 1.5 : 0, borderColor: palette.accent, opacity: enabled ? 1 : 0.35,
               })}>
      <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{text}</Text>
    </Pressable>
  );

  return (
    <View testID="day-calendar" style={{ width: "100%", maxWidth: 380, alignSelf: "center", gap: 6 }}>
      <Text style={{ ...TYPE.small, color: palette.sub, textAlign: "center" }}>{showingLine(day, now)}</Text>
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
        <Btn text="‹" label={DAY_PICKER.prevMonth} onPress={() => moveMonth(-1)} enabled={m.prev} />
        <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, fontSize: 16, color: palette.ink }}>{m.title}</Text>
        <Btn text="›" label={DAY_PICKER.nextMonth} onPress={() => moveMonth(1)} enabled={m.next} />
      </View>
      <View style={{ flexDirection: "row" }} importantForAccessibility="no-hide-descendants" aria-hidden>
        {m.weekdays.map((w) => (
          <Text key={w.long} style={{ ...TYPE.micro, flex: 1, textAlign: "center", color: palette.sub }}>{w.short}</Text>
        ))}
      </View>
      <View testID="day-grid" accessibilityLabel={DAY_PICKER.gridLabel(m.title)} {...keys}>
        {m.weeks.map((w, wi) => (
          <View key={wi} style={{ flexDirection: "row" }}>
            {w.map((c, ci) => c === null
              ? <View key={`b${ci}`} style={{ flex: 1, minHeight: TARGET }} />
              : (
                <Pressable key={c.iso} ref={(el) => { cellRefs.current.set(c.iso, el as never); }}
                           onPress={() => onDay(chooseDay(new Date(shownView.year, shownView.month, c.n, 12), now))}
                           disabled={c.disabled} accessibilityRole="button" accessibilityLabel={c.label}
                           aria-selected={c.chosen} aria-disabled={c.disabled}
                           tabIndex={c.iso === tabStop ? 0 : -1}
                           style={({ pressed }) => ({
                             flex: 1, minHeight: TARGET, minWidth: TARGET, alignItems: "center",
                             justifyContent: "center",
                             backgroundColor: c.chosen ? palette.accent : pressed ? palette.tint : "transparent",
                             borderWidth: c.today ? 1.5 : 0, borderColor: c.chosen ? palette.ink : palette.accent,
                             opacity: c.disabled ? 0.35 : 1,
                           })}>
                  <Text style={{ ...(c.today || c.chosen ? TYPE.bodyStrong : TYPE.body),
                                 color: c.chosen ? palette.onAccent : c.today ? palette.accent : palette.ink }}>
                    {c.n}
                  </Text>
                </Pressable>
              ))}
          </View>
        ))}
      </View>
      {m.reason && (
        <Text testID="day-reason" style={{ ...TYPE.small, color: palette.sub, textAlign: "center" }}>{m.reason}</Text>
      )}
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", marginTop: 4 }}>
        <Btn text={DAY_PICKER.earlier} label={DAY_PICKER.earlierLabel} onPress={() => step(-1)} enabled={canEarlier} />
        <Btn text={DAY_PICKER.today} label={DAY_PICKER.todayLabel} onPress={() => onDay(null)} strong />
        <Btn text={DAY_PICKER.later} label={DAY_PICKER.laterLabel} onPress={() => step(1)} enabled={canLater} />
      </View>
    </View>
  );
}
