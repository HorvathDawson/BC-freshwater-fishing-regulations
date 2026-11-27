/**
 * THE DAY CALENDAR (T49, owner 2026-10-21: "we need to fix date picker on regs map. could just be a calendar icon.").
 *
 * ONE component, two entry points: the map's day strip's calendar segment (`MapDayStrip`, in the Map tab's corner) and the
 * Regulations page's date pill both open a sheet (`regs/DaySheet`; a popover on a desktop browser, T915) holding this. It moves THE day — the Shell's
 * `mapDay`, null = today (decisions D7) — through `onDay` and keeps no day of its own, so the map's colours and the
 * page never disagree. It keeps only which MONTH it is showing.
 *
 * A month grid (today ringed, the chosen day filled; always six weeks of fixed sevenths, T877), ‹ › months, "Today", and the quick ‹ › day steps for tomorrow and
 * yesterday. Days outside the data's calendar year (`@app/core` `dayRange`) are disabled, and the reason is said under
 * the grid — never a silently wrong day — in a room the calendar always keeps, so it is one size in every month (T915). Every control is a 44 px target, every day cell says its day in words
 * ("Saturday, June 15"), and on the web the grid takes the arrow keys (roving focus: only the chosen day is in the tab
 * order). The words and the grid are `@app/core` `dayPicker.ts`; this only draws them.
 */
import { useEffect, useRef, useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { chooseDay, DAY_PICKER, dayButtonVM, monthVM, showingLine, stepDay, type DayRange, bcNow } from "@app/core";
import { CornerStrip, MAP_CELL } from "./CornerStrip";
import { Icon, type IconName } from "./icons";
import type { Palette } from "./theme";
import { TYPE } from "./type";
import { hitProps } from "./regs/hit";

/** A thumb's target: every button here is at least this tall and wide. */
const TARGET = 44;

/**
 * (T877, owner 2026-11-18: "The calendar shifts around") ONE CELL of the month grid and of its week heads: a FIXED seventh
 * of the row, blank or day alike, never `flex: 1`. With `flex: 1` and a 44 px `minWidth` on the days only, Yoga (the
 * native layout) grew each day from 44 and each blank from 0, so a short first or last week spread its days across the
 * row (Aug 31, a Monday, drew under Tue). Every cell the same seventh puts each day under its weekday by construction.
 */
const CELL = { width: `${100 / 7}%`, minWidth: TARGET } as const;

/**
 * (T1212, owner 2026-11-26: "One \"Today\" chip that opens the calendar"; option A "Tidy") THE MAP'S DAY CHIP, in the
 * top-left corner under search, level with the rail: the calendar icon, the day shown ("Today", or the day picked) and a
 * chevron; a press opens the date sheet (its month grid, Today and the day steps). It replaced ‹ · day · › (T213/T367):
 * the arrows went. A day other than today fills the chip (it repaints the whole map) and adds ✕ beside it, "Back to
 * today": one tap home. 44 px tall (`MAP_CELL`), the rail's height. The words are `@app/core` `dayButtonVM` / `DAY_PICKER`.
 */
export function MapDayStrip({ palette, day, onDay, onOpen, now }: {
  palette: Palette; day: Date | null;
  onDay: (d: Date | null) => void; onOpen: () => void;
  /** for tests; the device's clock otherwise */
  now?: Date;
}) {
  const shown = dayButtonVM(day, now ?? bcNow());
  return (
    <CornerStrip palette={palette} testID="map-day" label={DAY_PICKER.strip} coach="days" cellHeight={MAP_CELL}
                 segments={[
                   { key: "pick", chip: shown.text, icon: "calendar" as const, trail: "expand" as const, label: shown.label,
                     on: day !== null, onPress: onOpen },
                   ...(day === null ? [] : [{ key: "today", chip: "", icon: "close" as const, label: DAY_PICKER.todayLabel, on: false,
                                              onPress: () => onDay(null) }]),
                 ]} />
  );
}

/**
 * A month or day-step button. AT MODULE SCOPE, never inside `DayCalendar`'s render: a component declared there is a new
 * type every render, so React would remount the button on each press and a keyboard user's focus would fall to the page.
 */
function Btn({ palette, text, icon, label, onPress, enabled = true, strong = false }:
  { palette: Palette; text?: string; icon?: IconName; label: string; onPress: () => void; enabled?: boolean; strong?: boolean }) {
  return (
    <Pressable {...hitProps({ w: TARGET, h: TARGET })} onPress={onPress} disabled={!enabled} accessibilityRole="button" accessibilityLabel={label}
               aria-disabled={!enabled}
               style={({ pressed }) => ({
                 minHeight: TARGET, minWidth: TARGET, paddingHorizontal: 10, alignItems: "center",
                 justifyContent: "center", backgroundColor: pressed ? palette.tint : "transparent",
                 borderWidth: strong ? 1.5 : 0, borderColor: palette.accent, opacity: enabled ? 1 : 0.35,
               })}>
      {icon ? <Icon name={icon} colour={palette.accent} size={20} />
        : <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{text}</Text>}
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
  const now = nowProp ?? bcNow();
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
  const canEarlier = stepDay(day, -1, now, range) !== undefined;
  const canLater = stepDay(day, 1, now, range) !== undefined;
  const moveMonth = (n: number) => setView((v) => {
    const d = new Date(v.year, v.month + n, 1);
    return { year: d.getFullYear(), month: d.getMonth() };
  });
  // the tab stop: the chosen day when it is in this month, else its first open day
  const cellsShown = m.weeks.flat().filter((c) => c !== null);
  const tabStop = (cellsShown.find((c) => c!.chosen) ?? cellsShown.find((c) => !c!.disabled))?.iso;

  return (
    <View testID="day-calendar" style={{ width: "100%", maxWidth: 380, alignSelf: "center", gap: 6 }}>
      <Text style={{ ...TYPE.small, color: palette.sub, textAlign: "center" }}>{showingLine(day, now)}</Text>
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
        <Btn palette={palette} icon="back" label={DAY_PICKER.prevMonth} onPress={() => moveMonth(-1)} enabled={m.prev} />
        <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, fontSize: 16, color: palette.ink }}>{m.title}</Text>
        <Btn palette={palette} icon="next" label={DAY_PICKER.nextMonth} onPress={() => moveMonth(1)} enabled={m.next} />
      </View>
      <View testID="day-heads" style={{ flexDirection: "row" }} importantForAccessibility="no-hide-descendants" aria-hidden>
        {m.weekdays.map((w) => (
          <Text key={w.long} style={{ ...TYPE.micro, ...CELL, textAlign: "center", color: palette.sub }}>{w.short}</Text>
        ))}
      </View>
      <View testID="day-grid" role="group" accessibilityLabel={DAY_PICKER.gridLabel(m.title)} {...keys}>
        {m.weeks.map((w, wi) => (
          <View key={wi} style={{ flexDirection: "row" }}>
            {w.map((c, ci) => c === null
              ? <View key={`b${ci}`} style={{ ...CELL, minHeight: TARGET }} />
              : (
                <Pressable {...hitProps({ w: TARGET, h: TARGET })} key={c.iso} ref={(el) => { cellRefs.current.set(c.iso, el as never); }}
                           onPress={() => onDay(chooseDay(new Date(shownView.year, shownView.month, c.n, 12), now))}
                           disabled={c.disabled} accessibilityRole="button" accessibilityLabel={c.label}
                           aria-selected={c.chosen} aria-disabled={c.disabled}
                           tabIndex={c.iso === tabStop ? 0 : -1}
                           style={({ pressed }) => ({
                             ...CELL, minHeight: TARGET, alignItems: "center",
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
      {/*
        (T915, owner 2026-11-19: "calendar still jumpy in size from january to other dates") THE NOTE HOLDS ITS ROOM. The
        reason came and went (January's "Only days in 2026 …", the release's cut), two lines on a phone, so the sheet
        grew and shrank between months. The room is the longest reason any month says (`reasonRoom`), drawn unseen and
        unread, so it wraps as the real words would at this width and text size; this month's reason, if any, is drawn
        over it and is the one a screen reader hears.
      */}
      {m.reasonRoom !== "" && (
        <View testID="day-reason-room">
          <Text aria-hidden importantForAccessibility="no-hide-descendants" selectable={false}
                style={{ ...TYPE.small, textAlign: "center", opacity: 0 }}>{m.reasonRoom}</Text>
          {m.reason && (
            <Text testID="day-reason" style={{ ...TYPE.small, color: palette.sub, textAlign: "center",
                                               position: "absolute", top: 0, left: 0, right: 0 }}>{m.reason}</Text>
          )}
        </View>
      )}
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", marginTop: 4 }}>
        <Btn palette={palette} text={DAY_PICKER.earlier} label={DAY_PICKER.earlierLabel} onPress={() => step(-1)} enabled={canEarlier} />
        <Btn palette={palette} text={DAY_PICKER.today} label={DAY_PICKER.todayLabel} onPress={() => onDay(null)} strong />
        <Btn palette={palette} text={DAY_PICKER.later} label={DAY_PICKER.laterLabel} onPress={() => step(1)} enabled={canLater} />
      </View>
    </View>
  );
}
