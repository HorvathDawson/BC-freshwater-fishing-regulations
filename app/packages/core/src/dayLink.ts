/**
 * (T582, owner 2026-11-10) TWO DAYS, EACH LABELLED, WITH A SOFT LINK. The forecast is a short window; regulations can be
 * asked for any day. So a water screen has the REGULATIONS DAY (the map's day picker, any date; Regulations and the
 * water's Map answer it) and the CONDITIONS DAY (today plus the forecast days; its own chips). Neither moves the other.
 * Each tab says which day it shows, and Conditions says how the regulations day sits against its window:
 *  - inside the window: that day is marked on the chart;
 *  - past it: one line, "<day> is past the forecast.", and Conditions shows today and the forecast as always.
 * Pure and shared: web and phone draw these words, and nothing else decides them.
 */
import { FORECAST_DAYS } from "./gauge-policy.generated";
import { monthAbbr, weekdayAbbr } from "./flow";
import type { Horizon } from "./conditions";
import { pentadLabel } from "./runTiming";

/** The last day of the forecast window (days from today). */
export const FORECAST_WINDOW: number = Math.max(...FORECAST_DAYS);

const noon = (now: Date, offset: number) => new Date(now.getFullYear(), now.getMonth(), now.getDate() + offset, 12);
const startOf = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();

/** "Sat 14 Nov": a day as every label names it. */
export function dayWords(d: Date): string {
  return `${weekdayAbbr(d.getDay())} ${d.getDate()} ${monthAbbr(d.getMonth())}`;
}

/** Whole days from `now`'s day to `day`'s (calendar days, so a DST change cannot make one short). */
export function daysFromToday(day: Date, now: Date): number {
  return Math.round((startOf(day) - startOf(now)) / 86_400_000);
}

export type RegsDayLink =
  /** the regulations day is today (or none, or already gone): nothing to link */
  | { kind: "today" }
  /** inside the window: `at` (epoch ms, noon) and `mark` are what the chart marks */
  | { kind: "inside"; offset: number; name: string; at: number; mark: string; line: string }
  /** past the window: one line and no mark */
  | { kind: "past"; offset: number; name: string; line: string };

export const DAY_LINK_WORDS = {
  inside: (name: string) => `Regulations day: ${name}, marked on the chart.`,
  past: (name: string) => `${name} is past the forecast.`,
} as const;

/** How the regulations day (null: today) sits against the Conditions window. */
export function regsDayLink(regsDay: Date | null, now: Date): RegsDayLink {
  if (regsDay === null) return { kind: "today" };
  const offset = daysFromToday(regsDay, now);
  if (offset <= 0) return { kind: "today" };
  const name = dayWords(noon(now, offset));
  if (offset > FORECAST_WINDOW) return { kind: "past", offset, name, line: DAY_LINK_WORDS.past(name) };
  return { kind: "inside", offset, name, at: noon(now, offset).getTime(), mark: name,
           line: DAY_LINK_WORDS.inside(name) };
}

/** What each tab says about the day it shows (a tab switch never changes the day silently). */
export const DAY_LABELS = {
  /** Conditions: the horizon it is on (Change is the next ten days, no one day). */
  conditions: (horizon: Horizon, now: Date): string =>
    horizon === "change" ? "Conditions: expected change, next 10 days"
    : horizon === 0 ? "Conditions: today" : `Conditions: ${dayWords(noon(now, horizon))}`,
  /** Regulations and the water's Map: the regulations day. */
  regulations: (regsDay: Date | null, now: Date): string =>
    regsDay === null || daysFromToday(regsDay, now) <= 0 ? "Regulations: today" : `Regulations: ${dayWords(regsDay)}`,
  /**
   * (T624, owner 2026-11-10: "Runs is its own thing") Runs: the five days its slider holds (a pentad), and "(today)" when
   * they hold B.C.'s today. Runs' day is its own: the map's day never moves it and it never moves the map's.
   */
  runs: (pentad: number, today: number): string =>
    `Runs: ${pentadLabel(pentad)}${pentad === today ? " (today)" : ""}`,
  map: (regsDay: Date | null, now: Date): string =>
    regsDay === null || daysFromToday(regsDay, now) <= 0 ? "Map: today's rules" : `Map: rules for ${dayWords(regsDay)}`,
} as const;
