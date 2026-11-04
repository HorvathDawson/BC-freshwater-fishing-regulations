/**
 * (T377 item 1, owner idea T367 round 3 A2) THE FORECAST DAYS OF ONE WATERSHED GROUP: Now, then each forecast day the
 * horizon chips offer (`DAY_HORIZONS` — the chips without Change), each with the group's standing that day (the field's
 * value, `basinStandings` in @app/data) said in the app's own standing words (`standing` / `standingWord`, the bands the
 * map's ramp and its legend break at). A tap on a day shows that day: the horizon the map and the view are on.
 *
 * The group's name is AS SOURCED ("Chilliwack River"); the word around it is here. A day no gauge in the group reaches
 * says so — never a blank.
 */
import { standing, standingWord, type Standing } from "./flow";
import { DAY_HORIZONS, HORIZON_LABELS, horizonDateChip, type DayHorizon, type Horizon } from "./conditions";

export const BASIN_DAYS_WORDS = {
  /** the card's title: the group's name as sourced, and the word around it */
  title: (name: string) => `${name} watershed`,
  /** under it: what each day's colour ranks, by quantity */
  scale: { discharge: "Flow vs. past years, by day", level: "Water level vs. past years, by day" },
  /** a day no gauge in the group reaches */
  none: "No gauge reading",
  /** the card's foot when a day has no reading: what the dash means */
  noneKey: "— no gauge in this watershed reads that day",
  /** the group itself cannot be had */
  noGroup: { browse: "The map's centre is in no watershed group", water: "This water is in no watershed group" },
  /** what is loading or failed, for core's part line (`partLine`) */
  part: "Watershed days",
  /** the row's name aloud */
  list: (name: string) => `${name} watershed, standing by day`,
} as const;

/** One day of the list. */
export interface BasinDayVM {
  readonly day: DayHorizon;
  /** the chip's words: "Now", "Wed 5" (`horizonDateChip`, as the horizon chips say it) */
  readonly chip: string;
  /** the band (null: no gauge in the group reads this day) */
  readonly standing: Standing | null;
  /** the percentile 0–1, for the ramp's colour; null with `standing` */
  readonly percentile: number | null;
  /** the band in words ("Below normal") or `BASIN_DAYS_WORDS.none` */
  readonly word: string;
  /** the button's full name aloud: "Forecast 3 days ahead: below normal, showing" */
  readonly label: string;
  /** the day the map and view are on */
  readonly on: boolean;
}

export interface BasinDaysVM {
  readonly title: string;
  readonly scale: string;
  readonly list: string;
  readonly days: readonly BasinDayVM[];
  /** a key for the dash, when a day has none; null otherwise */
  readonly noneKey: string | null;
}

const sentence = (s: string) => s.charAt(0) + s.slice(1).toLowerCase();

/**
 * THE LIST: one row per day horizon, in order. `standings` holds the group's value for each day it has one (a day
 * missing from it, or null, is a day no gauge in the group reads). `on` is the horizon shown: Change lights no day.
 */
export function basinDaysVM(name: string, standings: ReadonlyMap<DayHorizon, number | null>, on: Horizon,
                            quantity: "discharge" | "level", now: Date): BasinDaysVM {
  const days = DAY_HORIZONS.map((d): BasinDayVM => {
    const p = standings.get(d);
    const has = typeof p === "number" && Number.isFinite(p);
    const s = has ? standing(p) : null;
    const word = s ? sentence(standingWord(s)) : BASIN_DAYS_WORDS.none;
    const lit = d === on;
    return { day: d, chip: horizonDateChip(d, now), standing: s, percentile: has ? p : null, word,
             label: `${HORIZON_LABELS[d].accessible}: ${word.toLowerCase()}${lit ? ", showing" : ""}`, on: lit };
  });
  return { title: BASIN_DAYS_WORDS.title(name), scale: BASIN_DAYS_WORDS.scale[quantity],
           list: BASIN_DAYS_WORDS.list(name), days,
           noneKey: days.some((d) => d.standing === null) ? BASIN_DAYS_WORDS.noneKey : null };
}
