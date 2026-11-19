/**
 * (T377 item 1, owner idea T367 round 3 A2) THE FORECAST DAYS OF ONE WATERSHED GROUP: Now, then each forecast day the
 * horizon chips offer (`DAY_HORIZONS` — the chips without Change), each with the group's standing that day (the field's
 * value, `basinStandings` in @app/data) said in the app's own standing words (`standing` / `standingWord`, the bands the
 * map's ramp and its legend break at). A tap on a day shows that day: the horizon the map and the view are on.
 *
 * The group's name is AS SOURCED ("Chilliwack River"); the word around it is here. A day no gauge in the group reaches
 * says so — never a blank.
 */
import { standing, type Standing } from "./flow";
import { bandWord } from "./standing";
import { DAY_HORIZONS, HORIZON_LABELS, horizonDateChip, type DayHorizon, type Horizon } from "./conditions";

export const BASIN_DAYS_WORDS = {
  /** the card's title: the group's name as sourced, and the word around it */
  title: (name: string) => `${name} watershed`,
  /** under it: what each day's colour ranks, by quantity */
  scale: { discharge: "Flow vs. past years, by day", level: "Water level vs. past years, by day" },
  /** (T427) the title row's right end in the bottom bar: the chosen day and its standing, "Now · much below normal" */
  verdict: (chip: string, word: string) => `${chip} · ${word.toLowerCase()}`,
  /** a day no gauge in the group reaches */
  none: "No gauge reading",
  /** the card's foot when a day has no reading: what the dash means */
  noneKey: "— no gauge in this watershed reads that day",
  /** the group itself cannot be had */
  noGroup: { browse: "No watershed group is on the map", water: "This water is in no watershed group" },
  /** what is loading or failed, for core's part line (`partLine`) */
  part: "Watershed days",
  /** the row's name aloud */
  list: (name: string) => `${name} watershed, standing by day`,
  /**
   * (T872, owner 2026-11-18: "These section should be collapsed in conditions I think unless someone expands") THE FOLDED
   * CARD'S ONE LINE: the first day, then only the days the standing CHANGES ("Now: Normal · Much above normal from Wed 25
   * · No gauge reading from Sat 28"); one standing all the way is said as such; past `SUMMARY_CHANGES` changes the rest
   * are counted, never listed.
   */
  summary: {
    first: (chip: string, word: string) => `${chip}: ${word}`,
    from: (word: string, chip: string) => `${word} from ${chip}`,
    same: (chip: string) => `the same through ${chip}`,
    more: (n: number) => `${n} more ${n === 1 ? "change" : "changes"}`,
    join: " · ",
  },
  /** (T872) the fold's button aloud — its expanded / collapsed state is the button's own (`aria-expanded`) */
  toggle: (name: string, summary: string) => `${name} watershed, by day: ${summary}`,
} as const;

/** (T872) how many change points the folded line names before it counts the rest */
export const SUMMARY_CHANGES = 3;

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
  /** (T427) the day shown, in words ("Now · much below normal"); null when no day is lit (Change) */
  readonly verdict: string | null;
  readonly list: string;
  /** (T872) the folded card's one line: the first day, then each day the standing changes (`basinDaysSummary`) */
  readonly summary: string;
  /** (T872) the fold's button aloud: the name and the summary */
  readonly toggle: string;
  readonly days: readonly BasinDayVM[];
  /** a key for the dash, when a day has none; null otherwise */
  readonly noneKey: string | null;
}


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
    const word = s ? bandWord(s) : BASIN_DAYS_WORDS.none;
    const lit = d === on;
    return { day: d, chip: horizonDateChip(d, now), standing: s, percentile: has ? p : null, word,
             label: `${HORIZON_LABELS[d].accessible}: ${word.toLowerCase()}${lit ? ", showing" : ""}`, on: lit };
  });
  const lit = days.find((d) => d.on);
  const summary = basinDaysSummary(days);
  return { title: BASIN_DAYS_WORDS.title(name), scale: BASIN_DAYS_WORDS.scale[quantity],
           verdict: lit ? BASIN_DAYS_WORDS.verdict(lit.chip, lit.word) : null,
           list: BASIN_DAYS_WORDS.list(name), summary, toggle: BASIN_DAYS_WORDS.toggle(name, summary), days,
           noneKey: days.some((d) => d.standing === null) ? BASIN_DAYS_WORDS.noneKey : null };
}

/**
 * (T872) THE FOLDED LINE: the days as RUNS of one word (a band, or no reading — a day no gauge reads is a change like any
 * other, never skipped). The first run is said with its day ("Now: Normal"); each later run by the day it starts
 * ("Much above normal from Wed 25"); a single run says it holds to the last day; past `SUMMARY_CHANGES` changes the rest
 * are counted.
 */
export function basinDaysSummary(days: readonly Pick<BasinDayVM, "chip" | "word">[]): string {
  const W = BASIN_DAYS_WORDS.summary;
  const first = days[0];
  if (!first) return "";
  const starts = days.filter((d, i) => i > 0 && d.word !== days[i - 1]!.word);
  const parts = [W.first(first.chip, first.word)];
  if (starts.length === 0) {
    const last = days[days.length - 1]!;
    if (last !== first) parts.push(W.same(last.chip));
  } else {
    for (const d of starts.slice(0, SUMMARY_CHANGES)) parts.push(W.from(d.word, d.chip));
    if (starts.length > SUMMARY_CHANGES) parts.push(W.more(starts.length - SUMMARY_CHANGES));
  }
  return parts.join(W.join);
}
