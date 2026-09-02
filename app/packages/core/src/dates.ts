/**
 * Season windows. Half the regulations in the synopsis are seasonal, so a map with no
 * date is wrong for most of the year, and a window that wraps the new year is the case
 * that a naive comparison gets silently backwards.
 *
 * A window is inclusive at both ends and carries no year: "Apr 1 - Jun 30" is the same
 * window every year, and "Oct 15 - Apr 15" wraps.
 */

/** Month is 1-12. Not a Date: a window has no year, and pretending it does invents one. */
export interface MonthDay {
  readonly month: number;
  readonly day: number;
}

export interface Window {
  readonly from: MonthDay;
  readonly to: MonthDay;
}

/** A calendar day, with no time and no zone. Regulations change at midnight local. */
export interface PlainDate {
  readonly year: number;
  readonly month: number;
  readonly day: number;
}

const ord = (m: number, d: number): number => m * 100 + d;

export function isValid({ month, day }: MonthDay): boolean {
  if (!Number.isInteger(month) || !Number.isInteger(day)) return false;
  if (month < 1 || month > 12 || day < 1) return false;
  // 29 Feb is a legitimate window edge; a leap year is not this type's business.
  return day <= [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]!;
}

/** Does `on` fall inside `w`? Handles windows that wrap the year end. */
export function inWindow(w: Window, on: PlainDate): boolean {
  const from = ord(w.from.month, w.from.day);
  const to = ord(w.to.month, w.to.day);
  const at = ord(on.month, on.day);
  return from <= to ? at >= from && at <= to : at >= from || at <= to;
}

/**
 * Is this rule in force on this date?
 *
 * No windows means all year — NOT "never". Getting that backwards would render every
 * year-round closure as open, which is the one failure with real consequences.
 */
export function inForce(windows: readonly Window[], on: PlainDate): boolean {
  if (windows.length === 0) return true;
  return windows.some((w) => inWindow(w, on));
}
