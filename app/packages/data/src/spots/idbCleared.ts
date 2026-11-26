/**
 * (T1092) THE BROWSER CLEARED THIS SITE'S SAVED DATA WHILE THE PAGE WAS OPEN (site data cleared, a private window closed,
 * the browser's own eviction): every IndexedDB request on the open connection fails with "Database deleted by request of
 * the user". That is the reader's own act and the browser's, not a defect: it is said in plain words, marked the
 * environment's (shown, never reported), and the caller drops its connection so the very next request opens a fresh
 * database and works. What was kept in the cleared database is gone by that act; nothing is shown from memory as saved.
 */
import { environmentError } from "@app/core";

/** The browser's own words (Chromium), whatever the error's name. */
const CLEARED = /Database deleted by request of the user/i;

/** What the reader is told when a request met the cleared database. */
export const STORAGE_CLEARED_WORDS = "Your browser cleared this site's saved data while the page was open. Try that again.";

/** The error this IndexedDB failure is: the plain environment error when it is the cleared database, else as it came. */
export function clearedOr(e: unknown): unknown {
  const message = e && typeof e === "object" ? (e as { message?: unknown }).message : null;
  return typeof message === "string" && CLEARED.test(message) ? environmentError(new Error(STORAGE_CLEARED_WORDS)) : e;
}

/** Is this the error `clearedOr` made (the connection under it is gone)? */
export const isCleared = (e: unknown): boolean => e instanceof Error && e.message === STORAGE_CLEARED_WORDS;
