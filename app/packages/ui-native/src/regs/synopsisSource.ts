/**
 * T215 — WHERE OUR COPY OF THE SYNOPSIS PDF IS: the release's own `synopsis` block (its url made absolute by the boot's
 * `locate`), set once by the app when the release is ready. The boxes the export ships (`rules[].book_box`) were cut from
 * THESE bytes, so the highlight is drawn on this copy and no other (the gov.bc.ca file is not byte-identical).
 * Null (no release block, e.g. the fixture release): `openBook` keeps opening the plain link.
 */
import type { SynopsisAppend } from "@app/data";

export interface SynopsisSource {
  readonly url: string; readonly sha256: string; readonly bytes: number;
  /**
   * (T285) where the SAME bytes are now, asked only when `url` answers 404. A release's files go when the next release
   * replaces it (staging keeps the latest only), yet the synopsis is named by its content and rides on into the next one:
   * a device still on the older release finds the file under the live release. Null: nowhere else.
   */
  readonly alternate?: () => Promise<string | null>;
  /** (T354) the release's page index for the cited-page-first append (`appendPdf.ts`); absent: the book opens the old way. */
  readonly append?: SynopsisAppend | null;
}

let source: SynopsisSource | null = null;

export const setSynopsisSource = (s: SynopsisSource | null): void => { source = s; };
export const getSynopsisSource = (): SynopsisSource | null => source;

/**
 * (T254) THE APP'S LINE, for the rule book: low-bandwidth mode and the measured rate (the offer says how long the
 * download would take), and the meter / online word the download itself uses. Null: no `Net` (a test): never offered.
 */
export interface SynopsisNet {
  readonly low: () => boolean;
  readonly bps: () => number | null;
  readonly meter?: import("@app/data").NetMeter | null;
  readonly online?: () => boolean;
  readonly waitOnline?: (signal?: AbortSignal) => Promise<void>;
}
let net: SynopsisNet | null = null;
export const setSynopsisNet = (n: SynopsisNet | null): void => { net = n; };
export const getSynopsisNet = (): SynopsisNet | null => net;
