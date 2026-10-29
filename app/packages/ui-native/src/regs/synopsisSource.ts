/**
 * T215 — WHERE OUR COPY OF THE SYNOPSIS PDF IS: the release's own `synopsis` block (its url made absolute by the boot's
 * `locate`), set once by the app when the release is ready. The boxes the export ships (`rules[].book_box`) were cut from
 * THESE bytes, so the highlight is drawn on this copy and no other (the gov.bc.ca file is not byte-identical).
 * Null (no release block, e.g. the fixture release): `openBook` keeps opening the plain link.
 */
export interface SynopsisSource { readonly url: string; readonly sha256: string; readonly bytes: number }

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
