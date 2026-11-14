/**
 * (T368) THE ONE WAY A LINK LEAVES THE APP — a web page, the rule book at gov.bc.ca, a mail draft, directions, the store.
 * `Linking.openURL` rejects when nothing on the device takes the link; that was `void`ed at a dozen taps, so a failed open
 * did nothing and (before the phone's rejection tracker) told no one. Here it is logged (the Updates log) and REPORTED.
 */
import { Linking, Platform } from "react-native";
import { directionsUrl, linkFailedLine } from "@app/core";
import { reportFailure } from "@app/ui";

export function openLink(url: string): Promise<void> {
  return Linking.openURL(url).then(() => undefined, (e: unknown) => {
    console.warn(`openLink: could not open ${url.split("?")[0]}`, e);
    reportFailure(e);
    // (T723 G9) and SHOWN: the app says what did not open (core's `linkFailedLine`), where the reader is looking
    shown?.(linkFailedLine(url));
  });
}

/** Where a refused open is said (the app's toast); set once by the app, `null` clears it. Returns the undo. */
let shown: ((line: string) => void) | null = null;
export function setLinkFailedSink(f: ((line: string) => void) | null): () => void {
  const prev = shown;
  shown = f;
  return () => { shown = prev; };
}

/**
 * (T390) The same open for a caller that SHOWS its own failure (the bell's Update button: "The update could not
 * start"): a refused open is logged and handed back — the caller says it and reports it (`@app/ui` `useAppUpdate`).
 */
export function openLinkShown(url: string): Promise<void> {
  return Linking.openURL(url).then(() => undefined, (e: unknown) => {
    console.warn(`openLink: could not open ${url.split("?")[0]}`, e);
    throw e;
  });
}

/**
 * (T723 G8) Directions to a point in the phone's own maps app: Apple Maps on an iPhone, Google Maps elsewhere (core's
 * `directionsUrl`). `os` defaults to the platform's, by `Platform.select` (a one-value choice), and tests pass it.
 */
export function openDirections(q: { lat: number; lon: number },
                               os: string = Platform.select({ ios: "ios", default: "other" })): Promise<void> {
  return openLink(directionsUrl(os, q.lat, q.lon));
}

/** `onPress={linkTo(url)}`: a tap that opens `url` through `openLink`. */
export const linkTo = (url: string) => () => { void openLink(url); };
