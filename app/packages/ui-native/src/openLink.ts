/**
 * (T368) THE ONE WAY A LINK LEAVES THE APP — a web page, the rule book at gov.bc.ca, a mail draft, directions, the store.
 * `Linking.openURL` rejects when nothing on the device takes the link; that was `void`ed at a dozen taps, so a failed open
 * did nothing and (before the phone's rejection tracker) told no one. Here it is logged (the Updates log) and REPORTED.
 */
import { Linking } from "react-native";
import { reportFailure } from "@app/ui";

export function openLink(url: string): Promise<void> {
  return Linking.openURL(url).then(() => undefined, (e: unknown) => {
    console.warn(`openLink: could not open ${url.split("?")[0]}`, e);
    reportFailure(e);
  });
}

/** `onPress={linkTo(url)}`: a tap that opens `url` through `openLink`. */
export const linkTo = (url: string) => () => { void openLink(url); };
