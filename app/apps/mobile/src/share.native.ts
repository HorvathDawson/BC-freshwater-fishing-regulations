/**
 * (T385) A water's link, sent through the phone's share sheet (React Native `Share`, no dependency). iOS takes the link
 * as `url`; Android's chooser takes text (`message`). A sheet closed without sending resolves "dismissed" (iOS says so;
 * Android cannot tell, and resolves "sent") — the reader's choice, never a failure.
 */
import { Platform, Share } from "react-native";
import { SITE_ORIGIN } from "@app/core";
import { slugReader, type SharePort } from "@app/ui";

/** (T624) the phone has no site of its own: a water's slug is read from the production site's slug shards (the same files the web reads). */
const slugs = slugReader(SITE_ORIGIN);

export const share: SharePort | null = {
  slugOf: slugs.of,
  send: async ({ url, title }) => {
    const r = await Share.share(Platform.OS === "ios" ? { url, title } : { message: url, title }, { dialogTitle: title });
    return r.action === Share.dismissedAction ? "dismissed" : "sent";
  },
};
