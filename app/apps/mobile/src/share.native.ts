/**
 * (T385) A water's link, sent through the phone's share sheet (React Native `Share`, no dependency). iOS takes the link
 * as `url`; Android's chooser takes text (`message`). A sheet closed without sending resolves "dismissed" (iOS says so;
 * Android cannot tell, and resolves "sent") — the reader's choice, never a failure.
 */
import { Platform, Share } from "react-native";
import type { SharePort } from "@app/ui";

export const share: SharePort | null = {
  send: async ({ url, title }) => {
    const r = await Share.share(Platform.OS === "ios" ? { url, title } : { message: url, title }, { dialogTitle: title });
    return r.action === Share.dismissedAction ? "dismissed" : "sent";
  },
};
