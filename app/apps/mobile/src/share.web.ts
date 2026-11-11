/**
 * (T385, owner 2026-11-04 "web and app") A water's link in a browser: `navigator.share` where the browser has it (phone
 * browsers), else the link copied to the clipboard ("Link copied"). The same link either way. A share sheet closed
 * without sending rejects with AbortError: that is the reader's choice, "dismissed", never a failure.
 */
import type { SharePort } from "@app/ui";
import { slugOf } from "./waterSlugs";

export const share: SharePort | null = typeof navigator === "undefined" ? null : {
  slugOf,
  send: async ({ url, title }) => {
    if (typeof navigator.share === "function") {
      try {
        await navigator.share({ url, title });
        return "sent";
      } catch (e) {
        if (e instanceof Error && e.name === "AbortError") return "dismissed";
        throw e;
      }
    }
    if (!navigator.clipboard) throw new Error("share: this browser has neither a share sheet nor a clipboard");
    await navigator.clipboard.writeText(url);
    return "copied";
  },
};
