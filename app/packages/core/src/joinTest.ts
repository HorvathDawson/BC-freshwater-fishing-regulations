/**
 * (T901) "Join the test" for a store whose page is not public yet: what to send, and where. The words are
 * `WEB_SPOTS_WORDS.join`; the address is the app's ONE contact value (More → Contact, `contactVM`).
 */
import { contactVM } from "./menu/contact";
import { mailtoUrl } from "./menu/report";
import { WEB_SPOTS_WORDS } from "./spotWords";

export type JoinStore = "android" | "iphone";
export interface JoinAsk { readonly subject: string; readonly body: string; readonly button: string;
                           /** an email to the contact address; null where none is set (the app's Report form is the way then) */
                           readonly mailto: string | null }

export function joinTestAsk(store: JoinStore, contact: string | null | undefined): JoinAsk {
  const j = WEB_SPOTS_WORDS.join[store];
  const to = contactVM(contact);
  return { subject: j.subject, body: j.body, button: j.button,
           mailto: to.kind === "email" ? mailtoUrl(to.value, { subject: j.subject, body: j.body }) : null };
}

/** One of the two places a store gets in the "Get the app" block: its store link once public, else its "Join the test". */
export type GetAppSlot =
  | { readonly kind: "store"; readonly store: JoinStore; readonly label: string; readonly url: string }
  | { readonly kind: "join"; readonly store: JoinStore; readonly label: string; readonly ask: JoinAsk };

/**
 * (T901, owner 2026-11-19) THE "GET THE APP" BLOCK, decided once for the app and the website: a plain line naming only the
 * stores still coming (`coming`), a small lead (`lead`, only above join buttons), and per platform one button in the same
 * spot, Android then iPhone: the store's link once its page is public, else "Join the test" where there is a way to act on
 * it (an email to the contact, or the Report form: `canReport`). No way to act: no button (never a dead one).
 */
export function getAppBlock(play: string | null, appStore: string | null, contact: string | null | undefined, canReport: boolean):
  { coming: string | null; lead: string | null; slots: GetAppSlot[] } {
  const W = WEB_SPOTS_WORDS;
  const slots: GetAppSlot[] = [];
  for (const [store, url, label] of [["android", play, W.playButton], ["iphone", appStore, W.appStoreButton]] as const) {
    if (url !== null) { slots.push({ kind: "store", store, label, url }); continue; }
    const ask = joinTestAsk(store, contact);
    if (ask.mailto !== null || canReport) slots.push({ kind: "join", store, label: ask.button, ask });
  }
  const coming = play === null && appStore === null ? W.coming.both : play === null ? W.coming.play : appStore === null ? W.coming.appStore : null;
  return { coming, lead: slots.some((s) => s.kind === "join") ? W.join.lead : null, slots };
}
