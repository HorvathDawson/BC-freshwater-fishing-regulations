/**
 * (T901) "Join the test" for a store whose page is not public yet: what to send, and where. The words are
 * `WEB_SPOTS_WORDS.join`; the address is the app's ONE contact value (More → Contact, `contactVM`).
 */
import { contactVM } from "./menu/contact";
import { mailtoUrl } from "./menu/report";
import { WEB_SPOTS_WORDS } from "./spotWords";
import { APP_STORE_PUBLIC, PLAY_PUBLIC } from "./storeLinks";

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

/** The stores whose test can still be asked for: those whose page is not public. */
export function joinStores(play: boolean = PLAY_PUBLIC, appStore: boolean = APP_STORE_PUBLIC): JoinStore[] {
  return [...(play ? [] : ["android" as const]), ...(appStore ? [] : ["iphone" as const])];
}
