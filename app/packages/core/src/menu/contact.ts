/** The ONE contact value (app config), or a placeholder until the owner supplies it. */
import { MENU_WORDS } from "./words";

export type ContactVM =
  | { readonly kind: "none"; readonly intro: string; readonly text: string }
  | { readonly kind: "email" | "link"; readonly intro: string; readonly label: string; readonly value: string; readonly url: string };

/** The app's ONE contact value (config): an email address, a web link, or nothing yet (a placeholder, never a guess). */
export function contactVM(contact: string | null | undefined): ContactVM {
  const w = MENU_WORDS.contact;
  const c = (contact ?? "").trim();
  if (/^https?:\/\/\S+$/i.test(c)) return { kind: "link", intro: w.intro, label: w.link, value: c, url: c };
  if (/^[^\s@/:]+@[^\s@/:]+\.[^\s@/:]+$/.test(c)) return { kind: "email", intro: w.intro, label: w.email, value: c, url: `mailto:${c}` };
  return { kind: "none", intro: w.intro, text: w.placeholder };
}
