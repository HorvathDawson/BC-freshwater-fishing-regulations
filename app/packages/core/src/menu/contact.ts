/** The ONE contact value (app config), or a placeholder until the owner supplies it. */
import { MENU_WORDS } from "./words";

/**
 * (T860) The intro around its link to the report form: the words before it, the link's own, the words after — joined,
 * exactly `intro`. No link in the intro (the words changed): all of it in `before`, the link and `after` empty.
 */
export interface ContactIntroLink { readonly before: string; readonly link: string; readonly after: string }

export type ContactVM =
  | { readonly kind: "none"; readonly intro: string; readonly introLink: ContactIntroLink; readonly text: string }
  | { readonly kind: "email" | "link"; readonly intro: string; readonly introLink: ContactIntroLink; readonly label: string;
      readonly value: string; readonly url: string };

/** (T860) `intro` split at the first `link`. */
export function introLink(intro: string, link: string): ContactIntroLink {
  const at = link ? intro.indexOf(link) : -1;
  return at < 0 ? { before: intro, link: "", after: "" }
    : { before: intro.slice(0, at), link, after: intro.slice(at + link.length) };
}

/** The app's ONE contact value (config): an email address, a web link, or nothing yet (a placeholder, never a guess). */
export function contactVM(contact: string | null | undefined): ContactVM {
  const w = MENU_WORDS.contact;
  const c = (contact ?? "").trim();
  // (T860) "Email me" only where there is an address to email
  const intro = (t: string) => ({ intro: t, introLink: introLink(t, w.report) });
  if (/^https?:\/\/\S+$/i.test(c)) return { kind: "link", ...intro(w.introNoEmail), label: w.link, value: c, url: c };
  if (/^[^\s@/:]+@[^\s@/:]+\.[^\s@/:]+$/.test(c))
    return { kind: "email", ...intro(w.intro), label: w.email, value: c, url: `mailto:${c}` };
  return { kind: "none", ...intro(w.introNoEmail), text: w.placeholder };
}
