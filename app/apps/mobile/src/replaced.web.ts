/**
 * (T1016) THE WEB: a tab kept open across two releases, or across a site deploy (see `replaced.d.ts`). Installs the
 * failure triage (core `setFailureTriage`) with the browser's two probes:
 *
 *   data   the live pointer (`createReplacedProbe`): a 404 on a file of a set it no longer names is the known state
 *   site   a code chunk that would not load (`AsyncRequireError`: the script tag says neither 404 nor why) is asked for
 *          again (`@app/ui` `siteProbe`), against this page's entry script and the site's own page (`/`): a 404 and an
 *          entry `/` no longer names is a deploy since this page opened; a 404 while `/` names it is a defect.
 */
import type { Manifest } from "@app/data";
import { createReplacedProbe } from "@app/data/release";
import { setFailureTriage } from "@app/core";
import { replacedTriage, siteProbe, type ReplacedState } from "@app/ui";

/** The entry script of this page (Expo's web export: `/_expo/static/js/web/<entry>-<hash>.js`), or null (a dev server). */
function entryScript(doc: Pick<Document, "querySelectorAll">): string | null {
  for (const s of Array.from(doc.querySelectorAll("script[src]"))) {
    const src = (s as HTMLScriptElement).src;
    if (/\/_expo\/static\/js\/web\/[^/]+\.js(\?|$)/.test(src)) return new URL(src).pathname;
  }
  return null;
}

export function installReplaced(o: { state: ReplacedState; manifestUrl: string; releaseBase: string;
                                     opened: () => Manifest | null; fetch: typeof fetch }): () => void {
  const data = createReplacedProbe({ manifestUrl: o.manifestUrl, releaseBase: o.releaseBase, opened: o.opened, fetch: o.fetch });
  // the site is our own page's origin, not the data origin: the page's own fetch, no app headers
  const site = siteProbe((u, i) => fetch(u, i), entryScript(document), new URL("/", window.location.href).href);
  return setFailureTriage(replacedTriage({ state: o.state, data, site, opened: () => o.opened()?.data_release ?? null }));
}

/** The code notice's action: the page loaded again — the new site's code, and with it the live release. */
export function reloadPage(): void { window.location.reload(); }
