import type { ReplacedState } from "@app/ui";

/**
 * (T1016) A PAGE OLDER THAN WHAT THE SERVER SERVES. `replaced.web.ts`: a browser tab kept open across releases (or a
 * site deploy) meets 404s on files production no longer serves; this installs the failure triage that tells that known
 * state from a defect (`@app/ui` `replacedTriage`, over the live pointer and the site's own entry) and reloads the page
 * for the code notice. `replaced.native.ts`: nothing to install — a phone holds its release whole on the device (every
 * read is from its disk; an update is staged beside it) and its code is in the binary, so neither 404 can happen there.
 * Bundlers pick by platform extension; TypeScript does not, so both are checked against this one signature.
 */
export declare function installReplaced(o: {
  state: ReplacedState;
  manifestUrl: string;
  /** the release root every manifest path is under (`ENDPOINTS.release`) */
  releaseBase: string;
  /** every manifest this page has opened, the one in use last (none before the boot): a read made on a set an apply
   *  has since replaced is judged against that set */
  opened: () => readonly import("@app/data").Manifest[];
  fetch: typeof fetch;
}): () => void;

/** The code notice's action: the page loaded again (the web); a phone never raises it. */
export declare function reloadPage(): void;

/**
 * (T1016, owner 2026-11-24 "Always auto refresh") The code verdict's AUTOMATIC reload, at most once per page code
 * (`@app/ui` `reloadOnce` over the browser's `sessionStorage`, which survives the reload): null — reloading now; else why
 * not (it would loop, or the marker cannot be kept), and the notice shows that. A phone never raises it.
 */
export declare function autoReload(): string | null;
