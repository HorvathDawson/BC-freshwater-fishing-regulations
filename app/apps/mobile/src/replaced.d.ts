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
  /** the manifest of the set in use, or null before the boot */
  opened: () => import("@app/data").Manifest | null;
  fetch: typeof fetch;
}): () => void;

/** The code notice's action: the page loaded again (the web); a phone never raises it. */
export declare function reloadPage(): void;
