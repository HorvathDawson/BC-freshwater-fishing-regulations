/**
 * The shared declaration for `./source` (P11): where the loader gets a release file's RAW, verified bytes.
 *
 *   source.web.ts      the cache (by immutable url), else `fetchVerified` from `${base}${url}` and into the cache. A
 *                      cached file is re-verified (length + sha256) and refetched when it is not the manifest's.
 *   source.native.ts   the installed copy (`install` verified it on the way in), else `fetchVerified`.
 */
import type { ReleaseSource } from "../loader";
import type { Held, Manifest } from "../../manifest";
import type { ByteCache } from "./cache";
import type { Net } from "../../net";

export interface SourceAt {
  /** The release root: every manifest url is relative to it (`ENDPOINTS.release`). */
  base: string;
  cache: ByteCache;
  /** The installed set (native) whose files are read from disk; null reads over the network. */
  held: Held | null;
  fetch?: typeof fetch;
  /**
   * (T254) The app's bad-connection kit (web): each network fetch takes its turn (`gate`: the answer first in
   * low-bandwidth mode), survives cuts (timeouts, retries, resume) and says how far it got (`activity`). Absent: a plain
   * verified fetch, as before.
   */
  net?: Net | null;
}

export declare function releaseSource(m: Manifest, at: SourceAt): ReleaseSource;
