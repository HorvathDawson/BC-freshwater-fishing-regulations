/**
 * THE NATIVE SOURCE (P11): a release file's bytes from the installed set — the phone works with no network
 * (MINIMAL-PACKAGING §5). Only `paint` and `flow-area` are read as bytes; the SQLite files open in place
 * (`open.native.ts`). A file of a release that is not the installed one is fetched and verified. See `source.d.ts`.
 */
import { File, Paths } from "expo-file-system";
import { fetchVerified } from "../../fetchVerified";
import type { Manifest } from "../../manifest";
import { heldPath } from "../install";
import type { ReleaseSource } from "../loader";
import type { SourceAt } from "./source";

export type { SourceAt } from "./source";

export function releaseSource(m: Manifest, at: SourceAt): ReleaseSource {
  return {
    async bytes(name) {
      const spec = m.files[name];
      if (!spec) throw new Error(`release ${m.data_release} has no file ${name}`);
      if (at.held?.release === m.data_release) {
        const f = new File(Paths.document, heldPath(at.held, spec.url));
        if (f.exists) return f.bytes();
      }
      return fetchVerified(`${at.base}${spec.url}`, spec, undefined, at.fetch ?? fetch);
    },
  };
}
