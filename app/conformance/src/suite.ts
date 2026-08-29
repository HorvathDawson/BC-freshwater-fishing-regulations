/**
 * ONE suite, run against EVERY RegsSource implementation.
 *
 * This is the file that keeps mobile and web answering the same question the same
 * way. When a query is added to the RegsSource interface, it is added here in the
 * same commit — otherwise an implementation can diverge without any test noticing,
 * which is exactly how the previous clients drifted.
 */
import { expect, it } from "vitest";
import type { ItemId, RegsSource } from "@app/data";

export function runConformance(name: string, make: () => Promise<RegsSource>) {
  it(`${name}: reports a bundle version`, async () => {
    const s = await make();
    expect((await s.info()).version).toBeTruthy();
  });

  it(`${name}: a missing item is absent, never a silent empty answer`, async () => {
    const s = await make();
    expect(await s.itemExists("gnis:does-not-exist" as ItemId)).toBe(false);
  });
}
