/**
 * (T242) THE CRASH STUB IS STARTED BY THE SCENARIO THAT NEEDS IT. A stub APK (`session.sh --crash-stub`) sends its reports
 * to the phone's localhost, `adb reverse`d here; session.sh starts no server. The `crash` scenario never started one, so on
 * a stub APK its exit record met a socket adb accepted and closed — the app said "unexpected end of stream" and the step
 * failed (2026-11-16, both builds), while `crash-bed` (which starts the stub) passed.
 *
 * Pinned here: (1) the app's own exit-record send (`createCrashReporter().sendExit` → `exitPayload`, an `am crash` record)
 * is answered 201 by the stub `lib/crash-stub.mjs` starts and lands under crashes-test/; (2) a dead port fails the start
 * naming the stub, not the app; (3) every Android scenario that waits on the app's crash reports starts the stub.
 */
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { createCrashReporter, type CrashSetting } from "@app/ui";
// @ts-expect-error plain .mjs, no types
import { startCrashStub } from "./testbed/lib/crash-stub.mjs";

const freePort = () => new Promise<number>((ok, no) => {
  const s = net.createServer().once("error", no).listen(0, () => { const p = (s.address() as net.AddressInfo).port; s.close(() => ok(p)); });
});
const on: CrashSetting = { get: () => true, set: () => {}, subscribe: () => () => {} };

describe("the crash stub (test bed)", () => {
  it("answers the app's exit record (am crash) 201 and stores it under crashes-test/", async () => {
    const port = await freePort();
    const stub = await startCrashStub({ dir: mkdtempSync(join(tmpdir(), "crash-stub-")), port });
    try {
      const reporter = createCrashReporter({
        url: `http://localhost:${port}/crash`, setting: on, online: () => true, test: true,
        context: () => ({ route: "map", water: null, view: null, part: null, whole: false, sheet: null,
                          app: "0.7.11", env: "staging", platform: "android", release: "r-000000000000", install: "0123456789abcdef" }),
      });
      const a = await reporter.sendExit({ timestamp: Date.parse("2026-11-16T18:33:43.678Z"), reason: "CRASH", importance: "FOREGROUND",
                                          status: 0, rssKb: 0, description: "crash" });
      // the answer as this branch's reporter words it: `true`, or (merge/held-group1) the server's answer
      expect([true, { sent: true, status: 201, error: null }]).toContainEqual(a);
      const posts = stub.posts();
      expect(posts.map((p: { status: number; body: { kind: string; name: string; message: string } }) =>
        `${p.status} ${p.body.kind} ${p.body.name}: ${p.body.message}`)).toEqual(["201 native NativeExit: CRASH while foreground: crash"]);
      const kinds = await stub.kinds();
      expect(kinds.map((k: { key: string }) => k.key.replace(/[0-9a-f]{16}/, "<hash>"))).toEqual(["private/crashes-test/<hash>.json"]);
    } finally { stub.stop(); }
  }, 30_000);

  it("a stub that cannot start fails naming the stub, never as the app's answer", async () => {
    const port = await freePort();
    const holder = net.createServer().listen(port);                   // the port is taken: the server exits at once
    try {
      await expect(startCrashStub({ dir: mkdtempSync(join(tmpdir(), "crash-stub-")), port, ms: 5000 }))
        .rejects.toThrow(new RegExp(`the crash stub on :${port} (exited|did not answer)`));
    } finally { holder.close(); }
  }, 30_000);

  it("every Android scenario that waits on the app's crash reports starts the stub itself", () => {
    const dir = resolve(__dirname, "testbed/scenarios");
    const needs = readdirSync(dir).filter((n) => n.endsWith(".mjs")).filter((n) => {
      const t = readFileSync(join(dir, n), "utf8");
      return /\[crash-reports\]|TESTBED_STUB\b/.test(t);
    });
    expect(needs).toEqual(expect.arrayContaining(["crash.mjs", "crash-bed.mjs", "retire-swap.mjs"]));
    const missing = needs.filter((n) => !/\bstartCrashStub\(/.test(readFileSync(join(dir, n), "utf8")));
    expect(missing).toEqual([]);
  });
});
