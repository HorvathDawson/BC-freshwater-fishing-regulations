/**
 * (T423) The update-channel switch in an installed build: the picked channel shows at once as "applies when you reopen
 * the app" (the running one stays what it is), the kept choice is read when the page opens, and a write that fails is
 * shown under the switch and reported (no-silent-fallbacks).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { setFailureSink, UPDATE_LOG_WORDS } from "@app/core";
import { createUpdateLog } from "@app/data";
import { LIGHT } from "../theme";
import { UpdatesLogPage, type UpdatesLogControls } from "./UpdatesLogPage";

afterEach(cleanup);

function controls(channel: UpdatesLogControls["channel"]): UpdatesLogControls {
  const snap = { checking: false, manual: null, feeds: [], release: null };
  const updates = { subscribe: () => () => {}, snapshot: () => snap };
  return {
    log: createUpdateLog(), updates: updates as never,
    header: () => ({ version: "0.6.2", platform: "android", env: "staging", origin: "https://x.y", channel: "live", release: null }),
    channel, offline: { get: () => false, set() {} }, forceRedownload: async () => {}, clearCache: async () => {},
    apply() {}, copy: async () => true,
  };
}
const text = (id: string) => screen.getByTestId(id).textContent;

describe("T423: the update-channel switch says what it did", () => {
  it("a pick shows at once as pending, and the kept choice shows when the page opens", () => {
    const picked: string[] = [];
    const { unmount } = render(<UpdatesLogPage palette={LIGHT} c={controls({ current: "live", stored: null, pick: (k) => { picked.push(k); } })} />);
    expect(screen.queryByTestId("updates-log-channel-pending")).toBeNull();
    fireEvent.click(screen.getByTestId("updates-log-channel-test"));
    expect([picked, text("updates-log-channel-pending")])
      .toEqual([["test"], UPDATE_LOG_WORDS.channel.pending(UPDATE_LOG_WORDS.channel.test)]);
    fireEvent.click(screen.getByTestId("updates-log-channel-live"));
    expect(screen.queryByTestId("updates-log-channel-pending")).toBeNull();
    unmount();
    render(<UpdatesLogPage palette={LIGHT} c={controls({ current: "live", stored: "test", pick() {} })} />);
    expect(text("updates-log-channel-pending")).toBe(UPDATE_LOG_WORDS.channel.pending(UPDATE_LOG_WORDS.channel.test));
  });

  it("a write that fails is shown under the switch and reported; the choice does not move", () => {
    const seen: unknown[] = [];
    const off = setFailureSink((e) => seen.push(e));
    const boom = new Error("disk full");
    render(<UpdatesLogPage palette={LIGHT} c={controls({ current: "live", stored: null, pick: () => { throw boom; } })} />);
    fireEvent.click(screen.getByTestId("updates-log-channel-test"));
    expect([text("updates-log-channel-failed"), seen, screen.queryByTestId("updates-log-channel-pending")])
      .toEqual([UPDATE_LOG_WORDS.channel.failed(UPDATE_LOG_WORDS.channel.test), [boom], null]);
    off();
  });
});
