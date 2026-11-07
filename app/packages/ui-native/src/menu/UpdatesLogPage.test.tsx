/**
 * (T423) The update-channel switch in an installed build: the picked channel shows at once as "applies when you reopen
 * the app" (the running one stays what it is), the kept choice is read when the page opens, and a write that fails is
 * shown under the switch and reported (no-silent-fallbacks). (T242) The crash-test row: drawn only when the shell hands
 * it levers (a tool's build), each button fires its own.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { setFailureSink, UPDATE_LOG_WORDS } from "@app/core";
import { createUpdateLog } from "@app/data";
import { LIGHT } from "../theme";
import { ChannelPicker, UpdatesLogPage, type UpdatesLogControls } from "./UpdatesLogPage";

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
    const { unmount } = render(<ChannelPicker palette={LIGHT} c={controls({ current: "live", stored: null, pick: (k) => { picked.push(k); } })} />);
    expect(screen.queryByTestId("updates-log-channel-pending")).toBeNull();
    fireEvent.click(screen.getByTestId("updates-log-channel-test"));
    expect([picked, text("updates-log-channel-pending")])
      .toEqual([["test"], UPDATE_LOG_WORDS.channel.pending(UPDATE_LOG_WORDS.channel.test)]);
    fireEvent.click(screen.getByTestId("updates-log-channel-live"));
    expect(screen.queryByTestId("updates-log-channel-pending")).toBeNull();
    unmount();
    render(<ChannelPicker palette={LIGHT} c={controls({ current: "live", stored: "test", pick() {} })} />);
    expect(text("updates-log-channel-pending")).toBe(UPDATE_LOG_WORDS.channel.pending(UPDATE_LOG_WORDS.channel.test));
  });

  it("a write that fails is shown under the switch and reported; the choice does not move", () => {
    const seen: unknown[] = [];
    const off = setFailureSink((e) => seen.push(e));
    const boom = new Error("disk full");
    render(<ChannelPicker palette={LIGHT} c={controls({ current: "live", stored: null, pick: () => { throw boom; } })} />);
    fireEvent.click(screen.getByTestId("updates-log-channel-test"));
    expect([text("updates-log-channel-failed"), seen, screen.queryByTestId("updates-log-channel-pending")])
      .toEqual([UPDATE_LOG_WORDS.channel.failed(UPDATE_LOG_WORDS.channel.test), [boom], null]);
    off();
  });
});

describe("Updates log: crash test row (T242)", () => {
  it("is drawn with its levers; each button fires its own", () => {
    const js = vi.fn(), promise = vi.fn();
    render(<UpdatesLogPage palette={LIGHT} c={{ ...controls(null), crashTest: { js, promise } }} />);
    expect(screen.getByText(UPDATE_LOG_WORDS.crashTest.label)).toBeTruthy();
    fireEvent.click(screen.getByTestId("updates-log-crash-js"));
    expect([js.mock.calls.length, promise.mock.calls.length]).toEqual([1, 0]);
    fireEvent.click(screen.getByTestId("updates-log-crash-promise"));
    expect([js.mock.calls.length, promise.mock.calls.length]).toEqual([1, 1]);
  });
  it("is not drawn without levers (every store build)", () => {
    render(<UpdatesLogPage palette={LIGHT} c={{ ...controls(null), crashTest: null }} />);
    expect(screen.queryByText(UPDATE_LOG_WORDS.crashTest.label)).toBeNull();
    expect(screen.queryByTestId("updates-log-crash-js")).toBeNull();
  });
});
