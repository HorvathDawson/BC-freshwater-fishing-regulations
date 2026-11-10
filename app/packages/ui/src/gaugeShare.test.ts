// @vitest-environment jsdom
/**
 * (T530) SHARE A GAUGE: the link is the gauge's public page (core `gaugeUrl`, the SEO build's own function); no control for a
 * station without a page; a press sends through the one share helper; a failure is said and reported.
 */
import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { GAUGE_SHARE_WORDS, SHARE_WORDS, SITE_ORIGIN, setFailureSink } from "@app/core";
import { useGaugeShare, type GaugePageCheck, type SharePort } from "./share";

const T = { station: "08MH001", name: "Chilliwack River at Vedder Crossing" };
const yes = async () => true;
const mount = (port: SharePort | null, target: Parameters<typeof useGaugeShare>[1] = T, has: GaugePageCheck | null = yes) =>
  renderHook(() => useGaugeShare(port, target, has));
/** the control, once the page check has answered */
const ready = async (r: ReturnType<typeof mount>) => { await waitFor(() => expect(r.result.current).not.toBeNull()); return r.result; };

describe("(T530) Share on a gauge", () => {
  it("sends the gauge page's address on the site, named for the gauge", async () => {
    const send = vi.fn(async () => "sent" as const);
    const result = await ready(mount({ send }));
    expect(result.current!.label).toBe(GAUGE_SHARE_WORDS.label);
    await act(async () => { result.current!.press(); });
    expect(send).toHaveBeenCalledWith({ url: `${SITE_ORIGIN}/gauge/08MH001/chilliwack-river-at-vedder-crossing`, title: SHARE_WORDS.title });
  });

  it("no control without a page (the check says no, or fails and is reported), without a name, or without a platform share", async () => {
    const ok: SharePort = { send: async () => "sent" };
    const sink = vi.fn(), undo = setFailureSink(sink);
    const boom = new Error("check failed");
    const nos = [mount(ok, T, async () => false), mount(ok, T, async () => { throw boom; }), mount(ok, { ...T, name: null }),
                 mount(ok, { ...T, station: null }), mount(ok, null), mount(null), mount(ok, T, null)];
    await waitFor(() => expect(sink).toHaveBeenCalledWith(boom));
    await new Promise((r) => setTimeout(r, 20));
    expect(nos.map((r) => r.result.current)).toEqual(nos.map(() => null));
    undo();
  });

  it("copied says so; a dismissed sheet says and reports nothing", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    const copied = mount({ send: async () => "copied" });
    await ready(copied); await act(async () => { copied.result.current!.press(); });
    await waitFor(() => expect(copied.result.current!.said).toBe(SHARE_WORDS.copied));
    const abort = Object.assign(new Error("Share canceled"), { name: "AbortError" });
    const gone = mount({ send: async () => { throw abort; } });
    await ready(gone); await act(async () => { gone.result.current!.press(); });
    await waitFor(() => expect(gone.result.current!.busy).toBe(false));
    expect([gone.result.current!.said, sink.mock.calls.length]).toEqual([null, 0]);
    undo();
  });

  it("a real failure is said under the control and reported", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    const boom = new Error("share: this browser has neither a share sheet nor a clipboard");
    const result = await ready(mount({ send: async () => { throw boom; } }));
    await act(async () => { result.current!.press(); });
    await waitFor(() => expect(result.current!.said).toBe(SHARE_WORDS.failed));
    expect(sink).toHaveBeenCalledWith(boom);
    undo();
  });
});
