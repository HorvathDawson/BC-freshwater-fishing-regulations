// (T554 item 2) Gauge alert rules are the reader's own: an unreadable notify.json is said (a note on More →
// Notifications), reported, and kept aside before anything writes over it — as the spots file is.
import { afterEach, describe, expect, it } from "vitest";
import { NOTIFY_WORDS, setFailureSink } from "@app/core";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyText } from "./settings";

let undo: (() => void) | null = null;
afterEach(() => { undo?.(); undo = null; });

function store(text: string | null, keep = true) {
  const kept: string[] = [];
  let t = text;
  const s: NotifyText = { read: () => t, write: (x) => { t = x; }, ...(keep ? { keepAside: (x: string) => { kept.push(x); } } : {}) };
  return { s, kept, now: () => t };
}

describe("notify.json that cannot be read (T554 item 2)", () => {
  const note = [{ line: NOTIFY_WORDS.refused.fileLine, why: NOTIFY_WORDS.refused.file }];

  it("torn, not JSON, or a version this app does not know: nothing on, the note shown, reported", () => {
    const got: string[] = [];
    undo = setFailureSink((e) => got.push(String(e)));
    for (const text of ["", "{\"v\":3,\"rules\":[", "{\"v\":9}"]) {
      const r = createNotifySettings(store(text).s).read();
      expect([r.on, r.rules, r.refused]).toEqual([false, [], note]);
    }
    expect(got.length).toBe(3);
    expect(createNotifySettings(store(null).s).read()).toBe(NOTIFY_DEFAULT);     // never written: no note
  });

  it("is kept aside before the first write over it; the note is then saved with the new state", () => {
    const st = store("{\"v\":3,\"rules\":[");
    const n = createNotifySettings(st.s);
    n.update((s) => ({ ...s, on: true }));
    expect(st.kept).toEqual(["{\"v\":3,\"rules\":["]);
    expect(JSON.parse(st.now()!)).toMatchObject({ on: true, refused: note });
    n.update((s) => ({ ...s, on: false }));
    expect(st.kept).toHaveLength(1);                                             // read now: nothing more kept
  });

  it("a store that cannot keep it aside does not write over it", () => {
    const st = store("not json", false);
    expect(() => createNotifySettings(st.s).update((s) => ({ ...s, on: true }))).toThrow(/not overwritten/);
    expect(st.now()).toBe("not json");
  });
});
