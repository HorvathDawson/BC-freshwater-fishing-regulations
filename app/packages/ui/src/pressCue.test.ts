/**
 * (T716) THE TAP'S CUE: lit at the lift, held through the click, out when no click follows or nothing opens.
 */
import { describe, expect, it } from "vitest";
import { CUE_WAIT_MS, pressCue, pressCueStore } from "./pressCue";

/** A cue over a hand-turned clock: `lit` is every section it was told to light, in order (null: out). */
function bench() {
  const lit: (string | number | null)[] = [];
  let at = 0;
  const due: { at: number; f: () => void; id: number }[] = [];
  let n = 0;
  const cue = pressCue((id) => lit.push(id), {
    set: (f, ms) => { const id = ++n; due.push({ at: at + ms, f, id }); return id; },
    clear: (id) => { const i = due.findIndex((d) => d.id === id); if (i >= 0) due.splice(i, 1); },
  });
  const pass = (ms: number) => {
    at += ms;
    for (const d of [...due].filter((x) => x.at <= at)) { due.splice(due.indexOf(d), 1); d.f(); }
  };
  return { cue, lit, pass };
}

describe("pressCue", () => {
  it("a tap: lit at the lift, still lit after the click however late, out only when told", () => {
    const b = bench();
    b.cue.start(298192);
    b.pass(300);
    b.cue.confirm(298192);
    b.pass(CUE_WAIT_MS * 3);
    expect(b.lit).toEqual([298192, 298192]);
    b.cue.drop();
    expect(b.lit).toEqual([298192, 298192, null]);
  });

  it("a double tap (a zoom): lit at the first lift, out by itself when no click follows", () => {
    const b = bench();
    b.cue.start(298192);
    b.pass(CUE_WAIT_MS - 1);
    expect(b.lit).toEqual([298192]);
    b.pass(1);
    expect(b.lit).toEqual([298192, null]);
  });

  it("a click with no lift before it (the web, or a lift that found nothing) still lights the tapped section", () => {
    const b = bench();
    b.cue.confirm(316633);
    expect(b.lit).toEqual([316633]);
  });

  it("a second tap moves the cue; the first one's wait never puts the second out", () => {
    const b = bench();
    b.cue.start(1);
    b.pass(CUE_WAIT_MS - 100);
    b.cue.start(2);
    b.pass(150);
    expect(b.lit).toEqual([1, 2]);
    b.cue.confirm(2);
    b.pass(CUE_WAIT_MS * 2);
    expect(b.lit).toEqual([1, 2, 2]);
  });

  it("MUTATION PIN: a cue whose click did not stop the lift's wait goes out under an open still in flight", () => {
    const lit: (string | number | null)[] = [];
    const fs: (() => void)[] = [];
    const cue = pressCue((id) => lit.push(id), { set: (f) => { fs.push(f); return fs.length; }, clear: () => {} });
    cue.start(5);
    cue.confirm(5);
    for (const f of fs) f();
    expect(lit).toEqual([5, 5, null]);    // what the real `clear` prevents (the first test)
  });

  it("the store: one list per section, its readers told only when what is lit changes", () => {
    const fs: (() => void)[] = [];
    const store = pressCueStore({ set: (f) => { fs.push(f); return fs.length; }, clear: () => {} });
    let told = 0;
    const off = store.subscribe(() => { told += 1; });
    const empty = store.lit();
    store.start(7);
    const first = store.lit();
    store.confirm(7);                          // the same section: no new list, nobody told
    expect([empty, first, store.lit() === first, told]).toEqual([[], [7], true, 1]);
    store.drop();
    off();
    store.start(8);
    expect([store.lit(), told]).toEqual([[8], 2]);
  });
});

describe("(T716) tapAhead: the open starts at the lift, is shown at the click, is taken back without one", () => {
  async function bed(answer: boolean | Error = true) {
    const said: string[] = [];
    const fs: (() => void)[] = [];
    let open = false;
    let release: (() => void) | null = null;
    const cue = pressCue((id) => said.push(`lit ${id}`), { set: () => 0, clear: () => {} });
    const { tapAhead } = await import("./pressCue");
    const t = tapAhead({
      cue,
      open: (sid) => new Promise<boolean>((ok, no) => {
        said.push(`open ${sid}`);
        release = () => { if (answer instanceof Error) no(answer); else { open = answer; ok(answer); } };
      }),
      dropLookup: () => said.push("cancel"),
      back: () => { said.push("back"); open = false; },
      stillOpen: () => open,
    }, { set: (f) => { fs.push(f); return fs.length; }, clear: (i) => { fs[(i as number) - 1] = () => {}; } });
    const settle = async () => { release?.(); await new Promise((r) => setTimeout(r, 0)); };
    return { t, said, fire: () => { for (const f of fs.splice(0)) f(); }, settle, isOpen: () => open };
  }

  it("lift then click: one open, made at the lift; held until the click, then shown", async () => {
    const b = await bed();
    b.t.lift(5, null, "regs");
    const waiting = b.t.awaiting();
    await b.settle();
    const ok = await b.t.click(5, null, "regs");
    expect([waiting, b.t.awaiting(), ok, b.said]).toEqual([true, false, true, ["lit 5", "open 5", "lit 5"]]);
  });

  it("a lift with no click (a double tap): the open made is undone, the cue out — nothing else", async () => {
    const b = await bed();
    b.t.lift(5, null, "regs");
    await b.settle();
    b.fire();
    expect([b.t.awaiting(), b.isOpen(), b.said]).toEqual([false, false, ["lit 5", "open 5", "cancel", "back", "lit null"]]);
  });

  it("a lift whose lookup is still out when the wait ends: dropped (cancel), nothing to undo", async () => {
    const b = await bed();
    b.t.lift(5, null, "regs");
    b.fire();
    expect(b.said).toEqual(["lit 5", "open 5", "cancel", "lit null"]);
  });

  it("a click with no lift (iOS, the web): opened at the click", async () => {
    const b = await bed();
    const p = b.t.click(9, null, "regs");
    await b.settle();
    expect([await p, b.said]).toEqual([true, ["lit 9", "open 9"]]);
  });

  it("a failed lookup reaches the click that waits for it (reported there, as before), the cue out", async () => {
    const b = await bed(new Error("store gone"));
    b.t.lift(5, null, "regs");
    const p = expect(b.t.click(5, null, "regs")).rejects.toThrow("store gone");
    await b.settle();
    await p;
  });

  it("the reader moved on before the wait ended (the route is no longer this tap's open): nothing undone", async () => {
    const { tapAhead } = await import("./pressCue");
    const said: string[] = [];
    const fs: (() => void)[] = [];
    const t = tapAhead({ cue: pressCue(() => {}, { set: () => 0, clear: () => {} }), open: async () => true,
                         dropLookup: () => said.push("cancel"), back: () => said.push("back"), stillOpen: () => false },
                       { set: (f) => { fs.push(f); return 0; }, clear: () => {} });
    t.lift(5, null, "regs");
    await new Promise((r) => setTimeout(r, 0));
    for (const f of fs) f();
    expect(said).toEqual(["cancel"]);
  });

});
