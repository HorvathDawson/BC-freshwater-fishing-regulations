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
