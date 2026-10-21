/**
 * T18 parity: every golden g28 group's quota tree — each tier (kicker, line with its big numbers and fresh Rule book page,
 * relation, sub, origin) and each kind row (sentence, number, How to tell, size chip, qualifiers, pictures, also-counts,
 * duty with "Steelhead stamp ›", presence) — built by core `treeOf` over the state's inputs and projected to the golden's
 * shape (keepGroups.ts), against app/design/regs-v11/port/golden/golden.json (regs v38 @ 7fb6809f).
 *
 * Not compared: a kind's `lines` — they carry the prototype's licence stamp lines (round UR7 stampsFor), which decide
 * which document a fish needs; that is a pipeline answer this port does not derive (tree.ts header). Every other kind
 * field is compared, so a kind's words are still proved.
 */
import { afterAll, describe, expect, it } from "vitest";
import { plain, treeOf, type KindVM, type Line, type TierVM, type TreeKind, type TreeNodeVM, type TreeTier } from "@app/core";
import { caseIds, golden, inputsOf, parityGate, summary } from "./golden";
import { norm } from "./eq";
import { excluded } from "./exclusions";
import { goldenTree, keepOf, nodeProj, treeProj } from "./keepGroups";

const gate = await parityGate();
const ids = caseIds("keep");
type GGroup = { tree: unknown; treeRoot: string | null };
const goldenGroups = (id: string) => (golden().states[id]!.keep as { groups: GGroup[] }).groups;

it(`T18 tree parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

/** a tree against the store's own nodes: each tier's words (Rule book links aside) are TreeTier.says, each kind's sentence
 *  starts with TreeKind.says (a folded kind draws none) — the store, not this port, is the oracle */
function storeWalk(vm: readonly TreeNodeVM[], store: readonly (TreeTier | TreeKind)[], id: string): void {
  expect(vm.length, id).toBe(store.length);
  vm.forEach((x, i) => {
    const d = store[i]!;
    if (x.t === "tier" && d.node === "tier") {
      if (!x.folded) expect(norm(plain(x.line.filter((r) => r.t !== "book"))), `${id} ${d.id}`).toBe(norm(d.says));
      storeWalk(x.children, d.children, id);
    } else if (x.t === "kind" && d.node === "kind") {
      if (d.folded) expect(x.says, id).toBeNull();
      else expect(norm(plain(x.says)).startsWith(norm(d.says)), `${id} ${d.says}`).toBe(true);
    } else expect.fail(`${id}: node ${i} is ${x.t}, the store's ${d.node}`);
  });
}

const walk = (ns: readonly TreeNodeVM[], f: (x: TreeNodeVM, path: readonly TierVM[]) => void, path: readonly TierVM[] = []) => {
  for (const x of ns) { f(x, path); if (x.t === "tier") walk(x.children, f, [...path, x]); }
};

describe.skipIf(gate.skip)("T18 quota tree", () => {
  let equal = 0, tiers = 0, kinds = 0;
  afterAll(() => console.info(`${summary("keep", equal, ids.length)} · ${tiers} tiers, ${kinds} kinds (tree)`));

  it.each(ids)("%s: every group's tree equals the golden (kind lines aside)", async (id) => {
    const k = keepOf((await inputsOf(id))!), gold = goldenGroups(id);
    expect(k.groups.length).toBe(gold.length);
    k.groups.forEach((g, i) => {
      const got = treeProj(g);
      if (excluded("keep", id, `groups.${i}.tree`)) {
        // the Kitimat ruling: held to the STORE's tree instead (verify_map.py's check — tier line = TreeTier.says, kind
        // sentence starts with TreeKind.says, folded kinds draw none), and the root stays the golden's
        expect(got.treeRoot).toBe(gold[i]!.treeRoot);
        storeWalk(treeOf(g.env) ?? [], g.ctx.hdrRoot ? g.ctx.hdrRoot.children : g.ctx.g.tree, id);
        return;
      }
      expect(got).toEqual({ tree: goldenTree(gold[i]!.tree), treeRoot: gold[i]!.treeRoot });
      JSON.stringify(got.tree, (key, v) => { if (key === "t" && v === "tier") tiers++; if (key === "t" && v === "kind") kinds++; return v; });
    });
    equal++;
  });

  const tree = async (id: string, i = 0) => keepOf((await inputsOf(id))!).groups[i]!;
  const find = (ns: readonly TreeNodeVM[], p: (x: TreeNodeVM) => boolean): TreeNodeVM | undefined => {
    let hit: TreeNodeVM | undefined;
    walk(ns, (x) => { if (!hit && p(x)) hit = x; });
    return hit;
  };
  const treeOfGroup = async (id: string, i = 0) => treeOf((await tree(id, i)).env)!;
  const tierById = async (id: string, tid: string, i = 0) => find(await treeOfGroup(id, i), (x) => x.t === "tier" && x.tid === tid) as TierVM;
  const kindNamed = async (id: string, name: string, i = 0) => find(await treeOfGroup(id, i), (x) => x.t === "kind" && x.kind === name) as KindVM;

  it("1. vedder_1010 share t3 folds into form a's lead: no kicker, no words; its origin keeps How to tell and p.21", async () => {
    const t = await tierById("vedder_1010", "t3");
    expect([t.folded, t.kicker, t.line, plain(t.origin!.says), t.origin!.howtell, plain(t.origin!.book)]).toEqual([true, null, [],
      "From streams, keep hatchery fish only — wild trout and char go back.", true, "Rule book p.21 ↗"]);
  });

  it("2. vedder_1010 size t4: both numbers big, the second a big2", async () => {
    const t = await tierById("vedder_1010", "t4");
    expect([plain(t.line), t.line.filter((r) => r.t === "num")]).toEqual(["Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead",
      [{ t: "num", s: "1", size: "big" }, { t: "num", s: "2", size: "big2" }]]);
  });

  it("3. vedder_1010 kinds under t4: the size_rule chip, the pictures, the steelhead's How to tell, duty and stamp link, presence", async () => {
    const b = await kindNamed("vedder_1010", "Brook trout, brown trout and cutthroat trout");
    expect([plain(b.says), { ...b.size, text: norm(b.size!.text) }, b.pictures.map((p) => (p.sil ? p.code : p.caption.replace(/\s/g, " ")))]).toEqual([
      "Up to 2 may be hatchery brook, brown or cutthroat trout", { text: "keep 30 cm (12 in) or longer; release the rest", rule: true },
      ["Brook trout ›", "Brown trout ›", "Coastal cutthroat trout ›"]]);
    const s = await kindNamed("vedder_1010", "Steelhead");
    expect([plain(s.says), s.size?.text.replace(/\s/g, " "), s.qualifiers, plain(s.duty), plain(s.presence)]).toEqual([
      "Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›", "over 50 cm (20 in)", "rainbow trout",
      "10 a year · Stop fishing the Vedder River after 2 a day · Record each on your licence right away Steelhead stamp ›",
      "A rainbow trout over 50 cm (20 in) here counts as a steelhead. ⓘ"]);
    expect(s.duty!.at(-1)).toEqual({ t: "link", s: "Steelhead stamp ›", open: { tab: "licence" } });
  });

  it("4. vedder_1010 own t2: the river's rule with its fresh page; its relation under it; the folded rainbow draws no sentence", async () => {
    const t = await tierById("vedder_1010", "t2");
    const rb = t.children[0] as KindVM;
    expect([plain(t.line), t.relation, rb.folded, rb.says, rb.size?.text.replace(/\s/g, " ")]).toEqual([
      "This river’s rule: up to 4 hatchery rainbow trout a day Rule book p.22 ↗",
      "Not part of the 2 from streams; part of the 4 a day across Region 2", true, null, "50 cm (20 in) or less"]);
  });

  it("6. glenn_501: the share keeps no page of its own (p.34 is the root's — once per branch)", async () => {
    const top = await treeOfGroup("glenn_501");
    const share = find(top, (x) => x.t === "tier" && x.tier === "share") as TierVM;
    expect(share.line.filter((r) => r.t === "book")).toEqual([]);
  });

  it("7. ape_501: a zero kind draws no number and no chip; its qualifier stays", async () => {
    const ct = find(await treeOfGroup("ape_501"), (x) => x.t === "kind" && x.zero) as KindVM;
    expect([plain(ct.says), ct.says!.some((r) => r.t === "num"), ct.size, ct.qualifiers]).toEqual([
      "No cutthroat trout may be kept", false, null, "catch and release"]);
  });

  it("5, 8, 9. klwa_1010, bc1_501, x25p_1010: whole kind and tier lines", async () => {
    const k = await treeOfGroup("klwa_1010");
    expect(k.filter((x): x is KindVM => x.t === "kind").map((x) => [plain(x.says), x.also.map(norm), x.pictures.map((p) => p.code)])).toEqual([
      ["Up to 2 may be brook, brown, cutthroat, lake or rainbow trout", ["Only 1 cutthroat or rainbow trout over 50 cm (20 in)"], ["EB", "GB", "CT", "LT", "RB"]],
      ["Only 1 of the 2 may be Dolly Varden/bull trout", [], ["DV"]]]);
    const own = (await treeOfGroup("bc1_501")).find((x) => x.t === "tier" && x.tier === "own") as TierVM;
    expect([plain(own.line), own.children.filter((x) => x.t === "tier").map((x) => plain((x as TierVM).line))]).toEqual([
      "This river’s rule: up to 2 a day (part of the 5) Rule book p.43 ↗",
      ["Of these, only 1 may be over 50 cm (20 in)", "1 cutthroat or rainbow trout in all (part of the 2)"]]);   // golden bc1_501 (v38 adds the size box)
    const st = find(await treeOfGroup("x25p_1010", 1), (x) => x.t === "kind" && x.kind === "Steelhead") as KindVM;
    expect([plain(st.presence), plain(st.duty)]).toEqual(["Steelhead are known to be in this water. ⓘ",
      "10 a year · Record each on your licence right away Steelhead stamp ›"]);
  });

  it("10. wilkes_1010: the steelhead presence line comes from the kind's own stored presence", async () => {
    const g = await tree("wilkes_1010");
    const st = find(treeOf(g.env)!, (x) => x.t === "kind" && x.species.includes("ST")) as KindVM;
    const stored: TreeKind[] = [];
    const walkStore = (ns: readonly (TreeTier | TreeKind)[]) => ns.forEach((x) => (x.node === "kind" ? stored.push(x) : walkStore(x.children)));
    walkStore(g.ctx.g.tree);
    const k = stored.find((x) => x.species.includes("ST"))!;
    expect([plain(st.presence), k.presence?.says ?? "(via an undrawn row)"]).toEqual([
      "Steelhead rules apply here; steelhead may not be present in this water. ⓘ", "Steelhead rules apply here; steelhead may not be present in this water."]);
  });

  it("11. property (r20b number-once; book once per branch)", async () => {
    let folded = 0, checked = 0;
    for (const id of ids) for (const g of keepOf((await inputsOf(id))!).groups) {
      const top = treeOf(g.env) ?? [];
      const pagesOn = (l: Line | null | undefined) => (l ?? []).flatMap((r) => (r.t === "book" ? [r.printed] : []));
      walk(top, (x, path) => {
        if (x.t === "kind" && x.folded) {
          folded++;
          const own = path.at(-1)!;
          expect(x.says, id).toBeNull();
          expect(plain(own.line), id).toMatch(/\d/);
        }
        if (x.t === "tier") {
          checked++;
          const along = [...g.ctx.rootPages, ...path.flatMap((p) => pagesOn(p.line)), ...path.flatMap((p) => pagesOn(p.origin?.book))];
          for (const p of pagesOn(x.line)) expect(along.filter((q) => q === p).length, `${id} ${x.tid} p.${p}`).toBe(0);
        }
      });
    }
    // the golden's own counts: 27 folded kinds, 130 tiers (Kitimat's ruled trees have as many of each)
    expect([folded, checked]).toEqual([27, 130]);
  });

  it("12. property (r20e/r20f): a drawn share under an ' across ' total reads 'Only N of the M …', N a big number, M ≠ N", async () => {
    let n = 0;
    for (const id of ids) for (const g of keepOf((await inputsOf(id))!).groups) {
      const root = g.ctx.hdrRoot;
      if (!root || !/ across /.test(root.says)) continue;
      for (const x of treeOf(g.env) ?? []) {
        if (x.t !== "tier" || x.tier !== "share" || x.folded) continue;
        n++;
        const m = /^Only (\d+) of the (\d+) /.exec(plain(x.line));
        expect(m, `${id} ${plain(x.line)}`).not.toBeNull();
        expect(x.line.find((r) => r.t === "num"), id).toEqual({ t: "num", s: m![1], size: "big" });
        expect(m![1], id).not.toBe(m![2]);
      }
    }
    expect(n).toBeGreaterThan(0);
  });

  it("the projection is the VM's (nodeProj reads only the VM)", async () => {
    const t = await tierById("vedder_1010", "t2");
    expect(nodeProj(t)).toMatchObject({ tier: "own", tid: "t2", folded: false, sub: t.relation });
  });
});
