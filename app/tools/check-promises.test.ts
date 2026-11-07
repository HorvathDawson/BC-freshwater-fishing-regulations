/**
 * (T458) The floating-promise gate catches T446's exact shape (an un-awaited `write` before a rename), and lets through
 * what typescript-eslint's `no-floating-promises` lets through: awaited, returned, assigned, `void`, a handled chain.
 */
import ts from "typescript";
import { describe, expect, it } from "vitest";
// @ts-expect-error a .mjs tool, no types
import { floating } from "./check-promises.mjs";

/** The gate over one in-memory file (a stand-in for expo-file-system's SDK 58 File). */
function check(body: string): string[] {
  const name = "/virtual/app.ts";
  const src = `declare class File { write(s: string): Promise<void>; rename(to: string): void }
declare const f: File; declare const maybe: (() => Promise<number>) | undefined;
export async function run(): Promise<unknown> {\n${body}\n}`;
  const host = ts.createCompilerHost({ strict: true, target: ts.ScriptTarget.ES2022, lib: ["lib.es2022.d.ts"] });
  const read = host.getSourceFile.bind(host);
  host.getSourceFile = (n, v) => n === name ? ts.createSourceFile(n, src, v) : read(n, v);
  const program = ts.createProgram([name], { strict: true, target: ts.ScriptTarget.ES2022, lib: ["lib.es2022.d.ts"], noEmit: true }, host);
  return (floating(program) as string[]).map((l) => l.replace(/^.*?:(\d+)\s+/, "$1 "));
}

describe("T458 floating promises", () => {
  it("T446's shape: a write not awaited before the rename is caught", () => {
    expect(check(`  f.write("x");\n  f.rename("y");`)).toEqual([expect.stringMatching(/^4 a promise is dropped/)]);
  });

  it("a maybe-promise and a chain with no rejection handler are caught", () => {
    expect(check(`  maybe?.();\n  f.write("x").then(() => 1);\n  f.write("x").then(() => 1).finally(() => 2);`)).toHaveLength(3);
  });

  it("awaited, returned, assigned, voided and handled chains pass", () => {
    expect(check([
      `  await f.write("x");`,
      `  const p = f.write("x"); await p;`,
      `  void f.write("x");`,
      `  f.write("x").catch(() => 0);`,
      `  f.write("x").then(() => 1, () => 2).finally(() => 3);`,
      `  return f.write("x");`,
    ].join("\n"))).toEqual([]);
  });
});
