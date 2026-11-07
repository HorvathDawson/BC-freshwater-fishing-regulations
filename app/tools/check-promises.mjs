#!/usr/bin/env node
/**
 * (T458) NO FLOATING PROMISES IN APP SOURCE. T446 was one: the Expo SDK 58 upgrade turned expo-file-system's `write` /
 * `move` / `copy` into promises, our calls were not awaited, and a rename moved an EMPTY file into place (fresh installs
 * got 0-byte release files). TypeScript accepts a dropped promise and the app has no ESLint, so this is the gate:
 * typescript-eslint's `no-floating-promises`, on the compiler API the app already has.
 *
 * A statement whose value is a promise (or may be one: `Promise | undefined`, a thenable) is an error unless it is
 * awaited, returned, assigned, discarded on purpose with `void p`, or its rejection is handled: the chain ends in
 * `.catch(h)` or `.then(a, b)` (typescript-eslint's `no-floating-promises` defaults). Tests are not checked (vitest
 * awaits what it is handed; a test's dropped promise fails the test).
 *
 *   node tools/check-promises.mjs            the projects `tsc -b` builds, minus conformance (tests only)
 */
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const APP = join(dirname(fileURLToPath(import.meta.url)), "..");
const PROJECTS = ["packages/core", "packages/data", "packages/ui", "packages/ui-native", "packages/map", "apps/mobile"];
const TEST = /\.test\.tsx?$|\/test\/|\.fixture\.ts$/;

/** Whether `t` is, or in a union may be, a thenable: a type with a callable `then`. */
function thenable(checker, t) {
  if (t.isUnion()) return t.types.some((u) => thenable(checker, u));
  const then = t.getProperty("then");
  if (!then) return false;
  const decl = then.valueDeclaration ?? then.declarations?.[0];
  if (!decl) return false;
  return checker.getTypeOfSymbolAtLocation(then, decl).getCallSignatures().length > 0;
}

/** Whether `e` ends its chain by handling a rejection: `.catch(h)` or `.then(a, b)`, a trailing `.finally(f)` looked past. */
function handled(e) {
  if (!ts.isCallExpression(e) || !ts.isPropertyAccessExpression(e.expression)) return false;
  const name = e.expression.name.text;
  if (name === "finally") return handled(e.expression.expression);
  return (name === "catch" && e.arguments.length >= 1) || (name === "then" && e.arguments.length >= 2);
}

const ASSIGN = new Set([ts.SyntaxKind.EqualsToken, ts.SyntaxKind.BarBarEqualsToken, ts.SyntaxKind.QuestionQuestionEqualsToken,
  ts.SyntaxKind.AmpersandAmpersandEqualsToken]);

export function floating(program) {
  const checker = program.getTypeChecker();
  const out = [];
  for (const sf of program.getSourceFiles()) {
    if (sf.isDeclarationFile || sf.fileName.includes("node_modules") || TEST.test(sf.fileName)) continue;
    const visit = (node) => {
      if (ts.isExpressionStatement(node)) {
        let e = node.expression;
        while (ts.isParenthesizedExpression(e)) e = e.expression;
        const at = sf.getLineAndCharacterOfPosition(node.getStart(sf));
        const where = `${relative(APP, sf.fileName)}:${at.line + 1}`;
        if (!ts.isVoidExpression(e) && !(ts.isBinaryExpression(e) && ASSIGN.has(e.operatorToken.kind))
            && !ts.isAwaitExpression(e) && !handled(e) && thenable(checker, checker.getTypeAtLocation(e))) {
          out.push(`${where}  a promise is dropped: await it, or \`void\` it with the reason — ${node.getText(sf).split("\n")[0].slice(0, 100)}`);
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(sf);
  }
  return out;
}

function programOf(dir) {
  const path = ts.findConfigFile(join(APP, dir), ts.sys.fileExists, "tsconfig.json");
  const cfg = ts.getParsedCommandLineOfConfigFile(path, { noEmit: true, composite: false, declaration: false,
    emitDeclarationOnly: false, incremental: false }, { ...ts.sys, onUnRecoverableConfigFileDiagnostic: (d) => {
    throw new Error(ts.flattenDiagnosticMessageText(d.messageText, "\n"));
  } });
  return ts.createProgram({ rootNames: cfg.fileNames.filter((f) => !TEST.test(f)), options: cfg.options,
    projectReferences: cfg.projectReferences });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const found = [...new Set(PROJECTS.flatMap((p) => floating(programOf(p))))].sort();
  if (found.length) {
    console.error(`✗ ${found.length} floating promise(s) (T458):\n  ${found.join("\n  ")}`);
    process.exit(1);
  }
  console.log(`✓ no floating promises (${PROJECTS.length} projects, tests excluded)`);
}
