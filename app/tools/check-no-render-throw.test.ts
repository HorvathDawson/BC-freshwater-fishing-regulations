/**
 * (T1164) The render-path throw gate: it sees a `throw` written straight into a component, a hook or a render-time callback,
 * ignores the ones that are not drawing (a handler, an effect, an async function, a plain helper), and the app has no
 * unlisted one.
 */
import { describe, expect, it } from "vitest";
// @ts-expect-error a .mjs tool, no types
import { ALLOWED, renderThrows, scan } from "./check-no-render-throw.mjs";

const at = (src: string, file = "x.tsx"): string[] => (renderThrows(file, src) as { at: string }[]).map((f) => f.at);

describe("T1164 no throw in a render path", () => {
  it("a component, a const-arrow component, a memo/forwardRef one, a hook and a class render are caught", () => {
    expect(at(`function Page() { throw new Error("x"); }`)).toEqual(["x.tsx::Page"]);
    expect(at(`const Card = () => { if (a) throw new Error("x"); return null; };`)).toEqual(["x.tsx::Card"]);
    expect(at(`const Row = memo(function Inner() { return 1; }); const Cell = memo(() => { throw 1; });`)).toEqual(["x.tsx::Cell"]);
    expect(at(`const Box = forwardRef((p, r) => { throw 1; });`)).toEqual(["x.tsx::Box"]);
    expect(at(`function useThing() { throw new Error("x"); }`)).toEqual(["x.tsx::useThing"]);
    expect(at(`class P extends Component { render() { throw 1; } }`)).toEqual(["x.tsx::P.render"]);
  });

  it("a callback React runs while drawing is caught: useMemo, a useState initialiser, a reducer", () => {
    expect(at(`function Page() { const v = useMemo(() => { throw 1; }, []); return v; }`)).toEqual(["x.tsx::Page>useMemo"]);
    expect(at(`function Page() { const [s] = useState(() => { throw 1; }); return s; }`)).toEqual(["x.tsx::Page>useState"]);
    expect(at(`function useX() { return useReducer((s, a) => { throw 1; }, 0); }`)).toEqual(["x.tsx::useX>useReducer"]);
  });

  it("what is not drawing is not caught: a handler, an effect, an async function, a helper, a test-style function", () => {
    expect(at(`function Page() { const on = () => { throw 1; }; useEffect(() => { throw 1; }, []); return on; }`)).toEqual([]);
    expect(at(`async function useLoad() { throw 1; }`)).toEqual([]);
    expect(at(`function helper() { throw 1; }`)).toEqual([]);
    expect(at(`function Page() { const go = useCallback(() => { throw 1; }, []); return go; }`)).toEqual([]);
  });

  it("the app has no render-path throw but the listed one, and the list has nothing stale", () => {
    const { found, fresh, stale } = scan();
    expect(fresh).toEqual([]);
    expect(stale).toEqual([]);
    expect(found.map((f: { at: string }) => f.at)).toEqual(["packages/ui-native/src/regs/KeepTab.tsx::KeepTab"]);
    expect(Object.keys(ALLOWED)).toHaveLength(1);
  });

  it("a new throw in a component is reported as new (a fixture against an empty list)", () => {
    const found = renderThrows("packages/ui-native/src/New.tsx", `export function New() { throw new Error("x"); }`);
    expect(found.filter((f: { at: string }) => !(f.at in ALLOWED))).toHaveLength(1);
  });
});
