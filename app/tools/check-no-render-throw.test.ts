/**
 * (T1164) The render-path throw gate: it sees a `throw` written into a component, a hook, a React class's lifecycle, an
 * effect, a render-time callback, an array-method callback in one of those, or a same-file helper one of those calls; it
 * leaves alone what is not drawing (a handler, `useCallback`, `.then`, async, a helper nothing in a render path calls);
 * and the app has no throw that ALLOWED does not list with its exact count.
 */
import { describe, expect, it } from "vitest";
// @ts-expect-error a .mjs tool, no types
import { ALLOWED, judge, renderThrows, scan } from "./check-no-render-throw.mjs";

const at = (src: string, file = "x.tsx"): string[] => (renderThrows(file, src) as { at: string }[]).map((f) => f.at);

describe("T1164 no throw in a render path", () => {
  it("a component, a const-arrow component, a memo/forwardRef one, a hook are caught", () => {
    expect(at(`function Page() { throw new Error("x"); }`)).toEqual(["x.tsx::Page"]);
    expect(at(`const Card = () => { if (a) throw new Error("x"); return null; };`)).toEqual(["x.tsx::Card"]);
    expect(at(`const Row = memo(function Inner() { return 1; }); const Cell = memo(() => { throw 1; });`)).toEqual(["x.tsx::Cell"]);
    expect(at(`const Box = forwardRef((p, r) => { throw 1; });`)).toEqual(["x.tsx::Box"]);
    expect(at(`function useThing() { throw new Error("x"); }`)).toEqual(["x.tsx::useThing"]);
  });

  it("an anonymous default export in a .tsx file is a component; in a .ts file it is not", () => {
    expect(at(`export default function () { throw 1; }`)).toEqual(["x.tsx::default"]);
    expect(at(`export default () => { throw 1; };`)).toEqual(["x.tsx::default"]);
    expect(at(`export default function () { throw 1; }`, "x.ts")).toEqual([]);
  });

  it("a React class's render, constructor and getDerivedStateFromProps are caught; another class's constructor is not", () => {
    expect(at(`class P extends Component { render() { throw 1; } }`)).toEqual(["x.tsx::P.render"]);
    expect(at(`class P extends React.PureComponent { constructor(p) { super(p); throw 1; } }`)).toEqual(["x.tsx::P.constructor"]);
    expect(at(`class P extends Component { static getDerivedStateFromProps() { throw 1; } }`)).toEqual(["x.tsx::P.getDerivedStateFromProps"]);
    expect(at(`class P extends Component { componentDidCatch() { throw 1; } }`)).toEqual([]);
    expect(at(`class NetError extends Error { constructor(m) { super(m); throw 1; } }`)).toEqual([]);
  });

  it("a callback React runs while drawing is caught: useMemo, a useState initialiser, a reducer", () => {
    expect(at(`function Page() { const v = useMemo(() => { throw 1; }, []); return v; }`)).toEqual(["x.tsx::Page>useMemo"]);
    expect(at(`function Page() { const [s] = useState(() => { throw 1; }); return s; }`)).toEqual(["x.tsx::Page>useState"]);
    expect(at(`function useX() { return useReducer((s, a) => { throw 1; }, 0); }`)).toEqual(["x.tsx::useX>useReducer"]);
  });

  it("an effect callback is caught (React sends its throw to the nearest boundary), its async form and a handler are not", () => {
    expect(at(`function Page() { useEffect(() => { throw 1; }, []); return null; }`)).toEqual(["x.tsx::Page>useEffect"]);
    expect(at(`function Page() { useLayoutEffect(() => { throw 1; }); return null; }`)).toEqual(["x.tsx::Page>useLayoutEffect"]);
    expect(at(`function Page() { useEffect(() => { void (async () => { throw 1; })(); }, []); return null; }`)).toEqual([]);
    expect(at(`function Page() { useEffect(() => () => { throw 1; }, []); return null; }`)).toEqual([]);
  });

  it("an array-method callback inside a component is caught (`items.map((i) => { throw … })`), also in JSX", () => {
    expect(at(`function List({ items }) { return items.map((i) => { if (!i.part) throw new Error("x"); return i; }); }`))
      .toEqual(["x.tsx::List>map"]);
    expect(at(`function List({ items }) { return <ul>{items.filter((i) => { throw 1; }).map((i) => <li />)}</ul>; }`))
      .toEqual(["x.tsx::List>filter"]);
    expect(at(`function List({ items }) { return items.map((i) => i.xs.some((x) => { throw 1; })); }`)).toEqual(["x.tsx::List>map>some"]);
    // the same call inside a handler or a plain function is not drawing
    expect(at(`function List({ items }) { const go = () => items.map((i) => { throw 1; }); return go; }`)).toEqual([]);
    expect(at(`function plain(items) { return items.map((i) => { throw 1; }); }`)).toEqual([]);
  });

  it("a same-file helper a render path calls is caught, through other helpers too; one nothing calls is not", () => {
    expect(at(`const layered = (n) => { throw 1; };\nfunction Shell() { return layered(1); }`)).toEqual(["x.tsx::layered"]);
    expect(at(`function check(v) { throw 1; }\nfunction wrap(v) { return check(v); }\nfunction Page() { return wrap(1); }`)).toEqual(["x.tsx::check"]);
    expect(at(`function Page({ items }) { return items.map((i) => guard(i)); }\nfunction guard(i) { throw 1; }`)).toEqual(["x.tsx::guard"]);
    expect(at(`function unused() { throw 1; }\nfunction Page() { return null; }`)).toEqual([]);
    // called only from a handler, an `onX` helper, or an async function
    expect(at(`function boom() { throw 1; }\nfunction Page() { const go = () => boom(); return go; }`)).toEqual([]);
    expect(at(`function onPick() { throw 1; }\nfunction Page() { return onPick(); }`)).toEqual([]);
    expect(at(`async function load() { throw 1; }\nfunction Page() { load(); return null; }`)).toEqual([]);
  });

  it("what is not drawing is not caught: a handler, useCallback, .then, an async function, a plain helper", () => {
    expect(at(`function Page() { const on = () => { throw 1; }; return on; }`)).toEqual([]);
    expect(at(`function Page() { const go = useCallback(() => { throw 1; }, []); return go; }`)).toEqual([]);
    expect(at(`function Page() { p.then(() => { throw 1; }); return null; }`)).toEqual([]);
    expect(at(`async function useLoad() { throw 1; }`)).toEqual([]);
    expect(at(`function helper() { throw 1; }`)).toEqual([]);
    expect(at(`function Page() { return <B onPress={() => { throw 1; }} />; }`)).toEqual([]);
  });

  it("judge: a new throw, a second throw in a listed function, a fixed one and a stale entry each fail", () => {
    const f = (a: string, line = 1) => ({ at: a, line });
    const allowed = { "a.tsx::A": { count: 1, why: "w" } };
    expect(judge([f("a.tsx::A")], allowed)).toEqual([]);
    expect(judge([f("a.tsx::A"), f("a.tsx::B", 9)], allowed)).toEqual([expect.stringMatching(/^a\.tsx::B:9: a throw in a render path/)]);
    expect(judge([f("a.tsx::A"), f("a.tsx::A", 7)], allowed)).toEqual([expect.stringMatching(/^a\.tsx::A: 2 throw\(s\) found, ALLOWED says 1/)]);
    expect(judge([], allowed)).toEqual(["a.tsx::A: listed in ALLOWED but no longer throws; remove it"]);
  });

  it("the app holds exactly the throws ALLOWED lists, each with its count", () => {
    const { found, problems } = scan();
    expect(problems).toEqual([]);
    const total = (Object.values(ALLOWED) as { count: number }[]).reduce((n, v) => n + v.count, 0);
    expect([found.length, Object.keys(ALLOWED).length, total]).toEqual([7, 6, 7]);
  });
});
