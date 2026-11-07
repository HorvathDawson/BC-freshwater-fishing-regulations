/**
 * (T456) THE WALKTHROUGH'S VIEW OF A CONTROL WHOSE STATE A SCREEN KEEPS TO ITSELF — a fold's Details, the chart's Flow /
 * Level and 72 h / Year, the panel's forecast day. A DO step about one of them moves on when the reader uses it, so the walk
 * must see its value; "Show me" uses it the same way the reader's tap would (its own setter). The screen tells
 * (`useGuideControl`); the Shell keeps the values (`useGuideControlHost`) and reads them into the walk's facts. Outside a
 * host (a screen mounted alone, a test) telling is a no-op.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";

export interface GuideControlSink {
  tell(key: string, value: unknown, set: (v: unknown) => void): void;
  gone(key: string, set: (v: unknown) => void): void;
}

export const GuideControlContext = createContext<GuideControlSink | null>(null);

/** A screen's control, told to the walk while it is drawn: its key, its value now, and how to set it. */
export function useGuideControl<T>(key: string | null | undefined, value: T, set: (v: T) => void): void {
  const sink = useContext(GuideControlContext);
  const latest = useRef(set);
  latest.current = set;
  const call = useCallback((v: unknown) => latest.current(v as T), []);
  useEffect(() => {
    if (!sink || !key) return;
    sink.tell(key, value, call);
  }, [sink, key, value, call]);
  useEffect(() => () => { if (sink && key) sink.gone(key, call); }, [sink, key, call]);
}

/** The Shell's side: the values the screens told (state, so the walk re-reads them), and each control's setter. */
export function useGuideControlHost(): {
  sink: GuideControlSink; values: Readonly<Record<string, unknown>>; set(key: string, v: unknown): boolean;
} {
  const [values, setValues] = useState<Readonly<Record<string, unknown>>>({});
  const setters = useRef(new Map<string, (v: unknown) => void>());
  const sink = useMemo<GuideControlSink>(() => ({
    tell: (key, value, set) => {
      setters.current.set(key, set);
      setValues((v) => (Object.is(v[key], value) && key in v ? v : { ...v, [key]: value }));
    },
    gone: (key, set) => {
      if (setters.current.get(key) !== set) return;      // a newer drawing of it took over
      setters.current.delete(key);
      setValues((v) => { if (!(key in v)) return v; const { [key]: _, ...rest } = v; return rest; });
    },
  }), []);
  const set = useCallback((key: string, v: unknown) => {
    const f = setters.current.get(key);
    if (f) f(v);
    return !!f;
  }, []);
  return { sink, values, set };
}
