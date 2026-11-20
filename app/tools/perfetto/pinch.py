"""(T876) The test bed's `zoom-pinch` traces: per trace, the gesture's window (the `T876gesture` mark + 1.5 s), the map
render thread's CPU in it (ms and share of the window), the app's other busy threads, and (`--perf`) the render thread's
hottest leaf functions from `linux.perf` samples when the trace has them.

  python app/tools/perfetto/pinch.py <run dir> [--perf] [--threads]
"""
import glob, json, os, sys
from perfetto.trace_processor import TraceProcessor, TraceProcessorConfig

TP = os.path.expanduser('~/.local/share/perfetto/prebuilts/trace_processor_shell-8488edbefadfc4be')
APP = 'ca.canifishthis.app'


def one(path, perf=False, threads=False):
    cfg = TraceProcessorConfig(bin_path=TP) if os.path.exists(TP) else None
    tp = TraceProcessor(trace=path, config=cfg) if cfg else TraceProcessor(trace=path)
    q = lambda s: list(tp.query(s))
    g = q("select ts, dur from slice where name = 'T876gesture' order by ts limit 1")
    if not g:
        tp.close(); return {'trace': os.path.basename(path), 'error': 'no gesture mark'}
    # the gesture's end: the `U` the test bed logs after the last lift (logcat's monotonic clock is the trace's here: the
    # start mark and the log's `S` agree to the ms on the emulator); the mark's own end is written from another shell
    t0, t1 = g[0].ts, g[0].ts + 1_500_000_000
    log = path.replace('.pftrace', '.log')
    if os.path.exists(log):
        import re
        ups = [float(m.group(1)) for m in re.finditer(r'^\s*(\d+\.\d+)\s.*CifCam\s*:\s*U\s*$', open(log).read(), re.M)]
        if ups: t1 = int(ups[-1] * 1e9) + 1_500_000_000
    win = (t1 - t0) / 1e6
    pr = q(f"select upid from process where name = '{APP}' or cmdline = '{APP}' limit 1")
    if not pr:
        tp.close(); return {'trace': os.path.basename(path), 'error': 'no app process'}
    upid = pr[0].upid
    rows = q(f"""select t.utid, t.tid, t.name, sum(min(s.ts + s.dur, {t1}) - max(s.ts, {t0})) / 1e6 ms
      from sched s join thread t using(utid) where t.upid = {upid} and s.ts < {t1} and s.ts + s.dur > {t0}
      group by t.utid order by ms desc limit 12""")
    # MapLibre's render thread: the one presenting (Vulkan `QueuePresentKHR` / GL `eglSwapBuffers` slices), else the
    # busiest thread that is not the UI, JS, HWUI or a tile worker. HWUI's own `RenderThread` presents too (the app's
    # views) and can out-present a stalled map, so it is never the map's
    pres = q(f"""select t.utid, count(*) n from slice s join thread_track tt on s.track_id = tt.id join thread t using(utid)
      where t.upid = {upid} and t.name != 'RenderThread' and (s.name like '%QueuePresent%' or s.name like 'eglSwapBuffers%') and s.ts between {t0} and {t1}
      group by t.utid order by n desc limit 1""")
    skip = ('mqt_js', 'Worker', 'mqt_native')
    named = lambda r: r.name and r.name != 'RenderThread' and not r.name.startswith(skip) and not APP.startswith(r.name)
    rt = next((r for r in rows if pres and r.utid == pres[0].utid), None) or next((r for r in rows if named(r)), None)
    out = {'trace': os.path.basename(path), 'window_ms': round(win), 'render_thread': f'{rt.name} ({rt.tid})' if rt else None,
           'render_ms': round(rt.ms) if rt else None, 'render_pct': round(100 * rt.ms / win, 1) if rt else None,
           'presents': pres[0].n if pres else None,
           'workers_ms': round(sum(r.ms for r in rows if r.name and r.name.startswith('Worker'))),
           'js_ms': round(sum(r.ms for r in rows if r.name and r.name.startswith('mqt_js')))}
    if threads:
        out['threads'] = [(r.name, r.tid, round(r.ms)) for r in rows]
    if perf and rt:
        try:
            hot = q(f"""select f.name fn, m.name lib, count(*) n from perf_sample p
              join stack_profile_callsite c on p.callsite_id = c.id join stack_profile_frame f on c.frame_id = f.id
              join stack_profile_mapping m on f.mapping = m.id
              where p.utid = {rt.utid} and p.ts between {t0} and {t1} group by fn, lib order by n desc limit 15""")
            out['perf'] = [(h.fn, os.path.basename(h.lib or ''), h.n) for h in hot]
        except Exception as e:  # noqa: BLE001 — a trace with no perf samples
            out['perf'] = f'none ({str(e)[:80]})'
    tp.close()
    return out


if __name__ == '__main__':
    d = sys.argv[1]
    for p in sorted(glob.glob(os.path.join(d, 'T876-*.pftrace'))):
        print(json.dumps(one(p, '--perf' in sys.argv, '--threads' in sys.argv)))
