"""(T566) THE FRAMES OF A TROUT / SALMON / RUNS RECORDING, per direction: for each switch, when the KEY (the legend over the
tab bar) changed, when the MAP reached the new colouring (the first frame from which it stays near the settled view: a
fifth of the before/after difference, four frames running) and when it stopped moving, how long the map still showed
the previous colours under the new key (`staleMs` = map on the new colouring - key change), and how many frames drew
the streams grey or blank (under half the coloured pixels of BOTH the view before the switch and the view after it).

    python3 -I app/tools/testbed/regs-switch-frames.py <run dir>   (reads T566-switch.mp4 + T566-switch.json; writes T566-frames.json)

The recording starts ~1.5 s before the switches are timed: the offset is found from the tab switches (the tab bar changes
at the tap) and applied to the rail's presses too. Needs ffmpeg and numpy.
"""
import json, os, re, subprocess, sys
import numpy as np

d = sys.argv[1]
W, H = 216, 480
sw = json.load(open(os.path.join(d, 'T566-switch.json')))['switches']
p = subprocess.run(['ffmpeg', '-v', 'info', '-i', os.path.join(d, 'T566-switch.mp4'), '-an', '-fps_mode', 'passthrough',
                    '-vf', f'scale={W}:{H},showinfo', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'], capture_output=True, check=True)
ts = np.array([float(m) for m in re.findall(rb'pts_time:([0-9.]+)', p.stderr)])
fr = np.frombuffer(p.stdout, np.uint8).reshape(-1, H, W, 3).astype(np.int16)
n = min(len(ts), len(fr)); ts = ts[:n]; fr = fr[:n]
body = fr[:, 70:380, 10:175]                    # the map: under the search bar, left of the rail, above the key
legend = fr[:, 384:434, 4:212]                   # the key over the tab bar
tab = fr[:, 436:472]
sat = body.max(axis=3) - body.min(axis=3)
coloured = (sat > 60).sum(axis=(1, 2))
step = lambda a: np.r_[0, np.abs(np.diff(a, axis=0)).mean(axis=tuple(range(1, a.ndim)))]
dtab, dbody = step(tab), step(body)

# the recording's offset: a tab switch's tap is the first tab-bar change 0.5-2.5 s after its logged time
offs = []
for s in sw:
    if s['to'] not in ('Runs', 'Map'): continue
    t = s['ms'] / 1000
    c = [i for i in range(n) if t + 0.5 < ts[i] < t + 2.5 and dtab[i] > 1.0]
    if c: offs.append(ts[c[0]] - t)
off = float(np.median(offs)) if offs else 1.5

def first(i0, i1, a, ref, thr):
    for i in range(i0, i1):
        if np.abs(a[i] - ref).mean() > thr: return i
    return None

dirs, rows = {}, []
for s in sw:
    t = s['ms'] / 1000 + off
    i0 = int(np.searchsorted(ts, t - 0.05)); pre = max(0, i0 - 1)
    # the last frame before the next press (2.2 s apart): the recorder writes a frame only when the screen changes
    end = max(i0, min(n - 1, int(np.searchsorted(ts, t + 1.2)) - 1))
    k = f"{s['from']}→{s['to']}"
    e = dirs.setdefault(k, {'switches': 0, 'staleMsMax': 0, 'staleMs': [], 'settleMs': [], 'greyFrames': 0, 'withGrey': 0})
    e['switches'] += 1
    li = first(i0, end, legend, legend[pre], 2.0)
    # the map is on the NEW colouring from the first frame it is (and stays) within a hair of the settled view
    gap = max(np.abs(body[pre] - body[end]).mean(), 1.0)
    near = [np.abs(body[i] - body[end]).mean() < 0.2 * gap for i in range(i0, end + 1)]
    mi = next((i0 + j for j in range(len(near)) if all(near[j:j + 4])), None)
    moving = [i for i in range(i0, end) if dbody[i] > 0.6]
    si = moving[-1] if moving else mi
    floor = 0.5 * min(coloured[pre], coloured[end])
    grey = [i for i in range(i0, end) if coloured[i] < floor]
    ms = lambda i: None if i is None else round((ts[i] - t) * 1000)
    stale = max(0, ms(mi) - ms(li)) if (li is not None and mi is not None) else None
    rows.append({'from': s['from'], 'to': s['to'], 'keyMs': ms(li), 'mapMs': ms(mi), 'settleMs': ms(si), 'staleMs': stale,
                 'grey': len(grey), 'colouredBefore': int(coloured[pre]), 'colouredAfter': int(coloured[end])})
    if stale is not None: e['staleMs'].append(stale); e['staleMsMax'] = max(e['staleMsMax'], stale)
    if ms(si) is not None: e['settleMs'].append(ms(si))
    e['greyFrames'] += len(grey); e['withGrey'] += bool(grey)
for k, e in dirs.items():
    q = lambda a: sorted(a)[len(a) // 2] if a else None
    print(f"{k:16s} n={e['switches']} key→map stale p50 {q(e['staleMs'])} / max {e['staleMsMax']} ms; "
          f"settled p50 {q(e['settleMs'])} / max {max(e['settleMs'] or [0])} ms; grey frames {e['greyFrames']} in {e['withGrey']} switches")
json.dump({'offsetS': off, 'dirs': dirs, 'rows': rows}, open(os.path.join(d, 'T566-frames.json'), 'w'), indent=1)
