"""(T451) THE FRAMES OF A TAB-SWITCH RECORDING, per direction: how many frames drew the streams UNCOLOURED after a switch
INTO Map — the Map style's ground (its green, darker than the Conditions field's) with under 30 % of the blue lines the
settled Map view shows. The owner's flicker ("mostly between Conditions/Run timing into Map"), counted.

    python3 -I app/tools/testbed/switch-frames.py <run dir>     (reads T451-switch.mp4 + T451-switch.json; writes T451-frames.json)

The tap is found in the recording itself (the first change of the tab bar 1.0–2.2 s after the logged time: the recorder
starts ~1.5 s before the switches are timed). Needs ffmpeg and numpy.
"""
import json, os, re, subprocess, sys
import numpy as np

d = sys.argv[1]
W, H = 216, 480
sw = json.load(open(os.path.join(d, 'T451-switch.json')))['switches']
p = subprocess.run(['ffmpeg', '-v', 'info', '-i', os.path.join(d, 'T451-switch.mp4'), '-an', '-fps_mode', 'passthrough',
                    '-vf', f'scale={W}:{H},showinfo', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'], capture_output=True, check=True)
ts = np.array([float(m) for m in re.findall(rb'pts_time:([0-9.]+)', p.stderr)])
fr = np.frombuffer(p.stdout, np.uint8).reshape(-1, H, W, 3).astype(np.int16)
n = min(len(ts), len(fr)); ts = ts[:n]; fr = fr[:n]
m = fr[:, 70:380, 10:180]                       # the map body: under the search bar, above the legend and tabs
r, g, b = m[..., 0], m[..., 1], m[..., 2]
blue = ((b > r + 40) & (b > g + 10)).sum(axis=(1, 2))
mapGround = np.median((g - r).reshape(n, -1), axis=1) > 50
tab = fr[:, 436:472]
dtab = np.r_[0, np.abs(np.diff(tab, axis=0)).mean(axis=(1, 2, 3))]
dirs, rows = {}, []
for s in sw:
    t0 = s['ms'] / 1000
    c = [i for i in range(n) if t0 + 1.0 < ts[i] < t0 + 2.2 and dtab[i] > 1.0]
    k = f"{s['from']}→{s['to']}"
    e = dirs.setdefault(k, {'switches': 0, 'found': 0, 'withBare': 0, 'bareFrames': 0, 'bareMsMax': 0})
    e['switches'] += 1
    if not c:
        rows.append({'from': s['from'], 'to': s['to'], 'tap': None}); continue
    i0 = c[0]; end = min(n - 1, int(np.searchsorted(ts, ts[i0] + 2.0)))
    bare = [i for i in range(i0, end) if s['to'] == 'Map' and mapGround[i] and blue[i] < 0.3 * blue[end]]
    ms = round((ts[min(bare[-1] + 1, n - 1)] - ts[bare[0]]) * 1000) if bare else 0
    rows.append({'from': s['from'], 'to': s['to'], 'tap': round(float(ts[i0]), 3), 'bare': len(bare), 'bareMs': ms, 'frames': bare})
    e['found'] += 1; e['withBare'] += bool(bare); e['bareFrames'] += len(bare); e['bareMsMax'] = max(e['bareMsMax'], ms)
for k, e in dirs.items(): print(f"{k:22s} {json.dumps(e)}")
json.dump({'dirs': dirs, 'rows': rows}, open(os.path.join(d, 'T451-frames.json'), 'w'), indent=1)
