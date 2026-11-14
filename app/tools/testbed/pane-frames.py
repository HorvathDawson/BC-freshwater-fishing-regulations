"""(T498, T499) A PANE RECORDING, CUT INTO WHAT WAS SHOWN. Android's `screenrecord` writes a frame only when the screen
changes, so every frame in the file is a change.

    python3 -I app/tools/testbed/pane-frames.py open <mp4> <tap s>    the screens shown from the tap until it settles
    python3 -I app/tools/testbed/pane-frames.py idle <mp4> <offset s> every change while nobody touches the phone
    python3 -I app/tools/testbed/pane-frames.py back <mp4> <tap s> <back s>  (T717) Back from a page: every frame after
                                                     it is the page, the map as left, or a flash (JSON on stdout)

open: frames from the tap on, grouped into SCREENS (a run of frames that differ by less than a small threshold from the
      run's first); each screen held, its first frame saved as <mp4 stem>-s<n>.png and all of them side by side in
      <stem>-strip.png. A pane that opens in one piece is two screens (the map, the finished pane) plus any motion.
idle: the map body's change per frame (mean absolute difference, 0–255, over the pane's map: the header and the bar
      under the map excluded); a frame over the threshold is a flicker, saved as a pair (<stem>-f<n>-a.png before,
      -b.png the flicker); `idle-<stem>.json` lists them. Needs ffmpeg and numpy.
"""
import json, os, re, subprocess, sys
import numpy as np

mode, mp4, at = sys.argv[1], sys.argv[2], float(sys.argv[3])
stem = os.path.splitext(mp4)[0]
W, H = 270, 600
p = subprocess.run(['ffmpeg', '-v', 'info', '-i', mp4, '-an', '-fps_mode', 'passthrough', '-vf', f'scale={W}:{H},showinfo',
                    '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'], capture_output=True, check=True)
ts = np.array([float(m) for m in re.findall(rb'pts_time:([0-9.]+)', p.stderr)])
fr = np.frombuffer(p.stdout, np.uint8).reshape(-1, H, W, 3)
n = min(len(ts), len(fr)); ts = ts[:n]; fr = fr[:n]


def save(img, name):
    subprocess.run(['ffmpeg', '-v', 'error', '-y', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-s', f'{W}x{H}', '-i', '-', name],
                   input=img.tobytes(), check=True)


def diff(a, b):
    return float(np.abs(a.astype(np.int16) - b.astype(np.int16)).mean())


if mode == 'open':
    # the first change after the tap (the recorder starts before the tap is sent)
    i0 = next((i for i in range(1, n) if ts[i] >= at and diff(fr[i], fr[i - 1]) > 0.5), None)
    if i0 is None:
        print(f'{os.path.basename(mp4)}: no change after {at:.2f} s'); sys.exit(0)
    screens = []                                   # [first frame index, last frame index]
    base = i0 - 1
    screens.append([base, base])
    for i in range(i0, n):
        if diff(fr[i], fr[screens[-1][0]]) > 3.0: screens.append([i, i])
        else: screens[-1][1] = i
    # a screen that lasts under one frame interval of motion (an animation's in-between) is still a screen shown
    rows = []
    for k, (a, b) in enumerate(screens):
        end = ts[screens[k + 1][0]] if k + 1 < len(screens) else ts[-1]
        rows.append({'n': k, 'at_ms': round((ts[a] - ts[i0]) * 1000), 'held_ms': round((end - ts[a]) * 1000),
                     'frames': b - a + 1})
        save(fr[a], f'{stem}-s{k}.png')
    # the strip: every screen whose hold was ≥ 40 ms (a frame a reader sees), in order, 8 at most
    seen = [r['n'] for r in rows if r['held_ms'] >= 40 or r['n'] in (0, len(rows) - 1)][:8]
    strip = np.concatenate([np.pad(fr[screens[k][0]], ((0, 0), (0, 6), (0, 0)), constant_values=255) for k in seen], axis=1)
    subprocess.run(['ffmpeg', '-v', 'error', '-y', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-s', f'{strip.shape[1]}x{H}',
                    '-i', '-', f'{stem}-strip.png'], input=strip.tobytes(), check=True)
    settled = rows[-1]['at_ms']
    json.dump(rows, open(f'{stem}-screens.json', 'w'), indent=1)
    print(f'{os.path.basename(mp4)}: {len(rows)} screens from the tap to settled at {settled} ms; seen ≥40 ms: '
          + ', '.join(f"#{r['n']} @{r['at_ms']} ms ({r['held_ms']} ms)" for r in rows if r['n'] in seen))
elif mode == 'back':
    # (T717) BACK FROM A WATER'S PAGE TO THE MAP: <tap s> = when the map was tapped, argv[4] = when Back was pressed. The
    # reference is the map as left (the last frame before the tap); the page is the last frame before Back. Every frame
    # from Back on is the PAGE (within TOL of it), the MAP (within TOL of the reference) or neither: a flash (blank,
    # grey, half-drawn, another palette). The status bar (top 4 %) is left out: its clock ticks.
    back = float(sys.argv[4])
    TOL = float(os.environ.get('BACK_TOL', '8'))
    top = int(H * 0.04)
    pre = [i for i in range(n) if ts[i] < at]
    on = [i for i in range(n) if ts[i] < back]
    after = [i for i in range(n) if ts[i] >= back]
    if not pre or not on or not after:
        print(json.dumps({'error': f'frames: {len(pre)} before the tap, {len(on)} before Back, {len(after)} after'})); sys.exit(0)
    ref, page = fr[pre[-1]][top:], fr[on[-1]][top:]
    end = float(ts[-1]) + 0.25                     # the recorder's last frame is held until it stops
    rows = []
    for k, i in enumerate(after):
        b = fr[i][top:]
        d_ref, d_page = diff(b, ref), diff(b, page)
        state = 'map' if d_ref <= TOL else 'page' if d_page <= TOL else 'flash'
        nxt = float(ts[after[k + 1]]) if k + 1 < len(after) else end
        rows.append({'i': i, 'at_ms': round((ts[i] - back) * 1000), 'held_ms': round((nxt - ts[i]) * 1000),
                     'state': state, 'd_map': round(d_ref, 1), 'd_page': round(d_page, 1),
                     'flat': round(float(b.reshape(-1, 3).std(axis=0).mean()), 1),
                     'rgb': [int(x) for x in b.reshape(-1, 3).mean(axis=0)]})
    # times from the page's leaving (the first frame after Back that is not the page): the boundary is set early
    left = next((r['at_ms'] for r in rows if r['state'] != 'page'), None)
    for r in rows:
        r['at_ms'] = r['at_ms'] - left if left is not None else r['at_ms']
    flashes = [r for r in rows if r['state'] == 'flash']
    # the strip: the map as left, the page, then the first frame of each run of one state after Back (8 at most)
    # (a new run, too, when a flash changes colour: a dark map's light frame shows in the strip)
    runs = [r for k, r in enumerate(rows) if k == 0 or r['state'] != rows[k - 1]['state']
            or (r['state'] == 'flash' and diff(fr[r['i']], fr[rows[k - 1]['i']]) > TOL)][:8]
    pics = [fr[pre[-1]], fr[on[-1]]] + [fr[r['i']] for r in runs]
    strip = np.concatenate([np.pad(p, ((0, 0), (0, 6), (0, 0)), constant_values=255) for p in pics], axis=1)
    subprocess.run(['ffmpeg', '-v', 'error', '-y', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-s', f'{strip.shape[1]}x{H}',
                    '-i', '-', f'{stem}-back-strip.png'], input=strip.tobytes(), check=True)
    for k, r in enumerate(flashes[:6]):
        save(fr[r['i']], f'{stem}-flash{k}.png')
    out = {'tol': TOL, 'page_left': left is not None, 'frames_after_back': len(rows), 'flash_frames': len(flashes),
           'flash_ms': sum(r['held_ms'] for r in flashes), 'settled': rows[-1]['state'],
           'first_map_ms': next((r['at_ms'] for r in rows if r['state'] == 'map'), None),
           'flashes': [{k: r[k] for k in ('at_ms', 'held_ms', 'd_map', 'd_page', 'flat', 'rgb')} for r in flashes[:12]],
           'runs': [{k: r[k] for k in ('at_ms', 'state', 'd_map', 'd_page')} for r in runs]}
    json.dump({**out, 'rows': rows}, open(f'{stem}-back.json', 'w'), indent=1)
    print(json.dumps(out))
else:
    # the pane's map body: under the header (top 22 %), above the bar under the map (bottom 30 %)
    body = fr[:, int(H * 0.22):int(H * 0.70)]
    d = np.r_[0, [diff(body[i], body[i - 1]) for i in range(1, n)]]
    hits = [i for i in range(1, n) if d[i] > 1.0]
    rows = []
    for k, i in enumerate(hits[:12]):
        save(fr[i - 1], f'{stem}-f{k}-a.png'); save(fr[i], f'{stem}-f{k}-b.png')
    for i in hits:
        rows.append({'t': round(at + float(ts[i]), 3), 'diff': round(float(d[i]), 2)})
    json.dump({'frames': n, 'changes': rows}, open(f'{stem}-idle.json', 'w'), indent=1)
    print(f'{os.path.basename(mp4)} (+{at:.0f} s): {n} frames in {ts[-1] if n else 0:.0f} s; changes over 1.0 in the map body: '
          f'{len(hits)}' + (f" at {', '.join(str(r['t']) + ' s (' + str(r['diff']) + ')' for r in rows[:20])}" if rows else ''))
