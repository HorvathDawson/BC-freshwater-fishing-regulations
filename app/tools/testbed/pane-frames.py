"""(T498, T499) A PANE RECORDING, CUT INTO WHAT WAS SHOWN. Android's `screenrecord` writes a frame only when the screen
changes, so every frame in the file is a change.

    python3 -I app/tools/testbed/pane-frames.py open <mp4> <tap s>    the screens shown from the tap until it settles
    python3 -I app/tools/testbed/pane-frames.py idle <mp4> <offset s> every change while nobody touches the phone

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
