// (T760, 2026-11-16) A SLOW LINE FOR THE EMULATOR: an HTTP proxy on this Mac that holds every byte coming back to the
// phone to one shared rate. The emulator's own speed shaping (`adb emu network speed`) does not reach its Wi-Fi, and on
// the Mac's full line the first install's 479 MB landed in 35 to 60 s, so the system's download was over before the
// bench could tap its notification. The phone is pointed at this proxy with `settings put global http_proxy
// 10.0.2.2:<port>` (the emulator's name for the Mac); the system downloader and the app both follow that setting. HTTPS
// goes through as CONNECT tunnels (the bytes are relayed, never read); plain HTTP is forwarded.
// `setRate(Infinity)` lifts the limit on every connection already open, so the rest of a run is not slowed.
import http from 'node:http';
import net from 'node:net';
import { Transform } from 'node:stream';

/** One budget shared by every connection: `rate` bytes a second, coming back to the phone. */
export function makeBucket(rate) {
  let bps = rate, nextFree = 0;
  return {
    setRate(r) { bps = r; nextFree = 0; },
    get rate() { return bps; },
    /** ms to hold `n` bytes before passing them on (0 when unlimited), booking them against the budget */
    take(n, now = Date.now()) {
      if (!Number.isFinite(bps)) return 0;
      const start = Math.max(now, nextFree);
      nextFree = start + (n / bps) * 1000;
      return Math.max(0, nextFree - now);
    },
  };
}

const slow = (bucket) => new Transform({
  transform(chunk, _enc, done) {
    const wait = bucket.take(chunk.length);
    if (wait <= 0) done(null, chunk); else setTimeout(() => done(null, chunk), wait);
  },
});

/** Start the proxy on 127.0.0.1 (the emulator reaches it as 10.0.2.2). Resolves to { port, setRate, bytes, close }. */
export function startThrottleProxy({ rate }) {
  const bucket = makeBucket(rate);
  const sockets = new Set();
  let bytes = 0;
  const track = (s) => { sockets.add(s); s.on('close', () => sockets.delete(s)); s.on('error', () => s.destroy()); };
  const server = http.createServer((req, res) => {
    // plain HTTP through a proxy: the request line carries the whole URL
    let url;
    try { url = new URL(req.url); } catch { res.writeHead(400).end('absolute URL expected'); return; }
    const up = http.request({ host: url.hostname, port: url.port || 80, path: url.pathname + url.search, method: req.method, headers: req.headers }, (r) => {
      res.writeHead(r.statusCode ?? 502, r.headers);
      r.on('data', (c) => { bytes += c.length; });
      r.pipe(slow(bucket)).pipe(res);
    });
    up.on('error', (e) => { if (!res.headersSent) res.writeHead(502); res.end(String(e.message)); });
    req.pipe(up);
  });
  server.on('connect', (req, client, head) => {
    track(client);
    const [host, port] = String(req.url).split(':');
    const up = net.connect(Number(port) || 443, host, () => {
      client.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head?.length) up.write(head);
      client.pipe(up);
      up.on('data', (c) => { bytes += c.length; });
      up.pipe(slow(bucket)).pipe(client);
    });
    track(up);
    up.on('error', () => client.destroy());
    client.on('close', () => up.destroy());
  });
  server.on('connection', track);
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve({
      port: server.address().port,
      setRate: (r) => bucket.setRate(r),
      get rate() { return bucket.rate; },
      get bytes() { return bytes; },
      close: () => new Promise((done) => { for (const s of sockets) s.destroy(); server.close(() => done()); }),
    }));
  });
}
