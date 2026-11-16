// (T760) node --test app/tools/testbed/lib/throttle-proxy.test.mjs
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import { test } from 'node:test';
import { makeBucket, startThrottleProxy } from './throttle-proxy.mjs';

test('the budget is shared: back-to-back chunks queue behind each other at the rate', () => {
  const b = makeBucket(1000);   // 1000 bytes a second
  assert.equal(b.take(500, 0), 500);
  assert.equal(b.take(500, 0), 1000);   // a second connection waits behind the first
  assert.equal(b.take(100, 5000), 100);   // an idle budget does not bank time
  b.setRate(Infinity);
  assert.equal(b.take(1e9, 0), 0);
});

// a CONNECT tunnel to a local server sending 200 kB: at 400 kB/s it takes about half a second; lifted, at once
const serve = () => new Promise((resolve) => {
  const s = http.createServer((_q, r) => r.end(Buffer.alloc(200_000, 1)));
  s.listen(0, '127.0.0.1', () => resolve(s));
});
const through = (proxyPort, port) => new Promise((resolve, reject) => {
  const t0 = Date.now();
  const sock = net.connect(proxyPort, '127.0.0.1', () => sock.write(`CONNECT 127.0.0.1:${port} HTTP/1.1\r\nHost: x\r\n\r\n`));
  let buf = Buffer.alloc(0), opened = false;
  sock.on('data', (c) => {
    buf = Buffer.concat([buf, c]);
    if (!opened && buf.includes('\r\n\r\n')) {
      assert.match(buf.toString('latin1'), /^HTTP\/1\.1 200/);
      opened = true; buf = Buffer.alloc(0);
      sock.write('GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n');
    }
  });
  sock.on('end', () => resolve({ ms: Date.now() - t0, got: buf.length }));
  sock.on('error', reject);
});

test('a tunnel through the proxy is held to the rate, and setRate(Infinity) lifts it', async () => {
  const origin = await serve();
  const proxy = await startThrottleProxy({ rate: 400_000 });
  try {
    const a = await through(proxy.port, origin.address().port);
    assert.ok(a.got > 200_000, `whole body came back (${a.got})`);
    assert.ok(a.ms >= 400, `held to the rate (${a.ms} ms)`);
    proxy.setRate(Infinity);
    const b = await through(proxy.port, origin.address().port);
    assert.ok(b.ms < 300, `lifted (${b.ms} ms)`);
    assert.ok(proxy.bytes > 400_000);
  } finally { await proxy.close(); origin.close(); }
});
