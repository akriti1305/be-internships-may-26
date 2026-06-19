import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';
import net from 'node:net';

// ── helpers ────────────────────────────────────────────────────────────────

/** Wait until a TCP port is accepting connections (max ~5 s) */
async function waitForPort(port, maxMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    const open = await new Promise((resolve) => {
      const s = net.createConnection({ port, host: '127.0.0.1' });
      s.on('connect', () => { s.destroy(); resolve(true); });
      s.on('error', () => resolve(false));
    });
    if (open) return;
    await wait(50);
  }
  throw new Error(`Port ${port} did not open within ${maxMs}ms`);
}

async function postJson(url, { headers = {}, body }) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
      },
      (res) => {
        let chunks = '';
        res.on('data', (d) => (chunks += d));
        res.on('end', () => {
          try { resolve({ status: res.statusCode, body: JSON.parse(chunks || '{}') }); }
          catch { resolve({ status: res.statusCode, body: {} }); }
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function startServer(env = {}) {
  return spawn('node', ['src/server.js'], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

// ── tests ──────────────────────────────────────────────────────────────────

test('idempotency: sequential requests with same key return same resource', async () => {
  const PORT = 19091;
  const proc = startServer({ API_KEY: 'k', PORT: String(PORT), DATABASE_URL: `./data/test_idem_seq_${PORT}.db` });
  await waitForPort(PORT);

  const base = `http://localhost:${PORT}`;
  const idem = `idem-seq-${Date.now()}`;

  const a = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k', 'idempotency-key': idem },
    body: { userId: 'u1', type: 'note', payload: 'x' },
  });

  const b = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k', 'idempotency-key': idem },
    body: { userId: 'u1', type: 'note', payload: 'x' },
  });

  proc.kill();

  assert.ok(a.status === 201 || a.status === 200, `First request status should be 2xx, got ${a.status}`);
  assert.ok(b.status === 200 || b.status === 201, `Second request status should be 2xx, got ${b.status}`);
  assert.equal(a.body.id, b.body.id, 'Both responses must return the same id');
  assert.equal(a.body.idempotencyKey, b.body.idempotencyKey, 'Both responses must have same idempotencyKey');
  assert.equal(a.body.idempotencyKey, idem);
});

test('idempotency: concurrent requests with same key must not create duplicates', async () => {
  const PORT = 19093;
  const proc = startServer({ API_KEY: 'k', PORT: String(PORT), DATABASE_URL: `./data/test_idem_conc_${PORT}.db` });
  await waitForPort(PORT);

  const base = `http://localhost:${PORT}`;
  const idem = `idem-conc-${Date.now()}`;

  // Fire 5 concurrent requests with the same idempotency key
  const results = await Promise.all(
    Array.from({ length: 5 }, () =>
      postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k', 'idempotency-key': idem },
        body: { userId: 'u2', type: 'alert', payload: 'concurrent' },
      })
    )
  );

  proc.kill();

  // All must succeed (200 or 201)
  for (const r of results) {
    assert.ok(r.status === 200 || r.status === 201, `Expected 200/201, got ${r.status}: ${JSON.stringify(r.body)}`);
  }

  // All must return the exact same id — no duplicates
  const ids = results.map((r) => r.body.id);
  assert.ok(
    ids.every((id) => id === ids[0]),
    `All concurrent responses must have same id, got: ${JSON.stringify(ids)}`
  );
});

test('idempotency: no idempotency-key creates independent records', async () => {
  const PORT = 19094;
  const proc = startServer({ API_KEY: 'k', PORT: String(PORT), DATABASE_URL: `./data/test_idem_nokey_${PORT}.db` });
  await waitForPort(PORT);

  const base = `http://localhost:${PORT}`;

  const a = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k' },
    body: { userId: 'u3', type: 'note', payload: 'first' },
  });
  const b = await postJson(`${base}/v1/signals`, {
    headers: { 'x-api-key': 'k' },
    body: { userId: 'u3', type: 'note', payload: 'second' },
  });

  proc.kill();

  assert.ok(a.status === 201, `Expected 201, got ${a.status}`);
  assert.ok(b.status === 201, `Expected 201, got ${b.status}`);
  assert.notEqual(a.body.id, b.body.id, 'Without idempotency key each request creates a new record');
});
