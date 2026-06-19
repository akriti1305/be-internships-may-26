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

async function postStatus(url, { headers = {}, body }) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode));
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

test('rate limit: allow 5 per minute, 6th is 429', async () => {
  const PORT = 19092;
  const proc = startServer({
    API_KEY: 'k',
    PORT: String(PORT),
    RATE_LIMIT_PER_MIN: '5',
    DATABASE_URL: `./data/test_rate_${PORT}.db`,
  });
  await waitForPort(PORT);

  const base = `http://localhost:${PORT}`;
  const statuses = [];

  for (let i = 0; i < 6; i++) {
    const code = await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u_rate', type: 'note', payload: String(i) },
    });
    statuses.push(code);
  }

  proc.kill();

  const counts = statuses.reduce((acc, c) => ({ ...acc, [c]: (acc[c] || 0) + 1 }), {});
  assert.ok(counts[201] >= 5, `Expected at least 5 successes (201), got: ${JSON.stringify(counts)}`);
  assert.ok(counts[429] >= 1, `Expected at least 1 rate-limited (429), got: ${JSON.stringify(counts)}`);
});

test('rate limit: different users have independent counters (unit)', async () => {
  // Unit test of checkAndConsume — no server spawn needed
  const { checkAndConsume } = await import('../src/rateLimit.js');
  const now = Date.now();

  // Use unique user ids to avoid interference from the integration test above
  const ua = 'unit_user_a_' + now;
  const ub = 'unit_user_b_' + now;

  const resA = [
    checkAndConsume(ua, now),
    checkAndConsume(ua, now + 1),
    checkAndConsume(ua, now + 2),
  ];
  const resB = [
    checkAndConsume(ub, now),
    checkAndConsume(ub, now + 1),
    checkAndConsume(ub, now + 2),
  ];

  // Both users' first requests should be ok
  assert.equal(resA[0].ok, true, 'user_a request 1 should be ok');
  assert.equal(resB[0].ok, true, 'user_b request 1 should be ok (independent of user_a)');

  // Response shape
  assert.ok('remaining' in resA[0], 'must have remaining field');
  assert.ok('resetMs' in resA[0], 'must have resetMs field');

  // Independent buckets — same number of calls → same remaining
  assert.equal(
    resA[2].remaining,
    resB[2].remaining,
    'Independent users with same # calls must have identical remaining'
  );
});

test('rate limit: response includes remaining and resetMs fields', async () => {
  const PORT = 19096;
  const proc = startServer({
    API_KEY: 'k',
    PORT: String(PORT),
    RATE_LIMIT_PER_MIN: '5',
    DATABASE_URL: `./data/test_rate_meta_${PORT}.db`,
  });
  await waitForPort(PORT);

  const base = `http://localhost:${PORT}`;

  // Exhaust the limit
  for (let i = 0; i < 5; i++) {
    await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u_fields', type: 'x', payload: String(i) },
    });
  }

  // 6th request should be 429 with rate-limit metadata
  const body = await new Promise((resolve, reject) => {
    const data = JSON.stringify({ userId: 'u_fields', type: 'x', payload: '6' });
    const req = http.request(
      `${base}/v1/signals`,
      { method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': 'k' } },
      (res) => {
        let chunks = '';
        res.on('data', (d) => (chunks += d));
        res.on('end', () => resolve(JSON.parse(chunks || '{}')));
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });

  proc.kill();

  assert.ok('remaining' in body, 'Rate-limit response must include remaining');
  assert.ok('resetMs' in body, 'Rate-limit response must include resetMs');
  assert.equal(body.remaining, 0);
});
