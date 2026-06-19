/**
 * In-process fixed-window rate limiter.
 * Concurrency-safe within a single Node.js process (single-threaded event loop).
 *
 * For multi-instance deployments, replace this Map with Redis INCR + EXPIRE
 * (see SCALE.md). The interface (checkAndConsume) stays the same.
 */

const RATE = Number(process.env.RATE_LIMIT_PER_MIN || 5);
const WINDOW_MS = 60_000;

// Map<userId, { windowStart: number, count: number }>
const buckets = new Map();

/**
 * Atomically check and consume one token for userId.
 * Returns { ok, remaining, resetMs }.
 */
export function checkAndConsume(userId, nowMs = Date.now()) {
  let ent = buckets.get(userId);

  if (!ent || nowMs >= ent.windowStart + WINDOW_MS) {
    // Start a fresh window
    ent = { windowStart: nowMs, count: 0 };
  }

  ent.count += 1;
  buckets.set(userId, ent);

  const ok = ent.count <= RATE;
  const resetMs = ent.windowStart + WINDOW_MS;
  const remaining = Math.max(RATE - ent.count, 0);

  return { ok, remaining, resetMs };
}
