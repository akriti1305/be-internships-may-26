import { insertSignal, getByIdemKey, listSignals } from './db.js';
import { checkAndConsume } from './rateLimit.js';

function nowMs() { return Date.now(); }

/**
 * Retry with exponential backoff + jitter.
 * Only retries on SQLITE_BUSY or simulated_db_failure.
 * This prevents duplicates: for inserts, the DB UNIQUE constraint is the
 * idempotency guard — a retried insert either succeeds or throws
 * SQLITE_CONSTRAINT_UNIQUE (handled separately), never creates a duplicate.
 */
async function retry(fn, retries = 3) {
  let delay = 50;
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      const isTransient =
        e.code === 'SQLITE_BUSY' ||
        e.message === 'simulated_db_failure';

      if (!isTransient) throw e;
      if (i === retries - 1) throw e;

      // Exponential backoff with full jitter
      await new Promise(r => setTimeout(r, delay + Math.random() * delay));
      delay *= 2;
    }
  }
}

export async function postSignal(req, reply) {
  const idem = req.headers['idempotency-key'] || null;
  const { userId, type, payload } = req.body || {};

  if (!userId || !type || typeof payload === 'undefined') {
    return reply.code(400).send({ error: 'invalid_body' });
  }

  const { ok, remaining, resetMs } = checkAndConsume(userId, nowMs());
  if (!ok) {
    return reply.code(429).send({ error: 'rate_limited', remaining, resetMs });
  }

  try {
    const t = nowMs();
    // Attempt atomic insert — DB UNIQUE constraint on idempotency_key
    // prevents duplicates even under concurrent requests.
    const info = await retry(() => insertSignal(userId, type, payload, idem, t));

    return reply.code(201).send({
      id: info.lastInsertRowid,
      userId,
      type,
      payload: String(payload),
      idempotencyKey: idem,
      createdAt: t,
    });

  } catch (e) {
    // Idempotent: duplicate idempotency key → return existing record
    if (idem && (e.code === 'SQLITE_CONSTRAINT_UNIQUE' || e.code === 'SQLITE_CONSTRAINT')) {
      try {
        const existing = await retry(() => getByIdemKey(idem));
        if (existing) {
          return reply.code(200).send(existing);
        }
      } catch (e2) {
        req.log.error({ err: e2, ctx: 'getByIdemKey' });
        return reply.code(503).send({ error: 'db_unavailable' });
      }
    }

    req.log.error({ err: e, ctx: 'insertSignal' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}

export async function getSignals(req, reply) {
  const { userId, limit = 20 } = req.query || {};
  if (!userId) return reply.code(400).send({ error: 'missing_userId' });
  const lim = Math.min(Number(limit) || 20, 100);
  try {
    const rows = await retry(() => listSignals(userId, lim));
    return reply.code(200).send({ items: rows });
  } catch (e) {
    req.log.error({ err: e, ctx: 'listSignals' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}
