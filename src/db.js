/**
 * db.js — uses Node.js built-in node:sqlite (stable since Node 22.5 / Node 24).
 * No native compilation needed — zero npm install failures.
 *
 * Drop-in replacement for better-sqlite3 API surface used by this project.
 * Error codes emitted by node:sqlite for constraint violations use
 * code: 'ERR_SQLITE_ERROR' with the SQLite extended error code in
 * err.errcode (e.g. SQLITE_CONSTRAINT_UNIQUE = 2067).
 * We normalise these to match the codes the rest of the app expects.
 */

import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new DatabaseSync(dbPath);

// Enable WAL mode for better concurrent read performance
db.exec('PRAGMA journal_mode=WAL;');
db.exec('PRAGMA synchronous=NORMAL;');

// Schema — idempotency_key has a UNIQUE constraint (atomic idempotency guard)
db.exec(`
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  idempotency_key TEXT UNIQUE,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_created ON signals(user_id, created_at);
`);

// ---------------------------------------------------------------------------
// Failure simulation (for test harness)
// ---------------------------------------------------------------------------
function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Normalise node:sqlite constraint errors so signals.js can catch them
// by the same codes used with better-sqlite3.
// ---------------------------------------------------------------------------
function normaliseError(e) {
  // node:sqlite throws ERR_SQLITE_ERROR; SQLite UNIQUE violation = errcode 2067
  if (e.code === 'ERR_SQLITE_ERROR') {
    if (e.errcode === 2067 || (e.message && e.message.includes('UNIQUE constraint failed'))) {
      e.code = 'SQLITE_CONSTRAINT_UNIQUE';
    } else if (e.errcode === 5 || (e.message && e.message.includes('database is locked'))) {
      e.code = 'SQLITE_BUSY';
    }
  }
  return e;
}

// ---------------------------------------------------------------------------
// Public API — synchronous (matches better-sqlite3 interface)
// ---------------------------------------------------------------------------

export function insertSignal(userId, type, payload, idemKey, nowMs) {
  maybeFail();
  try {
    const stmt = db.prepare(
      'INSERT INTO signals (user_id, type, payload, idempotency_key, created_at) VALUES (?,?,?,?,?)'
    );
    return stmt.run(userId, type, String(payload), idemKey || null, nowMs);
  } catch (e) {
    throw normaliseError(e);
  }
}

export function getByIdemKey(idemKey) {
  maybeFail();
  try {
    const stmt = db.prepare(
      `SELECT id,
              user_id        AS userId,
              type,
              payload,
              idempotency_key AS idempotencyKey,
              created_at     AS createdAt
       FROM signals WHERE idempotency_key = ?`
    );
    return stmt.get(idemKey);
  } catch (e) {
    throw normaliseError(e);
  }
}

export function listSignals(userId, limit) {
  maybeFail();
  try {
    const stmt = db.prepare(
      `SELECT id,
              user_id        AS userId,
              type,
              payload,
              idempotency_key AS idempotencyKey,
              created_at     AS createdAt
       FROM signals WHERE user_id = ? ORDER BY created_at DESC LIMIT ?`
    );
    return stmt.all(userId, limit);
  } catch (e) {
    throw normaliseError(e);
  }
}
