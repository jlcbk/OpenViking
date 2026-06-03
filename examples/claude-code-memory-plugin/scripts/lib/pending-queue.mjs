/**
 * Local pending queue for offline resilience.
 *
 * When the OpenViking server is unreachable, write operations (addMessage,
 * commitSession) serialize their payloads to `~/.openviking/pending/` as JSON
 * files. On next session-start, the queue is replayed: each pending item is
 * retried with exponential backoff up to `maxRetries` times.
 *
 * File format: `{timestamp}_{sessionId}_{retryCount}.json`
 * Each file contains: { type, sessionId, payload, createdAt, retries }
 *
 * Config (env vars):
 *   OPENVIKING_PENDING_DIR       — directory for queue files
 *                                    (default: ~/.openviking/pending)
 *   OPENVIKING_PENDING_MAX_RETRIES — max retry attempts per item (default: 3)
 *   OPENVIKING_PENDING_TTL_DAYS  — max age in days before stale cleanup
 *                                    (default: 7)
 */

import { mkdir, readdir, readFile, writeFile, unlink, stat } from "node:fs/promises";
import { join } from "node:path";
import { homedir } from "node:os";

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_TTL_DAYS = 7;
const DEFAULT_PENDING_DIR = () => join(homedir(), ".openviking", "pending");

function getPendingDir() {
  return process.env.OPENVIKING_PENDING_DIR || DEFAULT_PENDING_DIR();
}

function getMaxRetries() {
  const v = parseInt(process.env.OPENVIKING_PENDING_MAX_RETRIES || "", 10);
  return isNaN(v) ? DEFAULT_MAX_RETRIES : v;
}

function getTTLDays() {
  const v = parseInt(process.env.OPENVIKING_PENDING_TTL_DAYS || "", 10);
  return isNaN(v) ? DEFAULT_TTL_DAYS : v;
}

/**
 * Enqueue a failed operation to local disk.
 *
 * @param {string} type — "addMessage" or "commitSession"
 * @param {string} sessionId — OV session ID
 * @param {object} payload — the data that failed to send
 */
export async function enqueue(type, sessionId, payload) {
  const dir = getPendingDir();
  const now = Date.now();
  const filename = `${now}_${sessionId.replace(/[^A-Za-z0-9._-]/g, "_")}_0.json`;
  const entry = {
    type,
    sessionId,
    payload,
    createdAt: now,
    retries: 0,
  };
  try {
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, filename), JSON.stringify(entry), "utf-8");
    return { ok: true, path: filename };
  } catch (err) {
    // Best effort — if we can't write to disk either, there's nothing more we can do.
    return { ok: false, error: err?.message || String(err) };
  }
}

/**
 * List all pending queue entries.
 * Returns array of { filename, entry } sorted by createdAt ascending.
 */
export async function listPending() {
  const dir = getPendingDir();
  let files;
  try {
    files = await readdir(dir);
  } catch {
    return [];
  }

  const entries = [];
  for (const f of files) {
    if (!f.endsWith(".json")) continue;
    try {
      const raw = await readFile(join(dir, f), "utf-8");
      const entry = JSON.parse(raw);
      entries.push({ filename: f, entry });
    } catch {
      // Corrupted file — skip
    }
  }

  // Sort by createdAt ascending (oldest first for replay)
  entries.sort((a, b) => (a.entry.createdAt || 0) - (b.entry.createdAt || 0));
  return entries;
}

/**
 * Remove a pending entry after successful replay.
 */
export async function dequeue(filename) {
  const dir = getPendingDir();
  try {
    await unlink(join(dir, filename));
    return true;
  } catch {
    return false;
  }
}

/**
 * Increment retry count on a pending entry. Returns false if max retries exceeded.
 */
export async function incrementRetry(filename, entry) {
  const dir = getPendingDir();
  const maxRetries = getMaxRetries();
  entry.retries = (entry.retries || 0) + 1;

  if (entry.retries > maxRetries) {
    // Exceeded max retries — remove the entry to avoid infinite accumulation
    try {
      await unlink(join(dir, filename));
    } catch { /* best effort */ }
    return false;
  }

  // Update the file with incremented retry count
  const newFilename = filename.replace(/_\d+\.json$/, `_${entry.retries}.json`);
  try {
    await writeFile(join(dir, newFilename), JSON.stringify(entry), "utf-8");
    if (newFilename !== filename) {
      await unlink(join(dir, filename)).catch(() => {});
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * Clean up stale entries older than TTL.
 */
export async function cleanStale() {
  const ttlMs = getTTLDays() * 24 * 60 * 60 * 1000;
  const now = Date.now();
  const pending = await listPending();
  let cleaned = 0;

  for (const { filename, entry } of pending) {
    const age = now - (entry.createdAt || 0);
    if (age > ttlMs) {
      await dequeue(filename);
      cleaned++;
    }
  }
  return cleaned;
}

/**
 * Replay all pending entries. Call this during session-start when the server
 * is healthy. Each entry is replayed with its original operation type.
 *
 * @param {Function} fetchJSON — the configured fetchJSON from makeFetchJSON
 * @param {Function} log — logger function
 * @returns {{ replayed: number, failed: number, skipped: number }}
 */
export async function replayPending(fetchJSON, log) {
  const pending = await listPending();

  if (pending.length === 0) {
    return { replayed: 0, failed: 0, skipped: 0 };
  }

  log("pending-queue", { count: pending.length, action: "replay-start" });

  let replayed = 0;
  let failed = 0;
  let skipped = 0;

  for (const { filename, entry } of pending) {
    // Check if entry has exceeded max retries
    if ((entry.retries || 0) >= getMaxRetries()) {
      await dequeue(filename);
      skipped++;
      continue;
    }

    let res;
    try {
      const encodedSid = encodeURIComponent(entry.sessionId);
      if (entry.type === "addMessage") {
        res = await fetchJSON(`/api/v1/sessions/${encodedSid}/messages`, {
          method: "POST",
          body: JSON.stringify(entry.payload),
        });
      } else if (entry.type === "commitSession") {
        res = await fetchJSON(`/api/v1/sessions/${encodedSid}/commit`, {
          method: "POST",
          body: JSON.stringify({}),
        });
      } else {
        // Unknown type — skip
        await dequeue(filename);
        skipped++;
        continue;
      }
    } catch {
      res = { ok: false };
    }

    if (res?.ok) {
      await dequeue(filename);
      replayed++;
    } else {
      await incrementRetry(filename, entry);
      failed++;
    }
  }

  // Clean up stale entries after replay
  const cleaned = await cleanStale();

  log("pending-queue", {
    action: "replay-done",
    replayed,
    failed,
    skipped,
    cleaned,
  });

  return { replayed, failed, skipped };
}
