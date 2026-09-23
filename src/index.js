import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_FILE = 'messages.log';
const STATS_FILE = 'stats.json';

function assertJsonValue(value, seen) {
  if (value === null) return;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return;
  if (t === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('record contains a non-finite number');
    }
    return;
  }
  if (t === 'object') {
    if (seen.has(value)) {
      throw new TypeError('record contains a circular reference');
    }
    seen.add(value);
    if (Array.isArray(value)) {
      for (const item of value) assertJsonValue(item, seen);
    } else {
      for (const key of Object.keys(value)) assertJsonValue(value[key], seen);
    }
    seen.delete(value);
    return;
  }
  throw new TypeError(`record contains a value that JSON cannot hold: ${t}`);
}

function validateRecord(record) {
  if (record === null || typeof record !== 'object' || Array.isArray(record)) {
    throw new TypeError('record must be a JSON object');
  }
  if ('dedupKey' in record && typeof record.dedupKey !== 'string') {
    throw new TypeError('dedupKey must be a string when present');
  }
  assertJsonValue(record, new Set());
}

export function createBus(options = {}) {
  const dir = options === null || typeof options !== 'object' ? undefined : options.path;
  if (typeof dir !== 'string' || dir.length === 0) {
    throw new TypeError('createBus requires a non-empty string `path`');
  }
  const useFsync = Boolean(options.fsync);

  fs.mkdirSync(dir, { recursive: true });
  const logPath = path.join(dir, LOG_FILE);
  const statsPath = path.join(dir, STATS_FILE);

  const messages = [];
  const dedup = new Map();
  let lastSeq = 0;
  let published = 0;
  let bytes = 0;
  let replayed = 0;

  const absorb = (entry) => {
    const record = entry.record;
    messages.push({ seq: entry.seq, id: entry.id, record });
    if (typeof entry.dedupKey === 'string') {
      const receipt = { id: entry.id, seq: entry.seq };
      dedup.set(entry.dedupKey, { ...receipt, promise: Promise.resolve(receipt) });
    }
    lastSeq = entry.seq;
    published += 1;
    bytes += Buffer.byteLength(JSON.stringify(record), 'utf8');
  };

  // Reload the append-only log, dropping any torn tail left by a crash.
  let raw = null;
  try {
    raw = fs.readFileSync(logPath, 'utf8');
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
  if (raw !== null && raw.length > 0) {
    const completeEnd = raw.endsWith('\n') ? raw.length : raw.lastIndexOf('\n') + 1;
    const lines = completeEnd > 0 ? raw.slice(0, completeEnd - 1).split('\n') : [];
    let good = 0;
    for (const line of lines) {
      let entry;
      try {
        entry = JSON.parse(line);
      } catch {
        break;
      }
      absorb(entry);
      good += 1;
    }
    const kept = good === 0 ? '' : `${lines.slice(0, good).join('\n')}\n`;
    const keptBytes = Buffer.byteLength(kept, 'utf8');
    if (keptBytes < Buffer.byteLength(raw, 'utf8')) {
      fs.truncateSync(logPath, keptBytes);
    }
  }

  try {
    const saved = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
    if (Number.isSafeInteger(saved.replayed) && saved.replayed >= 0) {
      replayed = saved.replayed;
    }
  } catch {
    // No usable stats file yet; counters derived from the log still hold.
  }

  const fd = fs.openSync(logPath, 'a');

  let closed = false;
  let closePromise = null;
  let queue = Promise.resolve();
  const enqueue = (job) => {
    const result = queue.then(job);
    queue = result.catch(() => {});
    return result;
  };

  const persistStats = () => {
    const tmp = `${statsPath}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ replayed }));
    fs.renameSync(tmp, statsPath);
  };

  const publish = (record) => {
    if (closed) return Promise.reject(new Error('bus is closed'));
    let recordJson;
    try {
      validateRecord(record);
      recordJson = JSON.stringify(record);
    } catch (err) {
      return Promise.reject(err);
    }

    const key = record.dedupKey;
    if (typeof key === 'string') {
      const seen = dedup.get(key);
      if (seen) return seen.promise.then(({ id, seq }) => ({ id, seq }));
    }

    const seq = lastSeq + 1;
    const id = randomUUID();
    const entry =
      typeof key === 'string' ? { seq, id, dedupKey: key, record } : { seq, id, record };
    const line = Buffer.from(`${JSON.stringify(entry)}\n`, 'utf8');

    lastSeq = seq;
    published += 1;
    bytes += Buffer.byteLength(recordJson, 'utf8');
    messages.push({ seq, id, record: JSON.parse(recordJson) });

    const promise = enqueue(() => {
      let off = 0;
      while (off < line.length) off += fs.writeSync(fd, line, off, line.length - off);
      if (useFsync) fs.fsyncSync(fd);
    }).then(() => ({ id, seq }));

    if (typeof key === 'string') dedup.set(key, { id, seq, promise });
    return promise;
  };

  const replay = (from = 0) => {
    if (closed) return Promise.reject(new Error('bus is closed'));
    if (!Number.isInteger(from) || from < 0) {
      return Promise.reject(new RangeError('from must be a non-negative integer'));
    }
    const out = [];
    for (const m of messages) {
      if (m.seq > from) {
        out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
      }
    }
    replayed += out.length;
    return enqueue(persistStats).then(() => out);
  };

  const stats = () => ({ seq: lastSeq, bytes, published, replayed });

  const close = () => {
    if (closePromise === null) {
      closed = true;
      closePromise = enqueue(() => {
        fs.closeSync(fd);
      }).then(() => {});
    }
    return closePromise;
  };

  return { publish, replay, stats, close };
}
