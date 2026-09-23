import {
  openSync,
  closeSync,
  writeSync,
  fsyncSync,
  mkdirSync,
  readFileSync,
  truncateSync,
  existsSync,
} from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_NAME = 'bus.jsonl';

/**
 * Validate that `value` survives a compact JSON.stringify unchanged.
 * JSON silently drops undefined / functions and turns non-finite numbers
 * into null; the spec rejects those along with BigInt and circular
 * references as TypeError.
 */
function assertJsonSafe(value, stack = new Set()) {
  if (typeof value === 'bigint') {
    throw new TypeError('record contains a BigInt');
  }
  if (typeof value === 'function') {
    throw new TypeError('record contains a function');
  }
  if (typeof value === 'undefined') {
    throw new TypeError('record contains undefined');
  }
  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new TypeError('record contains a non-finite number');
  }
  if (value !== null && typeof value === 'object') {
    // Unbox boxed primitives (e.g. new Number(NaN)) before recursing.
    if (value instanceof Number || value instanceof String || value instanceof Boolean) {
      assertJsonSafe(value.valueOf(), stack);
      return;
    }
    if (stack.has(value)) {
      throw new TypeError('record contains a circular reference');
    }
    stack.add(value);
    if (Array.isArray(value)) {
      for (const item of value) assertJsonSafe(item, stack);
    } else {
      for (const key of Object.keys(value)) assertJsonSafe(value[key], stack);
    }
    stack.delete(value);
  }
}

export function createBus({ path: dir, fsync = false } = {}) {
  if (typeof dir !== 'string' || dir.length === 0) {
    throw new TypeError('createBus: "path" must be a non-empty string');
  }

  const logPath = path.join(dir, LOG_NAME);

  let seq = 0; // seq of the last effective message
  let bytes = 0;
  let published = 0;
  let replayed = 0;
  // dedupKey -> { id, seq } for effective messages
  const dedup = new Map();
  // dedupKey -> { promise, resolve, reject } for first publish in flight
  const pending = new Map();

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });
  if (existsSync(logPath)) {
    const raw = readFileSync(logPath);
    let start = 0;
    const applyLine = (line) => {
      const entry = JSON.parse(line);
      if (entry.t === 'm') {
        seq = entry.seq;
        bytes += entry.bytes;
        published += 1;
        if (typeof entry.d === 'string') {
          dedup.set(entry.d, { id: entry.id, seq: entry.seq });
        }
      } else if (entry.t === 's') {
        replayed += entry.n;
      }
    };
    for (let i = 0; i < raw.length; i++) {
      if (raw[i] === 0x0a) {
        const line = raw.toString('utf8', start, i);
        start = i + 1;
        if (line.length > 0) applyLine(line);
      }
    }
    // Bytes past the final newline are a torn (partially durable) trailing
    // write. Drop them: that publish was never acknowledged, so upstream
    // resends it.
    if (start < raw.length) {
      truncateSync(logPath, start);
    }
  }
  const fd = openSync(logPath, 'a');

  let chain = Promise.resolve();
  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is truncated on reopen.
  let broken = false;

  // All disk mutation happens inside this serial chain; writeSync keeps each
  // append atomic with respect to the event loop while still returning a
  // Promise to callers.
  const enqueue = (job) => {
    const run = chain.then(job);
    // A failed job must not stall every later operation.
    chain = run.then(
      () => {},
      () => {},
    );
    return run;
  };

  const bus = {
    publish(record) {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      if (record === null || typeof record !== 'object' || Array.isArray(record)) {
        return Promise.reject(new TypeError('publish: record must be a JSON object'));
      }
      const dedupKey = record.dedupKey;
      if (dedupKey !== undefined && typeof dedupKey !== 'string') {
        return Promise.reject(new TypeError('publish: dedupKey must be a string'));
      }
      try {
        assertJsonSafe(record);
      } catch (err) {
        return Promise.reject(err);
      }
      const size = Buffer.byteLength(JSON.stringify(record), 'utf8');

      // Duplicate of an effective message (possibly recovered from disk).
      if (dedupKey !== undefined) {
        const first = dedup.get(dedupKey);
        if (first) {
          return Promise.resolve({ id: first.id, seq: first.seq });
        }
        const inflight = pending.get(dedupKey);
        if (inflight) {
          return inflight.promise;
        }
      }

      // Reserve in-flight dedup at call time so concurrent resends of the
      // same order share the first result; FIFO enqueue assigns seq in
      // invocation order.
      let resolveResult;
      let rejectResult;
      const promise = new Promise((resolve, reject) => {
        resolveResult = resolve;
        rejectResult = reject;
      });
      if (dedupKey !== undefined) {
        pending.set(dedupKey, { promise, resolve: resolveResult, reject: rejectResult });
      }

      enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        const mySeq = seq + 1;
        const id = randomUUID();
        const entry = { t: 'm', seq: mySeq, id, bytes: size, record };
        if (dedupKey !== undefined) entry.d = dedupKey;
        try {
          // Durable before acknowledgement: state changes only after the
          // write (and optional fsync) succeeds.
          writeSync(fd, Buffer.from(JSON.stringify(entry) + '\n', 'utf8'));
          if (fsync) fsyncSync(fd);
        } catch (err) {
          broken = true;
          throw err;
        }
        seq = mySeq;
        bytes += size;
        published += 1;
        if (dedupKey !== undefined) {
          dedup.set(dedupKey, { id, seq: mySeq });
          pending.delete(dedupKey);
          resolveResult({ id, seq: mySeq });
        }
        return { id, seq: mySeq };
      }).then(
        (ack) => {
          if (dedupKey === undefined) resolveResult(ack);
        },
        (err) => {
          if (dedupKey !== undefined) pending.delete(dedupKey);
          rejectResult(err);
        },
      );

      return promise;
    },

    replay(from = 0) {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      if (typeof from !== 'number' || !Number.isInteger(from) || from < 0) {
        return Promise.reject(new RangeError('replay: from must be a non-negative integer'));
      }

      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        const raw = readFileSync(logPath);
        const out = [];
        let start = 0;
        for (let i = 0; i < raw.length; i++) {
          if (raw[i] === 0x0a) {
            const line = raw.toString('utf8', start, i);
            start = i + 1;
            if (line.length > 0) {
              const entry = JSON.parse(line);
              if (entry.t === 'm' && entry.seq >= from) {
                out.push({ seq: entry.seq, id: entry.id, record: entry.record });
              }
            }
          }
        }
        const n = out.length;
        if (n > 0) {
          try {
            writeSync(fd, Buffer.from(JSON.stringify({ t: 's', n }) + '\n', 'utf8'));
            if (fsync) fsyncSync(fd);
          } catch (err) {
            broken = true;
            throw err;
          }
          replayed += n;
        }
        return out;
      });
    },

    stats() {
      return { seq, bytes, published, replayed };
    },

    close() {
      if (closed) {
        return Promise.resolve();
      }
      closed = true;
      // Reject any publish whose dedup reservation is still queued.
      for (const [key, p] of pending) {
        p.reject(new Error('bus is closed'));
        pending.delete(key);
      }
      return enqueue(() => {
        try {
          if (fsync) fsyncSync(fd);
        } finally {
          closeSync(fd);
        }
      });
    },
  };

  return bus;
}

export default { createBus };
