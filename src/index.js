import {
  openSync,
  closeSync,
  writeSync,
  fsyncSync,
  mkdirSync,
  readFileSync,
  truncateSync,
  ftruncateSync,
  renameSync,
  unlinkSync,
  existsSync,
} from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_NAME = 'bus.jsonl';
const SNAP_NAME = 'snapshot.json';
const SNAP_TMP_NAME = 'snapshot.tmp';

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

function assertConsumerName(name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new TypeError('consumer name must be a non-empty string');
  }
}

export function createBus({ path: dir, fsync = false } = {}) {
  if (typeof dir !== 'string' || dir.length === 0) {
    throw new TypeError('createBus: "path" must be a non-empty string');
  }

  const logPath = path.join(dir, LOG_NAME);
  const snapPath = path.join(dir, SNAP_NAME);
  const snapTmpPath = path.join(dir, SNAP_TMP_NAME);

  let seq = 0; // seq of the last effective message
  let bytes = 0;
  let published = 0;
  let replayed = 0;
  // All effective messages, in seq order. The prefix seq<=snapSeq lives in
  // the snapshot file, the rest in the log tail; after recovery both halves
  // are joined here so replay/read never parse disk on the hot path.
  const messages = [];
  // seq of the newest message covered by snapshot.json on disk (0 = none).
  let snapSeq = 0;
  // dedupKey -> { id, seq } for effective messages
  const dedup = new Map();
  // consumer name -> consumed position (seq of the last consumed message)
  const offsets = new Map();
  // dedupKey -> { promise, resolve, reject } for first publish in flight
  const pending = new Map();

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });

  // A staged temp snapshot is always discardable: the committed snapshot
  // (if any) plus the log already contain everything durably committed.
  if (existsSync(snapTmpPath)) {
    try {
      unlinkSync(snapTmpPath);
    } catch {
      // best effort; a fresh temp is written with O_TRUNC below
    }
  }

  if (existsSync(snapPath)) {
    const snap = JSON.parse(readFileSync(snapPath, 'utf8'));
    if (snap.v !== 1) {
      throw new Error(`unsupported snapshot version: ${snap.v}`);
    }
    seq = snap.seq;
    bytes = snap.bytes;
    published = snap.published;
    replayed = snap.replayed;
    snapSeq = snap.seq;
    for (const m of snap.messages) {
      messages.push(m);
      if (typeof m.d === 'string') {
        dedup.set(m.d, { id: m.id, seq: m.seq });
      }
    }
    for (const [name, pos] of Object.entries(snap.offsets ?? {})) {
      offsets.set(name, pos);
    }
  }

  if (existsSync(logPath)) {
    const raw = readFileSync(logPath);
    let start = 0;
    const lines = [];
    for (let i = 0; i < raw.length; i++) {
      if (raw[i] === 0x0a) {
        const line = raw.toString('utf8', start, i);
        start = i + 1;
        if (line.length > 0) lines.push(line);
      }
    }
    const tornTail = start < raw.length;

    // Decide first, apply second: if the log still carries any message
    // already covered by the snapshot, compaction committed the rename but
    // crashed (or was killed) before truncating. The serial write chain
    // guarantees no post-snapshot record can coexist with that stale prefix,
    // so the whole log is obsolete — discard it and every marker in it (the
    // snapshot already holds equal-or-newer offsets and counters).
    const stale =
      snapSeq > 0 &&
      lines.some((line) => {
        try {
          const entry = JSON.parse(line);
          return entry.t === 'm' && entry.seq <= snapSeq;
        } catch {
          return false;
        }
      });

    if (stale) {
      truncateSync(logPath, 0);
    } else {
      for (const line of lines) {
        const entry = JSON.parse(line);
        if (entry.t === 'm') {
          const msg = { seq: entry.seq, id: entry.id, record: entry.record };
          if (typeof entry.d === 'string') {
            msg.d = entry.d;
            dedup.set(entry.d, { id: entry.id, seq: entry.seq });
          }
          messages.push(msg);
          seq = entry.seq;
          bytes += entry.bytes;
          published += 1;
        } else if (entry.t === 's') {
          // Current writes carry the cumulative total (idempotent if a stale
          // pre-truncation marker is seen twice); old logs only had `n`.
          if (Number.isFinite(entry.total)) {
            replayed = Math.max(replayed, entry.total);
          } else {
            replayed += entry.n;
          }
        } else if (entry.t === 'o') {
          // Positions only move forward, so max() makes overlapping stale
          // records harmless while still recovering the latest durable value.
          offsets.set(entry.name, Math.max(offsets.get(entry.name) ?? 0, entry.pos));
        }
      }
      if (tornTail) {
        // Bytes past the final newline are a torn (partially durable)
        // trailing write. Drop them: that publish was never acknowledged,
        // so upstream resends it.
        truncateSync(logPath, start);
      }
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

  // Must be called from inside the serial chain.
  const appendEntry = (entry) => {
    try {
      writeSync(fd, Buffer.from(JSON.stringify(entry) + '\n', 'utf8'));
      if (fsync) fsyncSync(fd);
    } catch (err) {
      broken = true;
      throw err;
    }
  };

  const publicView = (m) => ({ seq: m.seq, id: m.id, record: m.record });

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
      // Detach the stored record from the caller's object so later mutation
      // (or snapshotting) always sees exactly what was acknowledged.
      const storedRecord = JSON.parse(JSON.stringify(record));

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
        const entry = { t: 'm', seq: mySeq, id, bytes: size, record: storedRecord };
        const msg = { seq: mySeq, id, record: storedRecord };
        if (dedupKey !== undefined) {
          entry.d = dedupKey;
          msg.d = dedupKey;
        }
        try {
          // Durable before acknowledgement: state changes only after the
          // write (and optional fsync) succeeds.
          appendEntry(entry);
        } catch (err) {
          broken = true;
          throw err;
        }
        seq = mySeq;
        bytes += size;
        published += 1;
        messages.push(msg);
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
        const out = [];
        for (const m of messages) {
          if (m.seq >= from) out.push(publicView(m));
        }
        const n = out.length;
        if (n > 0) {
          // `total` makes the marker safe to apply twice if compaction
          // committed its snapshot before truncating the old log.
          appendEntry({ t: 's', n, total: replayed + n });
          replayed += n;
        }
        return out;
      });
    },

    register(name) {
      assertConsumerName(name);
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        if (!offsets.has(name)) {
          appendEntry({ t: 'o', name, pos: 0 });
          offsets.set(name, 0);
        }
        return offsets.get(name);
      });
    },

    advance(name, position) {
      assertConsumerName(name);
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      if (typeof position !== 'number' || !Number.isInteger(position) || position < 0) {
        return Promise.reject(new RangeError('advance: position must be a non-negative integer'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        const current = offsets.get(name);
        if (current === undefined) {
          // Unknown names are registered on first use, exactly as if
          // register() had been called first.
          appendEntry({ t: 'o', name, pos: position });
          offsets.set(name, position);
          return position;
        }
        if (position < current) {
          throw new RangeError(
            `advance: position ${position} is behind current position ${current}`,
          );
        }
        if (position > current) {
          appendEntry({ t: 'o', name, pos: position });
          offsets.set(name, position);
        }
        // Setting it to the current value is a successful no-op.
        return position;
      });
    },

    read(name) {
      assertConsumerName(name);
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        if (!offsets.has(name)) {
          // Auto-registration lands at zero: nothing consumed yet.
          appendEntry({ t: 'o', name, pos: 0 });
          offsets.set(name, 0);
        }
        const pos = offsets.get(name);
        // Seqs are contiguous 1..n, so position p means "next index is p".
        // read() never moves the position; callers land it with advance().
        return messages.slice(pos).map(publicView);
      });
    },

    compact() {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        const snapshot = {
          v: 1,
          seq,
          bytes,
          published,
          replayed,
          offsets: Object.fromEntries(offsets),
          messages: messages.map((m) => {
            const out = { seq: m.seq, id: m.id, record: m.record };
            if (m.d !== undefined) out.d = m.d;
            return out;
          }),
        };

        // Commit protocol, crash-safe at every step:
        //   1. full temp snapshot, fsync its data
        //   2. atomic rename onto snapshot.json, fsync the directory
        //   3. only then truncate the obsolete log prefix (the whole log
        //      here, since the snapshot covers seq 1..seq)
        // Any earlier crash leaves the previous snapshot + full log; the
        // rename-without-truncate crash leaves an overlapping log that
        // recovery discards.
        let tmpFd;
        try {
          tmpFd = openSync(snapTmpPath, 'w');
          writeSync(tmpFd, Buffer.from(JSON.stringify(snapshot), 'utf8'));
          fsyncSync(tmpFd);
        } catch (err) {
          if (tmpFd !== undefined) {
            try {
              closeSync(tmpFd);
            } catch {
              // best effort
            }
          }
          try {
            unlinkSync(snapTmpPath);
          } catch {
            // best effort; recovery removes stale temps
          }
          throw err;
        }
        closeSync(tmpFd);

        renameSync(snapTmpPath, snapPath);
        const dirFd = openSync(dir, 'r');
        try {
          fsyncSync(dirFd);
        } finally {
          closeSync(dirFd);
        }

        try {
          ftruncateSync(fd, 0);
          if (fsync) fsyncSync(fd);
        } catch (err) {
          broken = true;
          throw err;
        }
        snapSeq = seq;
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
