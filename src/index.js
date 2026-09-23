import {
  openSync,
  closeSync,
  writeSync,
  fsyncSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  truncateSync,
  ftruncateSync,
  renameSync,
  rmSync,
  existsSync,
} from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_NAME = 'bus.jsonl';
const SNAPSHOT_NAME = 'bus.snapshot.json';
const SNAPSHOT_TMP_NAME = 'bus.snapshot.tmp';

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

/**
 * Split a jsonl buffer into parsed entries. `durable` is the offset just
 * past the final newline; anything beyond it is a torn trailing write.
 */
function parseLog(raw) {
  const entries = [];
  let start = 0;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] === 0x0a) {
      if (i > start) entries.push(JSON.parse(raw.toString('utf8', start, i)));
      start = i + 1;
    }
  }
  return { entries, durable: start };
}

export function createBus({ path: dir, fsync = false } = {}) {
  if (typeof dir !== 'string' || dir.length === 0) {
    throw new TypeError('createBus: "path" must be a non-empty string');
  }

  const logPath = path.join(dir, LOG_NAME);
  const snapshotPath = path.join(dir, SNAPSHOT_NAME);
  const snapshotTmpPath = path.join(dir, SNAPSHOT_TMP_NAME);

  let seq = 0; // seq of the last effective message
  let bytes = 0;
  let published = 0;
  let replayed = 0;
  // dedupKey -> { id, seq } for effective messages
  const dedup = new Map();
  // dedupKey -> { promise, resolve, reject } for first publish in flight
  const pending = new Map();
  // consumer name -> position (seq of the last consumed message; 0 = none)
  const positions = new Map();
  // generation of the newest durable snapshot; 0 = no snapshot yet
  let snapshotGen = 0;
  // messages folded into the snapshot, served to replay/read after truncation
  let baseMessages = [];

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });

  // A leftover tmp file is a snapshot that crashed mid-write. The pre-compact
  // log is still intact, so the half snapshot carries no state: drop it.
  rmSync(snapshotTmpPath, { force: true });

  if (existsSync(snapshotPath)) {
    const snap = JSON.parse(readFileSync(snapshotPath, 'utf8'));
    snapshotGen = snap.gen;
    seq = snap.seq;
    bytes = snap.bytes;
    published = snap.published;
    replayed = snap.replayed;
    for (const [key, value] of snap.dedup) dedup.set(key, value);
    for (const [name, pos] of Object.entries(snap.positions)) positions.set(name, pos);
    baseMessages = snap.messages;
  }

  const applyEntry = (entry) => {
    if (entry.t === 'm') {
      seq = entry.seq;
      bytes += entry.bytes;
      published += 1;
      if (typeof entry.d === 'string') {
        dedup.set(entry.d, { id: entry.id, seq: entry.seq });
      }
    } else if (entry.t === 's') {
      replayed += entry.n;
    } else if (entry.t === 'p') {
      positions.set(entry.name, entry.pos);
    }
  };

  const markerLine = (gen) => JSON.stringify({ t: 'c', gen }) + '\n';

  if (existsSync(logPath)) {
    const raw = readFileSync(logPath);
    const { entries, durable } = parseLog(raw);
    // Bytes past the final newline are a torn (partially durable) trailing
    // write. Drop them: that publish was never acknowledged, so upstream
    // resends it.
    if (durable < raw.length) {
      truncateSync(logPath, durable);
    }
    if (snapshotGen > 0) {
      const first = entries[0];
      if (first && first.t === 'c' && first.gen === snapshotGen) {
        // Live post-compact log: only entries after the marker are new.
        for (const entry of entries.slice(1)) applyEntry(entry);
      } else {
        // Crash between snapshot rename and log truncation (or between
        // truncation and marker write): every byte of this log is already
        // folded into the snapshot. Reset it to just the marker so the
        // state is unambiguous from here on.
        writeFileSync(logPath, markerLine(snapshotGen));
      }
    } else {
      for (const entry of entries) {
        if (entry.t !== 'c') applyEntry(entry);
      }
    }
  } else if (snapshotGen > 0) {
    writeFileSync(logPath, markerLine(snapshotGen));
  }

  const fd = openSync(logPath, 'a');

  let chain = Promise.resolve();
  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is truncated on reopen.
  let broken = false;

  // All disk mutation by publish/replay/compact happens inside this serial
  // chain; writeSync keeps each append atomic with respect to the event loop
  // while still returning a Promise to callers.
  const enqueue = (job) => {
    const run = chain.then(job);
    // A failed job must not stall every later operation.
    chain = run.then(
      () => {},
      () => {},
    );
    return run;
  };

  const writeLine = (entry) => {
    writeSync(fd, Buffer.from(JSON.stringify(entry) + '\n', 'utf8'));
    if (fsync) fsyncSync(fd);
  };

  // Position writes are synchronous (register/advance/read are synchronous
  // methods) and order-independent against 'm'/'s' entries, so they append
  // directly rather than through the chain.
  const persistPosition = (name, pos) => {
    try {
      writeLine({ t: 'p', name, pos });
    } catch (err) {
      broken = true;
      throw err;
    }
  };

  const assertOpen = () => {
    if (closed || broken) {
      throw new Error('bus is closed');
    }
  };

  const assertName = (name) => {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('consumer name must be a non-empty string');
    }
  };

  // Messages with seq > pos, snapshot first then live log, ascending.
  const messagesAfter = (pos) => {
    const out = [];
    for (const m of baseMessages) {
      if (m.seq > pos) {
        // Snapshot messages are shared in-memory objects; hand out a copy so
        // callers cannot mutate replay/read results for later calls.
        out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
      }
    }
    const { entries } = parseLog(readFileSync(logPath));
    for (const entry of entries) {
      if (entry.t === 'm' && entry.seq > pos) {
        out.push({ seq: entry.seq, id: entry.id, record: entry.record });
      }
    }
    return out;
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
          writeLine(entry);
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
        // replay(from) is inclusive of from; messagesAfter takes an
        // exclusive lower bound.
        const out = messagesAfter(from - 1);
        const n = out.length;
        if (n > 0) {
          try {
            writeLine({ t: 's', n });
          } catch (err) {
            broken = true;
            throw err;
          }
          replayed += n;
        }
        return out;
      });
    },

    register(name) {
      assertOpen();
      assertName(name);
      const current = positions.get(name);
      // Re-registering an existing name must not reset its position.
      if (current !== undefined) {
        return current;
      }
      persistPosition(name, 0);
      positions.set(name, 0);
      return 0;
    },

    advance(name, to) {
      assertOpen();
      assertName(name);
      if (typeof to !== 'number' || !Number.isInteger(to) || to < 0) {
        throw new RangeError('advance: position must be a non-negative integer');
      }
      const current = positions.get(name);
      if (current !== undefined) {
        if (to < current) {
          throw new RangeError('advance: position cannot move backwards');
        }
        if (to === current) {
          return current;
        }
      }
      // An unknown name is auto-registered, same as register() first.
      persistPosition(name, to);
      positions.set(name, to);
      return to;
    },

    read(name) {
      assertOpen();
      assertName(name);
      let pos = positions.get(name);
      if (pos === undefined) {
        pos = 0;
        persistPosition(name, pos);
        positions.set(name, pos);
      }
      // read never moves the position; advance() is how consumption lands.
      return messagesAfter(pos);
    },

    compact() {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        const { entries } = parseLog(readFileSync(logPath));
        const messages = baseMessages.slice();
        for (const entry of entries) {
          if (entry.t === 'm') {
            messages.push({ seq: entry.seq, id: entry.id, record: entry.record });
          }
        }
        const gen = snapshotGen + 1;
        const snapshot = {
          v: 1,
          gen,
          seq,
          bytes,
          published,
          replayed,
          positions: Object.fromEntries(positions),
          dedup: Array.from(dedup.entries()),
          messages,
        };
        // The snapshot is durable (fsync + atomic rename) before the log is
        // truncated, so a crash anywhere leaves either the intact old log
        // (half tmp snapshot, discarded on reopen) or the new marker-led
        // log — never something in between.
        const tmpFd = openSync(snapshotTmpPath, 'w');
        try {
          writeSync(tmpFd, Buffer.from(JSON.stringify(snapshot) + '\n', 'utf8'));
          fsyncSync(tmpFd);
        } finally {
          closeSync(tmpFd);
        }
        renameSync(snapshotTmpPath, snapshotPath);
        try {
          ftruncateSync(fd, 0);
          writeSync(fd, Buffer.from(markerLine(gen), 'utf8'));
          fsyncSync(fd);
        } catch (err) {
          // If the marker did not land, later appends would sit in a log the
          // recovery path would discard as pre-snapshot. Stop the bus instead.
          broken = true;
          throw err;
        }
        snapshotGen = gen;
        baseMessages = messages;
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
