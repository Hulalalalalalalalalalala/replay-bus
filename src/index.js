import {
  openSync,
  closeSync,
  writeSync,
  fsyncSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  truncateSync,
  renameSync,
  rmSync,
  existsSync,
  readdirSync,
  statSync,
} from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_NAME = 'bus.jsonl';
const LOG_TMP_NAME = 'bus.log.tmp';
const SNAPSHOT_NAME = 'bus.snapshot.json';
const SNAPSHOT_TMP_NAME = 'bus.snapshot.tmp';
// The log is a chain of segment files: the active `bus.jsonl` plus finalized
// `bus.<index>.jsonl` segments. Index order is oldest first; an index is
// never reused within the lifetime of a directory.
const SEGMENT_RE = /^bus\.(\d+)\.jsonl$/;
const segmentName = (index) => `bus.${String(index).padStart(10, '0')}.jsonl`;

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
  const logTmpPath = path.join(dir, LOG_TMP_NAME);
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
  // dedupKey -> { promise, resolve, reject } reserved by the first
  // in-flight batch carrying that key; lets later singles/batches share
  // the batch's first acknowledgement just like `pending` does for singles
  const groupPending = new Map();
  // consumer name -> position (seq of the last consumed message; 0 = none)
  const positions = new Map();
  // generation of the newest durable snapshot; 0 = no snapshot yet
  let snapshotGen = 0;
  // messages folded into the snapshot, served to replay/read after truncation
  let baseMessages = [];
  // Every message with seq <= horizon has been discarded by truncate().
  let horizon = 0;
  // Every message with seq <= foldedSeq lives in the snapshot, not the log.
  let foldedSeq = 0;
  // Index handed to the next finalized segment; never reused within a run.
  let nextSegmentIndex = 1;

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });

  // A leftover tmp file is a snapshot that crashed mid-write. The pre-compact
  // log is still intact, so the half snapshot carries no state: drop it.
  rmSync(snapshotTmpPath, { force: true });
  // Likewise a log tmp left behind when compact crashed before the rename:
  // the live log (if any) is still the source of truth.
  rmSync(logTmpPath, { force: true });

  if (existsSync(snapshotPath)) {
    const snap = JSON.parse(readFileSync(snapshotPath, 'utf8'));
    snapshotGen = snap.gen;
    seq = snap.seq;
    bytes = snap.bytes;
    published = snap.published;
    replayed = snap.replayed;
    // Snapshots written before truncation existed carry no horizon.
    horizon = snap.horizon ?? 0;
    foldedSeq = snap.seq;
    for (const [key, value] of snap.dedup) dedup.set(key, value);
    for (const [name, pos] of Object.entries(snap.positions)) positions.set(name, pos);
    baseMessages = snap.messages;
  }

  const applyEntry = (entry) => {
    if (entry.t === 'm') {
      // At/below the horizon the message was discarded by truncation; only
      // its cumulative counters (carried by the marker) survive.
      if (entry.seq <= horizon) return;
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

  // A truncate marker is an absolute checkpoint: the cumulative counters,
  // positions and dedup table it carries replace whatever earlier segments
  // established, so recovery can start from the newest marker and ignore
  // everything before it.
  const applyMarker = (entry) => {
    seq = entry.seq;
    bytes = entry.bytes;
    published = entry.published;
    replayed = entry.replayed;
    horizon = entry.horizon;
    positions.clear();
    for (const [name, pos] of Object.entries(entry.positions)) positions.set(name, pos);
    dedup.clear();
    for (const [key, value] of entry.dedup) dedup.set(key, value);
  };

  const markerLine = (gen) => JSON.stringify({ t: 'c', gen }) + '\n';

  // Replay complete log lines and apply only the committed prefix.
  // A batch is bracketed by {t:'b',id} .. entries .. {t:'bk',id}: a begin
  // without its matching commit (crash mid-write, or overtaken by a newer
  // begin) is severed wholesale and none of its entries take effect.
  // Returns the byte offset just past the last committed line, so both a
  // torn trailing write and an uncommitted batch tail can be truncated.
  const recoverLog = (raw) => {
    let lineStart = 0;
    let committedEnd = 0;
    let group = null;
    for (let i = 0; i < raw.length; i++) {
      if (raw[i] !== 0x0a) continue;
      const entry = i > lineStart ? JSON.parse(raw.toString('utf8', lineStart, i)) : null;
      if (group !== null) {
        if (entry && entry.t === 'bk' && entry.id === group.id) {
          for (const e of group.entries) applyEntry(e);
          group = null;
          committedEnd = i + 1;
        } else if (entry && entry.t === 'b') {
          // The previous group never committed; start tracking the new one.
          group = { id: entry.id, entries: [] };
        } else if (entry) {
          group.entries.push(entry);
        }
      } else if (entry && entry.t === 'b') {
        group = { id: entry.id, entries: [] };
      } else {
        if (entry) {
          if (entry.t === 't') applyMarker(entry);
          else applyEntry(entry);
        }
        committedEnd = i + 1;
      }
      lineStart = i + 1;
    }
    return committedEnd;
  };

  // Finalized segments present in the directory, oldest first.
  const listSegments = () => {
    const out = [];
    for (const name of readdirSync(dir)) {
      const match = SEGMENT_RE.exec(name);
      if (match) out.push({ index: Number(match[1]), path: path.join(dir, name) });
    }
    out.sort((a, b) => a.index - b.index);
    return out;
  };

  // First complete line of a log file, or null when there is none.
  const firstEntry = (filePath) => {
    const raw = readFileSync(filePath);
    const nl = raw.indexOf(0x0a);
    if (nl <= 0) return null;
    try {
      return JSON.parse(raw.toString('utf8', 0, nl));
    } catch {
      return null;
    }
  };

  // Highest message seq in a segment file; null when it carries no messages.
  const segmentMaxSeq = (filePath) => {
    const { entries } = parseLog(readFileSync(filePath));
    let max = null;
    for (const entry of entries) {
      if (entry.t === 'm' && (max === null || entry.seq > max)) max = entry.seq;
    }
    return max;
  };

  const segments = listSegments();
  for (const seg of segments) {
    if (seg.index >= nextSegmentIndex) nextSegmentIndex = seg.index + 1;
  }
  // The ordered log is every finalized segment (oldest first) followed by
  // the active log file.
  const ordered = segments.map((seg) => seg.path);
  if (existsSync(logPath)) ordered.push(logPath);

  // The newest anchor — a segment whose first line is a compact ('c') or
  // truncate ('t') marker for the current snapshot generation — starts the
  // live suffix of the log. Everything before it is folded into the snapshot
  // or checkpointed into that marker.
  let startAt = -1;
  for (let i = 0; i < ordered.length; i++) {
    const first = firstEntry(ordered[i]);
    if (first && (first.t === 'c' || first.t === 't') && first.gen === snapshotGen) {
      startAt = i;
    }
  }

  if (snapshotGen > 0 && startAt < 0) {
    // Crash between the snapshot rename and the log reset: every byte still
    // on disk is already folded into the snapshot. Reset to a marker-led log
    // so the state is unambiguous from here on.
    for (const seg of segments) rmSync(seg.path, { force: true });
    writeFileSync(logPath, markerLine(snapshotGen));
  } else {
    // Replay the live suffix; a torn tail or uncommitted batch is severed.
    for (let i = Math.max(startAt, 0); i < ordered.length; i++) {
      const raw = readFileSync(ordered[i]);
      const committedEnd = recoverLog(raw);
      if (committedEnd < raw.length) truncateSync(ordered[i], committedEnd);
    }
    // Reconcile leftovers of an interrupted truncate/compact: finalized
    // segments before the anchor whose content is folded into the snapshot
    // or at/below the truncation horizon can go. Segments a consumer still
    // needs (above the horizon) stay untouched.
    for (let i = 0; i < startAt; i++) {
      const max = segmentMaxSeq(ordered[i]);
      if (max === null || max <= foldedSeq || max <= horizon) {
        rmSync(ordered[i], { force: true });
      }
    }
  }

  // Reassigned by compact()/truncate() when they swap the active log.
  let fd = openSync(logPath, 'a');

  let chain = Promise.resolve();
  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is truncated on reopen.
  let broken = false;

  // All disk mutation by publish/replay/compact/truncate happens inside this
  // serial chain; writeSync keeps each append atomic with respect to the
  // event loop while still returning a Promise to callers.
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

  // Messages with seq > pos that survived truncation, snapshot first then
  // live segments, ascending. A position below the truncation horizon simply
  // starts at the earliest surviving message: the gap is already truncated.
  const messagesAfter = (pos) => {
    const floor = Math.max(pos, horizon);
    const out = [];
    for (const m of baseMessages) {
      if (m.seq > floor) {
        // Snapshot messages are shared in-memory objects; hand out a copy so
        // callers cannot mutate replay/read results for later calls.
        out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
      }
    }
    // Log entries at/below foldedSeq are covered by the snapshot (a leftover
    // segment from an interrupted compaction can still be on disk).
    const logFloor = Math.max(floor, foldedSeq);
    const collect = (filePath) => {
      const { entries } = parseLog(readFileSync(filePath));
      for (const entry of entries) {
        if (entry.t === 'm' && entry.seq > logFloor) {
          out.push({ seq: entry.seq, id: entry.id, record: entry.record });
        }
      }
    };
    for (const seg of listSegments()) collect(seg.path);
    collect(logPath);
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
        const inflightGroup = groupPending.get(dedupKey);
        if (inflightGroup) {
          return inflightGroup.promise;
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

    publishBatch(records) {
      // Spec: a non-array argument is always a synchronous TypeError.
      if (!Array.isArray(records)) {
        throw new TypeError('publishBatch: records must be an array');
      }
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }

      // Validate and serialize every record up front. Any single bad shape
      // (or value JSON cannot carry) rejects the whole group before the
      // serial chain is touched: seq/bytes/published/dedup/positions stay
      // exactly as they were.
      const prepared = [];
      try {
        for (const record of records) {
          if (record === null || typeof record !== 'object' || Array.isArray(record)) {
            throw new TypeError('publishBatch: record must be a JSON object');
          }
          const dedupKey = record.dedupKey;
          if (dedupKey !== undefined && typeof dedupKey !== 'string') {
            throw new TypeError('publishBatch: dedupKey must be a string');
          }
          assertJsonSafe(record);
          // Serialize exactly once per record: the byte count used for
          // stats and the bytes actually appended must be the same string.
          const recordJson = JSON.stringify(record);
          prepared.push({
            dedupKey,
            recordJson,
            size: Buffer.byteLength(recordJson, 'utf8'),
          });
        }
      } catch (err) {
        return Promise.reject(err);
      }
      if (prepared.length === 0) {
        return Promise.resolve([]);
      }

      // Reserve the group's first-occurrence keys at call time, mirroring
      // publish()'s `pending`: a concurrent single or batch carrying the
      // same key shares this group's first acknowledgement. Keys already in
      // history or reserved by an earlier in-flight op are left alone.
      const reservations = new Map();
      for (const item of prepared) {
        const key = item.dedupKey;
        if (key === undefined) continue;
        if (dedup.has(key) || pending.has(key) || groupPending.has(key)) continue;
        if (reservations.has(key)) continue;
        let resolveResult;
        let rejectResult;
        const promise = new Promise((resolve, reject) => {
          resolveResult = resolve;
          rejectResult = reject;
        });
        // This promise is a coordination primitive: callers that race the
        // reservation attach their own branch (publish returns it). A group
        // with no such racer would otherwise leave the rejection unhandled
        // when the bus closes before the job runs. The noop branch only
        // handles the event for the process; racers still observe it.
        promise.catch(() => {});
        const reservation = { promise, resolve: resolveResult, reject: rejectResult };
        reservations.set(key, reservation);
        groupPending.set(key, reservation);
      }

      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }

        // Resolve the whole plan against durable state inside the serial
        // job, so groups queued behind earlier publishes/batches observe
        // their committed dedup keys.
        const gid = randomUUID();
        let nextSeq = seq;
        const owned = new Map(); // key -> first acknowledgement within group
        const keyAcks = new Map(); // every dedup key's resolved acknowledgement
        const planned = prepared.map((item) => {
          const key = item.dedupKey;
          if (key !== undefined) {
            const hist = dedup.get(key);
            if (hist) {
              const reuse = { id: hist.id, seq: hist.seq };
              keyAcks.set(key, reuse);
              return { reuse };
            }
            const earlier = owned.get(key);
            if (earlier) {
              return { reuse: earlier };
            }
          }
          nextSeq += 1;
          const id = randomUUID();
          const ack = { id, seq: nextSeq };
          if (key !== undefined) {
            owned.set(key, ack);
            keyAcks.set(key, ack);
          }
          // Splice the already-serialized record into the line so the
          // persisted bytes are exactly what `size` counted.
          let line = `{"t":"m","seq":${nextSeq},"id":${JSON.stringify(id)},"bytes":${item.size}`;
          if (key !== undefined) line += `,"d":${JSON.stringify(key)}`;
          line += `,"record":${item.recordJson}}\n`;
          return { effective: { ack, line, size: item.size, key } };
        });

        // One begin-bracket, the group's new messages, one commit-bracket.
        // Recovery applies the bracketed entries only when the commit is
        // present, so a crash leaves no trace of the group.
        const buffers = [Buffer.from(JSON.stringify({ t: 'b', id: gid }) + '\n', 'utf8')];
        for (const part of planned) {
          if (part.effective) {
            buffers.push(Buffer.from(part.effective.line, 'utf8'));
          }
        }
        buffers.push(Buffer.from(JSON.stringify({ t: 'bk', id: gid }) + '\n', 'utf8'));

        try {
          // Coalesce in small chunks (bounds peak memory for huge groups)
          // and loop over partial writes. Single-buffer writeSync is used
          // rather than writev so the append behaves identically on Windows.
          // Everything here is synchronous inside this job, so no position
          // write can interleave. The begin/commit brackets keep the group
          // atomic across a crash even if a partial write occurs.
          const CHUNK = 256;
          for (let i = 0; i < buffers.length; i += CHUNK) {
            const chunk = Buffer.concat(buffers.slice(i, i + CHUNK));
            let off = 0;
            while (off < chunk.length) {
              const written = writeSync(fd, chunk, off, chunk.length - off);
              if (!Number.isInteger(written) || written <= 0) {
                throw new Error('publishBatch: write made no progress, group is not durable');
              }
              off += written;
            }
          }
          if (fsync) fsyncSync(fd);
        } catch (err) {
          // Nothing is applied in memory; the uncommitted bracket is
          // severed on reopen. Stop the bus so later writes cannot glue
          // themselves onto the partial group. Reservations are reclaimed
          // by the outer catch below.
          broken = true;
          throw err;
        }

        // Durable first, state after.
        for (const part of planned) {
          if (!part.effective) continue;
          const { ack, size, key } = part.effective;
          seq = ack.seq;
          bytes += size;
          published += 1;
          if (key !== undefined) dedup.set(key, { id: ack.id, seq: ack.seq });
        }

        const acks = planned.map((part) =>
          part.reuse ? { id: part.reuse.id, seq: part.reuse.seq } : { id: part.effective.ack.id, seq: part.effective.ack.seq },
        );
        for (const [key, reservation] of reservations) {
          groupPending.delete(key);
          reservation.resolve(keyAcks.get(key));
        }
        return acks;
      }).catch((err) => {
        // Covers write failure inside the job and rejection before the job
        // body (closed/broken bus): release any reservation still pointing
        // at this failed group.
        for (const [key, reservation] of reservations) {
          if (groupPending.get(key) === reservation) groupPending.delete(key);
          reservation.reject(err);
        }
        throw err;
      });
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
        // Fold every surviving message: snapshot contents plus the live log,
        // skipping anything truncation already discarded.
        const messages = [];
        for (const m of baseMessages) {
          if (m.seq > horizon) messages.push(m);
        }
        const logFloor = Math.max(horizon, foldedSeq);
        const collect = (filePath) => {
          const { entries } = parseLog(readFileSync(filePath));
          for (const entry of entries) {
            if (entry.t === 'm' && entry.seq > logFloor) {
              messages.push({ seq: entry.seq, id: entry.id, record: entry.record });
            }
          }
        };
        for (const seg of listSegments()) collect(seg.path);
        collect(logPath);
        const gen = snapshotGen + 1;
        const snapshot = {
          v: 1,
          gen,
          seq,
          bytes,
          published,
          replayed,
          horizon,
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
        // Durability of directory entries (the renames above and below, the
        // segment unlinks) is best-effort: some platforms/filesystems expose
        // no directory fsync. A failure here weakens only the durability
        // guarantee, never the compaction result — the snapshot covers every
        // byte of the old log, so recovery folds it harmlessly on reopen.
        const syncDir = () => {
          try {
            const dirFd = openSync(dir, 'r');
            try {
              fsyncSync(dirFd);
            } finally {
              closeSync(dirFd);
            }
          } catch {
            // Directory sync unavailable/failed: compaction still stands.
          }
        };
        syncDir();

        // Replacing the log must work on Windows: ftruncate() of an open
        // append handle does not shrink the file there. Close the handle,
        // write a fresh marker-only log to a temp name, fsync it, then
        // atomically rename it over the old one (libuv replaces the target)
        // and reopen. The whole swap is one synchronous segment, so no
        // reader can observe an intermediate state.
        closeSync(fd);
        // Every finalized segment is folded into the snapshot; drop them.
        // A failed unlink leaves a harmless leftover: the foldedSeq floor
        // keeps it out of replay and recovery reconciles it on reopen.
        for (const seg of listSegments()) {
          try {
            rmSync(seg.path, { force: true });
          } catch {
            // Leftover segment: ignored now, cleaned up on reopen.
          }
        }
        let replaced = false;
        try {
          const newFd = openSync(logTmpPath, 'w');
          try {
            writeSync(newFd, Buffer.from(markerLine(gen), 'utf8'));
            fsyncSync(newFd);
          } finally {
            closeSync(newFd);
          }
          renameSync(logTmpPath, logPath);
          replaced = true;
          // Best-effort durability of the directory entry (may be
          // unavailable on Windows / some filesystems); a crash here
          // recovers to either the intact old log or the new one — both
          // fully covered by the snapshot above.
          syncDir();
          fd = openSync(logPath, 'a');
          fsyncSync(fd);
        } catch (err) {
          if (!replaced) {
            // New log never landed; the old log is fully covered by the new
            // snapshot regardless. Reopen it so close() still works, but
            // refuse further writes on this instance.
            try {
              fd = openSync(logPath, 'a');
            } catch {
              // Nothing usable: leave fd closed; every later op throws.
            }
          }
          broken = true;
          throw err;
        }
        snapshotGen = gen;
        foldedSeq = seq;
        baseMessages = messages;
      });
    },

    truncate(before) {
      // Spec: a bound that is not a non-negative integer is always a
      // synchronous TypeError, even on a closed bus.
      if (typeof before !== 'number' || !Number.isInteger(before) || before < 0) {
        throw new TypeError('truncate: before must be a non-negative integer');
      }
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      if (before === 0) {
        // A zero bound discards nothing by definition.
        return Promise.resolve();
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }

        // Truncation deals in whole segments only: seal the active segment
        // so its contents become eligible, and roll a fresh one. The rename
        // is atomic, so a crash here leaves the content under exactly one
        // name — never duplicated, never half moved.
        if (existsSync(logPath) && statSync(logPath).size > 0) {
          try {
            closeSync(fd);
            renameSync(logPath, path.join(dir, segmentName(nextSegmentIndex)));
            nextSegmentIndex += 1;
            fd = openSync(logPath, 'a');
          } catch (err) {
            // Recover the append handle if possible; the bus refuses further
            // writes either way rather than risking a mixed-up log.
            try {
              fd = openSync(logPath, 'a');
            } catch {
              // Nothing usable: leave fd closed; every later op throws.
            }
            broken = true;
            throw err;
          }
        }

        // Deletion is a prefix of the segment chain: sweep oldest first and
        // stop at the first segment that must stay. A segment stays while
        // any registered consumer's position has not passed its newest
        // message, or while that message is beyond the bound. A segment
        // without messages carries no retention weight and goes with the
        // prefix (its position/stat lines are checkpointed below).
        const doomed = [];
        let newHorizon = horizon;
        for (const seg of listSegments()) {
          const max = segmentMaxSeq(seg.path);
          if (max !== null) {
            if (max <= foldedSeq) continue; // pre-compaction leftover, not truncation's job
            if (max > before) break;
            let held = false;
            for (const pos of positions.values()) {
              if (pos < max) {
                held = true;
                break;
              }
            }
            if (held) break;
            if (max > newHorizon) newHorizon = max;
          }
          doomed.push(seg);
        }
        // Nothing can go: the roll above is the only effect of this call.
        if (doomed.length === 0) {
          return;
        }

        // Checkpoint every state the doomed segments carry BEFORE unlinking
        // them. The marker is the first line of the fresh active segment and
        // becomes the new recovery anchor, so a crash after it lands loses
        // nothing: recovery resets to the marker and reconciles leftovers.
        const marker = {
          t: 't',
          gen: snapshotGen,
          horizon: newHorizon,
          seq,
          bytes,
          published,
          replayed,
          positions: Object.fromEntries(positions),
          dedup: Array.from(dedup.entries()),
        };
        try {
          writeSync(fd, Buffer.from(JSON.stringify(marker) + '\n', 'utf8'));
          // Always fsync here regardless of the fsync option: deletion is
          // only safe once the checkpoint is durable.
          fsyncSync(fd);
        } catch (err) {
          broken = true;
          throw err;
        }
        // Make the roll's and the marker's directory entries durable before
        // unlinking. Where directory sync is unavailable, keep the segments:
        // the marker is valid either way, and a later truncate or the next
        // recovery finishes the job.
        let dirSynced = false;
        try {
          const dirFd = openSync(dir, 'r');
          try {
            fsyncSync(dirFd);
            dirSynced = true;
          } finally {
            closeSync(dirFd);
          }
        } catch {
          // Deletion is skipped; everything else about the truncate stands.
        }
        if (dirSynced) {
          for (const seg of doomed) {
            try {
              rmSync(seg.path, { force: true });
            } catch {
              // Leftovers are filtered by the horizon and cleaned on reopen.
            }
          }
          // Best-effort durability for the unlinks; a crash here recovers to
          // the marker plus whichever segments remain — both consistent.
          try {
            const dirFd = openSync(dir, 'r');
            try {
              fsyncSync(dirFd);
            } finally {
              closeSync(dirFd);
            }
          } catch {
            // Unlink durability is best-effort; leftovers are harmless.
          }
        }
        horizon = newHorizon;
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
      // And every reservation held by a batch still waiting in the chain.
      for (const [key, g] of groupPending) {
        g.reject(new Error('bus is closed'));
        groupPending.delete(key);
      }
      return enqueue(() => {
        // A failed compact may have left the append handle unusable; close
        // must stay idempotent and never reject.
        try {
          if (fsync) fsyncSync(fd);
        } catch {
          // Ignore: the handle may already be closed.
        }
        try {
          closeSync(fd);
        } catch {
          // Already closed by the compact failure path.
        }
      });
    },
  };

  return bus;
}
