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
} from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

const LOG_NAME = 'bus.jsonl';
const LOG_TMP_NAME = 'bus.log.tmp';
const SNAPSHOT_NAME = 'bus.snapshot.json';
const SNAPSHOT_TMP_NAME = 'bus.snapshot.tmp';
// Sealed log segments are whole files named by the (inclusive) seq range of
// the messages they hold, so recovery can rebuild the segment list from a
// directory scan alone — no separate metadata file to keep crash-consistent.
const SEGMENT_RE = /^bus\.seg\.(\d+)-(\d+)\.jsonl$/;
const SEGMENT_TMP_RE = /^bus\.seg\..*\.tmp$/;

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
  // sealed segment descriptors ({ name, first, last }), ascending by first
  let segments = [];
  // seq of the strongest truncation checkpoint applied during recovery;
  // messages at or below it are already folded into the checkpoint's totals
  let checkpointSeq = 0;

  const maxSealedSeq = () => (segments.length ? segments[segments.length - 1].last : 0);

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });

  // A leftover tmp file is a snapshot that crashed mid-write. The pre-compact
  // log is still intact, so the half snapshot carries no state: drop it.
  rmSync(snapshotTmpPath, { force: true });
  // Likewise a log tmp left behind when compact/truncate crashed before the
  // rename: the live log (if any) is still the source of truth.
  rmSync(logTmpPath, { force: true });
  // Same for a segment tmp: the active log still holds those messages.
  for (const name of readdirSync(dir)) {
    if (SEGMENT_TMP_RE.test(name)) rmSync(path.join(dir, name), { force: true });
  }

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
    // Messages up to the snapshot horizon are accounted by its totals.
    checkpointSeq = seq;
  }

  const applyEntry = (entry) => {
    if (entry.t === 'm') {
      // At or below the truncation checkpoint the message's contribution is
      // already folded into the checkpoint's cumulative counters.
      if (entry.seq <= checkpointSeq) return;
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
    } else if (entry.t === 'd') {
      // Dedup carry: keeps a key's first acknowledgement durable after the
      // segment holding its message has been truncated away.
      dedup.set(entry.k, { id: entry.id, seq: entry.seq });
    } else if (entry.t === 'x') {
      // Truncation checkpoint: cumulative counters as of the truncation,
      // including whatever the deleted segments still held. Max-based so a
      // stale carried checkpoint can never move the totals backwards.
      checkpointSeq = Math.max(checkpointSeq, entry.seq);
      seq = Math.max(seq, entry.seq);
      bytes = Math.max(bytes, entry.bytes);
      published = Math.max(published, entry.published);
      replayed = Math.max(replayed, entry.replayed);
    }
  };

  const markerLine = (gen) => JSON.stringify({ t: 'c', gen }) + '\n';

  // Split a jsonl buffer into the committed entry prefix. A batch is
  // bracketed by {t:'b',id} .. entries .. {t:'bk',id}: a begin without its
  // matching commit (crash mid-write, or overtaken by a newer begin) is
  // severed wholesale and none of its entries take effect. Returns the
  // committed entries in order plus the byte offset just past the last
  // committed line, so both a torn trailing write and an uncommitted batch
  // tail can be truncated.
  const scanLog = (raw) => {
    const committed = [];
    let lineStart = 0;
    let committedEnd = 0;
    let group = null;
    for (let i = 0; i < raw.length; i++) {
      if (raw[i] !== 0x0a) continue;
      const entry = i > lineStart ? JSON.parse(raw.toString('utf8', lineStart, i)) : null;
      if (group !== null) {
        if (entry && entry.t === 'bk' && entry.id === group.id) {
          for (const e of group.entries) committed.push(e);
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
        if (entry) committed.push(entry);
        committedEnd = i + 1;
      }
      lineStart = i + 1;
    }
    return { committed, committedEnd };
  };

  // Sealed segments, oldest first. A segment whose range is entirely covered
  // by the snapshot was folded by a compact that crashed before removing the
  // file: the snapshot is authoritative, so drop the duplicate.
  const snapshotSeq = seq;
  const found = [];
  for (const name of readdirSync(dir)) {
    const match = SEGMENT_RE.exec(name);
    if (match) found.push({ name, first: Number(match[1]), last: Number(match[2]) });
  }
  found.sort((a, b) => a.first - b.first);
  for (const seg of found) {
    if (seg.last <= snapshotSeq) {
      rmSync(path.join(dir, seg.name), { force: true });
      continue;
    }
    const segPath = path.join(dir, seg.name);
    const raw = readFileSync(segPath);
    const { committed, committedEnd } = scanLog(raw);
    for (const entry of committed) applyEntry(entry);
    if (committedEnd < raw.length) truncateSync(segPath, committedEnd);
    segments.push(seg);
  }

  // Messages at or below the sealed horizon are duplicates of segment
  // content (a truncate crashed between landing the segment and replacing
  // the active log); the segment copy is authoritative.
  const applyActiveEntry = (entry) => {
    if (entry.t === 'm' && entry.seq <= maxSealedSeq()) return;
    applyEntry(entry);
  };

  if (existsSync(logPath)) {
    const raw = readFileSync(logPath);
    const { entries } = parseLog(raw);
    if (snapshotGen > 0) {
      const first = entries[0];
      if (first && first.t === 'c' && first.gen === snapshotGen) {
        // Live post-compact log: the marker line applies as a no-op.
        const { committed, committedEnd } = scanLog(raw);
        for (const entry of committed) applyActiveEntry(entry);
        if (committedEnd < raw.length) truncateSync(logPath, committedEnd);
      } else {
        // Crash between snapshot rename and log truncation (or between
        // truncation and marker write): every byte of this log is already
        // folded into the snapshot. Reset it to just the marker so the
        // state is unambiguous from here on.
        writeFileSync(logPath, markerLine(snapshotGen));
      }
    } else {
      const { committed, committedEnd } = scanLog(raw);
      for (const entry of committed) applyActiveEntry(entry);
      if (committedEnd < raw.length) truncateSync(logPath, committedEnd);
    }
  } else if (snapshotGen > 0) {
    writeFileSync(logPath, markerLine(snapshotGen));
  }

  // Reassigned by compact()/truncate() when they close/replace/reopen the log.
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

  // Best-effort durability for directory entries (renames, deletions). Some
  // platforms/filesystems expose no directory fsync; a failure here only
  // weakens durability of the operation, never its result.
  const syncDir = () => {
    try {
      const dirFd = openSync(dir, 'r');
      try {
        fsyncSync(dirFd);
      } finally {
        closeSync(dirFd);
      }
    } catch {
      // Directory sync unavailable: the file-level fsyncs still order the
      // contents; a crash recovers to a consistent pre- or post-state.
    }
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

  // Messages with seq > pos, snapshot first, then sealed segments, then the
  // live log, ascending. A position below the earliest surviving seq simply
  // starts at the earliest survivor: the gap was truncated away.
  const messagesAfter = (pos) => {
    const out = [];
    for (const m of baseMessages) {
      if (m.seq > pos) {
        // Snapshot messages are shared in-memory objects; hand out a copy so
        // callers cannot mutate replay/read results for later calls.
        out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
      }
    }
    for (const seg of segments) {
      const { entries } = parseLog(readFileSync(path.join(dir, seg.name)));
      for (const entry of entries) {
        if (entry.t === 'm' && entry.seq > pos) {
          out.push({ seq: entry.seq, id: entry.id, record: entry.record });
        }
      }
    }
    const maxSealed = maxSealedSeq();
    const { entries } = parseLog(readFileSync(logPath));
    for (const entry of entries) {
      if (entry.t === 'm' && entry.seq > pos && entry.seq > maxSealed) {
        out.push({ seq: entry.seq, id: entry.id, record: entry.record });
      }
    }
    return out;
  };

  // Seal the active log's messages with seq <= upTo into their own segment
  // file and roll a fresh active log. The sealed segment holds message lines
  // only; everything else (compact marker, positions, replay counters,
  // checkpoints) is carried into the new active log, and the full dedup
  // table is rewritten as carry entries — so physically deleting a segment
  // later can never take positions, stats or dedup keys with it.
  const rollSegment = (upTo) => {
    const { committed } = scanLog(readFileSync(logPath));
    const maxSealed = maxSealedSeq();
    const live = committed.filter((e) => e.t === 'm' && e.seq > maxSealed);
    const toSeal = live.filter((e) => e.seq <= upTo);
    if (toSeal.length === 0) return;
    const first = toSeal[0].seq;
    const last = toSeal[toSeal.length - 1].seq;
    const name = `bus.seg.${first}-${last}.jsonl`;
    const segTmpPath = path.join(dir, `bus.seg.${first}-${last}.tmp`);

    const segFd = openSync(segTmpPath, 'w');
    try {
      for (const m of toSeal) {
        writeSync(segFd, Buffer.from(JSON.stringify(m) + '\n', 'utf8'));
      }
      fsyncSync(segFd);
    } finally {
      closeSync(segFd);
    }

    const lines = [];
    for (const entry of committed) {
      // Batch brackets are crash-recovery scaffolding for in-flight groups;
      // committed ones mean nothing. Old dedup carries are superseded by the
      // full table rewritten below.
      if (entry.t === 'm' || entry.t === 'b' || entry.t === 'bk' || entry.t === 'd') continue;
      lines.push(JSON.stringify(entry) + '\n');
    }
    for (const m of live) {
      if (m.seq > upTo) lines.push(JSON.stringify(m) + '\n');
    }
    for (const [key, value] of dedup) {
      lines.push(JSON.stringify({ t: 'd', k: key, id: value.id, seq: value.seq }) + '\n');
    }
    const tmpFd = openSync(logTmpPath, 'w');
    try {
      for (const line of lines) {
        writeSync(tmpFd, Buffer.from(line, 'utf8'));
      }
      fsyncSync(tmpFd);
    } finally {
      closeSync(tmpFd);
    }

    // Land the sealed segment first, then replace the active log. A crash
    // between the two renames leaves the messages in both files; recovery
    // drops the active-log duplicates at or below the sealed horizon.
    closeSync(fd);
    try {
      renameSync(segTmpPath, path.join(dir, name));
      renameSync(logTmpPath, logPath);
    } catch (err) {
      // The old active log is still authoritative for whatever was not
      // replaced. Reopen so close() works, but refuse further writes.
      try {
        fd = openSync(logPath, 'a');
      } catch {
        // Nothing usable: leave fd closed; every later op throws.
      }
      broken = true;
      throw err;
    }
    fd = openSync(logPath, 'a');
    segments.push({ name, first, last });
    syncDir();
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

    truncate(before) {
      // Spec: a boundary that is not a non-negative integer is always a
      // synchronous TypeError, even on a closed bus.
      if (typeof before !== 'number' || !Number.isInteger(before) || before < 0) {
        throw new TypeError('truncate: before must be a non-negative integer');
      }
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        if (before === 0) {
          return; // nothing can fall below a zero boundary: no-op
        }
        // Seal the boundary prefix of the active log into its own segment
        // and roll a fresh active segment, so the deletion below is always
        // whole-file — never half a segment.
        rollSegment(before);
        // A segment only disappears once every registered consumer's
        // position has passed it. With no registered consumers there is
        // nothing to protect. Segments not passed stay as-is, no error.
        const minPos = positions.size === 0 ? Infinity : Math.min(...positions.values());
        const victims = segments.filter((s) => s.last <= before && s.last <= minPos);
        if (victims.length === 0) {
          return; // nothing deletable: deletion is a no-op
        }
        // Durable checkpoint of the cumulative counters BEFORE any file goes
        // away: after a crash the totals must not drop with the deleted
        // segments. A crash before this line simply keeps the segments.
        writeLine({ t: 'x', seq, bytes, published, replayed });
        for (const victim of victims) {
          rmSync(path.join(dir, victim.name), { force: true });
        }
        segments = segments.filter((s) => !victims.includes(s));
        syncDir();
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
        const messages = baseMessages.slice();
        for (const seg of segments) {
          const { entries } = parseLog(readFileSync(path.join(dir, seg.name)));
          for (const entry of entries) {
            if (entry.t === 'm') {
              messages.push({ seq: entry.seq, id: entry.id, record: entry.record });
            }
          }
        }
        const maxSealed = maxSealedSeq();
        const { entries } = parseLog(readFileSync(logPath));
        for (const entry of entries) {
          if (entry.t === 'm' && entry.seq > maxSealed) {
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
        // Best-effort durability for the rename itself. If syncing the
        // directory fails (some platforms/filesystems expose no directory
        // fsync) leaving the old log in place is still safe: every byte of
        // it is covered by the new snapshot, so recovery folds it
        // harmlessly on reopen.
        syncDir();

        // The folded segments are covered by the snapshot now; a crash
        // before their removal is cleaned up on reopen (a segment at or
        // below the snapshot horizon is a duplicate).
        for (const seg of segments) {
          rmSync(path.join(dir, seg.name), { force: true });
        }
        segments = [];

        // Replacing the log must work on Windows: ftruncate() of an open
        // append handle does not shrink the file there. Close the handle,
        // write a fresh marker-only log to a temp name, fsync it, then
        // atomically rename it over the old one (libuv replaces the target)
        // and reopen. The whole swap is one synchronous segment, so no
        // reader can observe an intermediate state.
        closeSync(fd);
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
          // Best-effort durability of the directory entry. A failure here
          // only weakens durability of the rename — the compaction itself
          // already succeeded and must not be failed or break the bus.
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
