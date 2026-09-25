import {
  openSync,
  closeSync,
  writeSync,
  readSync,
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
// Cross-process coordination files.
//  - bus.lock is a short-lived mutex directory (O_EXCL mkdir) held only while
//    a process actually mutates the directory, so bytes from two writers can
//    never interleave in a file.
//  - bus.owner.json is the write lease. It carries the epoch credential that
//    increments on every takeover and the sessions holding the current lease;
//    a heartbeat keeps it fresh while a process is alive.
//  - bus.epoch is the durable high-water mark for epochs, so an epoch can
//    never be reused across takeovers/restarts.
const LOCK_NAME = 'bus.lock';
const OWNER_NAME = 'bus.owner.json';
const EPOCH_NAME = 'bus.epoch';
// Per-session heartbeat files (bus.hb.<sessionId>) are written WITHOUT the
// mutex, so a process blocked waiting for the lock still proves it is alive
// and is never wrongly taken over.
const HB_PREFIX = 'bus.hb.';
const HB_TMP_PREFIX = 'bus.hb.tmp.';
// Every temp file a write uses is session-scoped under one of these
// prefixes, so two writers never clobber each other's tmp; stale leftovers
// from a crashed holder are swept under the lock on the next open.
const TMP_PREFIXES = [
  'bus.owner.tmp.',
  'bus.epoch.tmp.',
  HB_TMP_PREFIX,
  'bus.snapshot.tmp.',
  'bus.snapshot.mirror.tmp.',
  'bus.log.tmp.',
  'bus.log.reset.tmp.',
];
const envLease = Number(process.env.BUS_LEASE_MS);
const LEASE_MS = envLease > 0 ? envLease : 3000;
const envRenew = Number(process.env.BUS_RENEW_MS);
const RENEW_MS = envRenew > 0 ? envRenew : Math.min(250, Math.floor(LEASE_MS / 10));
// A mutex directory untouched for longer than this, whose owning session's
// heartbeat is also stale, belongs to a process that died mid-job; the next
// waiter breaks it.
const LOCK_STALE_MS = LEASE_MS;
const LOCK_SPIN_MS = 5;
const LOCK_WAIT_MS = 15000;
// Best-effort stats()/usage() scan: never block long behind a write job.
const VIEW_WAIT_MS = 100;

// The log is a chain of segment files: the active `bus.jsonl` plus finalized
// `bus.<index>.jsonl` segments. Index order is oldest first; an index is
// never reused within the lifetime of a directory.
const SEGMENT_RE = /^bus\.(\d+)\.jsonl$/;
const segmentName = (index) => `bus.${String(index).padStart(10, '0')}.jsonl`;
// Multi-writer snapshots live in gen/epoch-scoped files so a preempted
// holder completing a stale compaction can never clobber a newer snapshot by
// renaming over a fixed name. The unscoped bus.snapshot.json stays readable
// as the legacy/canonical form (older buses and hand-crafted layouts).
const SNAPSHOT_SCOPED_RE = /^bus\.snapshot\.g(\d+)\.e(\d+)\.json$/;
const snapshotScopedName = (gen, epoch) => `bus.snapshot.g${gen}.e${epoch}.json`;

const sleepSync = (ms) => {
  const slot = new Int32Array(new SharedArrayBuffer(4));
  Atomics.wait(slot, 0, 0, Math.max(1, ms));
};

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

// Finalized segments present in the directory, oldest first.
const listSegments = (dir) => {
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

// Epoch helpers shared by the full scanner and the incremental applier. A
// line is stale only when it carries an explicit epoch stamp below the
// fence watermark; unstamped legacy lines always count.
const epochOfEntry = (entry) =>
  entry !== null && typeof entry.g === 'number' ? entry.g : 0;
const isStaleEntry = (entry, watermark) =>
  entry !== null && typeof entry.g === 'number' && entry.g < watermark;

/**
 * Apply one committed log entry to a scan-shaped state. Exactly the same
 * filtering/accounting rules scanDirectory uses for a full replay, factored
 * out so an incremental delta can reuse them:
 *  - stale-epoch bytes (a preempted holder writing after the fence) never
 *    enter counters, positions, dedup, occupancy or the visible log;
 *  - messages at/below the truncation horizon stay discarded (only the
 *    marker carries their cumulative counters);
 *  - retained occupancy grows by the entry's business bytes.
 */
function applyCommittedEntry(st, entry, watermark) {
  if (isStaleEntry(entry, watermark)) return;
  if (entry.t === 'm') {
    if (entry.seq <= st.horizon) return;
    st.seq = entry.seq;
    st.bytes += entry.bytes;
    st.usageBytes += entry.bytes;
    st.published += 1;
    if (typeof entry.d === 'string') {
      st.dedup.set(entry.d, { id: entry.id, seq: entry.seq });
    }
    st.live.push({ seq: entry.seq, id: entry.id, record: entry.record });
  } else if (entry.t === 's') {
    st.replayed += entry.n;
  } else if (entry.t === 'p') {
    st.positions.set(entry.name, entry.pos);
  }
}

// A truncate marker is an absolute checkpoint: counters, positions and the
// dedup table it carries replace whatever earlier history established.
function applyMarkerEntry(st, entry) {
  st.seq = entry.seq;
  st.bytes = entry.bytes;
  st.published = entry.published;
  st.replayed = entry.replayed;
  st.horizon = entry.horizon;
  st.positions.clear();
  for (const [name, pos] of Object.entries(entry.positions)) st.positions.set(name, pos);
  st.dedup.clear();
  for (const [key, value] of entry.dedup) st.dedup.set(key, value);
}

/**
 * Apply a buffer of NEWLY visible log bytes on top of an existing scanned
 * state, using exactly the committed-bracket / fence recovery rules a full
 * replay uses. Only appended bytes are ever passed in (the structural
 * fingerprint guarantees the files' older prefixes are unchanged), so this
 * is the piece that replaces the per-write full directory rescan: the
 * common publish path reads just the bytes the previous commit added.
 *
 * Returns how many of the new bytes were processed, the watermark at the
 * end, and `tornLength` — the physical file length the active log must be
 * cut back to when the suffix held an uncommitted bracket or a torn line.
 * Uncommitted entries are collected but never applied.
 */
function applyLogDelta(st, buf, baseOffset, startWatermark) {
  let watermark = startWatermark;
  let lineStart = 0;
  let processedEnd = 0;
  let group = null;
  let tornLength = null;

  for (let i = 0; i < buf.length; i++) {
    if (buf[i] !== 0x0a) continue;
    const entry = i > lineStart ? JSON.parse(buf.toString('utf8', lineStart, i)) : null;
    const completeEnd = i + 1;
    if (entry && entry.t === 'f') {
      if (epochOfEntry(entry) > watermark) watermark = epochOfEntry(entry);
      st.fenceEpoch = Math.max(st.fenceEpoch, watermark);
      // The fence aborts any batch a preempted holder left open.
      group = null;
    } else if (group !== null) {
      if (isStaleEntry(entry, watermark)) {
        // Preempted holder's bytes (even a matching-looking commit) never
        // close or apply the current group.
      } else if (entry && entry.t === 'bk' && entry.id === group.id) {
        for (const e of group.entries) applyCommittedEntry(st, e, watermark);
        group = null;
      } else if (entry && entry.t === 'b') {
        // The previous group never committed; track the new one instead.
        group = { id: entry.id, entries: [], beginOffset: lineStart };
      } else if (entry) {
        group.entries.push(entry);
      }
    } else if (entry && entry.t === 'b') {
      if (isStaleEntry(entry, watermark)) {
        // A stale uncommitted group: ignore it but keep its bytes.
      } else {
        group = { id: entry.id, entries: [], beginOffset: lineStart };
      }
    } else if (entry) {
      if (entry.t === 't') applyMarkerEntry(st, entry);
      else applyCommittedEntry(st, entry, watermark);
    }
    processedEnd = completeEnd;
    lineStart = completeEnd;
  }

  if (group !== null) {
    // An open bracket at EOF: crash/seizure mid-group. Its physical prefix
    // ends before the begin line; none of its entries were applied.
    tornLength = baseOffset + group.beginOffset;
  } else if (lineStart < buf.length) {
    // A torn trailing line with no terminating newline.
    tornLength = baseOffset + lineStart;
  }
  return { consumed: processedEnd, watermark, tornLength };
}

/**
 * Non-destructive view of the whole directory. Applies exactly the same
 * recovery rules the single-process opener used (snapshot anchor, truncate
 * checkpoint marker, committed batch brackets only, torn tails severed) but
 * changes nothing on disk: callers run it under the cross-process lock and
 * act on its result. Because every surviving fact is read from the shared
 * files each time, dedup keys, positions, quota occupancy and cumulative
 * stats are one merged state across every writer.
 */
function scanDirectory(dir) {
  const logPath = path.join(dir, LOG_NAME);
  const snapshotPath = path.join(dir, SNAPSHOT_NAME);

  const state = {
    seq: 0,
    bytes: 0,
    published: 0,
    replayed: 0,
    horizon: 0,
    dedup: new Map(),
    positions: new Map(),
  };
  let snapshotGen = 0;
  let foldedSeq = 0;
  let baseMessages = [];
  // Surviving messages visible above the checkpoint/horizon floors, in
  // ascending order (retained pre-anchor segments plus the committed
  // suffix).
  const live = [];
  // Every accepted log-side message (same filtering as recovery), used to
  // recompute retained occupancy without re-parsing files.
  const accepted = [];
  // Highest epoch fenced off in this directory.
  let fenceEpoch = 0;
  let nextSegmentIndex = 1;

  const epochOf = (entry) => epochOfEntry(entry);
  const isStale = (entry, watermark) => isStaleEntry(entry, watermark);

  const applyEntry = (entry, watermark) => {
    if (isStale(entry, watermark)) return;
    if (entry.t === 'm') {
      if (entry.seq <= state.horizon) return;
      state.seq = entry.seq;
      state.bytes += entry.bytes;
      state.published += 1;
      if (typeof entry.d === 'string') {
        state.dedup.set(entry.d, { id: entry.id, seq: entry.seq });
      }
      const msg = { seq: entry.seq, id: entry.id, record: entry.record };
      live.push(msg);
      accepted.push(msg);
    } else if (entry.t === 's') {
      state.replayed += entry.n;
    } else if (entry.t === 'p') {
      state.positions.set(entry.name, entry.pos);
    }
  };

  const applyMarker = (entry) => applyMarkerEntry(state, entry);

  const markerLine = (gen) => JSON.stringify({ t: 'c', gen }) + '\n';

  // Replay complete log lines and apply only the committed, non-fenced
  // prefix. Batch brackets, fences and torn tails follow the same rules
  // applyLogDelta() implements incrementally; see there for the rationale.
  const recoverLog = (raw, startWatermark) => {
    let watermark = startWatermark;
    let lineStart = 0;
    let retainableEnd = 0;
    let group = null;
    for (let i = 0; i < raw.length; i++) {
      if (raw[i] !== 0x0a) continue;
      const entry = i > lineStart ? JSON.parse(raw.toString('utf8', lineStart, i)) : null;
      const completeEnd = i + 1;
      if (entry && entry.t === 'f') {
        if (epochOf(entry) > watermark) watermark = epochOf(entry);
        fenceEpoch = Math.max(fenceEpoch, watermark);
        group = null;
        retainableEnd = completeEnd;
      } else if (group !== null) {
        if (isStale(entry, watermark)) {
          retainableEnd = completeEnd;
        } else if (entry && entry.t === 'bk' && entry.id === group.id) {
          for (const e of group.entries) applyEntry(e, watermark);
          group = null;
          retainableEnd = completeEnd;
        } else if (entry && entry.t === 'b') {
          group = { id: entry.id, entries: [], beginOffset: lineStart };
          retainableEnd = completeEnd;
        } else if (entry) {
          group.entries.push(entry);
        }
      } else if (entry && entry.t === 'b') {
        if (isStale(entry, watermark)) {
          retainableEnd = completeEnd;
        } else {
          group = { id: entry.id, entries: [], beginOffset: lineStart };
        }
      } else {
        if (entry) {
          if (entry.t === 't') applyMarker(entry);
          else applyEntry(entry, watermark);
        }
        retainableEnd = completeEnd;
      }
      lineStart = completeEnd;
    }
    if (group !== null) {
      return Math.min(retainableEnd, group.beginOffset);
    }
    return retainableEnd;
  };

  const segments = listSegments(dir);
  for (const seg of segments) {
    if (seg.index >= nextSegmentIndex) nextSegmentIndex = seg.index + 1;
  }
  const ordered = segments.map((seg) => seg.path);
  if (existsSync(logPath)) ordered.push(logPath);

  // Pending physical repairs discovered by the scan; applied by healLocked()
  // while the cross-process lock is held.
  const tails = [];
  const removable = [];
  let reset = false;

  // Fences raise the epoch watermark in log order. Walk every file first so
  // each file is scanned with the watermark as of its own position and the
  // global maximum is known before choosing a snapshot.
  const watermarkAtStart = [];
  let globalFence = 0;
  {
    let wm = 0;
    for (let i = 0; i < ordered.length; i++) {
      watermarkAtStart.push(wm);
      const { entries } = parseLog(readFileSync(ordered[i]));
      for (const entry of entries) {
        if (entry.t === 'f' && epochOf(entry) > wm) wm = epochOf(entry);
      }
    }
    globalFence = wm;
  }

  // Choose the newest snapshot among the canonical file and the
  // gen/epoch-scoped files. Generation is primary; within one generation a
  // higher epoch wins.
  const snapshotCandidates = [];
  if (existsSync(snapshotPath)) snapshotCandidates.push({ file: snapshotPath, gen: -1, epoch: 0 });
  for (const name of readdirSync(dir)) {
    const m = SNAPSHOT_SCOPED_RE.exec(name);
    if (m) {
      snapshotCandidates.push({
        file: path.join(dir, name),
        gen: Number(m[1]),
        epoch: Number(m[2]),
      });
    }
  }
  snapshotCandidates.sort((a, b) =>
    a.gen !== b.gen ? a.gen - b.gen : a.epoch - b.epoch,
  );

  const loadSnapshot = (cand) => {
    const snap = JSON.parse(readFileSync(cand.file, 'utf8'));
    snapshotGen = snap.gen;
    fenceEpoch = Math.max(globalFence, typeof snap.e === 'number' ? snap.e : 0);
    state.seq = snap.seq;
    state.bytes = snap.bytes;
    state.published = snap.published;
    state.replayed = snap.replayed;
    state.horizon = snap.horizon ?? 0;
    foldedSeq = snap.seq;
    state.dedup.clear();
    state.positions.clear();
    for (const [key, value] of snap.dedup) state.dedup.set(key, value);
    for (const [name, pos] of Object.entries(snap.positions)) state.positions.set(name, pos);
    baseMessages = snap.messages;
  };

  // Resolve the usable snapshot newest-first. A candidate is usable when its
  // compaction completed (an anchor marker for its generation exists in the
  // log) or, when anchor-less, its writer's epoch is at least the current
  // fence watermark. Anchor-less candidates below the watermark are orphans
  // and resolution falls back to the next-newest snapshot.
  let chosenSnapshot = null;
  let chosenHasAnchor = false;
  for (let k = snapshotCandidates.length - 1; k >= 0; k--) {
    const cand = snapshotCandidates[k];
    const snap = JSON.parse(readFileSync(cand.file, 'utf8'));
    let hasAnchor = false;
    for (const filePath of ordered) {
      const first = firstEntry(filePath);
      if (first && (first.t === 'c' || first.t === 't') && first.gen === snap.gen) {
        hasAnchor = true;
        break;
      }
    }
    if (hasAnchor || cand.epoch >= globalFence) {
      chosenSnapshot = cand;
      chosenHasAnchor = hasAnchor;
      loadSnapshot(cand);
      break;
    }
    removable.push(cand.file);
  }
  if (chosenSnapshot) {
    for (const cand of snapshotCandidates) {
      if (cand === chosenSnapshot) continue;
      if (cand.file === snapshotPath) continue;
      if (cand.gen < snapshotGen) removable.push(cand.file);
    }
  }
  fenceEpoch = Math.max(fenceEpoch, globalFence);

  let startAt = -1;
  for (let i = 0; i < ordered.length; i++) {
    const first = firstEntry(ordered[i]);
    if (first && (first.t === 'c' || first.t === 't') && first.gen === snapshotGen) {
      startAt = i;
    }
  }

  if (chosenSnapshot && !chosenHasAnchor) {
    reset = true;
    for (const seg of segments) removable.push(seg.path);
  } else {
    for (let i = Math.max(startAt, 0); i < ordered.length; i++) {
      const raw = readFileSync(ordered[i]);
      const committedEnd = recoverLog(raw, watermarkAtStart[i]);
      if (committedEnd < raw.length) tails.push({ path: ordered[i], length: committedEnd });
    }
    for (let i = 0; i < startAt; i++) {
      const max = segmentMaxSeq(ordered[i]);
      if (max === null || max <= foldedSeq || max <= state.horizon) {
        removable.push(ordered[i]);
      } else {
        const floor = Math.max(state.horizon, foldedSeq);
        const { entries } = parseLog(readFileSync(ordered[i]));
        let wm = watermarkAtStart[i];
        for (const entry of entries) {
          if (entry.t === 'f') {
            if (epochOf(entry) > wm) wm = epochOf(entry);
            continue;
          }
          if (entry.t === 'm' && entry.seq > floor && !isStale(entry, wm)) {
            const msg = { seq: entry.seq, id: entry.id, record: entry.record };
            live.push(msg);
            accepted.push(msg);
          }
        }
      }
    }
  }
  live.sort((a, b) => a.seq - b.seq);

  let usageBytes = 0;
  for (const m of baseMessages) {
    if (m.seq > state.horizon) usageBytes += Buffer.byteLength(JSON.stringify(m.record), 'utf8');
  }
  for (const m of accepted) {
    if (m.seq > state.horizon) usageBytes += Buffer.byteLength(JSON.stringify(m.record), 'utf8');
  }

  return {
    ...state,
    usageBytes,
    snapshotGen,
    foldedSeq,
    baseMessages,
    live,
    nextSegmentIndex,
    fenceEpoch,
    markerLine,
    tails,
    removable,
    reset,
  };
}

// Messages with seq > pos that survived truncation, snapshot first then the
// live suffix, ascending. A position below the truncation horizon starts at
// the earliest surviving message: the gap is already truncated. Log-side
// messages at/below foldedSeq are covered by the snapshot and skipped.
const messagesAfter = (st, pos) => {
  const floor = Math.max(pos, st.horizon);
  const out = [];
  for (const m of st.baseMessages) {
    if (m.seq > floor) {
      // Snapshot messages are shared in-memory objects; hand out a copy so
      // callers cannot mutate replay/read results for later calls.
      out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
    }
  }
  const logFloor = Math.max(floor, st.foldedSeq);
  for (const m of st.live) {
    if (m.seq > logFloor) {
      out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
    }
  }
  return out;
};

export function createBus({ path: dir, fsync = false, maxBytes } = {}) {
  if (typeof dir !== 'string' || dir.length === 0) {
    throw new TypeError('createBus: "path" must be a non-empty string');
  }
  // Omitted (undefined) means unlimited; any given value must be a positive
  // integer — zero, negatives, fractions and non-numbers are TypeErrors.
  let quota;
  if (maxBytes !== undefined) {
    if (typeof maxBytes !== 'number' || !Number.isInteger(maxBytes) || maxBytes <= 0) {
      throw new TypeError('createBus: "maxBytes" must be a positive integer');
    }
    quota = maxBytes;
  }

  const logPath = path.join(dir, LOG_NAME);
  const logTmpPath = path.join(dir, LOG_TMP_NAME);
  const snapshotPath = path.join(dir, SNAPSHOT_NAME);
  const snapshotTmpPath = path.join(dir, SNAPSHOT_TMP_NAME);
  const lockPath = path.join(dir, LOCK_NAME);
  const ownerPath = path.join(dir, OWNER_NAME);
  const epochPath = path.join(dir, EPOCH_NAME);

  // ---- acquire the bus directory synchronously, then the write lease ----
  try {
    mkdirSync(dir, { recursive: true });
  } catch (err) {
    throw new Error(`createBus: cannot open bus directory: ${err.message}`);
  }

  const sessionId = randomUUID();
  const me = { id: sessionId, pid: process.pid, born: Date.now() };
  let epoch = 0;
  let fenced = false;

  const ownerTmpPath = path.join(
    dir,
    `bus.owner.tmp.${process.pid}.${sessionId.slice(0, 8)}`,
  );
  const heartbeatPath = path.join(dir, `${HB_PREFIX}${sessionId}`);

  const readLeaseLocked = () => {
    try {
      const lease = JSON.parse(readFileSync(ownerPath, 'utf8'));
      if (
        lease &&
        typeof lease.epoch === 'number' &&
        Array.isArray(lease.sessions)
      ) {
        return lease;
      }
    } catch {
      // Missing or half a lease file: there is no credible current holder.
    }
    return null;
  };

  const readEpochHighWaterLocked = () => {
    try {
      const value = Number(readFileSync(epochPath, 'utf8'));
      if (Number.isInteger(value) && value > 0) return value;
    } catch {
      // No high-water file yet.
    }
    return 0;
  };

  const writeFileAtomic = (file, tmpFile, contents) => {
    const fd = openSync(tmpFile, 'w');
    try {
      const buf = Buffer.from(contents, 'utf8');
      let off = 0;
      while (off < buf.length) off += writeSync(fd, buf, off, buf.length - off);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(tmpFile, file);
  };

  const writeLeaseLocked = (lease) => {
    writeFileAtomic(ownerPath, ownerTmpPath, JSON.stringify(lease));
  };

  const writeEpochHighWaterLocked = (value) => {
    const tmp = path.join(dir, `bus.epoch.tmp.${process.pid}.${sessionId.slice(0, 8)}`);
    writeFileAtomic(epochPath, tmp, String(value));
  };

  // A session is alive iff its own heartbeat file was touched recently. The
  // file is written WITHOUT the mutex (atomic rename), so a process keeps
  // proving liveness even while blocked waiting for the lock.
  const heartbeatAlive = (sessionIdToCheck) => {
    try {
      return Date.now() - statSync(path.join(dir, `${HB_PREFIX}${sessionIdToCheck}`)).mtimeMs
        <= LEASE_MS;
    } catch {
      return false;
    }
  };

  const anySessionAlive = (lease) => {
    if (!lease) return false;
    return lease.sessions.some((s) => heartbeatAlive(s.id));
  };

  // The heartbeat carries no state that is ever read: liveness is judged
  // solely from the file's mtime.
  const writeHeartbeat = () => {
    let fd;
    try {
      fd = openSync(heartbeatPath, 'w');
      writeSync(fd, String(Date.now()));
    } catch {
      // Directory may have been removed; the next operation surfaces this.
    } finally {
      if (fd !== undefined) {
        try {
          closeSync(fd);
        } catch {
          // Already closed.
        }
      }
    }
  };

  const syncDirBestEffort = () => {
    try {
      const dirFd = openSync(dir, 'r');
      try {
        fsyncSync(dirFd);
      } finally {
        closeSync(dirFd);
      }
    } catch {
      // Directory sync unavailable/failed: it weakens only the durability
      // guarantee, never the outcome of an operation.
    }
  };

  // Apply repairs a scan found (torn tails, leftovers of an interrupted
  // compact/truncate, snapshot-landed-but-log-not reset). Unlinks are
  // unconditional; directory syncs are best-effort and never block or fail
  // the repair.
  const healLocked = (st) => {
    let changed = false;
    for (const tail of st.tails) {
      try {
        truncateSync(tail.path, tail.length);
        changed = true;
      } catch {
        // Another path may have healed it concurrently under the same lock.
      }
    }
    for (const segPath of st.removable) {
      try {
        rmSync(segPath, { force: true });
        changed = true;
      } catch {
        // Leftover segment: filtered by horizon/folded floors, cleaned later.
      }
    }
    if (st.reset) {
      const tmp = path.join(dir, `bus.log.reset.tmp.${process.pid}.${sessionId.slice(0, 8)}`);
      let head = st.markerLine(st.snapshotGen);
      if (st.fenceEpoch > 0) head += JSON.stringify({ t: 'f', g: st.fenceEpoch }) + '\n';
      const fd = openSync(tmp, 'w');
      try {
        writeSync(fd, Buffer.from(head, 'utf8'));
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(tmp, logPath);
      changed = true;
    }
    if (changed) syncDirBestEffort();
    return changed;
  };

  // Take the cross-process mutex once, synchronously. A lock is broken only
  // when BOTH its directory mtime and the holder's heartbeat are stale, so a
  // live process in the middle of a long write is never preempted.
  const withLockSync = (fn, waitMs = LOCK_WAIT_MS) => {
    const started = Date.now();
    let lastBeat = Date.now();
    for (;;) {
      try {
        mkdirSync(lockPath);
        try {
          writeFileSync(path.join(lockPath, 'holder'), sessionId);
        } catch {
          // The mtime + lease fallback below still works.
        }
        break;
      } catch (err) {
        if (err.code !== 'EEXIST') {
          throw new Error(`bus lock unavailable: ${err.message}`);
        }
        // While blocked in this synchronous spin the event loop (and the
        // heartbeat timer) is frozen; refresh the heartbeat inline.
        if (Date.now() - lastBeat > RENEW_MS) {
          writeHeartbeat();
          lastBeat = Date.now();
        }
        let lockAge = null;
        try {
          lockAge = Date.now() - statSync(lockPath).mtimeMs;
        } catch {
          // The holder may already be removing it; retry immediately.
        }
        if (lockAge !== null && lockAge > LOCK_STALE_MS) {
          let holderId = null;
          try {
            holderId = readFileSync(path.join(lockPath, 'holder'), 'utf8').trim() || null;
          } catch {
            // No payload: fall back to the lease sessions.
          }
          let trulyDead = false;
          if (holderId) {
            trulyDead = !heartbeatAlive(holderId);
          } else {
            try {
              trulyDead = !anySessionAlive(readLeaseLocked());
            } catch {
              trulyDead = true;
            }
          }
          if (trulyDead) {
            try {
              rmSync(lockPath, { recursive: true, force: true });
            } catch {
              // A live holder removing it concurrently wins; just retry.
            }
          } else {
            sleepSync(LOCK_SPIN_MS);
          }
        } else {
          sleepSync(LOCK_SPIN_MS);
        }
      }
      if (Date.now() - started > waitMs) {
        throw new Error('bus lock busy');
      }
    }
    try {
      return fn();
    } finally {
      try {
        rmSync(lockPath, { recursive: true, force: true });
      } catch {
        // Unlock is best-effort; the stale-lock path above reclaims it.
      }
    }
  };

  // Heartbeat first: proves THIS opener is alive before any lease decision.
  writeHeartbeat();

  let initialLease;
  try {
    initialLease = withLockSync(() => {
      const high = readEpochHighWaterLocked();
      const current = readLeaseLocked();
      let lease;
      let tookOver = false;
      if (current && anySessionAlive(current) && current.epoch >= high) {
        epoch = current.epoch;
        const live = current.sessions.filter((s) => {
          if (heartbeatAlive(s.id)) return true;
          rmSync(path.join(dir, `${HB_PREFIX}${s.id}`), { force: true });
          return false;
        });
        lease = { epoch, sessions: live.concat([me]) };
      } else {
        epoch = Math.max(current ? current.epoch : 0, high) + 1;
        lease = { epoch, sessions: [me] };
        tookOver = current !== null;
        if (current) {
          for (const s of current.sessions) {
            if (!heartbeatAlive(s.id)) {
              rmSync(path.join(dir, `${HB_PREFIX}${s.id}`), { force: true });
            }
          }
        }
      }
      writeLeaseLocked(lease);
      writeEpochHighWaterLocked(epoch);

      // Opening performs the same physical recovery the single-process bus
      // did synchronously: discard stale tmp files, sever torn tails and
      // uncommitted batches, reconcile an interrupted compact/truncate.
      {
        const now = Date.now();
        let names = [];
        try {
          names = readdirSync(dir);
        } catch {
          names = [];
        }
        for (const name of names) {
          if (TMP_PREFIXES.some((p) => name.startsWith(p))) {
            try {
              if (now - statSync(path.join(dir, name)).mtimeMs > LEASE_MS) {
                rmSync(path.join(dir, name), { force: true });
              }
            } catch {
              // Already gone: fine.
            }
          }
        }
        rmSync(snapshotTmpPath, { force: true });
        rmSync(logTmpPath, { force: true });
      }
      healLocked(scanDirectory(dir));

      if (tookOver) {
        // Stamp the epoch barrier into the log. Always fsynced: the barrier
        // must be at least as durable as the stale writes it invalidates.
        const fenceFd = openSync(logPath, 'a');
        try {
          writeSync(fenceFd, Buffer.from(JSON.stringify({ t: 'f', g: epoch }) + '\n', 'utf8'));
          fsyncSync(fenceFd);
        } finally {
          closeSync(fenceFd);
        }
      }
      writeHeartbeat();
      return lease;
    });
  } catch (err) {
    throw new Error(`createBus: cannot acquire write ownership: ${err.message}`);
  }
  void initialLease;

  // Cached merged view, refreshed under the lock by every job and used as
  // the after-close / lock-busy fallback.
  let seq = 0;
  let bytes = 0;
  let published = 0;
  let replayed = 0;
  let usageBytes = 0;
  const dedup = new Map();
  const positions = new Map();
  // dedupKey -> reservation for the first in-flight single / batch carrying
  // that key; concurrent resends share the reservation's promise just like
  // the committed table shares one first acknowledgement.
  const pending = new Map();
  const groupPending = new Map();

  const absorb = (st) => {
    seq = st.seq;
    bytes = st.bytes;
    published = st.published;
    replayed = st.replayed;
    usageBytes = st.usageBytes;
    dedup.clear();
    for (const [key, value] of st.dedup) dedup.set(key, value);
    positions.clear();
    for (const [name, pos] of st.positions) positions.set(name, pos);
  };

  // ---------------------------------------------------------------------
  // Incremental directory view.
  //
  // Every mutation used to rebuild its input by scanning the whole
  // directory: readdir plus a full read/parse of every finalized segment,
  // the active log and the chosen snapshot. The commit pipeline instead
  // keeps one long-lived view and reconciles it under the lock:
  //
  //   - fingerprint: readdir + stat of the state-bearing entries (metadata
  //     only, no file contents). A plain append to bus.jsonl changes only
  //     the active size — by far the common case under load.
  //   - delta: for a pure append, positional-read just the newly visible
  //     bytes (fd read at the saved offset) and apply them through the same
  //     committed-bracket/fence rules recovery uses.
  //   - full scan: only when the structure actually changed — segment
  //     seal/delete (truncate), log replacement (compact), snapshot
  //     churn, epoch movement, or a shrunk/replaced active log — i.e. the
  //     rare operations, not every publish.
  //
  // `active` remembers how many active-log bytes were already consumed and
  // the fence watermark as of that prefix. The active file is always last in
  // segment order, so its starting watermark is the directory's fence
  // high-water mark.
  const view = {
    ready: false,
    st: null,
    fp: null,
    activeConsumed: 0,
    activeIno: '',
    activeWatermark: 0,
    activeMissing: true,
    // Physical length to cut the active log back to when its unread suffix
    // held a torn line / uncommitted bracket (null when healthy).
    tornLength: null,
  };

  // Metadata fingerprint. `value` covers every state-bearing entry except
  // the active log's content; the active log is identified by its inode
  // (dev+ino) plus size, because truncate can replace bus.jsonl with a
  // brand-new file of the SAME byte size while sealing+deleting nets out the
  // segment set — size alone cannot prove the cached prefix still lines up.
  const fingerprintLocked = () => {
    let names;
    try {
      names = readdirSync(dir);
    } catch {
      return { value: '__unreadable__', activeSize: -2, activeIno: '' };
    }
    const parts = [];
    let activeSize = -1;
    let activeIno = '';
    for (const name of names) {
      if (name === LOG_NAME) {
        try {
          const s = statSync(logPath);
          activeSize = s.size;
          activeIno = `${s.dev}:${s.ino}`;
        } catch {
          activeSize = -1;
          activeIno = '';
        }
        continue;
      }
      if (
        SEGMENT_RE.test(name) ||
        name === SNAPSHOT_NAME ||
        SNAPSHOT_SCOPED_RE.test(name) ||
        name === EPOCH_NAME
      ) {
        let size = -1;
        let mtime = 0;
        let ino = '';
        try {
          const s = statSync(path.join(dir, name));
          size = s.size;
          mtime = Math.round(s.mtimeMs);
          ino = `${s.dev}:${s.ino}`;
        } catch {
          // Vanished between readdir and stat: encode as missing.
        }
        parts.push(`${name}:${ino}:${size}:${mtime}`);
      }
    }
    parts.sort();
    return { value: parts.join('|'), activeSize, activeIno };
  };

  const seedFromScanLocked = (fp) => {
    const st = scanDirectory(dir);
    view.st = st;
    view.fp = fp;
    view.tornLength = null;
    let activeConsumed = 0;
    let activeMissing = true;
    try {
      activeConsumed = statSync(logPath).size;
      activeMissing = false;
    } catch {
      activeConsumed = 0;
      activeMissing = true;
    }
    view.activeConsumed = activeConsumed;
    view.activeIno = activeMissing ? '' : fp.activeIno;
    // The active file is last in order; fences only ever rise, so the
    // watermark anywhere in its committed prefix is the directory high
    // water mark.
    view.activeWatermark = st.fenceEpoch;
    view.activeMissing = activeMissing;
    // The full scan already classified physical repairs non-destructively.
    const activeTail = st.tails.find((tail) => tail.path === logPath);
    if (activeTail) view.tornLength = activeTail.length;
    view.ready = true;
    return st;
  };

  // Positional read of [start, end) of the active log.
  const readActiveRangeLocked = (start, end) => {
    const buf = Buffer.allocUnsafe(Math.max(0, end - start));
    const fd = openSync(logPath, 'r');
    try {
      let off = 0;
      while (off < buf.length) {
        const n = readSync(fd, buf, off, buf.length - off, start + off);
        if (!n) break;
        off += n;
      }
      return buf.subarray(0, off);
    } finally {
      closeSync(fd);
    }
  };

  // Bring `view` up to date with the shared directory while the cross
  // process lock is held. Never mutates disk. Returns the merged state.
  const reconcileLocked = () => {
    const fp = fingerprintLocked();

    if (!view.ready) {
      return seedFromScanLocked(fp);
    }

    // This view already classified the active log as carrying an
    // uncommitted/torn suffix that only a mutating job can remove. Another
    // holder may have truncated it away (and appended past the old size) in
    // one critical section under the same mutex, so an offset/identity
    // comparison alone cannot prove the cached prefix still lines up:
    // rebuild fully until the directory reports a healthy tail. This is a
    // crash-path cost only — a healthy active log never sets it.
    if (view.tornLength !== null) {
      return seedFromScanLocked(fp);
    }

    const structural = fp.value !== view.fp.value;
    const activeShrank = !view.activeMissing && fp.activeSize < view.activeConsumed;
    const activeAppeared = view.activeMissing && fp.activeSize >= 0;
    // The active log was replaced (truncate seal/roll, compaction reset)
    // even though its new size/segment set may coincidentally match: the
    // cached offset points into a different inode, so rebuild.
    const activeReplaced =
      !view.activeMissing && fp.activeIno !== '' && fp.activeIno !== view.activeIno;

    if (structural || activeShrank || activeReplaced) {
      // Segments/snapshots/epoch changed, the active log shrank (a heal) or
      // it was replaced in place by truncate/compaction: rebuild the view.
      return seedFromScanLocked(fp);
    }

    view.fp = fp;
    view.activeIno = fp.activeIno;

    const activeGrew = fp.activeSize > view.activeConsumed;
    if (!activeGrew) {
      // Same bytes: the cached state is current.
      return view.st;
    }

    if (activeAppeared) {
      // The active log was created since the last view; start it at zero.
      view.activeMissing = false;
      view.activeConsumed = 0;
      view.activeIno = fp.activeIno;
      view.activeWatermark = view.st.fenceEpoch;
    }

    let chunk;
    try {
      chunk = readActiveRangeLocked(view.activeConsumed, fp.activeSize);
    } catch {
      // The log vanished/changed between stat and read: rebuild fully.
      return seedFromScanLocked(fingerprintLocked());
    }
    if (chunk.length < fp.activeSize - view.activeConsumed) {
      // The file shrank while we read: rebuild fully.
      return seedFromScanLocked(fingerprintLocked());
    }

    const result = applyLogDelta(
      view.st,
      chunk,
      view.activeConsumed,
      view.activeWatermark,
    );
    view.activeConsumed += result.consumed;
    view.activeWatermark = result.watermark;
    if (result.tornLength !== null) view.tornLength = result.tornLength;
    return view.st;
  };

  // Mutating entry point. withOwnership already reconciled the view under
  // the lock; this only physically heals crash debris the reconciler
  // classified (torn tails, interrupted compact/truncate leftovers) and
  // rebuilds once if healing moved bytes. The common healthy append costs a
  // single fingerprint per commit and no content reads.
  const prepareMutationLocked = () => {
    let st = view.st;
    const needsHeal =
      view.tornLength !== null || st.tails.length > 0 || st.removable.length > 0 || st.reset;
    if (needsHeal) {
      if (view.tornLength !== null) {
        st.tails.push({ path: logPath, length: view.tornLength });
      }
      healLocked(st);
      st = seedFromScanLocked(fingerprintLocked());
      // A tail that survives the heal is re-reported by the fresh scan; a
      // later mutating job retries. Nothing is appended onto it here.
    }
    return st;
  };

  // Force a full rebuild on the next reconcile after a structural operation
  // this instance performed itself (compact/truncate rebuild the log).
  const invalidateViewLocked = () => {
    view.ready = false;
    return reconcileLocked();
  };

  // Fold bytes THIS holder just durably appended and confirmed into the
  // cached view WITHOUT re-reading/re-parsing them: the entries are exactly
  // the ones this commit planned, stamped with the current (non-stale)
  // epoch, and the active file's committed prefix grew by exactly
  // `byteLength` bytes. This is what keeps a commit to one metadata
  // fingerprint plus its own write — no trailing positional read.
  const noteOwnAppendLocked = (entries, byteLength) => {
    for (const entry of entries) {
      applyCommittedEntry(view.st, entry, view.activeWatermark);
    }
    if (view.activeMissing) {
      view.activeMissing = false;
      view.activeConsumed = 0;
    }
    view.activeConsumed += byteLength;
    // Keep the cached active identity/size consistent for the next
    // fingerprint check; content identity is structural.
    try {
      const s = statSync(logPath);
      view.fp.activeSize = s.size;
      view.fp.activeIno = `${s.dev}:${s.ino}`;
      view.activeIno = view.fp.activeIno;
    } catch {
      // The next reconcile rebuilds if the file is unexpectedly gone.
    }
    return view.st;
  };

  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is healed on the next
  // locked operation (and on reopen).
  let broken = false;

  // All disk mutation serializes through ONE in-process commit pipeline in
  // addition to the cross-process lock. Consecutive publish/publishBatch
  // requests already queued when the pump runs are merged into a single
  // commit group: seq allocation, dedup registration, the per-entry quota
  // judgement and the four cumulative counters are settled once for the
  // merged group, and one bracketed, chunked, fsynced write covers it.
  // Replay/truncate/compact/close are barriers: writers queued before them
  // flush first; writers queued after wait behind them — invocation order is
  // the total order.
  const queue = [];
  let pumping = false;

  const isWriterRequest = (item) => item.kind === 'single' || item.kind === 'batch';

  // Verify the epoch credential against the shared lease inside the lock.
  const assertOwnershipLocked = (lease) => {
    if (closed || broken) {
      throw new Error('bus is closed');
    }
    if (fenced) {
      throw new Error('bus write ownership has been invalidated');
    }
    if (!lease) {
      fenced = true;
      throw new Error('bus write ownership has been invalidated');
    }
    if (lease.epoch > epoch) {
      fenced = true;
      throw new Error('bus write ownership was taken over by another process');
    }
    if (lease.epoch < epoch) {
      fenced = true;
      throw new Error('bus write ownership has been invalidated');
    }
    if (!lease.sessions.some((s) => s.id === sessionId)) {
      fenced = true;
      throw new Error('bus write ownership has been invalidated');
    }
  };

  // Run `fn(lease)` under the cross-process lock with a valid credential,
  // reconciling the shared view first. Read-only callers never assert the
  // credential, so a fenced instance can still read the shared files.
  const withOwnership = (fn, { waitMs = LOCK_WAIT_MS, readonly = false } = {}) =>
    withLockSync(() => {
      if (closed || broken) {
        throw new Error('bus is closed');
      }
      const lease = readLeaseLocked();
      if (!readonly) {
        assertOwnershipLocked(lease);
        // Entering the critical section counts as a fresh liveness signal
        // for the whole (synchronous, timer-frozen) job.
        writeHeartbeat();
      }
      reconcileLocked();
      return fn(lease, view.st);
    }, waitMs);

  // Re-verify ownership at a pipeline phase boundary while still holding
  // the lock. A raised epoch (a takeover that landed between stages) fences
  // this instance and invalidates the whole group: none of the bytes the
  // group planned may reach the committed log.
  const reassertLocked = () => {
    if (closed || broken) {
      throw new Error('bus is closed');
    }
    const lease = readLeaseLocked();
    if (
      !lease ||
      lease.epoch !== epoch ||
      !lease.sessions.some((s) => s.id === sessionId)
    ) {
      fenced = true;
      throw new Error('bus write ownership was taken over by another process');
    }
    writeHeartbeat();
  };

  // Append a whole buffer to a file with its own short-lived append handle.
  // There is deliberately no shared fd: compact/truncate replace the log by
  // rename, and an old handle would keep writing to the replaced inode.
  const appendTo = (file, buf, wantFsync, onProgress = null) => {
    const fd = openSync(file, 'a');
    try {
      let off = 0;
      while (off < buf.length) {
        const written = writeSync(fd, buf, off, buf.length - off);
        if (!Number.isInteger(written) || written <= 0) {
          throw new Error('write made no progress');
        }
        off += written;
        if (onProgress && off < buf.length) onProgress();
      }
      if (wantFsync) fsyncSync(fd);
    } finally {
      try {
        closeSync(fd);
      } catch {
        // Already closed.
      }
    }
  };

  const writeEntry = (entry, wantFsync = fsync) => {
    appendTo(logPath, Buffer.from(JSON.stringify(entry) + '\n', 'utf8'), wantFsync);
  };

  const isLockError = (err) =>
    err && typeof err.message === 'string' && err.message.startsWith('bus lock');

  // Position writes are synchronous (register/advance/read are synchronous
  // methods). Under multi-writer they still take the lock and validate the
  // credential, and the merged position map comes from the shared files.
  const persistPosition = (name, pos) => {
    try {
      withOwnership(() => {
        prepareMutationLocked();
        const entry = { t: 'p', name, pos, g: epoch };
        const json = JSON.stringify(entry) + '\n';
        writeEntry(entry);
        noteOwnAppendLocked([entry], Buffer.byteLength(json, 'utf8'));
        absorb(view.st);
      });
    } catch (err) {
      if (!fenced && !isLockError(err)) broken = true;
      throw err;
    }
  };

  const assertOpen = () => {
    if (closed || broken) {
      throw new Error('bus is closed');
    }
    if (fenced) {
      throw new Error('bus write ownership was taken over by another process');
    }
  };

  const assertName = (name) => {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('consumer name must be a non-empty string');
    }
  };

  // Heartbeat: keep this session's heartbeat file fresh while the process
  // is alive. It runs WITHOUT the mutex. Takeover is detected on the next
  // write attempt (the epoch check), not here.
  const renew = () => {
    if (closed) return;
    writeHeartbeat();
  };
  const renewTimer = setInterval(renew, RENEW_MS);
  renewTimer.unref?.();

  const bestEffortView = () => {
    try {
      withLockSync(() => {
        reconcileLocked();
        absorb(view.st);
      }, VIEW_WAIT_MS);
    } catch {
      // Lock held by a writer right now: the cached view stays valid.
    }
  };

  // ---------------------------------------------------------------------
  // Commit group execution
  // ---------------------------------------------------------------------

  // One planned record: either a reuse of an earlier (history or in-group)
  // first acknowledgement, or a new effective message carrying its line.
  // Reject one request (quota/IO/takeover/close): release its reservations
  // and fail it. State was never applied on its account.
  const rejectRequest = (req, err) => {
    for (const [key, reservation] of req.reservations) {
      const map = req.kind === 'single' ? pending : groupPending;
      if (map.get(key) === reservation) map.delete(key);
      reservation.reject(err);
    }
    req.reject(err);
  };

  // Settle accepted requests after the bracket is confirmed, plus the dedup
  // reservations their calls registered (concurrent resends attach to
  // those). Acknowledgements come from the planner's key->ack table; the
  // request itself gets per-record acks in input order.
  const settleAccepted = (accepted, ctx) => {
    for (const { req } of accepted) {
      for (const [key, reservation] of req.reservations) {
        const map = req.kind === 'single' ? pending : groupPending;
        if (map.get(key) === reservation) map.delete(key);
        reservation.resolve(ctx.keyAcks.get(key));
      }
    }
    for (const { req, items } of accepted) {
      const acks = items.map((item) => (item.reuse ? item.reuse : item.effective));
      req.resolve(req.kind === 'single' ? acks[0] : acks);
    }
  };

  // Fail every accepted request (used for a write/fsync/takeover failure
  // after planning but before confirmation).
  const failAccepted = (accepted, err) => {
    for (const { req } of accepted) rejectRequest(req, err);
  };

  const runCommitGroup = (reqs) => {
    // Calls accepted during planning in this execution; read by the outer
    // catch so a write/fsync/takeover failure fails only accepted calls
    // (per-request quota rejections are already settled inside).
    let accepted = [];
    try {
      if (closed || broken) throw new Error('bus is closed');
      withOwnership(() => {
        // Stage 1: ownership validated at the lock door; reconcile and heal
        // any crash debris before a byte is planned.
        const st = prepareMutationLocked();

        // Boundary after heal/reconcile: a takeover landing while the lock
        // was waited for invalidates the whole queued group up front.
        reassertLocked();

        // Stages 2+3: plan AND judge quota one request at a time, in
        // invocation order. Each call keeps the standalone semantics: a
        // single record or whole batch that does not fit rejects and
        // allocates nothing, while fitting calls queued beside it still
        // commit. Sequence numbers, the first-occurrence dedup overlay and
        // running occupancy advance only for accepted calls, so the merged
        // commit is byte-for-byte what serial commits would have produced.
        let nextSeq = st.seq;
        let used = st.usageBytes;
        const owned = new Map(); // key -> first ack within accepted calls
        const keyAcks = new Map();
        const effectives = [];
        accepted = [];

        for (const req of reqs) {
          // Pass 1: classify every prepared record as a reuse (history, an
          // earlier accepted call, or the first occurrence within THIS call)
          // or tentative-new, and measure the call's new occupancy. In-call
          // duplicates are charged once (their first occurrence).
          const newItems = []; // prepared records that would allocate a seq
          const newIndexByPrepared = []; // -> index into newItems, or -1
          const firstInCall = new Map(); // key -> index in newItems
          let newBytes = 0;
          let overflow = false;

          for (const item of req.prepared) {
            const key = item.dedupKey;
            let isReuse =
              key !== undefined &&
              (st.dedup.has(key) || owned.has(key) || firstInCall.has(key));
            if (isReuse) {
              newIndexByPrepared.push(-1);
              continue;
            }
            const idx = newItems.length;
            newItems.push(item);
            newIndexByPrepared.push(idx);
            newBytes += item.size;
            if (key !== undefined) firstInCall.set(key, idx);
            if (quota !== undefined && used + newBytes > quota) overflow = true;
          }

          if (overflow) {
            // The whole call rejects before a seq/key/byte is reserved; its
            // new keys stay free for later calls.
            rejectRequest(
              req,
              req.kind === 'batch'
                ? new RangeError('publishBatch: retained bytes would exceed maxBytes')
                : new RangeError('publish: retained bytes would exceed maxBytes'),
            );
            continue;
          }

          // Pass 2: accepted. Assign contiguous seqs to the new records and
          // materialize their persisted lines and parsed entries.
          const newEffs = [];
          for (const item of newItems) {
            const key = item.dedupKey;
            nextSeq += 1;
            const id = randomUUID();
            const ack = { id, seq: nextSeq };
            const entry = {
              t: 'm',
              seq: ack.seq,
              id,
              bytes: item.size,
              g: epoch,
              record: JSON.parse(item.recordJson),
            };
            if (key !== undefined) entry.d = key;
            let line = `{"t":"m","seq":${ack.seq},"id":${JSON.stringify(id)},"bytes":${item.size},"g":${epoch}`;
            if (key !== undefined) line += `,"d":${JSON.stringify(key)}`;
            line += `,"record":${item.recordJson}}\n`;
            const eff = { ack, size: item.size, key, line, entry };
            newEffs.push(eff);
            effectives.push(eff);
            used += item.size;
            if (key !== undefined) {
              owned.set(key, ack);
              keyAcks.set(key, ack);
            }
          }
          // Build the per-record acknowledgements in input order.
          const items = newIndexByPrepared.map((ni, pi) => {
            if (ni >= 0) return { effective: newEffs[ni].ack };
            const key = req.prepared[pi].dedupKey;
            const hist = st.dedup.get(key);
            const reuse = hist ? { id: hist.id, seq: hist.seq } : owned.get(key);
            return { reuse };
          });
          // Ensure reservations/racers for history-reuse keys resolve too.
          for (const item of req.prepared) {
            const key = item.dedupKey;
            if (key !== undefined && !keyAcks.has(key)) {
              const hist = st.dedup.get(key);
              if (hist) keyAcks.set(key, { id: hist.id, seq: hist.seq });
            }
          }
          accepted.push({ req, items });
        }

        if (accepted.length === 0) {
          // Every call was a quota rejection (or a pure dup that nonetheless
          // is accepted above with zero effectives). Nothing to write.
          return;
        }

        if (effectives.length === 0) {
          // Every accepted call was a pure duplicate: nothing to write;
          // first acknowledgements allocate no seq and no bytes.
          absorb(st);
          settleAccepted(accepted, { keyAcks });
          return;
        }

        // Stage 4: one bracketed write covers all accepted calls. The
        // records land first (chunked, with heartbeat/credential
        // boundaries), the commit marker only after the records are
        // flushed: a crash or a takeover in the window leaves an
        // uncommitted bracket every reader/reopen severs wholesale, so a
        // call's records are all-visible or invisible, never torn-applied.
        const gid = randomUUID();
        const beginJson = JSON.stringify({ t: 'b', id: gid, g: epoch }) + '\n';
        const commitJson = JSON.stringify({ t: 'bk', id: gid, g: epoch }) + '\n';
        const beginLine = Buffer.from(beginJson, 'utf8');
        const commitLine = Buffer.from(commitJson, 'utf8');
        // Exact physical bytes appended: begin bracket, every effective
        // message line, and the commit bracket.
        let appendedBytes = Buffer.byteLength(beginJson, 'utf8');
        for (const eff of effectives) appendedBytes += Buffer.byteLength(eff.line, 'utf8');
        appendedBytes += Buffer.byteLength(commitJson, 'utf8');
        const CHUNK = 256;
        let fd;
        try {
          fd = openSync(logPath, 'a');
          const writeBuffer = (buf) => {
            let off = 0;
            while (off < buf.length) {
              const written = writeSync(fd, buf, off, buf.length - off);
              if (!Number.isInteger(written) || written <= 0) {
                throw new Error('commit group: write made no progress');
              }
              off += written;
            }
          };
          writeBuffer(beginLine);
          for (let i = 0; i < effectives.length; i += CHUNK) {
            const part = effectives.slice(i, i + CHUNK);
            writeBuffer(Buffer.concat(part.map((eff) => Buffer.from(eff.line, 'utf8'))));
            // Stage boundary inside a chunked oversized-group flush: refresh
            // liveness AND re-validate the credential. A takeover found here
            // aborts before the commit marker, so the open bracket is
            // severable and the whole group stays unconfirmed.
            reassertLocked();
          }
          // Final pre-commit boundary (also covers the single-chunk case).
          reassertLocked();
          // The commit marker closes the bracket; then ONE flush covers the
          // entire merged group at once (its single durability point).
          writeBuffer(commitLine);
          if (fsync) fsyncSync(fd);
          // Boundary after the commit flush.
          reassertLocked();
        } catch (err) {
          if (fd !== undefined) {
            try {
              closeSync(fd);
            } catch {
              // Already closed.
            }
          }
          // Write/flush failure or a mid-group takeover: state is untouched
          // (nothing is applied until the commit is confirmed) and the
          // uncommitted suffix is severed before the next append. Stop the
          // instance so later writes cannot glue onto the partial group.
          if (!fenced) broken = true;
          throw err;
        }
        try {
          closeSync(fd);
        } catch {
          // Already closed.
        }

        // Stage 5: confirmed. Fold THIS group's entries straight into the
        // cached view and advance the consumed offset by the exact bytes
        // appended — no positional re-read of bytes we just wrote. Bracket
        // lines carry no state; only the effective message entries apply.
        noteOwnAppendLocked(
          effectives.map((eff) => eff.entry),
          appendedBytes,
        );
        absorb(view.st);
        settleAccepted(accepted, { keyAcks });
      });
    } catch (err) {
      // The lock could not be taken (contention), the bus is closed, or a
      // boundary/IO failure escaped the critical section. Per-request quota
      // rejections were already delivered during planning; fail every call
      // that had been accepted (or every call when planning never ran).
      // Contention keeps the instance usable (the calls may be retried); a
      // write/fsync failure or takeover already marked it broken/fenced.
      if (accepted.length === 0) {
        for (const req of reqs) rejectRequest(req, err);
      } else {
        failAccepted(accepted, err);
      }
    }
  };

  const pump = async () => {
    try {
      while (queue.length > 0) {
        const head = queue[0];
        if (head.kind === 'barrier') {
          queue.shift();
          await head.run();
          continue;
        }
        if (closed || broken) {
          // Writers queued ahead of a barrier never run once the bus is
          // closing/broken; barriers still run (they self-check).
          const reqs = [];
          while (queue.length > 0 && isWriterRequest(queue[0])) reqs.push(queue.shift());
          const err = new Error('bus is closed');
          for (const req of reqs) {
            for (const [key, reservation] of req.reservations) {
              const map = req.kind === 'single' ? pending : groupPending;
              if (map.get(key) === reservation) map.delete(key);
              reservation.reject(err);
            }
            req.reject(err);
          }
          continue;
        }
        // Merge every writer request queued right now into one commit group.
        const reqs = [];
        while (queue.length > 0 && isWriterRequest(queue[0])) reqs.push(queue.shift());
        runCommitGroup(reqs);
      }
    } finally {
      pumping = false;
      // A request may have landed while the loop was finishing; schedulePump
      // calls from push() cover it, but re-check to close that race.
      if (queue.length > 0) schedulePump();
    }
  };

  function schedulePump() {
    if (pumping || queue.length === 0) return;
    pumping = true;
    Promise.resolve().then(pump);
  }

  const enqueueWriter = (req) => {
    queue.push(req);
    schedulePump();
    return req.promise;
  };

  const enqueueBarrier = (run) => {
    const req = { kind: 'barrier', run };
    queue.push(req);
    schedulePump();
  };

  // Build one writer request's promise/reservation bookkeeping. The
  // request's own promise resolves to an ack (single) or ack array (batch);
  // each reserved dedup key additionally owns a SEPARATE promise that
  // resolves to that key's first acknowledgement, because a concurrent
  // single/batch racing the key attaches to it and the shapes differ.
  const makeWriterRequest = (kind, prepared) => {
    let resolveResult;
    let rejectResult;
    const promise = new Promise((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    const req = { kind, prepared, reservations: new Map(), resolve: resolveResult, reject: rejectResult, promise };
    // Reserve this request's first-occurrence keys at call time, mirroring
    // the committed first-acknowledgement rule for concurrent resends.
    for (const item of prepared) {
      const key = item.dedupKey;
      if (key === undefined || req.reservations.has(key)) continue;
      if (dedup.has(key) || pending.has(key) || groupPending.has(key)) continue;
      let resolveKey;
      let rejectKey;
      const keyPromise = new Promise((resolve, reject) => {
        resolveKey = resolve;
        rejectKey = reject;
      });
      // The coordination promise may reject (bus closed/fenced/takeover)
      // with no racer attached; swallow that branch so the process does not
      // surface an unhandled rejection. Real racers still observe it.
      keyPromise.catch(() => {});
      const reservation = { promise: keyPromise, resolve: resolveKey, reject: rejectKey };
      req.reservations.set(key, reservation);
      if (kind === 'single') pending.set(key, reservation);
      else groupPending.set(key, reservation);
    }
    return req;
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
      const recordJson = JSON.stringify(record);
      const size = Buffer.byteLength(recordJson, 'utf8');

      // Duplicate of an effective message seen by this instance (possibly
      // recovered from disk). Keys first published by another process miss
      // this cache and resolve to the shared history inside the locked job.
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

      const req = makeWriterRequest('single', [{ dedupKey, recordJson, size }]);
      return enqueueWriter(req);
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
      // rejects the whole group before the pipeline is touched.
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

      const req = makeWriterRequest('batch', prepared);
      return enqueueWriter(req);
    },

    replay(from = 0) {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      if (typeof from !== 'number' || !Number.isInteger(from) || from < 0) {
        return Promise.reject(new RangeError('replay: from must be a non-negative integer'));
      }

      const result = new Promise((resolve, reject) => {
        enqueueBarrier(() => {
          try {
            if (closed || broken) throw new Error('bus is closed');
            withOwnership(() => {
              const st = prepareMutationLocked();
              // replay(from) is inclusive of from; messagesAfter takes an
              // exclusive lower bound.
              const out = messagesAfter(st, from - 1);
              const n = out.length;
              if (n > 0) {
                try {
                  const entry = { t: 's', n, g: epoch };
                  const json = JSON.stringify(entry) + '\n';
                  writeEntry(entry);
                  noteOwnAppendLocked([entry], Buffer.byteLength(json, 'utf8'));
                } catch (err) {
                  broken = true;
                  throw err;
                }
                absorb(view.st);
              } else {
                absorb(st);
              }
              resolve(out);
            });
          } catch (err) {
            if (!fenced && !isLockError(err) && !closed) broken = true;
            reject(err);
          }
        });
      });
      return result;
    },

    register(name) {
      if (closed || broken) throw new Error('bus is closed');
      assertName(name);
      // The authoritative position table is the shared one. Read-only
      // lookup: no healing, no credential check (works even after a
      // takeover); only registering a brand-new name writes and is fenced.
      let current;
      try {
        current = withOwnership(
          () => {
            const value = view.st.positions.get(name);
            absorb(view.st);
            return value;
          },
          { readonly: true },
        );
      } catch (err) {
        if (!fenced && !isLockError(err)) broken = true;
        throw err;
      }
      if (current !== undefined) {
        return current;
      }
      if (fenced) {
        throw new Error('bus write ownership was taken over by another process');
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
      // Read the merged current value under the lock so two processes
      // advancing the same consumer obey the no-backwards rule together.
      let current;
      try {
        current = withOwnership(
          () => {
            const value = view.st.positions.get(name);
            absorb(view.st);
            return value;
          },
          { readonly: true },
        );
      } catch (err) {
        if (!fenced && !isLockError(err)) broken = true;
        throw err;
      }
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
      if (closed || broken) {
        throw new Error('bus is closed');
      }
      assertName(name);
      // Read-only: even a fenced instance may read, but an unknown name is
      // auto-registered (a write), which a fenced instance may not do.
      let st;
      let pos;
      try {
        withOwnership(
          () => {
            st = view.st;
            pos = view.st.positions.get(name);
          },
          { readonly: true },
        );
      } catch (err) {
        if (!fenced && !isLockError(err)) broken = true;
        throw err;
      }
      if (pos === undefined) {
        persistPosition(name, 0);
        pos = 0;
        st = view.st;
        st.positions.set(name, 0);
        absorb(st);
      } else {
        absorb(st);
      }
      // read never moves the position; advance() is how consumption lands.
      return messagesAfter(st, pos);
    },

    compact() {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return new Promise((resolve, reject) => {
        enqueueBarrier(() => {
          try {
            if (closed || broken) throw new Error('bus is closed');
            withOwnership(() => {
              const st = prepareMutationLocked();

              // Fold every surviving message: snapshot contents plus the
              // live log, skipping anything truncation already discarded.
              const messages = [];
              for (const m of st.baseMessages) {
                if (m.seq > st.horizon) messages.push(m);
              }
              for (const m of st.live) {
                if (m.seq > st.horizon) {
                  messages.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
                }
              }
              messages.sort((a, b) => a.seq - b.seq);
              const gen = st.snapshotGen + 1;
              const scopedSnapshotPath = path.join(dir, snapshotScopedName(gen, epoch));
              const snapshotTmpMine = path.join(
                dir,
                `bus.snapshot.tmp.${process.pid}.${sessionId.slice(0, 8)}`,
              );
              const logTmpMine = path.join(
                dir,
                `bus.log.tmp.${process.pid}.${sessionId.slice(0, 8)}`,
              );
              const snapshot = {
                v: 1,
                gen,
                e: epoch,
                seq: st.seq,
                bytes: st.bytes,
                published: st.published,
                replayed: st.replayed,
                horizon: st.horizon,
                positions: Object.fromEntries(st.positions),
                dedup: Array.from(st.dedup.entries()),
                messages,
              };
              writeHeartbeat();
              const tmpFd = openSync(snapshotTmpMine, 'w');
              try {
                writeSync(tmpFd, Buffer.from(JSON.stringify(snapshot) + '\n', 'utf8'));
                fsyncSync(tmpFd);
              } finally {
                closeSync(tmpFd);
              }
              renameSync(snapshotTmpMine, scopedSnapshotPath);
              syncDirBestEffort();
              writeHeartbeat();
              // Re-verify before the destructive half of the compaction.
              reassertLocked();
              const canonTmp = path.join(
                dir,
                `bus.snapshot.mirror.tmp.${process.pid}.${sessionId.slice(0, 8)}`,
              );
              const canonFd = openSync(canonTmp, 'w');
              try {
                writeSync(canonFd, readFileSync(scopedSnapshotPath));
                fsyncSync(canonFd);
              } finally {
                closeSync(canonFd);
              }
              renameSync(canonTmp, snapshotPath);

              for (const seg of listSegments(dir)) {
                try {
                  rmSync(seg.path, { force: true });
                } catch {
                  // Leftover segment: ignored now, cleaned up later.
                }
              }
              let head = st.markerLine(gen);
              if (st.fenceEpoch > 0) head += JSON.stringify({ t: 'f', g: st.fenceEpoch }) + '\n';
              const newFd = openSync(logTmpMine, 'w');
              try {
                writeSync(newFd, Buffer.from(head, 'utf8'));
                fsyncSync(newFd);
              } finally {
                closeSync(newFd);
              }
              // Final ownership check immediately before replacing the log.
              reassertLocked();
              renameSync(logTmpMine, logPath);
              syncDirBestEffort();

              // Structural change: rebuild the view from the new layout and
              // let the scan reclaim superseded scoped snapshots.
              invalidateViewLocked();
              healLocked(view.st);
              absorb(invalidateViewLocked());
            });
            resolve();
          } catch (err) {
            if (!fenced && !isLockError(err) && !closed) broken = true;
            reject(err);
          }
        });
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
        return Promise.resolve();
      }
      return new Promise((resolve, reject) => {
        enqueueBarrier(() => {
          try {
            if (closed || broken) throw new Error('bus is closed');
            withOwnership(() => {
              let st = prepareMutationLocked();

              // Truncation deals in whole segments only: seal the active
              // segment so its contents become eligible, and roll a fresh
              // one. Re-verify immediately before the rename so a preempted
              // holder cannot seal the new owner's active log.
              if (existsSync(logPath) && statSync(logPath).size > 0) {
                reassertLocked();
                const sealedName = path.join(dir, segmentName(st.nextSegmentIndex));
                renameSync(logPath, sealedName);
              }
              reassertLocked();

              // Deletion is a prefix of the segment chain: sweep oldest
              // first and stop at the first segment that must stay.
              const doomed = [];
              let newHorizon = st.horizon;
              for (const seg of listSegments(dir)) {
                const max = segmentMaxSeq(seg.path);
                if (max !== null) {
                  if (max <= st.foldedSeq) continue; // pre-compaction leftover
                  if (max > before) break;
                  let held = false;
                  for (const pos of st.positions.values()) {
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
              if (doomed.length === 0) {
                // The roll above is the only effect; no marker is written.
                invalidateViewLocked();
                absorb(view.st);
                return;
              }

              // Checkpoint every state the doomed segments carry BEFORE
              // unlinking them. The marker is the fresh active segment's
              // first line and becomes the new recovery anchor.
              const marker = {
                t: 't',
                gen: st.snapshotGen,
                horizon: newHorizon,
                seq: st.seq,
                bytes: st.bytes,
                published: st.published,
                replayed: st.replayed,
                positions: Object.fromEntries(st.positions),
                dedup: Array.from(st.dedup.entries()),
              };
              const markerFd = openSync(logPath, 'a');
              try {
                writeSync(markerFd, Buffer.from(JSON.stringify(marker) + '\n', 'utf8'));
                if (st.fenceEpoch > 0) {
                  writeSync(
                    markerFd,
                    Buffer.from(JSON.stringify({ t: 'f', g: st.fenceEpoch }) + '\n', 'utf8'),
                  );
                }
                // Always fsync here regardless of the fsync option.
                fsyncSync(markerFd);
              } catch (err) {
                try {
                  closeSync(markerFd);
                } catch {
                  // Already closed.
                }
                broken = true;
                throw err;
              }
              try {
                closeSync(markerFd);
              } catch {
                // Already closed.
              }
              reassertLocked();
              syncDirBestEffort();
              for (const seg of doomed) {
                try {
                  rmSync(seg.path, { force: true });
                } catch {
                  // Leftovers are filtered by the horizon and cleaned on
                  // the next heal/reopen.
                }
              }
              syncDirBestEffort();

              // Structural change: rebuild merged state (usage now reflects
              // freed segments, including snapshot-folded bytes the new
              // horizon covers); the cumulative counters stay put.
              invalidateViewLocked();
              absorb(view.st);
            });
            resolve();
          } catch (err) {
            if (!fenced && !isLockError(err) && !closed) broken = true;
            reject(err);
          }
        });
      });
    },

    stats() {
      // Reading the merged counters is allowed even after a takeover; after
      // close the last cache stands.
      if (!closed) bestEffortView();
      return { seq, bytes, published, replayed };
    },

    usage() {
      if (!closed) bestEffortView();
      return usageBytes;
    },

    close() {
      if (closed) {
        return Promise.resolve();
      }
      closed = true;
      if (renewTimer) clearInterval(renewTimer);
      // Reject any publish whose dedup reservation is still queued.
      for (const [key, p] of pending) {
        p.reject(new Error('bus is closed'));
        pending.delete(key);
      }
      for (const [key, g] of groupPending) {
        g.reject(new Error('bus is closed'));
        groupPending.delete(key);
      }
      return new Promise((resolve) => {
        enqueueBarrier(() => {
          try {
            withLockSync(() => {
              rmSync(heartbeatPath, { force: true });
              const lease = readLeaseLocked();
              if (!lease) return;
              const sessions = lease.sessions.filter((s) => s.id !== sessionId);
              if (sessions.length === 0) {
                rmSync(ownerPath, { force: true });
              } else {
                writeLeaseLocked({ epoch: lease.epoch, sessions });
              }
            }, VIEW_WAIT_MS * 10);
          } catch {
            // Closing never rejects: heartbeat expiry reclaims the session.
          }
          resolve();
        });
      });
    },
  };

  // Prime the cache from the shared directory before createBus returns, so
  // stats() is correct immediately (single-process behaviour).
  try {
    withLockSync(() => {
      reconcileLocked();
      absorb(view.st);
    }, VIEW_WAIT_MS * 10);
  } catch {
    // Another writer holds the lock for long: cache stays zero and the
    // first operation reconciles under the lock.
  }

  return bus;
}
