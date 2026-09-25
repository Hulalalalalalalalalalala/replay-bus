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
// and is never wrongly taken over. A session whose newest heartbeat is older
// than LEASE_MS is dead and may be replaced.
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
  // Highest epoch fenced off in this directory: complete log bytes carrying
  // a smaller epoch that physically follow a fence line are bytes a
  // preempted holder wrote after losing ownership — never mixed into state.
  let fenceEpoch = 0;
  let nextSegmentIndex = 1;

  // Snapshot candidates are resolved AFTER the ordered log and its fence
  // watermark are known (see below): an anchor-less snapshot from a holder a
  // higher fence displaced must never become a reset basis.

  // A line is stale only when it carries an explicit epoch stamp below the
  // fence watermark. Lines written before multi-writer support have no
  // stamp and were committed by a legitimate holder, so they always count.
  const epochOf = (entry) => (entry !== null && typeof entry.g === 'number' ? entry.g : 0);
  const isStale = (entry, watermark) =>
    entry !== null && typeof entry.g === 'number' && entry.g < watermark;

  const applyEntry = (entry, watermark) => {
    // A line stamped with an epoch below the current fence watermark was
    // written by a holder that had already lost ownership: it never mixes
    // into counters, positions, dedup or the visible log.
    if (isStale(entry, watermark)) return;
    if (entry.t === 'm') {
      // At/below the horizon the message was discarded by truncation; only
      // its cumulative counters (carried by the marker) survive.
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

  // A truncate marker is an absolute checkpoint: the cumulative counters,
  // positions and dedup table it carries replace whatever earlier segments
  // established, so recovery can start from the newest marker and ignore
  // everything before it.
  const applyMarker = (entry) => {
    state.seq = entry.seq;
    state.bytes = entry.bytes;
    state.published = entry.published;
    state.replayed = entry.replayed;
    state.horizon = entry.horizon;
    state.positions.clear();
    for (const [name, pos] of Object.entries(entry.positions)) state.positions.set(name, pos);
    state.dedup.clear();
    for (const [key, value] of entry.dedup) state.dedup.set(key, value);
  };

  const markerLine = (gen) => JSON.stringify({ t: 'c', gen }) + '\n';

  // Replay complete log lines and apply only the committed, non-fenced
  // prefix. A batch is bracketed by {t:'b',id} .. entries .. {t:'bk',id}: a
  // begin without its matching commit (crash mid-write, overtaken by a newer
  // begin, or a fence line) is severed wholesale and none of its entries
  // take effect. A {t:'f',e} fence raises the epoch watermark: every later
  // line stamped with a smaller epoch is a preempted holder's byte and is
  // ignored even when physically complete. The watermark lives in
  // `fenceState` so it carries across files in segment order. Returns the
  // byte offset just past the last retainable line, so torn tails and
  // current-epoch uncommitted tails can be truncated while fenced leftovers
  // stay put (logically invisible) rather than reshaping the file.
  // `startWatermark` is the fence watermark established by every earlier
  // file in segment order. It is positional, not global: history committed
  // before the fence line (even by the same older epoch) is legitimate and
  // must be retained; only bytes physically after a fence with a smaller
  // stamp are discarded.
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
        // The fence aborts any batch a preempted holder left open.
        group = null;
        retainableEnd = completeEnd;
      } else if (group !== null) {
        if (isStale(entry, watermark)) {
          // Preempted holder's bytes (even a matching-looking commit) never
          // close or apply the current group.
          retainableEnd = completeEnd;
        } else if (entry && entry.t === 'bk' && entry.id === group.id) {
          for (const e of group.entries) applyEntry(e, watermark);
          group = null;
          retainableEnd = completeEnd;
        } else if (entry && entry.t === 'b') {
          // The previous group never committed; start tracking the new one.
          group = { id: entry.id, entries: [], beginOffset: lineStart };
          retainableEnd = completeEnd;
        } else if (entry) {
          group.entries.push(entry);
        }
      } else if (entry && entry.t === 'b') {
        if (isStale(entry, watermark)) {
          // A stale uncommitted group: ignore it but keep its bytes.
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
    // A current-epoch group still open at EOF is a crash/seizure mid-batch:
    // its bytes must be severed, so the retainable prefix ends before the
    // begin line (anything stale past a fence was already ignored above).
    if (group !== null) {
      return Math.min(retainableEnd, group.beginOffset);
    }
    return retainableEnd;
  };

  const segments = listSegments(dir);
  for (const seg of segments) {
    if (seg.index >= nextSegmentIndex) nextSegmentIndex = seg.index + 1;
  }
  // The ordered log is every finalized segment (oldest first) followed by
  // the active log file.
  const ordered = segments.map((seg) => seg.path);
  if (existsSync(logPath)) ordered.push(logPath);

  // Pending physical repairs discovered by the scan; applied by healLocked()
  // while the cross-process lock is held.
  const tails = [];
  const removable = [];
  let reset = false;

  // Fences raise the epoch watermark in log order. Walk every file first so
  // each file is scanned with the watermark as of its own position and the
  // global maximum is known before choosing a snapshot: a fence in an
  // earlier segment governs later files, never earlier ones.
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
  // higher epoch wins, so a preempted holder finishing a stale compaction
  // after a takeover can never eclipse the current owner's snapshot.
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
  // fence watermark — i.e. the snapshot landed in its own legitimate
  // crash window rather than being finished by a holder a later takeover
  // displaced. Anchor-less candidates below the watermark are orphans: they
  // are removed and resolution falls back to the next-newest snapshot (whose
  // anchored history is still intact).
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
    // Displaced holder's orphaned snapshot: remove it physically.
    removable.push(cand.file);
  }
  // Scoped snapshots from older generations are superseded: their contents
  // live in the chosen snapshot and the anchored log, so reclaim them
  // rather than letting every compaction pile files up forever.
  if (chosenSnapshot) {
    for (const cand of snapshotCandidates) {
      if (cand === chosenSnapshot) continue;
      if (cand.file === snapshotPath) continue;
      if (cand.gen < snapshotGen) removable.push(cand.file);
    }
  }
  fenceEpoch = Math.max(fenceEpoch, globalFence);

  // The anchor position for the chosen snapshot generation. A generation-0
  // truncate marker anchors even when no snapshot file exists.
  let startAt = -1;
  for (let i = 0; i < ordered.length; i++) {
    const first = firstEntry(ordered[i]);
    if (first && (first.t === 'c' || first.t === 't') && first.gen === snapshotGen) {
      startAt = i;
    }
  }

  if (chosenSnapshot && !chosenHasAnchor) {
    // Snapshot rename landed but the log reset did not: every byte still on
    // disk is already folded into the snapshot. Recovery deletes the
    // segments and resets to a marker-led log. The orphan rule above
    // prevents a displaced holder's late snapshot from triggering this.
    reset = true;
    for (const seg of segments) removable.push(seg.path);
  } else {
    // Replay the live suffix starting at THIS file's positional watermark
    // (a fence in a later file must not retroactively invalidate history
    // committed before it). Torn tails / uncommitted batch tails are
    // remembered for physical severing rather than truncated during read.
    for (let i = Math.max(startAt, 0); i < ordered.length; i++) {
      const raw = readFileSync(ordered[i]);
      const committedEnd = recoverLog(raw, watermarkAtStart[i]);
      if (committedEnd < raw.length) tails.push({ path: ordered[i], length: committedEnd });
    }
    // Segments before the anchor whose content is folded into the snapshot
    // or at/below the truncation horizon can go. Segments a consumer still
    // needs (above the horizon) stay untouched and stay readable; their
    // messages still pass the fence filter as of that segment's position.
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

  // Retained occupancy is exactly the retained messages: snapshot survivors
  // above the horizon plus the accepted log-side messages collected above.
  // Torn tails, uncommitted batches and fenced stale bytes never count.
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
    // Directory deleted out from under us, or insufficient permissions:
    // opening throws Error synchronously.
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
  // solely from the file's mtime. Write it with a cheap truncating
  // open/write/close (no fsync, no atomic rename) so the periodic timer stays
  // light even on slow filesystems and never contends on directory entries.
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
  // the repair. Safe to run during acquisition: it only needs the scan.
  // Returns true when it physically changed something, so the caller knows
  // it must rescan instead of reusing the pre-repair view.
  const healLocked = (st) => {
    let changed = false;
    for (const tail of st.tails) {
      try {
        truncateSync(tail.path, tail.length);
        changed = true;
      } catch {
        // Another path may have healed it concurrently under the same lock;
        // a remaining tail is reconciled again next time / on reopen.
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

  // Settle a mutation critical section from an already-scanned view: heal
  // whatever it found, then rescan only when healing actually moved bytes.
  // This keeps the common case (a healthy directory) to the single scan
  // withOwnership already performed.
  const prepareLocked = (st) => (healLocked(st) ? scanDirectory(dir) : st);

  // Take the cross-process mutex once, synchronously, for acquisition: the
  // lease decision must itself be serialized against other openers. A lock
  // is broken only when BOTH its directory mtime and the holder's heartbeat
  // are stale, so a live process in the middle of a long write is never
  // preempted.
  const withLockSync = (fn, waitMs = LOCK_WAIT_MS) => {
    const started = Date.now();
    let lastBeat = Date.now();
    for (;;) {
      try {
        mkdirSync(lockPath);
        // Payload lets a waiter identify THIS holder's heartbeat file.
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
        // heartbeat timer) is frozen; refresh the heartbeat inline so a long
        // wait for a contended lock never makes THIS process look dead.
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
          // Find the holder's heartbeat via the lock payload written on
          // acquire; break only if that session is also stale.
          let holderId = null;
          try {
            holderId = readFileSync(path.join(lockPath, 'holder'), 'utf8').trim() || null;
          } catch {
            // No payload (a crash before it was written): fall back to the
            // lease sessions, and break only if none of them are alive.
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
        // A live holder (possibly this same process opening a second bus):
        // join its epoch, so concurrent live writers interleave under one
        // still-valid credential. Drop sessions whose heartbeat already
        // expired (a crashed peer sharing the lease) and their leftovers.
        epoch = current.epoch;
        const live = current.sessions.filter((s) => {
          if (heartbeatAlive(s.id)) return true;
          rmSync(path.join(dir, `${HB_PREFIX}${s.id}`), { force: true });
          return false;
        });
        lease = { epoch, sessions: live.concat([me]) };
      } else {
        // No fresh lease — first open, clean close, or a crashed/stale
        // holder: take over. The epoch credential increments and the old
        // holder is fenced on its very next write.
        epoch = Math.max(current ? current.epoch : 0, high) + 1;
        lease = { epoch, sessions: [me] };
        tookOver = current !== null;
        // Heartbeat files of dead sessions are pure leftovers; drop them.
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
      // did synchronously: discard half snapshot/log tmp files, sever torn
      // tails and uncommitted batches, reconcile an interrupted
      // compact/truncate, and reset when the snapshot landed but the log did
      // not. One opener healing here means no later writer has to guess.
      //
      // Sweep every stale temp file (fixed legacy names plus session-scoped
      // leftovers from crashed holders). A fresh, actively-written tmp from a
      // concurrent holder cannot exist here: this opener holds the mutex and
      // only removes tmp files older than the lease window, and a live
      // holder's tmp lives for milliseconds inside its own lock.
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
        // Fixed legacy temp names are always safe to drop (they can only be
        // half-written crash leftovers; current code never writes them).
        rmSync(snapshotTmpPath, { force: true });
        rmSync(logTmpPath, { force: true });
      }
      healLocked(scanDirectory(dir));

      if (tookOver) {
        // A previous lease was displaced: stamp the epoch barrier into the
        // log. Anything a preempted holder still manages to append after
        // this line (it was frozen mid-write, or racing the takeover) is
        // stamped with a smaller epoch and ignored by every reader, so old
        // bytes can never mix into the log. Always fsynced: the barrier
        // must be at least as durable as the stale writes it invalidates.
        const fenceFd = openSync(logPath, 'a');
        try {
          writeSync(fenceFd, Buffer.from(JSON.stringify({ t: 'f', g: epoch }) + '\n', 'utf8'));
          fsyncSync(fenceFd);
        } finally {
          closeSync(fenceFd);
        }
      }
      // The opener is fully live: make the heartbeat fresh post-recovery.
      writeHeartbeat();
      return lease;
    });
  } catch (err) {
    throw new Error(`createBus: cannot acquire write ownership: ${err.message}`);
  }
  void initialLease;

  // Cached merged view, refreshed under the lock by every job and used as
  // the after-close / lock-busy fallback. Starts from the lease-time view.
  let seq = 0;
  let bytes = 0;
  let published = 0;
  let replayed = 0;
  let usageBytes = 0;
  const dedup = new Map();
  const positions = new Map();
  // dedupKey -> { promise, resolve, reject } for first publish in flight
  const pending = new Map();
  // dedupKey -> { promise, resolve, reject } reserved by the first
  // in-flight batch carrying that key; lets later singles/batches share
  // the batch's first acknowledgement just like `pending` does for singles
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

  // Prime the cache from the shared directory before createBus returns, so
  // stats() is correct immediately (single-process behaviour).
  try {
    withLockSync(() => absorb(scanDirectory(dir)), VIEW_WAIT_MS * 10);
  } catch {
    // Another writer holds the lock for long: cache stays zero and the
    // first operation rescans under the lock.
  }

  let chain = Promise.resolve();
  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is healed on the next
  // locked operation (and on reopen).
  let broken = false;

  // All disk mutation by publish/replay/compact/truncate happens inside this
  // per-process serial chain; each job additionally takes the cross-process
  // lock, so the on-disk order is a total order across every writer.
  const enqueue = (job) => {
    const run = chain.then(job);
    // A failed job must not stall every later operation.
    chain = run.then(
      () => {},
      () => {},
    );
    return run;
  };

  // Verify the epoch credential against the shared lease inside the lock.
  // A raised epoch means ownership was taken over; a missing session means
  // the credential was revoked. Either way this instance is permanently
  // fenced: it must never mix another byte into the log. Liveness is proven
  // separately by the per-session heartbeat file, which the background
  // timer refreshes even while this call waits for the lock.
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
      // The lease file cannot legitimately move backwards; treat it as
      // tampering and fence rather than write under uncertainty.
      fenced = true;
      throw new Error('bus write ownership has been invalidated');
    }
    if (!lease.sessions.some((s) => s.id === sessionId)) {
      fenced = true;
      throw new Error('bus write ownership has been invalidated');
    }
  };

  // Run `fn(lease, scanView)` under the cross-process lock with a valid
  // credential. The scan passed in is the current merged directory state.
  // Read-only callers (an existing position lookup) never assert the
  // credential, so a fenced instance can still read the shared files; they
  // must not mutate anything themselves.
  const withOwnership = (fn, { waitMs = LOCK_WAIT_MS, readonly = false } = {}) =>
    withLockSync(() => {
      if (closed || broken) {
        throw new Error('bus is closed');
      }
      const lease = readLeaseLocked();
      if (!readonly) {
        assertOwnershipLocked(lease);
        // Entering the critical section counts as a fresh liveness signal
        // for the whole (synchronous, timer-frozen) job; the long phases
        // refresh again between fsyncs.
        writeHeartbeat();
      }
      return fn(lease, scanDirectory(dir));
    }, waitMs);

  // Re-verify ownership at a multi-phase boundary while still holding the
  // lock. A holder keeps its heartbeat fresh even inside long synchronous
  // writes (writeHeartbeat between chunks), so a positive result here is
  // sound: the lock cannot have been stolen out from under a live job.
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
    // A successful check at a phase boundary also pushes the heartbeat past
    // the long fsync/rename that just completed.
    writeHeartbeat();
  };

  // Append a whole buffer to a file with its own short-lived append handle.
  // There is deliberately no shared fd: compact/truncate replace the log by
  // rename, and an old handle would keep writing to the replaced inode.
  // `onProgress` (if given) fires between chunks so a long synchronous write
  // can refresh the session heartbeat while the event loop is blocked.
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

  // A failure to even take the mutex is contention, not a torn write: the
  // instance stays usable and the call may be retried. Only a failure after
  // the lock is held (an actual disk write) makes the bus `broken`.
  const isLockError = (err) =>
    err && typeof err.message === 'string' && err.message.startsWith('bus lock');

  // Position writes are synchronous (register/advance/read are synchronous
  // methods). Under multi-writer they still take the lock and validate the
  // credential, and the merged position map comes from the shared files.
  const persistPosition = (name, pos) => {
    try {
      withOwnership((lease, scanned) => {
        const st = prepareLocked(scanned);
        writeEntry({ t: 'p', name, pos, g: epoch });
        // Reflect locally without requiring a full rescan.
        st.positions.set(name, pos);
        absorb(st);
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
  // is alive. It runs WITHOUT the mutex, so even a process blocked waiting
  // for the lock is never mistaken for a dead holder. Takeover is detected
  // on the next write attempt (the epoch check), not here.
  const renew = () => {
    if (closed) return;
    writeHeartbeat();
  };
  const renewTimer = setInterval(renew, RENEW_MS);
  renewTimer.unref?.();

  const bestEffortView = () => {
    try {
      withLockSync(() => absorb(scanDirectory(dir)), VIEW_WAIT_MS);
    } catch {
      // Lock held by a writer right now: the cached view stays valid.
    }
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
        return withOwnership((lease, st0) => {
          const st = prepareLocked(st0);

          // The key may have landed from another process (or another
          // session of this one) between the cache check and the lock:
          // share that first acknowledgement byte-for-byte.
          if (dedupKey !== undefined) {
            const shared = st.dedup.get(dedupKey);
            if (shared) {
              dedup.set(dedupKey, { id: shared.id, seq: shared.seq });
              absorb(st);
              return { id: shared.id, seq: shared.seq, reused: true };
            }
          }

          // Quota is judged inside the lock against the merged occupancy,
          // so bytes another process committed count. An exact fit
          // succeeds; one byte over (or one oversized record) rejects
          // before anything is written — shared state stays untouched.
          if (quota !== undefined && st.usageBytes + size > quota) {
            absorb(st);
            throw new RangeError('publish: retained bytes would exceed maxBytes');
          }
          const mySeq = st.seq + 1;
          const id = randomUUID();
          const entry = { t: 'm', seq: mySeq, id, bytes: size, g: epoch, record };
          if (dedupKey !== undefined) entry.d = dedupKey;
          try {
            // Durable before acknowledgement: state changes only after the
            // write (and optional fsync) succeeds.
            appendTo(
              logPath,
              Buffer.from(JSON.stringify(entry) + '\n', 'utf8'),
              fsync,
            );
          } catch (err) {
            broken = true;
            throw err;
          }
          // Defense in depth: confirm the credential still holds after the
          // write completes. A live job's heartbeat cannot go stale, so this
          // only trips under tampering; on the rare race the caller sees a
          // rejection and the stamped line is filtered by the fence anyway.
          reassertLocked();
          // Apply to the merged view and cache it.
          st.seq = mySeq;
          st.bytes += size;
          st.usageBytes += size;
          st.published += 1;
          if (dedupKey !== undefined) st.dedup.set(dedupKey, { id, seq: mySeq });
          absorb(st);
          return { id, seq: mySeq, reused: false };
        });
      }).then(
        (ack) => {
          const clean = { id: ack.id, seq: ack.seq };
          if (dedupKey !== undefined) pending.delete(dedupKey);
          resolveResult(clean);
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
      // lock is touched: seq/bytes/published/dedup/positions stay exactly
      // as they were.
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
        return withOwnership((lease, st0) => {
          const st = prepareLocked(st0);

          // Resolve the whole plan against the merged durable state inside
          // the locked job, so groups queued behind any writer's publishes
          // observe committed dedup keys from every process.
          const gid = randomUUID();
          let nextSeq = st.seq;
          let effectiveBytes = 0;
          const owned = new Map(); // key -> first acknowledgement within group
          const keyAcks = new Map(); // every dedup key's resolved acknowledgement
          const planned = prepared.map((item) => {
            const key = item.dedupKey;
            if (key !== undefined) {
              const hist = st.dedup.get(key);
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
            let line = `{"t":"m","seq":${nextSeq},"id":${JSON.stringify(id)},"bytes":${item.size},"g":${epoch}`;
            if (key !== undefined) line += `,"d":${JSON.stringify(key)}`;
            line += `,"record":${item.recordJson}}\n`;
            effectiveBytes += item.size;
            return { effective: { ack, line, size: item.size, key } };
          });

          // Shape validation happened at call time; quota is judged for the
          // whole group here against merged occupancy, counting only the
          // first occurrence of each key. One byte over rejects the entire
          // group before the bracket is opened — no seq, stat or file change
          // anywhere in the shared directory.
          if (quota !== undefined && st.usageBytes + effectiveBytes > quota) {
            absorb(st);
            throw new RangeError('publishBatch: retained bytes would exceed maxBytes');
          }

          // One begin-bracket, the group's new messages, one commit-bracket.
          // Recovery applies the bracketed entries only when the commit is
          // present, so a crash leaves no trace of the group.
          const buffers = [
            Buffer.from(JSON.stringify({ t: 'b', id: gid, g: epoch }) + '\n', 'utf8'),
          ];
          for (const part of planned) {
            if (part.effective) {
              buffers.push(Buffer.from(part.effective.line, 'utf8'));
            }
          }
          buffers.push(
            Buffer.from(JSON.stringify({ t: 'bk', id: gid, g: epoch }) + '\n', 'utf8'),
          );

          // Everything is written through one append handle, synchronously,
          // while the cross-process lock is held: no position line or other
          // writer can interleave. Single-buffer writeSync in chunks behaves
          // identically on Windows.
          const fd = openSync(logPath, 'a');
          try {
            const writeBuffer = (buf) => {
              let off = 0;
              while (off < buf.length) {
                const written = writeSync(fd, buf, off, buf.length - off);
                if (!Number.isInteger(written) || written <= 0) {
                  throw new Error('publishBatch: write made no progress, group is not durable');
                }
                off += written;
              }
            };
            const CHUNK = 256;
            for (let i = 0; i < buffers.length; i += CHUNK) {
              writeBuffer(Buffer.concat(buffers.slice(i, i + CHUNK)));
              // A long synchronous group blocks the event loop; refresh the
              // heartbeat here so no waiter mistakes this holder for dead.
              writeHeartbeat();
            }
            if (fsync) fsyncSync(fd);
          } catch (err) {
            try {
              closeSync(fd);
            } catch {
              // Already closed.
            }
            // Nothing is applied in memory; the uncommitted bracket is
            // severed on reopen/heal. Stop the instance so later writes
            // cannot glue themselves onto the partial group.
            broken = true;
            throw err;
          }
          try {
            closeSync(fd);
          } catch {
            // Already closed.
          }
          // Same post-write credential check as single publish.
          reassertLocked();

          // Durable first, state after.
          for (const part of planned) {
            if (!part.effective) continue;
            const { ack, size, key } = part.effective;
            st.seq = ack.seq;
            st.bytes += size;
            st.usageBytes += size;
            st.published += 1;
            if (key !== undefined) st.dedup.set(key, { id: ack.id, seq: ack.seq });
          }
          absorb(st);

          const acks = planned.map((part) =>
            part.reuse
              ? { id: part.reuse.id, seq: part.reuse.seq }
              : { id: part.effective.ack.id, seq: part.effective.ack.seq },
          );
          for (const [key, reservation] of reservations) {
            groupPending.delete(key);
            reservation.resolve(keyAcks.get(key));
          }
          return acks;
        });
      }).catch((err) => {
        // Covers write failure inside the job and rejection before the job
        // body (closed/broken/fenced bus): release any reservation still
        // pointing at this failed group.
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
        return withOwnership((lease, st0) => {
          const st = prepareLocked(st0);
          // replay(from) is inclusive of from; messagesAfter takes an
          // exclusive lower bound.
          const out = messagesAfter(st, from - 1);
          const n = out.length;
          if (n > 0) {
            try {
              writeEntry({ t: 's', n, g: epoch });
            } catch (err) {
              broken = true;
              throw err;
            }
            st.replayed += n;
            absorb(st);
          } else {
            absorb(st);
          }
          return out;
        });
      });
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
          (lease, st) => {
            const value = st.positions.get(name);
            absorb(st);
            return value;
          },
          { readonly: true },
        );
      } catch (err) {
        if (!fenced && !isLockError(err)) broken = true;
        throw err;
      }
      // Re-registering an existing name must not reset its position.
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
          (lease, st) => {
            const value = st.positions.get(name);
            absorb(st);
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
          (lease, view) => {
            st = view;
            pos = view.positions.get(name);
          },
          { readonly: true },
        );
      } catch (err) {
        if (!fenced && !isLockError(err)) broken = true;
        throw err;
      }
      if (pos === undefined) {
        // Persists a position line, so it validates the credential.
        persistPosition(name, 0);
        pos = 0;
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
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        withOwnership((lease, st0) => {
          // Credential validated at the lock door; heal any interrupted
          // earlier compact/truncate before starting this one.
          const st = prepareLocked(st0);

          // Fold every surviving message: snapshot contents plus the live
          // log, skipping anything truncation already discarded.
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
          // The snapshot is durable (fsync + atomic rename) before the log
          // is truncated, so a crash anywhere leaves either the intact old
          // log (half tmp snapshot, discarded on reopen) or the new
          // marker-led log — never something in between. It lands at a
          // gen/epoch-scoped name first: a preempted holder finishing a
          // frozen compaction cannot overwrite the current owner's
          // snapshot, and recovery simply ignores its older-epoch orphan.
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
          // Re-verify before the destructive half of the compaction. A
          // stale holder stops here: its scoped snapshot is an orphan the
          // next scan removes, and it never touches the canonical file, the
          // segments or the live log.
          reassertLocked();
          // Mirror to the canonical name only while ownership is current,
          // so legacy/hand-crafted layouts and any external reader still
          // see bus.snapshot.json. Uses a session-scoped tmp for the same
          // non-clobbering reason.
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

          // Every finalized segment is folded into the snapshot; drop
          // them. Deletion is unconditional: a failed unlink leaves only a
          // harmless leftover (the foldedSeq floor keeps it out of replay;
          // the next heal/reopen removes it).
          for (const seg of listSegments(dir)) {
            try {
              rmSync(seg.path, { force: true });
            } catch {
              // Leftover segment: ignored now, cleaned up later.
            }
          }
          // The fresh log leads with the compact marker and, when a
          // takeover has ever fenced this directory, a fence line carrying
          // the current watermark: replacing the log must not erase the
          // barrier, or a preempted holder's later append would be accepted.
          let head = st.markerLine(gen);
          if (st.fenceEpoch > 0) head += JSON.stringify({ t: 'f', g: st.fenceEpoch }) + '\n';
          const newFd = openSync(logTmpMine, 'w');
          try {
            writeSync(newFd, Buffer.from(head, 'utf8'));
            fsyncSync(newFd);
          } finally {
            closeSync(newFd);
          }
          // Final ownership check immediately before the live log is
          // replaced: no preempted holder may swap its (older-generation)
          // marker log over the current one.
          reassertLocked();
          renameSync(logTmpMine, logPath);
          syncDirBestEffort();

          // Refresh the cache from the settled directory and let the scan
          // physically reclaim superseded scoped snapshots it flags.
          const settled = scanDirectory(dir);
          healLocked(settled);
          absorb(scanDirectory(dir));
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
        // A zero bound discards nothing by definition.
        return Promise.resolve();
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        withOwnership((lease, st0) => {
          // Credential validated before the seal; the whole truncation is
          // one locked critical section.
          let st = prepareLocked(st0);

          // Truncation deals in whole segments only: seal the active
          // segment so its contents become eligible, and roll a fresh one.
          // The rename is atomic, so a crash here leaves the content under
          // exactly one name — never duplicated, never half moved.
          if (existsSync(logPath) && statSync(logPath).size > 0) {
            // A preempted holder frozen at the start of the critical
            // section must not seal the active log the new owner is about to
            // create: re-verify immediately before the rename.
            reassertLocked();
            const sealedName = path.join(dir, segmentName(st.nextSegmentIndex));
            renameSync(logPath, sealedName);
          }
          // The seal is the point of no return for this truncation: a
          // preempted holder frozen across a takeover must not proceed to
          // write a stale checkpoint into the fresh active log the new
          // owner already created.
          reassertLocked();

          // Deletion is a prefix of the segment chain: sweep oldest first
          // and stop at the first segment that must stay. A segment stays
          // while any registered consumer's position has not passed its
          // newest message, or while that message is beyond the bound. A
          // segment without messages carries no retention weight and goes
          // with the prefix (its position/stat lines are checkpointed
          // below).
          const doomed = [];
          let newHorizon = st.horizon;
          for (const seg of listSegments(dir)) {
            const max = segmentMaxSeq(seg.path);
            if (max !== null) {
              if (max <= st.foldedSeq) continue; // pre-compaction leftover, not truncation's job
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
          // Nothing can go: the roll above (if any) is the only effect.
          // Write no marker; the fresh active log stays empty.
          if (doomed.length === 0) {
            absorb(scanDirectory(dir));
            return;
          }

          // Checkpoint every state the doomed segments carry BEFORE
          // unlinking them. The marker is the first line of the fresh
          // active segment and becomes the new recovery anchor, so a crash
          // after it lands loses nothing: recovery resets to the marker and
          // reconciles leftovers.
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
            // The barrier must survive the segment seal/deletion: the
            // doomed sealed segments may be the only files carrying the
            // current watermark, so re-stamp it in the fresh active log,
            // immediately after its anchor marker.
            if (st.fenceEpoch > 0) {
              writeSync(
                markerFd,
                Buffer.from(JSON.stringify({ t: 'f', g: st.fenceEpoch }) + '\n', 'utf8'),
              );
            }
            // Always fsync here regardless of the fsync option: deletion is
            // only safe once the checkpoint is durable.
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
          // Checkpoint durable; re-verify before unlinking segments.
          reassertLocked();
          // Best-effort durability of the roll's and marker's directory
          // entries before unlinking. Segment deletion is UNCONDITIONAL:
          // a platform without directory fsync must not keep doomed
          // segments forever. A crash here recovers to the marker plus
          // whichever unlinks landed — both consistent.
          syncDirBestEffort();
          for (const seg of doomed) {
            try {
              rmSync(seg.path, { force: true });
            } catch {
              // Leftovers are filtered by the horizon and cleaned on the
              // next heal/reopen.
            }
          }
          syncDirBestEffort();

          // Refresh authoritative state from disk; usage now reflects the
          // freed segments (and any snapshot-folded bytes the new horizon
          // covers). The cumulative stats bytes/published stay put by
          // construction — only the marker carries them.
          st = scanDirectory(dir);
          absorb(st);
        });
      });
    },

    stats() {
      // Reading the merged counters is allowed even after a takeover: only
      // writes/truncation are fenced. After close the last cache stands.
      if (!closed) bestEffortView();
      return { seq, bytes, published, replayed };
    },

    // Business bytes of the messages still retained (not yet discarded by
    // retention truncation). Compaction does not change it; an empty bus is
    // zero. Merged across writers. Readable after close, like stats().
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
      // And every reservation held by a batch still waiting in the chain.
      for (const [key, g] of groupPending) {
        g.reject(new Error('bus is closed'));
        groupPending.delete(key);
      }
      return enqueue(() => {
        // Drop this session from the shared lease and remove its heartbeat
        // file. With no live sessions left remove the lease file so the
        // next opener starts clean; the epoch high-water file stays, so the
        // epoch still never repeats.
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
          // Closing never rejects: the lease heartbeat expiry reclaims the
          // session even if the lock cannot be taken right now.
        }
      });
    },
  };

  return bus;
}
