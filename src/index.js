import {
  openSync,
  closeSync,
  readSync,
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
//
// Commit pipeline files. Writers stage publish/publishBatch requests into
// bus.q.<token>.json WITHOUT holding the mutex (after sync validation and
// reservation), then take the lock and drain the whole staging area: every
// staged request from every process is merged into ONE bracketed commit
// group, so one flush covers the combined set. The drain's plan (seqs,
// dedup, per-request quota, stats) is persisted in a decision file before
// the group is flushed, and per-request outcomes (acks / rejection) are
// written to bus.r.<token>.json so a request drained by another process —
// or after a crash mid-flush — is adopted with the same first
// acknowledgement. A chunked group's torn tail is healed against the
// decision: missing chunks make the whole group invisible.
const QUEUE_PREFIX = 'bus.q.';
const DECISION_PREFIX = 'bus.c.';
const RESULT_PREFIX = 'bus.r.';
const TMP_PREFIXES = [
  'bus.owner.tmp.',
  'bus.epoch.tmp.',
  HB_TMP_PREFIX,
  'bus.snapshot.tmp.',
  'bus.snapshot.mirror.tmp.',
  'bus.log.tmp.',
  'bus.log.reset.tmp.',
  'bus.q.tmp.',
  'bus.c.tmp.',
  'bus.r.tmp.',
];
const envLease = Number(process.env.BUS_LEASE_MS);
const LEASE_MS = envLease > 0 ? envLease : 3000;
const envRenew = Number(process.env.BUS_RENEW_MS);
const RENEW_MS = envRenew > 0 ? envRenew : Math.min(250, Math.floor(LEASE_MS / 10));
// Test/diagnostic seams (not part of the public surface), read live so
// they can be toggled per test:
//  - BUS_DRAIN_DELAY_MS: hold the mutex at the drain entrance (heartbeat
//    kept fresh) so concurrently staged requests deterministically join
//    the same commit group.
//  - BUS_DRAIN_STALL_MS: after the first flush chunk block WITHOUT
//    refreshing the heartbeat, reproducing a holder seized mid group.
//  - BUS_FLUSH_FAIL_AT: fail the flush once this many group bytes are down.
const seamNumber = (name) => Number(process.env[name]) || 0;
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

// Pipeline file name patterns (constructors live in createBus, where the
// session id exists): token = pid + '-' + session tag + '-' + nonce, so two
// processes never collide and a process never reuses a crashed token.
const QUEUE_RE = /^bus\.q\.(.+)\.json$/;
const DECISION_RE = /^bus\.c\.(.+)\.json$/;
const RESULT_RE = /^bus\.r\.(.+)\.json$/;

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

// Read just the first line of the active log as a structural fingerprint.
// Compaction/truncate always replace that line with a marker, so comparing
// it detects a rename-based reset even when the filesystem reuses an inode
// number. Capped at a small prefix: only the head bytes are needed.
const readHeadSync = (filePath) => {
  let fd;
  try {
    fd = openSync(filePath, 'r');
  } catch {
    return null;
  }
  try {
    const buf = Buffer.allocUnsafe(4096);
    const n = readSync(fd, buf, 0, buf.length, 0);
    if (!Number.isInteger(n) || n <= 0) return '';
    const got = buf.subarray(0, n);
    const nl = got.indexOf(0x0a);
    return got.subarray(0, nl < 0 ? got.length : nl).toString('utf8');
  } catch {
    return null;
  } finally {
    try {
      closeSync(fd);
    } catch {
      // Already closed.
    }
  }
};

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

/**
 * Extract the committed, non-stale entries from a raw log buffer, using the
 * same bracket/fence rules as scanDirectory's recovery walk but without
 * touching any state: the incremental tail reader applies the result onto
 * the cached merged view. Returns { units, watermark, closed, structural }:
 * `closed` is false when the buffer ends inside an uncommitted batch
 * bracket (a crash mid group), in which case the caller heals with a full
 * scan; `structural` is set when a truncate/compact marker or a fence was
 * seen, which also forces a full rescan.
 */
function committedUnits(raw, startWatermark) {
  const units = [];
  let watermark = startWatermark;
  let lineStart = 0;
  let group = null;
  let structural = false;
  const stale = (entry) =>
    entry !== null && typeof entry.g === 'number' && entry.g < watermark;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] !== 0x0a) continue;
    const entry = i > lineStart ? JSON.parse(raw.toString('utf8', lineStart, i)) : null;
    if (entry && entry.t === 'f') {
      if (typeof entry.g === 'number' && entry.g > watermark) watermark = entry.g;
      structural = true;
      group = null;
    } else if (group !== null) {
      if (stale(entry)) {
        // A preempted holder's bytes never close or enter the group.
      } else if (entry && entry.t === 'bk' && entry.id === group.id) {
        for (const member of group.entries) units.push(member);
        group = null;
      } else if (entry && entry.t === 'b') {
        group = { id: entry.id, entries: [] };
      } else if (entry) {
        group.entries.push(entry);
      }
    } else if (entry && entry.t === 'b') {
      if (!stale(entry)) group = { id: entry.id, entries: [] };
    } else if (entry) {
      if (entry.t === 't' || entry.t === 'c') {
        structural = true;
      } else if (!stale(entry)) {
        units.push(entry);
      }
    }
    lineStart = i + 1;
  }
  // Bytes past the final newline are a torn trailing write (a crash mid
  // line). They are never parsed; the caller treats them like an open
  // bracket — heal before appending.
  const torn = lineStart < raw.length;
  return { units, watermark, closed: group === null, structural, torn };
}


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

  // Identity of the active log as the scan left it. The incremental
  // catch-up is valid only while the SAME physical file has grown: the
  // (dev,ino) pair is the cheap check, and the first-line fingerprint is
  // the portable guard — a compaction reset or a truncation seal always
  // rewrites the head (a {t:'c'} / {t:'t'} marker), whereas ordinary
  // appends leave it byte-identical. The head check matters on filesystems
  // (e.g. WSL drvfs) where rename can preserve an inode number.
  let activeId = null;
  try {
    const stt = statSync(logPath);
    activeId = { dev: stt.dev, ino: stt.ino, size: stt.size, head: readHeadSync(logPath) };
  } catch {
    activeId = { dev: 0, ino: 0, size: 0, head: null };
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
    activeId,
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

  // ---- commit-pipeline state -------------------------------------------
  // Scan-shaped merged view kept current between full directory scans by
  // the incremental active-log tail reader. Full scans happen on open,
  // takeover fences, truncate seals and compaction; ordinary appends only
  // read newly durable bytes.
  let view = null;
  let horizon = 0;
  let fenceWatermark = 0;
  // Cursor for the incremental reader: cache is current through byte
  // offset `off` of active log inode (dev,ino). A seal/compact rename
  // replaces the inode and forces a full directory scan.
  let cursor = null;
  const sessionTag = sessionId.slice(0, 8);
  // Monotonic per-process counter embedded in each token so queue files
  // sharing one mtime tick still order by local enqueue sequence.
  let tokenCounter = 0;
  const newToken = () =>
    `${process.pid}-${sessionTag}-${Date.now().toString(36)}-${String(tokenCounter++).padStart(6, '0')}-${Math.random().toString(36).slice(2, 8)}`;
  const qPath = (token) => path.join(dir, `${QUEUE_PREFIX}${token}.json`);
  const rPath = (token) => path.join(dir, `${RESULT_PREFIX}${token}.json`);
  const cPath = (gid) => path.join(dir, `${DECISION_PREFIX}${gid}.json`);
  const rTmpPath = (token) =>
    path.join(dir, `bus.r.tmp.${process.pid}.${sessionTag}.${token.slice(-6)}`);
  const qTmpPath = (token) => path.join(dir, `bus.q.tmp.${token}`);
  const cTmpPath = (gid) =>
    path.join(dir, `bus.c.tmp.${process.pid}.${sessionTag}.${gid.slice(0, 8)}`);

  const FENCE_MESSAGE = 'bus write ownership was taken over by another process';
  const RANGE_MESSAGE_SINGLE = 'publish: retained bytes would exceed maxBytes';
  const RANGE_MESSAGE_BATCH = 'publishBatch: retained bytes would exceed maxBytes';

  // Copy the scan-shaped view into the scalar/map caches the bus methods
  // read directly.
  const syncScalars = () => {
    seq = view.seq;
    bytes = view.bytes;
    published = view.published;
    replayed = view.replayed;
    usageBytes = view.usageBytes;
    dedup.clear();
    for (const [key, value] of view.dedup) dedup.set(key, value);
    positions.clear();
    for (const [name, pos] of view.positions) positions.set(name, pos);
  };

  // Adopt a full scan as the current cache and rebase the tail cursor on
  // the active log's post-scan identity.
  const adoptFullScan = (st) => {
    view = st;
    horizon = st.horizon;
    fenceWatermark = st.fenceEpoch;
    cursor = {
      dev: st.activeId.dev,
      ino: st.activeId.ino,
      head: st.activeId.head,
      off: st.activeId.size,
      watermark: st.fenceEpoch,
    };
    syncScalars();
    // A structural change this writer just observed/healed must replace the
    // read view too: its cached message pages predate the reset. Defined
    // later in the file (an arrow const), so guard for the acquisition-time
    // prime that runs before it is initialized.
    if (typeof adoptReadScan === 'function') adoptReadScan(st);
  };

  // Read a slice [off, end) of a regular file synchronously.
  const readTail = (file, off, length) => {
    const buf = Buffer.allocUnsafe(length);
    const fd = openSync(file, 'r');
    try {
      let got = 0;
      while (got < length) {
        const n = readSync(fd, buf, got, length - got, off + got);
        if (!Number.isInteger(n) || n <= 0) break;
        got += n;
      }
      return buf.subarray(0, got);
    } finally {
      closeSync(fd);
    }
  };

  // Apply committed tail units (m / s / p lines, fence-filtered) to the
  // view and scalar caches. Only counters and tables are retained on the
  // hot path — message payloads are NOT cached, so a long-lived writer's
  // memory does not grow with the log. replay/read/compact read message
  // contents with a full scan the way the baseline did; the write path
  // (the cost being removed) never does.
  const applyUnits = (units) => {
    for (const entry of units) {
      if (entry.t === 'm') {
        if (entry.seq <= horizon) continue;
        view.seq = entry.seq;
        view.bytes += entry.bytes;
        view.usageBytes += entry.bytes;
        view.published += 1;
        if (typeof entry.d === 'string') {
          view.dedup.set(entry.d, { id: entry.id, seq: entry.seq });
        }
      } else if (entry.t === 's') {
        view.replayed += entry.n;
      } else if (entry.t === 'p') {
        view.positions.set(entry.name, entry.pos);
      }
    }
    syncScalars();
  };

  /**
   * Bring the merged cache current. The common case reads only the bytes
   * appended to the active log since the last look; a changed inode
   * (truncate seal / compact reset), a torn or uncommitted tail, or a
   * structural marker escalates to a full scanDirectory (+ healing when
   * `mutating`). A mutating catch-up first reconciles decision files and
   * sweeps queues a crashed drain left behind.
   */
  const catchupLocked = ({ mutating }) => {
    if (mutating) {
      reconcileLocked();
      sweepOrphanQueuesLocked();
    }
    // Fast path: same device, same inode number, AND an unchanged head
    // (the portable reset guard for filesystems that reuse inode numbers
    // across rename), and the file only grew. Anything else is a
    // structural change → full scan.
    let info;
    try {
      info = statSync(logPath);
    } catch {
      info = { dev: 0, ino: 0, size: 0 };
    }
    if (
      view &&
      cursor &&
      cursor.dev === info.dev &&
      cursor.ino === info.ino &&
      info.size >= cursor.off
    ) {
      const headNow = info.size > 0 ? readHeadSync(logPath) : '';
      if (headNow === cursor.head) {
        if (info.size === cursor.off) return;
        const raw = readTail(logPath, cursor.off, info.size - cursor.off);
        const parsed = committedUnits(raw, cursor.watermark);
        if (parsed.closed && !parsed.structural && !parsed.torn) {
          applyUnits(parsed.units);
          cursor.off = info.size;
          cursor.watermark = parsed.watermark;
          fenceWatermark = Math.max(fenceWatermark, parsed.watermark);
          view.fenceEpoch = fenceWatermark;
          return;
        }
      }
      if (!mutating) {
        // Open bracket / torn tail / marker: read-only callers observe the
        // authoritative scan but must not heal anything.
        adoptFullScan(scanDirectory(dir));
        return;
      }
      // Fall through to the healing full scan below.
    }
    if (mutating) {
      let st = scanDirectory(dir);
      if (healLocked(st)) st = scanDirectory(dir);
      adoptFullScan(st);
    } else {
      adoptFullScan(scanDirectory(dir));
    }
  };

  // ---- lock-free read view ----------------------------------------------
  // A read-only materialized view, refreshed WITHOUT the cross-process
  // mutex and without walking the directory: ordinary appends are caught by
  // the same incremental active-log tail reader the write path uses; a
  // snapshot switch (compact/truncate), a fence, or a writer caught mid
  // group escalates to one non-destructive scanDirectory (which never heals
  // or unlinks), so readers always observe a committed prefix and never
  // block behind a drain. Reads therefore stay off the write serial chain
  // entirely: stats, usage, position lookups, read, readRange and the read
  // half of replay all serve from here.
  let readSt = null;
  let readCursor = null;

  const cloneMsg = (m) => ({ seq: m.seq, id: m.id, record: structuredClone(m.record) });

  // Adopt a scan as the read view WITHOUT aliasing the write view: the
  // drain mutates its own `view` in place after every commit, so sharing the
  // object would let a write advance the read counters and then let the
  // incremental tail reader apply the same bytes again. Structural scans
  // (compact/truncate) are rare, so copying the message arrays here is
  // cheap; ordinary appends never go through this.
  const adoptReadScan = (st) => {
    readSt = {
      seq: st.seq,
      bytes: st.bytes,
      published: st.published,
      replayed: st.replayed,
      horizon: st.horizon,
      usageBytes: st.usageBytes,
      dedup: new Map(st.dedup),
      positions: new Map(st.positions),
      snapshotGen: st.snapshotGen,
      foldedSeq: st.foldedSeq,
      fenceEpoch: st.fenceEpoch,
      baseMessages: st.baseMessages.map(cloneMsg),
      live: st.live.map(cloneMsg),
    };
    // A read-only scan does not heal the open-bracket/torn tail it found:
    // the cursor must stop at the durable prefix (the tail's planned
    // length), not at EOF, so the incremental reader re-reads the group
    // once its commit line lands instead of skipping past its messages.
    let off = st.activeId.size;
    for (const tail of st.tails) {
      if (tail.path === logPath && tail.length < off) off = tail.length;
    }
    readCursor = {
      dev: st.activeId.dev,
      ino: st.activeId.ino,
      head: st.activeId.head,
      off,
      watermark: st.fenceEpoch,
    };
  };

  // Fold committed tail units into the read view. Unlike the write hot
  // cache, the read view also retains message payloads (in `live`) so a
  // paged read needs no directory walk at all.
  const applyReadUnits = (units) => {
    for (const entry of units) {
      if (entry.t === 'm') {
        if (entry.seq <= readSt.horizon) continue;
        readSt.seq = entry.seq;
        readSt.bytes += entry.bytes;
        readSt.usageBytes += entry.bytes;
        readSt.published += 1;
        if (typeof entry.d === 'string') {
          readSt.dedup.set(entry.d, { id: entry.id, seq: entry.seq });
        }
        readSt.live.push({ seq: entry.seq, id: entry.id, record: entry.record });
      } else if (entry.t === 's') {
        readSt.replayed += entry.n;
      } else if (entry.t === 'p') {
        readSt.positions.set(entry.name, entry.pos);
      }
    }
  };

  const refreshReadView = () => {
    const publish = () => {
      if (readSt) {
        seq = readSt.seq;
        bytes = readSt.bytes;
        published = readSt.published;
        replayed = readSt.replayed;
        usageBytes = readSt.usageBytes;
      }
    };
    // First time, or after an invalidation: one lock-free full scan.
    if (!readSt || !readCursor) {
      try {
        adoptReadScan(scanDirectory(dir));
      } catch {
        // A writer reshaping the directory mid-scan: keep the empty view;
        // the next read retries.
        if (!readSt) {
          adoptReadScan({
            seq: 0, bytes: 0, published: 0, replayed: 0, horizon: 0,
            usageBytes: 0, dedup: new Map(), positions: new Map(),
            snapshotGen: 0, foldedSeq: 0, fenceEpoch: 0,
            baseMessages: [], live: [], tails: [],
            activeId: { dev: 0, ino: 0, size: 0, head: null },
          });
        }
      }
      publish();
      return;
    }
    let info;
    try {
      info = statSync(logPath);
    } catch {
      // Active log momentarily absent (a rename in flight): the last
      // materialized view is still a valid committed prefix.
      return;
    }
    if (
      readCursor.dev === info.dev &&
      readCursor.ino === info.ino &&
      info.size >= readCursor.off
    ) {
      const headNow = info.size > 0 ? readHeadSync(logPath) : '';
      if (headNow === readCursor.head) {
        if (info.size === readCursor.off) return;
        let parsed = null;
        try {
          const raw = readTail(logPath, readCursor.off, info.size - readCursor.off);
          parsed = committedUnits(raw, readCursor.watermark);
        } catch {
          // A racing rename/unlink: leave the view for the next refresh.
          return;
        }
        if (parsed.closed && !parsed.structural && !parsed.torn) {
          applyReadUnits(parsed.units);
          readCursor.off = info.size;
          readCursor.watermark = parsed.watermark;
          publish();
          return;
        }
        // Open bracket (writer mid group), torn tail or a marker/fence:
        // fall through to a lock-free authoritative scan. It never heals,
        // and parks the cursor at the durable prefix, so the committing
        // group is picked up whole by the next incremental read.
      }
    }
    try {
      adoptReadScan(scanDirectory(dir));
    } catch {
      // Directory reshaped while the scan read it (rename/unlink race):
      // serve the previous materialized view — an immutable committed
      // prefix — and retry on the next call.
    }
    publish();
  };

  // Binary search: first index whose seq is strictly greater than floor.
  const firstSeqAbove = (arr, floor) => {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid].seq > floor) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  };

  // Inclusive-of-start ascending page, at most `limit` entries (limit may
  // be Infinity). Snapshot survivors first, then the live suffix; both
  // arrays are entered by binary search so a page near the tail costs
  // O(log n + limit), never O(n).
  const readViewPage = (start, limit) => {
    if (!readSt) return [];
    const floor = Math.max(start - 1, readSt.horizon);
    const out = [];
    let i = firstSeqAbove(readSt.baseMessages, floor);
    for (; i < readSt.baseMessages.length && out.length < limit; i++) {
      const m = readSt.baseMessages[i];
      out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
    }
    if (out.length < limit) {
      const logFloor = Math.max(floor, readSt.foldedSeq);
      let j = firstSeqAbove(readSt.live, logFloor);
      for (; j < readSt.live.length && out.length < limit; j++) {
        const m = readSt.live[j];
        out.push({ seq: m.seq, id: m.id, record: structuredClone(m.record) });
      }
    }
    return out;
  };

  // Stage one publish/publishBatch request into the lock-free queue. The
  // file is durable before the mutex is taken, so whichever process drains
  // next observes the request even if the staging process never gets the
  // lock itself.
  const stageRequest = (token, kind, items) => {
    const payload = JSON.stringify({
      v: 1,
      e: epoch,
      s: sessionId,
      k: kind,
      items: items.map((it) => {
        const part = { j: it.recordJson, z: it.size };
        if (it.dedupKey !== undefined) part.d = it.dedupKey;
        return part;
      }),
    });
    const tmp = qTmpPath(token);
    const fd = openSync(tmp, 'w');
    try {
      writeSync(fd, Buffer.from(payload, 'utf8'));
      if (fsync) fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(tmp, qPath(token));
  };

  const writeOutcomeLocked = (token, result) => {
    try {
      const tmp = rTmpPath(token);
      const fd = openSync(tmp, 'w');
      try {
        writeSync(fd, Buffer.from(JSON.stringify({ v: 1, result }), 'utf8'));
        if (fsync) fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(tmp, rPath(token));
      return true;
    } catch {
      // The decision + visible commit remain the source of truth: a
      // missing outcome is re-synthesised by the next reconcile, which
      // runs before any drain gathers the still-present queue.
      return false;
    }
  };

  const readOutcome = (token) => {
    try {
      return JSON.parse(readFileSync(rPath(token), 'utf8')).result;
    } catch {
      return null;
    }
  };

  // Search the ordered log for a commit bracket a recovery walk would
  // actually apply: a matching bk that is complete and not stamped below
  // the fence watermark in effect at its physical position. A bk a
  // preempted holder landed after a takeover fence is invisible and must
  // not settle its tokens as success; one committed before the fence is
  // legitimate history even when the decision's epoch is older.
  const committedGroupVisible = (gid) => {
    const files = listSegments(dir).map((s) => s.path);
    if (existsSync(logPath)) files.push(logPath);
    let wm = 0;
    let openId = null;
    for (const file of files) {
      let raw;
      try {
        raw = readFileSync(file);
      } catch {
        continue;
      }
      let lineStart = 0;
      for (let i = 0; i < raw.length; i++) {
        if (raw[i] !== 0x0a) continue;
        let entry = null;
        if (i > lineStart) {
          try {
            entry = JSON.parse(raw.toString('utf8', lineStart, i));
          } catch {
            entry = null;
          }
        }
        const stale =
          entry !== null && typeof entry.g === 'number' && entry.g < wm;
        if (entry && entry.t === 'f') {
          if (typeof entry.g === 'number' && entry.g > wm) wm = entry.g;
          openId = null;
        } else if (openId !== null) {
          if (!stale && entry && entry.t === 'bk' && entry.id === openId) {
            if (openId === gid) return true;
            openId = null;
          } else if (!stale && entry && entry.t === 'b') {
            openId = entry.id;
          }
        } else if (entry && entry.t === 'b' && !stale) {
          openId = entry.id;
        }
        lineStart = i + 1;
      }
    }
    return false;
  };

  /**
   * Reconcile decision files a crashed drain left behind:
 *  - a committed, visible group: every per-token outcome promised by the
 *    decision is (re)synthesised and its queue removed; every requester
 *    (across processes or after restart) adopts exactly that first ack.
 *  - anything else: the decision is discarded. A same-epoch partial group
 *    is severed by the caller's healing scan; a fenced group's bytes are
 *    filtered by the watermark and never mix in.
   */
  const reconcileLocked = () => {
    let names = [];
    try {
      names = readdirSync(dir);
    } catch {
      return;
    }
    for (const name of names) {
      const m = DECISION_RE.exec(name);
      if (!m) continue;
      const gid = m[1];
      let decision;
      try {
        decision = JSON.parse(readFileSync(path.join(dir, name), 'utf8'));
      } catch {
        // Half-written decision file: it cannot have led to a commit.
        try {
          rmSync(path.join(dir, name), { force: true });
        } catch {
          // Removed concurrently.
        }
        continue;
      }
      if (committedGroupVisible(gid)) {
        for (const part of decision.results || []) {
          writeOutcomeLocked(part.token, part.result);
          try {
            rmSync(qPath(part.token), { force: true });
          } catch {
            // Already gone: the requester's adopt path settles it.
          }
        }
      }
      // Spent once the visible-commit outcomes are settled. An uncommitted
      // group's bytes are healed/fenced by the catch-up that follows.
      try {
        rmSync(path.join(dir, name), { force: true });
      } catch {
        // Another drain removed it.
      }
    }
  };

  // Reap queue files a crashed/frozen caller left behind, plus outcome
  // files that can never be consumed: a queue whose requester heartbeat is
  // stale belongs to a dead process (its token outcome, if one exists, goes
  // with it), and any older-than-lease outcome file is garbage because
  // outcomes are adopted within milliseconds of being written.
  const sweepOrphanQueuesLocked = () => {
    const now = Date.now();
    let names = [];
    try {
      names = readdirSync(dir);
    } catch {
      return;
    }
    const staleResults = new Set();
    for (const name of names) {
      const qm = QUEUE_RE.exec(name);
      if (qm) {
        const token = qm[1];
        let req = null;
        try {
          req = JSON.parse(readFileSync(path.join(dir, name), 'utf8'));
        } catch {
          req = null;
        }
        const dead = !req || typeof req.s !== 'string' || !heartbeatAlive(req.s);
        if (readOutcome(token) !== null || dead) {
          if (dead) staleResults.add(token);
          try {
            rmSync(path.join(dir, name), { force: true });
          } catch {
            // Concurrent cleanup.
          }
        }
      } else if (RESULT_RE.test(name)) {
        let age = Infinity;
        try {
          age = now - statSync(path.join(dir, name)).mtimeMs;
        } catch {
          age = Infinity;
        }
        if (age > LEASE_MS) staleResults.add(RESULT_RE.exec(name)[1]);
      }
    }
    for (const token of staleResults) {
      try {
        rmSync(rPath(token), { force: true });
      } catch {
        // Already gone.
      }
    }
  };

  /**
   * The drain: merge every staged live request in the directory into one
   * bracketed commit group. The plan (continuous seqs, first-occurrence
   * dedup across requests and history, per-item quota, four cumulative
   * counters) is shaped once, persisted as a decision, flushed in one
   * chunked write, and only then exposed via per-token outcome files.
   * Returns the caller's own result object.
   */
  const drainLocked = (lease, ownToken) => {
    const fenceResult = { ok: false, ctor: 'Error', message: FENCE_MESSAGE };

    // Test seam: hold the lock at the drain entrance (heartbeat fresh) so
    // requests other processes stage during the wait deterministically
    // merge into this same group.
    const drainDelay = seamNumber('BUS_DRAIN_DELAY_MS');
    if (drainDelay > 0) {
      const slot = new Int32Array(new SharedArrayBuffer(4));
      const waited = Date.now();
      while (Date.now() - waited < drainDelay) {
        writeHeartbeat();
        Atomics.wait(slot, 0, 0, Math.min(20, drainDelay));
      }
    }

    // Gather staged requests oldest first; mtime is the cross-process
    // enqueue order, the token is a deterministic tie-breaker. The gather
    // is a bounded grace window, not a single snapshot: requests other
    // writers stage WHILE this drain already holds the mutex (including
    // ones arriving during the entrance wait above) are re-collected until
    // the directory is briefly quiet, so real concurrent writers merge into
    // this one commit group. The wait is paid only while another live
    // session shares the lease (a sole writer needs no grace and keeps the
    // uncontended fast path), stays bounded, and keeps the heartbeat fresh.
    const gatherMs = seamNumber('BUS_DRAIN_GATHER_MS') || 25;
    const settleMs = Math.min(5, gatherMs);
    const otherLive = lease.sessions.some((s) => s.id !== sessionId && heartbeatAlive(s.id));
    const files = [];
    const seen = new Set();
    const scanOnce = () => {
      let added = 0;
      for (const name of readdirSync(dir)) {
        const m = QUEUE_RE.exec(name);
        if (!m || seen.has(m[1])) continue;
        seen.add(m[1]);
        let mt = 0;
        try {
          mt = statSync(path.join(dir, name)).mtimeMs;
        } catch {
          mt = 0;
        }
        files.push({ name, token: m[1], mt });
        added += 1;
      }
      return added;
    };
    scanOnce();
    if (otherLive) {
      // While arrivals keep coming, extend the window (bounded); stop after
      // one quiet tick so an uncontended drain waits at most one tick.
      const gatherStart = Date.now();
      let quietTicks = 0;
      for (;;) {
        writeHeartbeat();
        sleepSync(settleMs);
        const added = scanOnce();
        const elapsed = Date.now() - gatherStart;
        if (added > 0) quietTicks = 0;
        else quietTicks += 1;
        if (elapsed >= gatherMs || quietTicks >= 1) break;
      }
    }
    files.sort((a, b) => (a.mt !== b.mt ? a.mt - b.mt : a.token < b.token ? -1 : 1));

    const requests = [];
    for (const f of files) {
      let req = null;
      try {
        req = JSON.parse(readFileSync(path.join(dir, f.name), 'utf8'));
      } catch {
        req = null;
      }
      // A token carrying an outcome is already settled; drop the leftover.
      const settled = readOutcome(f.token);
      if (settled !== null) {
        try {
          rmSync(path.join(dir, f.name), { force: true });
        } catch {
          // Concurrent cleanup.
        }
        continue;
      }
      if (!req || !Array.isArray(req.items) || typeof req.s !== 'string') {
        // Corrupt/half queue file (never expected: staging is atomic): take
        // no byte from it and remove.
        try {
          rmSync(path.join(dir, f.name), { force: true });
        } catch {
          // Already gone.
        }
        continue;
      }
      if (!heartbeatAlive(req.s)) {
        // Dead requester: orphaned intent, never drained.
        try {
          rmSync(path.join(dir, f.name), { force: true });
        } catch {
          // Already gone.
        }
        continue;
      }
      // Epoch barrier at the pipeline boundary: a request a preempted
      // holder queued before losing ownership fails wholesale and no byte
      // of it mixes into the group.
      if (typeof req.e !== 'number' || req.e !== lease.epoch) {
        requests.push({ token: f.token, stale: true });
        continue;
      }
      requests.push({ token: f.token, kind: req.k === 'batch' ? 'batch' : 'single', items: req.items });
    }

    // Plan the merged group. Each request is resolved as a whole against
    // the running merged plan; a request that over quota contributes
    // nothing (no seq, no key registration, no line) and gets a RangeError
    // while earlier and later requests proceed untouched.
    const gid = randomUUID();
    // Running merged plan, mutated only by accepted requests.
    let nextSeq = seq;
    let groupBytes = 0;
    let groupCount = 0;
    // Keys whose first ack is allocated inside THIS group, shared across
    // every request merged into it.
    const fresh = new Map();
    const lineBuffers = [];
    const results = [];

    for (const req of requests) {
      if (req.stale) {
        results.push({ token: req.token, result: fenceResult });
        continue;
      }
      // Scratch state for this request alone; merged into the group plan
      // only when every item resolves and the quota fits.
      const acks = [];
      const owned = new Map();
      const plannedLines = [];
      let addBytes = 0;
      let addCount = 0;
      let trialSeq = nextSeq;
      let rejected = null;
      for (const item of req.items) {
        const size =
          typeof item.z === 'number' ? item.z : Buffer.byteLength(item.j, 'utf8');
        const key = typeof item.d === 'string' ? item.d : undefined;
        let hit = null;
        if (key !== undefined) {
          hit = dedup.get(key) || fresh.get(key) || owned.get(key) || null;
        }
        if (hit) {
          acks.push({ id: hit.id, seq: hit.seq });
          continue;
        }
        // Per-item occupancy against the merged usage plus the bytes every
        // already-accepted request of this group adds. One byte over
        // rejects the single record or the whole batch with the group
        // plan exactly as it was.
        if (quota !== undefined && usageBytes + groupBytes + addBytes + size > quota) {
          rejected =
            req.kind === 'batch'
              ? { ok: false, ctor: 'RangeError', message: RANGE_MESSAGE_BATCH }
              : { ok: false, ctor: 'RangeError', message: RANGE_MESSAGE_SINGLE };
          break;
        }
        trialSeq += 1;
        const id = randomUUID();
        const ack = { id, seq: trialSeq };
        acks.push(ack);
        if (key !== undefined) owned.set(key, ack);
        let line = `{"t":"m","seq":${trialSeq},"id":${JSON.stringify(id)},"bytes":${size},"g":${lease.epoch}`;
        if (key !== undefined) line += `,"d":${JSON.stringify(key)}`;
        line += `,"record":${item.j}}\n`;
        plannedLines.push(Buffer.from(line, 'utf8'));
        addBytes += size;
        addCount += 1;
      }
      if (rejected) {
        // Nothing of this request enters the merged plan: scratch is simply
        // discarded and nextSeq/groupBytes stay at the previous request's
        // values, so seqs assigned to later requests have no hole.
        results.push({ token: req.token, result: rejected });
      } else {
        lineBuffers.push(...plannedLines);
        groupBytes += addBytes;
        groupCount += addCount;
        nextSeq = trialSeq;
        for (const [key, ack] of owned) fresh.set(key, ack);
        results.push({
          token: req.token,
          result:
            req.kind === 'batch' ? { ok: true, acks } : { ok: true, ack: acks[0] },
        });
      }
    }

    // If nothing effective survived (all rejects/duplicates-only groups
    // with... duplicates alone also produce no lines), expose outcomes
    // directly without opening a bracket.
    if (groupCount === 0) {
      for (const r of results) {
        writeOutcomeLocked(r.token, r.result);
        try {
          rmSync(qPath(r.token), { force: true });
        } catch {
          // Already drained/removed.
        }
      }
      const own = results.find((r) => r.token === ownToken);
      return own ? own.result : null;
    }

    // Persist the decision before a single group byte moves: on a torn
    // flush a reopen reconcile knows exactly which tokens the group owed,
    // and whether the commit line made it decides all-or-nothing.
    const decision = { v: 1, id: gid, e: lease.epoch, results };
    {
      const tmp = cTmpPath(gid);
      const fd = openSync(tmp, 'w');
      try {
        writeSync(fd, Buffer.from(JSON.stringify(decision), 'utf8'));
        if (fsync) fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(tmp, cPath(gid));
      if (fsync) syncDirBestEffort();
    }
    // Boundary check: the group is planned but no byte is written yet.
    reassertLocked();

    const head = Buffer.from(JSON.stringify({ t: 'b', id: gid, g: lease.epoch }) + '\n', 'utf8');
    const tail = Buffer.from(JSON.stringify({ t: 'bk', id: gid, g: lease.epoch }) + '\n', 'utf8');

    // Byte offset the log must return to if the flush fails: a write or
    // fsync failure rolls the whole group back physically, leaving the bus
    // byte-for-byte as it was before the bracket.
    let startOffset = 0;
    try {
      startOffset = statSync(logPath).size;
    } catch {
      startOffset = 0;
    }

    const fd = openSync(logPath, 'a');
    let bkWritten = false;
    let wrote = 0;
    let flushError = null;
    try {
      const writeAll = (buf) => {
        let off = 0;
        while (off < buf.length) {
          const n = writeSync(fd, buf, off, buf.length - off);
          if (!Number.isInteger(n) || n <= 0) {
            throw new Error('commit: write made no progress, group is not durable');
          }
          off += n;
        }
      };
      writeAll(head);
      wrote += head.length;
      // Long groups flush in chunks; the heartbeat and the epoch credential
      // are re-validated at every chunk boundary, so a holder seized
      // mid-group stops before the commit line and its partial group is
      // wholly invisible.
      const CHUNK = 256;
      for (let i = 0; i < lineBuffers.length; i += CHUNK) {
        writeHeartbeat();
        reassertLocked();
        const part = Buffer.concat(lineBuffers.slice(i, i + CHUNK));
        writeAll(part);
        wrote += part.length;
        // Seam: inject a disk-write failure once this many group bytes are
        // down, exercising whole-group rollback and torn-tail healing.
        if (seamNumber('BUS_FLUSH_FAIL_AT') > 0 && wrote - head.length >= seamNumber('BUS_FLUSH_FAIL_AT')) {
          throw new Error('injected flush failure');
        }
        // Seam: after the first chunk block WITHOUT refreshing the
        // heartbeat, exactly as a process seized mid group would; the next
        // boundary check observes the takeover and the partial bracket
        // (no commit line) stays wholly invisible behind the fence.
        const stall = seamNumber('BUS_DRAIN_STALL_MS');
        if (stall > 0 && i === 0) {
          sleepSync(stall);
          reassertLocked();
        }
      }
      writeHeartbeat();
      reassertLocked();
      writeAll(tail);
      wrote += tail.length;
      bkWritten = true;
      // A flush (fsync) failure is a write failure: the group must roll
      // back wholesale rather than be acknowledged un-durably.
      if (fsync) fsyncSync(fd);
    } catch (err) {
      flushError = err;
    }
    try {
      closeSync(fd);
    } catch {
      // Already closed.
    }

    if (flushError !== null) {
      const wasFence =
        fenced ||
        (flushError && /taken over|invalidated/.test(String(flushError.message)));
      if (!wasFence) {
        // Disk write/fsync failure: sever the group physically so the log
        // is exactly what it was before the bracket, then settle every
        // token of the group with the failure and burn the decision. The
        // instance is broken: nothing else may append until reopen heals.
        try {
          truncateSync(logPath, startOffset);
        } catch {
          // The truncate failed too; the uncommitted bracket is severed by
          // the next lock holder's healing scan / reopen.
        }
        const failResult = {
          ok: false,
          ctor: 'Error',
          message: flushError.message || 'commit failed',
        };
        for (const r of results) {
          writeOutcomeLocked(r.token, failResult);
          try {
            rmSync(qPath(r.token), { force: true });
          } catch {
            // Leftover: the outcome settles it regardless.
          }
        }
        try {
          rmSync(cPath(gid), { force: true });
        } catch {
          // Already removed.
        }
        broken = true;
        cursor = null;
        throw flushError;
      }
      // Fenced mid-flush: the partial bytes are stamped with the stale
      // epoch and stay physically in place exactly as a preempted
      // holder's racy append would — the fence watermark filters them from
      // every reader. The queued group as a whole fails; burn its decision
      // so no token can settle as success.
      for (const r of results) {
        writeOutcomeLocked(r.token, fenceResult);
        try {
          rmSync(qPath(r.token), { force: true });
        } catch {
          // Leftover: the outcome settles it regardless.
        }
      }
      try {
        rmSync(cPath(gid), { force: true });
      } catch {
        // Already removed.
      }
      fenced = true;
      cursor = null;
      throw flushError;
    }

    // Final boundary check once the flush is durable. A takeover observed
    // only after the commit line landed cannot un-write the group (it is
    // legitimate history before any later fence), so settle the success
    // outcomes first; this instance is still fenced for every later op.
    let postFence = null;
    try {
      reassertLocked();
    } catch (err) {
      postFence = err;
    }

    // Durable first, state after: advance the merged view by exactly the
    // planned group.
    view.seq = nextSeq;
    view.bytes += groupBytes;
    view.usageBytes += groupBytes;
    view.published += groupCount;
    for (const [key, ack] of fresh) view.dedup.set(key, { id: ack.id, seq: ack.seq });
    syncScalars();
    if (cursor) {
      cursor.off += wrote;
      cursor.watermark = Math.max(cursor.watermark, fenceWatermark);
    } else {
      try {
        const info = statSync(logPath);
        cursor = {
          dev: info.dev,
          ino: info.ino,
          head: info.size > 0 ? readHeadSync(logPath) : '',
          off: info.size,
          watermark: fenceWatermark,
        };
      } catch {
        cursor = null;
      }
    }

    // Expose outcomes (durable), then retire each queue whose outcome
    // landed. A token whose outcome write failed keeps BOTH its queue and
    // the decision, so the very next reconcile (same token's adopt retry
    // or another opener) re-synthesises the same result from the visible
    // commit — never a false failure or a second publish.
    let allSettled = true;
    for (const r of results) {
      if (writeOutcomeLocked(r.token, r.result)) {
        try {
          rmSync(qPath(r.token), { force: true });
        } catch {
          // Leftover: the outcome settles it on the next sweep.
        }
      } else {
        allSettled = false;
      }
    }
    if (allSettled) {
      try {
        rmSync(cPath(gid), { force: true });
      } catch {
        // Another reconcile path removed it.
      }
    }
    syncDirBestEffort();

    if (postFence) throw postFence;

    // The caller's result is known in-memory regardless of whether its
    // outcome file landed; the durable handoff above guarantees every
    // OTHER requester adopts the identical result.
    const own = results.find((r) => r.token === ownToken);
    return own ? own.result : null;
  };

  // Prime the cache from the shared directory before createBus returns, so
  // stats() is correct immediately (single-process behaviour). This is a
  // mutating (but append-free) catch-up under the lock: it reconcles any
  // decision a crashed drain left behind, reaps dead sessions' queues,
  // heals torn/uncommitted tails and then adopts the merged view.
  try {
    withLockSync(() => catchupLocked({ mutating: true }), VIEW_WAIT_MS * 10);
  } catch {
    // Another writer holds the lock for long: cache stays empty and the
    // first operation catches up under the lock.
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

  // Run `fn(lease, view)` under the cross-process lock with a valid
  // credential. `view` is the merged directory state kept current by the
  // incremental tail reader (a full scan happens only on structural
  // change). Read-only callers (position lookups) never assert the
  // credential and never heal, so a fenced instance can still read.
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
      catchupLocked({ mutating: !readonly });
      return fn(lease, view);
    }, waitMs);

  // Bring the incremental cursors forward after a direct append the drain
  // did not perform (a replay/position line). A structural replacement
  // cannot happen while this instance holds the lock, so the inode is the
  // same; anything unexpected invalidates a cursor and forces a rescan.
  // The read cursor is advanced only when it was already exactly byteCount
  // behind EOF (these callers refresh it first); otherwise it is dropped and
  // the next read rebuilds it, never double-counting the marker line.
  const noteAppended = (byteCount) => {
    if (cursor) {
      try {
        const info = statSync(logPath);
        if (info.dev === cursor.dev && info.ino === cursor.ino && info.size >= cursor.off) {
          cursor.off = Math.max(cursor.off + byteCount, info.size);
        } else {
          cursor = null;
        }
      } catch {
        cursor = null;
      }
    }
    if (readCursor) {
      try {
        const info = statSync(logPath);
        const headNow = info.size > 0 ? readHeadSync(logPath) : '';
        if (
          info.dev === readCursor.dev &&
          info.ino === readCursor.ino &&
          headNow === readCursor.head &&
          info.size === readCursor.off + byteCount
        ) {
          readCursor.off = info.size;
        } else {
          readCursor = null;
        }
      } catch {
        readCursor = null;
      }
    }
  };

  /**
   * Run one publish ('single') or publishBatch ('batch') request through
   * the grouped commit pipeline:
   *   1. stage the validated, serialized items into the lock-free queue;
   *   2. take the mutex, catch up the merged view and drain ALL staged
   *      requests (this process's and every other live writer's) as one
   *      bracketed commit group;
   *   3. read back this token's outcome — which the drain itself or an
   *      earlier reconcile produced — and return acks or rethrow the exact
   *      rejection.
   * A request is staged durably before the lock, so even if this process
   *  is the one displaced waiting for the mutex, its queued group simply
   * fails as a whole at the epoch boundary; its bytes never mix in.
   */
  const commitThroughPipeline = (kind, items) => {
    const token = newToken();
    stageRequest(token, kind, items);
    let outcome = null;
    let lastErr = null;
    // A contended mutex is retried with the SAME token: staging is
    // idempotent under one token, so an attempt another drain settled and
    // an attempt we drain ourselves can never both publish. Bounded so a
    // wedged lock still rejects the call rather than hanging forever.
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        withOwnership((lease) => {
          outcome = drainLocked(lease, token);
        });
        lastErr = null;
        break;
      } catch (err) {
        const settled = readOutcome(token);
        if (settled !== null) {
          outcome = settled;
          lastErr = null;
          break;
        }
        if (!isLockError(err)) {
          // Fence/closed/disk failure: a failed flush already truncated
          // and settled every token (or this token never entered a group);
          // withdraw it so it cannot be drained afterwards.
          cleanupStaged(token, true);
          throw err;
        }
        lastErr = err;
      }
    }
    if (lastErr !== null) {
      // Mutex never became available. Decide atomically under the lock:
      // another drain is either already done (outcome present → adopt) or
      // has not gathered this token yet (withdraw it). Holding the mutex
      // makes those the only two cases, so the caller can never see an
      // error while its bytes later commit.
      try {
        withLockSync(() => {
          // Settle any decision a crashed drain left behind FIRST: if that
          // commit covered this token, reconcile writes its outcome and we
          // adopt rather than withdraw.
          reconcileLocked();
          const settled = readOutcome(token);
          if (settled !== null) {
            outcome = settled;
          } else {
            try {
              rmSync(qPath(token), { force: true });
            } catch {
              // Best effort; the next sweep reaps it via liveness/close.
            }
          }
        }, LOCK_WAIT_MS);
      } catch {
        // Even the finalizing lock failed: leave the staged request in
        // place (the caller got a contention error). Its heartbeat keeps
        // it alive for one more attempt; if this process gives up, the
        // orphan sweep reaps it once the session goes stale.
        throw lastErr;
      }
      if (outcome === null) throw lastErr;
    }
    if (outcome === null || outcome === undefined) {
      outcome = readOutcome(token);
    }
    if (outcome === null || outcome === undefined) {
      cleanupStaged(token, true);
      throw new Error('commit group was not drained');
    }
    // The token is settled: its queue/outcome are this token's private
    // leftovers (another process only ever read them), so remove both.
    cleanupStaged(token, true);
    if (outcome.ok) {
      return kind === 'batch' ? outcome.acks : outcome.ack;
    }
    if (outcome.ctor === 'RangeError') throw new RangeError(outcome.message);
    throw new Error(outcome.message || FENCE_MESSAGE);
  };

  // Remove a token's queue/outcome leftovers after its result is known.
  // With removeOutcome set (failure paths that never want this token
  // drained again) the outcome is removed too; otherwise it stays only
  // until the drain's own cleanup (it is idempotent and tiny).
  const cleanupStaged = (token, removeOutcome) => {
    try {
      rmSync(qPath(token), { force: true });
    } catch {
      // Best effort.
    }
    if (removeOutcome) {
      try {
        rmSync(rPath(token), { force: true });
      } catch {
        // Best effort.
      }
    }
  };


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

  // A failure to even take the mutex is contention, not a torn write: the
  // instance stays usable and the call may be retried. Only a failure after
  // the lock is held (an actual disk write) makes the bus `broken`.
  const isLockError = (err) =>
    err && typeof err.message === 'string' && err.message.startsWith('bus lock');

  // Persist (or confirm) a consumer position while holding the write lock.
  // The lock-free read view supplies the fast-path lookup, but the
  // authoritative no-reset / no-backward decision is made HERE under the
  // mutex against the freshly caught-up merged view, so two processes can
  // never reset or move a position backwards together.
  //   kind 'register': an existing position is returned untouched; only a
  //                    brand-new name is persisted at 0.
  //   kind 'advance' : a backwards move throws RangeError; an equal value
  //                    is a write-free no-op; a higher value is persisted.
  // Returns the resulting position.
  const positionLocked = (name, pos, kind) => {
    let resulting = pos;
    try {
      withOwnership(() => {
        const current = view.positions.get(name);
        if (current !== undefined) {
          if (kind === 'advance') {
            if (pos < current) {
              throw new RangeError('advance: position cannot move backwards');
            }
            if (pos === current) {
              // Setting the current value is a write-free no-op success.
              resulting = current;
              return;
            }
            // A strictly higher value falls through and is persisted.
          } else {
            // register of an existing name never resets it.
            resulting = current;
            return;
          }
        }
        const line = JSON.stringify({ t: 'p', name, pos, g: epoch }) + '\n';
        appendTo(logPath, Buffer.from(line, 'utf8'), fsync);
        // Reflect locally on the cached views without a full rescan.
        view.positions.set(name, pos);
        syncScalars();
        if (readSt) readSt.positions.set(name, pos);
        noteAppended(Buffer.byteLength(line, 'utf8'));
        resulting = pos;
      });
    } catch (err) {
      if (!fenced && !isLockError(err)) broken = true;
      throw err;
    }
    return resulting;
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

  // Refresh the lock-free read view (no mutex, no directory walk on the
  // append-only fast path). Every stats/usage/position/read call refreshes:
  // an unchanged log costs one stat, appends cost only the new tail bytes,
  // and a snapshot switch (compact/truncate) costs one non-destructive scan.
  // The view is always an immutable committed prefix, never a half group.
  // After close the last materialized view stands and is simply returned.
  const bestEffortView = () => {
    if (closed) return;
    refreshReadView();
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
      const recordJson = JSON.stringify(record);

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
        // The merged commit pipeline: staged lock-free, then this lock
        // holder drains every staged request (across processes) into one
        // bracketed group and returns this token's exact outcome. A key
        // another process committed between the cache check and the drain
        // is a reuse result from the merged plan, handled uniformly here.
        return commitThroughPipeline('single', [
          dedupKey === undefined
            ? { recordJson, size }
            : { dedupKey, recordJson, size },
        ]);
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
        // One staged request for the whole batch; the drain merges it with
        // every other staged request (singles and batches, this process
        // and others) into a single commit group and judges this batch's
        // occupancy as a whole against the merged plan.
        return commitThroughPipeline(
          'batch',
          prepared.map((item) =>
            item.dedupKey === undefined
              ? { recordJson: item.recordJson, size: item.size }
              : { dedupKey: item.dedupKey, recordJson: item.recordJson, size: item.size },
          ),
        );
      }).then(
        (acks) => {
          // Resolve racers that attached to this group's first-occurrence
          // keys with the acks the merged group actually assigned.
          const keyFirst = new Map();
          for (let i = 0; i < prepared.length; i++) {
            const key = prepared[i].dedupKey;
            if (key !== undefined && !keyFirst.has(key)) keyFirst.set(key, acks[i]);
          }
          for (const [key, reservation] of reservations) {
            groupPending.delete(key);
            reservation.resolve(keyFirst.get(key));
          }
          return acks;
        },
        (err) => {
          // Covers write failure inside the job and rejection before the
          // job body (closed/broken/fenced bus): release any reservation
          // still pointing at this failed group.
          for (const [key, reservation] of reservations) {
            if (groupPending.get(key) === reservation) groupPending.delete(key);
            reservation.reject(err);
          }
          throw err;
        },
      );
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
        return withOwnership(() => {
          // The returned page comes from the lock-free read view; refresh it
          // once (the write mutex is already held here, and this refresh
          // takes none) so the marker reflects exactly what was returned.
          // replay(from) is inclusive of from; the page takes an exclusive
          // lower bound. Replay does not drain staged publish requests: a
          // request still only staged lands after this marker.
          refreshReadView();
          const out = messagesAfter(readSt, from - 1);
          const n = out.length;
          if (n > 0) {
            const line = JSON.stringify({ t: 's', n, g: epoch }) + '\n';
            try {
              appendTo(logPath, Buffer.from(line, 'utf8'), fsync);
            } catch (err) {
              broken = true;
              throw err;
            }
            reassertLocked();
            view.replayed += n;
            syncScalars();
            if (readSt) readSt.replayed = view.replayed;
            noteAppended(Buffer.byteLength(line, 'utf8'));
          }
          return out;
        });
      });
    },

    register(name) {
      if (closed || broken) throw new Error('bus is closed');
      assertName(name);
      // The lookup is lock-free; the authoritative no-reset decision is
      // made inside the write lock, so it works even while writers are
      // draining or after a takeover (a fenced instance only fails when it
      // actually has to register a brand-new name).
      refreshReadView();
      const seen = readSt ? readSt.positions.get(name) : undefined;
      if (seen !== undefined) return seen;
      if (fenced) {
        throw new Error('bus write ownership was taken over by another process');
      }
      return positionLocked(name, 0, 'register');
    },

    advance(name, to) {
      assertOpen();
      assertName(name);
      if (typeof to !== 'number' || !Number.isInteger(to) || to < 0) {
        throw new RangeError('advance: position must be a non-negative integer');
      }
      // Fast path from the lock-free view; the no-backwards rule is
      // re-adjudicated under the write lock against the merged position, so
      // two processes advancing one consumer obey it together.
      refreshReadView();
      const seen = readSt ? readSt.positions.get(name) : undefined;
      if (seen !== undefined && to < seen) {
        throw new RangeError('advance: position cannot move backwards');
      }
      // An unknown name is auto-registered, same as register() first.
      return positionLocked(name, to, 'advance');
    },

    read(name) {
      if (closed || broken) {
        throw new Error('bus is closed');
      }
      assertName(name);
      // Entirely lock-free: even a fenced instance or one parked behind a
      // long drain reads. An unknown name is auto-registered (a write),
      // which a fenced instance may not do; the authoritative position
      // comes back from the write lock.
      refreshReadView();
      let pos = readSt ? readSt.positions.get(name) : undefined;
      if (pos === undefined) {
        pos = positionLocked(name, 0, 'register');
      }
      // read never moves the position; advance() is how consumption lands.
      return messagesAfter(readSt, pos);
    },

    // Synchronous, lock-free paged read. Inclusive of start, ascending, at
    // most `limit` records (fewer at the tail). A start inside a truncated
    // gap begins at the earliest surviving message; a wholly deleted range
    // returns []. It changes no seq, position or stat, is readable after
    // close like stats()/usage(), and is side-effect free on repeat calls.
    readRange(start, limit) {
      if (typeof start !== 'number' || !Number.isInteger(start) || start < 0) {
        throw new TypeError('readRange: start must be a non-negative integer');
      }
      if (typeof limit !== 'number' || !Number.isInteger(limit) || limit <= 0) {
        throw new TypeError('readRange: limit must be a positive integer');
      }
      if (!closed) refreshReadView();
      return readViewPage(start, limit);
    },

    compact() {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(() => {
        if (closed || broken) {
          throw new Error('bus is closed');
        }
        withOwnership(() => {
          // Credential validated at the lock door; the catch-up there
          // healed any interrupted earlier compact/truncate. Folding
          // needs every surviving payload, so take the fresh full scan
          // (the write hot-cache deliberately omits message bodies).
          const st = scanDirectory(dir);
          adoptFullScan(st);

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
          let settled = scanDirectory(dir);
          if (healLocked(settled)) settled = scanDirectory(dir);
          adoptFullScan(settled);
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
        withOwnership(() => {
          // Credential validated before the seal; the whole truncation is
          // one locked critical section. The catch-up reconciled and
          // healed everything before this point.
          let st = view;

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
            adoptFullScan(scanDirectory(dir));
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
          adoptFullScan(st);
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
        // next opener starts clean; the epoch high-water file stays, so
        // the epoch still never repeats. Any staging files this session
        // left behind (only possible if a job was interrupted) are
        // reclaimed here; a settled token keeps its outcome for exactly
        // one sweep, an unsettled one is removed wholesale.
        try {
          withLockSync(() => {
            rmSync(heartbeatPath, { force: true });
            for (const name of (() => {
              try {
                return readdirSync(dir);
              } catch {
                return [];
              }
            })()) {
              const qm = QUEUE_RE.exec(name);
              if (qm) {
                let req = null;
                try {
                  req = JSON.parse(readFileSync(path.join(dir, name), 'utf8'));
                } catch {
                  req = null;
                }
                if (!req || req.s === sessionId) {
                  rmSync(path.join(dir, name), { force: true });
                }
              }
            }
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
        // Freeze the read view at everything committed so far: readRange,
        // stats() and usage() keep serving this last materialized prefix
        // after close. Lock-free (the lock may just have been released),
        // and best-effort — a failed refresh leaves the previous committed
        // view, which is still valid.
        try {
          refreshReadView();
        } catch {
          // Last view stands.
        }
      });
    },
  };

  return bus;
}
