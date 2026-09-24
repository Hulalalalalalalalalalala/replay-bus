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
// Write ownership lives in a small lock directory. Each ownership term is a
// subdirectory `lease-<epoch>` created with an exclusive mkdir, so exactly one
// process can ever hold a given epoch — no rename-based compare-and-swap is
// involved and no contender can overwrite a live owner.
const LOCK_DIR_NAME = '.bus.lock';
const LEASE_DIR_RE = /^lease-(\d+)$/;
const leaseDirName = (epoch) => `lease-${String(epoch).padStart(10, '0')}`;
const OWNER_NAME = 'owner.json';
const OWNER_TMP_NAME = 'owner.tmp';
const RELEASED_NAME = 'released';
// Consumer positions ('p') and replay counters ('s') are shared by every
// opener, including processes that never hold the write lease. They live in
// their own append-only sidecar, which (unlike the active log) is never
// renamed, replaced or deleted by compact/truncate, so a non-owner's line
// can never be swallowed by an owner's log swap.
const META_NAME = 'bus.meta.jsonl';
// The log is a chain of segment files: the active `bus.jsonl` plus finalized
// `bus.<index>.jsonl` segments. Index order is oldest first; an index is
// never reused within the lifetime of a directory.
const SEGMENT_RE = /^bus\.(\d+)\.jsonl$/;
const segmentName = (index) => `bus.${String(index).padStart(10, '0')}.jsonl`;

// Cross-process write ownership rides on a single lease file. A writer holds
// the lease for LEASE_MS and keeps renewing it while it has work; when its
// serial chain drains it releases the lease early, so a contender normally
// takes over immediately instead of waiting for the term to time out.
//
// The timings are overridable through the environment exclusively for tests
// (short terms make takeover/expiry boundaries deterministic and fast).
const positiveIntEnv = (name, fallback) => {
  const v = Number(process.env[name]);
  return Number.isInteger(v) && v > 0 ? v : fallback;
};
const LEASE_MS = positiveIntEnv('REPLAY_BUS_LEASE_MS', 4000);
// Renew with this much headroom so ordinary scheduler/IO latency cannot make
// a healthy owner look expired.
const RENEW_AT_MS = Math.min(positiveIntEnv('REPLAY_BUS_RENEW_MS', 2000), LEASE_MS);
// Conservative bound on how far two wall clocks may disagree. A foreign
// lease is treated as expired only past its deadline plus this grace, so
// clock skew can never let an expired token pass.
const CLOCK_SKEW_MS = positiveIntEnv('REPLAY_BUS_SKEW_MS', 1000);
// A contender polls at this cadence while another owner's lease is still
// within its (skew-adjusted) term.
const RETRY_MS = positiveIntEnv('REPLAY_BUS_RETRY_MS', 50);

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

// Parse every complete, committed line of a log buffer. A batch is bracketed
// by {t:'b',id} .. entries .. {t:'bk',id}; entries of a group whose commit
// never landed are skipped, and a torn/unparseable final line ends the scan.
// Reads use this so a half-written group (ours or a predecessor's) is never
// visible.
function parseCommitted(raw) {
  const entries = [];
  let start = 0;
  let group = null;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] !== 0x0a) continue;
    let entry = null;
    try {
      entry = i > start ? JSON.parse(raw.toString('utf8', start, i)) : null;
    } catch {
      return entries;
    }
    if (group !== null) {
      if (entry && entry.t === 'bk' && entry.id === group.id) {
        entries.push(...group.entries);
        group = null;
      } else if (entry && entry.t === 'b') {
        // The previous group never committed; track the new one instead.
        group = { id: entry.id, entries: [] };
      } else if (entry) {
        group.entries.push(entry);
      }
    } else if (entry && entry.t === 'b') {
      group = { id: entry.id, entries: [] };
    } else if (entry) {
      entries.push(entry);
    }
    start = i + 1;
  }
  return entries;
}

// Parse the complete lines of an append-only meta sidecar buffer. Every line
// is written with a leading newline, so a torn trailing write left by a
// crashed process is terminated by the next writer's leading newline and can
// never glue itself onto a good line; garbled lines are skipped in isolation.
function parseMeta(raw) {
  const entries = [];
  let start = 0;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] !== 0x0a) continue;
    if (i > start) {
      try {
        entries.push(JSON.parse(raw.toString('utf8', start, i)));
      } catch {
        // Torn/garbled line: isolated by framing newlines; skip it alone.
      }
    }
    start = i + 1;
  }
  return entries;
}

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
  const lockDirPath = path.join(dir, LOCK_DIR_NAME);
  const metaPath = path.join(dir, META_NAME);

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
  // Business bytes of the messages still retained (seq > horizon, i.e.
  // snapshot survivors plus live-log messages). Unlike the cumulative
  // `bytes` stat this shrinks when retention truncation frees messages;
  // compaction only reshapes the log and never changes it.
  let usageBytes = 0;

  // ---- cross-process lease state -------------------------------------------
  // Ownership is taken lazily, only when a mutating call first needs it, so a
  // read-only opener never contends. `ownerId` is this instance's identity
  // for its whole lifetime; `epoch` is bumped on every hostile takeover so a
  // fenced owner recognises its old token even when wall clocks disagree.
  const ownerId = randomUUID();
  let leaseEpoch = 0;
  let leaseAcquiredAt = 0;
  let leaseExpiresAt = 0;
  let heartbeat = null;
  // Latched the moment another owner fences this instance. From then on this
  // process is an ex-owner: every later write-path call rejects, even if the
  // directory later goes idle and the lease could technically be re-won. Its
  // reads keep working.
  let fenced = false;

  const statOrNull = (p) => {
    try {
      return statSync(p);
    } catch {
      return null;
    }
  };

  // ---- synchronous recovery so stats() is correct the moment createBus returns
  mkdirSync(dir, { recursive: true });

  // Leftover tmp files are operations that crashed mid-rename; the durable
  // state they targeted is whatever currently sits under the final name.
  rmSync(snapshotTmpPath, { force: true });
  rmSync(logTmpPath, { force: true });
  // The lock directory is never created until the first write, and never
  // cleaned up here: epoch ownership inside it is the arbitration record.

  // Sidecar byte offset already folded into the snapshot's baseline.
  let metaFromSnapshot = 0;
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
    // Sidecar bytes already folded into this snapshot's counters/positions.
    metaFromSnapshot = typeof snap.po === 'number' ? snap.po : 0;
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
      // Pre-sidecar layouts kept replay counters in the log.
      replayed += entry.n;
    } else if (entry.t === 'p') {
      // Pre-sidecar layouts kept positions in the log.
      positions.set(entry.name, entry.pos);
    }
  };

  // A truncate marker is an absolute checkpoint: the cumulative counters,
  // positions and dedup table it carries replace whatever earlier segments
  // established, so recovery can start from the newest marker and ignore
  // everything before it.
  let metaFromMarker = -1;
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
    if (typeof entry.po === 'number') metaFromMarker = entry.po;
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

  // Fold the shared sidecar tail not yet covered by the snapshot or by the
  // newest truncate marker. Markers/snapshots record the sidecar size they
  // already merged, so a line is counted exactly once across restarts and
  // takeovers, even though the sidecar itself is never deleted.
  let metaSize = statOrNull(metaPath)?.size ?? 0;
  {
    const from = metaFromMarker >= 0 ? metaFromMarker : metaFromSnapshot;
    applyMetaTail(from);
  }

  // Recompute retained bytes from the surviving messages after recovery:
  // snapshot survivors plus messages in finalized segments and the active
  // log above both the horizon and the folded floor. Torn tails and
  // uncommitted batches were severed above, so their bytes never count —
  // usage always matches the messages actually alive.
  for (const m of baseMessages) {
    if (m.seq > horizon) usageBytes += Buffer.byteLength(JSON.stringify(m.record), 'utf8');
  }
  {
    const logFloor = Math.max(horizon, foldedSeq);
    const sumFile = (filePath) => {
      if (!existsSync(filePath)) return;
      const { entries } = parseLog(readFileSync(filePath));
      for (const entry of entries) {
        if (entry.t === 'm' && entry.seq > logFloor) usageBytes += entry.bytes;
      }
    };
    for (const seg of listSegments()) sumFile(seg.path);
    sumFile(logPath);
  }

  // Append handles. The active log is opened O_APPEND so each writev is one
  // whole line at EOF; the sidecar is never renamed, so its handle is stable
  // for the lifetime of the instance. Either open may fail (a read-only bus
  // directory): the instance still serves read paths, and the handle is
  // (re)opened lazily when a write path actually runs — by which point write
  // ownership, which requires creating files in the directory, has either
  // succeeded (so the open works) or the call has already rejected.
  let fd = -1;
  let metaFd = -1;
  try {
    fd = openSync(logPath, 'a');
  } catch {
    fd = -1;
  }
  try {
    metaFd = openSync(metaPath, 'a');
  } catch {
    metaFd = -1;
  }
  let logIno = statOrNull(logPath)?.ino ?? 0;
  let logSize = statOrNull(logPath)?.size ?? 0;
  let snapMtimeMs = statOrNull(snapshotPath)?.mtimeMs ?? 0;

  const closeFdQuiet = () => {
    if (fd >= 0) {
      try {
        closeSync(fd);
      } catch {
        // already closed
      }
      fd = -1;
    }
  };
  const closeMetaFdQuiet = () => {
    if (metaFd >= 0) {
      try {
        closeSync(metaFd);
      } catch {
        // already closed
      }
      metaFd = -1;
    }
  };
  // Open the active log for append, reopening after a compact/truncate swap
  // or a lazily-deferred initial open. Throws if the directory is unwritable.
  const openLogFd = () => {
    if (fd < 0) fd = openSync(logPath, 'a');
    return fd;
  };

  // Merge committed sidecar lines starting at byte offset `from`. Positions
  // advance monotonically (the same "never backwards" rule advance() enforces
  // in-process), and replay counts add.
  function applyMetaTail(from) {
    if (!existsSync(metaPath)) return;
    const total = statOrNull(metaPath)?.size ?? 0;
    if (total <= from) {
      metaSize = total;
      return;
    }
    const fh = openSync(metaPath, 'r');
    let raw;
    try {
      raw = Buffer.allocUnsafe(total - from);
      let off = 0;
      while (off < raw.length) {
        const n = readSync(fh, raw, off, raw.length - off, from + off);
        if (!n) break;
        off += n;
      }
      raw = raw.subarray(0, off);
    } finally {
      closeSync(fh);
    }
    for (const entry of parseMeta(raw)) {
      if (entry.t === 'p') {
        const prev = positions.get(entry.name);
        if (prev === undefined || entry.pos > prev) positions.set(entry.name, entry.pos);
      } else if (entry.t === 's') {
        replayed += entry.n;
      }
    }
    metaSize = total;
  }

  let chain = Promise.resolve();
  let closed = false;
  // A failed append may leave a torn line; stop appending so later writes
  // cannot glue themselves onto it. The torn tail is truncated on reopen
  // (and, across processes, on lease takeover).
  let broken = false;

  // All disk mutation by publish/compact/truncate happens inside this serial
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

  // ---- lease handling ------------------------------------------------------

  // ---- lease handling ------------------------------------------------------
  //
  // Ownership terms are subdirectories `.bus.lock/lease-<epoch>` created with
  // an exclusive mkdir, so a term has exactly one owner for its whole life.
  // Inside each: an atomic owner.json ({owner,epoch,expiresAt}) and, on clean
  // release, a `released` marker. Heartbeats only ever rewrite the owner.json
  // inside the term's own directory; takeover creates the *next* epoch
  // directory once the current term is released or past its deadline (plus
  // the clock-skew grace). There is no compare-and-swap rename to lose.

  const fsyncDirQuiet = (p) => {
    try {
      const dfd = openSync(p, 'r');
      try {
        fsyncSync(dfd);
      } finally {
        closeSync(dfd);
      }
    } catch {
      // Directory sync unavailable: the namespace change already landed.
    }
  };

  const ensureLockDir = () => {
    try {
      mkdirSync(lockDirPath, { recursive: false });
    } catch (err) {
      if (err && err.code === 'EEXIST') return;
      throw err;
    }
  };

  const listLeaseDirs = () => {
    let names;
    try {
      names = readdirSync(lockDirPath);
    } catch (err) {
      if (err && (err.code === 'ENOENT' || err.code === 'ENOTDIR')) return [];
      throw err;
    }
    const out = [];
    for (const name of names) {
      const m = LEASE_DIR_RE.exec(name);
      if (m) out.push({ epoch: Number(m[1]), path: path.join(lockDirPath, name) });
    }
    out.sort((a, b) => a.epoch - b.epoch);
    return out;
  };

  const inspectLease = (entry) => {
    let info = null;
    try {
      info = JSON.parse(readFileSync(path.join(entry.path, OWNER_NAME), 'utf8'));
    } catch {
      info = null;
    }
    const released = existsSync(path.join(entry.path, RELEASED_NAME));
    // A term whose owner record never landed (crashed between mkdir and the
    // atomic rename) is treated as released: it owns nothing durable.
    return {
      epoch: entry.epoch,
      owner: info?.owner ?? null,
      expiresAt: info?.expiresAt ?? 0,
      released: released || !info,
    };
  };

  // Highest epoch term on disk, or null when the lock directory is empty.
  const currentLease = () => {
    const dirs = listLeaseDirs();
    if (dirs.length === 0) return null;
    return inspectLease(dirs[dirs.length - 1]);
  };

  // Highest epoch number on disk (0 when empty). Fencing is decided by this
  // alone: epoch directories are exclusively created, so a higher one while
  // we hold ours can only be a handover, and reading owner.json is not needed
  // on the hot path.
  const highestEpoch = () => {
    const dirs = listLeaseDirs();
    return dirs.length ? dirs[dirs.length - 1].epoch : 0;
  };

  // Publish owner.json inside an already exclusively-owned term directory.
  const publishOwner = (termDir, owner) => {
    const tmp = path.join(termDir, OWNER_TMP_NAME);
    const final = path.join(termDir, OWNER_NAME);
    const lf = openSync(tmp, 'w');
    try {
      writeSync(lf, Buffer.from(JSON.stringify(owner), 'utf8'));
      fsyncSync(lf);
    } finally {
      closeSync(lf);
    }
    renameSync(tmp, final);
    fsyncDirQuiet(lockDirPath);
  };

  // Create the next term. Returns the owner record on winning the exclusive
  // mkdir, or null when another process won the same epoch.
  const createLease = (now) => {
    ensureLockDir();
    const dirs = listLeaseDirs();
    const highest = dirs.length ? dirs[dirs.length - 1].epoch : 0;
    // Terms are an append-only arbitration record, but only the newest is
    // consulted; drop older ones so a long-lived directory cannot grow
    // without bound. Never touches the newest term, so a concurrent owner or
    // a racing contender is unaffected.
    for (let i = 0; i + 1 < dirs.length; i++) {
      try {
        rmSync(dirs[i].path, { recursive: true, force: true });
      } catch {
        // Pruning is best effort.
      }
    }
    const epoch = highest + 1;
    const termDir = path.join(lockDirPath, leaseDirName(epoch));
    try {
      mkdirSync(termDir, { recursive: false });
    } catch (err) {
      if (err && err.code === 'EEXIST') return null;
      throw err;
    }
    const owner = { owner: ownerId, epoch, acquiredAt: now, expiresAt: now + LEASE_MS };
    publishOwner(termDir, owner);
    return owner;
  };

  // Push our current term's deadline forward. Touches only our own epoch
  // directory, so it can never interfere with another owner.
  const renewLease = (now) => {
    const termDir = path.join(lockDirPath, leaseDirName(leaseEpoch));
    const owner = {
      owner: ownerId,
      epoch: leaseEpoch,
      acquiredAt: leaseAcquiredAt,
      expiresAt: now + LEASE_MS,
    };
    publishOwner(termDir, owner);
    leaseExpiresAt = owner.expiresAt;
  };

  const markReleased = () => {
    try {
      const marker = path.join(lockDirPath, leaseDirName(leaseEpoch), RELEASED_NAME);
      writeFileSync(marker, '');
      fsyncDirQuiet(lockDirPath);
    } catch {
      // Best effort: without the marker the term simply times out.
    }
  };

  const stopHeartbeat = () => {
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
  };

  const startHeartbeat = () => {
    if (heartbeat || closed) return;
    heartbeat = setInterval(() => {
      if (closed) return;
      const termDir = path.join(lockDirPath, leaseDirName(leaseEpoch));
      if (
        leaseEpoch === 0 ||
        !existsSync(path.join(termDir, OWNER_NAME)) ||
        existsSync(path.join(termDir, RELEASED_NAME)) ||
        highestEpoch() > leaseEpoch
      ) {
        stopHeartbeat();
        return;
      }
      const now = Date.now();
      if (now < leaseExpiresAt - (LEASE_MS - RENEW_AT_MS)) return;
      try {
        renewLease(now);
      } catch {
        // A failed heartbeat (e.g. ENOSPC) simply lets the term run out; the
        // next mutating call re-validates and takes over or refuses.
      }
    }, Math.min(500, RENEW_AT_MS / 2));
    // Never keep the event loop alive just for the heartbeat.
    if (typeof heartbeat.unref === 'function') heartbeat.unref();
  };

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  // Rebuild all merged state from disk. With `sever` true (only the lease
  // owner runs this) a predecessor's torn tail / dangling batch is physically
  // truncated exactly like open-time recovery; with `sever` false (a read-
  // only opener catching up) nothing on disk is modified.
  const rebuildState = ({ sever } = {}) => {
    const bag = {
      seq: 0,
      bytes: 0,
      published: 0,
      replayed: 0,
      horizon: 0,
      positions: new Map(),
      dedup: new Map(),
    };

    let nGen = 0;
    let nFoldedSeq = 0;
    let nBase = [];
    let nPo = 0;
    let nNextSegment = 1;

    if (existsSync(snapshotPath)) {
      const snap = JSON.parse(readFileSync(snapshotPath, 'utf8'));
      nGen = snap.gen;
      bag.seq = snap.seq;
      bag.bytes = snap.bytes;
      bag.published = snap.published;
      bag.replayed = snap.replayed;
      bag.horizon = snap.horizon ?? 0;
      nFoldedSeq = snap.seq;
      if (typeof snap.po === 'number') nPo = snap.po;
      for (const [key, value] of snap.dedup) bag.dedup.set(key, value);
      for (const [name, pos] of Object.entries(snap.positions)) bag.positions.set(name, pos);
      nBase = snap.messages;
    }

    const segs = listSegments();
    for (const seg of segs) {
      if (seg.index >= nNextSegment) nNextSegment = seg.index + 1;
    }
    const files = segs.map((seg) => seg.path);
    if (existsSync(logPath)) files.push(logPath);

    let anchor = -1;
    for (let i = 0; i < files.length; i++) {
      const first = firstEntry(files[i]);
      if (first && (first.t === 'c' || first.t === 't') && first.gen === nGen) {
        anchor = i;
      }
    }

    let markerPo = -1;
    const onMarker = (entry) => {
      bag.seq = entry.seq;
      bag.bytes = entry.bytes;
      bag.published = entry.published;
      bag.replayed = entry.replayed;
      bag.horizon = entry.horizon;
      bag.positions.clear();
      for (const [name, pos] of Object.entries(entry.positions)) bag.positions.set(name, pos);
      bag.dedup.clear();
      for (const [key, value] of entry.dedup) bag.dedup.set(key, value);
      if (typeof entry.po === 'number') markerPo = entry.po;
    };
    const onEntry = (entry) => {
      if (entry.t === 'm') {
        if (entry.seq <= bag.horizon) return;
        bag.seq = entry.seq;
        bag.bytes += entry.bytes;
        bag.published += 1;
        if (typeof entry.d === 'string') {
          bag.dedup.set(entry.d, { id: entry.id, seq: entry.seq });
        }
      } else if (entry.t === 's') {
        bag.replayed += entry.n;
      } else if (entry.t === 'p') {
        bag.positions.set(entry.name, entry.pos);
      }
    };

    if (nGen > 0 && anchor < 0) {
      if (sever) {
        // Snapshot landed without its marker-led log: a compact crashed in
        // that window (or the previous owner died in it). Regenerate it.
        for (const seg of segs) rmSync(seg.path, { force: true });
        writeFileSync(logPath, markerLine(nGen));
        files.length = 0;
        files.push(logPath);
        anchor = 0;
      }
      // A non-owning opener simply serves the snapshot; the owner repairs.
    } else {
      const walk = (raw, filePath) => {
        if (sever) {
          // Strict walk: sever a torn tail / dangling batch of the active log.
          let lineStart = 0;
          let committedEnd = 0;
          let group = null;
          for (let i = 0; i < raw.length; i++) {
            if (raw[i] !== 0x0a) continue;
            const entry = i > lineStart ? JSON.parse(raw.toString('utf8', lineStart, i)) : null;
            if (group !== null) {
              if (entry && entry.t === 'bk' && entry.id === group.id) {
                for (const e of group.entries) onEntry(e);
                group = null;
                committedEnd = i + 1;
              } else if (entry && entry.t === 'b') {
                group = { id: entry.id, entries: [] };
              } else if (entry) {
                group.entries.push(entry);
              }
            } else if (entry && entry.t === 'b') {
              group = { id: entry.id, entries: [] };
            } else {
              if (entry) {
                if (entry.t === 't') onMarker(entry);
                else onEntry(entry);
              }
              committedEnd = i + 1;
            }
            lineStart = i + 1;
          }
          if (committedEnd < raw.length && filePath === logPath) {
            truncateSync(logPath, committedEnd);
          }
        } else {
          for (const e of parseCommitted(raw)) {
            if (e.t === 't') onMarker(e);
            else onEntry(e);
          }
        }
      };
      for (let i = Math.max(anchor, 0); i < files.length; i++) {
        let raw;
        try {
          raw = readFileSync(files[i]);
        } catch {
          continue;
        }
        walk(raw, files[i]);
      }
    }

    // Sidecar tail beyond the newest checkpoint's folded offset.
    const metaFrom = markerPo >= 0 ? markerPo : nPo;
    if (existsSync(metaPath)) {
      const total = statSync(metaPath).size;
      if (total > metaFrom) {
        const fh = openSync(metaPath, 'r');
        try {
          const raw = Buffer.allocUnsafe(total - metaFrom);
          let off = 0;
          while (off < raw.length) {
            const n = readSync(fh, raw, off, raw.length - off, metaFrom + off);
            if (!n) break;
            off += n;
          }
          for (const e of parseMeta(raw.subarray(0, off))) {
            if (e.t === 'p') {
              const prev = bag.positions.get(e.name);
              if (prev === undefined || e.pos > prev) bag.positions.set(e.name, e.pos);
            } else if (e.t === 's') {
              bag.replayed += e.n;
            }
          }
        } finally {
          closeSync(fh);
        }
      }
    }

    // Retained bytes of the surviving set.
    let nUsage = 0;
    for (const m of nBase) {
      if (m.seq > bag.horizon) nUsage += Buffer.byteLength(JSON.stringify(m.record), 'utf8');
    }
    const logFloor = Math.max(bag.horizon, nFoldedSeq);
    const sumFile = (filePath) => {
      if (!existsSync(filePath)) return;
      for (const entry of parseCommitted(readFileSync(filePath))) {
        if (entry.t === 'm' && entry.seq > logFloor) nUsage += entry.bytes;
      }
    };
    for (const seg of listSegments()) sumFile(seg.path);
    sumFile(logPath);

    return {
      seq: bag.seq,
      bytes: bag.bytes,
      published: bag.published,
      replayed: bag.replayed,
      horizon: bag.horizon,
      foldedSeq: nFoldedSeq,
      gen: nGen,
      base: nBase,
      dedup: bag.dedup,
      positions: bag.positions,
      usage: nUsage,
      nextSegmentIndex: nNextSegment,
      metaSize: statOrNull(metaPath)?.size ?? 0,
    };
  };

  const adoptState = (s) => {
    seq = s.seq;
    bytes = s.bytes;
    published = s.published;
    replayed = s.replayed;
    horizon = s.horizon;
    foldedSeq = s.foldedSeq;
    snapshotGen = s.gen;
    baseMessages = s.base;
    dedup.clear();
    for (const [k, v] of s.dedup) dedup.set(k, v);
    positions.clear();
    for (const [k, v] of s.positions) positions.set(k, v);
    usageBytes = s.usage;
    nextSegmentIndex = s.nextSegmentIndex;
    metaSize = s.metaSize;
  };

  const refreshFileCache = () => {
    closeFdQuiet();
    try {
      fd = openSync(logPath, 'a');
    } catch {
      // Unwritable right now (a read-only opener catching up). Read paths
      // keep working; a write path reopens lazily after it holds the lease.
      fd = -1;
    }
    const st = statOrNull(logPath);
    logIno = st?.ino ?? 0;
    logSize = st?.size ?? 0;
    snapMtimeMs = statOrNull(snapshotPath)?.mtimeMs ?? 0;
  };

  // True when nothing structural could have changed since our cache was
  // built: the active log is the same inode and length, the snapshot is
  // untouched, and the sidecar did not grow.
  const diskUnchangedSinceCache = () => {
    const lst = statOrNull(logPath);
    const sst = statOrNull(snapshotPath);
    const mst = statOrNull(metaPath);
    if (!lst) return false;
    if (lst.ino !== logIno || lst.size !== logSize) return false;
    if ((sst?.mtimeMs ?? 0) !== snapMtimeMs) return false;
    if ((mst?.size ?? 0) !== metaSize) return false;
    return true;
  };

  // Become the writer. Resolves once this instance holds a current term and
  // its in-memory state covers everything durable on disk; rejects with an
  // Error when this instance has been fenced, a live owner keeps its term
  // past the bounded contention window, the directory is missing, or access
  // is denied. A disk-full here surfaces as an Error — never a RangeError
  // (quota failures originate at the call sites instead).
  const acquire = async () => {
    if (closed || broken) throw new Error('bus is closed');
    if (fenced) throw new Error('bus write ownership was taken by another process');
    // Bounded contention: a live owner keeps renewing, so waiting forever
    // cannot help. The window opens when an unexpired foreign term is first
    // seen and spans a full term plus the skew grace; a predecessor that
    // actually stopped heartbeating expires inside it, while a continuously
    // renewing owner outlasts it and the call fails.
    let contentionDeadline = 0;
    for (;;) {
      if (closed) throw new Error('bus is closed');
      if (fenced) throw new Error('bus write ownership was taken by another process');
      const now = Date.now();

      if (leaseEpoch > 0) {
        // Fast path for the current owner: fencing is just "does a higher
        // exclusive epoch exist", which needs a single readdir.
        const high = highestEpoch();
        if (high > leaseEpoch) {
          // A different owner opened the next term while ours is live.
          fenced = true;
          stopHeartbeat();
          throw new Error('bus write ownership was taken by another process');
        }
        if (high === leaseEpoch) {
          if (now <= leaseExpiresAt) {
            // Another process may have advanced a position or replayed while
            // we held the token; fold those shared lines before deciding.
            mergeForeignMeta();
            startHeartbeat();
            return;
          }
          // Past the deadline but still the highest epoch: renew in place.
          try {
            renewLease(now);
          } catch (err) {
            throw err instanceof Error ? err : new Error(String(err));
          }
          if (highestEpoch() > leaseEpoch) {
            fenced = true;
            stopHeartbeat();
            throw new Error('bus write ownership was taken by another process');
          }
          mergeForeignMeta();
          startHeartbeat();
          return;
        }
        // Our term directory vanished (external deletion): treat as lost.
        stopHeartbeat();
        throw new Error('bus write ownership was taken by another process');
      }

      const cur = currentLease();
      const available =
        !cur || cur.released || now > cur.expiresAt + CLOCK_SKEW_MS;
      if (!available) {
        if (contentionDeadline === 0) {
          contentionDeadline = now + LEASE_MS + CLOCK_SKEW_MS + 50;
        } else if (now >= contentionDeadline) {
          throw new Error('bus is locked by another live process');
        }
        // Owners release early when their write queue drains, so poll at a
        // short cadence; the deadline bounds the total wait.
        await sleep(RETRY_MS);
        continue;
      }

      const prevOwner = cur ? cur.owner : null;
      let owner;
      try {
        owner = createLease(now);
      } catch (err) {
        throw err instanceof Error ? err : new Error(String(err));
      }
      if (!owner) {
        // Lost the exclusive mkdir for this epoch to a live contender;
        // re-read (its newer epoch becomes the current term).
        continue;
      }
      leaseEpoch = owner.epoch;
      leaseAcquiredAt = owner.acquiredAt;
      leaseExpiresAt = owner.expiresAt;
      contentionDeadline = 0;
      // We hold the token. A real predecessor may have left messages, a
      // half-written batch, positions or counters behind. Reacquiring after
      // our own clean release only needs a non-destructive reload if the
      // bytes on disk actually changed.
      try {
        if (prevOwner && prevOwner !== ownerId) {
          // Running inside a serial-chain job: reload synchronously inline
          // (enqueueing here would queue behind ourselves and deadlock).
          const s = rebuildState({ sever: true });
          adoptState(s);
          refreshFileCache();
          broken = false;
        } else if (!diskUnchangedSinceCache()) {
          mergeForeignMeta();
          const s = rebuildState({ sever: false });
          adoptState(s);
          refreshFileCache();
        } else {
          mergeForeignMeta();
        }
      } catch (err) {
        broken = true;
        stopHeartbeat();
        throw err instanceof Error ? err : new Error(String(err));
      }
      startHeartbeat();
      return;
    }
  };

  // Token gate run immediately before every durable mutation. Renews a term
  // close to its deadline; throws an ownership Error if a newer term now
  // exists or ours was released, so an expired or fenced token never writes.
  const ensureLease = () => {
    if (closed || broken) throw new Error('bus is closed');
    if (fenced) throw new Error('bus write ownership was taken by another process');
    const myTermDir = path.join(lockDirPath, leaseDirName(leaseEpoch));
    const lostOwnership =
      leaseEpoch === 0 ||
      !existsSync(path.join(myTermDir, OWNER_NAME)) ||
      existsSync(path.join(myTermDir, RELEASED_NAME)) ||
      highestEpoch() > leaseEpoch;
    if (lostOwnership) {
      if (leaseEpoch > 0 && highestEpoch() > leaseEpoch) fenced = true;
      stopHeartbeat();
      throw new Error('bus write ownership was taken by another process');
    }
    const now = Date.now();
    if (now >= leaseExpiresAt - (LEASE_MS - RENEW_AT_MS) || now > leaseExpiresAt) {
      try {
        renewLease(now);
      } catch (err) {
        throw err instanceof Error ? err : new Error(String(err));
      }
      // A contender may have opened the next epoch while we rewrote our own
      // owner.json; the higher exclusive epoch fences us.
      if (highestEpoch() > leaseEpoch) {
        fenced = true;
        stopHeartbeat();
        throw new Error('bus write ownership was taken by another process');
      }
    }
  };

  // A held lease stays held while the process is alive: the heartbeat keeps
  // renewing it, and ownership changes hands only on an explicit close()
  // (which marks the term released) or when a predecessor stops renewing and
  // its term expires. So there is deliberately no idle release; mutating jobs
  // run unwrapped.
  const trackLeaseJob = (job) => job;

  // ---- cross-process state merging for read paths ---------------------------

  // Fold just the shared sidecar's new tail (positions/replay counts).
  const mergeForeignMeta = () => {
    const total = statOrNull(metaPath)?.size ?? 0;
    if (total > metaSize) applyMetaTail(metaSize);
    else metaSize = total;
  };

  // Cheap coherence check before any read or stats call. The sidecar tail is
  // always merged; any structural change to the active log or snapshot
  // triggers a full (non-destructive) rebuild.
  const catchUp = () => {
    if (closed) return;
    try {
      mergeForeignMeta();
      const lst = statOrNull(logPath);
      const sst = statOrNull(snapshotPath);
      const sameInode = !!lst && lst.ino === logIno;
      const snapshotChanged = (sst?.mtimeMs ?? 0) !== snapMtimeMs;
      if (snapshotChanged || !sameInode) {
        const s = rebuildState({ sever: false });
        adoptState(s);
        refreshFileCache();
        return;
      }
      if (lst.size !== logSize) {
        // Same file, new bytes: another owner appended messages (positions
        // and replay counts ride the sidecar). Rebuild from committed state.
        const s = rebuildState({ sever: false });
        adoptState(s);
        refreshFileCache();
      }
    } catch {
      // A transient stat/read failure (e.g. a swap in progress) must not
      // break a read: serve the last consistent in-memory state.
    }
  };

  const writeLine = (entry) => {
    writeSync(openLogFd(), Buffer.from(JSON.stringify(entry) + '\n', 'utf8'));
    if (fsync) fsyncSync(fd);
  };

  // Append one shared-state line. Goes to the stable sidecar as one O_APPEND
  // writev, framed by a leading newline: if a previous process died mid-line
  // the leading newline terminates that torn prefix before our JSON starts,
  // so concurrent processes can never glue two records into one line. A
  // damaged or not-yet-opened handle is (re)opened once; returns whether the
  // line landed (false on a read-only / removed directory).
  const writeMetaLine = (entry) => {
    const json = JSON.stringify(entry);
    const buf = Buffer.from('\n' + json + '\n', 'utf8');
    const attempt = () => {
      if (metaFd < 0) metaFd = openSync(metaPath, 'a');
      writeSync(metaFd, buf);
      if (fsync) fsyncSync(metaFd);
    };
    try {
      attempt();
    } catch {
      closeMetaFdQuiet();
      try {
        attempt();
      } catch {
        return false;
      }
    }
    metaSize = statOrNull(metaPath)?.size ?? metaSize;
    return true;
  };

  // Position writes are synchronous (register/advance/read are synchronous
  // methods) and never need write ownership: positions are shared state every
  // process may advance, and the sidecar is never involved in log swaps.
  const persistPosition = (name, pos) => {
    if (!writeMetaLine({ t: 'p', name, pos })) {
      throw new Error('could not persist consumer position');
    }
  };

  // read() auto-registers an unknown name; that bookkeeping is part of the
  // read path and must never turn a read into a failure.
  const persistPositionQuiet = (name, pos) => {
    writeMetaLine({ t: 'p', name, pos });
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
  // Only committed content is visible, so a half-written batch belonging to
  // the current writer or a predecessor never appears.
  const messagesAfter = (pos) => {
    catchUp();
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
      let raw;
      try {
        raw = readFileSync(filePath);
      } catch {
        return;
      }
      for (const entry of parseCommitted(raw)) {
        if (entry.t === 'm' && entry.seq > logFloor) {
          out.push({ seq: entry.seq, id: entry.id, record: entry.record });
        }
      }
    };
    let segs = [];
    try {
      segs = listSegments();
    } catch {
      segs = [];
    }
    for (const seg of segs) collect(seg.path);
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

      // A key effective anywhere (history, a recovered log, a merged sidecar
      // peer, or a predecessor this process will take over) rides the first
      // acknowledgement and allocates no seq or quota.
      if (dedupKey !== undefined) {
        catchUp();
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

      enqueue(
        trackLeaseJob(async () => {
          if (closed || broken) {
            throw new Error('bus is closed');
          }
          await acquire();
          ensureLease();
          // Ownership may have brought a predecessor's committed history:
          // recheck after the takeover reload, before consuming a seq.
          if (dedupKey !== undefined) {
            const hist = dedup.get(dedupKey);
            if (hist) {
              pending.delete(dedupKey);
              return { id: hist.id, seq: hist.seq };
            }
          }
          // Quota is checked inside the serial job, so a truncate queued
          // earlier releases its bytes before this publish is judged. An
          // exact fit succeeds; one byte over (or a single oversized record)
          // rejects before anything is written — state stays untouched.
          if (quota !== undefined && usageBytes + size > quota) {
            throw new RangeError('publish: retained bytes would exceed maxBytes');
          }
          const mySeq = seq + 1;
          const id = randomUUID();
          const entry = { t: 'm', seq: mySeq, id, bytes: size, record };
          if (dedupKey !== undefined) entry.d = dedupKey;
          // Token immediately before the bytes hit disk: if ownership changed
          // between validation and write, nothing is appended and no seq is
          // consumed.
          ensureLease();
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
          usageBytes += size;
          published += 1;
          if (dedupKey !== undefined) {
            dedup.set(dedupKey, { id, seq: mySeq });
            pending.delete(dedupKey);
          }
          logSize = statOrNull(logPath)?.size ?? logSize;
          // Token once more after the durable append: a synchronous writeSync
          // cannot be interleaved by a rename, but this fences a term that
          // expired during the IO window before the acknowledgement leaves.
          ensureLease();
          return { id, seq: mySeq };
        }),
      ).then(
        (ack) => {
          resolveResult(ack);
        },
        (err) => {
          if (dedupKey !== undefined && pending.get(dedupKey)?.reject === rejectResult) {
            pending.delete(dedupKey);
          }
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

      // Merge any other writer's committed keys before reserving, so a resend
      // of a key another process committed rides the original acknowledgement.
      catchUp();

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

      return enqueue(
        trackLeaseJob(async () => {
          if (closed || broken) {
            throw new Error('bus is closed');
          }
          await acquire();
          ensureLease();

          // Resolve the whole plan against durable state inside the serial
          // job, so groups queued behind earlier publishes/batches observe
          // their committed dedup keys — including ones a predecessor owner
          // committed and the takeover reload just merged.
          const gid = randomUUID();
          let nextSeq = seq;
          let effectiveBytes = 0;
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
            effectiveBytes += item.size;
            return { effective: { ack, line, size: item.size, key } };
          });

          // Shape validation happened at call time; quota is judged for the
          // whole group here, against committed state and counting only the
          // first occurrence of each key. One byte over rejects the entire
          // group before the bracket is opened — no seq, stat or file change.
          if (quota !== undefined && usageBytes + effectiveBytes > quota) {
            throw new RangeError('publishBatch: retained bytes would exceed maxBytes');
          }

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

          // Token check after planning and quota, immediately before bytes.
          ensureLease();
          const writeFd = openLogFd();
          try {
            // Coalesce in small chunks (bounds peak memory for huge groups)
            // and loop over partial writes. Single-buffer writeSync is used
            // rather than writev so the append behaves identically on
            // Windows. Everything here is synchronous inside this job, so no
            // position write can interleave. The begin/commit brackets keep
            // the group atomic across a crash even if a partial write occurs.
            const CHUNK = 256;
            for (let i = 0; i < buffers.length; i += CHUNK) {
              const chunk = Buffer.concat(buffers.slice(i, i + CHUNK));
              let off = 0;
              while (off < chunk.length) {
                const written = writeSync(writeFd, chunk, off, chunk.length - off);
                if (!Number.isInteger(written) || written <= 0) {
                  throw new Error('publishBatch: write made no progress, group is not durable');
                }
                off += written;
              }
            }
            if (fsync) fsyncSync(writeFd);
          } catch (err) {
            // Nothing is applied in memory; the uncommitted bracket is
            // severed on reopen/takeover. Stop the bus so later writes cannot
            // glue themselves onto the partial group. Reservations are
            // reclaimed below.
            broken = true;
            throw err;
          }
          // Commit is on disk: a fenced owner must not apply its own group.
          ensureLease();

          // Durable first, state after.
          for (const part of planned) {
            if (!part.effective) continue;
            const { ack, size, key } = part.effective;
            seq = ack.seq;
            bytes += size;
            usageBytes += size;
            published += 1;
            if (key !== undefined) dedup.set(key, { id: ack.id, seq: ack.seq });
          }

          const acks = planned.map((part) =>
            part.reuse
              ? { id: part.reuse.id, seq: part.reuse.seq }
              : { id: part.effective.ack.id, seq: part.effective.ack.seq },
          );
          for (const [key, reservation] of reservations) {
            groupPending.delete(key);
            reservation.resolve(keyAcks.get(key));
          }
          logSize = statOrNull(logPath)?.size ?? logSize;
          ensureLease();
          return acks;
        }),
      ).catch((err) => {
        // Release any reservation still pointing at this failed group.
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

      // replay is a read path: it never contends for write ownership, so it
      // keeps working while another process owns the directory. Its count
      // lands in the shared sidecar like a consumer position.
      return enqueue(() => {
        if (closed) {
          throw new Error('bus is closed');
        }
        mergeForeignMeta();
        // replay(from) is inclusive of from; messagesAfter takes an
        // exclusive lower bound.
        const out = messagesAfter(from - 1);
        const n = out.length;
        if (n > 0) {
          // The counter is sidecar bookkeeping; a read-only / unwritable
          // directory must not turn a replay into a failure. Count it only if
          // it actually lands, so in-memory stats never lead the disk.
          if (writeMetaLine({ t: 's', n })) replayed += n;
        }
        return out;
      });
    },

    register(name) {
      assertOpen();
      assertName(name);
      catchUp();
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
      catchUp();
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
      catchUp();
      let pos = positions.get(name);
      if (pos === undefined) {
        pos = 0;
        // Auto-registration is read-path bookkeeping; never fail a read over
        // it. Track it in memory even if the sidecar cannot take the line.
        persistPositionQuiet(name, pos);
        positions.set(name, pos);
      }
      // read never moves the position; advance() is how consumption lands.
      return messagesAfter(pos);
    },

    compact() {
      if (closed) {
        return Promise.reject(new Error('bus is closed'));
      }
      return enqueue(
        trackLeaseJob(async () => {
          if (closed || broken) {
            throw new Error('bus is closed');
          }
          await acquire();
          ensureLease();
          // Fold every surviving message: snapshot contents plus the live log,
          // skipping anything truncation already discarded.
          const messages = [];
          for (const m of baseMessages) {
            if (m.seq > horizon) messages.push(m);
          }
          const logFloor = Math.max(horizon, foldedSeq);
          const collect = (filePath) => {
            let raw;
            try {
              raw = readFileSync(filePath);
            } catch {
              return;
            }
            for (const entry of parseCommitted(raw)) {
              if (entry.t === 'm' && entry.seq > logFloor) {
                messages.push({ seq: entry.seq, id: entry.id, record: entry.record });
              }
            }
          };
          for (const seg of listSegments()) collect(seg.path);
          collect(logPath);
          // Fold the shared sidecar fully and make its bytes durable before
          // they are checkpointed into the snapshot.
          mergeForeignMeta();
          try {
            if (metaFd < 0) metaFd = openSync(metaPath, 'a');
            fsyncSync(metaFd);
          } catch {
            // Best effort; the line write itself succeeded.
          }
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
            po: metaSize,
          };
          ensureLease();
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
          snapMtimeMs = statOrNull(snapshotPath)?.mtimeMs ?? 0;
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
          closeFdQuiet();
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
                fd = -1;
              }
            } else {
              fd = -1;
            }
            broken = true;
            throw err;
          }
          ensureLease();
          snapshotGen = gen;
          foldedSeq = seq;
          baseMessages = messages;
          const st = statOrNull(logPath);
          logIno = st?.ino ?? 0;
          logSize = st?.size ?? 0;
        }),
      );
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
      return enqueue(
        trackLeaseJob(async () => {
          if (closed || broken) {
            throw new Error('bus is closed');
          }
          const oldHorizon = horizon;
          await acquire();
          ensureLease();

          // Truncation deals in whole segments only: seal the active segment
          // so its contents become eligible, and roll a fresh one. The rename
          // is atomic, so a crash here leaves the content under exactly one
          // name — never duplicated, never half moved.
          if (existsSync(logPath) && statSync(logPath).size > 0) {
            try {
              closeFdQuiet();
              renameSync(logPath, path.join(dir, segmentName(nextSegmentIndex)));
              nextSegmentIndex += 1;
              fd = openSync(logPath, 'a');
              logIno = statOrNull(logPath)?.ino ?? 0;
              logSize = 0;
            } catch (err) {
              // Recover the append handle if possible; the bus refuses further
              // writes either way rather than risking a mixed-up log.
              try {
                fd = openSync(logPath, 'a');
              } catch {
                // Nothing usable: leave fd closed; every later op throws.
                fd = -1;
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
          let freedBytes = 0;
          const segmentBytes = (filePath) => {
            const { entries } = parseLog(readFileSync(filePath));
            let n = 0;
            // Exactly the bytes this run still counts as retained: above both
            // the folded floor and the current horizon. This keeps a stale
            // segment (its earlier unlink failed and it is swept again) from
            // freeing the same bytes twice.
            const floor = Math.max(horizon, foldedSeq);
            for (const entry of entries) {
              if (entry.t === 'm' && entry.seq > floor) n += entry.bytes;
            }
            return n;
          };
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
              freedBytes += segmentBytes(seg.path);
            }
            doomed.push(seg);
          }
          // Nothing can go: the roll above is the only effect of this call.
          if (doomed.length === 0) {
            ensureLease();
            return;
          }

          // Checkpoint every state the doomed segments carry BEFORE unlinking
          // them — including the shared sidecar, folded to EOF. The marker is
          // the first line of the fresh active segment and becomes the new
          // recovery anchor, so a crash after it lands loses nothing.
          mergeForeignMeta();
          try {
            if (metaFd < 0) metaFd = openSync(metaPath, 'a');
            fsyncSync(metaFd);
          } catch {
            // Best effort.
          }
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
            po: metaSize,
          };
          // Token immediately before the checkpoint: a lease lost here rejects
          // the whole call before any marker without authority is written.
          ensureLease();
          try {
            const markerFd = openLogFd();
            writeSync(markerFd, Buffer.from(JSON.stringify(marker) + '\n', 'utf8'));
            // Always fsync here regardless of the fsync option: deletion is
            // only safe once the checkpoint is durable.
            fsyncSync(markerFd);
          } catch (err) {
            broken = true;
            throw err;
          }
          // Make the roll's and the marker's directory entries durable best
          // effort. Deletion no longer depends on this sync succeeding: a
          // failure weakens only the crash-durability guarantee. The marker
          // is valid either way and recovery reconciles leftovers on reopen.
          try {
            const dirFd = openSync(dir, 'r');
            try {
              fsyncSync(dirFd);
            } finally {
              closeSync(dirFd);
            }
          } catch {
            // Directory sync unavailable/failed: deletion still proceeds.
          }
          // Verify ownership one last time before the destructive step. Even
          // at the exact expiry instant an expired token never passes: this
          // either extends the term or rejects, and on rejection nothing is
          // unlinked.
          ensureLease();
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
          horizon = newHorizon;
          // The discarded segments' business bytes are genuinely gone now
          // (marker durable, unlinks attempted); release their quota. The
          // horizon can also run past messages folded into the snapshot:
          // release their bytes too, so usage matches the surviving set.
          // The cumulative stats bytes/published deliberately stay put.
          usageBytes -= freedBytes;
          for (const m of baseMessages) {
            if (m.seq > oldHorizon && m.seq <= newHorizon) {
              usageBytes -= Buffer.byteLength(JSON.stringify(m.record), 'utf8');
            }
          }
          logSize = statOrNull(logPath)?.size ?? logSize;
          ensureLease();
        }),
      );
    },

    stats() {
      catchUp();
      return { seq, bytes, published, replayed };
    },

    // Business bytes of the messages still retained (not yet discarded by
    // retention truncation). Compaction does not change it; an empty bus is
    // zero. Readable after close, like stats().
    usage() {
      return usageBytes;
    },

    close() {
      if (closed) {
        return Promise.resolve();
      }
      closed = true;
      stopHeartbeat();
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
        // Release our term (if held) so a waiting contender takes over
        // immediately; the epoch directory stays as the arbitration record.
        if (leaseEpoch > 0) {
          markReleased();
          leaseEpoch = 0;
          leaseExpiresAt = 0;
        }
        // Failed swaps (or a read-only directory) may have left a handle
        // unopened or unusable; close must stay idempotent and never reject.
        try {
          if (fd >= 0 && fsync) fsyncSync(fd);
        } catch {
          // Ignore: the handle may already be closed.
        }
        closeFdQuiet();
        try {
          if (metaFd >= 0 && fsync) fsyncSync(metaFd);
        } catch {
          // Ignore.
        }
        closeMetaFdQuiet();
      });
    },
  };

  return bus;
}
