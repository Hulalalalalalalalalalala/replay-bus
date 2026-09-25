import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { rm, mkdir } from 'node:fs/promises';
import { writeFileSync, appendFileSync, chmodSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { createBus } from '../src/index.js';

const WORKER = fileURLToPath(new URL('../helpers/worker.mjs', import.meta.url));

let dirs = [];
let workers = [];

async function freshDir() {
  const dir = path.join(tmpdir(), `replay-bus-mw-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  dirs.push(dir);
  return dir;
}

afterEach(async () => {
  for (const w of workers) w.kill();
  workers = [];
  const all = dirs;
  dirs = [];
  await Promise.all(all.map((d) => rm(d, { recursive: true, force: true })));
});

const ERROR_CTORS = { Error, TypeError, RangeError };

// A bus living in a child process, driven over line-delimited JSON.
class Worker {
  constructor(dir, env = {}) {
    this.child = spawn(process.execPath, [WORKER], {
      stdio: ['pipe', 'pipe', 'inherit'],
      env: { ...process.env, ...env, BUS_DIR: dir },
    });
    this.nextId = 1;
    this.pendingReqs = new Map();
    this.buf = '';
    this.child.stdout.setEncoding('utf8');
    this.child.stdout.on('data', (chunk) => {
      this.buf += chunk;
      let idx;
      while ((idx = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, idx);
        this.buf = this.buf.slice(idx + 1);
        if (!line) continue;
        const msg = JSON.parse(line);
        const settle = this.pendingReqs.get(msg.id);
        if (settle) {
          this.pendingReqs.delete(msg.id);
          settle(msg);
        }
      }
    });
    this.child.on('exit', () => {
      for (const settle of this.pendingReqs.values()) {
        settle({ error: 'Error', message: 'worker exited' });
      }
      this.pendingReqs.clear();
    });
    workers.push(this);
  }

  call(op, params = {}) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pendingReqs.set(id, (msg) => {
        if (msg.error) {
          const Ctor = ERROR_CTORS[msg.error] || Error;
          reject(new Ctor(msg.message));
        } else {
          resolve(msg.value);
        }
      });
      this.child.stdin.write(JSON.stringify({ id, op, ...params }) + '\n');
    });
  }

  stop() {
    this.child.kill('SIGSTOP');
  }

  resume() {
    this.child.kill('SIGCONT');
  }

  kill() {
    try {
      this.child.kill('SIGKILL');
    } catch {
      // Already gone.
    }
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('multi-process: two live processes interleave publishes with continuous seqs', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);

  // Deterministic alternation first.
  const seqs = [];
  for (let i = 0; i < 3; i++) {
    seqs.push((await a.call('publish', { record: { w: 'a', i } })).seq);
    seqs.push((await b.call('publish', { record: { w: 'b', i } })).seq);
  }
  assert.deepEqual(seqs, [1, 2, 3, 4, 5, 6]);

  // Then a truly concurrent burst from both processes.
  const burst = [];
  for (let i = 0; i < 20; i++) {
    burst.push(a.call('publish', { record: { w: 'a', n: i } }));
    burst.push(b.call('publish', { record: { w: 'b', n: i } }));
  }
  const acks = await Promise.all(burst);
  const allSeqs = acks.map((x) => x.seq).sort((x, y) => x - y);
  assert.deepEqual(allSeqs, Array.from({ length: 40 }, (_, i) => i + 7));

  // Concurrent batches from both processes.
  const [ba, bb] = await Promise.all([
    a.call('publishBatch', { records: [{ g: 1 }, { g: 2 }, { g: 3 }] }),
    b.call('publishBatch', { records: [{ g: 4 }, { g: 5 }] }),
  ]);
  const batchSeqs = ba.concat(bb).map((x) => x.seq).sort((x, y) => x - y);
  assert.deepEqual(batchSeqs, [47, 48, 49, 50, 51]);

  await a.call('close');
  await b.call('close');

  // A fresh opener sees one continuous, gapless, duplication-free log.
  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.equal(got.length, 51);
  assert.deepEqual(
    got.map((m) => m.seq),
    Array.from({ length: 51 }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, 51);
  assert.equal(bus.stats().seq, 51);
  await bus.close();
});

test('multi-process: dedup key is one shared state across processes and restarts', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);

  const first = await a.call('publish', { record: { order: 'A', dedupKey: 'k1' } });
  // Same key from another process: same first acknowledgement, no new message.
  const again = await b.call('publish', { record: { order: 'B', dedupKey: 'k1' } });
  assert.deepEqual(again, first);
  // And within a batch in the other process.
  const batch = await b.call('publishBatch', {
    records: [{ x: 1, dedupKey: 'k1' }, { x: 2, dedupKey: 'k2' }],
  });
  assert.deepEqual(batch[0], first);
  assert.equal(batch[1].seq, 2);

  assert.equal((await a.call('stats')).published, 2);
  assert.equal((await b.call('stats')).published, 2);

  await a.call('close');
  await b.call('close');

  // Across a restart the key still resolves to the first acknowledgement.
  const c = new Worker(dir);
  const third = await c.call('publish', { record: { order: 'C', dedupKey: 'k1' } });
  assert.deepEqual(third, first);
  assert.equal((await c.call('stats')).published, 2);
  await c.call('close');
});

test('multi-process: consumer positions advance from two processes as one state', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);

  for (let i = 1; i <= 5; i++) await a.call('publish', { record: { i } });

  assert.equal(await a.call('register', { name: 'c' }), 0);
  // The other process sees the same position table.
  assert.equal(await b.call('register', { name: 'c' }), 0);

  assert.equal(await b.call('advance', { name: 'c', to: 3 }), 3);
  // A reads from the merged position: only messages after 3.
  assert.deepEqual((await a.call('read', { name: 'c' })).map((m) => m.seq), [4, 5]);
  // Backwards moves are rejected against the merged position too.
  await assert.rejects(a.call('advance', { name: 'c', to: 2 }), RangeError);
  assert.equal(await a.call('register', { name: 'c' }), 3);

  assert.equal(await a.call('advance', { name: 'c', to: 5 }), 5);
  assert.deepEqual(await b.call('read', { name: 'c' }), []);

  await a.call('close');
  await b.call('close');

  // Positions persist across the restart for every process.
  const bus = createBus({ path: dir });
  assert.equal(bus.register('c'), 5);
  await bus.close();
});

test('multi-process: quota judges merged occupancy; truncation frees it for everyone', async () => {
  const dir = await freshDir();
  const r1 = { pad: 'x'.repeat(30) };
  const size = Buffer.byteLength(JSON.stringify(r1), 'utf8');
  const maxBytes = size * 2;

  const a = new Worker(dir, { BUS_MAX_BYTES: String(maxBytes) });
  const b = new Worker(dir, { BUS_MAX_BYTES: String(maxBytes) });

  await a.call('publish', { record: r1 });
  await b.call('publish', { record: { pad: 'y'.repeat(30) } });
  assert.equal(await a.call('usage'), maxBytes);

  // Full for both processes: one byte over rejects RangeError, state untouched.
  await assert.rejects(b.call('publish', { record: { z: 1 } }), RangeError);
  await assert.rejects(a.call('publish', { record: { z: 2 } }), RangeError);
  assert.equal((await b.call('stats')).published, 2);

  // Either process can truncate; the freed headroom is shared.
  await b.call('truncate', { before: 2 });
  assert.equal(await a.call('usage'), 0);
  const ack = await b.call('publish', { record: r1 });
  assert.equal(ack.seq, 3);

  await a.call('close');
  await b.call('close');
});

test('multi-process: takeover fences the old holder; its writes never mix in', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);

  await a.call('publish', { record: { w: 'a', n: 1 } });
  await a.call('publish', { record: { w: 'a', n: 2 } });

  // Freeze the old holder without killing it: its heartbeat goes stale while
  // the process is technically alive, so the next opener must take over.
  a.stop();
  await sleep(900);

  const b = new Worker(dir, env);
  const b1 = await b.call('publish', { record: { w: 'b', n: 1 } });
  assert.equal(b1.seq, 3);

  // The old holder wakes up: every write is rejected wholesale.
  a.resume();
  await assert.rejects(a.call('publish', { record: { w: 'a', n: 3 } }), Error);
  await assert.rejects(a.call('publishBatch', { records: [{ w: 'a', n: 4 }] }), Error);
  await assert.rejects(a.call('truncate', { before: 2 }), Error);

  // The new holder keeps writing; nothing from the fenced process mixes in.
  const b2 = await b.call('publish', { record: { w: 'b', n: 2 } });
  assert.equal(b2.seq, 4);

  a.kill();
  await b.call('close');

  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    [
      { w: 'a', n: 1 },
      { w: 'a', n: 2 },
      { w: 'b', n: 1 },
      { w: 'b', n: 2 },
    ],
  );
  assert.deepEqual(bus.stats(), {
    seq: 4,
    bytes: got.reduce((n, m) => n + Buffer.byteLength(JSON.stringify(m.record), 'utf8'), 0),
    published: 4,
    replayed: 4,
  });
  await bus.close();
});

test('multi-process: crash leaves stale lease, torn line and half segment; reopen recovers', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);
  await a.call('publish', { record: { i: 1 } });
  await a.call('publish', { record: { i: 2, dedupKey: 'k' } });
  // Hard kill: no close, no lease release, heartbeat file left behind.
  a.kill();
  await sleep(100);

  // Crash debris: a torn trailing line in the active log and a half lease.
  appendFileSync(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":3,"id":"x",');
  writeFileSync(path.join(dir, 'bus.owner.json'), '{"epoch":7,"sessions":[{');

  const bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 2);
  assert.equal(bus.stats().published, 2);
  const ack = await bus.publish({ i: 3 });
  assert.equal(ack.seq, 3);
  // Dedup from before the crash still resolves.
  const dup = await bus.publish({ whatever: 1, dedupKey: 'k' });
  assert.equal(dup.seq, 2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3]);
  await bus.close();
});

test('multi-process: replay counters merge across writers and restarts', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);

  await a.call('publish', { record: { i: 1 } });
  await b.call('publish', { record: { i: 2 } });
  await a.call('publish', { record: { i: 3 } });

  assert.equal((await a.call('replay', { from: 0 })).length, 3);
  assert.equal((await b.call('replay', { from: 2 })).length, 2);
  // Each side observes the merged cumulative counters.
  assert.equal((await a.call('stats')).replayed, 5);
  assert.equal((await b.call('stats')).replayed, 5);
  assert.equal((await a.call('stats')).published, 3);

  await a.call('close');
  await b.call('close');

  const bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), {
    seq: 3,
    bytes: 3 * Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8'),
    published: 3,
    replayed: 5,
  });
  await bus.close();
});

test('multi-process: compact and truncate from either process stay consistent', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);

  for (let i = 1; i <= 6; i++) await a.call('publish', { record: { i, dedupKey: `d${i}` } });
  await b.call('compact');
  for (let i = 7; i <= 9; i++) await b.call('publish', { record: { i } });
  // A (the other process) truncates; no consumers registered anywhere.
  await a.call('truncate', { before: 9 });
  assert.deepEqual(await b.call('replay', { from: 0 }), []);

  // Dedup survives across the compaction/truncation done by other processes.
  const dup = await b.call('publish', { record: { x: 1, dedupKey: 'd3' } });
  assert.equal(dup.seq, 3);
  const ack = await a.call('publish', { record: { i: 10 } });
  assert.equal(ack.seq, 10);

  await a.call('close');
  await b.call('close');

  const bus = createBus({ path: dir });
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [10]);
  assert.equal(bus.stats().published, 10);
  assert.equal(bus.stats().seq, 10);
  await bus.close();
});

test('multi-process: stale bytes appended after the fence are invisible; history before it stands', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);
  await a.call('publish', { record: { w: 'a', n: 1 } });
  await a.call('publish', { record: { w: 'a', n: 2 } });
  a.stop();
  await sleep(900);

  const b = new Worker(dir, env);
  assert.equal((await b.call('publish', { record: { w: 'b', n: 1 } })).seq, 3);
  await b.call('close');

  // Simulate the preempted holder waking after the takeover and appending
  // complete lines / a complete batch stamped with its old epoch, directly to
  // the file (bypassing the in-memory credential check, as a raw OS append
  // would). Read the owner file to learn both epochs from the fence.
  const log = readFileSync(path.join(dir, 'bus.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  const fenceLine = log.find((e) => e.t === 'f');
  assert.ok(fenceLine, 'a fence line was written on takeover');
  const newEpoch = fenceLine.g;
  const oldEpoch = newEpoch - 1;
  assert.equal(oldEpoch >= 1, true);

  const dirty =
    JSON.stringify({ t: 'm', seq: 99, id: 'dirty1', bytes: 9, g: oldEpoch, record: { dirty: 1 } }) +
    '\n' +
    JSON.stringify({ t: 'b', id: 'dirty-g', g: oldEpoch }) +
    '\n' +
    JSON.stringify({ t: 'm', seq: 100, id: 'dirty2', bytes: 9, g: oldEpoch, record: { dirty: 2 } }) +
    '\n' +
    JSON.stringify({ t: 'bk', id: 'dirty-g', g: oldEpoch }) +
    '\n' +
    JSON.stringify({ t: 'p', name: 'ghost', pos: 99, g: oldEpoch }) +
    '\n';
  appendFileSync(path.join(dir, 'bus.jsonl'), dirty);

  const bus = createBus({ path: dir });
  // The two committed pre-takeover messages and the new holder's message
  // survive; none of the dirty bytes count, and the sequence has no hole.
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    [{ w: 'a', n: 1 }, { w: 'a', n: 2 }, { w: 'b', n: 1 }],
  );
  assert.deepEqual(bus.stats(), {
    seq: 3,
    bytes:
      Buffer.byteLength(JSON.stringify({ w: 'a', n: 1 }), 'utf8') +
      Buffer.byteLength(JSON.stringify({ w: 'a', n: 2 }), 'utf8') +
      Buffer.byteLength(JSON.stringify({ w: 'b', n: 1 }), 'utf8'),
    published: 3,
    replayed: 3,
  });
  assert.equal(bus.usage(), bus.stats().bytes);
  // The ghost position from the fenced append did not land.
  assert.equal(bus.register('ghost'), 0);
  // The new holder continues the sequence cleanly.
  const next = await bus.publish({ w: 'b', n: 2 });
  assert.equal(next.seq, 4);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  await bus.close();

  a.kill();
});

test('multi-process: truncate after a takeover keeps the fence effective', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);
  await a.call('publish', { record: { i: 1 } });
  a.stop();
  await sleep(900);
  const b = new Worker(dir, env);
  await b.call('publish', { record: { i: 2 } });
  // A truncation seals segments and rewrites the fence into the new log.
  await b.call('truncate', { before: 2 });
  assert.deepEqual(await b.call('replay', { from: 0 }), []);

  // Append an old-epoch line directly: it must stay invisible post-truncate.
  const fenceLine = readFileSync(path.join(dir, 'bus.jsonl'), 'utf8')
    .trim()
    .split('\n')
    .map(JSON.parse)
    .find((e) => e.t === 'f');
  const oldEpoch = fenceLine.g - 1;
  appendFileSync(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 99, id: 'z', bytes: 7, g: oldEpoch, record: { z: 1 } }) + '\n',
  );

  const bus = createBus({ path: dir });
  assert.deepEqual(await bus.replay(0), []);
  assert.equal(bus.stats().published, 2);
  assert.equal(bus.usage(), 0);
  await bus.close();

  a.kill();
  await b.call('close');
});

test('multi-process: many writers plus compaction never produce gaps or duplicates', async () => {
  const dir = await freshDir();
  const N = 4;
  const PER = 60;
  const procs = [];
  for (let w = 0; w < N; w++) procs.push(new Worker(dir));

  const jobs = [];
  for (let w = 0; w < N; w++) {
    for (let i = 0; i < PER; i++) {
      jobs.push(procs[w].call('publish', { record: { w, i } }));
      if (i % 17 === 0) jobs.push(procs[w].call('compact'));
      if (i % 23 === 0) jobs.push(procs[w].call('replay', { from: 0 }));
    }
  }
  const acks = await Promise.all(jobs.filter((j) => j !== undefined));
  const publishAcks = acks.filter(
    (a) => a && typeof a.seq === 'number' && typeof a.id === 'string',
  );
  const total = N * PER;
  assert.equal(publishAcks.length, total);
  const seqs = publishAcks.map((a) => a.seq).sort((x, y) => x - y);
  assert.deepEqual(
    seqs,
    Array.from({ length: total }, (_, i) => i + 1),
  );
  assert.equal(new Set(publishAcks.map((a) => a.id)).size, total);

  for (const p of procs) await p.call('close');

  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.equal(got.length, total);
  assert.deepEqual(
    got.map((m) => m.seq),
    Array.from({ length: total }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, total);
  await bus.close();
});

test('multi-process: fsync on, concurrent truncation across processes stays consistent', async () => {
  const dir = await freshDir();
  const env = { BUS_FSYNC: '1' };
  const a = new Worker(dir, env);
  const b = new Worker(dir, env);

  const jobs = [];
  for (let i = 1; i <= 40; i++) {
    jobs.push((i % 2 ? a : b).call('publish', { record: { i } }));
    if (i % 8 === 0) jobs.push(a.call('truncate', { before: i }));
    if (i % 11 === 0) jobs.push(b.call('truncate', { before: i }));
    if (i % 6 === 0) jobs.push(a.call('compact'));
  }
  await Promise.all(jobs);

  for (const p of [a, b]) await p.call('close');

  // Whatever remains is a contiguous suffix, fully re-readable after restart.
  const bus = createBus({ path: dir, fsync: true });
  const got = await bus.replay(0);
  if (got.length > 0) {
    for (let k = 0; k < got.length; k++) {
      assert.equal(got[k].seq, got[0].seq + k);
    }
    assert.equal(got[got.length - 1].seq, 40);
  }
  assert.equal(bus.stats().published, 40);
  assert.equal(bus.stats().seq, 40);
  // Cumulative bytes never shrink and equal the sum over all 40 records.
  let sum = 0;
  for (let i = 1; i <= 40; i++) sum += Buffer.byteLength(JSON.stringify({ i }), 'utf8');
  assert.equal(bus.stats().bytes, sum);
  await bus.close();
});

test('read view: continuous cross-process writers page as one committed prefix while read lock-free', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  const b = new Worker(dir);
  await a.call('stats');
  await b.call('stats');

  // A third, purely-reading instance in the parent process: it never writes
  // and its readRange must keep serving while both workers commit.
  const reader = createBus({ path: dir });
  const PER = 60;
  let nextA = 1;
  let nextB = 1;
  const writers = (async () => {
    const jobs = [];
    for (let i = 0; i < PER; i++) {
      jobs.push(a.call('publishBatch', { records: [{ w: 'a', n: i }, { w: 'a', n: i + 0.5 }] }));
      jobs.push(b.call('publish', { record: { w: 'b', n: i } }));
      if (i % 13 === 0) jobs.push(b.call('compact'));
      if (i % 17 === 0) jobs.push(a.call('truncate', { before: 0 })); // no-op roll boundary
    }
    await Promise.all(jobs);
  })();
  void nextA; void nextB;

  const total = PER * 3; // 2 per A batch + 1 per B publish
  let saw = 0;
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const page = reader.readRange(1, 32);
    if (page.length > 0) {
      // Every page is ascending; across repeated reads seqs never repeat or
      // reorder (a committed prefix), and it ends on a real message.
      for (let k = 1; k < page.length; k++) {
        assert.ok(page[k].seq > page[k - 1].seq);
      }
      saw = Math.max(saw, page[page.length - 1].seq);
    }
    if (saw >= total) break;
    await sleep(10);
  }
  await writers;

  const all = reader.readRange(1, total);
  assert.equal(all.length, total);
  assert.deepEqual(all.map((m) => m.seq), Array.from({ length: total }, (_, i) => i + 1));
  assert.equal(reader.stats().published, total);
  await reader.close();

  for (const p of [a, b]) await p.call('close');
});

test('read view: the same range reads identically across a cross-process compaction', async () => {
  const dir = await freshDir();
  const writer = new Worker(dir);
  for (let i = 1; i <= 20; i++) await writer.call('publish', { record: { i } });

  const reader = createBus({ path: dir });
  const compactor = new Worker(dir);

  // Baseline page taken by the independent reader.
  const baseline = reader.readRange(1, 100).map((m) => m.seq);
  assert.deepEqual(baseline, Array.from({ length: 20 }, (_, i) => i + 1));

  // Another process compacts; the reader never takes the lock and the page
  // is byte-for-byte the same across the snapshot switch.
  await compactor.call('compact');
  assert.deepEqual(reader.readRange(1, 100).map((m) => m.seq), baseline);

  // Repeated compact/publish cycles: every observation is a continuous
  // committed prefix, never a half snapshot.
  for (let round = 0; round < 4; round++) {
    for (let i = 0; i < 5; i++) {
      await compactor.call('publish', { record: { round, i } });
    }
    await compactor.call('compact');
    const page = reader.readRange(1, 1000);
    for (let k = 1; k < page.length; k++) {
      assert.equal(page[k].seq, page[k - 1].seq + 1);
    }
  }
  const finalPage = reader.readRange(1, 1000);
  assert.equal(finalPage[finalPage.length - 1].seq, 40);
  assert.deepEqual(finalPage.map((m) => m.seq), Array.from({ length: 40 }, (_, i) => i + 1));

  await writer.call('close');
  await compactor.call('close');
  await reader.close();
});

test('read view: a taken-over instance keeps reading (readRange/stats) while its queued group wholly fails', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);
  for (let i = 1; i <= 3; i++) await a.call('publish', { record: { w: 'a', n: i } });

  a.stop();
  await sleep(900);
  const b = new Worker(dir, env);
  assert.equal((await b.call('publish', { record: { w: 'b', n: 1 } })).seq, 4);

  a.resume();
  // Every queued write from the displaced holder fails wholesale...
  await assert.rejects(a.call('publishBatch', { records: [{ w: 'a', n: 99 }] }), Error);
  // ...yet its read-only path keeps working lock-free, seeing exactly the
  // committed prefix with none of its rejected bytes mixed in.
  const got = await a.call('readRange', { start: 1, limit: 100 });
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4]);
  assert.deepEqual((await a.call('stats')).seq, 4);
  assert.deepEqual((await a.call('readRange', { start: 2, limit: 2 })).map((m) => m.seq), [2, 3]);

  // The new owner continues with a continuous sequence after the failed group.
  assert.equal((await b.call('publish', { record: { w: 'b', n: 2 } })).seq, 5);
  a.kill();
  await b.call('close');
});

test('open throws Error synchronously when the directory is unusable', async () => {
  const dir = await freshDir();
  // The path exists as a regular file: cannot become a bus directory.
  const file = path.join(dir, 'afile');
  writeFileSync(file, 'x');
  assert.throws(() => createBus({ path: file }), Error);

  // A directory without write permission rejects the open.
  if (typeof process.geteuid === 'function' && process.geteuid() !== 0) {
    const sealed = path.join(dir, 'sealed');
    await mkdir(sealed);
    chmodSync(sealed, 0o500);
    try {
      assert.throws(() => createBus({ path: path.join(sealed, 'bus') }), Error);
      assert.throws(() => createBus({ path: sealed }), Error);
    } finally {
      chmodSync(sealed, 0o700);
    }
  }
});
