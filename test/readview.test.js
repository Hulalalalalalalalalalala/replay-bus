// Tests for the lock-free read view and readRange: paged fetching by
// sequence number, reads that never take the write mutex or the write
// serial chain, consistency across truncation/compaction snapshot
// switches, reads after close and after a takeover, and the drain's
// coalescing window merging genuinely concurrent writers into one group.
import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { rm, mkdir } from 'node:fs/promises';
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { createBus } from '../src/index.js';

const WORKER = fileURLToPath(new URL('../helpers/worker.mjs', import.meta.url));

let dirs = [];
let workers = [];

async function freshDir() {
  const dir = path.join(tmpdir(), `replay-bus-rv-${Math.random().toString(36).slice(2)}`);
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
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

const logEntries = (dir) => {
  const file = path.join(dir, 'bus.jsonl');
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));
};

test('readRange pages ascending from an inclusive start, short at the tail', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 10; i++) await bus.publish({ i });

  assert.deepEqual(bus.readRange(1, 4).map((m) => m.seq), [1, 2, 3, 4]);
  assert.deepEqual(bus.readRange(5, 4).map((m) => m.seq), [5, 6, 7, 8]);
  // Fewer than the limit near the end, never an error.
  assert.deepEqual(bus.readRange(9, 4).map((m) => m.seq), [9, 10]);
  // Past the end is an empty array.
  assert.deepEqual(bus.readRange(11, 3), []);
  // The start is inclusive and the shape matches replay exactly.
  const replay = await bus.replay(0);
  assert.deepEqual(bus.readRange(1, 100), replay);
  assert.deepEqual(bus.readRange(3, 1)[0], replay[2]);
  await bus.close();
});

test('readRange validates arguments synchronously, even after close', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  for (const bad of [-1, 1.5, NaN, '1', undefined, null]) {
    assert.throws(() => bus.readRange(bad, 1), TypeError);
  }
  for (const bad of [0, -2, 1.5, NaN, '2', undefined, null]) {
    assert.throws(() => bus.readRange(1, bad), TypeError);
  }
  await bus.close();
  // Still TypeError, not a closed-bus error, once closed.
  assert.throws(() => bus.readRange(-1, 1), TypeError);
  assert.throws(() => bus.readRange(1, 0), TypeError);
});

test('readRange has no side effects on stats or positions', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  bus.register('c');
  const before = bus.stats();
  bus.readRange(1, 3);
  bus.readRange(1, 100);
  bus.readRange(4, 2);
  assert.deepEqual(bus.stats(), before); // replayed does not move
  assert.equal(bus.register('c'), 0); // positions do not move
  await bus.close();
});

test('readRange starts at the earliest survivor of a deleted range; empty when all deleted', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 6; i++) await bus.publish({ i });
  await bus.truncate(6); // the sealed segment 1-6 is deleted
  for (let i = 7; i <= 10; i++) await bus.publish({ i });

  // A start inside the truncated gap begins at the earliest survivor.
  assert.deepEqual(bus.readRange(1, 100).map((m) => m.seq), [7, 8, 9, 10]);
  assert.deepEqual(bus.readRange(3, 2).map((m) => m.seq), [7, 8]);
  assert.deepEqual(bus.readRange(3, 2).map((m) => m.record), [{ i: 7 }, { i: 8 }]);

  await bus.truncate(10); // everything is deleted
  assert.deepEqual(bus.readRange(1, 5), []);
  // Stats never shrink and the freed occupancy is gone.
  assert.equal(bus.stats().seq, 10);
  assert.equal(bus.stats().published, 10);
  assert.equal(bus.usage(), 0);
  await bus.close();
});

test('readRange is identical across a compaction snapshot switch', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 8; i++) await bus.publish({ i });
  const before = bus.readRange(1, 100);
  const pageBefore = bus.readRange(3, 3);
  await bus.compact();
  assert.deepEqual(bus.readRange(1, 100), before);
  assert.deepEqual(bus.readRange(3, 3), pageBefore);
  // New publishes after the fold extend the same continuous sequence.
  await bus.publish({ i: 9 });
  assert.deepEqual(bus.readRange(7, 100).map((m) => m.seq), [7, 8, 9]);
  // A second fold keeps every page stable.
  const mixed = bus.readRange(1, 100);
  await bus.compact();
  assert.deepEqual(bus.readRange(1, 100), mixed);
  assert.deepEqual(bus.readRange(1, 100).map((m) => m.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  await bus.close();
});

test('readRange spans snapshot, a retained sealed segment and the active log after reopen', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.compact(); // 1-5 folded into the snapshot
  for (let i = 6; i <= 7; i++) await bus.publish({ i });
  // A consumer at 0 holds the sealed segment so truncation keeps it whole.
  bus.register('c');
  await bus.truncate(7); // rolls 6-7 into a finalized segment, retained
  for (let i = 8; i <= 10; i++) await bus.publish({ i });
  await bus.close();

  const reopened = createBus({ path: dir });
  const got = reopened.readRange(1, 100);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.deepEqual(
    got.map((m) => m.record),
    Array.from({ length: 10 }, (_, i) => ({ i: i + 1 })),
  );
  // A page crossing every layer (snapshot → segment → active) is intact.
  assert.deepEqual(reopened.readRange(4, 4).map((m) => m.seq), [4, 5, 6, 7]);
  assert.deepEqual(await reopened.replay(6), reopened.readRange(6, 100));
  await reopened.close();
});

test('readRange stays readable after close, repeatable and side-effect free', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.close();
  const first = bus.readRange(2, 10);
  assert.deepEqual(first.map((m) => m.seq), [2, 3, 4]);
  // Repeated fetches are identical and change nothing.
  assert.deepEqual(bus.readRange(2, 10), first);
  assert.deepEqual(bus.stats(), { seq: 4, bytes: bus.stats().bytes, published: 4, replayed: 0 });
  assert.equal(bus.usage(), 4 * Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8'));
});

test('reads never take the write mutex: committed prefix while a writer holds the drain', async () => {
  const dir = await freshDir();
  const a = new Worker(dir);
  await a.call('publishBatch', { records: [{ i: 1 }, { i: 2 }, { i: 3 }] });

  // The reader opens before the slow drain starts.
  const bus = createBus({ path: dir });
  bus.register('c');
  assert.deepEqual(bus.readRange(1, 10).map((m) => m.seq), [1, 2, 3]);

  // A remote writer freezes at the drain entrance, holding bus.lock.
  const c = new Worker(dir, { BUS_DRAIN_DELAY_MS: '600' });
  const pending = c.call('publishBatch', { records: [{ i: 4 }, { i: 5 }] });
  await sleep(200);

  // Every read path answers immediately with the committed prefix; the
  // in-flight group is wholly invisible (never half a group).
  let t0 = Date.now();
  assert.deepEqual(bus.readRange(1, 10).map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(bus.read('c').map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(bus.stats(), {
    seq: 3,
    bytes: 3 * Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8'),
    published: 3,
    replayed: 0,
  });
  assert.equal(bus.usage(), 3 * Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8'));
  assert.ok(Date.now() - t0 < 400, `reads blocked behind the write mutex (${Date.now() - t0}ms)`);

  await pending;
  // Once the group commits it is wholly visible, with continuous seqs.
  assert.deepEqual(bus.readRange(1, 10).map((m) => m.seq), [1, 2, 3, 4, 5]);
  assert.equal(bus.stats().published, 5);
  await bus.close();
  await a.call('close');
  await c.call('close');
});

test('replay does not queue behind the write chain and returns the committed prefix', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 3; i++) await bus.publish({ i });

  // A slow drain holds the mutex in another process.
  const c = new Worker(dir, { BUS_DRAIN_DELAY_MS: '500' });
  const pending = c.call('publish', { record: { i: 4 } });
  await sleep(150);

  // The replay's data comes from the lock-free view; only its marker
  // waits for the mutex, so the result is the call-time committed prefix.
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  await pending;
  assert.equal(bus.stats().replayed, 3);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  assert.equal(bus.stats().replayed, 7);
  await bus.close();
  await c.call('close');
});

test('drain coalescing: a writer that arrives while the drainer waits merges into the group', async () => {
  const dir = await freshDir();
  // No drain-entrance delay: the bounded re-gather window alone merges a
  // request staged after the first gather, while the drainer still holds
  // the mutex.
  const env = { BUS_DRAIN_MERGE_MS: '200' };
  const a = new Worker(dir, env);
  const b = new Worker(dir, env);
  await b.call('stats'); // both open before the race

  // A reaches the drain first and waits inside its coalescing window;
  // B stages (lock-free) a moment later and is gathered into the SAME group.
  const [ackA, ackB] = await Promise.all([
    a.call('publish', { record: { w: 'a' } }),
    sleep(60).then(() => b.call('publishBatch', { records: [{ w: 'b1' }, { w: 'b2' }] })),
  ]);
  assert.equal(ackA.seq, 1);
  assert.deepEqual(ackB.map((x) => x.seq), [2, 3]);

  // One bracketed group covers the merged single and batch, with no gap.
  const entries = logEntries(dir);
  assert.equal(entries.filter((e) => e.t === 'b').length, 1);
  assert.equal(entries.filter((e) => e.t === 'bk').length, 1);
  assert.deepEqual(
    entries.filter((e) => e.t === 'm').map((e) => e.seq),
    [1, 2, 3],
  );
  await a.call('close');
  await b.call('close');
});

test('a taken-over instance keeps reading; its writes fail wholesale', async () => {
  const dir = await freshDir();
  const env = { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' };
  const a = new Worker(dir, env);
  await a.call('publishBatch', { records: [{ i: 1 }, { i: 2 }] });
  assert.equal(await a.call('register', { name: 'c' }), 0);

  // Freeze the old holder so the next opener takes the lease over.
  a.stop();
  await sleep(900);
  const b = new Worker(dir, env);
  const ack = await b.call('publish', { record: { i: 3 } });
  assert.equal(ack.seq, 3);
  a.resume();

  // The displaced instance reads fine: readRange, stats, usage, read.
  assert.deepEqual((await a.call('readRange', { start: 1, limit: 10 })).map((m) => m.seq), [1, 2, 3]);
  assert.equal((await a.call('stats')).published, 3);
  assert.equal((await a.call('usage')), 3 * Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8'));
  assert.deepEqual((await a.call('read', { name: 'c' })).map((m) => m.seq), [1, 2, 3]);
  // Its writes and truncation fail wholesale; nothing mixes into the log.
  // The replay marker is a write too, so replay rejects as before.
  await assert.rejects(a.call('replay', { from: 0 }), Error);
  await assert.rejects(a.call('publish', { record: { i: 99 } }), Error);
  await assert.rejects(a.call('publishBatch', { records: [{ i: 98 }] }), Error);
  await assert.rejects(a.call('truncate', { before: 2 }), Error);
  assert.deepEqual((await b.call('readRange', { start: 1, limit: 10 })).map((m) => m.seq), [1, 2, 3]);

  a.kill();
  await b.call('close');
});

test('reads interleaved with repeated compaction stay consistent', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const expected = [];
  for (let round = 0; round < 5; round++) {
    for (let i = 1; i <= 4; i++) {
      const n = round * 4 + i;
      expected.push({ seq: n, record: { n } });
      await bus.publish({ n });
    }
    await bus.compact();
    // Every fetch after every fold is the same continuous committed prefix.
    const got = bus.readRange(1, 1000);
    assert.deepEqual(
      got.map((m) => m.seq),
      expected.map((m) => m.seq),
    );
    assert.deepEqual(
      got.map((m) => m.record),
      expected.map((m) => m.record),
    );
    // Seq continuity with no duplicates, page by page.
    for (let start = 1; start <= expected.length; start += 3) {
      const page = bus.readRange(start, 3);
      assert.deepEqual(
        page.map((m) => m.seq),
        expected.slice(start - 1, start + 2).map((m) => m.seq),
      );
    }
  }
  assert.equal(bus.stats().published, 20);
  await bus.close();
});

test('readRange tracks continuous commits from another process without rescans', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const w = new Worker(dir);
  // Continuous commits from the writer; the reader keeps up incrementally.
  for (let batch = 0; batch < 5; batch++) {
    await w.call('publishBatch', {
      records: Array.from({ length: 4 }, (_, i) => ({ n: batch * 4 + i + 1 })),
    });
    const got = bus.readRange(1, 1000);
    assert.deepEqual(
      got.map((m) => m.seq),
      Array.from({ length: (batch + 1) * 4 }, (_, i) => i + 1),
    );
    assert.equal(bus.stats().published, (batch + 1) * 4);
  }
  // A mid-log page comes straight from the locator index.
  assert.deepEqual(bus.readRange(9, 4).map((m) => m.seq), [9, 10, 11, 12]);
  await bus.close();
  await w.call('close');
});

test('paged reads stay coherent while another process publishes and compacts', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const w = new Worker(dir);
  await w.call('publishBatch', { records: [{ n: 1 }] });

  // The writer grows and repeatedly folds the log; the reader pages through
  // concurrently. Every page is ascending, gapless and record-consistent
  // whether it observed the pre- or post-switch snapshot.
  let published = 1;
  const rounds = [];
  const writer = (async () => {
    for (let r = 0; r < 4; r++) {
      await w.call('publishBatch', {
        records: Array.from({ length: 5 }, (_, i) => ({ n: published + 1 + i })),
      });
      published += 5;
      await w.call('compact');
      rounds.push(published);
    }
  })();

  const deadline = Date.now() + 8000;
  while (Date.now() < deadline && rounds.length < 4) {
    // Walk the committed prefix page by page.
    let start = 1;
    let prev = 0;
    for (;;) {
      const page = bus.readRange(start, 3);
      if (page.length === 0) break;
      for (const m of page) {
        assert.equal(m.seq, prev + 1); // continuous, no repeats
        assert.deepEqual(m.record, { n: m.seq });
        prev = m.seq;
      }
      start += page.length;
      if (page.length < 3) break;
    }
    await sleep(5);
  }
  await writer;

  // Settled view: the whole prefix intact through every snapshot switch.
  const all = bus.readRange(1, 1000);
  assert.deepEqual(
    all.map((m) => m.seq),
    Array.from({ length: 21 }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, 21);
  await bus.close();
  await w.call('close');
});
