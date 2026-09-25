// Tests for the cross-process grouped commit pipeline: deterministic
// request merging across processes, shared dedup/quota inside one group,
// whole-group rollback on flush failure, and a commit group seized by a
// takeover mid-flush.
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
  const dir = path.join(tmpdir(), `replay-bus-pipe-${Math.random().toString(36).slice(2)}`);
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

test('pipeline: two processes staged together land in one commit group', async () => {
  const dir = await freshDir();
  // A holds the mutex at the drain entrance so B's request, staged while A
  // waits, is gathered into the very same group.
  const a = new Worker(dir, { BUS_DRAIN_DELAY_MS: '300' });
  const b = new Worker(dir);
  await b.call('stats'); // make sure B is open before the race starts

  const [ackA, ackB] = await Promise.all([
    a.call('publish', { record: { w: 'a' } }),
    sleep(80).then(() => b.call('publish', { record: { w: 'b' } })),
  ]);
  assert.deepEqual([ackA.seq, ackB.seq], [1, 2]);

  // Exactly one begin/commit bracket covers both messages.
  const entries = logEntries(dir);
  assert.equal(entries.filter((e) => e.t === 'b').length, 1);
  assert.equal(entries.filter((e) => e.t === 'bk').length, 1);
  assert.deepEqual(
    entries.filter((e) => e.t === 'm').map((e) => e.seq),
    [1, 2],
  );

  await a.call('close');
  await b.call('close');

  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2]);
  assert.equal(bus.stats().published, 2);
  await bus.close();
});

test('pipeline: a dedup key shared inside one merged group is charged once', async () => {
  const dir = await freshDir();
  const a = new Worker(dir, { BUS_DRAIN_DELAY_MS: '300' });
  const b = new Worker(dir);
  await b.call('stats');

  const [ackA, ackB] = await Promise.all([
    a.call('publish', { record: { order: 'A', dedupKey: 'k' } }),
    sleep(80).then(() => b.call('publish', { record: { order: 'B', dedupKey: 'k' } })),
  ]);
  assert.deepEqual(ackA, ackB);

  await a.call('close');
  await b.call('close');

  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.equal(got.length, 1);
  assert.deepEqual(got[0].record, { order: 'A', dedupKey: 'k' });
  assert.equal(bus.stats().published, 1);
  // The first acknowledgement stays the only one across restart.
  const again = await bus.publish({ order: 'C', dedupKey: 'k' });
  assert.deepEqual(again, ackA);
  await bus.close();
});

test('pipeline: quota is judged per item across the merged group', async () => {
  const dir = await freshDir();
  const rec1 = { w: 'a', pad: 'x'.repeat(30) };
  const rec2 = { w: 'b', pad: 'y'.repeat(30) };
  const size = Buffer.byteLength(JSON.stringify(rec1), 'utf8');
  assert.equal(Buffer.byteLength(JSON.stringify(rec2), 'utf8'), size);
  const a = new Worker(dir, { BUS_DRAIN_DELAY_MS: '300', BUS_MAX_BYTES: String(size) });
  const b = new Worker(dir, { BUS_MAX_BYTES: String(size) });
  await b.call('stats');

  const results = await Promise.allSettled([
    a.call('publish', { record: rec1 }),
    sleep(80).then(() => b.call('publish', { record: rec2 })),
  ]);
  const fulfilled = results.filter((r) => r.status === 'fulfilled');
  const rejected = results.filter((r) => r.status === 'rejected');
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].reason instanceof RangeError, true);

  assert.equal(await a.call('usage'), size);
  assert.equal((await b.call('stats')).published, 1);
  // Exactly one message visible, continuous seq.
  assert.equal(fulfilled[0].value.seq, 1);

  await a.call('close');
  await b.call('close');
});

test('pipeline: a flush failure rolls the whole group back byte-for-byte', async () => {
  const dir = await freshDir();
  process.env.BUS_FLUSH_FAIL_AT = '1';
  let bus;
  try {
    bus = createBus({ path: dir, fsync: true });
    await assert.rejects(
      bus.publishBatch([{ i: 1 }, { i: 2 }, { i: 3 }]),
      /injected flush failure/,
    );
  } finally {
    delete process.env.BUS_FLUSH_FAIL_AT;
  }
  // State is exactly the pre-write state: no seq, no stats, no bytes.
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  assert.equal(bus.usage(), 0);
  const file = path.join(dir, 'bus.jsonl');
  if (existsSync(file)) assert.equal(readFileSync(file).length, 0);
  await bus.close();

  // Reopen: the failed group left no trace; the next group starts at seq 1.
  const reopened = createBus({ path: dir });
  const acks = await reopened.publishBatch([{ i: 1 }, { i: 2 }]);
  assert.deepEqual(acks.map((x) => x.seq), [1, 2]);
  assert.deepEqual((await reopened.replay(0)).map((m) => m.seq), [1, 2]);
  assert.equal(reopened.stats().published, 2);
  await reopened.close();
});

test('pipeline: a group seized mid-flush by a takeover is wholly invisible', async () => {
  const dir = await freshDir();
  const env = {
    BUS_LEASE_MS: '400',
    BUS_RENEW_MS: '40',
    BUS_DRAIN_STALL_MS: '1200',
  };
  const a = new Worker(dir, env);

  // 300 records flush past the first 256-line chunk, then the holder
  // freezes without a heartbeat: the next opener takes over mid group.
  const batchP = a.call(
    'publishBatch',
    { records: Array.from({ length: 300 }, (_, i) => ({ i })) },
  );

  await sleep(300);
  const b = new Worker(dir, { BUS_LEASE_MS: '400', BUS_RENEW_MS: '40' });
  // The new owner's sequence starts clean: the seized group never landed.
  const bAck = await b.call('publish', { record: { w: 'b' } });
  assert.equal(bAck.seq, 1);

  // The displaced holder's whole queued group fails as one; none of its
  // bytes mixed into the log.
  await assert.rejects(batchP, Error);

  assert.deepEqual((await b.call('replay', { from: 0 })).map((m) => m.seq), [1]);
  assert.equal((await b.call('stats')).published, 1);

  a.kill();
  await b.call('close');

  // Reopen: all-or-nothing held, no hole, no repeat.
  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1]);
  assert.deepEqual(got.map((m) => m.record), [{ w: 'b' }]);
  assert.equal(bus.stats().published, 1);
  assert.equal(bus.stats().seq, 1);
  const next = await bus.publish({ w: 'b2' });
  assert.equal(next.seq, 2);
  await bus.close();
});
