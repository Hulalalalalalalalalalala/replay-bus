import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { rm, mkdir, appendFile } from 'node:fs/promises';
import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { createBus } from '../src/index.js';

let dirs = [];

async function freshDir() {
  const dir = path.join(tmpdir(), `replay-bus-mw-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  dirs.push(dir);
  return dir;
}

afterEach(async () => {
  const all = dirs;
  dirs = [];
  await Promise.all(all.map((d) => rm(d, { recursive: true, force: true })));
});

const SRC_URL = new URL('../src/index.js', import.meta.url).href;

// Run a snippet in a real second process against the same bus directory.
function runChild(dir, body) {
  const script = `const { createBus } = await import(${JSON.stringify(SRC_URL)});\n${body}`;
  return execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    env: { ...process.env, BUS_DIR: dir },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

test('multi-writer: a second open takes ownership; the stale holder fails wholesale', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  await a.publish({ x: 1 });

  const b = createBus({ path: dir }); // takeover
  // Every mutating method of the old holder rejects with Error, no trace.
  await assert.rejects(a.publish({ x: 2 }), Error);
  await assert.rejects(a.publishBatch([{ x: 2 }]), Error);
  await assert.rejects(a.truncate(1), Error);
  await assert.rejects(a.compact(), Error);

  // The new holder writes on; the sequence has no hole and no duplicate.
  const ack = await b.publish({ x: 2 });
  assert.equal(ack.seq, 2);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1, 2]);
  assert.equal(b.stats().published, 2);

  // The stale holder still reads the merged state.
  assert.deepEqual((await a.replay(0)).map((m) => m.seq), [1, 2]);
  assert.equal(a.stats().published, 2);

  await b.close();
  await a.close(); // close stays idempotent for the stale holder
});

test('multi-writer: interleaved publishing across processes matches a single-process run', async () => {
  const dir = await freshDir();
  // Alternate ownership through close/reopen: the merged log must look
  // exactly like one process publishing 1..8 in order.
  const expected = [];
  for (let round = 0; round < 4; round++) {
    const bus = createBus({ path: dir });
    for (const n of [round * 2 + 1, round * 2 + 2]) {
      const ack = await bus.publish({ n });
      assert.equal(ack.seq, n);
      expected.push({ n });
    }
    await bus.close();
  }
  const bus = createBus({ path: dir });
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3, 4, 5, 6, 7, 8],
  );
  assert.deepEqual(
    got.map((m) => m.record),
    expected,
  );
  assert.equal(bus.stats().published, 8);
  await bus.close();
});

test('multi-writer: real second process publishes; stats and replay merge', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  await a.publish({ from: 'parent' });

  runChild(
    dir,
    `const bus = createBus({ path: process.env.BUS_DIR });
     await bus.publish({ from: 'child' });
     await bus.publish({ from: 'child' });
     await bus.close();`,
  );

  // The child took the lease: the parent's writes now fail with Error.
  await assert.rejects(a.publish({ from: 'parent-again' }), Error);
  // But its reads see the merged log, in order, with no gaps or duplicates.
  assert.deepEqual(
    (await a.replay(0)).map((m) => m.record.from),
    ['parent', 'child', 'child'],
  );
  assert.equal(a.stats().published, 3);
  assert.equal(a.stats().seq, 3);
  await a.close();

  // Reopen: the parent holds the lease again and continues the sequence.
  const b = createBus({ path: dir });
  const ack = await b.publish({ from: 'parent-2' });
  assert.equal(ack.seq, 4);
  await b.close();
});

test('multi-writer: dedup keys are shared across writers, takeover and restart', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  const first = await a.publish({ order: 'A', dedupKey: 'k1' });

  const b = createBus({ path: dir }); // takeover
  // Resend from another process: same acknowledgement, counted once.
  assert.deepEqual(await b.publish({ order: 'A-other', dedupKey: 'k1' }), first);
  assert.equal(b.stats().published, 1);
  // Batch resend from the other process resolves the same way.
  const batch = await b.publishBatch([{ order: 'A2', dedupKey: 'k1' }, { n: 1 }]);
  assert.deepEqual(batch[0], first);
  assert.equal(batch[1].seq, 2);
  assert.equal(b.stats().published, 2);
  await b.close();

  // Across a restart the first acknowledgement still wins.
  const c = createBus({ path: dir });
  assert.deepEqual(await c.publish({ order: 'A3', dedupKey: 'k1' }), first);
  assert.equal(c.stats().published, 2);
  await c.close();
  await a.close();
});

test('multi-writer: consumer positions are one shared state across processes', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  for (let i = 1; i <= 3; i++) await a.publish({ i });
  const b = createBus({ path: dir }); // b now holds the lease

  // An advance in one process is visible to the other.
  a.advance('c', 2);
  assert.equal(b.register('c'), 2);
  b.advance('c', 3);
  // Moving backwards against the merged position fails like single-process.
  assert.throws(() => a.advance('c', 1), RangeError);
  assert.equal(a.register('c'), 3);
  assert.deepEqual(a.read('c'), []);

  // A consumer first seen by the other process reads the full log.
  assert.deepEqual(b.read('d').map((m) => m.seq), [1, 2, 3]);
  assert.equal(a.register('d'), 0);

  await b.close();
  await a.close();
});

test('multi-writer: stats merge across writers and never shrink', async () => {
  const dir = await freshDir();
  const r = (n) => ({ n });
  const size = (n) => Buffer.byteLength(JSON.stringify(r(n)), 'utf8');
  const a = createBus({ path: dir });
  await a.publish(r(1));
  const b = createBus({ path: dir });
  await b.publish(r(2));
  await b.publish(r(3));
  await b.replay(0); // 3 replayed, counted in the shared log

  const expected = { seq: 3, bytes: size(1) + size(2) + size(3), published: 3, replayed: 3 };
  assert.deepEqual(a.stats(), expected);
  assert.deepEqual(b.stats(), expected);

  // Compaction by the holder reshapes the log; the merged counters stand.
  await b.compact();
  assert.deepEqual(a.stats(), expected);
  await b.close();
  await a.close();

  const reopened = createBus({ path: dir });
  assert.deepEqual(reopened.stats(), expected);
  await reopened.close();
});

test('multi-writer: quota is judged on merged occupancy', async () => {
  const dir = await freshDir();
  const record = { a: 1, dedupKey: 'k' };
  const size = Buffer.byteLength(JSON.stringify(record), 'utf8');
  const a = createBus({ path: dir, maxBytes: size });
  const first = await a.publish(record);
  assert.equal(a.usage(), size);

  const b = createBus({ path: dir, maxBytes: size });
  // Merged usage is already full: a new message rejects with RangeError
  // and leaves the shared state untouched.
  assert.equal(b.usage(), size);
  await assert.rejects(b.publish({ b: 2 }), RangeError);
  await assert.rejects(b.publishBatch([{ b: 2 }, { b: 3 }]), RangeError);
  assert.equal(b.usage(), size);
  assert.equal(b.stats().published, 1);
  // A resend of the shared key still succeeds and allocates nothing.
  assert.deepEqual(await b.publish({ other: true, dedupKey: 'k' }), first);

  await b.close();
  await a.close();
});

test('multi-writer: an expired credential rejects writes with Error', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  await a.publish({ x: 1 });

  // Simulate a takeover the bus did not perform itself: the lease on disk
  // no longer matches this instance's credential.
  writeFileSync(path.join(dir, 'bus.lease.json'), JSON.stringify({ v: 1, epoch: 99, owner: 'intruder' }) + '\n');
  await assert.rejects(a.publish({ x: 2 }), Error);
  await assert.rejects(a.truncate(1), Error);
  // Nothing was mixed into the log.
  const b = createBus({ path: dir });
  assert.equal(b.stats().published, 1);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1]);
  await b.close();

  // A deleted lease is equally fatal to writes.
  const c = createBus({ path: dir });
  await rm(path.join(dir, 'bus.lease.json'), { force: true });
  await assert.rejects(c.publish({ x: 2 }), Error);
  await c.close();
  await a.close();
});

test('multi-writer: a half-written lease and lease tmp are recovered on open', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  await a.publish({ x: 1 });
  await a.close();

  // A crashed predecessor left a torn lease and a lease tmp behind.
  writeFileSync(path.join(dir, 'bus.lease.json'), '{"v":1,"epoch":7,"own');
  writeFileSync(path.join(dir, 'bus.lease.tmp'), '{"v":1,"epo');

  const b = createBus({ path: dir });
  const ack = await b.publish({ x: 2 });
  assert.equal(ack.seq, 2);
  assert.equal(existsSync(path.join(dir, 'bus.lease.tmp')), false);
  // The new lease is valid and parseable.
  const lease = JSON.parse(readFileSync(path.join(dir, 'bus.lease.json'), 'utf8'));
  assert.equal(typeof lease.epoch, 'number');
  assert.equal(typeof lease.owner, 'string');
  await b.close();
});

test('multi-writer: reopen after a crashed predecessor keeps no-loss no-dup, no seq gaps', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  await a.publish({ i: 1 });
  await a.publishBatch([{ i: 2 }, { i: 3 }]);
  // Crash: no close, a torn trailing line and a dangling batch bracket.
  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'dead' }) + '\n');
  await appendFile(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":4,"id":"x",');

  const b = createBus({ path: dir }); // recovery + takeover
  assert.equal(b.stats().seq, 3);
  assert.equal(b.stats().published, 3);
  const ack = await b.publish({ i: 4 });
  assert.equal(ack.seq, 4);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  // The crashed holder cannot write any more.
  await assert.rejects(a.publish({ i: 9 }), Error);
  await b.close();
  await a.close();
});

test('multi-writer: truncate honours a consumer registered by another process', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await a.publish({ i });

  const b = createBus({ path: dir }); // b holds the lease now
  b.register('slow'); // position 0, registered through the new holder
  await a.close();

  // The merged position holds every segment: nothing is deleted.
  await b.truncate(5);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5]);
  assert.equal(readdirSync(dir).filter((n) => /^bus\.\d+\.jsonl$/.test(n)).length, 1);

  // Once the consumer passes, the segment goes — whole, and really gone.
  b.advance('slow', 5);
  await b.truncate(5);
  assert.deepEqual(await b.replay(0), []);
  assert.equal(readdirSync(dir).filter((n) => /^bus\.\d+\.jsonl$/.test(n)).length, 0);
  assert.equal(b.stats().published, 5); // cumulative stats never shrink
  await b.close();
});

test('multi-writer: compact by the holder folds another writer’s positions and dedup', async () => {
  const dir = await freshDir();
  const a = createBus({ path: dir });
  const first = await a.publish({ order: 'A', dedupKey: 'k1' });
  await a.publish({ order: 'B' });
  const b = createBus({ path: dir });
  b.advance('c', 1);
  await b.compact();

  // Everything the other process established survived the fold.
  assert.equal(b.register('c'), 1);
  assert.deepEqual(await b.publish({ order: 'A2', dedupKey: 'k1' }), first);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1, 2]);
  await b.close();

  // And it all survives a reopen too.
  const c = createBus({ path: dir });
  assert.equal(c.register('c'), 1);
  assert.deepEqual(await c.publish({ order: 'A3', dedupKey: 'k1' }), first);
  const ack = await c.publish({ order: 'C' });
  assert.equal(ack.seq, 3);
  await c.close();
  await a.close();
});

test('multi-writer: open fails synchronously with Error when the directory is not writable', async () => {
  if (process.getuid && process.getuid() === 0) {
    // Root ignores permission bits; nothing meaningful to assert.
    return;
  }
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.close();
  const { chmodSync } = await import('node:fs');
  chmodSync(dir, 0o555);
  try {
    assert.throws(() => createBus({ path: dir }), Error);
  } finally {
    chmodSync(dir, 0o755);
  }
});

test('multi-writer: writes fail with Error after the directory is deleted', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ x: 1 });
  await rm(dir, { recursive: true, force: true });
  await assert.rejects(bus.publish({ x: 2 }), Error);
  await assert.rejects(bus.truncate(1), Error);
});
