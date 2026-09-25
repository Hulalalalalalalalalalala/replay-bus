import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { rm, mkdir, appendFile } from 'node:fs/promises';
import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { createBus } from '../src/index.js';

let dirs = [];

async function freshDir() {
  const dir = path.join(tmpdir(), `replay-bus-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  dirs.push(dir);
  return dir;
}

beforeEach(() => {});

afterEach(async () => {
  const all = dirs;
  dirs = [];
  await Promise.all(all.map((d) => rm(d, { recursive: true, force: true })));
});

test('createBus returns bus synchronously and rejects bad path with TypeError', () => {
  assert.throws(() => createBus(), TypeError);
  assert.throws(() => createBus({}), TypeError);
  assert.throws(() => createBus({ path: undefined }), TypeError);
  assert.throws(() => createBus({ path: '' }), TypeError);
  assert.throws(() => createBus({ path: 123 }), TypeError);
  // Synchronous return, not a Promise.
  const dir = '/tmp/replay-bus-sync-check';
  const bus = createBus({ path: dir });
  assert.equal(typeof bus.publish, 'function');
  dirs.push(dir);
  return bus.close();
});

test('empty bus: stats zeros, replay empty, beyond-end empty', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  assert.deepEqual(await bus.replay(), []);
  assert.deepEqual(await bus.replay(0), []);
  assert.deepEqual(await bus.replay(5), []);
  assert.equal(bus.stats().replayed, 0);
  await bus.close();
});

test('publish: seq from 1 continuous, ids unique, bytes are compact UTF-8', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const r1 = { a: 1 };
  const r2 = { msg: '你好', nested: { x: [1, 2, 3] } };
  const a = await bus.publish(r1);
  const b = await bus.publish(r2);
  assert.deepEqual(a, { id: a.id, seq: 1 });
  assert.equal(b.seq, 2);
  assert.equal(typeof a.id, 'string');
  assert.notEqual(a.id, b.id);
  const expected =
    Buffer.byteLength(JSON.stringify(r1), 'utf8') +
    Buffer.byteLength(JSON.stringify(r2), 'utf8');
  assert.deepEqual(bus.stats(), {
    seq: 2,
    bytes: expected,
    published: 2,
    replayed: 0,
  });
  await bus.close();
});

test('replay is inclusive of start, ascending, record as-is', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = [{ v: 1 }, { v: 2, x: '你好' }, { v: 3 }];
  const acks = [];
  for (const r of records) acks.push(await bus.publish(r));

  let got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(got.map((m) => m.record), records);
  assert.deepEqual(got.map((m) => m.id), acks.map((a) => a.id));

  got = await bus.replay(2);
  assert.deepEqual(got.map((m) => m.seq), [2, 3]);
  assert.deepEqual(got[0].record, records[1]);

  got = await bus.replay(3);
  assert.deepEqual(got.map((m) => m.seq), [3]);

  got = await bus.replay(99);
  assert.deepEqual(got, []);

  // Each replay counts the messages it returned; empty replays count nothing.
  assert.equal(bus.stats().replayed, 3 + 2 + 1);
  await bus.close();
});

test('dedupKey: resend reuses first id/seq, no new message', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const first = await bus.publish({ order: 'A', dedupKey: 'k1' });
  const again = await bus.publish({ order: 'A-different-body', dedupKey: 'k1' });
  assert.deepEqual(again, first);
  assert.equal(bus.stats().published, 1);
  assert.equal(bus.stats().seq, 1);
  const got = await bus.replay(0);
  assert.equal(got.length, 1);
  assert.deepEqual(got[0].record, { order: 'A', dedupKey: 'k1' });
  await bus.close();
});

test('dedup serves an in-flight first publish and keys without dedupKey are distinct', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const p1 = bus.publish({ order: 'A', dedupKey: 'k1' });
  const p2 = bus.publish({ order: 'A', dedupKey: 'k1' });
  const [a, b] = await Promise.all([p1, p2]);
  assert.deepEqual(a, b);
  assert.equal(bus.stats().published, 1);

  const c = await bus.publish({ x: 1 });
  const d = await bus.publish({ x: 1 });
  assert.notEqual(c.id, d.id);
  assert.equal(c.seq, 2);
  assert.equal(d.seq, 3);
  await bus.close();
});

test('concurrent publishes take seq in invocation order', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const ps = [];
  for (let i = 0; i < 20; i++) {
    ps.push(bus.publish({ i }));
  }
  const acks = await Promise.all(ps);
  assert.deepEqual(
    acks.map((a) => a.seq),
    acks.map((_, i) => i + 1),
  );
  assert.equal(bus.stats().seq, 20);
  await bus.close();
});

test('survives reopen: seq, bytes, counters, dedup all continue', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  await bus.publish({ a: 1 });
  await bus.publish({ b: 2, dedupKey: 'ord-1' });
  await bus.replay(1); // 2 messages
  const before = bus.stats();
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);

  // New publish continues the sequence.
  const ack = await bus.publish({ c: 3 });
  assert.equal(ack.seq, 3);

  // Old dedup key still effective.
  const dup = await bus.publish({ whatever: true, dedupKey: 'ord-1' });
  assert.equal(dup.seq, 2);
  assert.equal(bus.stats().published, 3);

  // Replay sees the full history in order.
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  assert.equal(bus.stats().replayed, before.replayed + 3);
  await bus.close();
});

test('torn trailing line after crash is dropped, sequence resumes cleanly', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.publish({ a: 2 });
  await bus.close();

  // Simulate a partially durable write (no terminating newline).
  await appendFile(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":3,"id":"x",');

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 2);
  assert.equal(bus.stats().published, 2);
  const ack = await bus.publish({ a: 3 });
  assert.equal(ack.seq, 3);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  await bus.close();
});

test('publish rejects TypeError for non-object or non-JSON-safe records', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });

  for (const bad of [null, [1, 2], 'str', 42, true]) {
    await assert.rejects(bus.publish(bad), TypeError);
  }
  await assert.rejects(bus.publish({ dedupKey: 7 }), TypeError);
  await assert.rejects(bus.publish({ dedupKey: null }), TypeError);
  await assert.rejects(bus.publish({ n: NaN }), TypeError);
  await assert.rejects(bus.publish({ n: Infinity }), TypeError);
  await assert.rejects(bus.publish({ u: undefined }), TypeError);
  await assert.rejects(bus.publish({ f: () => 1 }), TypeError);
  await assert.rejects(bus.publish({ g: 10n }), TypeError);
  const circular = { a: 1 };
  circular.self = circular;
  await assert.rejects(bus.publish(circular), TypeError);
  const nested = { a: { b: {} } };
  nested.a.b.back = nested;
  await assert.rejects(bus.publish(nested), TypeError);

  // A shared (diamond, acyclic) reference is allowed.
  const child = { x: 1 };
  await bus.publish({ a: child, b: child });

  // Rejected publishes consume no sequence number.
  assert.equal(bus.stats().seq, 1);
  const ack = await bus.publish({ ok: true });
  assert.equal(ack.seq, 2);
  await bus.close();
});

test('replay rejects RangeError for invalid from', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (const bad of [-1, 1.5, NaN, '1', {}, null, true]) {
    await assert.rejects(bus.replay(bad), RangeError);
  }
  await bus.close();
});

test('close is idempotent; publish/replay after close reject Error; stats still work', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.close();
  await bus.close(); // repeatable
  assert.equal(bus.stats().seq, 1); // still readable
  await assert.rejects(bus.publish({ a: 2 }), Error);
  await assert.rejects(bus.replay(0), Error);
});

test('operations queued before close are rejected and nothing is half-written', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const p = bus.publish({ a: 1 });
  const closePromise = bus.close();
  await assert.rejects(p, Error);
  await closePromise;

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().published, 0);
  const ack = await reopened.publish({ a: 1 });
  assert.equal(ack.seq, 1);
  await reopened.close();
});

// ---- consumer positions ----------------------------------------------------

test('register: first position zero, repeat register keeps current value', () => {
  const dir = '/tmp/replay-bus-register-check';
  const bus = createBus({ path: dir });
  dirs.push(dir);
  assert.equal(bus.register('alice'), 0);
  assert.equal(bus.register('alice'), 0);
  bus.advance('alice', 2);
  assert.equal(bus.register('alice'), 2);
  return bus.close();
});

test('read starts after position, read does not advance, beyond end is empty', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ v: 1 });
  await bus.publish({ v: 2 });
  await bus.publish({ v: 3 });

  assert.deepEqual(bus.read('bob').map((m) => m.seq), [1, 2, 3]);
  // Position zero: nothing consumed yet; read again returns the same batch.
  assert.deepEqual(bus.read('bob').map((m) => m.seq), [1, 2, 3]);

  bus.advance('bob', 2);
  assert.deepEqual(bus.read('bob').map((m) => m.seq), [3]);
  bus.advance('bob', 3);
  assert.deepEqual(bus.read('bob'), []);
  await bus.close();
});

test('read results have the replay shape: { seq, id, record }', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const r = { msg: '你好' };
  const ack = await bus.publish(r);
  const [got] = bus.read('c');
  assert.deepEqual(got, { seq: 1, id: ack.id, record: r });
  await bus.close();
});

test('unknown names auto-register in advance and read exactly like register', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ v: 1 });
  bus.advance('auto1', 1);
  assert.equal(bus.register('auto1'), 1);
  assert.deepEqual(bus.read('auto2').map((m) => m.seq), [1]);
  assert.equal(bus.register('auto2'), 0);
  await bus.close();
});

test('positions survive reopen and publishing then reading still works', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  await bus.publish({ v: 1 });
  await bus.publish({ v: 2 });
  bus.register('keeper');
  bus.advance('keeper', 1);
  bus.read('reader');
  await bus.close();

  bus = createBus({ path: dir });
  assert.equal(bus.register('keeper'), 1);
  assert.equal(bus.register('reader'), 0);
  assert.deepEqual(bus.read('keeper').map((m) => m.seq), [2]);
  await bus.publish({ v: 3 });
  bus.advance('keeper', 2);
  assert.deepEqual(bus.read('keeper').map((m) => m.seq), [3]);
  await bus.close();
});

test('register/advance/read throw TypeError synchronously for bad names', () => {
  const dir = '/tmp/replay-bus-name-check';
  const bus = createBus({ path: dir });
  dirs.push(dir);
  for (const bad of ['', 1, null, undefined, {}, [], true]) {
    assert.throws(() => bus.register(bad), TypeError);
    assert.throws(() => bus.advance(bad, 0), TypeError);
    assert.throws(() => bus.read(bad), TypeError);
  }
  return bus.close();
});

test('advance throws RangeError for non-non-negative-integer and backwards moves', () => {
  const dir = '/tmp/replay-bus-advance-check';
  const bus = createBus({ path: dir });
  dirs.push(dir);
  bus.register('n');
  for (const bad of [-1, 1.5, NaN, '1', {}, null, true, 2n]) {
    assert.throws(() => bus.advance('n', bad), RangeError);
  }
  bus.advance('n', 3);
  assert.throws(() => bus.advance('n', 2), RangeError);
  // Setting to the current value is a success, not an error.
  assert.equal(bus.advance('n', 3), 3);
  return bus.close();
});

test('register/advance/read reject synchronously after close; close is repeatable', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  bus.register('n');
  await bus.close();
  await bus.close();
  assert.throws(() => bus.register('x'), Error);
  assert.throws(() => bus.advance('n', 1), Error);
  assert.throws(() => bus.read('n'), Error);
});

// ---- readRange --------------------------------------------------------------

test('readRange: inclusive of start, ascending, capped at limit, fewer at the tail', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = [];
  for (let i = 1; i <= 10; i++) {
    records.push({ i, t: '你好' });
    await bus.publish(records[i - 1]);
  }
  assert.deepEqual(bus.readRange(0, 3).map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(bus.readRange(1, 3).map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(bus.readRange(4, 2).map((m) => m.seq), [4, 5]);
  // Fewer than limit at the tail.
  assert.deepEqual(bus.readRange(9, 10).map((m) => m.seq), [9, 10]);
  // Walking the whole log page by page reassembles it, ascending, no gaps.
  const pages = [];
  for (let start = 1; ; ) {
    const page = bus.readRange(start, 3);
    if (page.length === 0) break;
    pages.push(...page);
    start = page[page.length - 1].seq + 1;
  }
  assert.deepEqual(pages.map((m) => m.seq), records.map((_, i) => i + 1));
  assert.deepEqual(pages.map((m) => m.record), records);
  // Past the end.
  assert.deepEqual(bus.readRange(11, 5), []);
  await bus.close();
});

test('readRange: record shape matches replay/read', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const ack = await bus.publish({ msg: '你好' });
  const replayGot = await bus.replay(0);
  assert.deepEqual(bus.readRange(1, 1), replayGot);
  assert.deepEqual(bus.readRange(1, 1)[0], { seq: 1, id: ack.id, record: { msg: '你好' } });
  await bus.close();
});

test('readRange: throws TypeError synchronously for bad start or limit', () => {
  const dir = '/tmp/replay-bus-readrange-args';
  const bus = createBus({ path: dir });
  dirs.push(dir);
  for (const bad of [-1, 1.5, NaN, '1', {}, null, true, undefined, 2n]) {
    assert.throws(() => bus.readRange(bad, 1), TypeError);
  }
  for (const bad of [0, -1, 1.5, NaN, '2', {}, null, true, undefined, 2n]) {
    assert.throws(() => bus.readRange(0, bad), TypeError);
  }
  return bus.close();
});

test('readRange: no side effects — stats, positions, replayed unchanged; repeat identical', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  bus.register('c');
  bus.advance('c', 2);
  const statsBefore = bus.stats();
  const posBefore = bus.register('c');
  const first = JSON.stringify(bus.readRange(1, 3));
  for (let k = 0; k < 5; k++) {
    assert.equal(JSON.stringify(bus.readRange(1, 3)), first);
  }
  assert.deepEqual(bus.stats(), statsBefore);
  assert.equal(bus.stats().replayed, 0);
  assert.equal(bus.register('c'), posBefore);
  assert.deepEqual(bus.read('c').map((m) => m.seq), [3, 4, 5]);
  await bus.close();
});

test('readRange: stays readable after close with the last view', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.close();
  assert.deepEqual(bus.readRange(0, 10).map((m) => m.seq), [1, 2, 3, 4]);
  assert.deepEqual(bus.readRange(3, 10).map((m) => m.seq), [3, 4]);
  assert.deepEqual(bus.readRange(99, 10), []);
});

test('readRange: survives compaction unchanged; pages snapshot and live ranges', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 8; i++) await bus.publish({ i });
  const before = bus.readRange(0, 100).map((m) => m.seq);
  await bus.compact();
  assert.deepEqual(bus.readRange(0, 100).map((m) => m.seq), before);
  for (let i = 9; i <= 12; i++) await bus.publish({ i });
  // Crosses the snapshot/live boundary inside one page.
  assert.deepEqual(bus.readRange(6, 4).map((m) => m.seq), [6, 7, 8, 9]);
  assert.deepEqual(bus.readRange(0, 100).map((m) => m.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
  await bus.close();
});

test('readRange: start in a deleted range clamps to earliest survivor; wholly deleted is empty', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 10; i++) await bus.publish({ i });
  // No consumers: first truncate rolls the straddled segment, second drops it.
  await bus.truncate(5);
  await bus.truncate(10);
  assert.deepEqual(bus.readRange(0, 100), []);
  for (let i = 11; i <= 13; i++) await bus.publish({ i });
  // A start inside the deleted 1..10 gap begins at the earliest survivor.
  assert.deepEqual(bus.readRange(1, 2).map((m) => m.seq), [11, 12]);
  assert.deepEqual(bus.readRange(10, 1).map((m) => m.seq), [11]);
  assert.deepEqual(bus.readRange(12, 10).map((m) => m.seq), [12, 13]);
  // A wholly deleted single-seq window never throws and yields the survivor.
  assert.equal(bus.readRange(5, 1).length, 1);
  assert.equal(bus.readRange(5, 1)[0].seq, 11);
  // Records and ids are intact.
  assert.deepEqual(bus.readRange(11, 1)[0].record, { i: 11 });
  await bus.close();
});

test('readRange: serves concurrently with continuous writers and always shows a committed prefix', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const N = 400;
  // Background continuous writer.
  const writes = (async () => {
    for (let i = 1; i <= N; i++) {
      await bus.publish({ i });
    }
  })();
  // Read on every tick while writes commit; each page is ascending with no
  // repeats, and the visible set is always a committed prefix.
  let lastSeenMax = 0;
  const tick = () => new Promise((resolve) => setImmediate(resolve));
  while (lastSeenMax < N) {
    await tick();
    // Page forward from the last observed seq so progress always advances.
    const page = bus.readRange(lastSeenMax + 1, 64);
    if (page.length > 0) {
      for (let k = 1; k < page.length; k++) assert.equal(page[k].seq, page[k - 1].seq + 1);
      lastSeenMax = page[page.length - 1].seq;
    }
  }
  await writes;
  assert.deepEqual(
    bus.readRange(1, N).map((m) => m.seq),
    Array.from({ length: N }, (_, i) => i + 1),
  );
  await bus.close();
});

// ---- compact ---------------------------------------------------------------

test('compact: replay of the same range before and after is identical', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const records = [];
  for (let i = 1; i <= 10; i++) {
    records.push({ i, t: '你好' });
    await bus.publish(records[i - 1]);
  }
  const before = await bus.replay(0);
  const before3 = await bus.replay(3);
  const statsBefore = bus.stats();
  await bus.compact();

  assert.deepEqual(await bus.replay(0), before);
  assert.deepEqual(await bus.replay(3), before3);
  assert.deepEqual(await bus.replay(7), before.slice(6));
  assert.deepEqual(await bus.replay(99), []);
  // replayed is the only stat that legitimately moved (each replay counts).
  assert.equal(bus.stats().seq, statsBefore.seq);
  assert.equal(bus.stats().bytes, statsBefore.bytes);
  assert.equal(bus.stats().published, statsBefore.published);

  // seq continues from where it was.
  const ack = await bus.publish({ i: 11 });
  assert.equal(ack.seq, 11);
  const all = await bus.replay(0);
  assert.deepEqual(all.map((m) => m.seq), records.map((_, i) => i + 1).concat(11));
  assert.deepEqual(all[4].record, records[4]);
  assert.deepEqual(all[10].record, { i: 11 });
  await bus.close();
});

test('compact keeps dedup across compaction and reopen', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const first = await bus.publish({ order: 'A', dedupKey: 'k1' });
  await bus.publish({ other: 1 });
  await bus.compact();
  // Resend after compaction still returns the first acknowledgement.
  assert.deepEqual(await bus.publish({ order: 'B', dedupKey: 'k1' }), first);
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(await bus.publish({ order: 'C', dedupKey: 'k1' }), first);
  assert.equal(bus.stats().published, 2);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2]);
  await bus.close();
});

test('compact preserves consumer positions', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  bus.advance('slow', 2);
  bus.register('fresh');
  await bus.compact();
  assert.equal(bus.register('slow'), 2);
  assert.deepEqual(bus.read('slow').map((m) => m.seq), [3, 4, 5]);
  assert.deepEqual(bus.read('fresh').map((m) => m.seq), [1, 2, 3, 4, 5]);
  await bus.close();
});

test('multiple compacts and interleaved publishes stay consistent', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.compact();
  for (let i = 5; i <= 8; i++) await bus.publish({ i });
  await bus.compact();
  for (let i = 9; i <= 12; i++) await bus.publish({ i });
  await bus.compact();
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    Array.from({ length: 12 }, (_, i) => ({ i: i + 1 })),
  );
  assert.deepEqual(
    got.map((m) => m.seq),
    Array.from({ length: 12 }, (_, i) => i + 1),
  );
  await bus.close();
});

test('compact concurrent with publishes/replays/positions serializes cleanly', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [];
  for (let i = 1; i <= 30; i++) {
    jobs.push(bus.publish({ i }));
    if (i % 5 === 0) jobs.push(bus.compact());
    if (i % 7 === 0) jobs.push(bus.replay(0));
    if (i % 10 === 0) {
      // synchronous position work interleaved on the event loop
      bus.register('c');
      bus.advance('c', i);
    }
  }
  await Promise.all(jobs);
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    Array.from({ length: 30 }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, 30);
  assert.equal(bus.register('c'), 30);
  await bus.close();
});

test('reopen after a completed compact restores everything', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 6; i++) await bus.publish({ i, dedupKey: `k${i}` });
  bus.advance('p', 4);
  await bus.replay(2);
  const before = bus.stats();
  await bus.compact();
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);
  assert.equal(bus.register('p'), 4);
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    Array.from({ length: 6 }, (_, i) => ({ i: i + 1, dedupKey: `k${i + 1}` })),
  );
  assert.deepEqual(await bus.publish({ x: 1, dedupKey: 'k2' }), {
    id: got[1].id,
    seq: 2,
  });
  const ack = await bus.publish({ i: 7 });
  assert.equal(ack.seq, 7);
  await bus.close();
});

test('crash recovery: half snapshot tmp is discarded with the old log intact', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 3; i++) await bus.publish({ i });
  await bus.close();

  // Compact crashed during snapshot write: tmp exists, snapshot/log untouched.
  writeFileSync(path.join(dir, 'bus.snapshot.tmp'), '{"gen":1,"half');
  const raw = readFileSync(path.join(dir, 'bus.jsonl'));

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().seq, 3);
  // Recovery only deleted the half snapshot; the log itself is untouched.
  assert.equal(readFileSync(path.join(dir, 'bus.jsonl')).equals(raw), true);
  assert.deepEqual((await reopened.replay(0)).map((m) => m.seq), [1, 2, 3]);
  await reopened.compact();
  assert.deepEqual((await reopened.replay(0)).map((m) => m.seq), [1, 2, 3]);
  await reopened.close();
});

test('crash recovery: snapshot renamed but log not yet truncated does not double-apply', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  const acks = [];
  for (let i = 1; i <= 3; i++) acks.push(await bus.publish({ i, dedupKey: `k${i}` }));
  bus.advance('g', 2);
  const statsBefore = bus.stats();
  await bus.close();

  // Simulate the crash window after snapshot rename but before log
  // truncation: build the snapshot exactly as compact would have, but leave
  // the full pre-compact log (no marker) in place.
  const { entries } = (() => {
    const raw = readFileSync(path.join(dir, 'bus.jsonl'), 'utf8');
    return { entries: raw.trim().split('\n').map((l) => JSON.parse(l)) };
  })();
  const snapshot = {
    v: 1,
    gen: 1,
    seq: statsBefore.seq,
    bytes: statsBefore.bytes,
    published: statsBefore.published,
    replayed: statsBefore.replayed,
    positions: { g: 2 },
    dedup: entries
      .filter((e) => e.t === 'm' && typeof e.d === 'string')
      .map((e) => [e.d, { id: e.id, seq: e.seq }]),
    messages: entries
      .filter((e) => e.t === 'm')
      .map((e) => ({ seq: e.seq, id: e.id, record: e.record })),
  };
  writeFileSync(path.join(dir, 'bus.snapshot.json'), JSON.stringify(snapshot));

  bus = createBus({ path: dir });
  // Folded entries must not be counted twice.
  assert.deepEqual(bus.stats(), statsBefore);
  assert.equal(bus.register('g'), 2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3]);
  // Dedup survived the folded log.
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'k1' }), acks[0]);
  // Sequence continues cleanly on top of the snapshot.
  const ack = await bus.publish({ i: 4 });
  assert.equal(ack.seq, 4);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  await bus.close();
});

test('crash recovery: torn trailing log line after compact is dropped', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 3; i++) await bus.publish({ i });
  await bus.compact();
  await bus.publish({ i: 4 });
  await bus.close();

  await appendFile(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":5,"id":"x",');

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 4);
  const ack = await bus.publish({ i: 5 });
  assert.equal(ack.seq, 5);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5]);
  await bus.close();
});

test('stats are unchanged by compact: bytes and published never shrink', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ big: '你好'.repeat(i), dedupKey: `d${i}` });
  // Duplicates never counted.
  await bus.publish({ big: 'ignored', dedupKey: 'd2' });
  const before = bus.stats();
  await bus.compact();
  assert.deepEqual(bus.stats(), before);
  await bus.compact();
  assert.deepEqual(bus.stats(), before);
  await bus.close();

  const reopened = createBus({ path: dir });
  assert.deepEqual(reopened.stats(), before);
  await reopened.close();
});

test('compact after close rejects Error', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.close();
  await assert.rejects(bus.compact(), Error);
});

test('default export is removed; only named createBus is exported', async () => {
  const mod = await import('../src/index.js');
  assert.equal(typeof mod.createBus, 'function');
  assert.equal(mod.default, undefined);
});

// ---- publishBatch ----------------------------------------------------------

test('publishBatch: empty array resolves to [] and changes no state', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const acks = await bus.publishBatch([]);
  assert.deepEqual(acks, []);
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  assert.deepEqual(await bus.replay(0), []);
  await bus.close();
});

test('publishBatch: non-array argument throws TypeError synchronously', () => {
  const dir = '/tmp/replay-bus-batch-arg-check';
  const bus = createBus({ path: dir });
  dirs.push(dir);
  for (const bad of [null, undefined, {}, 'x', 42, true, { length: 0 }]) {
    assert.throws(() => bus.publishBatch(bad), TypeError);
  }
  return bus.close();
});

test('publishBatch: acks in input order, effective seqs continuous, shape matches publish', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = [{ a: 1 }, { msg: '你好' }, { a: 3 }];
  const acks = await bus.publishBatch(records);
  assert.equal(Array.isArray(acks), true);
  assert.equal(acks.length, 3);
  assert.deepEqual(
    acks.map((a) => a.seq),
    [1, 2, 3],
  );
  for (const a of acks) {
    assert.equal(typeof a.id, 'string');
    assert.deepEqual(Object.keys(a).sort(), ['id', 'seq']);
  }
  const expectedBytes = records.reduce((n, r) => n + Buffer.byteLength(JSON.stringify(r), 'utf8'), 0);
  assert.deepEqual(bus.stats(), { seq: 3, bytes: expectedBytes, published: 3, replayed: 0 });

  // Seqs continue after the group, interleaved with single publishes.
  const single = await bus.publish({ a: 4 });
  assert.equal(single.seq, 4);
  const more = await bus.publishBatch([{ a: 5 }, { a: 6 }]);
  assert.deepEqual(
    more.map((a) => a.seq),
    [5, 6],
  );
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3, 4, 5, 6],
  );
  assert.deepEqual(
    got.map((m) => m.record),
    records.concat([{ a: 4 }, { a: 5 }, { a: 6 }]),
  );
  await bus.close();
});

test('publishBatch: large group lands in one shot with continuous seqs', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const N = 600; // exceeds the internal writev chunk size
  const records = Array.from({ length: N }, (_, i) => ({ i, pad: '你好'.repeat(i % 5) }));
  const acks = await bus.publishBatch(records);
  assert.deepEqual(
    acks.map((a) => a.seq),
    records.map((_, i) => i + 1),
  );
  assert.equal(new Set(acks.map((a) => a.id)).size, N);
  assert.equal(bus.stats().published, N);
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    records,
  );
  await bus.close();
});

test('publishBatch: any invalid item rejects the whole group and leaves state untouched', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ before: 1, dedupKey: 'hist' });

  const goodStats = bus.stats();
  const goodRecords = [
    { v: 1 },
    { v: 2, dedupKey: 'g1' },
    { v: 3, dedupKey: 'g1' }, // in-group duplicate, still valid
    { v: 4, dedupKey: 'g2' },
  ];

  const badVariants = [
    null,
    [1, 2],
    'str',
    42,
    true,
    { dedupKey: 7 },
    { n: NaN },
    { n: Infinity },
    { u: undefined },
    { f: () => 1 },
    { g: 10n },
  ];
  for (const bad of badVariants) {
    await assert.rejects(bus.publishBatch([{ ok: 1 }, bad, { ok: 2 }]), TypeError);
    assert.deepEqual(bus.stats(), goodStats);
  }
  // Circular reference anywhere in the group rejects it wholesale.
  const circular = { a: 1 };
  circular.self = circular;
  await assert.rejects(bus.publishBatch([{ ok: 1 }, circular]), TypeError);
  assert.deepEqual(bus.stats(), goodStats);

  // Nothing from the rejected groups is visible.
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1]);

  // Keys mentioned only in rejected groups are still free.
  const fresh = await bus.publish({ v: 2, dedupKey: 'g1' });
  assert.equal(fresh.seq, 2);
  const hist = await bus.publish({ whatever: 1, dedupKey: 'hist' });
  assert.deepEqual(hist, { id: hist.id, seq: 1 });
  assert.equal(bus.stats().published, 2);
  await bus.close();
});

test('publishBatch: in-group duplicate keys reuse the first ack and allocate no new seq', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const acks = await bus.publishBatch([
    { order: 'A', dedupKey: 'k1' },
    { order: 'B' },
    { order: 'A2', dedupKey: 'k1' }, // duplicate of index 0
    { order: 'C', dedupKey: 'k2' },
    { order: 'C2', dedupKey: 'k2' }, // duplicate of index 3
  ]);
  assert.equal(acks.length, 5);
  assert.deepEqual(acks[2], acks[0]);
  assert.deepEqual(acks[4], acks[3]);
  assert.deepEqual(
    acks.map((a) => a.seq),
    [1, 2, 1, 3, 3],
  );
  assert.equal(bus.stats().published, 3);
  assert.equal(bus.stats().seq, 3);
  // Only the first occurrence of each key is stored.
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(got[0].record, { order: 'A', dedupKey: 'k1' });
  assert.deepEqual(got[2].record, { order: 'C', dedupKey: 'k2' });

  // A later single and batch reuse the same acks.
  assert.deepEqual(await bus.publish({ x: 1, dedupKey: 'k1' }), acks[0]);
  const next = await bus.publishBatch([{ x: 2, dedupKey: 'k2' }, { x: 3 }]);
  assert.deepEqual(next[0], acks[3]);
  assert.equal(next[1].seq, 4);
  assert.equal(bus.stats().published, 4);
  await bus.close();
});

test('publishBatch: keys overlapping history (single or earlier group) reuse the historical ack', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const firstSingle = await bus.publish({ order: 'A', dedupKey: 'h1' });
  const firstGroup = await bus.publishBatch([
    { order: 'B', dedupKey: 'b1' },
    { order: 'C' },
  ]);

  const acks = await bus.publishBatch([
    { order: 'X', dedupKey: 'h1' },
    { order: 'Y', dedupKey: 'b1' },
    { order: 'Z' },
    { order: 'W', dedupKey: 'h1' },
  ]);
  assert.deepEqual(acks[0], firstSingle);
  assert.deepEqual(acks[1], firstGroup[0]);
  assert.equal(acks[2].seq, 4); // only one new message
  assert.deepEqual(acks[3], firstSingle);
  assert.equal(bus.stats().published, 4);
  assert.equal(bus.stats().seq, 4);

  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3, 4],
  );
  await bus.close();

  // Dedup survives restart for batch-originated keys too.
  const reopened = createBus({ path: dir });
  assert.deepEqual(await reopened.publishBatch([{ q: 1, dedupKey: 'b1' }]), [firstGroup[0]]);
  assert.deepEqual(await reopened.publish({ q: 2, dedupKey: 'h1' }), firstSingle);
  assert.equal(reopened.stats().published, 4);
  await reopened.close();
});

test('publishBatch: a concurrent single with a group-reserved key shares the group ack', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const groupP = bus.publishBatch([
    { order: 'A', dedupKey: 'race' },
    { order: 'B' },
  ]);
  const singleP = bus.publish({ order: 'A-late' , dedupKey: 'race' });
  const [groupAcks, singleAck] = await Promise.all([groupP, singleP]);
  assert.deepEqual(singleAck, groupAcks[0]);
  assert.equal(bus.stats().published, 2);
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.record),
    [{ order: 'A', dedupKey: 'race' }, { order: 'B' }],
  );
  await bus.close();
});

test('publishBatch: invocation order is honoured against concurrent singles and groups', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [
    bus.publish({ x: 's1' }),
    bus.publishBatch([{ x: 'b1' }, { x: 'b2' }]),
    bus.publish({ x: 's2' }),
    bus.publishBatch([{ x: 'b3' }]),
  ];
  const results = await Promise.all(jobs);
  assert.equal(results[0].seq, 1);
  assert.deepEqual(results[1].map((a) => a.seq), [2, 3]);
  assert.equal(results[2].seq, 4);
  assert.deepEqual(results[3].map((a) => a.seq), [5]);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.record),
    [{ x: 's1' }, { x: 'b1' }, { x: 'b2' }, { x: 's2' }, { x: 'b3' }],
  );
  await bus.close();
});

test('publishBatch crash: committed group survives, dangling begin/group tail is severed', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const good = await bus.publishBatch([{ i: 1 }, { i: 2, dedupKey: 'k1' }]);
  await bus.close();

  // Crash mid second group: begin + one full message line + one torn line,
  // no commit line.
  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'dead' }) + '\n');
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 3, id: 'z', bytes: 1, record: { i: 3 } }) + '\n',
  );
  await appendFile(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":4,"id":"y",');

  bus = createBus({ path: dir });
  const expectedBytes =
    Buffer.byteLength(JSON.stringify({ i: 1 }), 'utf8') +
    Buffer.byteLength(JSON.stringify({ i: 2, dedupKey: 'k1' }), 'utf8');
  assert.deepEqual(bus.stats(), {
    seq: 2,
    bytes: expectedBytes,
    published: 2,
    replayed: 0,
  });
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2],
  );
  // The severed tail was physically removed; the next write starts clean.
  const ack = await bus.publish({ i: 3 });
  assert.equal(ack.seq, 3);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3],
  );
  // Dedup from the committed group still effective; the dead group's key
  // (had one existed) would be free.
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'k1' }), good[1]);
  await bus.close();
});

test('publishBatch crash: begin alone and commit-less groups vanish even without prior data', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  await bus.close();

  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'g1' }) + '\n');
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 1, id: 'z', bytes: 1, record: { i: 1 } }) + '\n',
  );

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 0);
  assert.equal(bus.stats().published, 0);
  assert.deepEqual(await bus.replay(0), []);
  const ack = await bus.publishBatch([{ i: 1 }]);
  assert.equal(ack[0].seq, 1);
  await bus.close();
});

test('publishBatch crash: full group + half group keeps only the full group', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  await bus.publishBatch([{ i: 1 }, { i: 2 }]);
  await bus.close();

  const begin = JSON.stringify({ t: 'b', id: 'dead2' }) + '\n';
  const msg = JSON.stringify({ t: 'm', seq: 3, id: 'z', bytes: 1, record: { i: 3 } }) + '\n';
  await appendFile(path.join(dir, 'bus.jsonl'), begin + msg);

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 2);
  assert.equal(bus.stats().published, 2);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2],
  );
  const next = await bus.publishBatch([{ i: 3 }, { i: 4 }]);
  assert.deepEqual(
    next.map((a) => a.seq),
    [3, 4],
  );
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4],
  );
  await bus.close();
});

test('publishBatch crash: dangling group after a compact is severed too', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  await bus.publishBatch([{ i: 1 }, { i: 2 }]);
  await bus.compact();
  const live = await bus.publishBatch([{ i: 3 }, { i: 4 }]);
  await bus.close();

  // Crash while a third group is half-written, on a marker-led post-compact log.
  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'dead3' }) + '\n');
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 5, id: 'z', bytes: 1, record: { i: 5 } }) + '\n',
  );

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 4);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4],
  );
  const next = await bus.publishBatch([{ i: 5 }]);
  assert.equal(next[0].seq, 5);
  assert.deepEqual(live[1], { id: live[1].id, seq: 4 });
  await bus.compact();
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );
  await bus.close();
});

test('publishBatch: concurrent with replay/positions/compact; replay identical around compact', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [];
  for (let i = 1; i <= 24; i++) {
    if (i % 4 === 0) {
      jobs.push(
        bus.publishBatch([
          { i, dedupKey: `k${i}` },
          { i: i + 0.5, dup: true, dedupKey: `k${i}` },
        ]),
      );
    } else {
      jobs.push(bus.publish({ i }));
    }
    if (i % 6 === 0) jobs.push(bus.compact());
    if (i % 5 === 0) jobs.push(bus.replay(0));
    if (i % 10 === 0) {
      bus.register('c');
      bus.advance('c', i);
    }
  }
  await Promise.all(jobs);

  // 24 logical messages: 18 singles + 6 effective batch messages.
  assert.equal(bus.stats().published, 24);
  const got = await bus.replay(0);
  const expectedSeqs = Array.from({ length: 24 }, (_, i) => i + 1);
  assert.deepEqual(
    got.map((m) => m.seq),
    expectedSeqs,
  );
  assert.equal(bus.register('c'), 20);

  await bus.compact();
  const after = await bus.replay(0);
  assert.deepEqual(after, got);
  assert.deepEqual(
    (await bus.replay(7)).map((m) => m.seq),
    expectedSeqs.slice(6),
  );
  // Dedup still resolves after compaction.
  const sample = await bus.publish({ x: 1, dedupKey: 'k8' });
  assert.equal(sample.seq, 8);
  // Next new seq continues.
  assert.equal((await bus.publish({ fresh: true })).seq, 25);
  await bus.close();

  // Reopen after compaction: no loss, no duplication, stats intact.
  const reopened = createBus({ path: dir });
  const reopenedGot = await reopened.replay(0);
  assert.deepEqual(reopenedGot.map((m) => m.seq), expectedSeqs.concat(25));
  assert.equal(reopened.stats().published, 25);
  assert.deepEqual(await reopened.publish({ x: 2, dedupKey: 'k12' }), {
    id: reopenedGot.find((m) => m.seq === 12).id,
    seq: 12,
  });
  await reopened.close();
});

test('publishBatch after close rejects Error; close stays idempotent', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publishBatch([{ a: 1 }]);
  await bus.close();
  await bus.close();
  await assert.rejects(bus.publishBatch([{ a: 2 }]), Error);
  // Non-array is still a synchronous TypeError even when closed.
  assert.throws(() => bus.publishBatch(null), TypeError);
  assert.equal(bus.stats().published, 1);
});

test('publishBatch queued before close is rejected with no durable trace', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  // All three calls land synchronously; the chain job only runs on a later
  // microtask, by which point the bus is closed — same rule as publish.
  const batchP = bus.publishBatch([{ a: 2 }, { a: 3 }]);
  const closeP = bus.close();
  await assert.rejects(batchP, Error);
  await closeP;

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().published, 0);
  assert.deepEqual(await reopened.replay(0), []);
  const acks = await reopened.publishBatch([{ a: 2 }, { a: 3 }]);
  assert.deepEqual(
    acks.map((a) => a.seq),
    [1, 2],
  );
  await reopened.close();
});

// ---- truncate ---------------------------------------------------------------

const segmentFiles = (dir) => readdirSync(dir).filter((n) => /^bus\.\d+\.jsonl$/.test(n));

test('truncate: invalid bound throws TypeError synchronously, even after close', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (const bad of [-1, 1.5, NaN, '1', {}, null, true, undefined, 2n]) {
    assert.throws(() => bus.truncate(bad), TypeError);
  }
  await bus.close();
  // Still a synchronous TypeError for the shape, a rejected Error for closed.
  assert.throws(() => bus.truncate(-1), TypeError);
  await assert.rejects(bus.truncate(1), Error);
  await assert.rejects(bus.truncate(0), Error);
});

test('truncate: a zero bound is a no-op and rolls no segment', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.truncate(0);
  await bus.publish({ v: 1 });
  await bus.truncate(0);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1]);
  assert.deepEqual(bus.stats(), {
    seq: 1,
    bytes: Buffer.byteLength(JSON.stringify({ v: 1 }), 'utf8'),
    published: 1,
    replayed: 1,
  });
  // No segment file was ever created.
  assert.deepEqual(segmentFiles(dir), []);
  await bus.close();
});

test('truncate: a straddled segment is kept whole; a bound past the end acts as the end', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 10; i++) await bus.publish({ i });
  const statsBefore = bus.stats();

  // The whole history sits in one segment; truncating into the middle of it
  // must not delete half of it. The active segment is rolled, though.
  await bus.truncate(4);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.equal(segmentFiles(dir).length, 1);

  // No consumer is registered, so a bound past the end drops every segment.
  await bus.truncate(99);
  assert.deepEqual(await bus.replay(0), []);
  assert.deepEqual(segmentFiles(dir), []);
  // Stats keep their cumulative meaning: nothing shrinks.
  assert.equal(bus.stats().seq, statsBefore.seq);
  assert.equal(bus.stats().bytes, statsBefore.bytes);
  assert.equal(bus.stats().published, statsBefore.published);

  // The sequence continues where it was.
  const ack = await bus.publish({ i: 11 });
  assert.equal(ack.seq, 11);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [11]);
  await bus.close();
});

test('truncate: a segment is deleted only once every registered consumer has passed it', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  bus.register('slow'); // position 0: holds every segment
  bus.advance('fast', 5);
  await bus.truncate(5); // rolls, but 'slow' has not passed the segment
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );
  assert.equal(segmentFiles(dir).length, 1);

  for (let i = 6; i <= 10; i++) await bus.publish({ i });
  bus.advance('slow', 5);
  await bus.truncate(7); // first segment (1..5) can go; the second straddles
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [6, 7, 8, 9, 10],
  );
  // 'slow' sits below the horizon: it reads from the earliest survivor,
  // the truncated gap is skipped — nothing repeated, nothing lost.
  assert.deepEqual(
    bus.read('slow').map((m) => m.seq),
    [6, 7, 8, 9, 10],
  );

  bus.advance('slow', 10);
  bus.advance('fast', 10);
  await bus.truncate(10); // everyone past everything: the rest goes
  assert.deepEqual(await bus.replay(0), []);
  assert.deepEqual(segmentFiles(dir), []);
  await bus.close();
});

test('truncate: replay of a surviving range is identical before and after', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = [];
  for (let i = 1; i <= 15; i++) {
    records.push({ i, pad: '你好'.repeat(i % 3) });
  }
  for (let i = 0; i < 12; i++) await bus.publish(records[i]);
  await bus.truncate(6); // rolls; the single segment straddles, nothing deleted
  for (let i = 12; i < 15; i++) await bus.publish(records[i]);

  const before = await bus.replay(13);
  const statsBefore = bus.stats();
  await bus.truncate(12); // deletes the first segment (1..12)

  // The surviving range replays byte-for-byte as before: seqs, ids, records.
  assert.deepEqual(await bus.replay(13), before);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [13, 14, 15],
  );
  assert.deepEqual((await bus.replay(0)).map((m) => m.record), records.slice(12));
  // A start inside the deleted range begins at the earliest survivor.
  assert.deepEqual(
    (await bus.replay(4)).map((m) => m.seq),
    [13, 14, 15],
  );
  // Only the replay counter moved.
  assert.equal(bus.stats().seq, statsBefore.seq);
  assert.equal(bus.stats().bytes, statsBefore.bytes);
  assert.equal(bus.stats().published, statsBefore.published);
  await bus.close();
});

test('truncate: a consumer positioned below the horizon reads from the earliest survivor', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.truncate(5); // no consumers yet: everything goes
  assert.deepEqual(await bus.replay(0), []);

  // A brand-new consumer starts at the earliest surviving message, not at 1.
  assert.deepEqual(bus.read('late'), []);
  for (let i = 6; i <= 8; i++) await bus.publish({ i });
  assert.deepEqual(
    bus.read('late').map((m) => m.seq),
    [6, 7, 8],
  );
  // The position itself is still 0; the gap below the horizon is skipped.
  assert.equal(bus.register('late'), 0);
  bus.advance('late', 7);
  assert.deepEqual(
    bus.read('late').map((m) => m.seq),
    [8],
  );
  await bus.close();
});

test('truncate: dedup keys keep their first acknowledgement across truncation and restart', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  const first = await bus.publish({ order: 'A', dedupKey: 'k1' });
  const second = await bus.publish({ order: 'B', dedupKey: 'k2' });
  await bus.publish({ other: 1 });
  await bus.truncate(3); // no consumers: every message is discarded
  assert.deepEqual(await bus.replay(0), []);

  // Resends reuse the first acknowledgement even though the messages are gone.
  assert.deepEqual(await bus.publish({ order: 'A2', dedupKey: 'k1' }), first);
  assert.deepEqual(await bus.publish({ order: 'B2', dedupKey: 'k2' }), second);
  assert.equal(bus.stats().published, 3);
  assert.equal(bus.stats().seq, 3);
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(await bus.publish({ order: 'A3', dedupKey: 'k1' }), first);
  assert.deepEqual(await bus.publish({ order: 'B3', dedupKey: 'k2' }), second);
  assert.equal(bus.stats().published, 3);
  const ack = await bus.publish({ fresh: true });
  assert.equal(ack.seq, 4);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [4]);
  await bus.close();
});

test('truncate: stats never shrink, across reopen and compaction', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const acks = [];
  for (let i = 1; i <= 6; i++) acks.push(await bus.publish({ big: '你好'.repeat(i), dedupKey: `d${i}` }));
  await bus.replay(0); // 6
  await bus.truncate(6); // everything goes
  const before = bus.stats();
  assert.equal(before.seq, 6);
  assert.equal(before.published, 6);
  assert.equal(before.replayed, 6);

  await bus.compact();
  assert.deepEqual(bus.stats(), before);
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);
  assert.deepEqual(await bus.replay(0), []);
  const ack = await bus.publish({ i: 7 });
  assert.equal(ack.seq, 7);
  // Truncated-away dedup keys still resolve after compaction and restart.
  assert.deepEqual(await bus.publish({ x: 1, dedupKey: 'd2' }), acks[1]);
  await bus.close();
});

test('truncate crash: checkpoint written but segments not yet deleted recovers consistently', async () => {
  const dir = await freshDir();
  // Hand-craft the post-checkpoint / pre-delete state: two finalized
  // segments and an active log led by a truncate marker covering seqs 1..3.
  const line = (i) =>
    JSON.stringify({
      t: 'm',
      seq: i,
      id: `id-${i}`,
      bytes: Buffer.byteLength(JSON.stringify({ i }), 'utf8'),
      record: { i },
    }) + '\n';
  writeFileSync(path.join(dir, 'bus.0000000001.jsonl'), [1, 2, 3].map(line).join(''));
  writeFileSync(path.join(dir, 'bus.0000000002.jsonl'), [4, 5].map(line).join(''));
  const bytes = [1, 2, 3, 4, 5].reduce((n, i) => n + Buffer.byteLength(JSON.stringify({ i }), 'utf8'), 0);
  const marker = {
    t: 't',
    gen: 0,
    horizon: 3,
    seq: 5,
    bytes,
    published: 5,
    replayed: 0,
    positions: { c: 3 },
    dedup: [['k1', { id: 'id-1', seq: 1 }]],
  };
  writeFileSync(path.join(dir, 'bus.jsonl'), JSON.stringify(marker) + '\n');

  const bus = createBus({ path: dir });
  // No loss, no duplication: state comes from the marker, messages 4..5 survive.
  assert.deepEqual(bus.stats(), { seq: 5, bytes, published: 5, replayed: 0 });
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [4, 5]);
  assert.equal(bus.register('c'), 3);
  // The truncated segment file is reconciled away; the survivor stays.
  assert.deepEqual(segmentFiles(dir), ['bus.0000000002.jsonl']);
  // Dedup and positions from the marker are live.
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'k1' }), { id: 'id-1', seq: 1 });
  const ack = await bus.publish({ i: 6 });
  assert.equal(ack.seq, 6);
  await bus.close();

  // The recovered state is stable across another reopen.
  const reopened = createBus({ path: dir });
  assert.deepEqual((await reopened.replay(0)).map((m) => m.seq), [4, 5, 6]);
  assert.equal(reopened.stats().published, 6);
  await reopened.close();
});

test('truncate crash: a half-written checkpoint marker is dropped, nothing is lost', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.truncate(3); // rolls segment 1; it straddles the bound and stays
  await bus.close();

  // Crash while the checkpoint of a later truncate was being written: the
  // active log holds only a torn marker line.
  writeFileSync(path.join(dir, 'bus.jsonl'), '{"t":"t","gen":0,"hor');

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 5);
  assert.equal(bus.stats().published, 5);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5]);
  const ack = await bus.publish({ i: 6 });
  assert.equal(ack.seq, 6);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5, 6]);
  await bus.close();
});

test('truncate: repeating the same truncation is harmless', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.truncate(4);
  assert.deepEqual(await bus.replay(0), []);
  await bus.truncate(4);
  await bus.truncate(4);
  assert.deepEqual(await bus.replay(0), []);
  assert.equal(bus.stats().seq, 4);
  assert.equal(bus.stats().published, 4);
  await bus.close();

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().seq, 4);
  assert.equal(reopened.stats().published, 4);
  assert.deepEqual(await reopened.replay(0), []);
  const ack = await reopened.publish({ i: 5 });
  assert.equal(ack.seq, 5);
  await reopened.close();
});

test('truncate: batches published around a truncation keep their acks and order', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const g1 = await bus.publishBatch([{ i: 1 }, { i: 2, dedupKey: 'k1' }]);
  await bus.truncate(2); // no consumers: the whole segment goes
  const g2 = await bus.publishBatch([{ i: 3 }, { i: 4 }]);
  assert.deepEqual(
    g2.map((a) => a.seq),
    [3, 4],
  );
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [3, 4]);
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'k1' }), g1[1]);
  assert.equal(bus.stats().published, 4);
  await bus.close();
});

test('truncate and compact compose: stats cumulative, replay and dedup stable', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const acks = [];
  for (let i = 1; i <= 6; i++) acks.push(await bus.publish({ i, dedupKey: `d${i}` }));
  bus.advance('c', 6);
  await bus.truncate(6); // consumer past the segment: seqs 1..6 are discarded
  assert.deepEqual(await bus.replay(0), []);
  for (let i = 7; i <= 9; i++) acks.push(await bus.publish({ i, dedupKey: `d${i}` }));
  const statsBeforeCompact = bus.stats();
  await bus.compact();
  // Neither truncation nor compaction moved the cumulative counters.
  assert.deepEqual(bus.stats(), statsBeforeCompact);
  // The surviving range replays exactly as before compaction.
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [7, 8, 9],
  );
  // Dedup survives the truncation of the messages it points at.
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'd4' }), acks[3]);
  await bus.close();

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 9);
  assert.equal(bus.stats().published, 9);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [7, 8, 9],
  );
  assert.deepEqual(await bus.publish({ i: 100, dedupKey: 'd1' }), acks[0]);
  const ack = await bus.publish({ i: 10 });
  assert.equal(ack.seq, 10);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [7, 8, 9, 10],
  );
  await bus.close();
});

test('truncate: concurrent with publish/batch/replay/positions/compact stays consistent', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [];
  for (let i = 1; i <= 24; i++) {
    if (i % 4 === 0) {
      jobs.push(bus.publishBatch([{ i }, { i: i + 0.5 }]));
    } else {
      jobs.push(bus.publish({ i }));
    }
    if (i % 6 === 0) jobs.push(bus.truncate(i));
    if (i % 8 === 0) jobs.push(bus.compact());
    if (i % 5 === 0) jobs.push(bus.replay(0));
    if (i % 10 === 0) {
      bus.register('c');
      bus.advance('c', i);
    }
  }
  await Promise.all(jobs);

  // 18 singles + 6 batches of 2 = 30 effective messages, none ever lost.
  assert.equal(bus.stats().published, 30);
  assert.equal(bus.stats().seq, 30);
  assert.equal(bus.register('c'), 20);
  // Whatever survived truncation is a contiguous suffix: no gaps, no repeats.
  const got = await bus.replay(0);
  assert.ok(got.length > 0);
  for (let k = 0; k < got.length; k++) {
    assert.equal(got[k].seq, got[0].seq + k);
  }
  assert.equal(got[got.length - 1].seq, 30);
  await bus.close();

  // Reopen: the same suffix, the same cumulative stats.
  const reopened = createBus({ path: dir });
  const again = await reopened.replay(0);
  assert.deepEqual(
    again.map((m) => m.seq),
    got.map((m) => m.seq),
  );
  assert.equal(reopened.stats().published, 30);
  assert.equal(reopened.stats().seq, 30);
  assert.equal(reopened.register('c'), 20);
  await reopened.close();
});

test('truncate after compact: no deletable segment is a no-op; the horizon only moves with deletions', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.compact(); // messages 1..5 now live in the snapshot, not in segments
  // No message-bearing segment exists: the bound finds nothing to delete.
  await bus.truncate(5);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );

  // Once a real segment deletion pushes the horizon past the folded
  // messages, they are discarded like everything else at/below the horizon.
  for (let i = 6; i <= 8; i++) await bus.publish({ i });
  await bus.truncate(8); // no consumers: segment 6..8 goes, horizon 8
  assert.deepEqual(await bus.replay(0), []);
  await bus.close();

  // The horizon survives the restart: folded messages stay discarded.
  const reopened = createBus({ path: dir });
  assert.deepEqual(await reopened.replay(0), []);
  assert.equal(reopened.stats().seq, 8);
  assert.equal(reopened.stats().published, 8);
  await reopened.close();
});

test('truncate: queued before close is rejected and changes nothing', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  const truncateP = bus.truncate(1);
  const closeP = bus.close();
  await assert.rejects(truncateP, Error);
  await closeP;

  const reopened = createBus({ path: dir });
  assert.deepEqual((await reopened.replay(0)).map((m) => m.seq), [1]);
  await reopened.close();
});

// ---- byte quota (maxBytes / usage) ------------------------------------------

test('maxBytes validation: omitted is unlimited; non-positive-integer throws TypeError synchronously', () => {
  const dirsLocal = [];
  const make = (maxBytes) => {
    const dir = `/tmp/replay-bus-quota-check-${Math.random().toString(36).slice(2)}`;
    dirsLocal.push(dir);
    return createBus(maxBytes === undefined ? { path: dir } : { path: dir, maxBytes });
  };
  for (const bad of [0, -1, -100, 1.5, 0.1, NaN, Infinity, '5', '', null, true, false, {}, []]) {
    assert.throws(() => make(bad), TypeError);
  }
  // Omitted or an actual positive integer are fine.
  const unbounded = make(undefined);
  assert.equal(unbounded.usage(), 0);
  const one = make(1);
  assert.equal(one.usage(), 0);
  return Promise.all([unbounded.close(), one.close()]).then(() =>
    Promise.all(dirsLocal.map((d) => rm(d, { recursive: true, force: true }))),
  );
});

test('usage: zero on an empty bus and equal to stats bytes while nothing is truncated', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, maxBytes: 100000 });
  assert.equal(bus.usage(), 0);
  await bus.publish({ a: 1 });
  await bus.publish({ msg: '你好' });
  assert.equal(bus.usage(), bus.stats().bytes);
  // Snapshot changes shape, not the retained byte count.
  await bus.compact();
  assert.equal(bus.usage(), bus.stats().bytes);
  await bus.close();
});

test('quota: an exact fit succeeds; one byte over rejects RangeError with state untouched', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const size = Buffer.byteLength(JSON.stringify({ a: 1 }), 'utf8');
  await bus.close();

  const capped = createBus({ path: dir, maxBytes: size });
  const ack = await capped.publish({ a: 1 });
  assert.equal(ack.seq, 1);
  assert.equal(capped.usage(), size);

  // Even a tiny record cannot fit once the quota is full.
  await assert.rejects(capped.publish({ a: 2 }), RangeError);
  // Nothing moved: seq, cumulative stats and usage all stay at the fit.
  assert.equal(capped.stats().seq, 1);
  assert.equal(capped.stats().published, 1);
  assert.equal(capped.stats().bytes, size);
  assert.equal(capped.usage(), size);
  // The rejected record is not visible.
  assert.deepEqual((await capped.replay(0)).map((m) => m.seq), [1]);
  await capped.close();
});

test('quota: a single record larger than maxBytes can never be published', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, maxBytes: 5 });
  assert.equal(bus.usage(), 0);
  await assert.rejects(bus.publish({ definitely: 'too big' }), RangeError);
  assert.equal(bus.usage(), 0);
  assert.equal(bus.stats().seq, 0);
  // The rejection consumed no headroom: a small record still lands.
  const tiny = {};
  await assert.rejects(bus.publish({ alsoTooBig: 'yes' }), RangeError);
  const ack = await bus.publish(tiny);
  assert.equal(ack.seq, 1);
  assert.equal(bus.usage(), Buffer.byteLength(JSON.stringify(tiny), 'utf8'));
  await bus.close();
});

test('quota: resending an existing dedupKey while full succeeds, reuses the ack, uses no quota', async () => {
  const dir = await freshDir();
  const record = { order: 'A', dedupKey: 'k1' };
  const size = Buffer.byteLength(JSON.stringify(record), 'utf8');
  const bus = createBus({ path: dir, maxBytes: size });
  const first = await bus.publish(record);
  assert.equal(bus.usage(), size);
  // Full: a brand-new message is refused.
  await assert.rejects(bus.publish({ fresh: true }), RangeError);
  // But the resend rides the first acknowledgement and allocates nothing.
  const again = await bus.publish({ order: 'totally-different-body', dedupKey: 'k1' });
  assert.deepEqual(again, first);
  assert.equal(bus.usage(), size);
  assert.equal(bus.stats().published, 1);
  await bus.close();
});

test('quota: read paths are unaffected when the bus is full', async () => {
  const dir = await freshDir();
  const r1 = { v: 1 };
  const size = Buffer.byteLength(JSON.stringify(r1), 'utf8');
  const bus = createBus({ path: dir, maxBytes: size });
  await bus.publish(r1);
  await assert.rejects(bus.publish({ v: 2 }), RangeError);
  // replay / register / advance / read / stats all behave normally.
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1]);
  assert.equal(bus.register('c'), 0);
  bus.advance('c', 1);
  assert.deepEqual(bus.read('c'), []);
  assert.equal(bus.stats().seq, 1);
  await bus.close();
});

test('quota batch: shape validation first, then whole-group occupancy; an all-dup group always fits', async () => {
  const dir = await freshDir();
  const a = { v: 1, dedupKey: 'hist' };
  const sizeA = Buffer.byteLength(JSON.stringify(a), 'utf8');
  const bus = createBus({ path: dir, maxBytes: sizeA });
  const first = await bus.publish(a);
  assert.equal(bus.usage(), sizeA);

  // A group carrying one new byte too many is rejected wholesale.
  await assert.rejects(
    bus.publishBatch([{ v: 2, dedupKey: 'g1' }, { v: 3, dedupKey: 'g2' }]),
    RangeError,
  );
  assert.equal(bus.usage(), sizeA);
  assert.equal(bus.stats().seq, 1);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1]);

  // A group whose records all reuse an existing key needs no room.
  const acks = await bus.publishBatch([
    { v: 9, dedupKey: 'hist' },
    { v: 8, dedupKey: 'hist' },
  ]);
  assert.deepEqual(acks[0], first);
  assert.deepEqual(acks[1], first);
  assert.equal(bus.usage(), sizeA);

  // A bad shape still beats the quota check: TypeError, not RangeError.
  await assert.rejects(bus.publishBatch([null, { v: 2 }]), TypeError);
  await bus.close();
});

test('quota batch: in-group duplicate keys are charged once; exact group fit lands', async () => {
  const dir = await freshDir();
  const r1 = { order: 'A', dedupKey: 'k1' };
  const r2 = { order: 'B' };
  const need =
    Buffer.byteLength(JSON.stringify(r1), 'utf8') +
    Buffer.byteLength(JSON.stringify(r2), 'utf8');
  const bus = createBus({ path: dir, maxBytes: need });
  const acks = await bus.publishBatch([
    r1,
    r2,
    { order: 'A2', dedupKey: 'k1' }, // duplicate: only the first occurrence is charged
  ]);
  assert.deepEqual(acks.map((x) => x.seq), [1, 2, 1]);
  assert.equal(bus.usage(), need);
  // Exactly full: the next group refuses without a trace.
  const before = bus.stats();
  await assert.rejects(bus.publishBatch([{ z: 1 }]), RangeError);
  assert.deepEqual(bus.stats(), before);
  assert.equal(bus.usage(), need);
  await bus.close();
});

test('quota batch: oversized group leaves no durable trace and later messages still fit', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, maxBytes: 40 });
  await assert.rejects(
    bus.publishBatch([{ pad: 'x'.repeat(40) }, { pad: 'y'.repeat(40) }]),
    RangeError,
  );
  assert.equal(bus.usage(), 0);
  assert.equal(bus.stats().seq, 0);
  await bus.close();

  const reopened = createBus({ path: dir, maxBytes: 40 });
  assert.equal(reopened.usage(), 0);
  assert.deepEqual(await reopened.replay(0), []);
  await reopened.close();
});

test('quota: truncation frees bytes by the consumer-position rules and publishing resumes', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const sizes = [];
  for (let i = 1; i <= 5; i++) {
    const r = { i, pad: '你好'.repeat(i) };
    sizes.push(Buffer.byteLength(JSON.stringify(r), 'utf8'));
    await bus.publish(r);
  }
  const total = sizes.reduce((n, s) => n + s, 0);
  await bus.close();

  const capped = createBus({ path: dir, maxBytes: total });
  assert.equal(capped.usage(), total);
  await assert.rejects(capped.publish({ i: 6 }), RangeError);

  // A slow consumer at position 0 holds the single segment: nothing freed.
  capped.register('slow');
  await capped.truncate(5);
  assert.equal(capped.usage(), total);
  await assert.rejects(capped.publish({ i: 6 }), RangeError);

  // Once every consumer passes the segment, truncation discards and frees it.
  capped.advance('slow', 5);
  await capped.truncate(5);
  assert.equal(capped.usage(), 0);
  assert.deepEqual(await capped.replay(0), []);
  // Cumulative stats never shrink.
  assert.equal(capped.stats().bytes, total);

  const ack = await capped.publish({ i: 6 });
  assert.equal(ack.seq, 6);
  assert.ok(capped.usage() > 0 && capped.usage() <= total);
  await capped.close();
});

test('quota: compaction does not release any quota', async () => {
  const dir = await freshDir();
  const records = [{ a: 1 }, { b: '你好'.repeat(3) }, { c: [1, 2, 3] }];
  const total = records.reduce((n, r) => n + Buffer.byteLength(JSON.stringify(r), 'utf8'), 0);
  const bus = createBus({ path: dir, maxBytes: total });
  for (const r of records) await bus.publish(r);
  assert.equal(bus.usage(), total);
  await bus.compact();
  assert.equal(bus.usage(), total);
  await assert.rejects(bus.publish({ d: 1 }), RangeError);
  // Repeated compaction changes nothing about the quota.
  await bus.compact();
  assert.equal(bus.usage(), total);
  await bus.close();
});

test('quota: checks run in serial order — a queued truncate unblocks a queued publish', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const r = { fill: 'xy'.repeat(8) };
  const size = Buffer.byteLength(JSON.stringify(r), 'utf8');
  await bus.publish(r);
  await bus.close();

  const capped = createBus({ path: dir, maxBytes: size });
  assert.equal(capped.usage(), size);
  // Both calls land before either job runs; invocation order decides.
  const truncateP = capped.truncate(1);
  const publishP = capped.publish({ after: true });
  await truncateP;
  const ack = await publishP;
  assert.equal(ack.seq, 2);
  assert.equal(capped.usage(), Buffer.byteLength(JSON.stringify({ after: true }), 'utf8'));
  await capped.close();
});

test('quota: usage and rejections are stable across reopen; a half-written batch holds no bytes', async () => {
  const dir = await freshDir();
  const r1 = { a: 1 };
  const r2 = { b: '你好' };
  const need =
    Buffer.byteLength(JSON.stringify(r1), 'utf8') +
    Buffer.byteLength(JSON.stringify(r2), 'utf8');
  let bus = createBus({ path: dir, fsync: true, maxBytes: need });
  await bus.publish(r1);
  await bus.publish(r2);
  assert.equal(bus.usage(), need);
  await assert.rejects(bus.publish({ c: 3 }), RangeError);
  await bus.close();

  // Reopen with the same cap: retained occupancy comes back exactly.
  bus = createBus({ path: dir, maxBytes: need });
  assert.equal(bus.usage(), need);
  assert.equal(bus.stats().bytes, need);
  await assert.rejects(bus.publish({ c: 3 }), RangeError);
  await bus.close();

  // Crash mid group: the uncommitted bracket must neither be alive nor hold
  // quota after reopen, so a record of the freed size fits.
  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'dead' }) + '\n');
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 3, id: 'z', bytes: 99, record: { c: 3 } }) + '\n',
  );
  bus = createBus({ path: dir, maxBytes: need });
  assert.equal(bus.usage(), need);
  assert.equal(bus.stats().seq, 2);
  await assert.rejects(bus.publish({ definitelyTooLarge: true }), RangeError);
  await bus.close();
});

test('quota: truncation after compaction also releases the snapshot-folded bytes at runtime', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 5; i++) await bus.publish({ i, pad: '你好'.repeat(i) });
  const firstFive = bus.usage();
  await bus.compact();
  assert.equal(bus.usage(), firstFive); // compaction releases nothing
  for (let i = 6; i <= 8; i++) await bus.publish({ i });
  const withEight = bus.usage();
  // No consumers: the post-compaction segment (6..8) is deletable; its
  // horizon runs past the folded messages 1..5 as well.
  await bus.truncate(8);
  assert.deepEqual(await bus.replay(0), []);
  assert.equal(bus.usage(), 0);
  assert.equal(withEight > firstFive, true);
  // Cumulative stats are untouched by retention.
  assert.equal(bus.stats().bytes, withEight);

  // Headroom is back without a reopen; publishing resumes immediately.
  const ack = await bus.publish({ fresh: 1 });
  assert.equal(ack.seq, 9);
  const freshSize = Buffer.byteLength(JSON.stringify({ fresh: 1 }), 'utf8');
  assert.equal(bus.usage(), freshSize);

  // And the same accounting survives restart.
  await bus.close();
  const reopened = createBus({ path: dir, maxBytes: freshSize });
  assert.equal(reopened.usage(), freshSize);
  await assert.rejects(reopened.publish({ too: 'much' }), RangeError);
  await reopened.close();
});

test('quota: usage() stays readable after close; close is still idempotent', async () => {
  const dir = await freshDir();
  const r = { a: 1 };
  const size = Buffer.byteLength(JSON.stringify(r), 'utf8');
  const bus = createBus({ path: dir, maxBytes: size * 2 });
  await bus.publish(r);
  await bus.close();
  await bus.close();
  assert.equal(bus.usage(), size);
  await assert.rejects(bus.publish({ a: 2 }), Error);
});

test('quota: without maxBytes the bus stays unlimited regardless of occupancy', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 0; i < 50; i++) await bus.publish({ pad: '你好'.repeat(20) });
  assert.ok(bus.usage() > 0);
  await bus.publish({ oneMore: true });
  await bus.close();
});

