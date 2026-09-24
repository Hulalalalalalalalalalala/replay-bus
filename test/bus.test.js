import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { rm, mkdir, appendFile } from 'node:fs/promises';
import { readFileSync, writeFileSync } from 'node:fs';
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

// ---- truncate --------------------------------------------------------------

test('truncate: invalid boundary throws TypeError synchronously; zero is a no-op', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (const bad of [-1, 1.5, NaN, '1', {}, null, true, undefined, 2n, Infinity]) {
    assert.throws(() => bus.truncate(bad), TypeError);
  }
  await bus.truncate(0); // zero boundary: no-op
  for (let i = 1; i <= 3; i++) await bus.publish({ i });
  await bus.truncate(0); // still a no-op with data present
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3]);
  assert.equal(bus.stats().published, 3);
  await bus.close();
});

test('truncate: a segment disappears only once every consumer has passed it', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 10; i++) await bus.publish({ i });
  bus.advance('slow', 4);
  bus.advance('fast', 10);

  // slow is still inside the boundary segment: nothing is deleted, no error.
  await bus.truncate(6);
  assert.deepEqual(
    (await bus.replay(0)).map((m) => m.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );

  // Once slow passes the sealed segment, the same boundary deletes it.
  bus.advance('slow', 6);
  await bus.truncate(6);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [7, 8, 9, 10]);

  // Repeating the same truncation is harmless.
  await bus.truncate(6);
  await bus.truncate(3);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [7, 8, 9, 10]);
  await bus.close();
});

test('truncate: replay of the surviving range is identical, deleted start served from earliest survivor', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 8; i++) await bus.publish({ i, t: '你好' });
  const before = await bus.replay(0);
  bus.advance('c', 8);
  await bus.truncate(5);

  const survivors = await bus.replay(0);
  // Same entries down to the ids: seqs, bodies and order are untouched.
  assert.deepEqual(survivors, before.slice(5));
  // A start inside the deleted range begins at the earliest survivor.
  assert.deepEqual(await bus.replay(3), survivors);
  assert.deepEqual(await bus.replay(5), survivors);
  assert.deepEqual((await bus.replay(7)).map((m) => m.seq), [7, 8]);
  await bus.close();
});

test('truncate: boundary past the end is the end; everything gone replays empty, stats stay cumulative', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  bus.advance('c', 5);
  const statsBefore = bus.stats();
  await bus.truncate(999);
  assert.deepEqual(await bus.replay(0), []);
  assert.deepEqual(bus.read('c'), []);
  // Truncation never shrinks the counters.
  assert.deepEqual(bus.stats(), statsBefore);
  // The sequence continues where it was.
  const ack = await bus.publish({ i: 6 });
  assert.equal(ack.seq, 6);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [6]);
  await bus.close();
});

test('truncate: with no registered consumers the boundary deletes immediately', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.truncate(2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [3, 4]);
  await bus.close();
});

test('truncate: dedup acknowledgements survive truncation and restart, resend gets no new seq', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const first = await bus.publish({ order: 'A', dedupKey: 'k1' });
  for (let i = 2; i <= 4; i++) await bus.publish({ i, dedupKey: `k${i}` });
  bus.advance('c', 4);
  await bus.truncate(4);
  // The message is gone but its key still serves the first acknowledgement.
  assert.deepEqual(await bus.publish({ order: 'A-again', dedupKey: 'k1' }), first);
  assert.equal(bus.stats().seq, 4);
  assert.equal(bus.stats().published, 4);
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(await bus.publish({ order: 'A-third', dedupKey: 'k1' }), first);
  const k3 = await bus.publish({ x: 1, dedupKey: 'k3' });
  assert.equal(k3.seq, 3);
  assert.equal(bus.stats().published, 4);
  assert.deepEqual(await bus.replay(0), []);
  const ack = await bus.publish({ fresh: true });
  assert.equal(ack.seq, 5);
  await bus.close();
});

test('truncate: stats, positions and survivors are consistent after reopen', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 6; i++) await bus.publish({ i, dedupKey: `d${i}` });
  bus.advance('keeper', 6);
  await bus.replay(0); // 6 replayed
  await bus.truncate(4);
  const before = bus.stats();
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);
  assert.equal(bus.register('keeper'), 6);
  // A consumer first registered now starts below the earliest survivor.
  assert.deepEqual(bus.read('late').map((m) => m.seq), [5, 6]);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [5, 6]);
  // A repeated truncation after reopen is a harmless no-op.
  await bus.truncate(4);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [5, 6]);
  await bus.close();
});

test('truncate: a consumer behind the earliest survivor reads from the earliest survivor', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 6; i++) await bus.publish({ i });
  bus.advance('c1', 6);
  await bus.truncate(4);
  // Brand-new consumer starts at 0, below the earliest surviving seq.
  assert.deepEqual(bus.read('new').map((m) => m.seq), [5, 6]);
  // No duplication and nothing skipped that still exists.
  bus.advance('new', 5);
  assert.deepEqual(bus.read('new').map((m) => m.seq), [6]);
  await bus.close();
});

test('truncate: recovery cleans a stray segment tmp and a torn active tail', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 6; i++) await bus.publish({ i });
  bus.advance('c', 6);
  await bus.truncate(4);
  await bus.close();

  await appendFile(path.join(dir, 'bus.jsonl'), '{"t":"m","seq":7,"id":"x",');
  await appendFile(path.join(dir, 'bus.seg.7-9.tmp'), '{"t":"m","seq":7}');

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 6);
  assert.equal(bus.stats().published, 6);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [5, 6]);
  const ack = await bus.publish({ i: 7 });
  assert.equal(ack.seq, 7);
  await bus.close();
});

test('truncate crash window: sealed segment landed but active log not yet replaced recovers without duplication', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 5; i++) await bus.publish({ i, dedupKey: `k${i}` });
  await bus.close();

  // Simulate the crash between the two renames of a roll: the sealed
  // segment exists AND the old active log still holds the same messages.
  const raw = readFileSync(path.join(dir, 'bus.jsonl'), 'utf8');
  writeFileSync(path.join(dir, 'bus.seg.1-5.jsonl'), raw);

  bus = createBus({ path: dir });
  assert.equal(bus.stats().published, 5);
  assert.equal(bus.stats().seq, 5);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4, 5]);
  // Dedup from the segment is effective exactly once.
  const dup = await bus.publish({ i: 99, dedupKey: 'k2' });
  assert.equal(dup.seq, 2);
  const ack = await bus.publish({ i: 6 });
  assert.equal(ack.seq, 6);
  assert.equal(bus.stats().published, 6);
  await bus.close();
});

test('truncate: compacted messages survive; post-compact segments truncate away', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.compact();
  for (let i = 6; i <= 10; i++) await bus.publish({ i });
  bus.advance('c', 10);
  await bus.truncate(8);
  // Snapshot messages 1..5 are folded state, not a segment; 6..8 are gone.
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5, 9, 10]);
  // Truncation and compaction never shrink the cumulative counters.
  const statsBefore = bus.stats();
  // Compacting again after a truncation stays consistent.
  await bus.compact();
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5, 9, 10]);
  assert.equal(bus.stats().seq, statsBefore.seq);
  assert.equal(bus.stats().bytes, statsBefore.bytes);
  assert.equal(bus.stats().published, statsBefore.published);
  await bus.close();
});

test('truncate: concurrent with publish/batch/replay/positions serializes cleanly', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [];
  for (let i = 1; i <= 30; i++) {
    jobs.push(bus.publish({ i, dedupKey: `k${i % 7}` }));
    if (i % 4 === 0) jobs.push(bus.truncate(i));
    if (i % 5 === 0) bus.advance('c', i);
    if (i % 6 === 0) jobs.push(bus.replay(0));
  }
  jobs.push(bus.publishBatch([{ b: 1 }, { b: 2, dedupKey: 'kb' }]));
  await Promise.all(jobs);

  // 7 distinct keys among the singles, plus 2 effective batch messages.
  assert.equal(bus.stats().published, 9);
  assert.equal(bus.stats().seq, 9);
  // c is already at 30, past every message: the boundary deletes everything.
  await bus.truncate(999);
  assert.deepEqual(await bus.replay(0), []);
  // Dedup still resolves keys whose messages were truncated away.
  const dup = await bus.publish({ x: 1, dedupKey: 'k3' });
  assert.equal(dup.seq, 3);
  const ack = await bus.publish({ fresh: true });
  assert.equal(ack.seq, 10);
  await bus.close();

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().published, 10);
  assert.deepEqual(await reopened.publish({ x: 2, dedupKey: 'k3' }), dup);
  await reopened.close();
});

test('truncate: after close rejects Error; bad boundary is still a synchronous TypeError', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.close();
  await assert.rejects(bus.truncate(1), Error);
  assert.throws(() => bus.truncate(-1), TypeError);
  assert.throws(() => bus.truncate('x'), TypeError);
  assert.equal(bus.stats().published, 1);
});
