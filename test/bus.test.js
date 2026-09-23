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

