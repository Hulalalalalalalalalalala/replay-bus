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

test('crash recovery: log truncated but marker not written loses nothing beyond the snapshot', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  const acks = [];
  for (let i = 1; i <= 3; i++) acks.push(await bus.publish({ i, dedupKey: `k${i}` }));
  const statsBefore = bus.stats();
  await bus.close();

  // Simulate the crash window inside the portable truncation: the snapshot
  // is in place, the log was rewritten empty, the marker never landed.
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
    positions: {},
    dedup: entries
      .filter((e) => e.t === 'm' && typeof e.d === 'string')
      .map((e) => [e.d, { id: e.id, seq: e.seq }]),
    messages: entries
      .filter((e) => e.t === 'm')
      .map((e) => ({ seq: e.seq, id: e.id, record: e.record })),
  };
  writeFileSync(path.join(dir, 'bus.snapshot.json'), JSON.stringify(snapshot));
  writeFileSync(path.join(dir, 'bus.jsonl'), '');

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), statsBefore);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'k2' }), acks[1]);
  const ack = await bus.publish({ i: 4 });
  assert.equal(ack.seq, 4);
  await bus.close();
});

test('default export is removed; only named createBus is exported', async () => {
  const mod = await import('../src/index.js');
  assert.equal(typeof mod.createBus, 'function');
  assert.equal(mod.default, undefined);
});

// ---- publishBatch ----------------------------------------------------------

test('publishBatch: non-array input throws TypeError synchronously', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (const bad of [undefined, null, 'records', 42, true, {}, { 0: { a: 1 }, length: 1 }]) {
    assert.throws(() => bus.publishBatch(bad), TypeError);
  }
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  await bus.close();
});

test('publishBatch: empty batch succeeds with empty receipts', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  assert.deepEqual(await bus.publishBatch([]), []);
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  const ack = await bus.publish({ a: 1 });
  assert.equal(ack.seq, 1);
  await bus.close();
});

test('publishBatch: receipts in input order, seqs continuous across the group', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const first = await bus.publish({ v: 0 });
  const records = [{ v: 1 }, { v: 2, t: '你好' }, { v: 3, nested: { x: [1, 2] } }];
  const receipts = await bus.publishBatch(records);
  assert.equal(receipts.length, 3);
  assert.deepEqual(receipts.map((r) => r.seq), [2, 3, 4]);
  for (const r of receipts) assert.equal(typeof r.id, 'string');
  assert.notEqual(receipts[0].id, receipts[1].id);
  const last = await bus.publish({ v: 4 });
  assert.equal(last.seq, 5);

  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4, 5]);
  assert.deepEqual(got.slice(1, 4).map((m) => m.record), records);
  assert.deepEqual(got.slice(1, 4).map((m) => m.id), receipts.map((r) => r.id));

  const expectedBytes =
    Buffer.byteLength(JSON.stringify({ v: 0 }), 'utf8') +
    records.reduce((n, r) => n + Buffer.byteLength(JSON.stringify(r), 'utf8'), 0) +
    Buffer.byteLength(JSON.stringify({ v: 4 }), 'utf8');
  assert.deepEqual(bus.stats(), { seq: 5, bytes: expectedBytes, published: 5, replayed: 5 });
  assert.equal(first.seq, 1);
  await bus.close();
});

test('publishBatch: one illegal member rejects the whole group and changes nothing', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ ok: 1, dedupKey: 'keep' });
  bus.advance('c', 1);
  const before = bus.stats();
  const logBefore = readFileSync(path.join(dir, 'bus.jsonl'));

  const badGroups = [
    [{ a: 1 }, null],
    [{ a: 1 }, [1, 2]],
    [{ a: 1 }, 'str'],
    [{ a: 1 }, 42],
    [{ a: 1 }, { n: NaN }],
    [{ a: 1 }, { n: Infinity }],
    [{ a: 1 }, { u: undefined }],
    [{ a: 1 }, { f: () => 1 }],
    [{ a: 1 }, { g: 10n }],
    [{ a: 1 }, { dedupKey: 7 }],
  ];
  for (const group of badGroups) {
    await assert.rejects(bus.publishBatch(group), TypeError);
  }
  const circular = { x: 1 };
  circular.self = circular;
  await assert.rejects(bus.publishBatch([{ a: 1 }, circular]), TypeError);

  // seq, bytes, published, positions and the log itself are untouched.
  assert.deepEqual(bus.stats(), before);
  assert.equal(bus.register('c'), 1);
  assert.equal(readFileSync(path.join(dir, 'bus.jsonl')).equals(logBefore), true);
  // The dedup table is untouched too.
  const dup = await bus.publish({ other: true, dedupKey: 'keep' });
  assert.equal(dup.seq, 1);
  // And the bus still works.
  const ack = await bus.publish({ ok: 2 });
  assert.equal(ack.seq, 2);
  await bus.close();
});

test('publishBatch: dedup inside the group and against history reuses the first receipt', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const hist = await bus.publish({ order: 'old', dedupKey: 'k-old' });
  const receipts = await bus.publishBatch([
    { order: 'A', dedupKey: 'k1' },
    { order: 'B' },
    { order: 'A-resent', dedupKey: 'k1' }, // duplicate inside the group
    { order: 'C', dedupKey: 'k-old' }, // duplicate of history
    { order: 'D', dedupKey: 'k2' },
    { order: 'D-resent', dedupKey: 'k2' },
  ]);
  assert.deepEqual(receipts.map((r) => r.seq), [2, 3, 2, 1, 4, 4]);
  assert.deepEqual(receipts[2], receipts[0]);
  assert.deepEqual(receipts[3], hist);
  assert.deepEqual(receipts[5], receipts[4]);
  // Only effective members got a seq.
  assert.equal(bus.stats().published, 4);
  assert.equal(bus.stats().seq, 4);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4]);
  assert.deepEqual(got[1].record, { order: 'A', dedupKey: 'k1' });
  // A resend after the batch still returns the first receipt.
  assert.deepEqual(await bus.publish({ order: 'A-again', dedupKey: 'k1' }), receipts[0]);
  await bus.close();
});

test('publishBatch: dedup and sequence survive reopen', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  const receipts = await bus.publishBatch([
    { v: 1, dedupKey: 'a' },
    { v: 2, dedupKey: 'b' },
    { v: 3, dedupKey: 'a' },
  ]);
  const before = bus.stats();
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);
  // Cross-restart dedup: the old keys still return their first receipts.
  assert.deepEqual(await bus.publish({ v: 9, dedupKey: 'a' }), receipts[0]);
  const more = await bus.publishBatch([{ v: 10, dedupKey: 'b' }, { v: 4 }]);
  assert.deepEqual(more[0], receipts[1]);
  assert.equal(more[1].seq, 3);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(
    got.map((m) => m.record),
    [{ v: 1, dedupKey: 'a' }, { v: 2, dedupKey: 'b' }, { v: 4 }],
  );
  await bus.close();
});

test('publishBatch: torn group line after a crash leaves no half group', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  await bus.publishBatch([{ v: 1 }, { v: 2 }]);
  await bus.close();

  // Simulate a crash in the middle of a batch write: the group line is only
  // partially durable, so the whole group must vanish on recovery.
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    '{"t":"b","msgs":[{"seq":3,"id":"x","bytes":7,"record":{"v":3}},',
  );

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 2);
  assert.equal(bus.stats().published, 2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2]);
  const receipts = await bus.publishBatch([{ v: 3 }, { v: 4 }]);
  assert.deepEqual(receipts.map((r) => r.seq), [3, 4]);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  await bus.close();
});

test('publishBatch: single publish of a reserved key shares the batch receipt', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const batchPromise = bus.publishBatch([{ v: 1, dedupKey: 'k' }, { v: 2 }]);
  const single = await bus.publish({ v: 9, dedupKey: 'k' });
  const receipts = await batchPromise;
  assert.deepEqual(single, receipts[0]);
  assert.equal(receipts[1].seq, 2);
  assert.equal(bus.stats().published, 2);
  await bus.close();
});

test('publishBatch: key already in flight from publish resolves to that receipt', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  const p = bus.publish({ v: 1, dedupKey: 'k' });
  const batch = await bus.publishBatch([{ v: 2, dedupKey: 'k' }, { v: 3 }]);
  const first = await p;
  assert.deepEqual(batch[0], first);
  assert.equal(batch[1].seq, 2);
  assert.equal(bus.stats().published, 2);
  await bus.close();
});

test('publishBatch: after close rejects Error and repeated close stays harmless', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.close();
  await bus.close();
  await assert.rejects(bus.publishBatch([{ a: 1 }]), Error);
  await assert.rejects(bus.publishBatch([]), Error);
});

test('publishBatch: batch queued before close is rejected and nothing is written', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const p = bus.publishBatch([{ a: 1 }, { a: 2 }]);
  const closePromise = bus.close();
  await assert.rejects(p, Error);
  await closePromise;

  const reopened = createBus({ path: dir });
  assert.equal(reopened.stats().published, 0);
  const ack = await reopened.publish({ a: 1 });
  assert.equal(ack.seq, 1);
  await reopened.close();
});

test('publishBatch: concurrent with publish/replay/positions/compact, seqs stay continuous', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const jobs = [];
  const acks = [];
  for (let i = 0; i < 10; i++) {
    jobs.push(
      bus.publishBatch([{ g: i, n: 0 }, { g: i, n: 1 }]).then((rs) => {
        acks.push(...rs);
      }),
    );
    jobs.push(
      bus.publish({ single: i }).then((r) => {
        acks.push(r);
      }),
    );
    if (i % 3 === 0) jobs.push(bus.compact());
    if (i % 4 === 0) jobs.push(bus.replay(0));
    if (i % 5 === 0) {
      bus.register('c');
      bus.advance('c', i);
    }
  }
  await Promise.all(jobs);
  const seqs = acks.map((a) => a.seq).sort((x, y) => x - y);
  assert.deepEqual(seqs, Array.from({ length: 30 }, (_, i) => i + 1));
  const got = await bus.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    Array.from({ length: 30 }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, 30);
  await bus.close();
});

test('publishBatch: replay and stats are identical across compaction and reopen', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  await bus.publishBatch([{ i: 1, dedupKey: 'x' }, { i: 2 }, { i: 3, dedupKey: 'y' }]);
  await bus.publish({ i: 4 });
  await bus.publishBatch([{ i: 5 }, { i: 6 }]);
  const before = await bus.replay(0);
  const statsBefore = bus.stats();
  await bus.compact();

  assert.deepEqual(await bus.replay(0), before);
  assert.deepEqual(await bus.replay(3), before.slice(2));
  assert.equal(bus.stats().seq, statsBefore.seq);
  assert.equal(bus.stats().bytes, statsBefore.bytes);
  assert.equal(bus.stats().published, statsBefore.published);
  // Dedup of batch-published keys survives compaction.
  assert.deepEqual(await bus.publish({ i: 99, dedupKey: 'x' }), { id: before[0].id, seq: 1 });
  // The sequence continues right where it was.
  const r = await bus.publishBatch([{ i: 7 }, { i: 8 }]);
  assert.deepEqual(r.map((x) => x.seq), [7, 8]);
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5, 6, 7, 8]);
  assert.deepEqual(await bus.publish({ i: 100, dedupKey: 'y' }), { id: before[2].id, seq: 3 });
  await bus.close();
});

test('publishBatch: large batch takes effect at once in group order', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = Array.from({ length: 2000 }, (_, i) => ({ i, pad: 'x'.repeat(20) }));
  const receipts = await bus.publishBatch(records);
  assert.equal(receipts.length, 2000);
  assert.deepEqual(
    receipts.map((r) => r.seq),
    Array.from({ length: 2000 }, (_, i) => i + 1),
  );
  assert.equal(bus.stats().published, 2000);
  assert.equal(bus.stats().seq, 2000);
  const got = await bus.replay(0);
  assert.deepEqual(got.map((m) => m.record), records);
  await bus.close();
});

