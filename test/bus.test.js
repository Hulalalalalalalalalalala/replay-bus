import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { rm, mkdir, appendFile, readFile, stat } from 'node:fs/promises';
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

// ---------------------------------------------------------------- offsets

test('register: first position zero; repeat registration keeps current value', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  assert.equal(await bus.register('alice'), 0);
  await bus.publish({ v: 1 });
  assert.equal(await bus.advance('alice', 1), 1);
  // Re-register after consuming: position untouched.
  assert.equal(await bus.register('alice'), 1);
  assert.equal(await bus.register('alice'), 1);
  // Independent consumers.
  assert.equal(await bus.register('bob'), 0);
  await bus.close();
});

test('read returns from next after position; read does not advance; beyond end empty', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.register('alice');
  for (let i = 1; i <= 3; i++) await bus.publish({ i });

  // Position zero => nothing consumed => every message.
  let got = await bus.read('alice');
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual(got[1], { seq: 2, id: got[1].id, record: { i: 2 } });

  // Reading again returns the same messages: read never lands the position.
  got = await bus.read('alice');
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3]);

  await bus.advance('alice', 2);
  got = await bus.read('alice');
  assert.deepEqual(got.map((m) => m.seq), [3]);

  await bus.advance('alice', 3);
  assert.deepEqual(await bus.read('alice'), []);
  await bus.close();
});

test('read shape matches replay for the same interval', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.advance('g', 2);
  const byRead = await bus.read('g');
  const byReplay = await bus.replay(3);
  assert.deepEqual(byRead, byReplay);
  await bus.close();
});

test('advance/register/read auto-register unknown names, same as register first', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ v: 1 });

  assert.equal(await bus.advance('direct-advance', 1), 1);
  // First read auto-registers at zero, so the published message is visible.
  assert.deepEqual((await bus.read('direct-read')).map((m) => m.seq), [1]);

  // Reopen: both names survived with their positions.
  await bus.close();
  const again = createBus({ path: dir });
  assert.equal(await again.register('direct-advance'), 1);
  assert.equal(await again.register('direct-read'), 0);
  assert.deepEqual(await again.read('direct-advance'), []);
  await again.close();
});

test('advance: setting current value succeeds; going backwards rejects RangeError', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.register('c');
  assert.equal(await bus.advance('c', 2), 2);
  assert.equal(await bus.advance('c', 2), 2); // same value, no error
  await assert.rejects(bus.advance('c', 1), RangeError);
  await assert.rejects(bus.advance('c', 0), RangeError);
  // Value unchanged after the rejected attempts.
  assert.equal(await bus.register('c'), 2);
  await bus.close();
});

test('offset methods throw TypeError synchronously for bad names; advance rejects bad position', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  for (const bad of [undefined, null, '', 7, true, {}, []]) {
    assert.throws(() => bus.register(bad), TypeError);
    assert.throws(() => bus.advance(bad, 0), TypeError);
    assert.throws(() => bus.read(bad), TypeError);
  }
  await bus.register('ok');
  for (const bad of [-1, 1.5, NaN, '1', null, true, {}]) {
    await assert.rejects(bus.advance('ok', bad), RangeError);
  }
  await bus.close();
});

test('after close, register/advance/read/compact reject Error; close repeatable', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.register('a');
  await bus.close();
  await bus.close();
  await assert.rejects(bus.register('a'), Error);
  await assert.rejects(bus.advance('a', 1), Error);
  await assert.rejects(bus.read('a'), Error);
  await assert.rejects(bus.compact(), Error);
});

test('positions survive restart even without compaction', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 5; i++) await bus.publish({ i });
  await bus.register('worker');
  await bus.advance('worker', 4);
  await bus.close();

  bus = createBus({ path: dir });
  assert.equal(await bus.register('worker'), 4);
  assert.deepEqual((await bus.read('worker')).map((m) => m.seq), [5]);
  await bus.close();
});

// -------------------------------------------------------------- compaction

test('compact: replay identical interval before vs after, message by message', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  const records = [];
  for (let i = 1; i <= 10; i++) {
    const r = { i, text: `msg-${i}-你好` };
    records.push(r);
    await bus.publish(r);
  }
  // Intervals covering start, middle, end and beyond-end.
  const intervals = [0, 1, 4, 7, 10, 11];
  const before = {};
  for (const from of intervals) before[from] = await bus.replay(from);

  await bus.compact();

  for (const from of intervals) {
    assert.deepEqual(await bus.replay(from), before[from], `interval from=${from} differs`);
  }
  // Full history with records and ids intact.
  const all = await bus.replay(0);
  assert.deepEqual(
    all.map((m) => m.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.deepEqual(
    all.map((m) => m.record),
    records,
  );
  await bus.close();
});

test('compact keeps published count and effective bytes; seq continues; replay count continues', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.publish({ b: 'xx' });
  await bus.publish({ c: 3, dedupKey: 'dup' });
  await bus.publish({ ignored: true, dedupKey: 'dup' }); // deduped: not counted
  await bus.replay(0);
  const before = bus.stats();

  await bus.compact();
  assert.deepEqual(bus.stats(), before);

  // The log was actually truncated.
  const logSize = (await stat(path.join(dir, 'bus.jsonl'))).size;
  assert.equal(logSize, 0);

  // Sequence continues, bytes/published only grow by effective publishes.
  const ack = await bus.publish({ d: 4 });
  assert.equal(ack.seq, 4);
  await bus.replay(0);
  const after = bus.stats();
  assert.equal(after.seq, 4);
  assert.equal(after.published, before.published + 1);
  assert.equal(after.bytes, before.bytes + Buffer.byteLength(JSON.stringify({ d: 4 })));
  assert.equal(after.replayed, before.replayed + 4);
  await bus.close();
});

test('compact: dedup still effective with original seq after compaction and restart', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  const first = await bus.publish({ order: 'A', dedupKey: 'k1' });
  await bus.publish({ other: 1, dedupKey: 'k2' });
  await bus.compact();
  await bus.close();

  bus = createBus({ path: dir });
  const again = await bus.publish({ order: 'different', dedupKey: 'k1' });
  assert.deepEqual(again, first);
  assert.equal(bus.stats().published, 2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2]);
  await bus.close();
});

test('compact: offsets survive and reads stay consistent', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 6; i++) await bus.publish({ i });
  await bus.register('fast');
  await bus.advance('fast', 5);
  await bus.register('slow');
  await bus.advance('slow', 2);
  await bus.compact();
  await bus.close();

  bus = createBus({ path: dir });
  assert.equal(await bus.register('fast'), 5);
  assert.equal(await bus.register('slow'), 2);
  assert.deepEqual((await bus.read('fast')).map((m) => m.seq), [6]);
  assert.deepEqual((await bus.read('slow')).map((m) => m.seq), [3, 4, 5, 6]);
  await bus.advance('slow', 6);
  assert.deepEqual(await bus.read('slow'), []);
  await bus.close();
});

test('concurrent compact with publish/replay/advance: all consistent, no loss or duplication', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir, fsync: true });
  for (let i = 1; i <= 20; i++) await bus.publish({ i });
  await bus.register('w');

  const jobs = [];
  // Fire a mixed batch without awaiting: the internal chain serializes the
  // disk work, so every result must agree with one serial history.
  for (let i = 21; i <= 60; i++) jobs.push(bus.publish({ i }));
  jobs.push(bus.compact());
  for (let i = 61; i <= 80; i++) jobs.push(bus.publish({ i, dedupKey: `k-${i}` }));
  jobs.push(bus.replay(50));
  jobs.push(bus.advance('w', 40));
  jobs.push(bus.compact());
  for (let i = 81; i <= 100; i++) jobs.push(bus.publish({ i }));
  jobs.push(bus.read('w'));
  const results = await Promise.allSettled(jobs);
  assert.ok(results.every((r) => r.status === 'fulfilled'));

  assert.equal(bus.stats().seq, 100);
  assert.equal(bus.stats().published, 100);
  const all = await bus.replay(0);
  assert.deepEqual(
    all.map((m) => m.seq),
    Array.from({ length: 100 }, (_, i) => i + 1),
  );
  assert.deepEqual(
    all.map((m) => m.record.i),
    Array.from({ length: 100 }, (_, i) => i + 1),
  );
  assert.ok((await bus.register('w')) >= 40);
  await bus.close();

  // And after restart: identical picture.
  const reopened = createBus({ path: dir });
  const all2 = await reopened.replay(0);
  assert.deepEqual(
    all2.map((m) => m.seq),
    Array.from({ length: 100 }, (_, i) => i + 1),
  );
  assert.equal(reopened.stats().published, 100);
  await reopened.close();
});

test('compact on empty bus is safe and replay stays empty', async () => {
  const dir = await freshDir();
  const bus = createBus({ path: dir });
  await bus.compact();
  assert.deepEqual(bus.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  assert.deepEqual(await bus.replay(0), []);
  await bus.close();
  const reopened = createBus({ path: dir });
  assert.deepEqual(await reopened.replay(0), []);
  await reopened.close();
});

// ----------------------------------------------------- crash / recovery

test('reopen after compact: stats and full replay equal pre-compact state', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 8; i++) {
    await bus.publish({ i });
  }
  await bus.publish({ d: 1, dedupKey: 'dk' });
  await bus.replay(0);
  await bus.advance('cg', 8);
  const beforeReplay = await bus.replay(0);
  const before = bus.stats();
  await bus.compact();
  await bus.close();

  bus = createBus({ path: dir });
  assert.deepEqual(bus.stats(), before);
  assert.deepEqual(await bus.replay(0), beforeReplay);
  assert.equal(await bus.register('cg'), 8);
  // Dedup table intact.
  const dup = await bus.publish({ d: 99, dedupKey: 'dk' });
  assert.equal(dup.seq, beforeReplay[beforeReplay.length - 1].seq);
  await bus.close();
});

test('crash with stale snapshot.tmp: discarded, committed state recovered', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 3; i++) await bus.publish({ i });
  await bus.compact();
  await bus.publish({ i: 4 });
  await bus.close();

  // Half-written temp left by a crash during staging.
  await appendFile(
    path.join(dir, 'snapshot.tmp'),
    '{"v":1,"seq":99,"messages":[{"seq":1,',
  );

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 4);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  assert.equal((await bus.publish({ i: 5 })).seq, 5);
  await bus.close();
});

test('crash after rename before truncate: overlapping log discarded, no duplicates', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let i = 1; i <= 4; i++) await bus.publish({ i });
  await bus.advance('ov', 3);
  await bus.compact();
  await bus.close();

  // Snapshot covers 1..4; append a copy of the "old" log plus offset/replay
  // markers, as if truncate never ran.
  const log = (await readFile(path.join(dir, 'bus.jsonl'), 'utf8')) || '';
  const entries = [];
  for (let i = 1; i <= 4; i++) {
    entries.push(
      JSON.stringify({ t: 'm', seq: i, id: `old-${i}`, bytes: 10, record: { old: i } }),
    );
  }
  entries.push(JSON.stringify({ t: 'o', name: 'ov', pos: 1 })); // older value
  entries.push(JSON.stringify({ t: 's', n: 4, total: 999 })); // inflated counter
  await appendFile(path.join(dir, 'bus.jsonl'), log + entries.join('\n') + '\n');

  bus = createBus({ path: dir });
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2, 3, 4]);
  assert.equal(bus.stats().published, 4);
  // Snapshot held the newer position; stale marker cannot move it back.
  assert.equal(await bus.register('ov'), 3);
  // Next publish continues at 5, proving no double-applied history.
  assert.equal((await bus.publish({ fresh: true })).seq, 5);
  await bus.close();
});

test('torn trailing line after compaction is dropped and resume is clean', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.compact();
  await bus.close();

  // New message torn mid-line; snapshot already covers seq 1.
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    '{"t":"m","seq":2,"id":"z","bytes":1,"record":{',
  );

  bus = createBus({ path: dir });
  assert.equal(bus.stats().seq, 1);
  assert.equal((await bus.publish({ a: 2 })).seq, 2);
  assert.deepEqual((await bus.replay(0)).map((m) => m.seq), [1, 2]);
  await bus.close();
});

test('corrupt snapshot.json (no temp) is surfaced rather than silently split-brain', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  await bus.publish({ a: 1 });
  await bus.compact();
  await bus.close();

  // A garbage committed snapshot cannot be reconciled safely.
  await appendFile(path.join(dir, 'snapshot.json'), 'NOT JSON');
  assert.throws(() => createBus({ path: dir }), SyntaxError);
});

test('multiple compactions and restarts: nothing lost, stats never shrink', async () => {
  const dir = await freshDir();
  let bus = createBus({ path: dir });
  for (let round = 0; round < 4; round++) {
    for (let i = 0; i < 5; i++) await bus.publish({ round, i });
    await bus.compact();
    await bus.close();
    bus = createBus({ path: dir });
    assert.equal(bus.stats().published, (round + 1) * 5);
    assert.equal(bus.stats().seq, (round + 1) * 5);
  }
  const all = await bus.replay(0);
  assert.equal(all.length, 20);
  assert.deepEqual(
    all.map((m) => m.seq),
    Array.from({ length: 20 }, (_, i) => i + 1),
  );
  await bus.close();
});

test('default export removed; createBus still a named export', async () => {
  const mod = await import('../src/index.js');
  assert.equal(typeof mod.createBus, 'function');
  assert.ok(!('default' in mod));
});
