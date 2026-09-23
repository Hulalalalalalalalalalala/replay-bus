import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createBus } from '../src/index.js';

const tmpdir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'replay-bus-'));

test('module entry exposes createBus', async () => {
  const m = await import('../src/index.js');
  assert.deepEqual(Object.keys(m), ['createBus']);
  assert.equal(typeof createBus, 'function');
});

test('createBus throws TypeError without a non-empty string path', () => {
  for (const bad of [undefined, null, {}, { path: '' }, { path: 42 }, { path: null }]) {
    assert.throws(() => createBus(bad), TypeError);
  }
});

test('publish assigns consecutive seq from 1 and acknowledges after write', async () => {
  const bus = createBus({ path: tmpdir() });
  const a = await bus.publish({ order: 1 });
  const b = await bus.publish({ order: 2 });
  assert.equal(a.seq, 1);
  assert.equal(b.seq, 2);
  assert.ok(a.id && b.id && a.id !== b.id);
  assert.deepEqual(bus.stats().seq, 2);
  await bus.close();
});

test('concurrent publishes take seq in call order', async () => {
  const bus = createBus({ path: tmpdir() });
  const receipts = await Promise.all([
    bus.publish({ n: 1 }),
    bus.publish({ n: 2 }),
    bus.publish({ n: 3 }),
  ]);
  assert.deepEqual(receipts.map((r) => r.seq), [1, 2, 3]);
  const replayed = await bus.replay();
  assert.deepEqual(replayed.map((m) => m.record.n), [1, 2, 3]);
  await bus.close();
});

test('messages survive restart and seq continues', async () => {
  const dir = tmpdir();
  const bus1 = createBus({ path: dir });
  await bus1.publish({ order: 'a' });
  await bus1.publish({ order: 'b' });
  await bus1.close();

  const bus2 = createBus({ path: dir });
  assert.equal(bus2.stats().seq, 2);
  assert.equal(bus2.stats().published, 2);
  const all = await bus2.replay(0);
  assert.deepEqual(all.map((m) => m.record.order), ['a', 'b']);
  const c = await bus2.publish({ order: 'c' });
  assert.equal(c.seq, 3);
  await bus2.close();
});

test('dedupKey redelivery reuses first receipt and stores nothing new', async () => {
  const bus = createBus({ path: tmpdir() });
  const first = await bus.publish({ dedupKey: 'order-1', amount: 10 });
  const again = await bus.publish({ dedupKey: 'order-1', amount: 10 });
  assert.deepEqual(again, first);
  assert.equal(bus.stats().published, 1);
  assert.equal(bus.stats().seq, 1);
  const all = await bus.replay();
  assert.equal(all.length, 1);
  await bus.close();
});

test('concurrent redelivery of the same dedupKey collapses to one message', async () => {
  const bus = createBus({ path: tmpdir() });
  const [a, b] = await Promise.all([
    bus.publish({ dedupKey: 'k', v: 1 }),
    bus.publish({ dedupKey: 'k', v: 1 }),
  ]);
  assert.deepEqual(a, b);
  assert.equal(bus.stats().published, 1);
  await bus.close();
});

test('dedup keys stay deduplicated across restart', async () => {
  const dir = tmpdir();
  const bus1 = createBus({ path: dir });
  const first = await bus1.publish({ dedupKey: 'order-9', v: 1 });
  await bus1.close();

  const bus2 = createBus({ path: dir });
  const again = await bus2.publish({ dedupKey: 'order-9', v: 1 });
  assert.deepEqual(again, first);
  assert.equal(bus2.stats().published, 1);
  assert.equal((await bus2.replay()).length, 1);
  await bus2.close();
});

test('replay returns only messages after from, ascending, empty past the end', async () => {
  const bus = createBus({ path: tmpdir() });
  for (const n of [1, 2, 3]) await bus.publish({ n });
  assert.deepEqual((await bus.replay()).map((m) => m.seq), [1, 2, 3]);
  assert.deepEqual((await bus.replay(1)).map((m) => m.seq), [2, 3]);
  assert.deepEqual((await bus.replay(3)), []);
  assert.deepEqual((await bus.replay(99)), []);
  const batch = await bus.replay(2);
  assert.equal(batch.length, 1);
  assert.deepEqual(batch[0].record, { n: 3 });
  await bus.close();
});

test('replay rejects RangeError for a non non-negative-integer start', async () => {
  const bus = createBus({ path: tmpdir() });
  for (const bad of [-1, 1.5, NaN, '1', null, Infinity]) {
    await assert.rejects(bus.replay(bad), RangeError);
  }
  await bus.close();
});

test('stats tracks seq, bytes, published and replayed across restart', async () => {
  const dir = tmpdir();
  const r1 = { a: 'x' };
  const r2 = { b: 'yz', n: 3 };
  const expectedBytes =
    Buffer.byteLength(JSON.stringify(r1), 'utf8') + Buffer.byteLength(JSON.stringify(r2), 'utf8');

  const bus1 = createBus({ path: dir });
  assert.deepEqual(bus1.stats(), { seq: 0, bytes: 0, published: 0, replayed: 0 });
  await bus1.publish(r1);
  await bus1.publish(r2);
  await bus1.publish({ ...r1, dedupKey: 'd' });
  await bus1.publish({ ...r1, dedupKey: 'd' });
  await bus1.replay(0);
  await bus1.replay(1);
  const s1 = bus1.stats();
  const bytesWithDedup = s1.bytes;
  assert.equal(s1.seq, 3);
  assert.equal(s1.published, 3);
  assert.equal(s1.replayed, 5); // 3 from replay(0) + 2 from replay(1)
  await bus1.close();

  const bus2 = createBus({ path: dir });
  const s2 = bus2.stats();
  assert.equal(s2.seq, 3);
  assert.equal(s2.published, 3);
  assert.equal(s2.replayed, 5);
  assert.equal(s2.bytes, bytesWithDedup);
  await bus2.replay(3);
  assert.equal(bus2.stats().replayed, 5);
  await bus2.replay(0);
  assert.equal(bus2.stats().replayed, 8);
  await bus2.close();

  const bus3 = createBus({ path: dir });
  assert.equal(bus3.stats().replayed, 8);
  await bus3.close();

  assert.ok(expectedBytes > 0);
});

test('bytes counts compact JSON UTF-8 of each effective publish only', async () => {
  const bus = createBus({ path: tmpdir() });
  const record = { msg: '订单', n: 1 };
  await bus.publish(record);
  await bus.publish({ ...record, dedupKey: 'x' });
  await bus.publish({ ...record, dedupKey: 'x' }); // redelivery: no extra bytes
  const expected =
    Buffer.byteLength(JSON.stringify(record), 'utf8') +
    Buffer.byteLength(JSON.stringify({ ...record, dedupKey: 'x' }), 'utf8');
  assert.equal(bus.stats().bytes, expected);
  await bus.close();
});

test('publish rejects TypeError for non-object records', async () => {
  const bus = createBus({ path: tmpdir() });
  for (const bad of [undefined, null, 42, 'x', true, [1, 2], () => {}]) {
    await assert.rejects(bus.publish(bad), TypeError);
  }
  await bus.close();
});

test('publish rejects TypeError for values JSON cannot hold', async () => {
  const bus = createBus({ path: tmpdir() });
  const circular = {};
  circular.self = circular;
  const bads = [
    { v: NaN },
    { v: Infinity },
    { v: -Infinity },
    { v: undefined },
    { v: () => {} },
    { v: 10n },
    { nested: [{ v: BigInt(1) }] },
    circular,
  ];
  for (const bad of bads) {
    await assert.rejects(bus.publish(bad), TypeError);
  }
  assert.equal(bus.stats().published, 0);
  await bus.close();
});

test('publish rejects TypeError when dedupKey is present but not a string', async () => {
  const bus = createBus({ path: tmpdir() });
  for (const bad of [{ dedupKey: 1 }, { dedupKey: null }, { dedupKey: {} }, { dedupKey: undefined }]) {
    await assert.rejects(bus.publish(bad), TypeError);
  }
  await bus.close();
});

test('close is idempotent; publish and replay reject Error after, stats still readable', async () => {
  const bus = createBus({ path: tmpdir() });
  await bus.publish({ n: 1 });
  await bus.close();
  await bus.close();
  await assert.rejects(bus.publish({ n: 2 }), Error);
  await assert.rejects(bus.replay(), Error);
  assert.deepEqual(bus.stats(), { seq: 1, bytes: bus.stats().bytes, published: 1, replayed: 0 });
});

test('fsync option publishes and persists', async () => {
  const dir = tmpdir();
  const bus = createBus({ path: dir, fsync: true });
  const r = await bus.publish({ n: 1 });
  assert.equal(r.seq, 1);
  await bus.close();
  const again = createBus({ path: dir, fsync: true });
  assert.equal((await again.replay()).length, 1);
  await again.close();
});

test('torn trailing line from a crash is discarded on reopen', async () => {
  const dir = tmpdir();
  const bus = createBus({ path: dir });
  await bus.publish({ n: 1 });
  await bus.close();
  fs.appendFileSync(path.join(dir, 'messages.log'), '{"seq":2,"id":"x","record":');
  const again = createBus({ path: dir });
  assert.equal(again.stats().seq, 1);
  const r = await again.publish({ n: 2 });
  assert.equal(r.seq, 2);
  assert.deepEqual((await again.replay()).map((m) => m.seq), [1, 2]);
  await again.close();
});
