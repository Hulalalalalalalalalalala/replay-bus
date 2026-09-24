import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, execSync } from 'node:child_process';
import { rm, mkdir, chmod, appendFile } from 'node:fs/promises';
import {
  writeFileSync,
  readFileSync,
  readdirSync,
  existsSync,
  mkdirSync,
} from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const workerPath = fileURLToPath(new URL('../test-support/worker.js', import.meta.url));
const dirs = [];

async function freshDir() {
  const dir = path.join(tmpdir(), `replay-bus-mp-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  dirs.push(dir);
  return dir;
}

after(async () => {
  await Promise.all(
    dirs.map((d) =>
      chmod(d, 0o755).catch(() => {}).then(() => rm(d, { recursive: true, force: true })),
    ),
  );
});

// Short, shared lease timings for every spawned process: takeover after a
// crashed owner lands in a few hundred ms, while a live owner keeps renewing.
const TIMING_ENV = {
  REPLAY_BUS_LEASE_MS: '300',
  REPLAY_BUS_RENEW_MS: '150',
  REPLAY_BUS_SKEW_MS: '100',
  REPLAY_BUS_RETRY_MS: '15',
};

class Worker {
  constructor(label = 'w') {
    this.label = label;
    this.nextId = 1;
    this.pending = new Map();
    this.proc = spawn(process.execPath, [workerPath], {
      env: { ...process.env, ...TIMING_ENV },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let buf = '';
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', (chunk) => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        let msg;
        try {
          msg = JSON.parse(line);
        } catch {
          continue; // ignore any non-frame noise
        }
        const p = this.pending.get(msg.id);
        if (!p) continue;
        this.pending.delete(msg.id);
        if (msg.ok) p.resolve(msg.value);
        else {
          const err = new Error(msg.error?.message || 'unknown worker error');
          err.name = msg.error?.name || 'Error';
          p.reject(err);
        }
      }
    });
    let errBuf = '';
    this.proc.stderr.on('data', (c) => {
      errBuf += c;
    });
    this.proc.on('exit', (code) => {
      const rest = errBuf;
      for (const [, p] of this.pending) {
        p.reject(new Error(`worker ${label} exited (${code})${rest ? ': ' + rest : ''}`));
      }
      this.pending.clear();
      this.exited = true;
    });
  }

  call(op, ...args) {
    if (this.exited) return Promise.reject(new Error(`worker ${this.label} exited`));
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`worker ${this.label} call ${op} timed out`));
      }, 15000);
      this.pending.set(id, {
        resolve: (v) => {
          clearTimeout(timer);
          resolve(v);
        },
        reject: (e) => {
          clearTimeout(timer);
          reject(e);
        },
      });
      this.proc.stdin.write(JSON.stringify({ id, op, args }) + '\n');
    });
  }

  open(dir, opts = {}) {
    return this.call('open', { path: dir, ...opts });
  }
  publish(r) {
    return this.call('publish', r);
  }
  publishBatch(rs) {
    return this.call('publishBatch', rs);
  }
  replay(from) {
    return this.call('replay', from);
  }
  register(n) {
    return this.call('register', n);
  }
  advance(n, to) {
    return this.call('advance', n, to);
  }
  read(n) {
    return this.call('read', n);
  }
  truncate(b) {
    return this.call('truncate', b);
  }
  compact() {
    return this.call('compact');
  }
  stats() {
    return this.call('stats');
  }
  usage() {
    return this.call('usage');
  }
  sleep(ms) {
    return this.call('sleep', ms);
  }
  publishAndHold(r) {
    return this.call('publishAndHold', r);
  }
  publishAndCrash(r) {
    return this.call('publishAndCrash', r);
  }
  async end() {
    if (this.exited) return;
    // Best-effort graceful close, then always release the process so a wedged
    // worker can never hang test teardown.
    const force = setTimeout(() => {
      try {
        this.proc.kill('SIGKILL');
      } catch {
        // already gone
      }
    }, 3000);
    force.unref?.();
    try {
      await this.call('close');
    } catch {
      // already gone / wedged: force kill below
    }
    try {
      this.proc.stdin.end();
    } catch {
      // ignore
    }
    await new Promise((resolve) => {
      if (this.exited) return resolve();
      this.proc.on('exit', resolve);
      setTimeout(resolve, 3000).unref?.();
    });
    clearTimeout(force);
    try {
      this.proc.kill('SIGKILL');
    } catch {
      // ignore
    }
  }
}

const segmentFiles = (dir) => readdirSync(dir).filter((n) => /^bus\.\d+\.jsonl$/.test(n));

// Force a brand-new ownership term into the lock directory with a far-future
// deadline, as though a different process had taken over. Deterministically
// fences a live worker without waiting for a lease to expire.
const forceNextOwner = (dir, ownerId) => {
  const lockDir = path.join(dir, '.bus.lock');
  const epochs = readdirSync(lockDir)
    .map((n) => /^lease-(\d+)$/.exec(n))
    .filter(Boolean)
    .map((m) => Number(m[1]));
  const epoch = (epochs.length ? Math.max(...epochs) : 0) + 1;
  const term = path.join(lockDir, `lease-${String(epoch).padStart(10, '0')}`);
  mkdirSync(term);
  writeFileSync(
    path.join(term, 'owner.json'),
    JSON.stringify({
      owner: ownerId || `forced-${Math.random()}`,
      epoch,
      acquiredAt: Date.now(),
      expiresAt: Date.now() + 600000,
    }),
  );
  return epoch;
};

test('multi-process: clean handoff continues the sequence and merges stats', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  const a1 = await a.publish({ i: 1 });
  assert.equal(a1.seq, 1);
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  const b2 = await b.publish({ i: 2 });
  assert.equal(b2.seq, 2);
  const got = await b.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2],
  );
  assert.deepEqual(
    got.map((m) => m.record),
    [{ i: 1 }, { i: 2 }],
  );
  assert.equal((await b.stats()).published, 2);
  await b.end();
});

test('multi-process: takeover after a crashed (stopped) owner severs nothing extra and continues', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publishAndCrash({ i: 1 });

  const b = new Worker('B');
  await b.open(dir);
  // A neither closed nor renewed; B waits out the term and takes over.
  const ack = await b.publish({ i: 2 });
  assert.equal(ack.seq, 2);
  const got = await b.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2],
  );
  assert.deepEqual((await b.stats()).published, 2);
  await b.end();
});

test('multi-process: three-process chain takeover keeps one continuous suffix', async () => {
  const dir = await freshDir();

  const a = new Worker('A');
  await a.open(dir);
  await a.publishAndCrash({ i: 1 });

  const b = new Worker('B');
  await b.open(dir);
  assert.equal((await b.publish({ i: 2 })).seq, 2);
  await b.publishAndCrash({ i: 3 });

  const c = new Worker('C');
  await c.open(dir);
  assert.equal((await c.publish({ i: 4 })).seq, 4);
  const got = await c.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3, 4],
  );
  assert.deepEqual((await c.stats()).published, 4);
  await c.end();
});

test('multi-process: predecessor half-written batch is severed on takeover, no gap no dup', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publish({ i: 1 });
  await a.publishAndCrash({ i: 2 });

  // A crashed (or was killed) mid second group: begin + one message line, no
  // commit, claiming seq 3.
  await appendFile(path.join(dir, 'bus.jsonl'), JSON.stringify({ t: 'b', id: 'dead' }) + '\n');
  await appendFile(
    path.join(dir, 'bus.jsonl'),
    JSON.stringify({ t: 'm', seq: 3, id: 'z', bytes: 1, record: { i: 3 } }) + '\n',
  );

  const b = new Worker('B');
  await b.open(dir);
  const ack = await b.publish({ i: 3 });
  assert.equal(ack.seq, 3);
  const got = await b.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    got.map((m) => m.record),
    [{ i: 1 }, { i: 2 }, { i: 3 }],
  );
  assert.equal((await b.stats()).published, 3);
  await b.end();
});

test('multi-process: a live, renewing owner blocks a contender past the bounded window', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publishAndHold({ i: 1 }); // keeps renewing, never closes

  const b = new Worker('B');
  await b.open(dir);
  await assert.rejects(b.publish({ i: 2 }), (err) => err.name === 'Error');
  // The blocked contender wrote nothing and consumed no seq.
  const held = await a.stats();
  assert.equal(held.seq, 1);
  assert.equal(held.published, 1);
  // A is still the owner and can keep writing.
  assert.equal((await a.publish({ i: 99 })).seq, 2);
  await a.end();
  await b.end();
});

test('multi-process: fenced old process cannot publish, batch or truncate; reads still work', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publishAndHold({ i: 1 });

  // Ownership is stolen out from under A.
  forceNextOwner(dir);

  // Every write-path call rejects with a plain Error.
  await assert.rejects(a.publish({ i: 2 }), (e) => e.name === 'Error');
  await assert.rejects(a.publishBatch([{ i: 2 }]), (e) => e.name === 'Error');
  await assert.rejects(a.truncate(1), (e) => e.name === 'Error');
  await assert.rejects(a.compact(), (e) => e.name === 'Error');

  // Read paths keep working and show the pre-takeover history.
  const got = await a.read('x');
  assert.deepEqual(
    got.map((m) => m.seq),
    [1],
  );
  assert.equal((await a.stats()).seq, 1);

  // The rejected calls left no trace in the log.
  const raw = readFileSync(path.join(dir, 'bus.jsonl'), 'utf8');
  assert.ok(!raw.includes('"i":2'));
  await a.end();
});

test('multi-process: dedup key is effective once across writers and restarts', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  const first = await a.publish({ order: 'A', dedupKey: 'k1' });
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  // Resend by the new owner rides A's first acknowledgement, no seq/quota.
  assert.deepEqual(await b.publish({ order: 'B', dedupKey: 'k1' }), first);
  assert.equal((await b.stats()).published, 1);
  assert.equal((await b.stats()).seq, 1);
  const next = await b.publish({ order: 'C' });
  assert.equal(next.seq, 2);
  await b.end();

  const c = new Worker('C');
  await c.open(dir);
  assert.deepEqual(await c.publish({ order: 'D', dedupKey: 'k1' }), first);
  assert.equal((await c.stats()).published, 2);
  const got = await c.replay(0);
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2],
  );
  await c.end();
});

test('multi-process: batches from successive owners commit continuously', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  const g1 = await a.publishBatch([{ i: 1 }, { i: 2 }]);
  assert.deepEqual(g1.map((x) => x.seq), [1, 2]);
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  const g2 = await b.publishBatch([{ i: 3 }, { i: 4, dedupKey: 'd' }]);
  assert.deepEqual(g2.map((x) => x.seq), [3, 4]);
  const g3 = await b.publishBatch([{ i: 5, dedupKey: 'd' }, { i: 6 }]);
  // Cross-call, cross-process resend inside a batch reuses the ack.
  assert.deepEqual(g3[0], g2[1]);
  assert.equal(g3[1].seq, 5);
  const got = await b.replay(0);
  assert.deepEqual(got.map((m) => m.seq), [1, 2, 3, 4, 5]);
  await b.end();
});

test('multi-process: consumer positions are one shared, monotonic map', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.advance('c', 2);
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  // The position survived the handover, not reset.
  assert.equal(await b.register('c'), 2);
  await b.advance('c', 5);

  // A reopened second process observes the newer value.
  const a2 = new Worker('A2');
  await a2.open(dir);
  assert.equal(await a2.register('c'), 5);
  // Never backwards, even from another process.
  await assert.rejects(a2.advance('c', 3), (e) => e.name === 'RangeError');
  await a2.advance('c', 7);
  assert.equal(await b.register('c'), 7);
  await b.end();
  await a2.end();
});

test('multi-process: replay counter accumulates across writers', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publish({ i: 1 });
  await a.publish({ i: 2 });
  await a.replay(0); // 2
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  await b.publish({ i: 3 });
  await b.replay(0); // 3
  assert.equal((await b.stats()).replayed, 5);
  await b.end();
});

test('multi-process: truncate under a new owner deletes whole segments for good', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  for (let i = 1; i <= 5; i++) await a.publish({ i });
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  for (let i = 6; i <= 8; i++) await b.publish({ i });
  // No registered consumers: the first segment (1..5) is deletable.
  await b.truncate(8);
  assert.deepEqual(segmentFiles(dir), []);
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), []);
  // Cumulative stats survive; the sequence continues.
  assert.equal((await b.stats()).published, 8);
  assert.equal((await b.publish({ i: 9 })).seq, 9);

  // A third process sees the same horizon and contiguous suffix.
  await b.end();
  const c = new Worker('C');
  await c.open(dir);
  assert.deepEqual((await c.replay(0)).map((m) => m.seq), [9]);
  assert.equal((await c.stats()).published, 9);
  await c.end();
});

test('multi-process: compaction by a new owner keeps replay, dedup and stats stable', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  const first = await a.publish({ i: 1, dedupKey: 'k' });
  for (let i = 2; i <= 4; i++) await a.publish({ i });
  await a.end();

  const b = new Worker('B');
  await b.open(dir);
  await b.publish({ i: 5 });
  await b.compact();
  assert.deepEqual((await b.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5]);
  assert.deepEqual(await b.publish({ x: 1, dedupKey: 'k' }), first);
  assert.equal((await b.publish({ i: 6 })).seq, 6);
  await b.end();

  const c = new Worker('C');
  await c.open(dir);
  assert.deepEqual((await c.replay(0)).map((m) => m.seq), [1, 2, 3, 4, 5, 6]);
  assert.deepEqual(await c.publish({ x: 2, dedupKey: 'k' }), first);
  await c.end();
});

test('multi-process: quota occupancy merges and frees across owners', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir, { maxBytes: 100000 });
  const r1 = { pad: 'x'.repeat(100) };
  await a.publish(r1);
  const used = await a.usage();
  assert.ok(used > 0);
  await a.end();

  const b = new Worker('B');
  await b.open(dir, { maxBytes: 100000 });
  // Occupancy survived the handover.
  assert.equal(await b.usage(), used);
  await b.truncate(1); // no consumers: frees the first segment
  assert.equal(await b.usage(), 0);
  const ack = await b.publish({ fresh: true });
  assert.equal(ack.seq, 2); // sequence continues, only occupancy was freed
  await b.end();
});

test('multi-process: removing the directory rejects writes with Error, reads keep working', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publish({ i: 1 });
  await a.sleep(20); // let the idle lease release land

  await rm(dir, { recursive: true, force: true });

  // Reads do not throw; there is simply nothing on disk anymore.
  assert.deepEqual(await a.read('r'), []);
  // Acquiring the write lease needs to create files in the missing dir.
  await assert.rejects(a.publish({ i: 2 }), (e) => e.name === 'Error');
  await assert.rejects(a.publishBatch([{ i: 2 }]), (e) => e.name === 'Error');
  await a.end();
});

test('multi-process: an unwritable directory rejects writes with Error', { skip: process.getuid?.() === 0 }, async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publish({ i: 1 });
  await a.end(); // cleanly release the lease

  // A *fresh* opener on an already read-only directory: construction and all
  // read paths work, but acquiring write ownership fails because it must
  // create entries in the directory.
  execSync(`chmod -R a=rX "${dir}"`);
  const b = new Worker('B');
  try {
    await b.open(dir);
    // Read paths are unaffected.
    assert.equal((await b.stats()).seq, 1);
    assert.deepEqual((await b.read('r')).map((m) => m.seq), [1]);
    // Every write path rejects with a plain Error, writes nothing.
    await assert.rejects(b.publish({ i: 2 }), (e) => e.name === 'Error');
    await assert.rejects(b.publishBatch([{ i: 2 }]), (e) => e.name === 'Error');
    await assert.rejects(b.truncate(1), (e) => e.name === 'Error');
    assert.equal((await b.stats()).seq, 1);
  } finally {
    execSync(`chmod -R u+wX "${dir}"`);
    await b.end();
  }
});

test('multi-process: a fenced owner stays fenced even after the directory later goes idle', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  await a.publishAndHold({ i: 1 });
  forceNextOwner(dir);

  // The first post-takeover call rejects...
  await assert.rejects(a.publish({ i: 2 }), (e) => e.name === 'Error');
  // ...and a later one still rejects, permanently; reads keep working.
  await assert.rejects(a.publish({ i: 3 }), (e) => e.name === 'Error');
  assert.deepEqual((await a.read('c')).map((m) => m.seq), [1]);
  await a.end();
});

test('multi-process: two processes advancing one position settle in call order', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  const b = new Worker('B');
  await a.open(dir);
  await a.publish({ i: 1 });
  // Both see/register the same consumer.
  await b.open(dir);
  await a.register('c');
  await b.register('c');
  await b.advance('c', 1);
  await a.advance('c', 1); // idempotent, not backwards
  assert.equal(await a.register('c'), 1);
  assert.equal(await b.register('c'), 1);
  // A backwards move from either process is rejected against the shared map.
  await assert.rejects(a.advance('c', 0), (e) => e.name === 'RangeError');
  await assert.rejects(b.advance('c', 0), (e) => e.name === 'RangeError');
  await b.advance('c', 4);
  assert.equal(await a.register('c'), 4);
  await a.end();
  await b.end();
});

test('multi-process: truncate by the new owner right after expiry is whole and durable', async () => {
  const dir = await freshDir();
  const a = new Worker('A');
  await a.open(dir);
  for (let i = 1; i <= 6; i++) await a.publish({ i });
  await a.publishAndCrash({ i: 7 });

  const b = new Worker('B');
  await b.open(dir);
  // First call after the crashed owner: take over, then truncate away the
  // whole history (no registered consumers), in one ownership acquisition.
  await b.truncate(7);
  assert.deepEqual(segmentFiles(dir), []);
  assert.deepEqual(await b.replay(0), []);
  assert.equal((await b.stats()).published, 7);
  const ack = await b.publish({ i: 8 });
  assert.equal(ack.seq, 8);
  await b.end();

  const c = new Worker('C');
  await c.open(dir);
  assert.deepEqual((await c.replay(0)).map((m) => m.seq), [8]);
  assert.equal((await c.stats()).published, 8);
  await c.end();
});
