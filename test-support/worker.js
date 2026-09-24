// Test-only worker: one bus instance driven by newline-delimited JSON-RPC on
// stdin so the multi-process tests can run several real OS processes against
// one bus directory. Not part of the public surface.
//
// Request:  { id, op, args }
// Response: { id, ok, value } | { id, error: { name, message } }
import { createBus } from '../src/index.js';

let bus = null;
let crashAfterFlush = false;

const fail = (id, err) => {
  process.stdout.write(
    JSON.stringify({
      id,
      error: { name: err?.name || 'Error', message: String(err?.message || err) },
    }) + '\n',
  );
};

const ok = (id, value) => {
  process.stdout.write(JSON.stringify({ id, ok: true, value }) + '\n', () => {
    if (crashAfterFlush) process.exit(0);
  });
};

const call = async (op, args = []) => {
  switch (op) {
    case 'open':
      bus = createBus(args[0]);
      return null;
    case 'publish':
      return bus.publish(args[0]);
    case 'publishBatch':
      return bus.publishBatch(args[0]);
    case 'replay':
      return bus.replay(args[0]);
    case 'register':
      return bus.register(args[0]);
    case 'advance':
      return bus.advance(args[0], args[1]);
    case 'read':
      return bus.read(args[0]);
    case 'truncate':
      return bus.truncate(args[0]);
    case 'compact':
      return bus.compact();
    case 'stats':
      return bus.stats();
    case 'usage':
      return bus.usage();
    case 'close':
      return bus.close();
    case 'sleep':
      return new Promise((resolve) => setTimeout(resolve, args[0]));
    // Publish then stay alive WITHOUT closing: the write lease is held and
    // the process keeps renewing it (used as the live, contended owner).
    case 'publishAndHold':
      return bus.publish(args[0]);
    // Publish then hard-exit with no close(): simulates a crashed owner. Its
    // lease is neither released nor renewed, so a contender takes over once
    // the term expires. The response is flushed before the exit below.
    case 'publishAndCrash': {
      const value = await bus.publish(args[0]);
      crashAfterFlush = true;
      return value;
    }
    default:
      throw new Error(`unknown op: ${op}`);
  }
};

let buffer = '';
process.stdin.on('data', (chunk) => {
  buffer += chunk.toString('utf8');
  let nl;
  while ((nl = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, nl).trim();
    buffer = buffer.slice(nl + 1);
    if (!line) continue;
    const { id, op, args } = JSON.parse(line);
    Promise.resolve()
      .then(() => call(op, args))
      .then((value) => ok(id, value))
      .catch((err) => fail(id, err));
  }
});

process.stdin.on('end', () => process.exit(0));
