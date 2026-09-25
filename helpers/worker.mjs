// Multi-process test worker: opens one bus and executes newline-delimited
// JSON commands from stdin, answering each with one JSON line on stdout.
// Not a test file (node --test only picks up *.test.js).
import { createInterface } from 'node:readline';
import { createBus } from '../src/index.js';

const dir = process.env.BUS_DIR;
const maxBytes = process.env.BUS_MAX_BYTES ? Number(process.env.BUS_MAX_BYTES) : undefined;
const fsync = process.env.BUS_FSYNC === '1';
const bus = createBus(maxBytes ? { path: dir, maxBytes, fsync } : { path: dir, fsync });

const reply = (id, payload) => {
  process.stdout.write(JSON.stringify({ id, ...payload }) + '\n');
};

const run = async (cmd) => {
  switch (cmd.op) {
    case 'publish':
      return { value: await bus.publish(cmd.record) };
    case 'publishBatch':
      return { value: await bus.publishBatch(cmd.records) };
    case 'replay':
      return { value: await bus.replay(cmd.from) };
    case 'register':
      return { value: bus.register(cmd.name) };
    case 'advance':
      return { value: bus.advance(cmd.name, cmd.to) };
    case 'read':
      return { value: bus.read(cmd.name) };
    case 'readRange':
      return { value: bus.readRange(cmd.start, cmd.limit) };
    case 'compact':
      await bus.compact();
      return { value: null };
    case 'truncate':
      await bus.truncate(cmd.before);
      return { value: null };
    case 'stats':
      return { value: bus.stats() };
    case 'usage':
      return { value: bus.usage() };
    case 'close':
      await bus.close();
      return { value: null };
    default:
      throw new Error(`unknown op ${cmd.op}`);
  }
};

const rl = createInterface({ input: process.stdin });
rl.on('line', async (line) => {
  let cmd;
  try {
    cmd = JSON.parse(line);
  } catch {
    return;
  }
  try {
    reply(cmd.id, await run(cmd));
  } catch (err) {
    reply(cmd.id, { error: err.constructor.name, message: err.message });
  }
});
