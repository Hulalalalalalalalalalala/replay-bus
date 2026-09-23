# replay-bus

Append-only message bus for one process: every publish is durable before it is acknowledged, and a replay from a sequence number never repeats a message twice.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createBus({ path, fsync = false }) -> Bus`.
- `Bus.publish(record) -> Promise<{ id, seq }>`.
- `Bus.replay(from = 0) -> Promise<Array<{ seq, id, record }>>`.
- `Bus.stats() -> { seq, bytes, published, replayed }`.
- `Bus.close() -> Promise<void>`.

## Tests

    npm test

## Limits

One process owns a bus directory; no cross-process coordination.
Records must be JSON-serialisable.
No network transport and no broker integration.
