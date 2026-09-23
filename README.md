# replay-bus

Append-only message bus for one process: every publish is durable before it is acknowledged, and a replay from a sequence number never repeats a message twice.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createBus({ path, fsync = false }) -> Bus` (named export only).
- `Bus.publish(record) -> Promise<{ id, seq }>`.
- `Bus.replay(from = 0) -> Promise<Array<{ seq, id, record }>>` — inclusive of `from`, ascending.
- `Bus.register(name) -> number` — register a consumer and return its position (0 for a new name; re-registering never resets it). Synchronous.
- `Bus.advance(name, to) -> number` — move a consumer's position to `to` (non-negative integer, never backwards; setting the current value is a no-op success). Synchronous.
- `Bus.read(name) -> Array<{ seq, id, record }>` — messages after the consumer's position, same shape as replay. Does not move the position; empty array past the end. Synchronous.
- `Bus.compact() -> Promise<void>` — fold all published messages, positions, dedup state and counters into a snapshot and truncate the log. Replay results, dedup semantics and stats are unchanged by compaction.
- `Bus.stats() -> { seq, bytes, published, replayed }`.
- `Bus.close() -> Promise<void>` — idempotent.

Unknown consumer names are auto-registered by `advance`/`read` exactly as if `register` had been called first. Positions, the dedup table and stats survive restarts; a crash anywhere inside `compact` (half-written snapshot, torn log tail) recovers to a consistent state with no lost or duplicated messages.

## Tests

    npm test

## Limits

One process owns a bus directory; no cross-process coordination.
Records must be JSON-serialisable.
No network transport and no broker integration.
