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
- `Bus.publishBatch(records) -> Promise<Array<{ id, seq }>>` — publish a group atomically. `records` must be an array (a non-array throws `TypeError` synchronously); an empty array resolves to `[]`. The whole group is validated first: if any record is not a JSON object or cannot be represented in JSON, the call rejects with `TypeError` and the bus is left byte-for-byte as it was (no seq/bytes/published/dedup/position change). Otherwise the group commits in one bracketed write: either every effective record lands or, after a crash, none of them is visible on reopen. Acknowledgements come back in input order, each shaped exactly like a `publish` result; effective seqs are continuous. Repeated `dedupKey`s within the group, or keys already in history (including after restart), reuse the first acknowledgement and allocate no new seq. After `close` the call rejects with `Error`.
- `Bus.replay(from = 0) -> Promise<Array<{ seq, id, record }>>` — inclusive of `from`, ascending.
- `Bus.register(name) -> number` — register a consumer and return its position (0 for a new name; re-registering never resets it). Synchronous.
- `Bus.advance(name, to) -> number` — move a consumer's position to `to` (non-negative integer, never backwards; setting the current value is a no-op success). Synchronous.
- `Bus.read(name) -> Array<{ seq, id, record }>` — messages after the consumer's position, same shape as replay. Does not move the position; empty array past the end. Synchronous.
- `Bus.compact() -> Promise<void>` — fold all published messages, positions, dedup state and counters into a snapshot and truncate the log. Replay results, dedup semantics and stats are unchanged by compaction.
- `Bus.stats() -> { seq, bytes, published, replayed }`.
- `Bus.close() -> Promise<void>` — idempotent.

Unknown consumer names are auto-registered by `advance`/`read` exactly as if `register` had been called first. Positions, the dedup table and stats survive restarts; a crash anywhere inside `compact` (half-written snapshot, torn log tail) or inside a batch (a half-written group bracket) recovers to a consistent state with no lost or duplicated messages. The post-compaction log is replaced via a synced temp file plus an atomic rename (rather than truncating an open append handle), so compaction also works on Windows.

## Tests

    npm test

## Limits

One process owns a bus directory; no cross-process coordination.
Records must be JSON-serialisable.
No network transport and no broker integration.
