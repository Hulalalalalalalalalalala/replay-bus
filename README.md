# replay-bus

Append-only message bus for one process: every publish is durable before it is acknowledged, a replay from a sequence number never repeats a message twice, named consumers track their own read position, and `compact()` rewrites history into a snapshot without changing replay results or statistics.

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
- `Bus.register(name) -> Promise<number>` — register a consumer; returns its
  position, zero on first registration. Registering again leaves the current
  position untouched.
- `Bus.advance(name, position) -> Promise<number>` — move the consumer's
  position to `position` (a non-negative integer). Never moves backwards;
  setting it to the current value is a successful no-op. An unknown name is
  registered automatically.
- `Bus.read(name) -> Promise<Array<{ seq, id, record }>>` — return every
  message after the consumer's current position. Reading never moves the
  position; land it with `advance`. Past the end this resolves to `[]`.
- `Bus.compact() -> Promise<void>` — fold all published messages, consumer
  positions and the dedup table into `snapshot.json` and truncate the
  obsolete log. Replay results, dedup semantics and `stats()` are unchanged.
- `Bus.stats() -> { seq, bytes, published, replayed }`.
- `Bus.close() -> Promise<void>` (idempotent).

Consumer names must be non-empty strings (a `TypeError` is thrown
synchronously by `register`/`advance`/`read`). `advance` rejects with a
`RangeError` for a position that is not a non-negative integer or that goes
backwards. After `close`, `publish`, `replay`, `register`, `advance`, `read`
and `compact` reject with an `Error`.

## On-disk files

- `bus.jsonl` — append-only message/offset/replay log.
- `snapshot.json` — committed compaction snapshot covering `seq 1..n`.
- `snapshot.tmp` — staging file for the next snapshot; discarded on open.

Recovery tolerates a torn trailing log line, a half-written `snapshot.tmp`,
and a snapshot committed while the log truncation did not run: in every case
the reopened bus has no lost or duplicated messages and consistent positions
and dedup state.

## Tests

    npm test

## Limits

One process owns a bus directory; no cross-process coordination.
Records must be JSON-serialisable.
No network transport and no broker integration.
