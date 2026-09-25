# replay-bus

Append-only message bus: every publish is durable before it is acknowledged, and a replay from a sequence number never repeats a message twice. The same bus directory may be opened and written by **multiple processes at once**: writers coordinate through files in the directory, and ownership carries an epoch credential that increments on every takeover, so a displaced writer can never mix bytes back into the log.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createBus({ path, fsync = false, maxBytes }) -> Bus` (named export only).
- `maxBytes` is an optional retained-byte quota. Omit it for unlimited; a given value must be a positive integer (zero, negatives, fractions and non-numbers throw `TypeError` synchronously). The quota counts business bytes of retained messages only, on the same basis as `stats().bytes`: truncated-away messages no longer count, duplicates allocate nothing, and compaction does not change the count.
- `Bus.publish(record) -> Promise<{ id, seq }>`. If the effective record would push retained bytes past `maxBytes`, the call rejects with `RangeError` and the bus is left exactly as it was (no seq/stat/usage change, nothing written). An exact fit succeeds; a single record larger than the quota can never land. Resending an existing `dedupKey` still succeeds when the quota is full: it reuses the first acknowledgement and allocates no bytes.
- `Bus.usage() -> number` — business bytes currently occupied by retained messages (zero on an empty bus). Retention truncation frees bytes immediately; compaction does not. Readable after `close`.
- `Bus.publishBatch(records) -> Promise<Array<{ id, seq }>>` — publish a group atomically. `records` must be an array (a non-array throws `TypeError` synchronously); an empty array resolves to `[]`. The whole group is validated first: if any record is not a JSON object or cannot be represented in JSON, the call rejects with `TypeError` and the bus is left byte-for-byte as it was (no seq/bytes/published/dedup/position change). Only after every shape is valid is the group's occupancy judged: in-group duplicate keys are charged once (their first occurrence), and if the whole group would exceed `maxBytes` it rejects with `RangeError` and leaves no trace. Otherwise the group commits in one bracketed write: either every effective record lands or, after a crash, none of them is visible on reopen. Acknowledgements come back in input order, each shaped exactly like a `publish` result; effective seqs are continuous. Repeated `dedupKey`s within the group, or keys already in history (including after restart), reuse the first acknowledgement and allocate no new seq. After `close` the call rejects with `Error`.
- `Bus.replay(from = 0) -> Promise<Array<{ seq, id, record }>>` — inclusive of `from`, ascending.
- `Bus.register(name) -> number` — register a consumer and return its position (0 for a new name; re-registering never resets it). Synchronous.
- `Bus.advance(name, to) -> number` — move a consumer's position to `to` (non-negative integer, never backwards; setting the current value is a no-op success). Synchronous.
- `Bus.read(name) -> Array<{ seq, id, record }>` — messages after the consumer's position, same shape as replay. Does not move the position; empty array past the end. Synchronous.
- `Bus.compact() -> Promise<void>` — fold all published messages, positions, dedup state and counters into a snapshot and truncate the log. Replay results, dedup semantics and stats are unchanged by compaction.
- `Bus.truncate(before) -> Promise<void>` — retention truncation at segment granularity. `before` must be a non-negative integer (anything else throws `TypeError` synchronously); after `close` the call rejects with `Error`. Every truncate seals the active segment and rolls a fresh one; then, oldest first, each whole segment whose newest message is `<= before` is deleted — but only once every registered consumer's position has passed that segment. Segments a consumer still needs stay put (no error), a segment is never half-deleted, a bound of `0` or nothing eligible is a no-op, and a bound past the end behaves as the end. Replay and read of surviving ranges are unchanged (seq, record, order); a start inside a deleted range begins at the earliest surviving message (empty array when nothing survives), and a consumer positioned below the earliest surviving seq reads from that first survivor — the truncated gap is skipped, never repeated. Dedup keys keep their first acknowledgement across truncation and restarts, and `stats()` never shrink. Truncation runs on the same serial chain as publish/replay/compact: invocation order decides.
- `Bus.stats() -> { seq, bytes, published, replayed }`.
- `Bus.close() -> Promise<void>` — idempotent.

Unknown consumer names are auto-registered by `advance`/`read` exactly as if `register` had been called first. Positions, the dedup table and stats survive restarts; a crash anywhere inside `compact` (half-written snapshot, torn log tail) or inside a batch (a half-written group bracket) recovers to a consistent state with no lost or duplicated messages. The post-compaction log is replaced via a synced temp file plus an atomic rename (rather than truncating an open append handle), so compaction also works on Windows; a failed directory sync along the way only weakens the durability guarantee, it never fails the compaction or blocks the bus. Segment files are always deleted unconditionally once their checkpoint is durable; the directory sync after an unlink is best-effort and its failure never skips the delete or blocks the bus.

## Multiple processes

One bus directory may be opened by several processes at the same time. `createBus` returns synchronously after acquiring write ownership; coordination is entirely file-based and needs no broker.

- **Lease and epoch credential.** On open a process joins the live lease if a heartbeat-fresh holder exists, or otherwise takes over. Every takeover increments the epoch (persisted in `bus.epoch`, so it is never reused across restarts) and stamps an epoch barrier (`{t:'f'}`) into the log. A per-session heartbeat file is refreshed without holding the mutex, even while a process waits for the write lock.
- **One mutation order.** Every append, batch, truncate and compaction takes a short-lived directory mutex (`bus.lock`) while it actually writes, so bytes from two processes can never interleave in a file. Each log line carries the writing epoch. Before any publish, batch, replay marker, position, truncate or compaction touches disk, the epoch credential is re-validated inside the lock, with further checks at multi-phase boundaries.
- **Fencing.** If ownership is taken over, the old instance rejects every subsequent write, batch and `truncate` with an `Error` (rejected `Promise` for the async methods, thrown `Error` for synchronous position writes). Any bytes a displaced holder still manages to land physically appear after the barrier stamped with its smaller epoch and are invisible to every reader — they never enter counters, positions, the dedup table or the visible log, and they leave no gap in the sequence. Reads and the read-only position lookups keep working.
- **Shared state.** Dedup keys, consumer positions, the retained-byte occupancy and the four cumulative counters are one merged state: the same `dedupKey` sent from a different process (or after restart) still returns the first acknowledgement once, positions advanced by two processes obey the no-backwards rule together, `maxBytes` is judged against the bytes committed by every writer (an over-quota record or whole group is rejected with state untouched), and `stats()`/`usage()` reflect the merge. The four cumulative counters are monotone across writers, takeovers and restarts.
- **Recovery after a dead holder.** A stale half-written lease, a half log line, an uncommitted half batch, a half-written segment or an orphaned snapshot left by a crashed/displaced holder is reconciled on the next open: torn tails and uncommitted brackets are severed, an anchor-less snapshot from a displaced epoch is discarded, and reopening restores exactly the committed messages — none lost, none duplicated, with no visible hole or repeated sequence.
- If the bus directory cannot be opened (it is a file, has been removed, or permissions are insufficient), `createBus` throws an `Error` synchronously.

## Log layout

The log is a chain of segment files in the bus directory: the active `bus.jsonl` plus finalized `bus.<index>.jsonl` segments (index order is oldest first). `truncate` seals the active segment, then deletes whole finalized segments only. Before any segment file is unlinked, a checkpoint marker is written at the head of the fresh active segment carrying the cumulative stats, the consumer positions, the dedup table and the truncation horizon; the marker is the recovery anchor, so a crash anywhere inside `truncate` (a half-written segment or marker, deletes done but not synced) recovers with no lost or duplicated messages, positions and dedup come back exactly, and repeating the same truncation is harmless.

Multi-writer coordination files in the same directory are `bus.lock` (the mutex), `bus.owner.json` (the lease: epoch and live sessions), `bus.epoch` (the durable epoch high-water mark) and one `bus.hb.<session>` heartbeat per open process. Snapshots taken with multiple writers possible are stored at generation/epoch-scoped names (`bus.snapshot.g<gen>.e<epoch>.json`), with `bus.snapshot.json` kept as a canonical mirror; all are valid snapshot inputs on recovery. None of these coordination files are part of the message log and all are managed automatically.

## Tests

    npm test

The suite covers the single-process behaviour (unchanged) and cross-process scenarios driven through child processes: interleaved publishes/batches from two live writers, fencing of a displaced writer, shared dedup across processes and restarts, positions advanced from two processes, merged quota rejection, monotone merged stats, concurrent truncation/compaction, and recovery from stale leases, torn lines and orphaned snapshots.

## Limits

Records must be JSON-serialisable.
No network transport and no broker integration.
