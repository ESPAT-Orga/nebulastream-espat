# Histogram Delta Compression Over the Wire (PoC)

An `EquiWidthHistogram` statistic is normally built and stored on the same node. This feature splits the
build across two nodes and sends only the per-window **change** to the histogram over the link between
them.

```
NODE 1 (builder/edge)                              NODE 2 (store owner)
  source
   → AggBuild[ EquiWidthHistogram ]            (standard reset/lift: build the current window)
   → AggProbe[ DELTA-GEN ]                      (on trigger: combine threads, then
        lower = combined_current − baseline      lower subtracts the baseline → SPARSE DELTA)
   → NetworkSink  ─────(delta blobs + watermarks)─────►  NetworkSource
                                                  → AggBuild[ DELTA-RESOLVER ]   (reset = zero+bounds;
                                                       lift = apply the incoming delta onto the state)
                                                  → AggProbe[ DELTA-RESOLVER ]   (on trigger: combine,
                                                       lower = state + baseline → FULL histogram)
                                                  → StatisticStoreWriter         (writes the full blob to
                                                                                  node 2's store)
```

## Design

**No bespoke blob handler, no bespoke build operator.** The delta logic lives in two new
`AggregationPhysicalFunction`s — `EquiWidthHistogramDeltaGenPhysicalFunction` (node 1) and
`EquiWidthHistogramDeltaResolverPhysicalFunction` (node 2) — plus one new probe operator. `reset`/`lift`
and the build operator stay standard on both sides.

**The baseline is injected at `lower`, not at `reset`.** Each worker thread keeps its own hashmap per
slice; `AggregationProbe` combines them into ONE final state per (window, key) and only then calls
`lower`. Seeding a baseline in `reset` (per thread) would let `combine` add it `T` times; injecting at
`lower` adds it exactly once, so the result is correct for any thread count.

**Round trips are exact under modular counter arithmetic.** GEN's `lower` emits
`combined_current − baseline`, RESOLVER's emits `state + baseline`. Counters are unsigned (mod 2^width),
so `prev + (cur − prev) mod M = cur mod M`, and real counts are `< M` — the round trip holds even when a
bin's delta is "negative" and wraps.

**Bin bounds never travel.** Both sides construct their bin structure from the query's histogram params
(numberOfBins, min, max, counterType), so only counters cross the wire. That also means window 0 needs no
special bootstrap: its baseline is zero, GEN emits `current − 0` in the ordinary sparse format and
RESOLVER emits `delta + 0 = current`.

### Keyframes

The baseline is NOT the previous window. Windows are grouped into **intervals** of `K` consecutive window
ordinals — the dense, `windowEnd`-ordered per-window sequence number the slice store assigns to every
window that actually occurs. Each interval's **keyframe** is its first window (`ordinal % K == 0`), which
lowers against a zero baseline (so it emits a full histogram) and publishes its combined state as the
interval's reference. Every other window of the interval is a delta against that one reference.

Keying on the ordinal rather than an absolute window index is what makes the keyframe always present: the
ordinal counts only windows that occur, so no delta can ever wait on a window that the data never
produces. Gaps in the stream and an unaligned first window are therefore both harmless.

This also turns the per-window dependency chain `W0→W1→W2→…` into independent stars `K→{deltas}`: all
delta windows of an interval read the SAME immutable reference and never depend on each other, so they
lower concurrently and out of order.

GEN derives `intervalIndex` and `isKeyframe` from its own build sequence number and **stamps both on the
wire** (a flag bit and the interval id in the blob header); RESOLVER obeys the stamped values rather than
recomputing them, because the two sides' build sequence numbers are independent.

### Reconstruction never blocks a worker

A worker NEVER blocks waiting for a keyframe. The engine does not process window-tasks in enqueue order,
so a keyframe can be dequeued after its deltas; if deltas blocked on it, all workers could end up blocked
on an interval whose keyframe is still queued — a thread-pool starvation deadlock.

Instead, a delta whose interval reference is not yet published re-enqueues its whole task
(`OpenReturnState::REPEAT`) and retries later. Two properties make that correct as well as live:

- **The readiness gate sits BEFORE the per-thread hashmap combine**, so the common-path reschedule has no
  side effects to undo. A second, rare `REPEAT` remains *after* the combine, covering a keyframe evicted
  between the gate and the emit; the probe clears the final hashmap at the start of every pass so that
  retry re-combines into an empty map instead of doubling the previous pass's combined state.
- **Every task emits AT MOST ONE record.** Emitting several varsized (delta-blob) records from a single
  task corrupts the downstream varsized child buffers.

Liveness does NOT rest on queue order. A keyframe window can never reschedule itself — it lowers against
the shared zero baseline unconditionally — so once its task runs, it publishes. And that task always
exists by the time any of its deltas run: the slice store hands out ordinals while walking windows in
`windowEnd` order, an interval's keyframe is its lowest ordinal and therefore its earliest `windowEnd`, and
the trigger loop stops at the first window the watermark has not yet passed. A keyframe is therefore
triggered in the same batch as its deltas or in an earlier one, never in a later one. Retries are bounded.

What is NOT guaranteed is the order in which those tasks physically enter the queue.
`getTriggerableWindowSlices` releases the window lock before `triggerSlices` allocates the buffers, so two
concurrent trigger batches can enqueue out of ordinal order and a delta can reach a worker ahead of its
keyframe. That costs extra retries, not termination.

`REPEAT` re-submits into the engine's INTERNAL task queue, which `TaskQueue` drains in full before it ever
reads the ADMISSION queue that carries source data — so a 0 ms repeat can hold off the very upstream
progress it is waiting for. `ExecutionContext::setOpenReturnState` therefore takes an optional retry
delay routed through `DelayedTaskSubmitter`; the delta probe retries at 1 ms.

## Components

### The two physical functions

`EquiWidthHistogramDeltaGenPhysicalFunction` (node 1) builds normally and, at `lower`, emits only the bins
whose counter changed:

```
[u64 numChangedBins | keyframe flag in bit 63][u64 nTuplesDelta][u64 intervalId]
[ { u64 binIndex, <counterType> counterDelta } * numChangedBins ]
```

The total byte size is carried out-of-band by the `VariableSizedData` wrapper, so it is not stored
in-band.

`EquiWidthHistogramDeltaResolverPhysicalFunction` (node 2) resets to zero+bounds as usual. Its `lift`
consumes one incoming delta blob per window (not raw data tuples), scattering the per-bin deltas onto the
zeroed state and recording the blob's keyframe flag and interval id into two trailing state words. Its
`lower` adds the baseline and serialises the full histogram for the store writer — from the pure
histogram size, so the two trailing words never reach the stored blob.

Both guard their per-bin loops with the same `kMaxStaticUnrollBins = 224` cut-off as the plain histogram
function: above it, the `static_val<>`-unrolled IR makes query compilation explode.

### The probe operator and the reference cache

`DeltaCompressionAggregationProbePhysicalOperator` is `AggregationProbePhysicalOperator` with a
baseline-aware lower loop: it determines this window's keyframe flag and interval id (GEN from its own
ordinal, RESOLVER from the state's trailing words), fetches the matching baseline, calls
`lower(state, baseline, isKeyframe, intervalId, memProvider)`, and — for a keyframe — publishes the
resulting state as the interval reference.

The reference cache lives on the existing `AggregationOperatorHandler` (`zeroBaselineFor`,
`tryGetKeyframeBaseline`, `isKeyframeReady`, `publishKeyframe`), not in a separate handler. It is bounded
by the engine's own watermark GC: `WindowBasedOperatorHandler` gained an `onGarbageCollect` hook called
from `garbageCollectSlicesAndWindows`, and the aggregation handler evicts every interval whose last window
ended before the new global probe watermark — the same signal that deletes the slices and windows, so it
cannot drop a reference a future delta still needs. `tryGetKeyframeBaseline` returns a COPY in a
thread-local scratch buffer, so a concurrent eviction can never dangle a reader.

The baseline-aware `lower` is a virtual on `AggregationPhysicalFunction` whose default ignores the extra
arguments and calls the 2-argument `lower`, so every other aggregation function is unaffected.

### Plan shape and the network cut

`DefaultStatisticQueryGenerator` emits
`source → StatisticBuild[GEN] → StatisticBuild[RESOLVER] → StatisticStoreWriter` (no node-1 store writer).
GEN turns raw data into windowed delta blobs, so RESOLVER must **re-window** them: event time on the GEN
window-start field (`STATISTICSTART`), `TUMBLING(size = the original window size)`, `onField` = the GEN
delta blob field. Each incoming delta then maps 1:1 onto its own slice — `reset` zeros it, `lift` applies
that one delta, `lower` adds the baseline.

The NetworkSink/Source pair is **not** hand-wired. `QueryDecomposer` compares each parent's and child's
`PlacementTrait.onNode` and builds a matched channel wherever they differ, handling channel id, addresses,
origin/partition ids, and watermarks. So the only thing this feature contributes is placement: `PlacementHintTrait`
carries a feature-agnostic `PlacementAnchor::Source`/`Sink`, and `addPlacementHintConstraints` in
`BottomUpPlacement::solvePlacement` hard-pins hinted operators with the existing `Highs_changeColBounds`
source/sink-pin pattern. Without it the ILP's distance objective collapses GEN and RESOLVER onto one node
and nothing ever crosses a wire. The generator stamps GEN with `Source` and RESOLVER + StatisticStoreWriter
with `Sink`.

On a single-node topology (source host == sink host) both halves still co-locate: correct results, no
wire. The cut only exists when the two hosts differ.

Watermarks cross the NetworkSink — it serialises `sequence_number`, `chunk_number`, `origin_id`, and
`watermark` into the buffer metadata and the receive side restores them — so node 2's RESOLVER windows
trigger from the propagated watermarks. No gRPC or proto change is involved.

`SET ('zstd' AS compress_statistic)` on the request wraps the blob in `ZstdCompress` / `ZstdDecompress`
around the network cut, so only compressed bytes cross it and `StatisticStoreWriter` still sees a raw
blob. It applies to both the plain and the delta chain, which is what lets the benchmark compare generic
compression against — and on top of — the delta split.

## Configuration

`enable_histogram_delta_compression` is an **optimizer** option
(`--optimizer enable_histogram_delta_compression=true` for the REPL, the topology's `optimizer:` block for
nes-cli). It selects the SHAPE OF THE PLAN and is therefore read in the frontend, before submission:
`DefaultStatisticQueryGenerator`'s constructor takes it. No worker ever reads it, and a frontend driving
remote workers cannot read worker configuration at all (`GRPCQuerySubmissionBackend` ignores the
topology's config for remote workers), so a `worker.default_query_execution` home would make it
unreachable in exactly the distributed deployments this feature targets.

Lowering does not read the flag. `LowerToPhysicalStatisticBuild` keys off the aggregation function *names*
(`EquiWidthHistogramDeltaGen` / `…Resolver`), so the delta probe operator appears exactly when the plan
contains delta functions — whether they came from the generator or from `EQUIWIDTHHISTOGRAMDELTA` in plain
SQL.

The keyframe interval `K` is different in kind: it IS consumed during lowering, so it stays a worker option
(`worker.default_query_execution.histogram_delta_keyframe_interval`, default 10), resolved independently on
each node. `K = 1` makes every window a keyframe (no compression); larger `K` compresses better but lets
references drift further. For a distributed run both nodes must be given the same value; a mismatch is not
detected.

## Tests

- `WindowAggregationHistogramDelta.test` — all four systest scenarios, in one file, ordered by keyframe
  interval because `GlobalConfiguration` applies to every query below it until the same key is redeclared.
  All four pin 4 worker threads, which overrides the 1/2/4 sweep the `Aggregation` ctest matrix applies.
  Statistic IDs are disjoint across the whole file (70-72, 90-92, 80-81, 82-83): queries are grouped into
  worker instances by their configuration override and one worker owns one statistic store, so a reused ID
  would cross-contaminate scenarios.
  - *Baseline* (K=10) — end-to-end GEN → delta bytes → RESOLVER at the default K.
  - *ManyBins* (K=10) — the benchmark's own configuration (budget 16384 ⇒ 682 bins over `[0,20009]`,
    width 29) across two windows, so window 0 is the interval keyframe and window 1 a genuine delta against
    it. This is the only scenario that exercises the **runtime** per-bin loops: five
    `numberOfBins < kMaxStaticUnrollBins` branch sites exist — two in the plain histogram function and three
    only the delta split reaches — and every other scenario uses budget 128 ⇒ 5 bins, which traces only the
    unrolled form. Values sit in nine widely separated bins including bin 681, so the blob carries a
    `binIndex` the 5-bin scenarios never reach. The probe output is filtered to populated bins, which keeps
    the golden at 18 rows instead of 1364 without hiding corruption: a stray counter in an untouched bin
    shows up as an extra row and fails.
  - *MultiThread* (K=3) — two intervals, so a reference leaking across intervals or a stale baseline under
    concurrency corrupts the reconstructed bins.
  - *Unaligned* (K=5) — a first window that is not aligned to an absolute keyframe index.

  Goldens throughout are written from the documented layout rather than captured from a run, so they are an
  absolute assertion; each scenario's plain-histogram equivalence case then adds that the delta round trip
  agrees with the plain build.
- `DistributedPlanningTest.PlacementHintForcesSplit` — on a source-node/sink-node topology with ample
  capacity (where the distance objective would otherwise co-locate), the source-anchored operator lands on
  the source node behind a Network sink and the sink-anchored operator on the sink node behind a Network
  source, i.e. the cut sits exactly between them.
- `nes-frontend/apps/repl/tests/histogram_delta_repl_test.sh` — REPL-driven end-to-end: for both flag
  settings it asserts the query deploys and runs, has the expected plan shape (ON ⇒ DeltaGen → DeltaResolver,
  OFF ⇒ plain EquiWidthHistogram), and probes the node-local store into a File sink, asserting the
  reconstructed bins match a golden AND are byte-identical between the delta and plain builds. The request
  must use `EVENTTIME <field>`; ingestion-time windows need `$record.creationTs`, which a File source lacks.
  `histogram_delta_onebucket_repl_test.sh` is its companion with windows that differ by exactly one bin, so
  each delta carries exactly one changed bin — the path the sparse encoding exists for.
- `nes-frontend/apps/cli/tests/distributed.bats` — the only coverage of a delta blob crossing a real network
  channel between two workers, on `tests/good/histogram-delta-2-nodes.yaml`. It asserts the placement pins
  land the two halves on different workers and that the histogram probed back out of the store node matches
  both the single-node golden and a plain build over the same wire.

## Results

All numbers are from the real Google cluster-monitoring 1 GB trace (the `ENABLE_LARGE_TESTS` dataset the
systests use). `scripts/benchmarking/histogram_delta/prepare_cluster_monitoring.py` downloads it and
projects it to the harness's 2-column `value,timestamp` schema: `value` = `taskId` (a real integer field,
range 0..20009 — the template's `userId` is anonymized to a constant in this trace and would collapse to
one bucket), `timestamp` = the real `creationTS` event time. 18.65 M rows. The benchmark windows on 60 s
rather than 1 s purely for throughput reasons (1 s windows leave ~29 k gaps in the trace).

### Wire bytes, 2-node

682 bins over taskId's true `[0,20009]` range, 60 s event-time windows, K=10, topology 1/1, 60 s hold, the
default 8192 buffer. All six variants ingested 11.96–12.00 M tuples (spread 0.31 %) across ~2950 windows,
so the byte counts compare directly. Root container eth0 RX:

| variant | root RX | B/window | vs split |
|---------|---------|----------|----------|
| prometheus (raw stream) | 115.28 MB | — | **2.1× more** |
| split (full synopsis) | 55.03 MB | 18,619 | 1.00× |
| split + zstd | 27.91 MB | 9,442 | 1.97× |
| delta | 12.99 MB | 4,395 | **4.24×** |
| delta + zstd | 6.74 MB | 2,281 | **8.16×** |
| local (4 scalars) | 0.71 MB | 240 | 77.4× |

Two things this settles:

1. **The delta earns its complexity at this bin count.** 4.24× over the uncompressed synopsis, and 2.1×
   over *compressing the same synopsis* — the O(changed bins) vs O(bins) prediction, on real data. About
   200 of 682 bins change per 60 s window. The crossover matters: below a few hundred bins, compressing
   the full synopsis is smaller than the delta and far simpler, with no keyframes, no reference cache and
   no cross-node interval agreement.
2. **Delta and compression COMPOSE.** `delta_zstd` is the best variant measured (8.16×), 1.9× below the
   raw delta. Generic compression is half the win for none of the keyframe machinery, and it stacks.

Charts: `scripts/benchmarking/histogram_delta/distributed/plots/cluster_monitoring/`.

### Throughput, single-node

`scripts/benchmarking/histogram_delta/`, 3 runs + warmup, COMPILER, taskId over [0,20009] ≈ 100 bins.
Metric = input tuples / (source-start → sink-complete) from the worker log, which excludes compile time.

| threads | plain (M/s) | delta median-K (M/s) | delta/plain |
|--------:|------------:|---------------------:|------------:|
| 1  | 1.59  | 1.48  | 93% |
| 2  | 3.03  | 2.82  | 93% |
| 4  | 5.95  | 5.46  | 92% |
| 8  | 10.82 | 10.08 | 93% |
| 16 | 18.99 | 17.45 | 92% |

Plain scales 12.0× and delta 11.8× over 1→16 threads, so the split is not parse-bound and the keyframe
scheme does scale with threads. Co-located, delta costs a **~7–8 % throughput overhead**, roughly constant
across thread counts. `K` barely affects throughput (≤ ~3 % spread across K∈{1,2,5,10,50} at fixed threads)
— it is a compression↔drift knob, not a throughput knob.

### Throughput, 2-node

An unthrottled run of the same 2-node setup (`GENERATOR_RATE=0`, median of 3, per-run spread ≤ 2 %); each
variant consumed the identical trace, so this is time-to-consume. `mean_tps` is the engine's own throughput
listener over the ingest stream.

The TCP + CSV column is a whole-pipeline rate (loopback socket, text parsing per tuple). The Memory column
removes exactly that: `MemorySource::setup()` parses the same CSV into native TupleBuffers before the query
starts, so transport and parsing sit outside the measurement. It runs on an 8×-replicated trace
(`DATASET_COPIES=8`, each copy shifted by a whole number of windows) because the raw trace drains in ~3.5 s,
which is too short a span to read as steady state; every value moved by ≤ 1 % and upward against the 1×
run, so the short-window numbers were validated rather than corrected.

| variant | TCP + CSV | Memory, 8× | vs split |
|---------|-----------|------------|----------|
| split | 2.07 M/s | **5.36 M/s** | — |
| split + zstd | 1.28 M/s | **2.11 M/s** | 61 % slower |
| delta | 2.17 M/s | **6.02 M/s** | **12 % faster** |
| delta + zstd | 1.98 M/s | **5.16 M/s** | 4 % slower |

**Across a real wire the delta split is a throughput gain, not a cost.** Single-node it is 7–8 % slower
than plain, because GEN and RESOLVER co-locate and the extra per-window work is pure overhead. Across the
link the ranking flips: the 4.2× smaller payload means less NetworkSink serialisation and less downstream
backpressure on the leaf's source, and that outweighs the delta machinery. Generic compression goes the
other way — zstd on the full synopsis costs 61 % of ingest throughput to buy its 2× wire saving, while
zstd on the already-small delta blob costs 4 %. So `delta_zstd` buys the best wire number (8.2×) at roughly
`split`'s throughput.

Removing the per-tuple parse raises every variant ~2.6× and roughly doubles every relative gap: the shared
overhead had been diluting them.

Note that this run's byte counts are NOT comparable across variants (unthrottled ⇒ each variant ingests at
its own rate over the hold); the wire table above is the one to quote for bytes. The two measurements
cannot come from a single run, which is why `GENERATOR_RATE` decides which one a run produces.

Charts: `plots/cluster_monitoring_throughput/`, `plots/cluster_monitoring_throughput_memory_x8/`.

## Out of scope for this PoC

- **Order, loss and recovery.** A node-2 restart loses the reference cache; robustness would need a
  periodic full re-baseline or a request-full protocol. Loss and reorder across the wire are not handled.
- **Sliding windows / multiple slices per window.** The PoC assumes tumbling, one slice per window.
- **A general placement rule** instead of the hint-driven pin.
- **Buffering unresolved deltas instead of rescheduling them.** Carrying the keyframe's `windowStart` id
  and keying the cache by it would let a delta be parked rather than retried, at the cost of the
  one-record-per-task property described above.
