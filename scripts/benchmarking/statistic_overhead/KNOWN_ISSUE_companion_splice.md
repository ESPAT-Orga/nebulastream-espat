# Known issue: attaching a companion stops the data query's source from ever starting

**This blocks `statistic_overhead`'s shared arm entirely.** An earlier version of this document
blamed the *second* companion; that was wrong — see "Correction" below.

## Minimal reproduction

One analytical query, one companion. The query deploys (the REPL prints its id), but the worker
produces **no throughput lines at all** — the data query's source never starts, because it waits on
`DeferSourceStartTrait` for a splice that never lands. Confirmed identical on a TCP source and on a
`LoopingMemory` file source, so source type is not the variable either.

## Cause found (2026-08-09)

`Repl.cpp` has two companion paths, and **only the switchable one works**. Measured on one query
with one companion, TCP source, everything else identical:

| | plain path (no `--companion-switch-to-sql`) | with `--companion-switch-to-sql` |
|---|---|---|
| throughput lines in worker log | **0** | **1649** |
| build branch deployed | — | yes, a second query id appears |

On the plain path the data query's source never starts at all — it waits on `DeferSourceStartTrait`
forever. With a paired SQL, `Repl.cpp` routes through `deployWithSwitchableAlternate` instead and the
source starts normally.

This is why adaptive-optimization never hit it: that benchmark always passes
`--companion-switch-to-sql`, so the plain "observability without runtime swap" path has apparently
never been exercised by a working benchmark.

**Workaround for `statistic_overhead`:** pass `--companion-switch-to-sql` with the query's own text
and a condition that never fires (e.g. `BINCOUNTER > UINT64(<huge>)`), engaging the machinery that
works without ever performing a swap. Ugly, and it should be documented in the harness so nobody
deletes the flag as redundant.

**Still to confirm:** that the histogram is actually populated. The throughput listener cannot show
this — a spliced branch consumes the data query's source thread and has no source pipeline, so it
never emits throughput events. (An earlier acceptance criterion of "the statistic query reports
throughput" was therefore wrong, and would have failed a working splice.) Check the statistic store
or the probe's gRPC reports instead.

## The remaining difference from adaptive-optimization

`Repl.cpp` has two companion paths. Ours is the plain one:

> *"Without a paired SQL, the merged data+build plan is submitted as a plain query — observability
> without runtime swap."*

The adaptive-optimization benchmark always passes `--companion-switch-to-sql`, which instead routes
through `deployWithSwitchableAlternate`. **The plain path appears never to have been exercised by a
working benchmark.** Next test: pass `--companion-switch-to-sql` with the same query text and see
whether the source starts.

## Correction (2026-08-09)

The original diagnosis — "the FIRST of TWO companions fails" — was measured only on Nexmark Q8 with a
companion pair. Re-tested on ClusterMonitoring Q2, a **single-source** query with a **single**
companion, the failure is identical and total:

```
N=0   accepts=10  PASS   10 queries reporting, 1,999,974 tup/s (100% of offered), issue=ok
N=10  accepts=0   FAIL   no source connected at all; 0 queries reporting
```

So companion *count* is not the variable. Both observations fit one rule: a companion submitted
while its data query is still being deployed never splices; only a companion submitted against an
already-deployed data query does (which is why Nexmark's second companion worked). A 20 s delay
between deploying the data plan and submitting the branch does **not** fix it, so it is not simply
elapsed time.

The remaining untested difference from the adaptive-optimization benchmark — which uses this
mechanism successfully — is the **source type**: it drives a `LoopingMemory` file source, whereas
`statistic_overhead` uses TCP. A TCP source must connect before it can produce, and a deferred one
never connects at all, so the registration/splice ordering may differ in a way that only shows up
here.

## Symptom

When one data query carries **two** companion statistics (`--companion-field` +
`--companion-field-2`), the **first** companion's build branch never attaches to its source. That
source carries `DeferSourceStartTrait`, so it waits forever for a splice that never lands and never
starts emitting — the query then runs half-fed.

At scale this is fatal: with 10 Nexmark Q8 joins each carrying a pair, the ten half-fed joins buffer
one input indefinitely while waiting for watermarks from the other, and the worker is OOM-killed
(`exit code -9`).

## Reproduction

Smallest case — one Q8 query with one companion pair, nine plain Q8 queries:

```bash
NES_BUILD_DIR=<release build> python3 -m scripts.benchmarking.statistic_overhead.run_statistic_overhead \
    --output-dir /tmp/bench-n2 --mode shared --statistic-query-counts 2 --num-runs 1 --skip-build
```

Expected 20 generator connections, observed 19:

```
generator accepts: 19
     10 stream=Auction
      9 stream=Person      <-- session 0's person source never connected
distinct queries reporting throughput: 10   (the Q8s only; no build branch reports)
statistic queries measured: 0/2
```

## It is positional, not source-specific

Swapping the pair so `auction` is primary and `person` secondary flips which source is orphaned:

| primary companion | accepts |
|---|---|
| person, auction | 10 Auction, **9 Person** |
| auction, person | **9 Auction**, 10 Person |

So it is not about the field, the schema, or the stream — it is about being the *first* companion.
The first `collectWorkloadStatistic` call is the one that also deploys the data plan; the second
finds it already deployed via the cache.

## Hypotheses tested and eliminated

| Hypothesis | Test | Verdict |
|---|---|---|
| Only the *second* companion splices | ClusterMonitoring Q2, one companion | fails identically — count is not the variable |
| Multi-source (join) plans break the splice | single-source Q2 | fails identically |
| The branch races the data query's deployment | 20 s delay between deploy and branch submit | no change |
| `CONNECT_TIMEOUT` (10 s) kills the deferred source | raised to 300 s | no change |
| TCP source vs `LoopingMemory` | **not yet run** | the last untested difference from the working adaptive-optimization config |

## What is ruled out

- **Not a coordinator failure.** Both calls return successfully with the same (correct) data query
  id — the REPL prints the id twice, e.g. `[{"query_id":"prancing_percheron"}]` twice.
- **Not the plan cache.** The plan-keyed cache in `collectWorkloadStatistic` correctly returns one
  deployment for both companions.
- **Not the multi-source resolution.** `splice_source` picks the right source; the auction branch
  built from the same code path splices fine.
- **Not buffer pressure.** At N=2 there is no `BUFFER_EXHAUSTION` and the worker survives.
- **Not a splice queue that never drains.** `RunningSourceRegistry::spliceOrEnqueue` logs
  `"splice for logical source '{}' queued; waiting for the data query to register it"` on the queued
  path. With `-DENGINE_LOG_LEVEL=WARNING` that message never appears, so the branch never reached
  `spliceOrEnqueue` at all.

## Open lead

The same run emits, repeatedly:

```
[W] [QueryEngine.cpp:123] Node Expired and pendingTasks could not be reduced
[W] [QueryEngine.cpp:566] Task <n> for Query QueryId(...)
```

Suggesting the build-branch query's plan node is reaped rather than left waiting for its source.
Worth checking whether a query whose only source is deferred/spliced gets torn down before the
splice can attach.

## Gotchas found on the way (independent of the bug)

- **`ENGINE_LOG_LEVEL` is a separate compile-time flag from `NES_LOG_LEVEL`, defaulting to `ERROR`**
  (`nes-query-engine/CMakeLists.txt:26`). Every `ENGINE_LOG_*` in the query engine — including the
  whole splice path — is invisible in a normal build. Configure `-DENGINE_LOG_LEVEL=WARNING`.
- **`RunningSourceRegistry` allows one live source per logical name, worker-wide** and throws
  otherwise ("the splice contract assumes a single source thread per logical name"). Any multi-query
  workload using companions must give each query distinct logical source names — hence
  `cluster_<i>` in `shared_submission.py`.
- **`START` and `END` are reserved tokens** in `AntlrSQL.g4`, so a windowed query's sink schema needs
  backticks: ``PERSONAUCTION.`START` ``. `nes-repl` reports *nothing* on a parse error — it simply
  stops consuming statements, which looks exactly like a hung worker.
- **`collectWorkloadStatistic` swallows build-branch failures** into `NES_WARNING` and continues, so
  a branch that fails to construct or deploy is silent at default log levels.

## Status

The multi-source splice work this was found with has been **reverted** — the benchmark moved to
ClusterMonitoring Q2, which has a single source and needs one companion, so
`collectWorkloadStatistic`'s existing single-source path suffices. That work (resolving the splice
target from a `splice_source` option, keying the deployed-data-query cache by plan rather than
source name, and per-source `DeferSourceStartTrait` counts) is recoverable from commit `5f64f5e7ed`
if anyone picks this defect up; it made the two-companion path *reachable* for joins but did not
cause the failure.

The reproduction above used that engine build. To reproduce on a clean tree you need a data query
with two companions; on a single-source query that means two `--companion-field`s over different
fields of the same source, which should exercise the same first-companion path.
