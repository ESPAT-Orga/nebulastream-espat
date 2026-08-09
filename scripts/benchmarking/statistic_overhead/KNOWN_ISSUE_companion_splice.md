# Known issue: the first of two companion statistics never splices

`MODE=shared` is blocked by this. `MODE=isolated` is unaffected and works today.

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
  `person_<i>` / `auction_<i>` in `shared_submission.py`.
- **`START` and `END` are reserved tokens** in `AntlrSQL.g4`, so a windowed query's sink schema needs
  backticks: ``PERSONAUCTION.`START` ``. `nes-repl` reports *nothing* on a parse error — it simply
  stops consuming statements, which looks exactly like a hung worker.
- **`collectWorkloadStatistic` swallows build-branch failures** into `NES_WARNING` and continues, so
  a branch that fails to construct or deploy is silent at default log levels.

## Engine changes already made for this (in the working tree)

`StatisticCoordinator.cpp` previously rejected multi-source data queries outright
(`NotImplemented: ... requires the data query to have exactly one source`). Now:

1. The splice target is resolved from a new `splice_source` request option naming the logical source
   (the field name cannot disambiguate — Nexmark's `person` and `auction` both have an `id`).
2. `deployedDataQueriesByPlan` is keyed by the data plan's root operator ids instead of by source
   name, which previously both re-deployed the join for its second companion and made two different
   data queries reading one source collide.
3. `DeferSourceStartTrait` is stamped per source with per-source splice counts (`splice_counts`
   option) rather than only on the first source with the session-wide total.

`ReplStarter.cpp` gained `--companion-source` / `--companion-source-2` and computes `splice_counts`.

Single-source behaviour is unchanged — 675 tests pass, including `StatisticRegistryTest` and
`DefaultStatisticQueryGeneratorTest`, so the adaptive-optimization path is unaffected.
