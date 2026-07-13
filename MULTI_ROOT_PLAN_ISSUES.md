# Multi-Root Plan Support — Issue Breakdown

**Context (shared by all issues):** We want to collect statistics about running queries (e.g. throughput)
by deploying statistic operators *together with* the query: the user plan and the statistic branches are
combined into ONE logical plan with **multiple sink roots sharing subplans** (a DAG instead of a tree).
Which statistics to collect is known at initial deployment; there is no runtime modification of queries.

The `LogicalPlan` data model already supports this (roots are a vector, operators are immutable and shared
via `shared_ptr`, so one operator instance can be the child of several parents), and the query engine
runtime already executes multi-successor pipeline graphs (successor vectors, emit fan-out, termination
cascade). What is missing sits in four places between plan construction and execution. Each issue below is
self-contained and can be filed independently; they are ordered by pipeline stage and are largely
independent, except that reproducing issue 3 end-to-end requires issue 2 to be fixed first (the repro for
issue 3 therefore hand-builds the physical plan).

All line references are against `main` @ `85ddb759`. Reproduction tests live on the branch
`multi-root-plan-issues`; they assert the *expected* behavior and currently FAIL (deliberately).

Out of scope for all four issues: **operator placement** (`BottomUpPlacement` /`QueryDecomposition` are
single-root by construction — `getRootOperators().front()` throughout, one network channel per cut edge).
Multi-root plans are targeted at the single-node path until distributed placement of DAGs is designed.

---

## Issue 1: Optimization rules corrupt multi-root logical plans

### Current behavior

Every optimization rule traverses and rebuilds the plan once per root (either via a private recursion over
`getChildren()`, or via `getOperatorByType` + `replaceOperator`/`replaceSubtree`, which recurse per root
internally). Because operators are immutable, "rebuilding" means copying — and every copy is assigned a
**fresh `OperatorId`** (the copy-constructing `TypedLogicalOperator` conversion, `LogicalOperator.hpp:115`,
delegates to `OperatorModel`'s id-generating constructor).

For a multi-root plan where an operator is reachable through more than one root, this has three effects:

1. **Silent duplication:** the shared subtree is rebuilt once per path into unrelated copies with unrelated
   ids. The DAG decays into disjoint trees; nothing downstream can re-merge them (the ids differ), so the
   shared source ends up instantiated once per sink and the input is read multiple times.
   Affected: every rule that rebuilds the plan (e.g. `TypeInferenceRule`, `InlineSourceBindingRule`,
   `LogicalSourceExpansionRule`, `InferModelResolutionRule`, `OriginIdInferenceRule`,
   `CalcTargetOrderRule`, the pushdown rules, `RedundantUnionRemovalRule`,
   `RedundantProjectionRemovalRule`, and the `Decide*` rules).
2. **Semantic divergence:** rules with per-visit state make different decisions for different visits of the
   same operator. `OriginIdInferenceRule` walks each root with a running counter
   (`OriginIdInferenceRule.cpp:111-117`), so a source shared by two sinks is assigned origin id 1 via the
   first root and origin id 2 via the second — origins drive watermarking, so the copies are semantically
   different operators afterwards.
3. **Hard crashes:** several rules carry an explicit single-root
   `PRECONDITION(queryPlan.getRootOperators().size() == 1, ...)` and reject multi-root plans outright:
   `DecideMemoryLayoutRule.cpp:64`, `DecideJoinTypesRule.cpp:111`, `LogicalSourceExpansionRule.cpp:110`,
   `InferModelResolutionRule.cpp:67`, `RedundantUnionRemovalRule.cpp:82`, `PredicatePushdownRule.cpp:395`,
   `WatermarkAssignerPushdownRule.cpp:258`; others assume `getRootOperators()[0]` implicitly
   (`CalcTargetOrderRule.cpp:154`, `DecideFieldOrder.cpp:165`).

Additionally, `LogicalSourceExpansionRule` asserts that a source-name operator has exactly one parent
(`LogicalSourceExpansionRule.cpp:90`), which is exactly what is not true in a plan that shares a source.

### Expected behavior

Rules produce correct results for multi-root plans. Per team decision there will be **no central traversal
framework**; instead every rule is individually responsible. The danger each rule must avoid:

> In a multi-root plan an operator can be reached through more than one parent. A rule that processes it
> once per path (a) silently forks the shared subplan into unrelated fresh-id copies, and (b) may make
> inconsistent per-visit decisions (counters, generated ids, config choices) for what is one operator.
> Whatever happens to a multi-parent operator must happen deliberately and consistently — typically by
> keeping a mapping from *pre-rewrite* identity (instance or pre-rewrite id; post-rewrite ids cannot work,
> they are regenerated on every copy) to the rewritten result and reusing it on subsequent encounters.
> A rule may intentionally unshare/specialize per consumer — but never accidentally.

Includes removing the single-root preconditions and deciding the origin-id semantics for shared sources
(natural choice: a shared source gets ONE origin set, visible to all consumers).

### Reproduction (branch `multi-root-plan-issues`)

`nes-query-optimizer/tests/Rules/MultiRootPlanRulesTest.cpp`:
- `TypeInferenceKeepsSharedOperatorsShared` — two sinks share one selection instance; after
  `TypeInferenceRule` the operator reachable via root 0 and root 1 has different ids (was one instance).
- `OriginIdInferenceAssignsOneOriginToASharedSource` — two sinks share one source; after
  `OriginIdInferenceRule` the two paths report different origin ids for the same source.
- `DecideMemoryLayoutAcceptsMultiRootPlans` — the rule throws its single-root precondition.

---

## Issue 2: LowerToPhysicalOperators drops additional sink roots and duplicates shared subplans

### Current behavior

Two independent defects in `nes-query-compiler/src/Phases/LowerToPhysicalOperators.cpp`:

1. `lowerOperatorRecursively` (`:77-122`) has no memoization: it recurses into `getChildren()` and lowers
   every reachable operator once per path. An operator shared by several parents is lowered several times
   into disjoint `PhysicalOperatorWrapper` subgraphs — including duplicated `SourcePhysicalOperator`s, i.e.
   the input is physically read once per copy.
2. `apply` (`:136`) passes only the first lowered root to the builder:
   `physicalPlanBuilder.addSinkRoot(newRootOperators[0]);` — all further sink roots are lowered and then
   silently discarded.

Related, one phase later: `PhysicalPlanBuilder::flip` has
`PRECONDITION(rootOperators.size() == 1, "For now we can only flip graphs with a single root")`
(`PhysicalPlanBuilder.cpp:139`) and starts its node collection from `rootOperators[0]` (`:170`). Notably,
the *rest* of flip is already DAG-correct (pointer-identity visited map, in-degree computation,
`verifyFlippedGraph` checks edge counts/reachability), so this part is a small fix.

### Expected behavior

- A logical operator shared by multiple parents lowers to ONE shared wrapper subgraph (all lowered parents
  link to the same instance) — an identity mapping (`OperatorId` → lowered subgraph) in the lowering
  recursion suffices, since the logical plan is frozen at this point.
- All sink roots are passed to the `PhysicalPlanBuilder`.
- `flip` accepts multiple sink roots (collect from all roots; the visited map already dedups shared nodes).

### Reproduction (branch `multi-root-plan-issues`)

`nes-query-compiler/tests/UnitTests/MultiRootLoweringTest.cpp`:
- `SecondSinkRootIsNotDropped` — a fully bound two-sink DAG plan compiled through `QueryCompiler` yields
  a `CompiledQueryPlan` with 1 sink instead of 2.
- `SharedSourceIsLoweredOnce` — a single-root diamond (union over two selections sharing ONE source
  instance) compiles to 2 sources instead of 1.

---

## Issue 3: PipeliningPhase mis-compiles operators with multiple consumers (fan-out)

### Current behavior

The pipelining phase walks the flipped physical graph (children = consumers) and fuses operators into
linear pipelines. Its child recursion passes the SAME current pipeline to every child
(`PipeliningPhase.cpp:443-448` and the equivalents in the other cases). Fan-IN (an operator with multiple
inputs, i.e. multiple parents post-flip) is handled explicitly via `findMergePoints` (`:56-83`) — but
fan-OUT (an operator with multiple consumers, i.e. multiple children post-flip) has no handling at all:

- If the consumers are fusible, they are appended **sequentially into one linear pipeline**: for a shared
  operator F with consumers A and B, the pipeline becomes `[scan|F|A|…|emit|B|…]` — at runtime B consumes
  A's output (or the emit's output) instead of F's output.
- If the consumers force pipeline breaks, `addDefaultEmit` is called once per consumer (`:373`), stacking
  multiple emit operators onto the shared pipeline.

No precondition fires; the result is a silently wrong plan. (Today this is latent, because issue 2 prevents
fan-out graphs from ever reaching this phase through the normal path — the repro hand-builds the physical
plan.)

### Expected behavior

Mirroring the merge-point design: an operator whose (post-flip) child count is > 1 is a fan-out point. Its
pipeline is closed with exactly ONE emit (carrying the operator's own output schema — every consumer must
see its full output), and each consumer starts its own successor pipeline (beginning with a scan). Requires
a third pipeline policy ("force new pipeline, predecessor is already emit-terminated — do not add another
emit") honored at every emit-adding site; non-native sinks among the consumers need their own formatting
pipeline each, since the shared emit must stay native for the sibling consumers. The runtime needs no
changes: emitted buffers are already delivered to ALL entries of a pipeline's successor vector.

### Reproduction (branch `multi-root-plan-issues`)

`nes-query-compiler/tests/UnitTests/PipeliningMultiConsumerTest.cpp`:
- `FanOutOperatorGetsOwnPipelinePerConsumer` — a hand-built single-root diamond
  (source → shared F → {A, B} → union → sink, flipped orientation) is pipelined; the pipeline containing F
  is expected to contain exactly one intermediate operator and one emit and to have two distinct successor
  pipelines. On main it contains F, A and B plus two emits, and its two successor entries are the same
  pipeline twice.

---

## Issue 4: ExecutableQueryPlan::instantiate rejects plans with multiple sinks

### Current behavior

`nes-runtime/src/ExecutableQueryPlan.cpp:74-77`:

```cpp
if (compiledQueryPlan.sinks.size() != 1)
{
    throw NotImplemented("Currently our execution model expects exactly one sink per query plan");
}
```

The check guards a real design constraint: the single `BackpressureController` created per plan is
move-only and "owned by exactly one sink, which controls all the BackpressureListeners of all sources
within the same query plan" (`nes-executable/include/BackpressureChannel.hpp:27,44-50`).

### Expected behavior

Multi-sink plans instantiate. The mechanical part is small — the singular sink block (structured binding on
`sinks.front()`, pipeline creation, predecessor wiring) becomes a loop; every downstream structure is
already plural-shaped (`instantiatedSinksWithSourcePredecessor` maps to a *vector* of sinks per source,
predecessor wiring appends to `successors` vectors, and the engine's emit/termination paths handle multiple
successors — covered by the existing engine test running one pipeline into two sinks).

The substantive part is the **backpressure ownership decision** for multiple sinks: either a
multi-controller/counting channel (any slow sink throttles the sources), or an explicit interim policy.
A safe interim: one channel per sink, sources listen only to the first sink's listener — i.e. additional
sinks do not exert backpressure (acceptable while additional sinks are lightweight statistic branches; a
slow additional sink then drains the buffer pool instead of throttling).

### Reproduction

None needed — the `NotImplemented` throw is the behavior.

---

## Bonus (small, independent): TraitSet variadic constructor keeps only the last trait

`nes-query-optimizer/include/Traits/TraitSet.hpp:39-44`:

```cpp
template <TraitConcept... TraitType>
explicit TraitSet(TraitType&&... traits)
{
    traitMap = std::unordered_map<std::type_index, Trait>{
        ((std::make_pair<std::type_index, Trait>(typeid(TraitType), std::forward<TraitType>(traits))), ...)};
}
```

The fold is over the **comma operator**, so the initializer list receives only the LAST pair —
`TraitSet{A, B}` silently drops `A`. Latent today because no in-tree caller passes two traits; any code
constructing multi-trait sets (as the reproduction tests on this branch must, to bind sources with both
`OutputOriginIdsTrait` and `MemoryLayoutTypeTrait`) hits it immediately. Fix: a pack expansion over
`traitMap.emplace(...)` instead of the comma fold. The repro tests work around it via `tryInsert`.
