# Multi-Sink Plan Support — Implementation Issues (GitHub-ready)

Each section below is one GitHub issue: the title states what is to be implemented, the body describes the
implementation work. Shared motivation, referenced by all of them: to collect statistics about a query
(e.g. throughput), statistic operators are deployed **together with the query** as one logical plan with
**multiple sink roots that share subplans**. Line references are against `main` @ `85ddb759`; the failing
reproduction tests live on branch `multi-root-plan-issues`.

---

## Issue: Handle shared operators in the optimization rules (multi-sink plans)

### Motivation

Multi-sink plans share operator instances between sink roots (the same operator is reachable through more
than one parent). The optimization rules must process such plans correctly; this is required for deploying
statistic-collecting sinks together with a query as one plan.

### What needs to be implemented

- Every rule that traverses and rebuilds the plan must handle operators that are reachable through
  multiple parents. Since rebuilding an operator assigns a fresh `OperatorId` (copy-constructing
  `TypedLogicalOperator` conversion, `LogicalOperator.hpp:115`), each such rule needs to **track node
  identities**: keep a mapping from *pre-rewrite* identity (operator instance or its id before the rule's
  rebuild — post-rewrite ids cannot work) to the rewritten operator, and reuse the mapped result when the
  operator is encountered again through another parent, instead of rebuilding it per path.
  Affected rules: `InlineSourceBindingRule`, `LogicalSourceExpansionRule`, `InferModelResolutionRule`,
  `TypeInferenceRule`, `OriginIdInferenceRule`, `CalcTargetOrderRule`, `PredicatePushdownRule`,
  `ProjectionPushdownRule`, `WatermarkAssignerPushdownRule`, `DecideJoinTypesRule`,
  `DecideMemoryLayoutRule`, `DecideFieldMappings`, `DecideFieldOrder`, `RedundantUnionRemovalRule`,
  `RedundantProjectionRemovalRule`.
  (`SinkBindingRule`/`InlineSinkBindingRule` only rewrite roots and need no change.)
- Remove the single-root preconditions and root-`[0]` assumptions; all rules must iterate all roots.
  Explicit preconditions: `DecideMemoryLayoutRule.cpp:64`, `DecideJoinTypesRule.cpp:111`,
  `LogicalSourceExpansionRule.cpp:110`, `InferModelResolutionRule.cpp:67`,
  `RedundantUnionRemovalRule.cpp:82`, `PredicatePushdownRule.cpp:395`,
  `WatermarkAssignerPushdownRule.cpp:258`; implicit `getRootOperators()[0]`:
  `CalcTargetOrderRule.cpp:154`, `DecideFieldOrder.cpp:165`.
- `OriginIdInferenceRule`: assign **one** origin set to a shared source, visible to all consumers. The rule
  currently advances a running counter per visit (`OriginIdInferenceRule.cpp:111-117`), which would give
  the same source a different origin id per consuming root.
- `LogicalSourceExpansionRule`: drop the assumption that a source-name operator has exactly one parent
  (`LogicalSourceExpansionRule.cpp:90`); the expansion of a shared source must itself be shared by all
  parents.

### Notes

- A rule may *deliberately* specialize or replace a shared operator differently per consumer — that is a
  semantic decision the rule is entitled to make. What must never happen is *accidental* duplication or
  per-visit divergence caused by traversal order.
- Failing tests (assert the target behavior): `MultiRootPlanRulesTest` on branch `multi-root-plan-issues`.

---

## Issue: Lower multi-sink logical plans into a shared physical operator DAG

### Motivation

A multi-sink logical plan shares subplans between its sink roots. Lowering must translate the shared
logical operators into **one** shared physical subgraph — otherwise the shared part (including the source,
i.e. reading the input) is duplicated per sink. Required for deploying statistic sinks together with a
query as one plan.

### What needs to be implemented

- Memoize the lowering recursion (`LowerToPhysicalOperators.cpp:77-122`,
  `lowerOperatorRecursively`): a mapping `OperatorId → lowered wrapper subgraph`, consulted before lowering
  and filled on every return path (including the operator-bypass path), so that an operator shared by
  multiple parents lowers exactly once and all lowered parents link to the same wrapper instance. (The
  logical plan is frozen at this point, so ids are a valid key here.)
- Pass **all** lowered sink roots to the `PhysicalPlanBuilder` — `apply` currently hands over only
  `newRootOperators[0]` (`LowerToPhysicalOperators.cpp:136`), silently discarding further sinks.
- Extend `PhysicalPlanBuilder::flip` to multiple sink roots: relax the single-root precondition
  (`PhysicalPlanBuilder.cpp:139`) and run the node collection over all roots (`:170`). The rest of the
  function (pointer-identity visited map, in-degree computation, `verifyFlippedGraph`) already handles
  DAGs and needs no change.

### Notes

- Failing tests: `MultiRootLoweringTest` on branch `multi-root-plan-issues` (a two-sink plan must compile
  to two sinks and one source; a single-root diamond sharing a source must compile to one source).

---

## Issue: Split pipelines at operators with multiple consumers in the pipelining phase

### Motivation

In the physical DAG of a multi-sink plan, a shared operator has multiple consumers (multiple children in
the flipped, data-flow-oriented graph). The pipelining phase must give every consumer its own pipeline fed
by the shared operator's output — analogous to the existing merge-point handling for operators with
multiple *inputs*. Required for deploying statistic sinks together with a query as one plan.

### What needs to be implemented

- Detect fan-out points in `buildPipelineRecursively` (`PipeliningPhase.cpp:199-479`): an operator whose
  (post-flip) child count is greater than one. Unlike merge points this needs no precomputation — it is a
  local property of the node.
- At a fan-out point, close the current pipeline with **exactly one** default (native) emit carrying the
  operator's own output schema — every consumer must observe the operator's full output.
- Introduce a third pipeline policy alongside `Continue`/`ForceNew` ("force a new pipeline; the
  predecessor pipeline is already emit-terminated, do not add another emit") and honor it at every
  emit-adding site of the recursion. Each consumer then starts its own successor pipeline beginning with a
  scan that reads the emitted buffers.
- Consumers that are non-native sinks need their **own formatting pipeline** (scan + formatting emit)
  in front of the sink pipeline, because the shared emit must stay native for the sibling consumers.
- No runtime changes are needed: emitted buffers are already delivered to all entries of a pipeline's
  successor vector, and termination cascades over all successors.

### Notes

- Failing test: `PipeliningMultiConsumerTest` on branch `multi-root-plan-issues`. The test hand-builds the
  physical plan because the lowering issue currently prevents fan-out graphs from reaching this phase
  through the normal compile path — fixing the lowering issue first allows an end-to-end test.

---

## Issue: Instantiate query plans with multiple sinks (incl. backpressure ownership)

### Motivation

Executing a multi-sink plan requires `ExecutableQueryPlan::instantiate` to create and wire one sink
pipeline per sink. It currently rejects such plans (`nes-runtime/src/ExecutableQueryPlan.cpp:74-77`,
`NotImplemented("Currently our execution model expects exactly one sink per query plan")`). Required for
deploying statistic sinks together with a query as one plan.

### What needs to be implemented

- Replace the sink-count check and the singular sink block (structured binding on `sinks.front()`,
  pipeline creation, predecessor wiring) with a loop over `compiledQueryPlan.sinks`. The downstream data
  structures are already plural-shaped (`instantiatedSinksWithSourcePredecessor` maps each source to a
  *vector* of sinks, predecessor wiring appends to `successors` vectors, and the engine's emit and
  termination paths handle multiple successors).
- Decide and implement the **backpressure ownership** for multiple sinks. The current design ties one
  move-only `BackpressureController` to exactly one sink, controlling the listeners of all sources
  (`nes-executable/include/BackpressureChannel.hpp:27,44-50`). Options:
  - a multi-controller (e.g. counting) channel so that any slow sink throttles the sources, or
  - an explicit interim policy: one channel per sink, sources listen only to the first sink's listener —
    additional sinks then do not exert backpressure (acceptable while additional sinks are lightweight
    statistic branches; a slow additional sink drains the buffer pool instead of throttling). If the
    interim policy is chosen, document it and file the multi-controller channel as follow-up.

### Notes

- No reproduction test needed; the `NotImplemented` throw is the current behavior.

---

## Issue: Fix the variadic TraitSet constructor to insert all traits

### Motivation

Constructing operators for multi-sink plans requires attaching several traits at once (e.g.
`OutputOriginIdsTrait` and `MemoryLayoutTypeTrait` on a source). The variadic `TraitSet` constructor
silently keeps only the **last** argument.

### What needs to be implemented

- `nes-query-optimizer/include/Traits/TraitSet.hpp:39-44`: the constructor folds over the comma operator,

  ```cpp
  traitMap = std::unordered_map<std::type_index, Trait>{
      ((std::make_pair<std::type_index, Trait>(typeid(TraitType), std::forward<TraitType>(traits))), ...)};
  ```

  so the initializer list receives a single (the last) pair and `TraitSet{A, B}` drops `A`. Replace the
  comma fold with a pack expansion that inserts every trait, e.g.

  ```cpp
  (traitMap.emplace(std::type_index{typeid(TraitType)}, std::forward<TraitType>(traits)), ...);
  ```

- Add a unit test constructing a `TraitSet` with two traits and asserting both are retrievable.

### Notes

- Latent today because no in-tree caller passes more than one trait; any multi-sink work hits it
  immediately (the reproduction tests on branch `multi-root-plan-issues` work around it via `tryInsert`).
