# DwC-DP to DwC-A mapping engine

This document describes the declarative DwC-DP → DwC-A mapping implementation under
`org.gbif.pipelines.spark.dwcdp.mapping`.

The mapping layer intentionally separates four concerns:

1. the **DwC-DP schema** defines resources, fields, and relations;
2. **mapping configuration** declares which schema-backed paths contribute to which DwC-A terms;
3. **compilation** resolves and validates the declarative mapping before Spark execution; and
4. **execution** applies the compiled navigation, cardinality, target, merge, and row-composition
   semantics to Spark datasets and assembles `ExtendedRecord` output.

The configuration classes are therefore not Spark pipelines and should not contain hand-written
join implementations. They describe logical mappings. Spark-specific behavior belongs to the
execution layer.

---

## Contents

1. [Architecture](#1-architecture)
2. [Design principles](#2-design-principles)
3. [Schema model and relation resolution](#3-schema-model-and-relation-resolution)
4. [Mapping paths](#4-mapping-paths)
5. [Target mappings and value aggregation](#5-target-mappings-and-value-aggregation)
6. [Fragments, extensions, and mapping plans](#6-fragments-extensions-and-mapping-plans)
7. [Compilation and validation](#7-compilation-and-validation)
8. [Spark execution](#8-spark-execution)
9. [Core and extension target merges](#9-core-and-extension-target-merges)
10. [Core identity and structural keys](#10-core-identity-and-structural-keys)
11. [Occurrence as core and as Event extension](#11-occurrence-as-core-and-as-event-extension)
12. [Configuration entry points](#12-configuration-entry-points)
13. [Adding or changing a mapping](#13-adding-or-changing-a-mapping)
14. [Testing](#14-testing)
15. [Lossy conversion rules](#15-lossy-conversion-rules)
16. [Deferred and intentionally separate work](#16-deferred-and-intentionally-separate-work)

---

## 1. Architecture

At the highest level the mapping engine is:

```text
DwC-DP profile/schema
        │
        ▼
    SchemaGraph
        │
        ├──────────────────────────────┐
        │                              │
        ▼                              │
Mapping configuration                  │
(EventDwcaMapping,                     │
 OccurrenceDwcaMapping,                │
 domain mapping classes)               │
        │                              │
        ▼                              │
    MappingPlan                        │
        │                              │
        ▼                              │
  MappingCompiler  ◄───────────────────┘
        │
        │ schema-resolved and validated
        ▼
  CompiledMapping
        │
        ▼
┌──────────────── Spark execution ───────────────┐
│                                                │
│  SparkMappingPathExecutor                      │
│      schema-backed navigation                  │
│      filters / optional relations              │
│      cardinality                               │
│                                                │
│  SparkTargetExpression                         │
│      source combination                        │
│      aggregation semantics                     │
│                                                │
│  SparkExtensionMaterializer                    │
│      extension row construction                │
│      ENRICH / UNION composition                │
│      row identity / row matching               │
│                                                │
│  SparkExtendedRecordExecutor                   │
│      core projection                           │
│      extension attachment                      │
│      final ExtendedRecord assembly             │
└────────────────────────────────────────────────┘
        │
        ▼
DwC-A-shaped ExtendedRecord
```

The important boundary is between **declaration/compilation** and **execution**. Mapping mistakes
which can be determined from the schema and mapping definition should fail during validation or
compilation, not after Spark has started translating the mapping into dataset operations.

### Package roles

The main mapping packages are:

```text
mapping/
├── schema/       DwC-DP resources, relations, paths, and graph resolution
├── definition/   declarative mapping DSL and mapping-plan model
├── config/       Event/Occurrence/domain mapping declarations
├── compilation/  validation, resolution, pruning, diagnostics, compiled model
├── execution/    Spark implementation of compiled semantics
└── engine/       higher-level execution/trace boundaries
```

This package split is intentional. A class in `config` should normally answer *what maps where*;
a class in `execution` should answer *how that already-validated meaning is implemented in Spark*.

---

## 2. Design principles

### 2.1 Schema-backed navigation

Mappings navigate the DwC-DP schema rather than independently reproducing foreign-key knowledge in
Spark code. Relations are resolved through `SchemaGraph` and `SchemaRelationResolver`.

This applies to both profile-declared strong relations and the supported weak relations. Mapping
configuration identifies the intended relation, usually using the relevant `via(...)` column; it does
not construct Spark join expressions itself.

### 2.2 Logical fields remain logical until execution

`FieldRef` identifies a field by its `SchemaPath` plus column name. It is not a Spark alias.

```text
FieldRef
  = logical schema path + field

Spark alias
  = execution-only physical binding of that FieldRef
```

Keeping these separate allows compiler diagnostics to describe the actual mapping source rather than
only an opaque Spark column name. It also avoids collisions when different resources expose the same
column name.

### 2.3 Ambiguity is explicit

The mapping engine does not silently select one row from an ambiguous one-to-many relation.
Configuration must declare the intended cardinality behavior.

Typical examples are:

- accepted Identification: filter first, then `EXACTLY_ONE`;
- evidence Material for Occurrence enrichment: `EXACTLY_ONE`;
- agent-role and reference junctions: `FAN_OUT` where multiple contributions are meaningful.

If an `EXACTLY_ONE` path produces zero or multiple usable matches, it contributes no resolved row.
That is deliberate lossiness rather than an arbitrary tie-break.

### 2.4 Structural identity is not DwC data

DwC-DP `*_pk` / `*_fk` fields are structural package keys. They are used for navigation, row
identity, grouping, and attachment, but are not emitted into a DwC term merely because a natural
identifier is missing.

When the DwC-A core requires an identifier and the public identifier is absent, the executor creates
a deterministic internal/public fallback from the structural primary key; see
[§10](#10-core-identity-and-structural-keys).

### 2.5 Target meaning is shared across output envelopes

A `CompiledTargetProducer` has one execution meaning regardless of whether the resulting term is
placed on the DwC-A core or an extension row. `SparkTargetExpression` owns that shared source and
aggregation interpretation.

The surrounding core and extension executors deliberately remain separate because their row/grouping
models are different.

### 2.6 Lossiness is deliberate

DwC-DP can represent relationships which DwC-A cannot represent without flattening, promotion, or
omission. Those choices belong in mapping configuration and documented conversion policy, not hidden
inside low-level Spark joins.

---

## 3. Schema model and relation resolution

### 3.1 `SchemaGraph`

`SchemaGraph` is the mapping engine's view of the DwC-DP profile. It provides:

- known resources;
- fields per resource;
- primary keys and schema metadata;
- directed relations between resources;
- relation lookup/resolution;
- schema paths used by the mapping DSL and compiler.

Mappings should rely on this graph rather than hard-code assumptions about how two tables happen to
be joined in one dataset.

### 3.2 `SchemaPath`

A `SchemaPath` identifies a root resource plus a sequence of resolved schema relations.

```text
occurrence
  └─ identification
       └─ identification-agent-role
            └─ agent
```

A field on the final path is represented by a `FieldRef`:

```java
agent.field("preferredAgentName")
```

The path is part of the identity of that field. `agent.preferredAgentName` reached through an
Identification AgentRole is therefore distinct from an Agent reached through another ownership path.

### 3.3 `SchemaRelationResolver`

Relation-step resolution is centralized in `SchemaRelationResolver`.

For a schema-backed step it delegates to `SchemaGraph.resolve(...)`. For an explicit-column relation
it verifies that:

- the target resource exists;
- the source column exists on the current resource; and
- the target column exists on the target resource.

The shared resolver is used by compilation, validation, and Spark path execution when they interpret
a `RelationStep`. This keeps explicit and inferred relation semantics aligned across the validation and
execution boundary.

Compiler-specific code still owns compiler diagnostics: the shared resolver defines relation
semantics, while the compiler wraps failures with mapping scope, nearby paths, and mapping decisions.

---

## 4. Mapping paths

`MappingPath` is the schema-aware navigation DSL used by mapping configuration.

A typical path is:

```java
MappingPath occurrence = MappingPath.root(graph, "occurrence");
MappingPath identification =
    occurrence
        .join("identification")
        .via("occurrence_fk")
        .filter(FilterExpression.eq("isAcceptedIdentification", true))
        .optional()
        .exactlyOne();
```

The resulting object contains both:

- the resolved `SchemaPath`; and
- the executable `RelationStep` chain used later by compilation/execution.

It is immutable and branchable, so a common prefix can safely feed several mappings:

```text
occurrence
    │
    └── accepted identification
           ├── identification-taxon
           ├── identification-agent-role → agent
           └── agent via identifiedByID
```

### 4.1 Relation requirement

A relation can be:

- `optional()` — an absent optional resource/path contributes nulls/no matching rows;
- `required()` — absence is an error.

This is distinct from cardinality. Requirement answers whether the relation/resource may be absent;
cardinality answers how many matching rows are acceptable.

### 4.2 Cardinality

The principal strategies are:

- `fanOut()` — preserve multiple matches;
- `exactlyOne()` — accept only an unambiguous single match.

Filters are applied before the cardinality decision. This matters for accepted Identification: there
may be many Identification rows, but the mapping asks whether there is exactly one row after
`isAcceptedIdentification = true` has been applied.

### 4.3 Filters

`FilterExpression` expresses row-level filtering as part of the mapping definition rather than in
Spark-specific code. It supports the logical expressions needed by the current mappings, including
comparisons, null checks, and compound expressions.

### 4.4 Explicit versus inferred relation columns

Most mappings use schema-backed navigation with `via(...)`. Explicit relation columns exist for cases
where the relation must be specified directly. Explicit relations are still schema-validated; they
are not an escape hatch for referencing nonexistent fields.

---

## 5. Target mappings and value aggregation

`TargetFieldMapping` describes how one or more logical source fields produce one target DwC-A term.

Conceptually:

```text
TargetFieldMapping
├── target term
├── source mode
│   ├── ONE_OF
│   └── ALL_OF
├── source FieldRefs
├── ValueAggregation
├── origin
│   ├── EXPLICIT
│   └── INFERRED
├── optional contribution identity
└── optional order-by field
```

### 5.1 `ONE_OF`

`ONE_OF` represents ordered alternatives for one logical value. With `FirstNonNull`, the first usable
source wins.

For example, a resolved Agent name can be a fallback after an explicit publisher-supplied literal:

```text
publisher literal
      │
      └── if absent → resolved agent name
```

The order of sources is therefore semantic.

### 5.2 `ALL_OF`

`ALL_OF` means the source fields are contributions rather than alternatives. This is used for
list-valued targets such as ordered agent-role names.

### 5.3 Aggregations

Current aggregation concepts include:

- `FirstNonNull` — choose the first usable alternative;
- `ExactlyOne` — emit the sole source value where the target contract expects one;
- `Delimited` — aggregate multiple contributions into a delimiter-separated target;
- `LabeledOrFallback` — render `label + separator + name`, otherwise use fallback sources;
- `PreferredLabeledOrFallback` — an explicit preferred source wins before labeled/fallback rendering;
- `Named` — retained as an explicit DSL aggregation concept for named/specialized semantics.

### 5.4 Delimited values

`Delimited` semantics are implemented once by `SparkTargetExpression` for producer-level execution.
The contract is:

1. all declared source contributions participate;
2. null values are removed;
3. contribution identity is respected when declared;
4. explicit ordering is respected when declared;
5. unordered output is sorted deterministically;
6. `distinct` is applied when requested; and
7. an empty aggregate is `null`, not an empty string.

Deterministic ordering matters because Spark row encounter order is not a mapping semantic and must
not leak into DwC-A output.

### 5.5 Contribution identity

Some fan-out paths can produce the same logical contribution more than once because of the physical
join shape. A mapping may therefore declare a `contributionIdentity(...)` separate from the rendered
value.

For example, ordered agent roles can use the role's agent FK as contribution identity while rendering
the Agent's preferred name. Deduplication then follows the logical contributor rather than relying on
string equality alone.

### 5.6 Ordering

`orderBy(...)` expresses semantic contribution order. It is carried through compilation and used by
the shared Spark target expression/merge logic.

Ordering belongs to the mapping definition. It should not be reconstructed later from incidental
Spark row order.

---

## 6. Fragments, extensions, and mapping plans

### 6.1 Core fragments

A `CoreFragment` contributes fields to the DwC-A core row. It has a source/path and a set of target
producers, but does not create a separate extension row set.

Examples include:

- accepted Identification enrichment of an Occurrence core;
- one evidence Material enriching an Occurrence core;
- Event protocol/provenance enrichment.

### 6.2 Extension fragments

An `ExtensionFragment` contributes rows or enrichment to one DwC-A extension row type.

An extension fragment declares the structural information needed to materialize those rows:

- `scopeKey` — which logical parent owns the row;
- optional `rowIdentity` — what defines a newly produced extension row;
- optional `rowMatch` — how an enrichment fragment matches an already produced row;
- target fields.

These are logical `FieldRef`s, not physical Spark aliases.

### 6.3 Extension row composition

An extension can compose fragments in two ways.

#### `ENRICH`

`ENRICH` describes one logical row set with additional fragments enriching those rows.

```text
base row fragment
      │
      ├── enrichment A
      ├── enrichment B
      └── target merges
```

Exactly one row-defining fragment is allowed. Enrichment fragments share the same source-resource
scope and are joined using parent key plus `rowMatch` when one is declared.

#### `UNION`

`UNION` describes independent row-producing branches which all contribute rows to the same DwC-A
extension.

```text
fragment A ─┐
fragment B ─┼── UNION → one extension row set
fragment C ─┘
```

This is used where several DwC-DP ownership routes become one flat DwC-A extension, for example media
or references sourced from several domain objects.

Visible extension payload is used for UNION deduplication; synthetic Spark row identity must not make
two otherwise identical extension rows appear different.

### 6.4 `MappingPlan`

A `MappingPlan` is the complete declarative mapping for one chosen DwC-A core type. It combines:

- core source resource and core type;
- direct core targets;
- imported core fragments;
- core target merges;
- extension definitions and fragments;
- extension target merges and row-composition policies.

`EventDwcaMapping.current(graph)` and `OccurrenceDwcaMapping.current(graph)` are the canonical current
configurations which assemble these plans.

---

## 7. Compilation and validation

The compiler is a semantic boundary, not merely a data-model conversion.

```text
MappingPlan
    │
    ▼
MappingCompiler
    ├── resolve schema paths/relations
    ├── validate fields and fragment scope
    ├── resolve target producers
    ├── validate target ownership/merges
    ├── determine required datasets/columns
    ├── prune unused dataset requirements
    └── record diagnostics/decisions
    │
    ▼
CompiledMapping
```


### 7.1 Fail before Spark where possible

Invalid configuration detectable from the mapping definition and schema should fail here.

Examples include:

- invalid relation targets/columns;
- missing fragment scope;
- invalid target ownership;
- a declared target merge with no producers;
- incompatible/ambiguous mapping declarations.

Execution still contains defensive invariant checks. Those should describe an impossible compiler
state rather than serve as the normal validation mechanism.

### 7.2 Compiled target producers

Compilation turns declarative target fields into `CompiledTargetProducer`s with resolved ownership,
logical source fields, aggregation, optional contribution identity, and ordering.

The executor should not have to infer which fragment owns a target or guess how two duplicate target
producers should be combined.

### 7.3 Target merges

When multiple producers intentionally contribute to the same target, that must be declared as a
target merge. The compiler distinguishes deliberate merging from accidental duplicate ownership.

This is important for mappings where the same DwC term has a defined precedence or list combination
across independent paths.

### 7.4 Input requirements and pruning

Compilation can determine which DwC-DP resources and fields are actually required by the chosen
mapping plan. `CompiledMappingDatasetPruner` reduces the execution input to those requirements rather
than treating the whole DwC-DP package as one mandatory fixed schema.

This also makes missing optional branches naturally skippable when no configured path requires a
row from them.

### 7.5 Diagnostics

Compiler diagnostics and mapping decisions are retained as engine vocabulary even when a particular
call path does not currently surface every field. They exist so mapping failures can report logical
context such as:

- mapping/fragment scope;
- target term;
- producer owner;
- aggregation;
- source/path;
- candidate or nearby schema relations.

That context is substantially more useful than a downstream Spark "column not found" or ambiguous
join failure.

---

## 8. Spark execution

The execution layer deliberately separates navigation, target-expression semantics, extension row
materialization, and final record assembly.

### 8.1 `SparkMappingPathExecutor`

`SparkMappingPathExecutor` implements a mapping path against Spark datasets.

It owns:

- loading path resources through `TableLoader`;
- schema-resolved joins;
- optional versus required resource behavior;
- row filters;
- cardinality handling;
- path-qualified Spark aliases;
- relation execution metrics;
- reusable path-prefix execution through `SparkPathPrefixCache`.

It deliberately stops before DwC-A target materialization.

```text
Mapping / RelationSteps
        │
        ▼
SparkMappingPathExecutor
        │
        ▼
SparkPathResult
├── Dataset<Row>
└── FieldRef → Spark alias bindings
```

The executor validates the mapping boundary before executing it. Shared relation resolution does not
move schema validation into late Spark execution.

### 8.2 Path-prefix reuse

Different mapping fragments often share an expensive navigation prefix. `SparkPathPrefixCache`
allows execution to reuse the longest already materialized prefix while preserving the corresponding
logical field aliases and relation metrics.

The cache is an execution optimization only. It does not change mapping semantics.

### 8.3 `SparkTargetExpression`

`SparkTargetExpression` implements the shared meaning of one `CompiledTargetProducer` once physical
source `Column`s have been bound.

It owns two related operations:

```text
row expression
    source values on one logical row
        → one target Column

aggregate expression
    source contributions across grouped rows
        → one target Column
```

Core and extension execution resolve their columns differently, but both delegate the actual source
combination and producer aggregation semantics here. This prevents the same mapping declaration from
behaving differently simply because its target happens to be on the core rather than an extension.

### 8.4 `SparkExtensionMaterializer`

`SparkExtensionMaterializer` materializes one compiled DwC-A extension independently of the final
core attachment.

It owns:

- `ENRICH` versus `UNION` composition;
- `scopeKey` → parent ownership;
- row-producing fragments;
- `rowIdentity` and `rowMatch`;
- keyless physical child rows;
- extension target merges;
- duplicate visible-row handling;
- empty-payload filtering;
- deterministic per-parent row limits.

It returns `ExtensionMaterializationResult` containing the materialized dataset, parent-key metadata,
row key, and target-term → physical-column map.

#### Keyless extension rows

Some legitimate child tables do not have a declared logical row key. For a row-producing fragment
without `rowIdentity`, each physical result row remains one extension row. A synthetic execution-only
row key prevents grouping all children of one parent into a single row.

That synthetic key is never emitted as a DwC term and is excluded from visible-payload deduplication
and deterministic row-limit ordering.

#### Deterministic row limits

When an extension declares a maximum number of rows per parent, the materializer ranks rows from
stable visible target payload rather than from Spark physical identity. This keeps the cap stable
across retries and partitions.

### 8.5 `SparkExtendedRecordExecutor`

`SparkExtendedRecordExecutor` owns the outer DwC-A-shaped record assembly.

It:

1. loads the chosen core resource;
2. establishes the core identity;
3. projects direct core targets;
4. executes core enrichment fragments;
5. executes core target merges;
6. asks `SparkExtensionMaterializer` to materialize each configured extension;
7. bridges each extension's logical parent scope back to the core;
8. groups extension rows onto the core; and
9. creates `ExtendedRecord` values.

The class should therefore remain distinct from `SparkExtensionMaterializer`: one owns whole-record
assembly and core attachment, while the other owns the internal row semantics of a single extension.

---

## 9. Core and extension target merges

A target merge combines multiple intentional producers for the same DwC target.

Typical cases are:

- ordered fallback: several producers with `FirstNonNull` precedence;
- list-valued target: contributions from multiple independent paths merged with `Delimited`.

Core and extension merges use different grouping envelopes:

```text
core merge
    grouped by core structural identity

extension merge
    grouped by parent key + extension row key
```

Those envelopes remain owned by their respective executors.

The semantic rules are common:

- producer order matters for `FirstNonNull`;
- null/empty contributions do not become values;
- contribution identity must be consistently declared across producers if it is used;
- order-by must be consistently declared across producers if it is used;
- identified contributions are deduplicated by their logical identity/value;
- ordered delimited output follows declared order;
- unordered delimited output is deterministic;
- `distinct` is applied according to `ValueAggregation.Delimited`.

A mixed merge where only some producers declare identity or ordering is rejected rather than silently
combining incompatible semantics.

---

## 10. Core identity and structural keys

DwC-DP structural keys and DwC identifiers have different jobs.

### 10.1 Structural primary key

The resource primary key (`event_pk`, `occurrence_pk`, etc.) is used for:

- stable row identity inside the package;
- joining compiled fragments back to the core;
- extension attachment bridges;
- deterministic fallback-ID construction.

It is not directly copied into a DwC target term.

### 10.2 Public/natural core identifier

For the two supported DwC-A core types the preferred IDs are:

```text
Event       → eventID
Occurrence  → occurrenceID
```

If the natural identifier is null/blank, the executor generates a deterministic fallback:

```text
urn:gbif:dwcdp:event:<event_pk>
urn:gbif:dwcdp:occurrence:<occurrence_pk>
```

This preserves a usable DwC-A core identifier without pretending that the package's structural key
was publisher-supplied DwC data.

---

## 11. Occurrence as core and as Event extension

Occurrence semantics are needed in two different output envelopes:

1. Occurrence is the selected DwC-A core; or
2. Event is core and Occurrence is a nested DwC-A Occurrence extension row.

The domain enrichment logic should not be duplicated between those scenarios, but their structural
attachment semantics should remain explicit.

```text
                    OccurrenceEnrichment
                    /                  \
                   /                    \
      OccurrenceCoreMapping         OccurrenceMapping
      CoreFragmentBuilder           ExtensionFragmentBuilder
                                    scopeKey(event_fk)
                                    rowMatch(occurrence_pk)
```

`OccurrenceEnrichment` contains shared occurrence-domain paths and target definitions, such as:

- Organism enrichment;
- accepted Identification and taxon fallback;
- accepted Identification agents/roles;
- evidence Material;
- Material geological context;
- Material protocol;
- Material provenance.

It does **not** own core/extension builders. `OccurrenceCoreMapping` remains responsible for core
fragments, while `OccurrenceMapping` remains responsible for Event-owned extension row scope and
matching.

This pattern is deliberate: commonize semantic rules without hiding meaningful output-envelope
differences behind a generic builder abstraction.

---

## 12. Configuration entry points

The canonical current plans are assembled by:

```java
EventDwcaMapping.current(graph)
OccurrenceDwcaMapping.current(graph)
```

These are the primary places to see which fragments/extensions are enabled for each core type.

Domain-specific configuration is split into focused classes such as:

```text
EventCoreMapping
OccurrenceCoreMapping
OccurrenceMapping
OccurrenceEnrichment
AgentMapping
AgentRoleMapping
AssertionMapping
ChronometricMapping
IdentificationMapping
IdentifierMapping
MultimediaMapping
NucleotideMapping
ReferenceMapping
DirectFieldMappings
TargetTerms
```

The exact list can evolve; the architectural rule is more important than the names:

- domain config declares schema paths and target semantics;
- plan config assembles those declarations for Event or Occurrence core;
- compiler validates/normalizes them;
- execution code does not contain domain-specific mapping choices.

### `DirectFieldMappings`

`DirectFieldMappings` is used where a DwC-DP resource can contribute its ordinary fields directly
through `TargetTerms` resolution. Explicit config is still used when precedence, aggregation,
renaming, ownership, or lossiness needs to be stated.

Generic direct mapping should not replace a deliberate domain rule merely to reduce configuration
code.

---

## 13. Adding or changing a mapping

A normal mapping change should follow this order.

### Step 1 — identify the schema-backed ownership path

Start from the actual DwC-DP schema relation, not from a desired Spark join.

```text
source resource
    → relation/junction
        → owned resource
```

Use the schema profile and `SchemaGraph` to confirm the intended relation and `via` column.

### Step 2 — decide cardinality and filtering

Ask explicitly:

- can the resource/path be absent?
- can more than one match exist?
- if multiple matches exist, are they all meaningful contributions or is the result ambiguous?
- does a filter narrow the candidate rows before cardinality is evaluated?

Then encode that in `MappingPath`.

### Step 3 — declare target semantics

Choose the target term and source behavior:

- `ONE_OF` for precedence/fallback;
- `ALL_OF` for contributions;
- appropriate `ValueAggregation`;
- contribution identity/order if fan-out ordering or deduplication matters.

Do not implement these rules directly in Spark code.

### Step 4 — choose the output envelope

Decide whether the mapping contributes to:

- the core (`CoreFragment`);
- an existing extension row (`ENRICH` + `rowMatch`); or
- a new/independent extension row (`rowIdentity` or keyless row production / `UNION`).

For extension mappings, define the logical parent `scopeKey` explicitly.

### Step 5 — add it to the canonical plan

Wire the fragment into `EventDwcaMapping.current(graph)` and/or
`OccurrenceDwcaMapping.current(graph)` as appropriate.

If the same domain rule is used in both core and extension envelopes, share the domain/path/target
semantics rather than duplicating them, while keeping each envelope's structural builder code visible.

### Step 6 — compile-time tests first

Where the issue can be detected from the schema/configuration, test compiler or mapping-definition
behavior rather than relying only on Spark execution failures.

### Step 7 — execution tests for behavioral semantics

Use Spark execution tests for semantics which only become meaningful during dataset operations, such
as:

- exactly-one behavior on actual rows;
- aggregation ordering/deduplication;
- extension row identity/matching;
- UNION row behavior;
- merge behavior;
- null/empty output semantics.

---

## 14. Testing

The test suite should preserve **behavioral contracts**, not internal implementation shape.

Important contracts include:

### Schema/path

- schema-backed relation resolution;
- explicit relation field validation;
- filter-before-cardinality behavior;
- optional/required path handling.

### Compilation

- invalid targets/relations fail before Spark execution;
- duplicate producers require an explicit target merge;
- declared merges have producers;
- fragment scope/identity is valid;
- diagnostics retain useful logical context.

### Target expressions

The same `CompiledTargetProducer` semantics must hold in core and extension contexts.

In particular:

- multi-source delimited mappings use all declared sources;
- distinct and ordering semantics are deterministic;
- contribution identity is respected;
- an aggregate with no values returns `null`, not `""`.

### Extension execution

- keyless children remain separate rows;
- `ENRICH` joins using the declared row match;
- `UNION` preserves independent row sets and deduplicates visible duplicate payload;
- row limits are deterministic and do not depend on synthetic Spark identity.

### Core identity

- natural Event/Occurrence IDs win when present;
- blank/missing IDs receive deterministic URN fallbacks;
- structural PKs are not emitted as DwC target terms.

---

## 15. Lossy conversion rules

DwC-DP → DwC-A is intentionally lossy in places where the target model cannot faithfully preserve
DwC-DP structure or where the source relationship is ambiguous.

### 15.1 Exactly-one enrichment

When flattening a related object onto a core/extension row requires one unambiguous owner, mappings
use `EXACTLY_ONE` rather than choosing an arbitrary match.

Examples include:

- accepted Identification after filtering;
- evidence Material used to enrich an Occurrence;
- Material geological context where one context is required for scalar flattening.

Zero or multiple matches contribute no flattened value.

### 15.2 Event-core media promotion

When Event is core, DwC-A cannot represent a Multimedia extension nested beneath an Occurrence
extension row. Occurrence- and Material-owned media that are mapped in that scenario are therefore
promoted to the Event's top-level Multimedia extension.

The original nested ownership is not fully representable in DwC-A.

### 15.3 Deterministic list flattening

Where multiple related records legitimately become one list-valued DwC target, the mapping uses
explicit aggregation, contribution identity, and ordering where available. Unordered output is
stabilized so Spark partition/encounter order does not become visible output semantics.

### 15.4 Taxonomic fallback

Identification-taxon fallback uses one unambiguous `identification-taxon` row. Multiple taxon-formula
components are not flattened by arbitrarily selecting one component.

### 15.5 References follow ownership junctions

Bibliographic resources are reached through the domain-specific `*-reference` junction and then
`bibliographic-resource`. A domain object is not treated as directly owning a bibliographic resource
through an invented shortcut relation.

---

## 16. Deferred and intentionally separate work

This document is not intended to be a manually maintained table-by-table coverage ledger. The
mapping configuration and compiler are the source of truth for what is currently wired.

Known areas can still be tracked here when they represent a design question rather than ordinary
configuration work. Current examples include:

- remaining AgentRole ownerships where the correct DwC-A representation needs to be chosen;
- remaining bibliography/ownership paths where mapping policy has not yet been selected;
- virtual Occurrence synthesis for Material records without a local evidence Occurrence.

Virtual Material Occurrence synthesis is intentionally a separate design problem. It changes the
row model by creating Occurrence rows rather than merely mapping another field/path and should not be
folded into ordinary mapping-completeness work.

When future coverage auditing is needed, prefer deriving the audit from the current schema graph and
canonical mapping plans rather than maintaining a large static table which can silently become stale.

---

## Implementation entry points

For someone entering the implementation, the most useful starting points are:

```text
Configuration / complete plans
  EventDwcaMapping.current(...)
  OccurrenceDwcaMapping.current(...)

Path and target declaration
  MappingPath
  TargetFieldMapping
  ValueAggregation

Schema
  SchemaGraph
  SchemaRelationResolver

Compilation
  MappingCompiler
  MappingValidator
  CompiledMapping
  CompiledMappingDatasetPruner

Execution
  SparkMappingPathExecutor
  SparkTargetExpression
  SparkExtensionMaterializer
  SparkExtendedRecordExecutor
```

Read the configuration to understand **what** is mapped, the compiler to understand **what is legal**,
and the execution classes to understand **how the compiled semantics are implemented in Spark**.
