# DwC-DP to DwC-A mapping — design & coverage

This document is the design and coverage record for the DwC-DP → DwC-A ingestion builders under
`org.gbif.pipelines.spark.dwcdp`. It owns the *why* and the *what's covered* — rationale, join
design, and gap tracking. Javadoc on the classes themselves stays minimal by design: joins, wiring,
and deferred scope only. If you're editing a builder and need the reasoning behind a decision, it's
here, not in the source.

## Contents

1. [Overview](#1-overview)
2. [Code structure](#2-code-structure)
3. [Scenario view](#3-scenario-view)
4. [Extension view](#4-extension-view)
5. [Coverage matrix](#5-coverage-matrix)
6. [Schema view](#6-schema-view)
7. [Implemented lossy behaviour](#7-implemented-lossy-behaviour)

---

## 1. Overview

```text
DwC-DP package
│
├── event ─────────────────────────────────────────────────────────────► DwC Event core
│   ├── direct event fields                                              │
│   ├── parent event, geological context, protocols, provenance ────────┘
│   ├── event-media + occurrence-media + material-media ───────────────► Multimedia
│   ├── event-assertion ───────────────────────────────────────────────► eMoF
│   ├── event-identifier ──────────────────────────────────────────────► Identifier
│   ├── occurrence + unlinked collection material ─────────────────────► Occurrence extension
│   └── survey + survey-target ─────────────────────────────────────────► Humboldt Event
│
└── occurrence ────────────────────────────────────────────────────────► DwC Occurrence core
    ├── direct occurrence fields                                         │
    ├── organism and accepted identification ───────────────────────────┘
    ├── material
    │   ├── usage-policy, provenance, protocol, geological-context ────► occurrence terms
    │   ├── material-media ────────────────────────────────────────────► Multimedia
    │   ├── material-assertion ────────────────────────────────────────► eMoF
    │   └── material-identifier ───────────────────────────────────────► Identifier
    ├── occurrence-media ──────────────────────────────────────────────► Multimedia
    ├── occurrence-assertion ──────────────────────────────────────────► eMoF
    ├── occurrence-identifier ─────────────────────────────────────────► Identifier
    └── identification history ─────────────────────────────────────────► Identification History
```

When `event` is core, occurrence rows and their occurrence-level extensions are serialised inside
the DwC Occurrence extension. DwC-A has no way to nest multimedia rows beneath those nested
occurrences, so occurrence and material media are promoted to the event's top-level Multimedia
extension (see [§7](#7-implemented-lossy-behaviour)).

**Status note:** `material` → virtual occurrence synthesis (materials with no local
`evidenceForOccurrenceID`) is currently **paused** (`MaterialJoinBuilder.VIRTUAL_MATERIAL_OCCURRENCES_ENABLED
= false`). While paused, those materials — and everything joined onto them (media, assertions,
protocol, provenance, geological context, identifiers) — are dropped, counted as `unresolved` in the
conversion report. See [§3.5](#35-material-evidence-linked) and [§4.4](#44-material--occurrence-enrichment).

---

## 2. Code structure

```text
   CoreBuilder
   (Event / Occurrence)
        │
        ├──enriches──▶ JoinBuilder ───────┐
        │              (same-shape         │ reused by
        │               enrich)            ▼
        └──attaches──▶ ExtensionBuilder ───┘
                       (new keyed extension)

        all three ──▶ shared utilities
                      (TermResolver, JoinFunnel,
                       ExtensionAggregator, CoreBuilderSupport)
```

Three class kinds, one per suffix. The suffix is a contract, not a domain label — a new class's
name should follow from its method signature, not the other way round.

| Suffix | Contract | Typical signature |
| --- | --- | --- |
| `*CoreBuilder` | One per DwC-A core type (`Event`, `Occurrence`). Orchestrates: calls `JoinBuilder`s to enrich the core Dataset in place, calls `ExtensionBuilder`s to build nested extensions, left-joins them on, maps final rows to `ExtendedRecord`. | `build(SparkSession, TableLoader) → Dataset<ExtendedRecord>` |
| `*JoinBuilder` | Enriches an existing core-shaped Dataset in place — same shape in, same shape out. Joins a lookup/junction table and adds or coalesces columns onto the existing rows. Also exposes `computeFunnel` mirroring its own join decision logic, for the conversion report. | `enrich(TableLoader, Dataset<Row>) → Dataset<Row>` <br> `computeFunnel(TableLoader, ...) → Optional<JoinFunnel>` |
| `*ExtensionBuilder` | Builds a *new*, separately-keyed Dataset representing one DwC-A extension row type (Multimedia, eMoF, Identifier, ...), via `ExtensionAggregator`. Never mutates core columns directly — always left-joined on afterward by the caller. | `build(SparkSession, TableLoader) → Optional<Dataset<Row>>` (two columns: `parentIdColumn`, `extJsonColumn`) |

**Exception to note:** `MaterialJoinBuilder#singleMaterialOccurrenceLinks` is a `JoinBuilder` method
reused as a shared FK-resolution utility by several `ExtensionBuilder`s (`MediaExtensionBuilder`,
`IdentifierExtensionBuilder`) and other `JoinBuilder`s (`MaterialProtocolJoinBuilder`,
`MaterialProvenanceJoinBuilder`), rather than each resolving material→occurrence independently. It's
the one place a `JoinBuilder` acts as shared infrastructure rather than a self-contained enrichment.

**Mechanical naming check for new classes:** if the method returns the same shape it took in and
mutates columns → `*JoinBuilder`. If it returns a new keyed Dataset meant to be left-joined on
afterward → `*ExtensionBuilder`. (`chronometric-age`, when built, is an `*ExtensionBuilder` by this
rule — it's a new row-type extension, not a flat enrichment onto an existing core row.)

**Shared utilities** (no domain of their own, used across builders):

- `TermResolver` / `RowTermMapper` — raw DwC-DP column name → qualified DwC term URI
- `DwcDpTermMappings` — explicit rename table `TermResolver` consults before falling through to `TermFactory`
- `JoinFunnel` — shared report-bucket record shape every `computeFunnel` returns
- `ExtensionAggregator` — shared groupBy→JSON helper every `*ExtensionBuilder` uses
- `CoreBuilderSupport` — shared natural-id fallback + extension-attachment helpers `*CoreBuilder` uses

---

## 3. Scenario view

Which builders fire, and in what order, differs by ingestion scenario. Each tree below is the
sequence for one scenario; skip conditions are noted where a step depends on optional tables.

### 3.1 Occurrence-core

```text
OccurrenceCoreBuilder.build
├─ load occurrence (required)
├─ OrganismJoinBuilder.enrichOccurrences           (skip: organism absent)
├─ IdentificationJoinBuilder.enrichOccurrences      (skip: identification absent, or not exactly-one-accepted)
├─ MaterialJoinBuilder.enrichOccurrences            (skip: not exactly-one-material-evidence)
│   ├─ MaterialGeologicalContextJoinBuilder.enrichOccurrences
│   ├─ MaterialProvenanceJoinBuilder.enrichOccurrences
│   └─ MaterialProtocolJoinBuilder.enrichOccurrences
├─ ProtocolJoinBuilder.resolveProtocolFk            (occurrenceProtocol_fk → samplingProtocol)
├─ AgentJoinBuilder.resolveAgentNameCoalesceInto     (recordedByID, identifiedByID)
├─ MediaExtensionBuilder.buildOccurrenceMediaExtension     (skip: occurrence-media & material-media both absent)
├─ AssertionExtensionBuilder.buildOccurrenceAssertionExtension (skip: occurrence-assertion & material-assertion both absent)
├─ IdentifierExtensionBuilder.buildOccurrence        (skip: occurrence-identifier & material-identifier both absent)
├─ IdentificationExtensionBuilder.build              (skip: identification absent)
├─ NucleotideExtensionBuilder.buildOccurrence        (skip: nucleotide-analysis absent, or no materialEntity_fk rows)
└─ map rows → ExtendedRecord (coreRowType = dwc:Occurrence)
```

### 3.2 Event-core (no occurrence)

```text
EventCoreBuilder.build
├─ load event (required)
├─ resolveParentEventId                              (parentEvent_fk → parentEventID)
├─ GeologicalContextJoinBuilder.enrichEvents          (skip: geological-context absent)
├─ ProtocolJoinBuilder (eventProtocol_fk, georeferenceProtocol_fk, event-protocol, survey-protocol)
├─ ProvenanceJoinBuilder.enrichEvents
├─ AgentJoinBuilder.resolveAgentNameCoalesceInto      (eventConductedByID, georeferencedByID)
├─ MediaExtensionBuilder.buildEventMediaExtension     (skip: event-media absent)
├─ AssertionExtensionBuilder.buildEventAssertionExtension (skip: event-assertion absent)
├─ IdentifierExtensionBuilder.buildEvent              (skip: event-identifier absent)
├─ HumboldtExtensionBuilder.build                     (skip: survey absent)
├─ NucleotideExtensionBuilder.buildEvent              (skip: nucleotide-analysis absent, or no event_fk-only rows)
└─ map rows → ExtendedRecord (coreRowType = dwc:Event)
```

### 3.3 Event-core + occurrence extension (nested)

```text
EventCoreBuilder.build  (as §3.2)
└─ OccurrenceExtensionBuilder.build                   (skip: occurrence absent, or occurrence has no event_fk)
    ├─ OrganismJoinBuilder.enrichOccurrences
    ├─ IdentificationJoinBuilder.enrichOccurrences
    ├─ MaterialJoinBuilder.enrichOccurrences
    │   ├─ MaterialGeologicalContextJoinBuilder.enrichOccurrences
    │   ├─ MaterialProvenanceJoinBuilder.enrichOccurrences
    │   └─ MaterialProtocolJoinBuilder.enrichOccurrences
    ├─ AgentJoinBuilder (recordedByID, identifiedByID)
    ├─ nest occurrence-media/occurrence-assertion/identification-history/occurrence-identifier
    │   as JSON columns (same builders as §3.1, called per-occurrence before aggregation)
    └─ left-join onto event by eventID, aggregate as occurrenceExtJson
        (occurrence/material media promoted to event-level Multimedia — see §7)
```

### 3.4 Material — evidence-linked (real occurrence)

```text
MaterialJoinBuilder.singleMaterialOccurrenceLinks
├─ evidenceForOccurrenceID resolves to a local occurrence  ─▶ real link
│   used by: MaterialGeologicalContextJoinBuilder, MaterialProvenanceJoinBuilder,
│            MaterialProtocolJoinBuilder, MediaExtensionBuilder (material-media),
│            AssertionExtensionBuilder (material-assertion), IdentifierExtensionBuilder
│            (material-identifier), NucleotideExtensionBuilder (materialEntity_fk path)
└─ material.collectionEvent_fk / derivationEvent_fk ── NOT surfaced (excluded columns, always)
```

### 3.5 Material — virtual occurrence (⏸ PAUSED)

```text
MaterialJoinBuilder.virtualMaterialOccurrences
├─ VIRTUAL_MATERIAL_OCCURRENCES_ENABLED == false  ──▶ returns Optional.empty() — always, currently
└─ (when re-enabled) material with no local evidenceForOccurrenceID,
   collectionEvent_fk resolves to a real event
   ──▶ synthesised occurrence: occurrenceID = materialEntityID, or
       urn:gbif:dwcdp:material:<materialEntity_pk> as fallback;
       materialSampleID = materialEntityID; basisOfRecord = MaterialSample;
       occurrenceStatus = present
   ──▶ everything in §3.4's "used by" list becomes reachable for these materials too
```

---

## 4. Extension view

One section per DwC-A target. Each lists the joins (mirrors the class's javadoc bullet — this is
the canonical copy design rationale lives in), the design decisions behind them, and known gaps.

### 4.1 Agent resolution

**Joins** (`AgentJoinBuilder`):
- `{idColumn} = agent.agentID` (left outer, coalesce-if-null) → `{nameColumn}`
- Wired for: `event.eventConductedByID→eventConductedBy`, `event.georeferencedByID→georeferencedBy`,
  `occurrence.recordedByID→recordedBy`, `occurrence.identifiedByID→identifiedBy`

**Design decisions:**
- Per the DwC-DP ingestion guide, explicit agent roles are fields on the core table itself (e.g.
  `georeferencedBy`), not separate `AgentRole` records — so this builder only targets those four
  confirmed pairs, not a generic "any `*ByID` column" sweep.
- `agentID` is the `agent` table's `weakPk`, not its `pk` — no `uniq` constraint in the DwC-DP
  profile. A publisher with duplicate `agentID`s fans this join out; not deduplicated, matching the
  trust-the-profile stance `ProtocolJoinBuilder` takes for its own surrogate keys.
- The `*ID` column itself (e.g. `recordedByID`) is a real DwC term (`dwc:recordedByID`) and is
  preserved unchanged — unlike `ProtocolJoinBuilder`'s FK, which is discarded after resolution,
  since it carries no DwC term of its own.

**Gaps:**
- All 9 `*-agent-role` junction tables are unhandled — no confirmed DwC-A field for roles without an
  explicit `*By`/`*ByID` pair (photographer, preparator, etc.). This is the single biggest unhandled
  category in the whole pipeline.
- `identification.identifiedByID` isn't agent-resolved — it's passed through raw by
  `IdentificationJoinBuilder`'s generic column copy, so if occurrence's own `identifiedBy` is null
  and the accepted identification's `identifiedByID` is set, an unresolved ID lands in the field
  instead of an agent name.

### 4.2 Protocol resolution

**Joins** (`ProtocolJoinBuilder`):
- `event.eventProtocol_fk` / `occurrence.occurrenceProtocol_fk` = `protocol.protocol_pk` (left outer, direct) → `samplingProtocol`
- `event.georeferenceProtocol_fk` = `protocol.protocol_pk` (left outer, coalesce-if-null) → `georeferenceProtocol`
- `event-protocol` / `survey-protocol` / `material-protocol` = `protocol.protocol_pk` (inner, junction, optional `protocolType` filter) → aggregated pipe-delimited list

**Design decisions:**
- Fallback policy: protocol table absent/malformed → raw FK value kept under the target column name,
  never dropped (mirrors `AssertionExtensionBuilder`'s tested fallback for `assertionProtocol_fk`).
- `eventProtocol_fk`/`occurrenceProtocol_fk` → `dwc:samplingProtocol` is this project's best
  inference from the DwC-DP schema, not independently confirmed against a mapping document the way
  the media field renames were.
- Display label: `"{protocolType}: {protocolName}"` when named, else `protocolDescription`.

**Gaps:**
- `identification.identificationProtocol_fk` is excluded in both `IdentificationJoinBuilder` and
  `IdentificationExtensionBuilder` — the protocol used to make an identification isn't surfaced.
- `molecular-protocol-agent-role` / `molecular-protocol-reference` — deferred (same open agent-role
  question as §4.1).
- `chronometric-age-protocol` — untouched; see §4.9.

### 4.3 Provenance

**Joins** (`ProvenanceJoinBuilder`, `MaterialProvenanceJoinBuilder`):
- `event.provenance_fk` / `event-provenance` = `provenance.provenance_pk` (left outer, direct + junction) → `fundingAttribution`, `fundingAttributionID`, `projectID`, `projectTitle` (unioned, deduped, pipe-delimited, sorted by `provenanceID`)
- `material.provenance_fk` / `material-provenance` — same aggregation, reused via `ProvenanceJoinBuilder#aggregateProvenanceFields`, resolved onto occurrence via `MaterialJoinBuilder#singleMaterialOccurrenceLinks`

**Design decisions:**
- Only the four list-valued fields above are handled — confirmed via `CoreInterpreter`'s
  `extractListValue`-based interpreters. `provenance.references` is deliberately left alone
  (`event.eventReferences` already sources it, and the target interpreter reads it as single-valued
  — pipe-joining would corrupt it).
- No occurrence/material precedence conflict for these four fields — occurrence has no
  `fundingAttribution`/`projectID`/`projectTitle` fields of its own to collide with.

**Gaps:**
- `provenance.source`, `creator`, `providerLiteral`, `metadataCreatorLiteral`,
  `metadataProviderLiteral`, `furtherInformationURL`, `feedbackURL`, `bibliographicCitation` (+
  their `*ID` counterparts) — no confirmed downstream interpreter target, left unhandled.
- `media-provenance` — entirely untouched.

### 4.4 Material → occurrence enrichment

**Joins** (`MaterialJoinBuilder`):
- `material.evidenceForOccurrenceID = occurrence.occurrenceID` (natural-key, weak FK) → institution/collection/specimen fields, gated to exactly-one-material-per-occurrence

**Design decisions:**
- Exactly-one-match rule: zero or multiple material rows citing an occurrence both leave it
  unenriched, rather than guessing a tie-break — same rule `IdentificationJoinBuilder` applies.
- Occurrence's own value always wins on overlapping fields (`identifiedBy`, `dateIdentified`,
  `taxonID`, `scientificName`, ...).
- `material-usage-policy` is folded in *before* the exactly-one filtering (enriches `material` itself
  via `UsagePolicyJoinBuilder`), so `license`/`rightsHolder` flow through the ordinary column-bring-in
  logic with no separate wiring.

**Gaps:**
- `material.collectionEvent_fk` / `derivationEvent_fk` are excluded unconditionally — even on the
  real-evidence path, unaffected by the virtual-occurrence pause. A specimen's link back to its
  collecting/derivation event is lost entirely, currently.
- Virtual-occurrence synthesis is paused (§3.5) — every material without a local
  `evidenceForOccurrenceID` is dropped, along with everything joined onto it (see §3.4/§3.5).

### 4.5 Multimedia extension

**Joins** (`MediaExtensionBuilder`):
- `event-media` / `occurrence-media` = `media.media_pk` (left outer) → Simple Multimedia extension rows
- `material-media`, resolved via `MaterialJoinBuilder#singleMaterialOccurrenceLinks`, merged into the same extension as the owning occurrence's direct media
- `usage-policy` enriches joined media with `license`/`rightsHolder` before aggregation

**Design decisions:**
- Target is DwC-A's Simple Multimedia extension, not Audubon Core — confirmed against project
  mapping notes.
- Event-core packages: occurrence/material media is promoted to the event's top-level Multimedia
  extension, since DwC-A can't nest multimedia beneath a nested occurrence extension row (§7).

**Gaps:**
- `media.creator` — no confirmed downstream target.
- `media-agent-role` — same open agent-role question as §4.1.

### 4.6 eMoF (assertions)

**Joins** (`AssertionExtensionBuilder`):
- `event-assertion` / `occurrence-assertion` = parent core table (surrogate FK → natural id) → eMoF rows, column-renamed to eMoF equivalents
- `material-assertion`, resolved via `singleMaterialOccurrenceLinks`, merged into the occurrence's eMoF rows
- `assertionProtocol_fk` optionally resolved to a description via `protocol`

**Gaps:**
- `nucleotide-analysis-assertion` / `molecular-protocol-assertion` — deferred; needs its own
  aggregation, unioned in carefully to avoid a cartesian fan-out against each other.
- `chronometric-age-assertion` — untouched; see §4.9.

### 4.7 Identifier extension

**Joins** (`IdentifierExtensionBuilder`):
- `event-identifier` (direct) → event path
- `occurrence-identifier` (direct) + `material-identifier` (via `singleMaterialOccurrenceLinks`) → occurrence path, merged

**Design decisions:**
- `Extension.IDENTIFIER`'s row type is confirmed to exist, but no confirmed evidence of a downstream
  interpreter that reads it — unlike Multimedia/eMoF/Humboldt/Identification. Fields pass through via
  ordinary `TermResolver` (no confirmed field-level mapping to verify a rename scheme against).

### 4.8 Identification / Identification History

**Joins** (`IdentificationJoinBuilder`, `IdentificationExtensionBuilder`):
- `identification.occurrence_fk = occurrence.occurrence_pk` (left outer) → taxonomic rank hierarchy flattened onto occurrence core, only when exactly one `isAcceptedIdentification = true` row exists
- All linked `identification` rows (accepted or not) → Identification History extension, independent of the flattening above

**Gaps:**
- `identification.materialEntity_fk`-linked identifications are deferred, pending `material`'s own
  extension/history work.
- `identification-agent-role`, `identification-reference` — untouched.
- `identification.identifiedByID` — see §4.1's gap note.

### 4.9 DNA Derived Data (Nucleotide)

**Joins** (`NucleotideExtensionBuilder`):
- `nucleotide-analysis.nucleotideSequence_fk` = `nucleotide-sequence.pk` (left outer) → sequence fields
- `nucleotide-analysis.molecularProtocol_fk` = `molecular-protocol.pk` (left outer) → MIxS method/protocol fields
- `materialEntity_fk` populated → resolved to occurrence via `singleMaterialOccurrenceLinks` (physical-specimen path)
- `event_fk` populated, `materialEntity_fk` absent → resolved directly to event (eDNA/metabarcoding path)

**Design decisions:**
- A row with both FKs populated is attached only via the occurrence path — never duplicated onto the
  event too.

**Gaps:**
- `nucleotide-analysis-assertion` / `molecular-protocol-assertion` — see §4.6.
- `molecular-protocol-agent-role` / `molecular-protocol-reference` — see §4.1/§4.2.
- `identification.nucleotideAnalysis_fk` / `nucleotideSequence_fk` (identification made *from* a DNA
  analysis) — untouched; that's provenance on the identification, not part of this extension.

### 4.10 Humboldt / Survey

**Joins** (`HumboldtExtensionBuilder`):
- `survey.event_fk = event.event_pk` (left outer) → Humboldt Ecological Inventory rows
- `survey-survey-target` / `survey-target` → fanned out one row per linked target

**Gaps:**
- `survey-agent-role`, `survey-assertion`, `survey-identifier`, `survey-reference` — untouched.

### 4.11 Chronometric age — not started

`chronometric-age` and its sub-tables (`chronometric-age-protocol`, `chronometric-age-agent-role`,
`chronometric-age-assertion`, `chronometric-age-media`, `chronometric-age-reference`) have no
builder at all. By the naming rule in [§2](#2-code-structure), this would be a
`ChronometricAgeExtensionBuilder` — a new DwC-A extension row type, not a flat enrichment — pending
confirmation of its DwC-A target extension.

---

## 5. Coverage matrix

At-a-glance status per DwC-DP schema table. ✅ mapped · ⚠️ partial · ❌ not started. Links point to
the relevant §4 section.

| Table | Status | Handled by | Notes |
| --- | --- | --- | --- |
| `agent` | ✅ | [§4.1](#41-agent-resolution) `AgentJoinBuilder` | 4 confirmed field pairs only |
| `agent-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) gap |
| `agent-identifier` | ❌ | — | |
| `agent-media` | ❌ | — | |
| `bibliographic-resource` | ❌ | — | |
| `chronometric-age` | ❌ | — | [§4.11](#411-chronometric-age--not-started) |
| `chronometric-age-*` (5 sub-tables) | ❌ | — | [§4.11](#411-chronometric-age--not-started) |
| `event` | ✅ | `EventCoreBuilder` | core table |
| `event-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `event-assertion` | ✅ | [§4.6](#46-emof-assertions) `AssertionExtensionBuilder` | |
| `event-identifier` | ✅ | [§4.7](#47-identifier-extension) `IdentifierExtensionBuilder` | |
| `event-media` | ✅ | [§4.5](#45-multimedia-extension) `MediaExtensionBuilder` | |
| `event-protocol` | ✅ | [§4.2](#42-protocol-resolution) `ProtocolJoinBuilder` | |
| `event-provenance` | ✅ | [§4.3](#43-provenance) `ProvenanceJoinBuilder` | 4 of 12+ fields |
| `event-reference` | ❌ | — | |
| `geological-context` | ✅ | `GeologicalContextJoinBuilder` | event-side |
| `geological-context-media` | ❌ | — | |
| `identification` | ⚠️ | [§4.8](#48-identification--identification-history) | occurrence_fk path only; identifiedByID not agent-resolved |
| `identification-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `identification-reference` | ❌ | — | |
| `identification-taxon` | ❌ | — | |
| `material` | ⚠️ | [§4.4](#44-material--occurrence-enrichment) `MaterialJoinBuilder` | evidence-linked only; virtual synthesis paused; collectionEvent_fk/derivationEvent_fk excluded |
| `material-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `material-assertion` | ✅ | [§4.6](#46-emof-assertions) | gated by material pause |
| `material-geological-context` | ✅ | [§4.4](#44-material--occurrence-enrichment) `MaterialGeologicalContextJoinBuilder` | |
| `material-identifier` | ✅ | [§4.7](#47-identifier-extension) | |
| `material-media` | ✅ | [§4.5](#45-multimedia-extension) | |
| `material-protocol` | ✅ | [§4.2](#42-protocol-resolution) `MaterialProtocolJoinBuilder` | |
| `material-provenance` | ✅ | [§4.3](#43-provenance) `MaterialProvenanceJoinBuilder` | |
| `material-reference` | ❌ | — | |
| `material-usage-policy` | ✅ | [§4.4](#44-material--occurrence-enrichment) `UsagePolicyJoinBuilder` | |
| `media` | ✅ | `UsagePolicyJoinBuilder` (license/rightsHolder) | |
| `media-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `media-assertion` | ❌ | — | |
| `media-identifier` | ❌ | — | |
| `media-provenance` | ❌ | — | [§4.3](#43-provenance) |
| `media-usage-policy` | ✅ | via `media.usagePolicy_fk` | |
| `molecular-protocol` | ✅ | [§4.9](#49-dna-derived-data-nucleotide) `NucleotideExtensionBuilder` | |
| `molecular-protocol-agent-role` | ❌ | — | [§4.1](#41-agent-resolution)/[§4.9](#49-dna-derived-data-nucleotide) |
| `molecular-protocol-assertion` | ❌ | — | [§4.6](#46-emof-assertions) |
| `molecular-protocol-reference` | ❌ | — | |
| `nucleotide-analysis` | ✅ | [§4.9](#49-dna-derived-data-nucleotide) | |
| `nucleotide-analysis-assertion` | ❌ | — | [§4.6](#46-emof-assertions) |
| `nucleotide-sequence` | ✅ | [§4.9](#49-dna-derived-data-nucleotide) | |
| `occurrence` | ✅ | `OccurrenceCoreBuilder` / `OccurrenceExtensionBuilder` | core or nested |
| `occurrence-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `occurrence-assertion` | ✅ | [§4.6](#46-emof-assertions) | |
| `occurrence-identifier` | ✅ | [§4.7](#47-identifier-extension) | |
| `occurrence-media` | ✅ | [§4.5](#45-multimedia-extension) | |
| `occurrence-protocol` | ✅ | [§4.2](#42-protocol-resolution) | |
| `occurrence-reference` | ❌ | — | |
| `organism` | ✅ | `OrganismJoinBuilder` | occurrence-side |
| `organism-assertion` | ❌ | — | |
| `organism-identifier` | ❌ | — | |
| `organism-interaction` | ❌ | — | |
| `organism-interaction-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `organism-interaction-assertion` | ❌ | — | |
| `organism-interaction-media` | ❌ | — | |
| `organism-interaction-reference` | ❌ | — | |
| `organism-reference` | ❌ | — | |
| `organism-relationship` | ❌ | — | |
| `protocol` | ✅ | [§4.2](#42-protocol-resolution) `ProtocolJoinBuilder` | |
| `protocol-reference` | ❌ | — | |
| `provenance` | ⚠️ | [§4.3](#43-provenance) | 4 of 12+ fields |
| `resource-relationship` | ❌ | — | |
| `survey` | ✅ | [§4.10](#410-humboldt--survey) `HumboldtExtensionBuilder` | |
| `survey-agent-role` | ❌ | — | [§4.1](#41-agent-resolution) |
| `survey-assertion` | ❌ | — | |
| `survey-identifier` | ❌ | — | |
| `survey-protocol` | ✅ | [§4.2](#42-protocol-resolution) | |
| `survey-reference` | ❌ | — | |
| `survey-target` | ✅ | [§4.10](#410-humboldt--survey) | |
| `usage-policy` | ✅ | `UsagePolicyJoinBuilder` | |

**Summary:** 28 of 74 tables fully mapped, 5 partial, 41 not started (mostly the `*-agent-role`,
`*-reference`, and `chronometric-age` families).

---

## 6. Schema view

Field-level detail for the tables above, for the "does field X flow anywhere?" question §5 can't
answer at table granularity. This section is populated as tables are worked on — a table only gets a
subsection once its fields have actually been checked against the code, so absence here means "not
yet audited," not "nothing to report."

*(Populated so far: agent, protocol, provenance, material — see the "Joins"/"Design decisions"
entries in [§4](#4-extension-view) for the fields those cover. Remaining tables — most notably the
`*-agent-role` family and `chronometric-age` — need their schema JSON reviewed field-by-field before
a subsection can be added here without guessing. Flag which schema source to use for this pass —
same DwC-DP profile JSON referenced in earlier sessions — and it can be filled in table by table.)*

---

## 7. Implemented lossy behaviour

| Situation | Current behaviour |
| --- | --- |
| Event-core package contains occurrence or material media | Media is promoted to the event's top-level Multimedia extension so it is interpreted and indexed. The original occurrence/material ownership is not representable in nested DwC-A extensions. |
| Multiple materials per occurrence | Material-derived fields and material-derived extensions are not flattened. |
| Multiple material geological contexts | Geological-context fields are not flattened. This also avoids adding null-only columns when no context is usable. |
| Multiple linked provenance or protocol records | Values are retained as deterministic pipe-delimited lists where the target term is list-valued. |
| Material with no evidence occurrence but a resolvable collection event | **Currently paused** (§3.5) — dropped, not synthesised. When re-enabled: represented as a virtual `MaterialSample` occurrence under that event. Materials with an evidence link, including a dangling one, are not synthesised even then, to avoid later duplication. Derivation-event links are never used, paused or not (§4.4). |

---

## Implementation entry points

Orchestration: `EventCoreBuilder`, `OccurrenceCoreBuilder`, `OccurrenceExtensionBuilder`. Domain
joins and extension construction: `org.gbif.pipelines.spark.dwcdp.builder.extension`, with focused
tests alongside each builder under `src/test/java`.
