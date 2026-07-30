# DwC-DP to DwC-A mapping

This document describes the DwC-DP transformations currently implemented by the Spark ingestion
builders. It is an as-is overview, not a complete DwC-DP schema crosswalk. Source columns that
already use a recognised DwC or extension term are passed through unchanged unless a row below
describes a join, rename, or special rule.

## Overview

```text
DwC-DP package
│
├── event ─────────────────────────────────────────────────────────────► DwC Event core
│   ├── direct event fields                                              │
│   ├── parent event, geological context, protocols, provenance ────────┘
│   ├── event-media + occurrence-media + material-media ───────────────► Multimedia
│   ├── event-assertion ───────────────────────────────────────────────► eMoF
│   ├── event-identifier ──────────────────────────────────────────────► Identifier
│   ├── occurrence ────────────────────────────────────────────────────► Occurrence extension
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
extension.

## Event core and event extensions

| DwC-DP source | DwC-A destination | Transformation |
| --- | --- | --- |
| `event` direct fields | DwC Event core terms | Recognised DwC terms pass through. `eventConductedBy` and `eventConductedByID` are renamed to `recordedBy` and `recordedByID`. |
| `event.parentEvent_fk → event.event_pk` | `dwc:parentEventID` | Self-join resolves the parent surrogate key to its natural `eventID`; internal keys are removed. |
| `event.geologicalContextID → geological-context` | DwC geological-context terms | Natural-key join. The event's own value wins if a column overlaps. |
| `event.eventProtocol_fk → protocol` | `dwc:samplingProtocol` | Uses `protocolType: protocolName` when named, otherwise `protocolDescription`. |
| `event-protocol → protocol` | `dwc:samplingProtocol` | Linked protocol display labels are distinct, deterministically ordered, and merged with the direct protocol value. |
| `event.georeferenceProtocol_fk → protocol` | `dwc:georeferenceProtocol` | Uses `protocolType: protocolName` when named, otherwise `protocolDescription`, only when the event's text field is null. |
| `event.provenance_fk` and `event-provenance → provenance` | `fundingAttribution`, `fundingAttributionID`, `projectID`, `projectTitle` | Direct and junction links are unioned, deduplicated, sorted by `provenanceID`, then pipe-delimited. |
| `event-media → media` | Simple Multimedia extension | Rows are attached to the event's natural `eventID`; `usage-policy` enriches media with `license` and `rightsHolder` when present. |
| `event-assertion` | eMoF extension | Assertion fields form eMoF rows; linked assertion protocols are resolved before serialisation. |
| `event-identifier` | Identifier extension | One identifier extension row per source row, attached to `eventID`. |
| `occurrence` | DwC Occurrence extension | Occurrences are resolved from `event_fk` to `eventID` and grouped below the event. The occurrence enrichments described below are applied first. |
| `survey`, `survey-survey-target`, `survey-target` | Humboldt Event extension | Survey rows attach to their event. A linked survey target fans out into one Humboldt row per target. |
| `survey.samplingProtocol[_fk]` | `eco:protocolDescriptions` | Supplied text wins; linked `protocolType: protocolName` is a fallback, then `protocolDescription`. |
| `survey.samplingEffortProtocol[_fk]` | `eco:samplingEffortProtocol` | Supplied text wins; linked `protocolType: protocolName` is a fallback, then `protocolDescription`. |

## Occurrence core and occurrence extensions

| DwC-DP source | DwC-A destination | Transformation |
| --- | --- | --- |
| `occurrence` direct fields | DwC Occurrence core terms | Recognised DwC terms pass through. `occurrenceReferences` is renamed to `dwc:associatedReferences`. |
| `organism` | Occurrence/Organism terms | Joined using the occurrence's organism relationship; internal keys are removed. |
| Accepted `identification` | Taxonomic occurrence terms | Flattened only when exactly one accepted identification is linked to the occurrence. |
| All `identification` rows | Identification History extension | Every linked identification becomes an extension row, independently of accepted-identification flattening. |
| `occurrenceProtocol_fk → protocol` | `dwc:samplingProtocol` | Uses `protocolType: protocolName` when named, otherwise `protocolDescription`. |
| `occurrence-media → media` | Simple Multimedia extension | Rows attach to `occurrenceID`; `usage-policy` supplies `license` and `rightsHolder` when available. |
| `occurrence-assertion` | eMoF extension | Assertion rows attach to `occurrenceID`; assertion protocol FKs are resolved. |
| `occurrence-identifier` | Identifier extension | Rows attach to `occurrenceID`. |

## Material-derived occurrence enrichment

Material is normalised in DwC-DP but DwC-A represents its useful fields on the occurrence. All
material-derived mappings therefore require exactly one material evidence row for the occurrence.
If there are zero or multiple material rows, material-derived fields and extensions are omitted for
that occurrence rather than choosing arbitrarily.

| DwC-DP source | DwC-A destination | Transformation |
| --- | --- | --- |
| `material` | DwC material/collection terms on the occurrence | Material fields such as institution, collection, catalogue, preparation, and type-status terms are flattened through the single-material rule. |
| `material.usagePolicy_fk → usage-policy` | `dcterms:license`, `dwc:rightsHolder` | Usage-policy fields enrich the material before it is flattened. |
| `material.provenance_fk` and `material-provenance → provenance` | `fundingAttribution`, `fundingAttributionID`, `projectID`, `projectTitle` | Direct and junction links are unioned, deduplicated, sorted by `provenanceID`, then pipe-delimited. |
| `material-protocol → protocol` | `dwc:samplingProtocol` | Linked protocol display labels are aggregated and merged with an occurrence protocol value. |
| `material-geological-context → geological-context` | DwC geological-context terms on the occurrence | Flattened only when the already-unambiguous material has exactly one linked geological context. Existing occurrence values win. |
| `material-media → media` | Simple Multimedia extension | Included with the occurrence's media when occurrence is core. With event core, it is promoted to the event-level Multimedia extension. |
| `material-assertion` | eMoF extension | Included with occurrence assertion rows using the single-material rule. |
| `material-identifier` | Identifier extension | Included with occurrence identifier rows using the single-material rule. |

## Implemented lossy behaviour

| Situation | Current behaviour |
| --- | --- |
| Event-core package contains occurrence or material media | Media is promoted to the event's top-level Multimedia extension so it is interpreted and indexed. The original occurrence/material ownership is not representable in nested DwC-A extensions. |
| Multiple materials per occurrence | Material-derived fields and material-derived extensions are not flattened. |
| Multiple material geological contexts | Geological-context fields are not flattened. This also avoids adding null-only columns when no context is usable. |
| Multiple linked provenance or protocol records | Values are retained as deterministic pipe-delimited lists where the target term is list-valued. |

## Implementation entry points

The orchestration is in `EventCoreBuilder`, `OccurrenceCoreBuilder`, and
`OccurrenceExtensionBuilder`. Domain joins and extension construction live in the
`org.gbif.pipelines.spark.dwcdp.builder.extension` package, with focused tests alongside each
builder under `src/test/java`.
