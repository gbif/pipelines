package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Declarative mappings for the GBIF Identifier extension. */
public final class IdentifierMapping {

  /** Extension.IDENTIFIER.getRowType(). */
  public static final String ROW_TYPE_IDENTIFIER = "http://rs.gbif.org/terms/1.0/Identifier";

  private IdentifierMapping() {}

  /** Direct {@code event-identifier} rows attached to Event core records. */
  public static ExtensionFragment eventIdentifiers(SchemaGraph graph) {
    SchemaPath identifiers = SchemaPath.root("event-identifier");
    ExtensionFragmentBuilder builder =
        extensionFragment("event-identifiers", ROW_TYPE_IDENTIFIER, "event-identifier")
            .scopeKey("event_fk");

    DirectFieldMappings.from(graph, "event-identifier", identifiers).addTo(builder);
    return builder.build();
  }

  /** {@code survey-identifier} rows promoted to their owning Event core record. */
  public static ExtensionFragment surveyIdentifiersForEvent(SchemaGraph graph) {
    SchemaPath event = SchemaPath.root("event");
    SchemaPath survey =
        event.append(graph.resolve("event", "survey", "event_fk", null));
    SchemaPath identifiers =
        survey.append(graph.resolve("survey", "survey-identifier", "survey_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("survey-identifiers-for-event", ROW_TYPE_IDENTIFIER, "event")
            .scopeKey("event_pk")
            .join("survey")
            .via("event_fk")
            .optional()
            .fanOut()
            .join("survey-identifier")
            .via("survey_fk")
            .optional()
            .fanOut()
            .endJoin();

    DirectFieldMappings.from(graph, "survey-identifier", identifiers).addTo(builder);
    return builder.build();
  }

  /** Direct {@code occurrence-identifier} rows attached to Occurrence core records. */
  public static ExtensionFragment occurrenceIdentifiers(SchemaGraph graph) {
    SchemaPath identifiers = SchemaPath.root("occurrence-identifier");
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-identifiers", ROW_TYPE_IDENTIFIER, "occurrence-identifier")
            .scopeKey("occurrence_fk");

    DirectFieldMappings.from(graph, "occurrence-identifier", identifiers).addTo(builder);
    return builder.build();
  }

  /**
   * {@code material-identifier} rows attached to Occurrence core only when the occurrence has one
   * unambiguous evidence material.
   */
  public static ExtensionFragment materialIdentifiersForOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID", null));
    SchemaPath identifiers =
        material.append(graph.resolve("material", "material-identifier", "materialEntity_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment("material-identifiers-for-occurrence", ROW_TYPE_IDENTIFIER, "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-identifier")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .endJoin();

    DirectFieldMappings.from(graph, "material-identifier", identifiers).addTo(builder);
    return builder.build();
  }
}
