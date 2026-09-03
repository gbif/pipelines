package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Declarative mappings for the GBIF Identifier extension. */
public final class IdentifierMapping {

  /** Extension.IDENTIFIER.getRowType(). */
  public static final String ROW_TYPE_IDENTIFIER = "http://rs.gbif.org/terms/1.0/Identifier";

  private IdentifierMapping() {}

  /** Direct {@code event-identifier} rows attached to Event core records. */
  public static ExtensionFragment eventIdentifiers(SchemaGraph graph) {
    MappingPath identifiers = MappingPath.root(graph, "event-identifier");
    ExtensionFragmentBuilder builder =
        extensionFragment("event-identifiers", ROW_TYPE_IDENTIFIER, "event-identifier")
            .scopeKey("event_fk");

    DirectFieldMappings.from(graph, "event-identifier", identifiers).addTo(builder);
    return builder.build();
  }

  /** {@code survey-identifier} rows promoted to their owning Event core record. */
  public static ExtensionFragment surveyIdentifiersForEvent(SchemaGraph graph) {
    MappingPath event = MappingPath.root(graph, "event");
    MappingPath survey = event.join("survey").via("event_fk").optional().fanOut();
    MappingPath identifiers = survey.join("survey-identifier").via("survey_fk").optional().fanOut();

    ExtensionFragmentBuilder builder =
        extensionFragment("survey-identifiers-for-event", ROW_TYPE_IDENTIFIER, identifiers)
            .scopeKey("event_pk");

    DirectFieldMappings.from(graph, "survey-identifier", identifiers).addTo(builder);
    return builder.build();
  }

  /** Direct Occurrence identifiers promoted to Event core and routed to that occurrence. */
  public static ExtensionFragment occurrenceIdentifiersForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identifiers =
        occurrence.join("occurrence-identifier").via("occurrence_fk").optional().fanOut();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-identifiers-for-event", ROW_TYPE_IDENTIFIER, identifiers)
            .scopeKey("event_fk")
            .rowIdentity(identifiers.field("identifier"));

    DirectFieldMappings.from(graph, "occurrence-identifier", identifiers).addTo(builder);
    OccurrenceExtensionRouting.addOccurrenceId(builder, occurrence);
    return builder.build();
  }

  /** Material identifiers owned through a specific Event-nested Occurrence. */
  public static ExtensionFragment materialIdentifiersForEvent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath identifiers =
        material.join("material-identifier").via("materialEntity_fk").optional().fanOut();

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-identifiers-for-event-occurrence", ROW_TYPE_IDENTIFIER, identifiers)
            .scopeKey("event_fk")
            .rowIdentity(identifiers.field("identifier"));

    DirectFieldMappings.from(graph, "material-identifier", identifiers).addTo(builder);
    OccurrenceExtensionRouting.addOccurrenceId(builder, occurrence);
    return builder.build();
  }

  /** Direct {@code occurrence-identifier} rows attached to Occurrence core records. */
  public static ExtensionFragment occurrenceIdentifiers(SchemaGraph graph) {
    MappingPath identifiers = MappingPath.root(graph, "occurrence-identifier");
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
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath identifiers =
        material.join("material-identifier").via("materialEntity_fk").optional().fanOut();

    ExtensionFragmentBuilder builder =
        extensionFragment("material-identifiers-for-occurrence", ROW_TYPE_IDENTIFIER, identifiers)
            .scopeKey("occurrence_pk");

    DirectFieldMappings.from(graph, "material-identifier", identifiers).addTo(builder);
    return builder.build();
  }
}
