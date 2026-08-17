package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/** Declarative mappings for the Extended Measurement or Fact (eMoF) extension. */
public final class AssertionMapping {

  public static final String ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT =
      "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact";

  private AssertionMapping() {}

  /** Direct event assertions, optionally enriched with their referenced protocol description. */
  public static ExtensionFragment eventAssertions(SchemaGraph graph) {
    SchemaPath assertion = SchemaPath.root("event-assertion");
    SchemaPath protocol =
        assertion.append(
            graph.resolve("event-assertion", "protocol", "assertionProtocol_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-assertions", ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT, "event-assertion")
            .scopeKey("event_fk")
            .rowIdentity(assertion.field("assertionID"))
            .join("protocol")
            .via("assertionProtocol_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    addAssertionFields(builder, assertion, protocol);
    return builder.build();
  }

  /** Direct occurrence assertions attached to Occurrence core. */
  public static ExtensionFragment occurrenceAssertions(SchemaGraph graph) {
    SchemaPath assertion = SchemaPath.root("occurrence-assertion");
    SchemaPath protocol =
        assertion.append(
            graph.resolve("occurrence-assertion", "protocol", "assertionProtocol_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-assertions", ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT, "occurrence-assertion")
            .scopeKey("occurrence_fk")
            .rowIdentity(assertion.field("assertionID"))
            .join("protocol")
            .via("assertionProtocol_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    addAssertionFields(builder, assertion, protocol);
    return builder.build();
  }

  /**
   * Material assertions attached to Occurrence core only when the occurrence has exactly one
   * evidence material.
   */
  public static ExtensionFragment materialAssertionsForOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID", null));
    SchemaPath assertion =
        material.append(
            graph.resolve("material", "material-assertion", "materialEntity_fk", null));
    SchemaPath protocol =
        assertion.append(
            graph.resolve("material-assertion", "protocol", "assertionProtocol_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "material-assertions-for-occurrence",
                ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT,
                "occurrence")
            .scopeKey("occurrence_pk")
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-assertion")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("protocol")
            .via("assertionProtocol_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    addAssertionFields(builder, assertion, protocol);
    return builder.build();
  }

  private static void addAssertionFields(
      ExtensionFragmentBuilder builder, SchemaPath assertion, SchemaPath protocol) {
    explicit(builder, assertion, "assertionID", "measurementID");
    explicit(builder, assertion, "assertionType", "measurementType");
    explicit(builder, assertion, "assertionTypeIRI", "measurementTypeID");
    explicit(builder, assertion, "assertionValue", "measurementValue");
    explicit(builder, assertion, "assertionValueIRI", "measurementValueID");
    explicit(builder, assertion, "assertionUnit", "measurementUnit");
    explicit(builder, assertion, "assertionUnitIRI", "measurementUnitID");
    explicit(builder, assertion, "assertionError", "measurementAccuracy");
    explicit(builder, assertion, "assertionBy", "measurementDeterminedBy");
    explicit(builder, assertion, "assertionMadeDate", "measurementDeterminedDate");
    explicit(builder, assertion, "assertionRemarks", "measurementRemarks");

    builder.field(
        TargetFieldMapping.oneOf(
            TargetTerms.resolve("measurementMethod"),
            ValueAggregation.firstNonNull(),
            protocol.field("protocolDescription")));
  }

  private static void explicit(
      ExtensionFragmentBuilder builder, SchemaPath source, String sourceField, String targetField) {
    builder.field(
        TargetFieldMapping.oneOf(
            TargetTerms.resolve(targetField),
            ValueAggregation.firstNonNull(),
            source.field(sourceField)));
  }
}
