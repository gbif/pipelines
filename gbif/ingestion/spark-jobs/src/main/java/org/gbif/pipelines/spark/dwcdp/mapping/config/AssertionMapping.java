package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

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
            protocol.field("protocolDescription"),
            assertion.field("assertionProtocol_fk")));

    return builder.build();
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
