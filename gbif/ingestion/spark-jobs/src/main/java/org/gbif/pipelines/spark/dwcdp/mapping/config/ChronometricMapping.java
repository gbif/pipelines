package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.Optional;
import org.gbif.dwc.terms.ChronoTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Declarative mappings for the Chronometric Age extension. */
public final class ChronometricMapping {

  public static final String ROW_TYPE_CHRONOMETRIC_AGE = ChronoTerm.ChronometricAge.qualifiedName();

  private ChronometricMapping() {}

  /** Base Chronometric Age rows owned directly by an Event. */
  public static ExtensionFragment eventAges(SchemaGraph graph) {
    SchemaPath age = SchemaPath.root("chronometric-age");
    ExtensionFragmentBuilder builder =
        extensionFragment("event-chronometric-age", ROW_TYPE_CHRONOMETRIC_AGE, "chronometric-age")
            .scopeKey("event_fk")
            .rowIdentity(age.field("chronometricAge_pk"));

    DirectFieldMappings.from(graph, "chronometric-age", age).addTo(builder);
    return builder.build();
  }


  /** Base Chronometric Age rows promoted from the Occurrence's owning Event. */
  public static ExtensionFragment agesForOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath event =
        occurrence.append(graph.resolve("occurrence", "event", "event_fk", null));
    SchemaPath age =
        event.append(graph.resolve("event", "chronometric-age", "event_fk", null));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "event-chronometric-age-for-occurrence",
                ROW_TYPE_CHRONOMETRIC_AGE,
                "occurrence")
            .scopeKey("occurrence_pk")
            .join("event")
            .via("event_fk")
            .optional()
            .exactlyOne()
            .join("chronometric-age")
            .via("event_fk")
            .optional()
            .fanOut()
            .rowIdentity(age.field("chronometricAge_pk"));

    DirectFieldMappings.from(graph, "chronometric-age", age).addTo(builder);
    return builder.build();
  }

  /** Protocol enrichment for Event-owned Chronometric rows promoted to Occurrence core. */
  public static ExtensionFragment ageProtocolForOccurrence(SchemaGraph graph) {
    return protocolForOccurrence(
        graph,
        "event-chronometric-age-protocol-for-occurrence",
        "chronometricAgeProtocol_fk",
        "chronometricAgeProtocol",
        ChronoTerm.chronometricAgeProtocol.qualifiedName());
  }

  /** Conversion-protocol enrichment for Chronometric rows promoted to Occurrence core. */
  public static ExtensionFragment conversionProtocolForOccurrence(SchemaGraph graph) {
    return protocolForOccurrence(
        graph,
        "event-chronometric-age-conversion-protocol-for-occurrence",
        "chronometricAgeConversionProtocol_fk",
        "chronometricAgeConversionProtocol",
        ChronoTerm.chronometricAgeConversionProtocol.qualifiedName());
  }

  /** Determiner-agent resolution for Chronometric rows promoted to Occurrence core. */
  public static ExtensionFragment determinedByForOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath event =
        occurrence.append(graph.resolve("occurrence", "event", "event_fk", null));
    SchemaPath age =
        event.append(graph.resolve("event", "chronometric-age", "event_fk", null));
    SchemaPath agent =
        age.append(graph.resolve("chronometric-age", "agent", "chronometricAgeDeterminedByID"));

    return extensionFragment(
            "event-chronometric-age-determined-by-for-occurrence",
            ROW_TYPE_CHRONOMETRIC_AGE,
            "occurrence")
        .scopeKey("occurrence_pk")
        .join("event")
        .via("event_fk")
        .optional()
        .exactlyOne()
        .join("chronometric-age")
        .via("event_fk")
        .optional()
        .fanOut()
        .join("agent")
        .via("chronometricAgeDeterminedByID")
        .optional()
        .fanOut()
        .rowMatch(age.field("chronometricAge_pk"))
        .field(
            TargetFieldMapping.oneOf(
                ChronoTerm.chronometricAgeDeterminedBy.qualifiedName(),
                ValueAggregation.firstNonNull(),
                age.field("chronometricAgeDeterminedBy"),
                agent.field("preferredAgentName")))
        .build();
  }

  /** Resolves chronometricAgeProtocol_fk while preserving the publisher's literal protocol value. */
  public static ExtensionFragment ageProtocol(SchemaGraph graph) {
    return protocol(
        graph,
        "event-chronometric-age-protocol",
        "chronometricAgeProtocol_fk",
        "chronometricAgeProtocol",
        ChronoTerm.chronometricAgeProtocol.qualifiedName());
  }

  /** Resolves conversion protocol FK while preserving the publisher's literal protocol value. */
  public static ExtensionFragment conversionProtocol(SchemaGraph graph) {
    return protocol(
        graph,
        "event-chronometric-age-conversion-protocol",
        "chronometricAgeConversionProtocol_fk",
        "chronometricAgeConversionProtocol",
        ChronoTerm.chronometricAgeConversionProtocol.qualifiedName());
  }

  /** Resolves chronometricAgeDeterminedByID through agent.agentID. */
  public static ExtensionFragment determinedBy(SchemaGraph graph) {
    SchemaPath age = SchemaPath.root("chronometric-age");
    return AgentMapping.extension(
        graph,
        ROW_TYPE_CHRONOMETRIC_AGE,
        new AgentMapping.Spec(
            "event-chronometric-age-determined-by",
            "chronometric-age",
            "chronometricAgeDeterminedByID",
            "chronometricAgeDeterminedBy",
            ChronoTerm.chronometricAgeDeterminedBy.qualifiedName()),
        Optional.of("event_fk"),
        Optional.of(age.field("chronometricAge_pk")));
  }

  private static ExtensionFragment protocolForOccurrence(
      SchemaGraph graph,
      String fragmentName,
      String protocolFk,
      String literalField,
      String targetTerm) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath event =
        occurrence.append(graph.resolve("occurrence", "event", "event_fk", null));
    SchemaPath age =
        event.append(graph.resolve("event", "chronometric-age", "event_fk", null));
    SchemaPath protocol =
        age.append(graph.resolve("chronometric-age", "protocol", protocolFk, null));

    return extensionFragment(fragmentName, ROW_TYPE_CHRONOMETRIC_AGE, "occurrence")
        .scopeKey("occurrence_pk")
        .join("event")
        .via("event_fk")
        .optional()
        .exactlyOne()
        .join("chronometric-age")
        .via("event_fk")
        .optional()
        .fanOut()
        .join("protocol")
        .via(protocolFk)
        .optional()
        .exactlyOne()
        .rowMatch(age.field("chronometricAge_pk"))
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.firstNonNull(),
                age.field(literalField),
                protocol.field("protocolDescription")))
        .build();
  }

  private static ExtensionFragment protocol(
      SchemaGraph graph,
      String fragmentName,
      String protocolFk,
      String literalField,
      String targetTerm) {
    SchemaPath age = SchemaPath.root("chronometric-age");
    SchemaPath protocol =
        age.append(graph.resolve("chronometric-age", "protocol", protocolFk, null));

    return extensionFragment(fragmentName, ROW_TYPE_CHRONOMETRIC_AGE, "chronometric-age")
        .scopeKey("event_fk")
        .rowMatch(age.field("chronometricAge_pk"))
        .join("protocol")
        .via(protocolFk)
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.firstNonNull(),
                age.field(literalField),
                protocol.field("protocolDescription")))
        .build();
  }
}
