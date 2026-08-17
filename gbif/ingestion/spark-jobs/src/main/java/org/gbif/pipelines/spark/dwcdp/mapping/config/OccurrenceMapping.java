package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder.extensionFragment;

import java.util.HashSet;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaResource;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Reusable occurrence-row mappings shared by Occurrence core and Event -> Occurrence extension. */
public final class OccurrenceMapping {

  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  private OccurrenceMapping() {}

  /**
   * The physical occurrence row set. event_fk identifies the containing Event scope while
   * occurrence_pk keeps individual occurrences distinct inside that scope.
   */
  public static ExtensionFragment directOccurrence(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-direct", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowIdentity(occurrence.field("occurrence_pk"));
    DirectFieldMappings.from(graph, "occurrence", occurrence).addTo(builder);
    return builder.build();
  }
  /** Organism fields enrich an existing occurrence row matched by occurrence_pk. */
  public static ExtensionFragment organism(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaRelation organismRelation =
        graph.resolve("occurrence", "organism", "organismID");
    SchemaPath organism = occurrence.append(organismRelation);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-organism", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("organism")
            .via("organismID")
            .optional()
            .exactlyOne()
            .endJoin();
    addOrganismTargets(graph, builder, occurrence, organism);
    return builder.build();
  }

  /**
   * The single accepted identification enriches its existing occurrence row. Filtering happens
   * before exactly-one cardinality, so zero or multiple accepted identifications contribute no
   * enrichment, matching the legacy current-identification rule.
   */
  public static ExtensionFragment acceptedIdentification(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaRelation identificationRelation =
        graph.resolve("occurrence", "identification", "occurrence_fk");
    SchemaPath identification = occurrence.append(identificationRelation);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("identification")
            .via("occurrence_fk")
            .filter(cols -> cols.col("isAcceptedIdentification").equalTo(true))
            .optional()
            .exactlyOne()
            .endJoin();

    SchemaResource resource =
        graph.resource("identification")
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource identification"));
    Set<String> occurrenceTargets = targetTerms(graph, "occurrence");
    Set<String> materialTargets = targetTerms(graph, "material");
    for (String column : resource.fields().keySet()) {
      if (column.endsWith("_pk")
          || column.endsWith("_fk")
          || column.equals("isAcceptedIdentification")) {
        continue;
      }
      String target = TargetTerms.resolve(column);
      boolean identificationPrecedesMaterial =
          materialTargets.contains(target) && !occurrenceTargets.contains(target);
      builder.field(
          identificationPrecedesMaterial
              ? TargetFieldMapping.oneOf(
                  target, ValueAggregation.firstNonNull(), identification.field(column))
              : TargetFieldMapping.inferredOneOf(
                  target, ValueAggregation.firstNonNull(), identification.field(column)));
    }
    return builder.build();
  }

  /**
   * Exactly one evidence material enriches its existing occurrence row. The relationship is a
   * schema-declared weak FK (material.evidenceForOccurrenceID -> occurrence.occurrenceID), so the
   * reverse traversal remains schema-backed via(...). Usage policy is traversed on the same path so
   * its fields are gated by the same exactly-one-material decision.
   */
  public static ExtensionFragment material(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaRelation materialRelation =
        graph.resolve("occurrence", "material", "evidenceForOccurrenceID");
    SchemaPath material = occurrence.append(materialRelation);
    SchemaRelation usagePolicyRelation =
        graph.resolve("material", "usage-policy", "usagePolicy_fk");
    SchemaPath usagePolicy = material.append(usagePolicyRelation);

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("usage-policy")
            .via("usagePolicy_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    SchemaResource materialResource =
        graph.resource("material")
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource material"));
    for (String column : materialResource.fields().keySet()) {
      if (column.endsWith("_pk")
          || column.endsWith("_fk")
          || column.equals("evidenceForOccurrenceID")) {
        continue;
      }
      builder.field(
          TargetFieldMapping.inferredOneOf(
              TargetTerms.resolve(column),
              ValueAggregation.firstNonNull(),
              material.field(column)));
    }
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }


  /** Direct material.provenance_fk contributions for an unambiguous evidence material. */
  public static ExtensionFragment materialDirectProvenance(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath provenance =
        material.append(graph.resolve("material", "provenance", "provenance_fk"));

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-direct-provenance", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("provenance")
            .via("provenance_fk")
            .optional()
            .exactlyOne()
            .endJoin();
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** material -> material-provenance -> provenance contributions for one evidence material. */
  public static ExtensionFragment materialProvenance(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath link =
        material.append(graph.resolve("material", "material-provenance", "materialEntity_fk"));
    SchemaPath provenance =
        link.append(graph.resolve("material-provenance", "provenance", "provenance_fk"));

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-provenance", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-provenance")
            .via("materialEntity_fk")
            .optional()
            .fanOut()
            .join("provenance")
            .via("provenance_fk")
            .optional()
            .exactlyOne()
            .endJoin();
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** Resolves recordedByID through agent.agentID for an Event-nested Occurrence row. */
  public static ExtensionFragment recordedBy(SchemaGraph graph) {
    return agentName(
        "occurrence-recorded-by-agent",
        "recordedByID",
        "recordedBy",
        DwcTerm.recordedBy.qualifiedName());
  }

  /** Resolves identifiedByID through agent.agentID for an Event-nested Occurrence row. */
  public static ExtensionFragment identifiedBy(SchemaGraph graph) {
    return agentName(
        "occurrence-identified-by-agent",
        "identifiedByID",
        "identifiedBy",
        DwcTerm.identifiedBy.qualifiedName());
  }

  /** Geological-context fields from one context on one unambiguous evidence material. */
  public static ExtensionFragment materialGeologicalContext(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath link =
        material.append(
            graph.resolve("material", "material-geological-context", "materialEntity_fk"));
    SchemaPath geologicalContext =
        link.append(
            graph.resolve(
                "material-geological-context", "geological-context", "geologicalContext_fk"));

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-material-geological-context", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"))
            .join("material")
            .via("evidenceForOccurrenceID")
            .optional()
            .exactlyOne()
            .join("material-geological-context")
            .via("materialEntity_fk")
            .optional()
            .exactlyOne()
            .join("geological-context")
            .via("geologicalContext_fk")
            .optional()
            .exactlyOne()
            .endJoin();

    SchemaResource resource =
        graph.resource("geological-context")
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "DwC-DP schema has no resource geological-context"));
    for (String column : resource.fields().keySet()) {
      if (column.endsWith("_pk")
          || column.endsWith("_fk")
          || column.equals("geologicalContextID")) {
        continue;
      }
      builder.field(
          TargetFieldMapping.inferredOneOf(
              TargetTerms.resolve(column),
              ValueAggregation.firstNonNull(),
              geologicalContext.field(column)));
    }
    return builder.build();
  }

  /** Material-linked protocols contribute to samplingProtocol on an Event-nested Occurrence row. */
  public static ExtensionFragment materialProtocols(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath link =
        material.append(graph.resolve("material", "material-protocol", "materialEntity_fk"));
    SchemaPath protocol =
        link.append(graph.resolve("material-protocol", "protocol", "protocol_fk"));

    return extensionFragment("occurrence-material-protocols", ROW_TYPE_OCCURRENCE, "occurrence")
        .scopeKey("event_fk")
        .rowMatch(occurrence.field("occurrence_pk"))
        .join("material")
        .via("evidenceForOccurrenceID")
        .optional()
        .exactlyOne()
        .join("material-protocol")
        .via("materialEntity_fk")
        .optional()
        .fanOut()
        .join("protocol")
        .via("protocol_fk")
        .optional()
        .exactlyOne()
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.samplingProtocol.qualifiedName(),
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  private static ExtensionFragment agentName(
      String name, String idColumn, String valueColumn, String targetTerm) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath agent =
        occurrence.append(
            SchemaRelation.relation(
                "occurrence", idColumn, "agent", "agentID", null, RelationCardinality.UNKNOWN));

    return extensionFragment(name, ROW_TYPE_OCCURRENCE, "occurrence")
        .scopeKey("event_fk")
        .rowMatch(occurrence.field("occurrence_pk"))
        .join("agent")
        .on(idColumn, "agentID")
        .optional()
        .fanOut()
        .field(
            TargetFieldMapping.oneOf(
                targetTerm,
                ValueAggregation.firstNonNull(),
                occurrence.field(valueColumn),
                agent.field("preferredAgentName")))
        .build();
  }

  private static void addProvenanceTargets(
      ExtensionFragmentBuilder builder, SchemaPath provenance) {
    for (String field :
        java.util.List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle")) {
      builder.field(
          TargetFieldMapping.oneOf(
                  TargetTerms.resolve(field),
                  ValueAggregation.firstNonNull(),
                  provenance.field(field))
              .contributionIdentity(provenance.field("provenance_pk"))
              .orderBy(provenance.field("provenanceID")));
    }
  }

  private static void addOrganismTargets(
      SchemaGraph graph,
      ExtensionFragmentBuilder builder,
      SchemaPath occurrence,
      SchemaPath organism) {
    SchemaResource resource =
        graph.resource("organism")
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource organism"));
    for (String column : resource.fields().keySet()) {
      if (column.endsWith("_pk") || column.endsWith("_fk")) {
        continue;
      }
      String target = TargetTerms.resolve(column);
      String occurrenceColumn = sourceColumnForTarget(graph, "occurrence", target);
      builder.field(
          occurrenceColumn == null
              ? TargetFieldMapping.inferredOneOf(
                  target, ValueAggregation.firstNonNull(), organism.field(column))
              : TargetFieldMapping.oneOf(
                  target,
                  ValueAggregation.presentOrFallback(),
                  occurrence.field(occurrenceColumn),
                  organism.field(column)));
    }
  }

  private static String sourceColumnForTarget(
      SchemaGraph graph, String resourceName, String target) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    for (String column : resource.fields().keySet()) {
      if (!column.endsWith("_pk")
          && !column.endsWith("_fk")
          && TargetTerms.resolve(column).equals(target)) {
        return column;
      }
    }
    return null;
  }

  private static Set<String> targetTerms(SchemaGraph graph, String resourceName) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    Set<String> targets = new HashSet<>();
    for (String column : resource.fields().keySet()) {
      if (!column.endsWith("_pk") && !column.endsWith("_fk")) {
        targets.add(TargetTerms.resolve(column));
      }
    }
    return targets;
  }

}
