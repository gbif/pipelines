package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder.coreFragment;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaResource;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;
import java.util.HashSet;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.ValueAggregation;

/** Reusable Occurrence-core enrichment fragments. */
public final class OccurrenceCoreMapping {

  private OccurrenceCoreMapping() {}

  /** Organism fields denormalized onto Occurrence via the schema-declared weak organismID relation. */
  public static CoreFragment organism(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath organism =
        occurrence.append(graph.resolve("occurrence", "organism", "organismID"));
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-organism", "occurrence")
            .join("organism")
            .via("organismID")
            .optional()
            .exactlyOne()
            .endJoin();
    addOrganismTargets(graph, builder, occurrence, organism);
    return builder.build();
  }

  /**
   * Exactly one accepted identification enriches Occurrence. The acceptance filter is applied before
   * exactly-one cardinality, so zero or multiple accepted rows contribute nothing.
   */
  public static CoreFragment acceptedIdentification(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath identification =
        occurrence.append(graph.resolve("occurrence", "identification", "occurrence_fk"));
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification", "occurrence")
            .join("identification")
            .via("occurrence_fk")
            .filter(FilterExpression.eq("isAcceptedIdentification", true))
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
      TargetTerms.resolveOutput(column, TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS)
          .ifPresent(
              target -> {
                boolean identificationPrecedesMaterial =
                    materialTargets.contains(target) && !occurrenceTargets.contains(target);
                builder.field(
                    identificationPrecedesMaterial
                        ? TargetFieldMapping.oneOf(
                            target, ValueAggregation.firstNonNull(), identification.field(column))
                        : TargetFieldMapping.inferredOneOf(
                            target, ValueAggregation.firstNonNull(), identification.field(column)));
              });
    }
    return builder.build();
  }

  /**
   * Exactly one material citing the occurrence as evidence enriches Occurrence. Usage-policy is on
   * the same path, so license/rightsHolder are suppressed whenever the material relationship is
   * ambiguous.
   */
  public static CoreFragment material(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath usagePolicy =
        material.append(graph.resolve("material", "usage-policy", "usagePolicy_fk"));

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material", "occurrence")
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
      TargetTerms.resolveOutput(column, TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS)
          .ifPresent(
              target ->
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), material.field(column))));
    }
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }




  /** Geological-context fields from one unambiguous context on one evidence material. */
  public static CoreFragment materialGeologicalContext(SchemaGraph graph) {
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

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-geological-context", "occurrence")
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
      if (column.endsWith("_pk") || column.endsWith("_fk") || column.equals("geologicalContextID")) {
        continue;
      }
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target ->
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), geologicalContext.field(column))));
    }
    return builder.build();
  }

  /** Material-linked protocols contribute to occurrence samplingProtocol for one evidence material. */
  public static CoreFragment materialProtocols(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath link =
        material.append(graph.resolve("material", "material-protocol", "materialEntity_fk"));
    SchemaPath protocol =
        link.append(graph.resolve("material-protocol", "protocol", "protocol_fk"));

    return coreFragment("occurrence-core-material-protocols", "occurrence")
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

  /** Direct material.provenance_fk contributions for an unambiguous evidence material. */
  public static CoreFragment materialDirectProvenance(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath provenance =
        material.append(graph.resolve("material", "provenance", "provenance_fk"));

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-direct-provenance", "occurrence")
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
  public static CoreFragment materialProvenance(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath material =
        occurrence.append(graph.resolve("occurrence", "material", "evidenceForOccurrenceID"));
    SchemaPath link =
        material.append(graph.resolve("material", "material-provenance", "materialEntity_fk"));
    SchemaPath provenance =
        link.append(graph.resolve("material-provenance", "provenance", "provenance_fk"));

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-provenance", "occurrence")
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

  private static void addProvenanceTargets(CoreFragmentBuilder builder, SchemaPath provenance) {
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


  /** Resolves recordedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment recordedBy(SchemaGraph graph) {
    return agentName(
        "occurrence-recorded-by-agent",
        "recordedByID",
        "recordedBy",
        DwcTerm.recordedBy.qualifiedName());
  }

  /** Resolves identifiedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment identifiedBy(SchemaGraph graph) {
    return agentName(
        "occurrence-identified-by-agent",
        "identifiedByID",
        "identifiedBy",
        DwcTerm.identifiedBy.qualifiedName());
  }

  /** Direct occurrenceProtocol_fk -> resolved samplingProtocol when the protocol exists. */
  public static CoreFragment directSamplingProtocol(SchemaGraph graph) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath protocol =
        occurrence.append(graph.resolve("occurrence", "protocol", "occurrenceProtocol_fk", null));

    return coreFragment("occurrence-direct-sampling-protocol", "occurrence")
        .join("protocol")
        .via("occurrenceProtocol_fk")
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

  private static CoreFragment agentName(
      String name, String idColumn, String valueColumn, String targetTerm) {
    SchemaPath occurrence = SchemaPath.root("occurrence");
    SchemaPath agent =
        occurrence.append(
            SchemaRelation.relation(
                "occurrence", idColumn, "agent", "agentID", null, RelationCardinality.UNKNOWN));

    return coreFragment(name, "occurrence")
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

  private static void addOrganismTargets(
      SchemaGraph graph,
      CoreFragmentBuilder builder,
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
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target -> {
                String occurrenceColumn = sourceColumnForTarget(graph, "occurrence", target);
                builder.field(
                    occurrenceColumn == null
                        ? TargetFieldMapping.inferredOneOf(
                            target, ValueAggregation.firstNonNull(), organism.field(column))
                        : TargetFieldMapping.oneOf(
                            target,
                            ValueAggregation.firstNonNull(),
                            occurrence.field(occurrenceColumn),
                            organism.field(column)));
              });
    }
  }

  private static String sourceColumnForTarget(
      SchemaGraph graph, String resourceName, String target) {
    SchemaResource resource =
        graph.resource(resourceName)
            .orElseThrow(
                () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    for (String column : resource.fields().keySet()) {
      if (!column.endsWith("_pk") && !column.endsWith("_fk")) {
        String resolved = TargetTerms.resolveOutput(column).orElse(null);
        if (target.equals(resolved)) {
          return column;
        }
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
        TargetTerms.resolveOutput(
                column,
                java.util.Set.of("occurrence", "identification", "material").contains(resourceName)
                    ? TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS
                    : java.util.Set.of())
            .ifPresent(targets::add);
      }
    }
    return targets;
  }

}
