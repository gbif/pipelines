package org.gbif.pipelines.spark.dwcdp.mapping.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;

/** Shared domain semantics for Occurrence enrichment, independent of core/extension attachment. */
final class OccurrenceEnrichment {

  private OccurrenceEnrichment() {}

  static MappingPath organismPath(MappingPath occurrence) {
    return occurrence.join("organism").via("organismID").optional().exactlyOne();
  }

  static MappingPath acceptedIdentificationTaxonPath(MappingPath identification) {
    return identification
        .join("identification-taxon")
        .via("identification_fk")
        .optional()
        .exactlyOne();
  }

  static MappingPath acceptedIdentificationAgentRolePath(MappingPath identification) {
    return identification
        .join("identification-agent-role")
        .via("identification_fk")
        .optional()
        .fanOut();
  }

  static MappingPath roleAgentPath(MappingPath role) {
    return role.join("agent").via("agent_fk").optional().exactlyOne();
  }

  static MappingPath identifiedByAgentPath(MappingPath identification) {
    return identification.join("agent").via("identifiedByID").optional().fanOut();
  }

  static MappingPath usagePolicyPath(MappingPath material) {
    return material.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();
  }

  static MappingPath geologicalContextPath(MappingPath material) {
    MappingPath link =
        material
            .join("material-geological-context")
            .via("materialEntity_fk")
            .optional()
            .exactlyOne();
    return link.join("geological-context").via("geologicalContext_fk").optional().exactlyOne();
  }

  static MappingPath materialProtocolPath(MappingPath material) {
    MappingPath link =
        material.join("material-protocol").via("materialEntity_fk").optional().fanOut();
    return link.join("protocol").via("protocol_fk").optional().exactlyOne();
  }

  static MappingPath directProvenancePath(MappingPath material) {
    return material.join("provenance").via("provenance_fk").optional().exactlyOne();
  }

  static MappingPath linkedProvenancePath(MappingPath material) {
    MappingPath link =
        material.join("material-provenance").via("materialEntity_fk").optional().fanOut();
    return link.join("provenance").via("provenance_fk").optional().exactlyOne();
  }

  static MappingPath acceptedIdentificationPath(MappingPath occurrence) {
    return occurrence
        .join("identification")
        .via("occurrence_fk")
        .filter(FilterExpression.eq("isAcceptedIdentification", true))
        .optional()
        .exactlyOne();
  }

  static MappingPath evidenceMaterialPath(MappingPath occurrence) {
    return occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
  }

  static List<TargetFieldMapping> organismTargets(
      SchemaGraph graph, MappingPath occurrence, MappingPath organism) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    SchemaResource resource = resource(graph, "organism");
    for (String column : resource.fields().keySet()) {
      if (structural(column)) {
        continue;
      }
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target -> {
                String occurrenceColumn = sourceColumnForTarget(graph, "occurrence", target);
                targets.add(
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
    return List.copyOf(targets);
  }

  static List<TargetFieldMapping> acceptedIdentificationTargets(
      SchemaGraph graph, MappingPath identification) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    SchemaResource resource = resource(graph, "identification");
    Set<String> occurrenceTargets = targetTerms(graph, "occurrence");
    Set<String> materialTargets = targetTerms(graph, "material");
    for (String column : resource.fields().keySet()) {
      if (structural(column) || column.equals("isAcceptedIdentification")) {
        continue;
      }
      TargetTerms.resolveOutput(column, TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS)
          .ifPresent(
              target -> {
                boolean identificationPrecedesMaterial =
                    materialTargets.contains(target) && !occurrenceTargets.contains(target);
                targets.add(
                    identificationPrecedesMaterial
                        ? TargetFieldMapping.oneOf(
                            target, ValueAggregation.firstNonNull(), identification.field(column))
                        : TargetFieldMapping.inferredOneOf(
                            target, ValueAggregation.firstNonNull(), identification.field(column)));
              });
    }
    return List.copyOf(targets);
  }

  static List<TargetFieldMapping> acceptedIdentificationTaxonTargets(MappingPath taxon) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    for (String column :
        List.of(
            "scientificName",
            "scientificNameID",
            "scientificNameAuthorship",
            "vernacularName",
            "taxonRank")) {
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target ->
                  targets.add(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), taxon.field(column))));
    }
    return List.copyOf(targets);
  }

  static List<TargetFieldMapping> acceptedIdentificationAgentRoleTargets(
      MappingPath role, MappingPath agent) {
    return List.of(
        TargetFieldMapping.allOf(
                DwcTerm.identifiedBy.qualifiedName(),
                ValueAggregation.pipeDelimitedDistinct(),
                agent.field("preferredAgentName"))
            .contributionIdentity(role.field("agent_fk"))
            .orderBy(role.field("agentRoleOrder")),
        TargetFieldMapping.allOf(
                DwcTerm.identifiedByID.qualifiedName(),
                ValueAggregation.pipeDelimitedDistinct(),
                agent.field("agentID"))
            .contributionIdentity(role.field("agent_fk"))
            .orderBy(role.field("agentRoleOrder")));
  }

  static List<TargetFieldMapping> acceptedIdentificationAgentTargets(
      MappingPath identification, MappingPath agent) {
    return List.of(
        TargetFieldMapping.oneOf(
            DwcTerm.identifiedBy.qualifiedName(),
            ValueAggregation.firstNonNull(),
            identification.field("identifiedBy"),
            agent.field("preferredAgentName")),
        TargetFieldMapping.oneOf(
            DwcTerm.identifiedByID.qualifiedName(),
            ValueAggregation.firstNonNull(),
            identification.field("identifiedByID"),
            agent.field("agentID")));
  }

  static List<TargetFieldMapping> materialTargets(SchemaGraph graph, MappingPath material) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    SchemaResource resource = resource(graph, "material");
    for (String column : resource.fields().keySet()) {
      if (structural(column) || column.equals("evidenceForOccurrenceID")) {
        continue;
      }
      TargetTerms.resolveOutput(column, TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS)
          .ifPresent(
              target ->
                  targets.add(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), material.field(column))));
    }
    return List.copyOf(targets);
  }

  static List<TargetFieldMapping> geologicalContextTargets(
      SchemaGraph graph, MappingPath geologicalContext) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    SchemaResource resource = resource(graph, "geological-context");
    for (String column : resource.fields().keySet()) {
      if (structural(column) || column.equals("geologicalContextID")) {
        continue;
      }
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target ->
                  targets.add(
                      TargetFieldMapping.inferredOneOf(
                          target,
                          ValueAggregation.firstNonNull(),
                          geologicalContext.field(column))));
    }
    return List.copyOf(targets);
  }

  static List<TargetFieldMapping> materialProtocolTargets(MappingPath protocol) {
    return List.of(
        TargetFieldMapping.oneOf(
            DwcTerm.samplingProtocol.qualifiedName(),
            ValueAggregation.labeledOrFallback(": "),
            protocol.field("protocolType"),
            protocol.field("protocolName"),
            protocol.field("protocolDescription")));
  }

  static List<TargetFieldMapping> provenanceTargets(MappingPath provenance) {
    List<TargetFieldMapping> targets = new ArrayList<>();
    for (String field :
        List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle")) {
      targets.add(
          TargetFieldMapping.oneOf(
                  TargetTerms.resolve(field),
                  ValueAggregation.firstNonNull(),
                  provenance.field(field))
              .contributionIdentity(provenance.field("provenance_pk"))
              .orderBy(provenance.field("provenanceID")));
    }
    return List.copyOf(targets);
  }

  private static String sourceColumnForTarget(
      SchemaGraph graph, String resourceName, String target) {
    SchemaResource resource = resource(graph, resourceName);
    for (String column : resource.fields().keySet()) {
      if (!structural(column)) {
        String resolved = TargetTerms.resolveOutput(column).orElse(null);
        if (target.equals(resolved)) {
          return column;
        }
      }
    }
    return null;
  }

  private static Set<String> targetTerms(SchemaGraph graph, String resourceName) {
    SchemaResource resource = resource(graph, resourceName);
    Set<String> targets = new HashSet<>();
    for (String column : resource.fields().keySet()) {
      if (!structural(column)) {
        TargetTerms.resolveOutput(
                column,
                Set.of("occurrence", "identification", "material").contains(resourceName)
                    ? TargetTerms.OCCURRENCE_ENRICHMENT_RAW_OUTPUTS
                    : Set.of())
            .ifPresent(targets::add);
      }
    }
    return targets;
  }

  private static SchemaResource resource(SchemaGraph graph, String resourceName) {
    return graph
        .resource(resourceName)
        .orElseThrow(
            () -> new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
  }

  private static boolean structural(String column) {
    return column.endsWith("_pk") || column.endsWith("_fk");
  }
}
