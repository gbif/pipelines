package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;

/** Reusable occurrence-row mappings shared by Occurrence core and Event -> Occurrence extension. */
public final class OccurrenceMapping {

  public static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();

  private OccurrenceMapping() {}

  /**
   * The physical occurrence row set. event_fk identifies the containing Event scope while
   * occurrence_pk keeps individual occurrences distinct inside that scope.
   */
  public static ExtensionFragment directOccurrence(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-direct", ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowIdentity(occurrence.field("occurrence_pk"));
    DirectFieldMappings.from(graph, "occurrence", occurrence).addTo(builder);
    return builder.build();
  }
  /** Organism fields enrich an existing occurrence row matched by occurrence_pk. */
  public static ExtensionFragment organism(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath organism = occurrence.join("organism").via("organismID").optional().exactlyOne();
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-organism", ROW_TYPE_OCCURRENCE, organism)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    addOrganismTargets(graph, builder, occurrence, organism);
    return builder.build();
  }

  /**
   * The single accepted identification enriches its existing occurrence row. Filtering happens
   * before exactly-one cardinality, so zero or multiple accepted identifications contribute no
   * enrichment, matching the legacy current-identification rule.
   */
  public static ExtensionFragment acceptedIdentification(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification", ROW_TYPE_OCCURRENCE, identification)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));

    SchemaResource resource =
        graph
            .resource("identification")
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
   * Uses one unambiguous IdentificationTaxon row as a taxonomic fallback for the accepted
   * Identification. Multiple taxon-formula components are deliberately not flattened into one
   * Occurrence value.
   */
  public static ExtensionFragment acceptedIdentificationTaxon(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath taxon =
        identification
            .join("identification-taxon")
            .via("identification_fk")
            .optional()
            .exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification-taxon", ROW_TYPE_OCCURRENCE, taxon)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));

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
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target, ValueAggregation.firstNonNull(), taxon.field(column))));
    }
    return builder.build();
  }

  /** All agents attached to the accepted Identification are a final identifiedBy fallback. */
  public static ExtensionFragment acceptedIdentificationAgentRoles(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath role =
        identification
            .join("identification-agent-role")
            .via("identification_fk")
            .optional()
            .fanOut();
    MappingPath agent = role.join("agent").via("agent_fk").optional().exactlyOne();

    return extensionFragment(
            "occurrence-accepted-identification-agent-roles", ROW_TYPE_OCCURRENCE, agent)
        .scopeKey("event_fk")
        .rowMatch(occurrence.field("occurrence_pk"))
        .field(
            TargetFieldMapping.allOf(
                    DwcTerm.identifiedBy.qualifiedName(),
                    ValueAggregation.pipeDelimitedDistinct(),
                    agent.field("preferredAgentName"))
                .contributionIdentity(role.field("agent_fk"))
                .orderBy(role.field("agentRoleOrder")))
        .field(
            TargetFieldMapping.allOf(
                    DwcTerm.identifiedByID.qualifiedName(),
                    ValueAggregation.pipeDelimitedDistinct(),
                    agent.field("agentID"))
                .contributionIdentity(role.field("agent_fk"))
                .orderBy(role.field("agentRoleOrder")))
        .build();
  }

  /** Resolves the sole accepted identification's identifiedByID before material fallback. */
  public static ExtensionFragment acceptedIdentificationAgent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath agent = identification.join("agent").via("identifiedByID").optional().fanOut();

    return extensionFragment("occurrence-accepted-identification-agent", ROW_TYPE_OCCURRENCE, agent)
        .scopeKey("event_fk")
        .rowMatch(occurrence.field("occurrence_pk"))
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.identifiedBy.qualifiedName(),
                ValueAggregation.firstNonNull(),
                identification.field("identifiedBy"),
                agent.field("preferredAgentName")))
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.identifiedByID.qualifiedName(),
                ValueAggregation.firstNonNull(),
                identification.field("identifiedByID"),
                agent.field("agentID")))
        .build();
  }

  private static MappingPath acceptedIdentificationPath(MappingPath occurrence) {
    return occurrence
        .join("identification")
        .via("occurrence_fk")
        .filter(FilterExpression.eq("isAcceptedIdentification", true))
        .optional()
        .exactlyOne();
  }

  /**
   * Exactly one evidence material enriches its existing occurrence row. The relationship is a
   * schema-declared weak FK (material.evidenceForOccurrenceID -> occurrence.occurrenceID), so the
   * reverse traversal remains schema-backed via(...). Usage policy is traversed on the same path so
   * its fields are gated by the same exactly-one-material decision.
   */
  public static ExtensionFragment material(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath usagePolicy =
        material.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material", ROW_TYPE_OCCURRENCE, usagePolicy)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));

    SchemaResource materialResource =
        graph
            .resource("material")
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

  /** Direct material.provenance_fk contributions for an unambiguous evidence material. */
  public static ExtensionFragment materialDirectProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath provenance =
        material.join("provenance").via("provenance_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-direct-provenance", ROW_TYPE_OCCURRENCE, provenance)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** material -> material-provenance -> provenance contributions for one evidence material. */
  public static ExtensionFragment materialProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link =
        material.join("material-provenance").via("materialEntity_fk").optional().fanOut();
    MappingPath provenance = link.join("provenance").via("provenance_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-provenance", ROW_TYPE_OCCURRENCE, provenance)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** Resolves recordedByID through agent.agentID for an Event-nested Occurrence row. */
  public static ExtensionFragment recordedBy(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    return AgentMapping.extension(
        graph,
        ROW_TYPE_OCCURRENCE,
        new AgentMapping.Spec(
            "occurrence-recorded-by-agent",
            "occurrence",
            "recordedByID",
            "recordedBy",
            DwcTerm.recordedBy.qualifiedName()),
        Optional.of("event_fk"),
        Optional.of(occurrence.field("occurrence_pk")));
  }

  /** Resolves identifiedByID through agent.agentID for an Event-nested Occurrence row. */
  public static ExtensionFragment identifiedBy(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    return AgentMapping.extension(
        graph,
        ROW_TYPE_OCCURRENCE,
        new AgentMapping.Spec(
            "occurrence-identified-by-agent",
            "occurrence",
            "identifiedByID",
            "identifiedBy",
            DwcTerm.identifiedBy.qualifiedName()),
        Optional.of("event_fk"),
        Optional.of(occurrence.field("occurrence_pk")));
  }

  /** Resolves material.collectedByID for one unambiguous evidence material. */
  public static ExtensionFragment materialCollectedBy(SchemaGraph graph) {
    return materialAgent(
        graph,
        "occurrence-material-collected-by-agent",
        "collectedByID",
        "collectedBy",
        DwcTerm.recordedBy.qualifiedName());
  }

  /** Resolves material.identifiedByID for one unambiguous evidence material. */
  public static ExtensionFragment materialIdentifiedBy(SchemaGraph graph) {
    return materialAgent(
        graph,
        "occurrence-material-identified-by-agent",
        "identifiedByID",
        "identifiedBy",
        DwcTerm.identifiedBy.qualifiedName());
  }

  /** Ordered collector AgentRoles are the final recordedBy fallback for one evidence material. */
  public static ExtensionFragment materialCollectorRoles(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    return AgentRoleMapping.linkedExtension(
        graph,
        ROW_TYPE_OCCURRENCE,
        AgentRoleMapping.LinkedSpec.orderedDistinctNames(
            "occurrence-material-collector-roles",
            "occurrence",
            "material",
            "evidenceForOccurrenceID",
            "material-agent-role",
            "materialEntity_fk",
            "collector",
            DwcTerm.recordedBy.qualifiedName()),
        Optional.of("event_fk"),
        Optional.of(occurrence.field("occurrence_pk")));
  }

  private static ExtensionFragment materialAgent(
      SchemaGraph graph,
      String fragmentName,
      String idColumn,
      String valueColumn,
      String targetTerm) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    return AgentMapping.linkedExtension(
        graph,
        ROW_TYPE_OCCURRENCE,
        new AgentMapping.LinkedSpec(
            fragmentName,
            "occurrence",
            "material",
            "evidenceForOccurrenceID",
            idColumn,
            valueColumn,
            targetTerm),
        Optional.of("event_fk"),
        Optional.of(occurrence.field("occurrence_pk")));
  }

  /** Geological-context fields from one context on one unambiguous evidence material. */
  public static ExtensionFragment materialGeologicalContext(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link =
        material
            .join("material-geological-context")
            .via("materialEntity_fk")
            .optional()
            .exactlyOne();
    MappingPath geologicalContext =
        link.join("geological-context").via("geologicalContext_fk").optional().exactlyOne();

    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-material-geological-context", ROW_TYPE_OCCURRENCE, geologicalContext)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));

    SchemaResource resource =
        graph
            .resource("geological-context")
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
      TargetTerms.resolveOutput(column)
          .ifPresent(
              target ->
                  builder.field(
                      TargetFieldMapping.inferredOneOf(
                          target,
                          ValueAggregation.firstNonNull(),
                          geologicalContext.field(column))));
    }
    return builder.build();
  }

  /** Material-linked protocols contribute to samplingProtocol on an Event-nested Occurrence row. */
  public static ExtensionFragment materialProtocols(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link =
        material.join("material-protocol").via("materialEntity_fk").optional().fanOut();
    MappingPath protocol = link.join("protocol").via("protocol_fk").optional().exactlyOne();

    return extensionFragment("occurrence-material-protocols", ROW_TYPE_OCCURRENCE, protocol)
        .scopeKey("event_fk")
        .rowMatch(occurrence.field("occurrence_pk"))
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.samplingProtocol.qualifiedName(),
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  private static void addProvenanceTargets(
      ExtensionFragmentBuilder builder, MappingPath provenance) {
    for (String field :
        List.of("fundingAttribution", "fundingAttributionID", "projectID", "projectTitle")) {
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
      MappingPath occurrence,
      MappingPath organism) {
    SchemaResource resource =
        graph
            .resource("organism")
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
        graph
            .resource(resourceName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
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
        graph
            .resource(resourceName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException("DwC-DP schema has no resource " + resourceName));
    Set<String> targets = new HashSet<>();
    for (String column : resource.fields().keySet()) {
      if (!column.endsWith("_pk") && !column.endsWith("_fk")) {
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
}
