package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaResource;

/** Reusable Occurrence-core enrichment fragments. */
public final class OccurrenceCoreMapping {

  private OccurrenceCoreMapping() {}

  /**
   * Organism fields denormalized onto Occurrence via the schema-declared weak organismID relation.
   */
  public static CoreFragment organism(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath organism = occurrence.join("organism").via("organismID").optional().exactlyOne();
    CoreFragmentBuilder builder = coreFragment("occurrence-core-organism", organism);
    addOrganismTargets(graph, builder, occurrence, organism);
    return builder.build();
  }

  /**
   * Exactly one accepted identification enriches Occurrence. The acceptance filter is applied
   * before exactly-one cardinality, so zero or multiple accepted rows contribute nothing.
   */
  public static CoreFragment acceptedIdentification(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification", identification);

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
  public static CoreFragment acceptedIdentificationTaxon(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath taxon =
        identification
            .join("identification-taxon")
            .via("identification_fk")
            .optional()
            .exactlyOne();

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification-taxon", taxon);

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
  public static CoreFragment acceptedIdentificationAgentRoles(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath role =
        identification
            .join("identification-agent-role")
            .via("identification_fk")
            .optional()
            .fanOut();
    MappingPath agent = role.join("agent").via("agent_fk").optional().exactlyOne();

    return coreFragment("occurrence-core-accepted-identification-agent-roles", agent)
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
  public static CoreFragment acceptedIdentificationAgent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = acceptedIdentificationPath(occurrence);
    MappingPath agent = identification.join("agent").via("identifiedByID").optional().fanOut();

    return coreFragment("occurrence-core-accepted-identification-agent", agent)
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

  /**
   * Exactly one material citing the occurrence as evidence enriches Occurrence. Usage-policy is on
   * the same path, so license/rightsHolder are suppressed whenever the material relationship is
   * ambiguous.
   */
  public static CoreFragment material(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath usagePolicy =
        material.join("usage-policy").via("usagePolicy_fk").optional().exactlyOne();

    CoreFragmentBuilder builder = coreFragment("occurrence-core-material", usagePolicy);

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

  /** Resolves material.collectedByID for one unambiguous evidence material. */
  public static CoreFragment materialCollectedBy(SchemaGraph graph) {
    return materialAgent(
        graph,
        "occurrence-core-material-collected-by-agent",
        "collectedByID",
        "collectedBy",
        DwcTerm.recordedBy.qualifiedName());
  }

  /** Resolves material.identifiedByID for one unambiguous evidence material. */
  public static CoreFragment materialIdentifiedBy(SchemaGraph graph) {
    return materialAgent(
        graph,
        "occurrence-core-material-identified-by-agent",
        "identifiedByID",
        "identifiedBy",
        DwcTerm.identifiedBy.qualifiedName());
  }

  /** Ordered collector AgentRoles are the final recordedBy fallback for one evidence material. */
  public static CoreFragment materialCollectorRoles(SchemaGraph graph) {
    return AgentRoleMapping.linkedCore(
        graph,
        AgentRoleMapping.LinkedSpec.orderedDistinctNames(
            "occurrence-core-material-collector-roles",
            "occurrence",
            "material",
            "evidenceForOccurrenceID",
            "material-agent-role",
            "materialEntity_fk",
            "collector",
            DwcTerm.recordedBy.qualifiedName()));
  }

  private static CoreFragment materialAgent(
      SchemaGraph graph,
      String fragmentName,
      String idColumn,
      String valueColumn,
      String targetTerm) {
    return AgentMapping.linkedCore(
        graph,
        new AgentMapping.LinkedSpec(
            fragmentName,
            "occurrence",
            "material",
            "evidenceForOccurrenceID",
            idColumn,
            valueColumn,
            targetTerm));
  }

  /** Geological-context fields from one unambiguous context on one evidence material. */
  public static CoreFragment materialGeologicalContext(SchemaGraph graph) {
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

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-geological-context", geologicalContext);

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

  /**
   * Material-linked protocols contribute to occurrence samplingProtocol for one evidence material.
   */
  public static CoreFragment materialProtocols(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link =
        material.join("material-protocol").via("materialEntity_fk").optional().fanOut();
    MappingPath protocol = link.join("protocol").via("protocol_fk").optional().exactlyOne();

    return coreFragment("occurrence-core-material-protocols", protocol)
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
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath provenance =
        material.join("provenance").via("provenance_fk").optional().exactlyOne();

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-direct-provenance", provenance);
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  /** material -> material-provenance -> provenance contributions for one evidence material. */
  public static CoreFragment materialProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material =
        occurrence.join("material").via("evidenceForOccurrenceID").optional().exactlyOne();
    MappingPath link =
        material.join("material-provenance").via("materialEntity_fk").optional().fanOut();
    MappingPath provenance = link.join("provenance").via("provenance_fk").optional().exactlyOne();

    CoreFragmentBuilder builder = coreFragment("occurrence-core-material-provenance", provenance);
    addProvenanceTargets(builder, provenance);
    return builder.build();
  }

  private static void addProvenanceTargets(CoreFragmentBuilder builder, MappingPath provenance) {
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

  /** Resolves recordedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment recordedBy(SchemaGraph graph) {
    return AgentMapping.core(
        graph,
        new AgentMapping.Spec(
            "occurrence-recorded-by-agent",
            "occurrence",
            "recordedByID",
            "recordedBy",
            DwcTerm.recordedBy.qualifiedName()));
  }

  /** Resolves identifiedByID through agent.agentID while preserving an explicit publisher value. */
  public static CoreFragment identifiedBy(SchemaGraph graph) {
    return AgentMapping.core(
        graph,
        new AgentMapping.Spec(
            "occurrence-identified-by-agent",
            "occurrence",
            "identifiedByID",
            "identifiedBy",
            DwcTerm.identifiedBy.qualifiedName()));
  }

  /** Direct occurrenceProtocol_fk -> resolved samplingProtocol when the protocol exists. */
  public static CoreFragment directSamplingProtocol(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath protocol =
        occurrence.join("protocol").via("occurrenceProtocol_fk").optional().exactlyOne();

    return coreFragment("occurrence-direct-sampling-protocol", protocol)
        .field(
            TargetFieldMapping.oneOf(
                DwcTerm.samplingProtocol.qualifiedName(),
                ValueAggregation.labeledOrFallback(": "),
                protocol.field("protocolType"),
                protocol.field("protocolName"),
                protocol.field("protocolDescription")))
        .build();
  }

  private static void addOrganismTargets(
      SchemaGraph graph,
      CoreFragmentBuilder builder,
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

  private static MappingPath acceptedIdentificationPath(MappingPath occurrence) {
    return occurrence
        .join("identification")
        .via("occurrence_fk")
        .filter(FilterExpression.eq("isAcceptedIdentification", true))
        .optional()
        .exactlyOne();
  }
}
