package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Reusable Occurrence-core enrichment fragments. */
public final class OccurrenceCoreMapping {

  private OccurrenceCoreMapping() {}

  /**
   * Organism fields denormalized onto Occurrence via the schema-declared weak organismID relation.
   */
  public static CoreFragment organism(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath organism = OccurrenceEnrichment.organismPath(occurrence);
    CoreFragmentBuilder builder = coreFragment("occurrence-core-organism", organism);
    OccurrenceEnrichment.organismTargets(graph, occurrence, organism).forEach(builder::field);
    return builder.build();
  }

  /**
   * Exactly one accepted identification enriches Occurrence. The acceptance filter is applied
   * before exactly-one cardinality, so zero or multiple accepted rows contribute nothing.
   */
  public static CoreFragment acceptedIdentification(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification", identification);
    OccurrenceEnrichment.acceptedIdentificationTargets(graph, identification)
        .forEach(builder::field);
    return builder.build();
  }

  /**
   * Uses one unambiguous IdentificationTaxon row as a taxonomic fallback for the accepted
   * Identification. Multiple taxon-formula components are deliberately not flattened into one
   * Occurrence value.
   */
  public static CoreFragment acceptedIdentificationTaxon(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath taxon = OccurrenceEnrichment.acceptedIdentificationTaxonPath(identification);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification-taxon", taxon);
    OccurrenceEnrichment.acceptedIdentificationTaxonTargets(taxon).forEach(builder::field);
    return builder.build();
  }

  /** All agents attached to the accepted Identification are a final identifiedBy fallback. */
  public static CoreFragment acceptedIdentificationAgentRoles(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath role = OccurrenceEnrichment.acceptedIdentificationAgentRolePath(identification);
    MappingPath agent = OccurrenceEnrichment.roleAgentPath(role);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification-agent-roles", agent);
    OccurrenceEnrichment.acceptedIdentificationAgentRoleTargets(role, agent)
        .forEach(builder::field);
    return builder.build();
  }

  /** Resolves the sole accepted identification's identifiedByID before material fallback. */
  public static CoreFragment acceptedIdentificationAgent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath agent = OccurrenceEnrichment.identifiedByAgentPath(identification);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-accepted-identification-agent", agent);
    OccurrenceEnrichment.acceptedIdentificationAgentTargets(identification, agent)
        .forEach(builder::field);
    return builder.build();
  }

  /**
   * Exactly one material citing the occurrence as evidence enriches Occurrence. Usage-policy is on
   * the same path, so license/rightsHolder are suppressed whenever the material relationship is
   * ambiguous.
   */
  public static CoreFragment material(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath usagePolicy = OccurrenceEnrichment.usagePolicyPath(material);
    CoreFragmentBuilder builder = coreFragment("occurrence-core-material", usagePolicy);
    OccurrenceEnrichment.materialTargets(graph, material).forEach(builder::field);
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
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath geologicalContext = OccurrenceEnrichment.geologicalContextPath(material);
    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-geological-context", geologicalContext);
    OccurrenceEnrichment.geologicalContextTargets(graph, geologicalContext).forEach(builder::field);
    return builder.build();
  }

  /**
   * Material-linked protocols contribute to occurrence samplingProtocol for one evidence material.
   */
  public static CoreFragment materialProtocols(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath protocol = OccurrenceEnrichment.materialProtocolPath(material);
    CoreFragmentBuilder builder = coreFragment("occurrence-core-material-protocols", protocol);
    OccurrenceEnrichment.materialProtocolTargets(protocol).forEach(builder::field);
    return builder.build();
  }

  /** Direct material.provenance_fk contributions for an unambiguous evidence material. */
  public static CoreFragment materialDirectProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath provenance = OccurrenceEnrichment.directProvenancePath(material);

    CoreFragmentBuilder builder =
        coreFragment("occurrence-core-material-direct-provenance", provenance);
    OccurrenceEnrichment.provenanceTargets(provenance).forEach(builder::field);
    return builder.build();
  }

  /** material -> material-provenance -> provenance contributions for one evidence material. */
  public static CoreFragment materialProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath provenance = OccurrenceEnrichment.linkedProvenancePath(material);

    CoreFragmentBuilder builder = coreFragment("occurrence-core-material-provenance", provenance);
    OccurrenceEnrichment.provenanceTargets(provenance).forEach(builder::field);
    return builder.build();
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
}
