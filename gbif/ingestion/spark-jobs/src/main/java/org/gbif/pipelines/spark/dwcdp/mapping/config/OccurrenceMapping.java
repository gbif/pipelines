package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.Optional;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

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
    MappingPath organism = OccurrenceEnrichment.organismPath(occurrence);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-organism", ROW_TYPE_OCCURRENCE, organism)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.organismTargets(graph, occurrence, organism).forEach(builder::field);
    return builder.build();
  }

  /**
   * The single accepted identification enriches its existing occurrence row. Filtering happens
   * before exactly-one cardinality, so zero or multiple accepted identifications contribute no
   * enrichment, matching the legacy current-identification rule.
   */
  public static ExtensionFragment acceptedIdentification(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification", ROW_TYPE_OCCURRENCE, identification)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.acceptedIdentificationTargets(graph, identification)
        .forEach(builder::field);
    return builder.build();
  }

  /**
   * Uses one unambiguous IdentificationTaxon row as a taxonomic fallback for the accepted
   * Identification. Multiple taxon-formula components are deliberately not flattened into one
   * Occurrence value.
   */
  public static ExtensionFragment acceptedIdentificationTaxon(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath taxon = OccurrenceEnrichment.acceptedIdentificationTaxonPath(identification);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification-taxon", ROW_TYPE_OCCURRENCE, taxon)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.acceptedIdentificationTaxonTargets(taxon).forEach(builder::field);
    return builder.build();
  }

  /** All agents attached to the accepted Identification are a final identifiedBy fallback. */
  public static ExtensionFragment acceptedIdentificationAgentRoles(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath role = OccurrenceEnrichment.acceptedIdentificationAgentRolePath(identification);
    MappingPath agent = OccurrenceEnrichment.roleAgentPath(role);
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-accepted-identification-agent-roles", ROW_TYPE_OCCURRENCE, agent)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.acceptedIdentificationAgentRoleTargets(role, agent)
        .forEach(builder::field);
    return builder.build();
  }

  /** Resolves the sole accepted identification's identifiedByID before material fallback. */
  public static ExtensionFragment acceptedIdentificationAgent(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath identification = OccurrenceEnrichment.acceptedIdentificationPath(occurrence);
    MappingPath agent = OccurrenceEnrichment.identifiedByAgentPath(identification);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-accepted-identification-agent", ROW_TYPE_OCCURRENCE, agent)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.acceptedIdentificationAgentTargets(identification, agent)
        .forEach(builder::field);
    return builder.build();
  }

  /**
   * Exactly one evidence material enriches its existing occurrence row. The relationship is a
   * schema-declared weak FK (material.evidenceForOccurrenceID -> occurrence.occurrenceID), so the
   * reverse traversal remains schema-backed via(...). Usage policy is traversed on the same path so
   * its fields are gated by the same exactly-one-material decision.
   */
  public static ExtensionFragment material(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath usagePolicy = OccurrenceEnrichment.usagePolicyPath(material);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material", ROW_TYPE_OCCURRENCE, usagePolicy)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.materialTargets(graph, material).forEach(builder::field);
    DirectFieldMappings.from(graph, "usage-policy", usagePolicy).addTo(builder);
    return builder.build();
  }

  /** Direct material.provenance_fk contributions for an unambiguous evidence material. */
  public static ExtensionFragment materialDirectProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath provenance = OccurrenceEnrichment.directProvenancePath(material);

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-direct-provenance", ROW_TYPE_OCCURRENCE, provenance)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.provenanceTargets(provenance).forEach(builder::field);
    return builder.build();
  }

  /** material -> material-provenance -> provenance contributions for one evidence material. */
  public static ExtensionFragment materialProvenance(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath provenance = OccurrenceEnrichment.linkedProvenancePath(material);

    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-provenance", ROW_TYPE_OCCURRENCE, provenance)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.provenanceTargets(provenance).forEach(builder::field);
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
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath geologicalContext = OccurrenceEnrichment.geologicalContextPath(material);
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-material-geological-context", ROW_TYPE_OCCURRENCE, geologicalContext)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.geologicalContextTargets(graph, geologicalContext).forEach(builder::field);
    return builder.build();
  }

  /** Material-linked protocols contribute to samplingProtocol on an Event-nested Occurrence row. */
  public static ExtensionFragment materialProtocols(SchemaGraph graph) {
    MappingPath occurrence = MappingPath.root(graph, "occurrence");
    MappingPath material = OccurrenceEnrichment.evidenceMaterialPath(occurrence);
    MappingPath protocol = OccurrenceEnrichment.materialProtocolPath(material);
    ExtensionFragmentBuilder builder =
        extensionFragment("occurrence-material-protocols", ROW_TYPE_OCCURRENCE, protocol)
            .scopeKey("event_fk")
            .rowMatch(occurrence.field("occurrence_pk"));
    OccurrenceEnrichment.materialProtocolTargets(protocol).forEach(builder::field);
    return builder.build();
  }
}
