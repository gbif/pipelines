package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.dwc.terms.DwcTerm;

import java.util.Optional;

/** Declarative mappings for DwC Identification extension rows. */
public final class IdentificationMapping {

  /** Extension.IDENTIFICATION.getRowType(). */
  public static final String ROW_TYPE_IDENTIFICATION =
      "http://rs.tdwg.org/dwc/terms/Identification";

  private IdentificationMapping() {}

  /**
   * Complete identification history attached directly to an Occurrence. Accepted and historical
   * rows are all preserved; the sole-accepted flattening is a separate core enrichment.
   */
  public static ExtensionFragment occurrenceHistory(SchemaGraph graph) {
    SchemaPath identification = SchemaPath.root("identification");
    ExtensionFragmentBuilder builder =
        extensionFragment(
                "occurrence-identification-history",
                ROW_TYPE_IDENTIFICATION,
                "identification")
            .scopeKey("occurrence_fk");
    DirectFieldMappings.from(graph, "identification", identification).addTo(builder);
    return builder.build();
  }

  /** Resolves identification.identifiedByID through agent.agentID on existing history rows. */
  public static ExtensionFragment identifiedBy(SchemaGraph graph) {
    SchemaPath identification = SchemaPath.root("identification");
    return AgentMapping.extension(
        graph,
        ROW_TYPE_IDENTIFICATION,
        new AgentMapping.Spec(
            "identification-identified-by-agent",
            "identification",
            "identifiedByID",
            "identifiedBy",
            DwcTerm.identifiedBy.qualifiedName()),
        Optional.of("occurrence_fk"),
        Optional.of(identification.field("identification_pk")));
  }

}
