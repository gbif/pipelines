package org.gbif.pipelines.spark.dwcdp.mapping.config;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/** Adds the DwC-A Event-core escape-hatch marker for extension rows owned by an Occurrence. */
final class OccurrenceExtensionRouting {

  private static final String OCCURRENCE_URN_PREFIX = "urn:gbif:dwcdp:occurrence:";

  private OccurrenceExtensionRouting() {}

  static void addOccurrenceId(ExtensionFragmentBuilder builder, MappingPath occurrence) {
    builder.field(
        TargetFieldMapping.oneOf(
            DwcTerm.occurrenceID.qualifiedName(),
            ValueAggregation.firstOrUrnFallback(OCCURRENCE_URN_PREFIX),
            occurrence.field("occurrenceID"),
            occurrence.field("occurrence_pk")));
  }
}
