package org.gbif.pipelines.spark.dwcdp.mapping.config;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;

/** Adds the DwC-A Event-core escape-hatch marker for extension rows owned by an Occurrence. */
final class OccurrenceExtensionRouting {

  private OccurrenceExtensionRouting() {}

  static void addOccurrenceId(ExtensionFragmentBuilder builder, MappingPath occurrence) {
    builder.field(
        TargetFieldMapping.expression(
            DwcTerm.occurrenceID.qualifiedName(),
            ValueExpression.firstNonBlank(
                ValueExpression.field(occurrence.field("occurrenceID")),
                ValueExpression.concat(
                    ValueExpression.literal("gbif:dwcdp:occurrence:occurrence_pk:"),
                    ValueExpression.field(occurrence.field("occurrence_pk"))))));
  }
}
