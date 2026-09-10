package org.gbif.pipelines.spark.dwcdp.mapping.config;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.CaseExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;

/** Declarative DwC basisOfRecord classification rules. */
public final class BasisOfRecordMapping {

  private BasisOfRecordMapping() {}

  public static ValueExpression expression(FieldRef materialCategory, FieldRef eventType) {
    ValueExpression material = ValueExpression.field(materialCategory);
    ValueExpression event = ValueExpression.field(eventType);

    return CaseExpression.builder()
        .whenIn(material, "PreservedSpecimen", "preserved")
        .whenIn(material, "FossilSpecimen", "fossilized")
        .whenIn(material, "LivingSpecimen", "living")
        .whenIn(material, "MaterialSample", "tissue", "DNA extract")
        .whenIn(event, "MaterialSample", "NucleotideAnalysis")
        .whenIn(event, "MachineObservation", "Sensor")
        .whenIn(event, "HumanObservation", "Observation")
        .otherwise("Occurrence");
  }
}
