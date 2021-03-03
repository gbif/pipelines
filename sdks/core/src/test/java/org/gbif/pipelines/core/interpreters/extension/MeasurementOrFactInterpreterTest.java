package org.gbif.pipelines.core.interpreters.extension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.junit.Assert;
import org.junit.Test;

public class MeasurementOrFactInterpreterTest {

  @Test
  public void measurementOrFactTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    MeasurementOrFactRecord mfr =
        MeasurementOrFactRecord.newBuilder().setId(record.getId()).setCreated(0L).build();

    // When
    MeasurementOrFactInterpreter.interpret(record, mfr);

    // Should
    Assert.assertEquals(1, mfr.getMeasurementOrFactItems().size());
    Assert.assertEquals("Id1", mfr.getMeasurementOrFactItems().get(0).getMeasurementID());
    Assert.assertEquals("Type1", mfr.getMeasurementOrFactItems().get(0).getMeasurementType());
    Assert.assertEquals("1.5", mfr.getMeasurementOrFactItems().get(0).getMeasurementValue());
    Assert.assertEquals(
        "Accurancy1", mfr.getMeasurementOrFactItems().get(0).getMeasurementAccuracy());
    Assert.assertEquals("Unit1", mfr.getMeasurementOrFactItems().get(0).getMeasurementUnit());
    Assert.assertEquals("By1", mfr.getMeasurementOrFactItems().get(0).getMeasurementDeterminedBy());
    Assert.assertEquals("Method1", mfr.getMeasurementOrFactItems().get(0).getMeasurementMethod());
    Assert.assertEquals("Remarks1", mfr.getMeasurementOrFactItems().get(0).getMeasurementRemarks());
    Assert.assertEquals(
        "2010/2011", mfr.getMeasurementOrFactItems().get(0).getMeasurementDeterminedDate());
  }
}
