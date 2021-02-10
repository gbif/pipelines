package org.gbif.pipelines.core.interpreters.extension;

import java.util.Arrays;
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

    // Expected
    String expected =
        "{\"id\": \"id\", \"created\": 0, \"measurementOrFactItems\": [{\"measurementType\": \"Type1\", "
            + "\"measurementValue\": \"1.5\", \"measurementUnit\": \"Unit1\"}, {\"measurementType\": \"Type2\", "
            + "\"measurementValue\": \"Value2\", \"measurementUnit\": \"Unit2\"}, {\"measurementType\": null, "
            + "\"measurementValue\": \"1\", \"measurementUnit\": null}], \"issues\": {\"issueList\": []}}";

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

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DwcTerm.measurementID.qualifiedName(), "Id2");
    ext2.put(DwcTerm.measurementType.qualifiedName(), "Type2");
    ext2.put(DwcTerm.measurementValue.qualifiedName(), "Value2");
    ext2.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy2");
    ext2.put(DwcTerm.measurementUnit.qualifiedName(), "Unit2");
    ext2.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By2");
    ext2.put(DwcTerm.measurementMethod.qualifiedName(), "Method2");
    ext2.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks2");
    ext2.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/12/12");

    Map<String, String> ext3 = new HashMap<>();
    ext3.put(DwcTerm.measurementValue.qualifiedName(), "1");
    ext3.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "not a date");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Arrays.asList(ext1, ext2, ext3));

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    MeasurementOrFactRecord mfr =
        MeasurementOrFactRecord.newBuilder().setId(record.getId()).setCreated(0L).build();

    // When
    MeasurementOrFactInterpreter.create().interpret(record, mfr);

    // Should
    Assert.assertEquals(expected, mfr.toString());
  }
}
