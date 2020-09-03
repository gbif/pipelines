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
        "{\"id\": \"id\", \"created\": 0, \"measurementOrFactItems\": [{\"id\": \"Id1\", \"type\": \"Type1\", \"value\": \"1.5\", "
            + "\"accuracy\": \"Accurancy1\", \"unit\": \"Unit1\", \"determinedDate\": \"2010/2011\", \"determinedBy\": "
            + "\"By1\", \"method\": \"Method1\", \"remarks\": \"Remarks1\", \"determinedDateParsed\": {\"gte\": \"2010\", "
            + "\"lte\": \"2011\"}, \"valueParsed\": 1.5}, {\"id\": \"Id2\", \"type\": \"Type2\", \"value\": \"Value2\","
            + " \"accuracy\": \"Accurancy2\", \"unit\": \"Unit2\", \"determinedDate\": \"2010/12/12\", \"determinedBy\": "
            + "\"By2\", \"method\": \"Method2\", \"remarks\": \"Remarks2\", \"determinedDateParsed\": {\"gte\": \"2010-12-12\", "
            + "\"lte\": null}, \"valueParsed\": null}, {\"id\": null, \"type\": null, \"value\": \"1\", \"accuracy\": null, "
            + "\"unit\": null, \"determinedDate\": \"not a date\", \"determinedBy\": null, \"method\": null, \"remarks\": null, "
            + "\"determinedDateParsed\": {\"gte\": null, \"lte\": null}, \"valueParsed\": 1.0}], \"issues\": {\"issueList\": "
            + "[\"MEASUREMENT_OR_FACT_DATE_INVALID\"]}}";

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
    MeasurementOrFactInterpreter.interpret(record, mfr);

    // Should
    Assert.assertEquals(expected, mfr.toString());
  }
}
