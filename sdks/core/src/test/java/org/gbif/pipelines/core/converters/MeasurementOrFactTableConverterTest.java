package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.junit.Assert;
import org.junit.Test;

public class MeasurementOrFactTableConverterTest {

  @Test
  public void measurementOrFactTableTest() {

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

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    GbifIdRecord gbifIdRecord = GbifIdRecord.newBuilder().setId("777").setGbifId(777L).build();

    // When
    Optional<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(gbifIdRecord, extendedRecord);

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("777", result.get().getGbifid());
    // Verbatim
    Assert.assertEquals("Id1", result.get().getVMeasurementid());
    Assert.assertEquals("Type1", result.get().getVMeasurementtype());
    Assert.assertEquals("1.5", result.get().getVMeasurementvalue());
    Assert.assertEquals("Accurancy1", result.get().getVMeasurementaccuracy());
    Assert.assertEquals("Unit1", result.get().getVMeasurementunit());
    Assert.assertEquals("By1", result.get().getVMeasurementdeterminedby());
    Assert.assertEquals("Method1", result.get().getVMeasurementmethod());
    Assert.assertEquals("Remarks1", result.get().getVMeasurementremarks());
    Assert.assertEquals("2010/2011", result.get().getVMeasurementdetermineddate());
    // Interpreted
    Assert.assertEquals("Id1", result.get().getMeasurementid());
    Assert.assertEquals("Type1", result.get().getMeasurementtype());
    Assert.assertEquals("1.5", result.get().getMeasurementvalue());
    Assert.assertEquals("Accurancy1", result.get().getMeasurementaccuracy());
    Assert.assertEquals("Unit1", result.get().getMeasurementunit());
    Assert.assertEquals("By1", result.get().getMeasurementdeterminedby());
    Assert.assertEquals("Method1", result.get().getMeasurementmethod());
    Assert.assertEquals("Remarks1", result.get().getMeasurementremarks());
    Assert.assertEquals("2010/2011", result.get().getMeasurementdetermineddate());
  }

  @Test
  public void noExtensionTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId("id").build();

    GbifIdRecord gbifIdRecord = GbifIdRecord.newBuilder().setId("777").setGbifId(777L).build();

    // When
    Optional<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(gbifIdRecord, extendedRecord);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void basicRecordNullTest() {
    // State
    GbifIdRecord gbifIdRecord = GbifIdRecord.newBuilder().setId("777").setGbifId(777L).build();

    // When
    Optional<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(gbifIdRecord, null);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void extendedRecordNullTest() {

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

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    // When
    Optional<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(null, extendedRecord);

    // Should
    Assert.assertFalse(result.isPresent());
  }
}
